//! FIX session layer: sequence numbers, heartbeats and message recovery.
//!
//! A [`Session`] holds the mutable per-connection state — inbound/outbound
//! sequence numbers, the heartbeat interval and the negotiated
//! [`FixVersion`](crate::repository::FixVersion). The [`crate::endpoint`] layer
//! runs the session loop internally; applications interact with a live session
//! through a [`SessionHandle`]:
//!
//! * `tx: mpsc::Sender<`[`SessionCommand`]`>` — send an application message with
//!   [`SessionCommand::Send`], drive a resend with [`SessionCommand::Replay`] /
//!   [`SessionCommand::ReplayComplete`], or [`SessionCommand::Disconnect`].
//! * `events: mpsc::Receiver<`[`SessionEvent`]`>` — a stream of session lifecycle
//!   and inbound-message events.
//!
//! The session automatically emits heartbeats, answers TestRequests, issues a
//! ResendRequest on a sequence gap, and handles logout — so application code
//! technically only needs to react to
//! [`SessionEvent::MessageReceived`] (inbound *application* messages),
//! [`SessionEvent::ResendRequest`] (inbound *resend requests*), and
//! [`SessionEvent::Disconnected`], though handling
//! [`SessionEvent::RawMessageSent`] and [`SessionEvent::SessionState`] are
//! usually needed in order to correctly recover the session.
//!
//! ```ignore
//! use babelfix::session::{SessionHandle, SessionEvent};
//! use babelfix::schema::FIX_Latest::Fields;
//! use futures::StreamExt;
//!
//! async fn drive(mut handle: SessionHandle) {
//!     while let Some(event) = handle.events.next().await {
//!         match event {
//!             SessionEvent::MessageReceived(msg) => {
//!                 if let Some(t) = msg.header.tag(Fields::MsgType) {
//!                     println!("received {}", t.as_string());
//!                 }
//!             }
//!             SessionEvent::Disconnected => break,
//!             // ...
//!             _ => {}
//!         }
//!     }
//! }
//! ```

use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use futures::{Sink, SinkExt};
use tracing::{debug, error, info};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionIdentifier {
  pub begin_string: String,
  pub sender_comp_id: String,
  pub target_comp_id: String,
}

#[derive(Clone, Default)]
pub struct Session {
  pub next_out_seq_num: u32,
  pub next_in_seq_num: u32,
  pub heartbeat_interval: std::time::Duration,
  pub fix_version: Arc<crate::repository::FixVersion>,
}

impl std::fmt::Debug for Session {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Session")
      .field("next_out_seq_num", &self.next_out_seq_num)
      .field("next_in_seq_num", &self.next_in_seq_num)
      .field("heartbeat_interval", &self.heartbeat_interval)
      .field("fix_version", &self.fix_version.name)
      .finish()
  }
}

impl Session {
  pub fn new(fix_version: Arc<crate::repository::FixVersion>) -> Self {
    Self {
      next_out_seq_num: 1,
      next_in_seq_num: 1,
      heartbeat_interval: std::time::Duration::from_secs(30),
      fix_version,
    }
  }

  pub async fn send(
    &mut self,
    mut msg: crate::message::builder::Message,
    session_id: &SessionIdentifier,
    writer: &mut (impl Sink<crate::message::FixMessage> + Unpin),
    session_event_sender: &mut futures::channel::mpsc::Sender<SessionEvent>,
  ) -> crate::Result<()> {
    // Only set the seqnum if it's not a gap fill
    if !msg
      .header
      .has_tag(crate::schema::FIX_Latest::Fields::MsgSeqNum)
    {
      msg.header.set_tag(
        crate::schema::FIX_Latest::Fields::MsgSeqNum,
        self.next_out_seq_num,
      );
      self.next_out_seq_num += 1;
    }

    msg.header.set_tag(
      crate::schema::FIX_Latest::Fields::SenderCompID,
      session_id.sender_comp_id.clone(),
    );
    msg.header.set_tag(
      crate::schema::FIX_Latest::Fields::TargetCompID,
      session_id.target_comp_id.clone(),
    );
    msg.header.set_tag(
      crate::schema::FIX_Latest::Fields::SendingTime,
      crate::util::time_now_fix(),
    );

    let msg = msg.as_message()?;
    debug!("Sending message: {:?}", msg);
    session_event_sender
      .send(SessionEvent::RawMessageSent(msg.clone(), self.clone()))
      .await?;
    writer
      .send(msg)
      .await
      .map_err(|_| crate::Error::connection_failed("Failed to send message"))
  }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum SessionCommand {
  /// Send the message on the session. The message is assigned the next outbound
  /// sequence number and has its session header fields populated. Any supplied
  /// `MsgSeqNum`, `SendingTime`, `SenderCompID` or `TargetCompID` is overwritten
  /// — applications cannot set these correctly, so they are left to the session.
  ///
  /// It is an error to attempt to send a message while a replay is in progress;
  /// use [`SessionCommand::Replay`] to send messages in response to a
  /// [`SessionEvent::ResendRequest`].
  Send(crate::message::builder::Message),

  /// Replay the sequence number in `MsgSeqNum` with the supplied message. Only
  /// valid between receipt of a [`SessionEvent::ResendRequest`] and a
  /// subsequent [`SessionCommand::ReplayComplete`].
  ///
  /// For more details on resends, see [`SessionEvent::ResendRequest`].
  Replay(crate::message::builder::Message),

  /// Indicate that all messages for the current resend request have been sent.
  ///
  /// For more details on resends, see [`SessionEvent::ResendRequest`].
  ReplayComplete,

  /// Disconnect the session. The completion of the disconnection will be
  /// indicated by a [`SessionEvent::Disconnected`] event.
  Disconnect,

  /// Request the current state of the session. The session state will be
  /// provided to the supplied [`oneshot::Sender`].
  GetSessionState(oneshot::Sender<Session>),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SessionEvent {
  /// A connection has been established to the peer, but no logon exchange has
  /// been performed yet.
  ConnectionEstablished,

  /// Local recovery has completed - the remote has sent all messages missed on
  /// the session. Note that this does not mean the remote has recevied, or even
  /// requested, any missing messages from us.
  RecoveryCompleted,

  /// Occasionally emitted to indicate the current state of the session,
  /// including the next expected inbound and outbound sequence numbers.
  /// The state is also included in other events and should be persisted by
  /// applications in order to correctly recover - the inbound and outbound
  /// sequence numbers are required to be provided in response to
  /// [`EndpointEvent::NewSession`](crate::endpoint::EndpointEvent::NewSession).
  SessionState(Session),

  /// Emitted when any FIX message was received from the remote, no matter whether
  /// this is valid or not. Includes admin messages (logon, logout, resend
  /// request, etc.). Useful for auditing, logging and display. Business
  /// logic and processing should not use this message: rather use the
  /// [`SessionEvent::MessageReceived`] event, which emitted for valid,
  /// well-sequenced messages.
  ///
  /// FIXME: Should include the socket send time
  RawMessageReceived(crate::message::FixMessage, Session),

  /// Emitted when any FIX message was sent to the remote, including admin
  /// messages. Useful for auditing, logging and display.
  ///
  /// FIXME: Should include the socket send time.
  ///
  /// Note that the message should be persisted by the application (unmodified)
  /// so that it can be replayed in response to any future resend request
  /// [`SessionEvent::ResendRequest`]. Note that this library does not provide
  /// any persistence at all, so you must implement your own persistence.
  RawMessageSent(crate::message::FixMessage, Session),

  /// Applications should use this event for business processing.
  ///
  /// Emitted for each valid, session protocol-compliant business message in
  /// sequence number order. Does not include session admin messages. When a
  /// resend is required from the remote, this event will be emitted for replayed
  /// messages before any new messages, thus the application does not need to be
  /// concerned with processing out-of-sequence messages, except to the extent
  /// that `PossDupFlag` might be set on the message by the remote.
  MessageReceived(crate::message::builder::Message),

  /// The remote requested replay (resend) of messages from the given sequence
  /// number range (inclusive of `end_seq_no`). `end_seq_no` is always supplied,
  /// even if the remote sent an open-ended resend request; this library
  /// determines the correct end sequence number in that case.
  ///
  /// Applications must action this resend request by using the
  /// [`SessionCommand::Replay`] and [`SessionCommand::ReplayComplete`] commands
  /// to send the requested message sequence.
  ///
  /// Only messages that were provided by the [`SessionEvent::RawMessageSent`]
  /// event should be replayed, or the application should faithfully construct
  /// each equivalent with the relevant sequence number populated in `MsgSeqNum`.
  /// Any skipped sequence numbers will be gap-filled automatically, allowing
  /// applications to decide not to replay certain messages, for example to avoid
  /// re-sending a stale order request. Once all messages have been replayed, the
  /// application should send [`SessionCommand::ReplayComplete`]; any remaining
  /// sequence numbers will be gap-filled automatically.
  ResendRequest {
    /// The resend request message itself. Applications do not typically need to
    /// inspect this.
    resend_request: crate::message::builder::Message,
    /// The first sequence number to be resent.
    begin_seq_no: u32,
    /// The last sequence number to be resent (inclusive).
    end_seq_no: u32,
  },

  /// Emitted when the session is disconnected, either due to a logout message
  /// or a network error.
  Disconnected,
  // Emitted when the session is disconnected due to an error, either a
  // protocol violation or a network error.
  //
  // FIXME: Not actually used yet.
  // Error(crate::Error),
}

#[derive(Debug)]
pub struct SessionHandle {
  pub session_id: SessionIdentifier,
  pub tx: mpsc::Sender<SessionCommand>,
  // FIXME: Remove this from the struct so that you can cheaply clone it
  pub events: mpsc::Receiver<SessionEvent>,
}

#[derive(Debug, Default, Clone)]
struct Replay {
  pub begin_seq_no: u32,
  pub end_seq_no: u32,

  next_expected_seq_num: u32,
  gap_fill_count: u32,

  queue: Vec<crate::message::builder::Message>,
}

pub(crate) struct SessionManager<'stream, E, D> {
  rx: tokio_util::codec::FramedRead<tokio::net::tcp::ReadHalf<'stream>, D>,
  tx: tokio_util::codec::FramedWrite<tokio::net::tcp::WriteHalf<'stream>, E>,
  session_id: SessionIdentifier,
  session: Session,
  session_event_sender: mpsc::Sender<SessionEvent>,
  session_msg_recv: mpsc::Receiver<SessionCommand>,
  rerequest_in_progress: Option<u32>, // seq num after which replay is complete
  recovery_tr_id: Option<String>,
  replay: Option<Replay>,
}

impl<'stream, E, D> SessionManager<'stream, E, D>
where
  D: tokio_util::codec::Decoder<Item = crate::message::FixMessage>,
  E: tokio_util::codec::Encoder<crate::message::FixMessage>,
  <D as tokio_util::codec::Decoder>::Error: std::fmt::Display,
{
  pub fn new(
    rx: tokio_util::codec::FramedRead<tokio::net::tcp::ReadHalf<'stream>, D>,
    tx: tokio_util::codec::FramedWrite<tokio::net::tcp::WriteHalf<'stream>, E>,
    session_id: SessionIdentifier,
    session: Session,
    session_event_sender: mpsc::Sender<SessionEvent>,
    session_msg_recv: mpsc::Receiver<SessionCommand>,
  ) -> Self {
    Self {
      rx,
      tx,
      session_id,
      session,
      session_event_sender,
      session_msg_recv,
      rerequest_in_progress: None,
      recovery_tr_id: None,
      replay: None,
    }
  }

  pub async fn run(
    &mut self,
    logon_msg_in: crate::message::builder::Message,
  ) -> crate::Result<()> {
    if !self.handle_session_message(logon_msg_in).await? {
      return Ok(());
    }

    let tr_id = format!(
      "HELO-{}",
      chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(chrono::Utc::now().timestamp_millis() * 1_000_000)
    );
    let mut test_request = crate::message::builder::Message::new(
      self.session.fix_version.clone(),
      "1",
    )?;
    test_request
      .body
      .set_tag(crate::schema::FIX_Latest::Fields::TestReqID, tr_id.clone());
    self
      .session
      .send(
        test_request,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await?;
    self.recovery_tr_id = Some(tr_id);

    let mut out_heartbeat_timer = tokio::time::interval_at(
      tokio::time::Instant::now() + self.session.heartbeat_interval,
      self.session.heartbeat_interval,
    );

    let mut in_heartbeat_timer = tokio::time::interval_at(
      tokio::time::Instant::now() + self.session.heartbeat_interval,
      self.session.heartbeat_interval,
    );
    let mut missed_heartbeats = 0;

    loop {
      tokio::select! {
        _ = out_heartbeat_timer.tick() => {
          let heartbeat =
            crate::message::builder::Message::new(
              self.session.fix_version.clone(),
              "0")?;
          self.send(heartbeat).await?;
        },
        _ = in_heartbeat_timer.tick() => {
          missed_heartbeats += 1;
          match missed_heartbeats {
            1 => {
              debug!("Missed first heartbeat");
            }
            2 => {
              debug!("Missed second heartbeat, sending TestRequest");
              let mut test_request =
                crate::message::builder::Message::new(
                  self.session.fix_version.clone(),
                  "1")?;
              test_request.body.set_tag(
                crate::schema::FIX_Latest::Fields::TestReqID,
                format!("HB{}", chrono::Utc::now().timestamp_millis()),
              );
              self.send(test_request).await?;
            }
            3.. => {
              error!("Missed third heartbeat, logging out");
              let mut logout =
                crate::message::builder::Message::new(
                  self.session.fix_version.clone(),
                  "5")?;
              logout.body.set_tag(
                crate::schema::FIX_Latest::Fields::Text,
                "Heartbeat timeout",
              );
              self.send(logout).await?;
              break;
            }
            _ => { unreachable!() }
          }
        }
        cmd = self.session_msg_recv.next() => {
          // The outbound heartbeat timer is reset by commands that put a
          // message on the wire, since that message is itself evidence of
          // liveness. Commands that transmit nothing must leave it alone, or
          // an application polling the session would silence its own
          // heartbeats and be declared dead by the peer.
          if let Some(cmd) = cmd {
            match cmd {
              SessionCommand::Send(msg) => {
                out_heartbeat_timer.reset();
                self.send(msg).await?;
              }
              SessionCommand::Replay(msg) => {
                out_heartbeat_timer.reset();
                self.replay(msg).await?;
              }
              SessionCommand::ReplayComplete => {
                out_heartbeat_timer.reset();
                self.complete_replay().await?;
              }
              SessionCommand::Disconnect => {
                out_heartbeat_timer.reset();
                info!("Session disconnect requested");
                // Always announce the intent to disconnect, so the peer can
                // tell an orderly shutdown from a network failure.
                let mut logout =
                  crate::message::builder::Message::new(
                    self.session.fix_version.clone(),
                    "5")?;
                logout.body.set_tag(
                  crate::schema::FIX_Latest::Fields::Text,
                  "Disconnect requested by application",
                );
                self.send(logout).await?;
                break;
              }
              SessionCommand::GetSessionState(resp) => {
                let _ = resp.send(self.session.clone());
              }
            }
          } else {
            debug!("Session message channel closed, stopping session manager");
            break;
          }
        },
        socket_event = self.rx.next() => {
          match socket_event {
            Some(Ok(fix_message)) => {
              in_heartbeat_timer.reset();
              missed_heartbeats = 0;
              let msg = crate::message::builder::Message::from_message(
                &fix_message,
              )?;
              self.session_event_sender
                .send(SessionEvent::RawMessageReceived(
                  fix_message,
                  self.session.clone()
                ))
                .await?;
              if !self.handle_session_message(msg).await? {
                break;
              }
            }
            Some(Err(e)) => {
              error!("Error reading from socket: {e}");
              break;
            }
            None => {
              info!("Client disconnected");
              break;
            }
          }
        }
        else => {
          // To STFU the errors
          break;
        }
      }
    }
    Ok(())
  }

  async fn handle_session_message(
    &mut self,
    msg: crate::message::builder::Message,
  ) -> crate::Result<bool> {
    let msg_seq_num = msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::MsgSeqNum)
      .ok_or_else(|| crate::Error::protocol_violation("Missing MsgSeqNum"))?
      .as_int()
      .ok_or_else(|| {
        crate::Error::protocol_violation("MsgSeqNum is not an integer")
      })? as u32;

    // A SequenceReset-GapFill stands in for the messages it skips over, so it
    // occupies a slot in the stream and is subject to the same sequence checks
    // as any other message. Accepting one differs only in where the expected
    // inbound sequence number lands: at NewSeqNo rather than one further on.
    let gap_fill_new_seq_num = if msg.fix_message.msg_type == "4" {
      Some(Self::gap_fill_new_seq_num(&msg)?)
    } else {
      None
    };

    match msg_seq_num.cmp(&self.session.next_in_seq_num) {
      std::cmp::Ordering::Equal => {
        if let Some(new_seq_num) = gap_fill_new_seq_num {
          if new_seq_num <= msg_seq_num {
            return Err(crate::Error::protocol_violation(format!(
              "Invalid NewSeqNo in GapFill; expected greater than {} but got {}",
              msg_seq_num, new_seq_num
            )));
          }
          info!(
            "GapFill received, advancing next_in_seq_num to {}",
            new_seq_num
          );
          self.session.next_in_seq_num = new_seq_num;
        } else {
          self.session.next_in_seq_num += 1;
        }
      }
      std::cmp::Ordering::Greater => {
        // A gap. Request everything from the first missing message onwards
        // with an open-ended EndSeqNo of 0, and discard this message and every
        // subsequent one until the gap closes: they all fall inside the range
        // just requested, so the peer will retransmit them in order. This is
        // the approach the session layer specification recommends, and it
        // avoids holding an unbounded queue of out-of-order messages.
        if self.rerequest_in_progress.is_none() {
          let mut rr = crate::message::builder::Message::new(
            msg.fix_version.clone(),
            "2",
          )?;
          rr.body.set_tag(
            crate::schema::FIX_Latest::Fields::BeginSeqNo,
            self.session.next_in_seq_num,
          );
          rr.body
            .set_tag(crate::schema::FIX_Latest::Fields::EndSeqNo, 0);
          self.rerequest_in_progress = Some(msg_seq_num);
          self
            .session
            .send(
              rr,
              &self.session_id,
              &mut self.tx,
              &mut self.session_event_sender,
            )
            .await?;
        }

        return Ok(true);
      }
      std::cmp::Ordering::Less => {
        // One of the two peers has lost session state and the connection is no
        // longer recoverable.
        self
          .send_logout(format!(
            "Invalid MsgSeqNum; too low. Expected {} but got {}.",
            self.session.next_in_seq_num, msg_seq_num
          ))
          .await?;
        return Ok(false);
      }
    }

    if self
      .rerequest_in_progress
      .is_some_and(|replay_seq| self.session.next_in_seq_num >= replay_seq)
    {
      self.rerequest_in_progress = None;
    }

    // TODO: Validate message matches session_id
    // The dispatch result decides whether the session continues: an inbound
    // Logout ends it once acknowledged.
    self.dispatch_message(msg).await
  }

  /// Read `NewSeqNo` from an inbound SequenceReset(35=4).
  ///
  /// Only the gap fill form is supported. A SequenceReset-Reset — GapFillFlag
  /// of "N", or absent, which the specification defines as the default — asks
  /// the peer to accept a new sequence number without regard to the message's
  /// own, and is rejected.
  fn gap_fill_new_seq_num(
    msg: &crate::message::builder::Message,
  ) -> crate::Result<u32> {
    if msg
      .body
      .tag(crate::schema::FIX_Latest::Fields::GapFillFlag)
      .ok_or_else(|| crate::Error::protocol_violation("Missing GapFillFlag"))?
      .as_string()
      != "Y"
    {
      return Err(crate::Error::protocol_violation(
        "Sequence reset message is garbage and not supported",
      ));
    }

    Ok(
      msg
        .body
        .tag(crate::schema::FIX_Latest::Fields::NewSeqNo)
        .ok_or_else(|| crate::Error::protocol_violation("Missing NewSeqNo"))?
        .as_int()
        .ok_or_else(|| crate::Error::protocol_violation("Expected integer"))?
        as u32,
    )
  }

  /// Send a Logout(35=5) carrying a diagnostic reason.
  async fn send_logout(
    &mut self,
    text: impl Into<crate::message::builder::TypedValue>,
  ) -> crate::Result<()> {
    let mut logout = crate::message::builder::Message::new(
      self.session.fix_version.clone(),
      "5",
    )?;
    logout
      .body
      .set_tag(crate::schema::FIX_Latest::Fields::Text, text);
    self
      .session
      .send(
        logout,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await
  }

  async fn dispatch_message(
    &mut self,
    msg: crate::message::builder::Message,
  ) -> crate::Result<bool> {
    self
      .session_event_sender
      .send(SessionEvent::SessionState(self.session.clone()))
      .await?;
    match msg.fix_message.msg_type.as_str() {
      "A" => {
        // we already mostly handled this
      }
      // Heartbeat
      "0" => {
        if let Some(test_req_id) = msg
          .body
          .tag(crate::schema::FIX_Latest::Fields::TestReqID)
          .map(|v| v.as_string())
        {
          if let Some(recovery_tr_id) = &self.recovery_tr_id {
            if &test_req_id == recovery_tr_id {
              debug!(
                "Received heartbeat for recovery test request, session is now established"
              );
              self.recovery_tr_id = None;
              self
                .session_event_sender
                .send(SessionEvent::RecoveryCompleted)
                .await?;
            }
          }
        }
      }
      // TestRequest
      "1" => {
        let mut heartbeat =
          crate::message::builder::Message::new(msg.fix_version, "0")?;
        heartbeat.body.set_tag(
          crate::schema::FIX_Latest::Fields::TestReqID,
          msg
            .body
            .tag(crate::schema::FIX_Latest::Fields::TestReqID)
            .ok_or_else(|| {
              crate::Error::protocol_violation("Missing TestReqID")
            })?
            .as_string(),
        );
        self.send(heartbeat).await?;
      }
      // ResendRequest
      "2" => {
        let begin_seq_no = msg
          .body
          .tag(crate::schema::FIX_Latest::Fields::BeginSeqNo)
          .ok_or_else(|| {
            crate::Error::protocol_violation("Missing BeginSeqNo")
          })?
          .as_int()
          .ok_or_else(|| crate::Error::protocol_violation("Expected integer"))?
          as u32;
        let end_seq_no = msg
          .body
          .tag(crate::schema::FIX_Latest::Fields::EndSeqNo)
          .ok_or_else(|| crate::Error::protocol_violation("Missing EndSeqNo"))?
          .as_int()
          .ok_or_else(|| crate::Error::protocol_violation("Expected integer"))?
          as u32;
        if end_seq_no > 0 && begin_seq_no > end_seq_no {
          return Err(crate::Error::protocol_violation(
            "Invalid ResendRequest",
          ));
        }
        if self.replay.is_some() {
          return Err(crate::Error::protocol_violation(
            "ResendRequest while a resend is already in progress",
          ));
        }
        self.replay = Some(Replay {
          begin_seq_no,
          // EndSeqNo of 0 means "everything since BeginSeqNo", which is
          // everything we have sent so far.
          end_seq_no: if end_seq_no > 0 {
            end_seq_no
          } else {
            self.session.next_out_seq_num.saturating_sub(1)
          },
          next_expected_seq_num: begin_seq_no,
          gap_fill_count: 0,
          queue: Vec::new(),
        });
        self
          .session_event_sender
          .send(SessionEvent::ResendRequest {
            resend_request: msg,
            begin_seq_no: self.replay.as_ref().unwrap().begin_seq_no,
            end_seq_no: self.replay.as_ref().unwrap().end_seq_no,
          })
          .await?
      }
      // Logout
      "5" => {
        let mut logout =
          crate::message::builder::Message::new(msg.fix_version.clone(), "5")?;
        logout.body.set_tag(
          crate::schema::FIX_Latest::Fields::Text,
          "Logout message received. Closing session.",
        );
        self
          .session
          .send(
            logout,
            &self.session_id,
            &mut self.tx,
            &mut self.session_event_sender,
          )
          .await?;
        return Ok(false);
      }
      _ if msg.is_admin_message() => {
        // Ignore other admin messages
      }
      &_ => {
        self
          .session_event_sender
          .send(SessionEvent::MessageReceived(msg))
          .await?;
      }
    }
    Ok(true)
  }

  /// Skip over `begin_seq_no..=end_seq_no` with a single SequenceReset-GapFill.
  ///
  /// Both bounds are inclusive and name messages that will *not* be
  /// retransmitted. `NewSeqNo` is therefore `end_seq_no + 1`: the sequence
  /// number of the next message the peer should expect, which must be the one
  /// transmitted immediately after this gap fill.
  async fn send_gap_fill(
    &mut self,
    begin_seq_no: u32,
    end_seq_no: u32,
  ) -> crate::Result<()> {
    let mut gap_fill = crate::message::builder::Message::new(
      self.session.fix_version.clone(),
      "4",
    )
    .unwrap();
    gap_fill
      .body
      .set_tag(crate::schema::FIX_Latest::Fields::GapFillFlag, "Y");
    gap_fill
      .header
      .set_tag(crate::schema::FIX_Latest::Fields::MsgSeqNum, begin_seq_no);
    gap_fill
      .body
      .set_tag(crate::schema::FIX_Latest::Fields::NewSeqNo, end_seq_no + 1);
    self
      .session
      .send(
        gap_fill,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await
  }

  async fn send(
    &mut self,
    mut msg: crate::message::builder::Message,
  ) -> crate::Result<()> {
    if let Some(replay) = self.replay.as_mut() {
      replay.queue.push(msg);
      return Ok(());
    }

    msg
      .header
      .remove_tag(crate::schema::FIX_Latest::Fields::MsgSeqNum);
    msg
      .header
      .remove_tag(crate::schema::FIX_Latest::Fields::PossDupFlag);

    debug!("Sending message to session: {:?}", msg);
    self
      .session
      .send(
        msg,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await
  }

  async fn replay(
    &mut self,
    mut message: crate::message::builder::Message,
  ) -> crate::Result<()> {
    // Replay logic:
    //
    // Find consecutive runs of admin messages, and send gap fill for that range
    // For any non-admin message, send the message as-is with PossDupFlag=Y, and
    // new SendingTime, copying SendingTime to OrigSendingTime If we run out of
    // records, send a gap fill to the end_seq_no. (though strictly speaking we
    // should just abort at this point)
    //

    let replay = self.replay.as_mut().ok_or_else(|| {
      crate::Error::protocol_violation("No replay in progress")
    })?;
    let msg_seq_num = message
      .header
      .tag(crate::schema::FIX_Latest::Fields::MsgSeqNum)
      .ok_or_else(|| {
        crate::Error::protocol_violation("No MsgSeqNum in replay message")
      })?
      .as_int()
      .ok_or_else(|| {
        crate::Error::protocol_violation("MsgSeqNum is not an integer")
      })? as u32;
    if msg_seq_num < replay.next_expected_seq_num {
      // Already processed
      return Ok(());
    }
    if msg_seq_num > replay.end_seq_no {
      // Beyond what the peer asked for. Retransmitting it would put a sequence
      // number on the wire that we are about to reuse for a new message.
      return Ok(());
    }
    replay.gap_fill_count += msg_seq_num - replay.next_expected_seq_num;
    replay.next_expected_seq_num = msg_seq_num + 1;
    if message.is_admin_message() {
      replay.gap_fill_count += 1;
      return Ok(());
    }
    // Non-admin message: close off the run of skipped messages preceding it
    // before sending it. The gap fill must stop at msg_seq_num - 1, because
    // msg_seq_num itself is about to be retransmitted.
    if replay.gap_fill_count > 0 {
      let first_skipped =
        replay.next_expected_seq_num - replay.gap_fill_count - 1;
      self.send_gap_fill(first_skipped, msg_seq_num - 1).await?;
    }

    // re-acquire our ref to the replay after send_gap_fill borrowed self
    let replay = self.replay.as_mut().ok_or_else(|| {
      crate::Error::protocol_violation("No replay in progress")
    })?;
    replay.gap_fill_count = 0;
    // Send the message as a PossDup
    use crate::schema::FIX_Latest::Fields;
    message.header.set_tag(
      Fields::OrigSendingTime,
      message
        .header
        .tag(Fields::SendingTime)
        .ok_or_else(|| crate::Error::protocol_violation("Missing SendingTime"))?
        .clone(),
    );
    message.header.set_tag(Fields::PossDupFlag, "Y");
    self
      .session
      .send(
        message,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await
  }

  async fn complete_replay(&mut self) -> crate::Result<()> {
    let replay = self.replay.as_mut().ok_or_else(|| {
      crate::Error::protocol_violation("No replay in progress")
    })?;
    let queue = std::mem::take(&mut replay.queue);
    if (replay.next_expected_seq_num - replay.gap_fill_count)
      <= replay.end_seq_no
    {
      let begin_seq_no = replay.next_expected_seq_num - replay.gap_fill_count;
      let end_seq_no = replay.end_seq_no;
      self.send_gap_fill(begin_seq_no, end_seq_no).await?;
    }
    self.replay = None;
    for msg in queue {
      self.send(msg).await?;
    }

    let tr_id = format!(
      "HELO-{}",
      chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(chrono::Utc::now().timestamp_millis() * 1_000_000)
    );
    let mut test_request = crate::message::builder::Message::new(
      self.session.fix_version.clone(),
      "1",
    )?;
    test_request
      .body
      .set_tag(crate::schema::FIX_Latest::Fields::TestReqID, tr_id.clone());
    self
      .session
      .send(
        test_request,
        &self.session_id,
        &mut self.tx,
        &mut self.session_event_sender,
      )
      .await?;
    self.recovery_tr_id = Some(tr_id);

    Ok(())
  }
}
