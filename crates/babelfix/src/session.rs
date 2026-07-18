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
//! ResendRequest on a sequence gap, queues out-of-order messages, and handles
//! logout — so application code usually only needs to react to
//! [`SessionEvent::MessageReceived`] (inbound *application* messages) and
//! [`SessionEvent::Disconnected`].
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

#[derive(Debug, Clone, Default)]
pub struct Session {
  pub next_out_seq_num: u32,
  pub next_in_seq_num: u32,
  pub heartbeat_interval: std::time::Duration,
  pub fix_version: Arc<crate::repository::FixVersion>,
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
pub enum SessionCommand {
  Send(crate::message::builder::Message),
  Replay(crate::message::builder::Message),
  ReplayComplete,
  Disconnect,
  GetSessionState(oneshot::Sender<Session>),
}

#[derive(Debug)]
pub enum SessionEvent {
  ConnectionEsablished,
  RecoveryStarted,
  RecoveryCompleted,
  SessionState(Session),
  RawMessageReceived(crate::message::FixMessage, Session),
  RawMessageSent(crate::message::FixMessage, Session),
  MessageReceived(crate::message::builder::Message),
  ResendRequest(crate::message::builder::Message, u32, u32),
  Disconnected,
  Error(crate::Error),
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
}

pub(crate) struct SessionManager<'stream, E, D> {
  rx: tokio_util::codec::FramedRead<tokio::net::tcp::ReadHalf<'stream>, D>,
  tx: tokio_util::codec::FramedWrite<tokio::net::tcp::WriteHalf<'stream>, E>,
  session_id: SessionIdentifier,
  session: Session,
  session_event_sender: mpsc::Sender<SessionEvent>,
  session_msg_recv: mpsc::Receiver<SessionCommand>,
  queued_messages: Vec<crate::message::builder::Message>,
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
      queued_messages: Vec::new(),
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
          self.session.send(
            heartbeat,
            &self.session_id,
            &mut self.tx,
            &mut self.session_event_sender,
            ).await?;
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
              self.session.send(
                test_request,
                &self.session_id,
                &mut self.tx,
                &mut self.session_event_sender,
                ).await?;
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
              self.session.send(
                logout,
                &self.session_id,
                &mut self.tx,
                &mut self.session_event_sender,
                ).await?;
              break;
            }
            _ => { unreachable!() }
          }
        }
        cmd = self.session_msg_recv.next() => {
          out_heartbeat_timer.reset();
          if let Some(cmd) = cmd {
            match cmd {
              SessionCommand::Send(msg) => {
                if self.replay.is_some() {
                  error!("Cannot send message while replay is in progress; use Resend");
                  break;
                }
                debug!("Sending message to session: {:?}", msg);
                if let Err(e) = self.session.send(msg,
                                                  &self.session_id,
                                                  &mut self.tx,
                                                  &mut self.session_event_sender).await {
                  error!("Failed to send message: {e}");
                  break;
                }
              }
              SessionCommand::Replay(msg) => {
                self.replay(msg).await?;
              }
              SessionCommand::ReplayComplete => {
                self.complete_replay().await?;
              }
              SessionCommand::Disconnect => {
                info!("Session disconnect requested");
                // TODO: Send logout...
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

    if msg_seq_num == self.session.next_in_seq_num {
      if msg.fix_message.msg_type == "4" {
        // gap fill
        if msg
          .body
          .tag(crate::schema::FIX_Latest::Fields::GapFillFlag)
          .ok_or_else(|| {
            crate::Error::protocol_violation("Missing GapFillFlag")
          })?
          .as_string()
          != "Y"
        {
          return Err(crate::Error::protocol_violation(
            "Sequence reset message is garbage and not supported",
          ));
        }

        let new_seq_num = msg
          .body
          .tag(crate::schema::FIX_Latest::Fields::NewSeqNo)
          .ok_or_else(|| crate::Error::protocol_violation("Missing NewSeqNo"))?
          .as_int()
          .ok_or_else(|| crate::Error::protocol_violation("Expected integer"))?
          as u32;
        if new_seq_num <= self.session.next_in_seq_num {
          return Err(crate::Error::protocol_violation(format!(
            "Invalid NewSeqNo in GapFill; expected greater than {} but got {}",
            self.session.next_in_seq_num, new_seq_num
          )));
        }
        self.session.next_in_seq_num = new_seq_num;
      } else {
        self.session.next_in_seq_num += 1;
      }
    } else if msg_seq_num > self.session.next_in_seq_num {
      if self.queued_messages.is_empty() {
        // start resend request
        let mut rr =
          crate::message::builder::Message::new(msg.fix_version.clone(), "2")?;
        rr.body.set_tag(
          crate::schema::FIX_Latest::Fields::BeginSeqNo,
          self.session.next_in_seq_num,
        );
        rr.body
          .set_tag(crate::schema::FIX_Latest::Fields::EndSeqNo, "0");
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
      debug!(
        "Queueing out-of-order message {:?} with MsgSeqNum {}, next expected: {}",
        msg, msg_seq_num, self.session.next_in_seq_num
      );
      self.queued_messages.push(msg);
      return Ok(true);
    } else if msg_seq_num < self.session.next_in_seq_num {
      // Invalid; send a logout message

      let mut logout =
        crate::message::builder::Message::new(msg.fix_version.clone(), "5")?;
      logout.body.set_tag(
        crate::schema::FIX_Latest::Fields::Text,
        format!(
          "Invalid MsgSeqNum; too low. Expected {} but got {}.",
          self.session.next_in_seq_num, msg_seq_num
        ),
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

    // TODO: Validate message matches session_id
    self.dispatch_message(msg).await?;

    let drain = if let Some(first_queued) = self.queued_messages.first() {
      let first_queued_seq_num = first_queued
        .header
        .tag(crate::schema::FIX_Latest::Fields::MsgSeqNum)
        .ok_or_else(|| crate::Error::protocol_violation("Missing MsgSeqNum"))?
        .as_int()
        .ok_or_else(|| crate::Error::protocol_violation("Expected integer"))?
        as u32;
      debug!(
        "First queued message has MsgSeqNum {}, next expected is {}",
        first_queued_seq_num, self.session.next_in_seq_num
      );

      first_queued_seq_num <= self.session.next_in_seq_num
    } else {
      false
    };

    if drain {
      let queued = std::mem::take(&mut self.queued_messages);
      for msg in queued {
        if !self.dispatch_message(msg).await? {
          return Ok(false);
        }
      }
    }

    Ok(true)
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
      "0" => {}
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
        self
          .session
          .send(
            heartbeat,
            &self.session_id,
            &mut self.tx,
            &mut self.session_event_sender,
          )
          .await?;
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
          end_seq_no: if end_seq_no > 0 {
            end_seq_no
          } else {
            self.session.next_out_seq_num - 1
          },
          next_expected_seq_num: begin_seq_no,
          gap_fill_count: 0,
        });
        self
          .session_event_sender
          .send(SessionEvent::ResendRequest(
            msg,
            self.replay.as_ref().unwrap().begin_seq_no,
            self.replay.as_ref().unwrap().end_seq_no,
          ))
          .await?
      }
      // Logout
      "5" => {
        let mut logout =
          crate::message::builder::Message::new(msg.fix_version, "5")?;
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
      &_ => {
        self
          .session_event_sender
          .send(SessionEvent::MessageReceived(msg))
          .await?;
      }
    }
    Ok(true)
  }

  async fn send_gap_fill(
    &mut self,
    begin_seq_no: u32,
    end_seq_no: u32,
  ) -> crate::Result<()> {
    // Gap fill
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
      .set_tag(crate::schema::FIX_Latest::Fields::NewSeqNo, end_seq_no);
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
    replay.gap_fill_count += msg_seq_num - replay.next_expected_seq_num;
    replay.next_expected_seq_num = msg_seq_num + 1;
    if message.is_admin_message() {
      replay.gap_fill_count += 1;
      return Ok(());
    }
    // Non-admin message, send any gap fill first
    if replay.gap_fill_count > 0 {
      let end_seq_no = replay.next_expected_seq_num - replay.gap_fill_count - 1;
      self.send_gap_fill(end_seq_no, msg_seq_num).await?;
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
    replay.gap_fill_count +=
      replay.end_seq_no + 1 - replay.next_expected_seq_num;
    if replay.gap_fill_count > 0 {
      let begin_seq_no = replay.next_expected_seq_num - replay.gap_fill_count;
      let end_seq_no = replay.next_expected_seq_num;
      self.send_gap_fill(begin_seq_no, end_seq_no).await?;
    }
    self.replay = None;
    Ok(())
  }
}
