//! FIX session layer: sequence numbers, heartbeats and message recovery.
//!
//! The protocol itself lives in [`babelfix_core::session`] as a sans-io state
//! machine. This module is the tokio driver for it: it owns the socket, the
//! clock and the channels, feeds the state machine, and flushes whatever it
//! produces.
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
//! ```no_run
//! use babelfix_tokio::session::{SessionHandle, SessionEvent};
//! use babelfix_tokio::schema::FIX_Latest::Fields;
//! use futures::StreamExt;
//!
//! async fn drive(mut handle: SessionHandle) {
//!     while let Some(event) = handle.events.next().await {
//!         match event {
//!             SessionEvent::MessageReceived(msg) => {
//!                 if let Some(t) = msg.body.tag(Fields::ClOrdID) {
//!                     println!("received order {}", t.as_string());
//!                 }
//!             }
//!             SessionEvent::Disconnected => break,
//!             // ...
//!             _ => {}
//!         }
//!     }
//! }
//! ```

use std::collections::VecDeque;

use bytes::BytesMut;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use babelfix_core::codec::FixEncoder;

/// The sans-io protocol, re-exported so `babelfix::session` is one place.
///
/// [`SessionState`] is what this module drives; you only need it directly if
/// you are writing a driver of your own, in which case `babelfix-core` is
/// probably the dependency you want.
pub use babelfix_core::session::{
  Command, Event, Progress, Replay, Session, SessionIdentifier, SessionOutput,
  SessionState,
};

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
  ///
  /// This exists because the session runs in its own task. It is answered by
  /// the driver without troubling the state machine, so — unlike the commands
  /// above — it puts nothing on the wire and does not defer the next heartbeat.
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
  /// FIXME: Should include the socket receive time
  RawMessageReceived(crate::message::FixMessage, Session),

  /// Emitted when any FIX message was sent to the remote, including admin
  /// messages. Useful for auditing, logging and display. `SendingTime` is the
  /// value that went on the wire.
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
}

impl SessionEvent {
  /// Take an owned copy of a borrowed core event.
  ///
  /// This is where the async tier pays for its convenience: the state machine
  /// hands out borrows, and turning them into owned events for delivery over a
  /// channel means cloning. A driver that implements
  /// [`SessionOutput`] directly pays none of it.
  fn from_core(event: Event<'_>) -> Option<Self> {
    Some(match event {
      Event::ConnectionEstablished => SessionEvent::ConnectionEstablished,
      Event::RecoveryCompleted => SessionEvent::RecoveryCompleted,
      Event::SessionState(s) => SessionEvent::SessionState(s.clone()),
      Event::RawMessageReceived(m, s) => {
        SessionEvent::RawMessageReceived(m.clone(), s.clone())
      }
      Event::RawMessageSent(m, s) => {
        SessionEvent::RawMessageSent(m.clone(), s.clone())
      }
      Event::MessageReceived(m) => SessionEvent::MessageReceived(m.clone()),
      Event::ResendRequest {
        resend_request,
        begin_seq_no,
        end_seq_no,
      } => SessionEvent::ResendRequest {
        resend_request: resend_request.clone(),
        begin_seq_no,
        end_seq_no,
      },
      Event::Disconnected => SessionEvent::Disconnected,
      // `Event` is `#[non_exhaustive]`; a variant this driver does not know
      // about is not worth crashing a live session over.
      _ => return None,
    })
  }
}

#[derive(Debug)]
pub struct SessionHandle {
  pub session_id: SessionIdentifier,
  pub tx: mpsc::Sender<SessionCommand>,
  // FIXME: Remove this from the struct so that you can cheaply clone it
  pub events: mpsc::Receiver<SessionEvent>,
}

/// Collects what the state machine produces during one synchronous pass, so the
/// driver can flush it afterwards.
///
/// The two queues exist because [`SessionOutput`] is synchronous and the socket
/// and the event channel are not. They are drained after every input, which is
/// what keeps the peer's backpressure connected to the application's: a slow
/// consumer blocks the flush, which stops the loop reading the socket.
pub(crate) struct PendingOutput {
  encoder: FixEncoder,
  /// Encoded bytes waiting to go to the socket.
  bytes: BytesMut,
  /// Events waiting to go to the application.
  events: VecDeque<SessionEvent>,
}

impl PendingOutput {
  fn new(delimiter: Option<u8>, precision: crate::time::TimePrecision) -> Self {
    Self {
      encoder: FixEncoder::new(delimiter).with_precision(precision),
      bytes: BytesMut::with_capacity(4096),
      events: VecDeque::new(),
    }
  }
}

impl SessionOutput for PendingOutput {
  fn transmit(
    &mut self,
    msg: &mut crate::message::FixMessage,
    _session: &Session,
  ) -> crate::Result<()> {
    // The clock is read here, once per message, immediately before the bytes
    // are produced — the latest point the sans-io boundary allows.
    self
      .encoder
      .encode_stamped(msg, chrono::Utc::now(), &mut self.bytes)
  }

  fn event(&mut self, event: Event<'_>) -> crate::Result<()> {
    if let Some(event) = SessionEvent::from_core(event) {
      self.events.push_back(event);
    }
    Ok(())
  }
}

/// Drives a [`SessionState`] over a tokio socket.
pub(crate) struct SessionRunner<W> {
  state: SessionState,
  out: PendingOutput,
  writer: W,
  event_sender: mpsc::Sender<SessionEvent>,
}

impl<W> SessionRunner<W> {
  pub(crate) fn state(&mut self) -> &mut SessionState {
    &mut self.state
  }
}

impl<W: tokio::io::AsyncWrite + Unpin> SessionRunner<W> {
  pub(crate) fn new(
    state: SessionState,
    writer: W,
    event_sender: mpsc::Sender<SessionEvent>,
    delimiter: Option<u8>,
  ) -> Self {
    let precision = state.session().time_precision;
    Self {
      state,
      out: PendingOutput::new(delimiter, precision),
      writer,
      event_sender,
    }
  }

  /// Deliver everything the last pass produced.
  ///
  /// Events go first, then bytes: the application must see
  /// [`SessionEvent::RawMessageSent`] before the peer could possibly have seen
  /// the message, which is the order the pre-sans-io session guaranteed.
  pub(crate) async fn flush(&mut self) -> crate::Result<()> {
    while let Some(event) = self.out.events.pop_front() {
      self
        .event_sender
        .send(event)
        .await
        .map_err(crate::chan_closed)?;
    }

    if !self.out.bytes.is_empty() {
      self.writer.write_all(&self.out.bytes).await?;
      self.writer.flush().await?;
      self.out.bytes.clear();
    }

    Ok(())
  }

  /// Emit a lifecycle event that comes from the driver rather than the protocol.
  pub(crate) async fn emit(
    &mut self,
    event: SessionEvent,
  ) -> crate::Result<()> {
    self
      .event_sender
      .send(event)
      .await
      .map_err(crate::chan_closed)
  }

  /// Send a message belonging to the logon exchange, which the driver still
  /// owns. Everything else goes through the state machine.
  pub(crate) async fn send_logon(
    &mut self,
    msg: crate::message::builder::Message,
  ) -> crate::Result<()> {
    self.state.send_logon(msg, &mut self.out)?;
    self.flush().await
  }

  /// Run the session until it ends.
  pub(crate) async fn run<R>(
    &mut self,
    rx: &mut R,
    commands: &mut mpsc::Receiver<SessionCommand>,
    logon: crate::message::builder::Message,
  ) -> crate::Result<()>
  where
    R: Stream<Item = crate::Result<crate::message::FixMessage>> + Unpin,
  {
    let progress =
      self
        .state
        .start(logon, std::time::Instant::now(), &mut self.out)?;
    self.flush().await?;
    if progress.is_close() {
      return Ok(());
    }

    loop {
      // `next_deadline` is never `None` for a live session, but a far-future
      // fallback keeps the select! arm well-formed either way.
      let deadline = self
        .state
        .next_deadline()
        .unwrap_or_else(|| std::time::Instant::now() + FAR_FUTURE);

      let progress = tokio::select! {
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
          self.state.on_timeout(std::time::Instant::now(), &mut self.out)?
        }
        cmd = commands.next() => {
          match cmd {
            Some(SessionCommand::GetSessionState(resp)) => {
              // Answered from here rather than the state machine: it transmits
              // nothing, so it must not defer the outbound heartbeat. An
              // application polling its own session used to be able to silence
              // its heartbeats entirely and be declared dead by the peer.
              let _ = resp.send(self.state.session().clone());
              Progress::Continue
            }
            Some(cmd) => {
              let cmd = match cmd {
                SessionCommand::Send(m) => Command::Send(m),
                SessionCommand::Replay(m) => Command::Replay(m),
                SessionCommand::ReplayComplete => Command::ReplayComplete,
                SessionCommand::Disconnect => Command::Disconnect,
                SessionCommand::GetSessionState(_) => unreachable!("handled above"),
              };
              self.state.on_command(cmd, std::time::Instant::now(), &mut self.out)?
            }
            None => {
              debug!("Session message channel closed, stopping session manager");
              break;
            }
          }
        }
        frame = rx.next() => {
          match frame {
            Some(Ok(fix_message)) => {
              self.state.on_message(fix_message, std::time::Instant::now(), &mut self.out)?
            }
            Some(Err(e)) => {
              tracing::error!("Error reading from socket: {e}");
              break;
            }
            None => self.state.on_peer_closed(&mut self.out)?,
          }
        }
      };

      // Everything the pass produced reaches the peer and the application
      // before the next input is read. See `PendingOutput`.
      self.flush().await?;

      if progress.is_close() {
        break;
      }
    }

    Ok(())
  }
}

/// Long enough to mean "no deadline" without risking an `Instant` overflow.
const FAR_FUTURE: std::time::Duration = std::time::Duration::from_secs(86_400);
