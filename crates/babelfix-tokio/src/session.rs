//! FIX session layer: sequence numbers, heartbeats and message recovery.
//!
//! The protocol itself lives in [`babelfix_core::session`] as a sans-io state
//! machine, driven by the core's [`driver`](crate::driver) types and numbered
//! by a [`Sequencer`]. This module is the tokio driver for them: it owns the
//! socket, the clock and the channels, runs the writes the sequencer asks for
//! against a [`SessionStore`], and flushes whatever the session produces.
//!
//! A session is described by a [`SessionSetup`]: its [`SessionConfig`], where
//! it [`Resume`]s, and the [`SessionStore`] each outbound message is persisted
//! to before it is sent. The [`crate::endpoint`] layer runs the session loop
//! internally; applications interact with a live session through a
//! [`SessionHandle`]:
//!
//! * `tx: mpsc::Sender<`[`SessionCommand`]`>` — send an application message with
//!   [`SessionCommand::Send`], drive a resend with [`SessionCommand::Replay`] /
//!   [`SessionCommand::ReplayComplete`], or [`SessionCommand::Disconnect`].
//! * `events: mpsc::Receiver<`[`SessionEvent`]`>` — a stream of session lifecycle
//!   and inbound-message events.
//!
//! The session automatically numbers, persists and sends heartbeats, answers
//! TestRequests, issues a ResendRequest on a sequence gap, and handles logout —
//! so application code technically only needs to react to
//! [`SessionEvent::MessageReceived`] (inbound *application* messages),
//! [`SessionEvent::ResendRequest`] (inbound *resend requests*, answered from
//! the store), and [`SessionEvent::Disconnected`].
//!
//! ```no_run
//! use babelfix_tokio::session::{SessionHandle, SessionEvent};
//! use babelfix_schema::fields::ClOrdID;
//! use futures::StreamExt;
//!
//! async fn drive(mut handle: SessionHandle) {
//!     while let Some(event) = handle.events.next().await {
//!         match event {
//!             SessionEvent::MessageReceived { msg, .. } => {
//!                 if let Ok(Some(id)) = msg.body().get(ClOrdID) {
//!                     println!("received order {id}");
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
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, error};

use babelfix_core::driver::SessionDriver;
use babelfix_core::sequencer::Sequencer;

use crate::store::{Sequenced, SessionStore, VolatileStore};

/// The sans-io protocol, re-exported so `babelfix::session` is one place.
///
/// [`SessionState`] is what the drivers wrap; you only need it directly if you
/// are writing a driver of your own, in which case `babelfix-core` is probably
/// the dependency you want.
pub use babelfix_core::session::{
  Command, Event, EventSink, Progress, Replay, SessionConfig,
  SessionIdentifier, SessionOutput, SessionState, Unstamped,
};

/// How a sequenced session follows the inbound messages it delivers.
pub use babelfix_core::sequencer::{InboundPolicy, Resume, WatermarkMode};

/// Everything a session needs to start: its settings, where it resumes, and
/// where its writes go.
#[derive(Clone)]
pub struct SessionSetup {
  pub config: SessionConfig,
  /// What was persisted last time: one past the highest outbound sequence
  /// number stored, and the inbound watermark.
  pub resume: Resume,
  /// When an inbound message counts as handled.
  pub inbound: InboundPolicy,
  /// Where outbound messages and the inbound watermark are persisted before
  /// anything depending on them happens.
  pub store: Arc<dyn SessionStore>,
}

impl SessionSetup {
  /// A session starting from scratch, persisting nothing.
  pub fn new(config: SessionConfig) -> Self {
    Self {
      config,
      resume: Resume::new(),
      inbound: InboundPolicy::OnDelivery,
      store: Arc::new(VolatileStore),
    }
  }

  pub fn resume(mut self, resume: Resume) -> Self {
    self.resume = resume;
    self
  }

  pub fn inbound(mut self, inbound: InboundPolicy) -> Self {
    self.inbound = inbound;
    self
  }

  pub fn store(mut self, store: Arc<dyn SessionStore>) -> Self {
    self.store = store;
    self
  }

  pub(crate) fn sequenced(&self, session_id: &SessionIdentifier) -> Sequenced {
    Sequenced::new(
      Sequencer::new(
        session_id,
        &self.config,
        self.resume,
        self.inbound,
        wall_clock,
      ),
      self.store.clone(),
    )
  }
}

impl std::fmt::Debug for SessionSetup {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SessionSetup")
      .field("config", &self.config)
      .field("resume", &self.resume)
      .field("inbound", &self.inbound)
      .finish_non_exhaustive()
  }
}

/// The wall clock the tokio driver stamps `SendingTime` from.
pub(crate) fn wall_clock() -> chrono::DateTime<chrono::Utc> {
  chrono::Utc::now()
}

#[derive(Debug)]
#[non_exhaustive]
pub enum SessionCommand {
  /// Send an application message. It is given the next outbound sequence
  /// number and a `SendingTime` (unless it has one), persisted to the
  /// session's [`SessionStore`], and sent once the write has completed.
  ///
  /// A message sent while a replay is in progress goes out after
  /// [`SessionCommand::ReplayComplete`]. To answer a
  /// [`SessionEvent::ResendRequest`], use [`SessionCommand::Replay`].
  Send(crate::message::Message),

  /// Persist and send a message under the `MsgSeqNum` it carries, with its
  /// header otherwise as built — `PossDupFlag` and all. For test tools. The
  /// number may jump forward, but not back.
  SendRaw(crate::message::Message),

  /// The application has finished with the inbound message `seq_num`, under
  /// [`InboundPolicy::Explicit`].
  Handled(u64),

  /// Replay the sequence number in `MsgSeqNum` with the supplied message. Only
  /// valid between receipt of a [`SessionEvent::ResendRequest`] and a
  /// subsequent [`SessionCommand::ReplayComplete`].
  ///
  /// For more details on resends, see [`SessionEvent::ResendRequest`].
  Replay(crate::message::Message),

  /// Indicate that all messages for the current resend request have been sent.
  ///
  /// For more details on resends, see [`SessionEvent::ResendRequest`].
  ReplayComplete,

  /// Disconnect the session. The completion of the disconnection will be
  /// indicated by a [`SessionEvent::Disconnected`] event.
  Disconnect,

  /// Request the current sequence numbers of the session.
  ///
  /// This exists because the session runs in its own task. It is answered by
  /// the driver without troubling the state machine, so it puts nothing on the
  /// wire and does not defer the next heartbeat.
  GetSessionState(oneshot::Sender<SessionStatus>),
}

/// Where a session's sequence numbers stand, for display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionStatus {
  /// The sequence number the next outbound message will be given.
  pub next_out_seq_num: u64,
  /// The sequence number expected on the next inbound message.
  pub next_in_seq_num: u64,
  /// The inbound sequence number a later connection would resume from.
  pub watermark: u64,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SessionEvent {
  /// A connection has been established to the peer, but no logon exchange has
  /// been performed yet.
  ConnectionEstablished,

  /// Both Logons have been exchanged.
  LoggedOn,

  /// Local recovery has completed - the remote has sent all messages missed on
  /// the session. Note that this does not mean the remote has received, or even
  /// requested, any missing messages from us.
  RecoveryCompleted,

  /// Emitted when any FIX message was received from the remote, no matter whether
  /// this is valid or not. Includes admin messages (logon, logout, resend
  /// request, etc.). Useful for auditing, logging and display; nothing in
  /// recovery depends on it. Business logic and processing should not use
  /// this: rather use the [`SessionEvent::MessageReceived`] event, which is
  /// emitted for valid, well-sequenced messages.
  ///
  /// The time is when the message's bytes were read from the socket. It is
  /// read before anything the message provokes is numbered or persisted, so
  /// a journal of both directions can order by it: a message received is
  /// never later than a reply to it. (The time the event happens to be
  /// handled is no use for that: an outbound message reaches the
  /// [`SessionStore`] from the session task, before the application has seen
  /// the event that provoked it.)
  RawMessageReceived(crate::message::Message, chrono::DateTime<chrono::Utc>),

  /// Emitted when any FIX message was sent to the remote, including admin
  /// messages, as it went on the wire. Useful for auditing, logging and
  /// display; nothing in recovery depends on it — outbound messages reach the
  /// [`SessionStore`] before they are sent.
  ///
  /// FIXME: Should include the socket send time.
  ///
  /// This is the message, not its bytes:
  /// [`Message::wire`](crate::message::Message::wire) is `None` for it. Encode
  /// it again for the bytes that were sent.
  RawMessageSent(crate::message::Message),

  /// Applications should use this event for business processing.
  ///
  /// Emitted for each valid, session protocol-compliant business message (and
  /// session-level Reject) in sequence number order. When a resend is required
  /// from the remote, this event will be emitted for replayed messages before
  /// any new messages, thus the application does not need to be concerned with
  /// processing out-of-sequence messages, except to the extent that
  /// `PossDupFlag` might be set on the message by the remote.
  ///
  /// Under [`InboundPolicy::Explicit`], report it handled with
  /// [`SessionCommand::Handled`]`(seq_num)`.
  MessageReceived {
    seq_num: u64,
    msg: crate::message::Message,
  },

  /// The remote requested replay (resend) of messages from the given sequence
  /// number range (inclusive of `end_seq_no`). `end_seq_no` is always supplied,
  /// even if the remote sent an open-ended resend request; this library
  /// determines the correct end sequence number in that case.
  ///
  /// Applications must action this resend request by using the
  /// [`SessionCommand::Replay`] and [`SessionCommand::ReplayComplete`] commands
  /// to send the requested message sequence.
  ///
  /// Replay the messages the [`SessionStore`] was given, or faithfully
  /// construct each equivalent with `MsgSeqNum` and its original `SendingTime`
  /// populated. The session moves `SendingTime` to `OrigSendingTime` (unless
  /// that is already set), sets `PossDupFlag=Y` and stamps a new
  /// `SendingTime`. Admin messages other than Reject and XMLnonFIX are
  /// gap-filled rather than resent. Any skipped sequence numbers will be
  /// gap-filled automatically, allowing applications to decide not to replay
  /// certain messages, for example to avoid re-sending a stale order request.
  /// Once all messages have been replayed, the application should send
  /// [`SessionCommand::ReplayComplete`]; any remaining sequence numbers will be
  /// gap-filled automatically.
  ResendRequest {
    /// The resend request message itself. Applications do not typically need to
    /// inspect this.
    resend_request: crate::message::Message,
    /// The first sequence number to be resent.
    begin_seq_no: u64,
    /// The last sequence number to be resent (inclusive).
    end_seq_no: u64,
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
  fn from_core(
    event: Event<'_>,
    received_at: chrono::DateTime<chrono::Utc>,
  ) -> Option<Self> {
    Some(match event {
      Event::ConnectionEstablished => SessionEvent::ConnectionEstablished,
      Event::LoggedOn => SessionEvent::LoggedOn,
      Event::RecoveryCompleted => SessionEvent::RecoveryCompleted,
      Event::RawMessageReceived(m) => {
        SessionEvent::RawMessageReceived(m.clone(), received_at)
      }
      Event::RawMessageSent(m) => SessionEvent::RawMessageSent(m.clone()),
      Event::MessageReceived { seq_num, msg } => {
        SessionEvent::MessageReceived {
          seq_num,
          msg: msg.clone(),
        }
      }
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
      // Admin requests are the sequencer's, and the inbound watermark is
      // followed by it; neither is the application's concern here. `Event` is
      // also `#[non_exhaustive]`, and a variant this driver does not know
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

/// Delivers a session's events to the application's channel.
///
/// Events go straight to the application where they can, and are buffered only
/// when the channel is full, which is what keeps the peer's backpressure
/// connected to the application's: a slow consumer blocks the flush, which
/// stops the loop reading the socket.
pub(crate) struct Delivery {
  /// Events the application could not take yet.
  events: VecDeque<SessionEvent>,
  event_sender: mpsc::Sender<SessionEvent>,
  /// When the bytes being processed were read from the socket.
  pub(crate) received_at: chrono::DateTime<chrono::Utc>,
}

impl Delivery {
  pub(crate) fn new(event_sender: mpsc::Sender<SessionEvent>) -> Self {
    Self {
      events: VecDeque::new(),
      event_sender,
      received_at: wall_clock(),
    }
  }

  /// Bytes have just been read from the socket.
  pub(crate) fn mark_received(&mut self) {
    self.received_at = wall_clock();
  }
}

impl EventSink for Delivery {
  fn event(&mut self, event: Event<'_>) -> crate::Result<()> {
    let Some(event) = SessionEvent::from_core(event, self.received_at) else {
      return Ok(());
    };

    // Ordering invariant: once anything is queued, everything queues until the
    // queue drains. Otherwise a `try_send` that succeeded could overtake an
    // event already waiting, and the application would see them out of order.
    if !self.events.is_empty() {
      self.events.push_back(event);
      return Ok(());
    }

    // Handed over here rather than at the flush, so the receiver is woken as
    // each event is produced — on a multi-threaded runtime another worker can
    // start on it while this pass is still running. `try_send` neither blocks
    // nor yields, and it is cheaper than the queue it replaces: one operation
    // instead of a push, a pop and a send.
    match self.event_sender.try_send(event) {
      Ok(()) => Ok(()),
      Err(e) if e.is_full() => {
        self.events.push_back(e.into_inner());
        Ok(())
      }
      Err(_) => Err(crate::Error::connection_failed("session channel closed")),
    }
  }
}

/// Drives a sequenced session over a tokio socket: a core driver, a
/// [`Sequencer`] and the writes it has in flight, and the application's event
/// channel.
///
/// The driver itself is passed in rather than owned, because a session changes
/// driver as it goes — the logon exchange, then the established session.
pub(crate) struct SessionRunner<W> {
  pub(crate) sequenced: Sequenced,
  pub(crate) delivery: Delivery,
  writer: W,
  read_buf: BytesMut,
}

/// Read buffer size. FIX messages are small; this is comfortably several.
const READ_CHUNK: usize = 8192;

impl<W: AsyncWrite + Unpin> SessionRunner<W> {
  pub(crate) fn new(
    writer: W,
    sequenced: Sequenced,
    delivery: Delivery,
  ) -> Self {
    Self {
      sequenced,
      delivery,
      writer,
      read_buf: BytesMut::with_capacity(READ_CHUNK),
    }
  }

  /// Read whatever the socket has next. `Ok(None)` when the peer has closed it.
  pub(crate) async fn read<R: AsyncRead + Unpin>(
    &mut self,
    reader: &mut R,
  ) -> crate::Result<Option<BytesMut>> {
    let n = reader.read_buf(&mut self.read_buf).await?;
    if n == 0 {
      return Ok(None);
    }
    self.delivery.mark_received();
    let bytes = std::mem::take(&mut self.read_buf);
    self.read_buf = BytesMut::with_capacity(READ_CHUNK);
    Ok(Some(bytes))
  }

  /// Deliver everything the last pass produced: events, then `bytes`.
  ///
  /// Events go first, then bytes, so the application hears about each message
  /// before the peer could have. (It cannot rely on that for anything that
  /// matters: an outbound message was persisted before it got this far.)
  ///
  /// The event queue is normally empty, because [`Delivery`] hands them over as
  /// it goes. Draining here is the backpressure path: awaiting a full channel
  /// is what stops the loop reading the socket.
  pub(crate) async fn flush(
    &mut self,
    bytes: &mut BytesMut,
  ) -> crate::Result<()> {
    while let Some(event) = self.delivery.events.pop_front() {
      self
        .delivery
        .event_sender
        .send(event)
        .await
        .map_err(crate::chan_closed)?;
    }

    if !bytes.is_empty() {
      self.writer.write_all(bytes).await?;
      self.writer.flush().await?;
      bytes.clear();
    }

    Ok(())
  }

  /// Run an established session until it ends.
  pub(crate) async fn run<R>(
    &mut self,
    driver: &mut SessionDriver,
    reader: &mut R,
    commands: &mut mpsc::Receiver<SessionCommand>,
  ) -> crate::Result<()>
  where
    R: AsyncRead + Unpin,
  {
    loop {
      // `next_deadline` is never `None` for a live session, but a far-future
      // fallback keeps the select! arm well-formed either way.
      let deadline = driver
        .next_deadline()
        .unwrap_or_else(|| Instant::now() + FAR_FUTURE);
      // A sequencer with too many writes outstanding stops taking commands,
      // which pushes back on the application through the channel.
      let accepting = !self.sequenced.seq.is_full();

      let progress = tokio::select! {
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
          driver.on_tick(Instant::now(), &mut self.sequenced.sink(&mut self.delivery))?
        }
        done = self.sequenced.writes.next() => {
          self.sequenced.complete(done, driver, &mut self.delivery)?
        }
        cmd = commands.next(), if accepting => {
          match cmd {
            Some(cmd) => self.command(cmd, driver)?,
            None => {
              debug!("Session message channel closed, stopping session manager");
              break;
            }
          }
        }
        read = reader.read_buf(&mut self.read_buf) => {
          match read {
            Ok(0) => driver.on_peer_closed(&mut self.sequenced.sink(&mut self.delivery))?,
            Ok(_) => {
              self.delivery.mark_received();
              let bytes = std::mem::take(&mut self.read_buf);
              let progress = driver.on_bytes(
                Instant::now(),
                &bytes,
                &mut self.sequenced.sink(&mut self.delivery),
              )?;
              // `on_bytes` keeps any partial frame itself, so the buffer
              // starts empty again.
              self.read_buf = BytesMut::with_capacity(READ_CHUNK);
              progress
            }
            Err(e) => {
              error!("Error reading from socket: {e}");
              break;
            }
          }
        }
      };

      // Writes a store completes at once are fed back now, so their messages
      // go out in this pass rather than the next.
      let progress = if progress.is_close() {
        progress
      } else {
        self.sequenced.complete_ready(driver, &mut self.delivery)?
      };

      // Everything the pass produced reaches the peer and the application
      // before the next input is read.
      self.flush(driver.pending_writes()).await?;

      if progress.is_close() {
        break;
      }
    }

    Ok(())
  }

  fn command(
    &mut self,
    cmd: SessionCommand,
    driver: &mut SessionDriver,
  ) -> crate::Result<Progress> {
    match cmd {
      SessionCommand::Send(msg) => {
        self.sequenced.send(msg, &mut self.delivery)?;
      }
      SessionCommand::SendRaw(msg) => {
        self.sequenced.send_raw(msg, &mut self.delivery)?;
      }
      SessionCommand::Handled(seq_num) => {
        self.sequenced.handled(seq_num, &mut self.delivery)?;
      }
      SessionCommand::GetSessionState(resp) => {
        // Answered from here rather than the state machine: it transmits
        // nothing, so it must not defer the outbound heartbeat. An application
        // polling its own session used to be able to silence its heartbeats
        // entirely and be declared dead by the peer.
        let _ = resp.send(SessionStatus {
          next_out_seq_num: self.sequenced.seq.next_out_seq_num(),
          next_in_seq_num: driver.state().next_in_seq_num(),
          watermark: self.sequenced.seq.watermark(),
        });
      }
      SessionCommand::Replay(msg) => {
        return self.on_command(Command::Replay(msg), driver);
      }
      SessionCommand::ReplayComplete => {
        return self.on_command(Command::ReplayComplete, driver);
      }
      SessionCommand::Disconnect => {
        return self.on_command(Command::Disconnect, driver);
      }
    }
    Ok(Progress::Continue)
  }

  fn on_command(
    &mut self,
    cmd: Command,
    driver: &mut SessionDriver,
  ) -> crate::Result<Progress> {
    driver.on_command(
      Instant::now(),
      cmd,
      &mut self.sequenced.sink(&mut self.delivery),
    )
  }
}

/// Long enough to mean "no deadline" without risking an `Instant` overflow.
const FAR_FUTURE: std::time::Duration = std::time::Duration::from_secs(86_400);
