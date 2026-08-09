//! FIX session layer: sequence numbers, heartbeats and message recovery,
//! as a sans-io state machine.
//!
//! [`SessionState`] owns the protocol: sequence number checking, heartbeats and
//! test requests, gap detection, resend and replay, and logout. It performs no
//! I/O, spawns nothing, and reads no clock. Instead:
//!
//! * the driver feeds it decoded messages, application commands and timeouts;
//! * it writes messages and events into a [`SessionOutput`] the driver supplies;
//! * it says when it next needs attention via [`SessionState::next_deadline`].
//!
//! That makes it usable from an async task, from a hand-rolled `epoll` loop, or
//! from a test with a clock the test advances by hand.
//!
//! ```no_run
//! # use std::time::Instant;
//! # use babelfix_core::session::{SessionState, SessionOutput, Progress};
//! # fn drive(state: &mut SessionState, out: &mut impl SessionOutput,
//! #          msg: babelfix_core::FixMessage) -> babelfix_core::Result<()> {
//! let now = Instant::now();
//! match state.on_message(msg, now, out)? {
//!   Progress::Continue => {}
//!   Progress::Close => return Ok(()),
//! }
//! if state.next_deadline().is_some_and(|d| d <= now) {
//!   state.on_timeout(now, out)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Backpressure
//!
//! The driver **must** flush everything a call emits before feeding the next
//! input. In the async world this happens naturally — awaiting the socket write
//! suspends the session, which stops it draining the socket, which pushes back
//! on the peer. A driver that buffers outputs without bound and keeps feeding
//! inputs deletes that, and a slow application will no longer throttle the wire.

mod handshake;
mod replay;
mod state;

use std::sync::Arc;

pub use handshake::{
  AcceptorHandshake, Established, InitiatorHandshake, expect_logon,
  logon_message, session_id_from_logon,
};
pub use replay::Replay;
pub use state::SessionState;

use crate::message::{FixMessage, builder};
use crate::repository::FixVersion;
use crate::time::TimePrecision;

/// Identifies a session by the triple FIX uses to route messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SessionIdentifier {
  pub begin_string: String,
  /// Our `SenderCompID` — the identifier we put on outbound messages.
  pub sender_comp_id: String,
  /// The peer's `SenderCompID` — our `TargetCompID`.
  pub target_comp_id: String,
}

/// The mutable per-connection state an application must persist to recover a
/// session: the sequence numbers, plus the negotiated settings.
#[derive(Clone, Default)]
pub struct Session {
  pub next_out_seq_num: u32,
  pub next_in_seq_num: u32,
  pub heartbeat_interval: std::time::Duration,
  pub fix_version: Arc<FixVersion>,
  /// Fractional-second precision for the `SendingTime` stamped on outbound
  /// messages. Defaults to nanoseconds.
  ///
  /// Some counterparties reject a `SendingTime` carrying more precision than
  /// they expect, so this is per-session rather than global.
  pub time_precision: TimePrecision,
}

impl std::fmt::Debug for Session {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Session")
      .field("next_out_seq_num", &self.next_out_seq_num)
      .field("next_in_seq_num", &self.next_in_seq_num)
      .field("heartbeat_interval", &self.heartbeat_interval)
      .field("fix_version", &self.fix_version.name)
      .field("time_precision", &self.time_precision)
      .finish()
  }
}

impl Session {
  pub fn new(fix_version: Arc<FixVersion>) -> Self {
    Self {
      next_out_seq_num: 1,
      next_in_seq_num: 1,
      heartbeat_interval: std::time::Duration::from_secs(30),
      fix_version,
      time_precision: TimePrecision::default(),
    }
  }
}

/// Something the application asks of a live session.
///
/// Note there is no "get session state" command: the state is right there, via
/// [`SessionState::session`]. It only needed to be a message when the state
/// lived inside a detached task.
#[derive(Debug)]
#[non_exhaustive]
pub enum Command {
  /// Send the message on the session. It is assigned the next outbound sequence
  /// number and has its session header fields populated. Any supplied
  /// `MsgSeqNum`, `SendingTime`, `SenderCompID` or `TargetCompID` is overwritten
  /// — applications cannot set these correctly, so they are left to the session.
  ///
  /// It is an error to send while a replay is in progress; use
  /// [`Command::Replay`] to answer an [`Event::ResendRequest`].
  Send(builder::Message),

  /// Replay the sequence number in `MsgSeqNum` with the supplied message. Only
  /// valid between an [`Event::ResendRequest`] and the matching
  /// [`Command::ReplayComplete`].
  Replay(builder::Message),

  /// All messages for the current resend request have been sent. Any remaining
  /// sequence numbers are gap-filled automatically.
  ReplayComplete,

  /// Disconnect the session, announcing the intent with a Logout first.
  Disconnect,
}

/// Something the session tells the application about.
///
/// Every payload is borrowed. A driver that wants owned events clones on the
/// way past; one that just wants to write bytes to a journal copies nothing.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event<'a> {
  /// A connection exists, but no logon exchange has happened yet.
  ConnectionEstablished,

  /// The peer has retransmitted everything we were missing. Note this says
  /// nothing about whether the peer has received, or even asked for, anything
  /// it is missing from us.
  RecoveryCompleted,

  /// The current sequence numbers. Applications must persist these to recover
  /// the session; they are what an acceptor supplies when a peer reconnects.
  SessionState(&'a Session),

  /// A FIX message arrived, valid or not, admin or application. Useful for
  /// auditing and display. Business logic wants [`Event::MessageReceived`],
  /// which fires only for valid, well-sequenced application messages.
  RawMessageReceived(&'a FixMessage, &'a Session),

  /// A FIX message was handed to the transport, admin messages included. The
  /// `SendingTime` has already been stamped by the [`SessionOutput`], so this
  /// is the message as it goes on the wire.
  ///
  /// Applications must persist these unmodified to answer a future
  /// [`Event::ResendRequest`]; this library provides no persistence of its own.
  RawMessageSent(&'a FixMessage, &'a Session),

  /// A valid, in-sequence application message. This is what business logic
  /// should act on.
  ///
  /// Replayed messages are delivered before new ones, so applications never see
  /// these out of order — beyond noticing that the peer may have set
  /// `PossDupFlag`.
  MessageReceived(&'a builder::Message),

  /// The peer asked for a retransmission of `begin_seq_no..=end_seq_no`.
  /// `end_seq_no` is always concrete, even when the peer sent an open-ended
  /// request.
  ///
  /// Answer with [`Command::Replay`] for each message, then
  /// [`Command::ReplayComplete`]. Skipped sequence numbers are gap-filled
  /// automatically, so an application may decline to replay a message — a stale
  /// order, say — without breaking the sequence.
  ResendRequest {
    resend_request: &'a builder::Message,
    begin_seq_no: u32,
    end_seq_no: u32,
  },

  /// The session has ended, through logout or a network failure.
  Disconnected,
}

/// Where a [`SessionState`] puts the messages and events it produces.
///
/// The driver implements this. A low-latency driver encodes straight into the
/// buffer it is about to hand to `write()`; the tokio driver clones events into
/// owned form and pushes them at the application.
pub trait SessionOutput {
  /// Stamp `SendingTime`, serialise, and arrange for the bytes to reach the
  /// peer.
  ///
  /// The message arrives with its `SendingTime` slot present but empty; the
  /// implementation fills it in — see
  /// [`stamp_sending_time`](crate::codec::stamp_sending_time) — so the clock is
  /// read once per message, as late as possible, rather than once per pass over
  /// the state machine. The `&mut` is what makes that stamp visible to the
  /// [`Event::RawMessageSent`] that follows.
  ///
  /// `session` is a snapshot taken *for this message*: the outbound sequence
  /// number has already been consumed. A driver must not substitute the
  /// session's later state, or a call emitting several messages would label
  /// them all with the last sequence number.
  fn transmit(
    &mut self,
    msg: &mut FixMessage,
    session: &Session,
  ) -> crate::Result<()>;

  /// Report an event to the application.
  fn event(&mut self, event: Event<'_>) -> crate::Result<()>;
}

/// The application-facing half of a [`SessionOutput`].
///
/// [`SessionDriver`](crate::driver::SessionDriver) already owns the transport
/// half — the codec and the buffers — so a caller using it only has to say what
/// to do with events. A closure will do:
///
/// ```no_run
/// # use babelfix_core::session::{Event, EventSink};
/// let mut sink = |event: Event<'_>| {
///   if let Event::MessageReceived(msg) = event {
///     println!("{msg:?}");
///   }
///   Ok(())
/// };
/// # let _: &mut dyn EventSink = &mut sink;
/// ```
pub trait EventSink {
  fn event(&mut self, event: Event<'_>) -> crate::Result<()>;
}

impl<F> EventSink for F
where
  F: FnMut(Event<'_>) -> crate::Result<()>,
{
  fn event(&mut self, event: Event<'_>) -> crate::Result<()> {
    self(event)
  }
}

/// Discards every event. Useful for a peer that only cares about the wire.
impl EventSink for () {
  fn event(&mut self, _event: Event<'_>) -> crate::Result<()> {
    Ok(())
  }
}

/// Whether the session is still alive after a call.
///
/// `#[must_use]`: a driver that drops this keeps running a session the protocol
/// has already ended.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
  Continue,
  /// The session is over. Everything emitted during the call — including the
  /// Logout, if there is one — precedes this, so the driver should flush, then
  /// tear the connection down.
  Close,
}

impl Progress {
  pub fn is_close(self) -> bool {
    matches!(self, Progress::Close)
  }
}
