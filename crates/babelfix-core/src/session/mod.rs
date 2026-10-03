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
//! #          msg: babelfix_core::message::Message) -> babelfix_core::Result<()> {
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

pub(crate) mod fields;
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

use crate::message::{Dictionary, Message};
use crate::time::{TimePrecision, write_fix_time};

/// Identifies a session by the triple FIX uses to route messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SessionIdentifier {
  /// The wire `BeginString(8)`: `FIX.4.4`, or `FIXT.1.1` for FIX.Latest.
  pub begin_string: String,
  /// Our `SenderCompID` — the identifier we put on outbound messages.
  pub sender_comp_id: String,
  /// The peer's `SenderCompID` — our `TargetCompID`.
  pub target_comp_id: String,
}

/// The negotiated settings of a session.
///
/// Nothing here is state to persist. The only state a session needs to resume
/// is the inbound sequence number it expects next, which is supplied alongside
/// this; outbound sequence numbers belong to the application, and arrive on
/// each message it sends (see [`Command::Send`]).
#[derive(Clone)]
pub struct SessionConfig {
  /// How often each side must send something (`HeartBtInt`).
  pub heartbeat_interval: std::time::Duration,
  /// The FIX version the session speaks.
  pub dict: Arc<Dictionary>,
  /// Fractional-second precision for the `SendingTime` stamped on outbound
  /// messages that do not carry one. Defaults to nanoseconds.
  ///
  /// Some counterparties reject a `SendingTime` carrying more precision than
  /// they expect, so this is per-session rather than global.
  pub time_precision: TimePrecision,
}

impl std::fmt::Debug for SessionConfig {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SessionConfig")
      .field("heartbeat_interval", &self.heartbeat_interval)
      .field("fix_version", &self.dict.version().name)
      .field("time_precision", &self.time_precision)
      .finish()
  }
}

impl SessionConfig {
  pub fn new(dict: Arc<Dictionary>) -> Self {
    Self {
      heartbeat_interval: std::time::Duration::from_secs(30),
      dict,
      time_precision: TimePrecision::default(),
    }
  }
}

/// An outbound message on its way to the wire, whose `SendingTime(52)` may
/// still need writing.
///
/// A message the application supplied a `SendingTime` for is sent with it.
/// One without has a slot of exactly the timestamp's width reserved, and is
/// stamped by the driver as late as possible. The core reads no clock, so the
/// time is the driver's to supply: [`stamp`](Self::stamp) is the sole route to
/// the message, so a driver cannot encode one it has not stamped, and the clock
/// is read only when there is something to stamp.
#[must_use = "an unstamped message is never sent"]
pub struct Unstamped<'a> {
  msg: &'a mut Message,
  /// `None` when the message already carries its `SendingTime`.
  precision: Option<TimePrecision>,
}

impl<'a> Unstamped<'a> {
  pub(crate) fn new(
    msg: &'a mut Message,
    precision: Option<TimePrecision>,
  ) -> Self {
    Self { msg, precision }
  }

  /// Write `SendingTime` from `clock`, at the session's precision, unless the
  /// message already has one, and hand back the message to encode.
  pub fn stamp(
    self,
    clock: impl FnOnce() -> chrono::DateTime<chrono::Utc>,
  ) -> &'a Message {
    if let Some(precision) = self.precision {
      let now = clock();
      self.msg.stamp_sending_time(precision.width(), |out| {
        write_fix_time(now, precision, out)
      });
    }
    self.msg
  }

  /// Whether [`stamp`](Self::stamp) will read the clock.
  pub fn needs_stamp(&self) -> bool {
    self.precision.is_some()
  }

  /// The type of the message waiting to be stamped, for logging.
  pub fn msg_type(&self) -> &str {
    self.msg.msg_type()
  }
}

impl std::fmt::Debug for Unstamped<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "Unstamped({})", self.msg.msg_type())
  }
}

/// Something the application asks of a live session.
///
/// The message header is the contract: the application says what goes on the
/// wire by what it puts in the header, and the session adds only what it must
/// (`SenderCompID`, `TargetCompID`, and a `SendingTime` if there is none).
#[derive(Debug)]
#[non_exhaustive]
pub enum Command {
  /// Send the message. `MsgSeqNum(34)` is required: the application owns
  /// outbound sequence numbers, whether it allocates them itself, derives them
  /// from its own event stream, or uses a
  /// [`Sequencer`](crate::sequencer::Sequencer).
  ///
  /// A `MsgSeqNum` at or below one already sent is rejected, leaving the
  /// session untouched. A jump forward is allowed: the peer will ask for the
  /// gap, and the application replays or gap-fills it.
  ///
  /// `SendingTime(52)`, if present, is sent as-is; otherwise it is stamped by
  /// the driver. `PossDupFlag(43)` and `OrigSendingTime(122)` are sent as
  /// supplied.
  ///
  /// This is also how the admin messages the session asks for with
  /// [`Event::AdminSendRequired`] reach the wire. The first message on a
  /// session must be its Logon; anything else before the logon exchange
  /// completes is rejected.
  ///
  /// A message sent while a replay is in progress is queued, and goes out after
  /// [`Command::ReplayComplete`]; its `MsgSeqNum` must be beyond the replayed
  /// range. A Logout ends the session once sent.
  Send(Message),

  /// Retransmit the message in `MsgSeqNum` in answer to an
  /// [`Event::ResendRequest`]. Only valid between that event and the matching
  /// [`Command::ReplayComplete`].
  ///
  /// `OrigSendingTime(122)` is kept if present, and otherwise taken from
  /// `SendingTime`; a message with neither is rejected (FIX Session Layer
  /// §4.8). `PossDupFlag=Y` is set and a new `SendingTime` is stamped. Admin
  /// messages other than Reject and XMLnonFIX are gap-filled rather than
  /// resent (§4.8.5).
  Replay(Message),

  /// All messages for the current resend request have been sent. Any remaining
  /// sequence numbers are gap-filled automatically.
  ReplayComplete,

  /// Disconnect the session. The session asks for a Logout with
  /// [`Event::AdminSendRequired`], and ends once it has been sent.
  Disconnect,
}

/// Something the session tells the application about.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event<'a> {
  /// A connection exists, but no logon exchange has happened yet.
  ConnectionEstablished,

  /// The session wants this admin message sent: a Logon, Heartbeat,
  /// TestRequest, ResendRequest or Logout.
  ///
  /// It is the reverse of [`Command::Send`]. The application gives it a
  /// `MsgSeqNum` — persisting whatever it needs to first, and modifying it if
  /// it likes — and sends it back with `Command::Send`. The session keeps no
  /// copy.
  ///
  /// Two rules apply. The `TestReqID` of a TestRequest must not be changed: the
  /// session recognises the answer by it. And a request belongs to the
  /// connection that made it: one persisted before a restart must never be
  /// sent on a later connection. Its sequence number is gap-filled instead,
  /// which a replay does for admin messages anyway.
  AdminSendRequired(Message),

  /// Both Logons have been exchanged: the session accepts application
  /// messages from here on.
  LoggedOn,

  /// The peer has retransmitted everything we were missing. Note this says
  /// nothing about whether the peer has received, or even asked for, anything
  /// it is missing from us.
  RecoveryCompleted,

  /// A FIX message arrived, valid or not, admin or application. Useful for
  /// auditing and display; nothing in recovery depends on it. Business logic
  /// wants [`Event::MessageReceived`].
  RawMessageReceived(&'a Message),

  /// A FIX message was handed to the transport, admin messages included, as it
  /// went on the wire: `MsgSeqNum` and `SendingTime` are in the header. Useful
  /// for auditing and display; nothing in recovery depends on it.
  RawMessageSent(&'a Message),

  /// A valid, in-sequence application message (or session-level Reject). This
  /// is what business logic should act on.
  ///
  /// `seq_num` identifies the message within the session: record it with the
  /// effects of processing the message, and resume with `seq_num + 1` as the
  /// next inbound sequence number, so a message is never processed twice and
  /// never skipped.
  ///
  /// Replayed messages are delivered before new ones, so applications never see
  /// these out of order — beyond noticing that the peer may have set
  /// `PossDupFlag`.
  MessageReceived { seq_num: u64, msg: &'a Message },

  /// An in-sequence message the application does not see — an admin message,
  /// or a gap fill — has moved the next expected inbound sequence number on.
  ///
  /// A hint, not a requirement: an application may fold the latest value into
  /// its next write, so that a quiet session does not resume far behind.
  InboundAdvanced { next_in_seq_num: u64 },

  /// The peer asked for a retransmission of `begin_seq_no..=end_seq_no`.
  /// `end_seq_no` is always concrete, even when the peer sent an open-ended
  /// request.
  ///
  /// Answer with [`Command::Replay`] for each message, then
  /// [`Command::ReplayComplete`]. Skipped sequence numbers are gap-filled
  /// automatically, so an application may decline to replay a message — a stale
  /// order, say — without breaking the sequence.
  ResendRequest {
    resend_request: &'a Message,
    begin_seq_no: u64,
    end_seq_no: u64,
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
  /// Stamp `SendingTime` if needed, serialise, and arrange for the bytes to
  /// reach the peer.
  ///
  /// The message is [`Unstamped`]: the only way to reach it — and so to encode
  /// it — is to [`stamp`](Unstamped::stamp) it, which reads the
  /// implementation's clock only if the message has no `SendingTime` yet. The
  /// core never reads a clock itself. The stamp is visible to the
  /// [`Event::RawMessageSent`] that follows.
  fn transmit(&mut self, msg: Unstamped<'_>) -> crate::Result<()>;

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
///   if let Event::MessageReceived { msg, .. } = event {
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
