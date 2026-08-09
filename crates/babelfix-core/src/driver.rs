//! A ready-assembled, still entirely synchronous FIX session.
//!
//! [`SessionState`] plus [`SessionOutput`] is the right substrate, but it is not
//! a product: asking someone to implement a trait and wire up a codec before
//! they can send a message is friction for no reason. [`SessionDriver`] is that
//! assembly — state machine, decoder, encoder and buffers — with no I/O, no
//! timers, no tasks and no runtime.
//!
//! You own the file descriptor. Feed it bytes, drain its buffer, and write that
//! buffer however you like: `epoll`, `io_uring`, a busy-polled socket, kernel
//! bypass. Nothing here will allocate a task or block on your behalf.
//!
//! ```no_run
//! # use std::time::Instant;
//! # use babelfix_core::driver::SessionDriver;
//! # use babelfix_core::session::{Event, Progress};
//! # fn run(driver: &mut SessionDriver, fd_bytes: &[u8]) -> babelfix_core::Result<()> {
//! let mut on_event = |event: Event<'_>| {
//!   if let Event::MessageReceived(msg) = event {
//!     // business logic
//!     let _ = msg;
//!   }
//!   Ok(())
//! };
//!
//! let progress = driver.on_bytes(Instant::now(), fd_bytes, &mut on_event)?;
//!
//! // Whatever the pass produced is now sitting here, ready for one write().
//! let out = driver.pending_writes();
//! // write(fd, out)...
//! out.clear();
//!
//! if progress == Progress::Close {
//!   // tear the connection down
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Timing
//!
//! [`next_deadline`](SessionDriver::next_deadline) says when
//! [`on_tick`](SessionDriver::on_tick) next has something to do — a heartbeat to
//! send, or a silent peer to give up on. Arm your loop's timeout with it, or
//! just call `on_tick` whenever you happen to come round; nothing fires early.

use std::time::Instant;

use bytes::BytesMut;
use chrono::{DateTime, Utc};

use crate::codec::{FixDecoder, FixEncoder};
use crate::message::{FixMessage, builder};
use crate::session::{
  Command, Event, EventSink, Progress, Session, SessionOutput, SessionState,
};
use crate::{Result, repository};

/// Reads the wall clock for `SendingTime`.
///
/// A plain function pointer rather than a trait: it keeps the driver free of
/// generics, and it is called once per outbound message so a caller with a
/// better clock than `Utc::now` — a TSC read, say — can supply it.
pub type Clock = fn() -> DateTime<Utc>;

/// The transport half of [`SessionOutput`], borrowing the caller's event sink
/// for the application half.
struct DriverOutput<'a, E> {
  encoder: &'a mut FixEncoder,
  bytes: &'a mut BytesMut,
  clock: Clock,
  sink: &'a mut E,
}

impl<E: EventSink> SessionOutput for DriverOutput<'_, E> {
  fn transmit(
    &mut self,
    msg: &mut FixMessage,
    _session: &Session,
  ) -> Result<()> {
    // One clock read per message, immediately before the bytes exist.
    self.encoder.encode_stamped(msg, (self.clock)(), self.bytes)
  }

  fn event(&mut self, event: Event<'_>) -> Result<()> {
    self.sink.event(event)
  }
}

/// A FIX session with its codec and buffers attached, driven synchronously.
pub struct SessionDriver {
  state: SessionState,
  decoder: FixDecoder,
  encoder: FixEncoder,
  /// Bytes received but not yet framed into a complete message.
  in_buf: BytesMut,
  /// Encoded messages waiting to be written to the peer.
  out_buf: BytesMut,
  clock: Clock,
}

impl SessionDriver {
  /// Assemble a driver around a session whose logon exchange has completed.
  ///
  /// `delimiter` is the field separator; `None` means the wire default, SOH.
  /// `clock` is read once per outbound message to stamp `SendingTime`.
  pub fn new(
    state: SessionState,
    repo: std::sync::Arc<repository::FixRepository>,
    delimiter: Option<u8>,
    clock: Clock,
  ) -> Self {
    let precision = state.session().time_precision;
    let fix_version = state.session().fix_version.clone();
    Self {
      state,
      decoder: FixDecoder::with_version(repo, delimiter, fix_version),
      encoder: FixEncoder::new(delimiter).with_precision(precision),
      in_buf: BytesMut::with_capacity(8192),
      out_buf: BytesMut::with_capacity(8192),
      clock,
    }
  }

  pub fn session(&self) -> &Session {
    self.state.session()
  }

  pub fn state(&self) -> &SessionState {
    &self.state
  }

  /// When [`on_tick`](Self::on_tick) next has work to do.
  pub fn next_deadline(&self) -> Option<Instant> {
    self.state.next_deadline()
  }

  /// Encoded bytes waiting for the peer.
  ///
  /// Write them and then `clear()`. They are not cleared for you, so a partial
  /// write can be handled by advancing the buffer instead.
  pub fn pending_writes(&mut self) -> &mut BytesMut {
    &mut self.out_buf
  }

  /// Whether anything is waiting to be written.
  pub fn has_pending_writes(&self) -> bool {
    !self.out_buf.is_empty()
  }

  /// Complete the logon exchange: process the peer's Logon and emit the
  /// synchronisation TestRequest.
  pub fn start(
    &mut self,
    logon: builder::Message,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let mut out = DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.clock,
      sink,
    };
    self.state.start(logon, now, &mut out)
  }

  /// Transmit a message belonging to the logon exchange, which the caller still
  /// owns. See [`SessionState::send_logon`].
  pub fn send_logon(
    &mut self,
    msg: builder::Message,
    sink: &mut impl EventSink,
  ) -> Result<()> {
    let mut out = DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.clock,
      sink,
    };
    self.state.send_logon(msg, &mut out)
  }

  /// Feed bytes straight off the socket.
  ///
  /// Frames whatever complete messages they contain and runs each through the
  /// session; a partial frame is retained until the rest arrives. Stops at the
  /// first message that ends the session, leaving any bytes after it unread.
  pub fn on_bytes(
    &mut self,
    now: Instant,
    src: &[u8],
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    self.in_buf.extend_from_slice(src);

    while let Some(msg) = self.decoder.decode(&mut self.in_buf)? {
      let mut out = DriverOutput {
        encoder: &mut self.encoder,
        bytes: &mut self.out_buf,
        clock: self.clock,
        sink,
      };
      if self.state.on_message(msg, now, &mut out)?.is_close() {
        return Ok(Progress::Close);
      }
    }

    Ok(Progress::Continue)
  }

  /// Apply an application command.
  pub fn on_command(
    &mut self,
    now: Instant,
    cmd: Command,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let mut out = DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.clock,
      sink,
    };
    self.state.on_command(cmd, now, &mut out)
  }

  /// Advance time. Safe to call whenever; nothing fires before its deadline.
  pub fn on_tick(
    &mut self,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let mut out = DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.clock,
      sink,
    };
    self.state.on_timeout(now, &mut out)
  }

  /// The peer closed the connection without logging out.
  pub fn on_peer_closed(
    &mut self,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let mut out = DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.clock,
      sink,
    };
    self.state.on_peer_closed(&mut out)
  }
}

impl std::fmt::Debug for SessionDriver {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SessionDriver")
      .field("state", &self.state)
      .field("in_buf", &self.in_buf.len())
      .field("out_buf", &self.out_buf.len())
      .finish()
  }
}
