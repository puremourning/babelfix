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
  Command, Event, EventSink, Handshake, Progress, Session, SessionIdentifier,
  SessionOutput, SessionState, Step,
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

/// What a driver needs besides the session itself.
#[derive(Clone)]
pub struct DriverConfig {
  pub repo: std::sync::Arc<repository::FixRepository>,
  /// Field separator; `None` means SOH.
  pub delimiter: Option<u8>,
  /// Read once per outbound message, to stamp `SendingTime`.
  pub clock: Clock,
  /// How long the peer has to complete the logon exchange.
  pub logon_timeout: std::time::Duration,
}

/// What a [`HandshakeDriver`] wants next.
#[must_use]
pub enum DriverStep {
  /// Keep feeding bytes.
  Continue,

  /// Acceptor only: the peer's Logon named this session. Look up the sequence
  /// numbers persisted for it and answer with
  /// [`HandshakeDriver::accept_session`].
  NeedsSession(SessionIdentifier),

  /// The logon exchange is complete. The codec, its buffers and any bytes that
  /// arrived alongside the Logon carry over into the session.
  Established {
    driver: Box<SessionDriver>,
    /// `Close` when the session ended during the exchange. See
    /// [`Step::Established`](crate::session::Step::Established).
    progress: Progress,
  },
}

/// The logon exchange with a codec and buffers attached.
///
/// [`SessionDriver`] is the session proper and always has a [`Session`]; this
/// is the phase before that, when an acceptor does not yet know which session
/// it is talking to. It yields a `SessionDriver` once the exchange completes.
///
/// ```no_run
/// # use std::time::Instant;
/// # use babelfix_core::driver::{DriverStep, HandshakeDriver, SessionDriver};
/// # use babelfix_core::session::Session;
/// # fn drive(
/// #   mut hs: HandshakeDriver,
/// #   bytes: &[u8],
/// #   lookup: impl Fn(&babelfix_core::session::SessionIdentifier) -> Session,
/// # ) -> babelfix_core::Result<Option<Box<SessionDriver>>> {
/// let now = Instant::now();
/// let mut sink = ();
/// let mut step = hs.on_bytes(now, bytes, &mut sink)?;
/// if let DriverStep::NeedsSession(id) = &step {
///   step = hs.accept_session(lookup(id), now, &mut sink)?;
/// }
/// Ok(match step {
///   DriverStep::Established { driver, .. } => Some(driver),
///   _ => None,
/// })
/// # }
/// ```
pub struct HandshakeDriver {
  handshake: Handshake,
  decoder: FixDecoder,
  encoder: FixEncoder,
  in_buf: BytesMut,
  out_buf: BytesMut,
  config: DriverConfig,
}

impl HandshakeDriver {
  /// Open a session, putting our Logon in the outbound buffer.
  pub fn initiator(
    session_id: SessionIdentifier,
    session: Session,
    config: DriverConfig,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    let mut encoder =
      FixEncoder::new(config.delimiter).with_precision(session.time_precision);
    let mut out_buf = BytesMut::with_capacity(8192);
    // The version is already known: it is the one we are asking for.
    let decoder = FixDecoder::with_version(
      config.repo.clone(),
      config.delimiter,
      session.fix_version.clone(),
    );

    let handshake = {
      let mut out = DriverOutput {
        encoder: &mut encoder,
        bytes: &mut out_buf,
        clock: config.clock,
        sink,
      };
      Handshake::initiator(
        session_id,
        session,
        config.logon_timeout,
        now,
        &mut out,
      )?
    };

    Ok(Self {
      handshake,
      decoder,
      encoder,
      in_buf: BytesMut::with_capacity(8192),
      out_buf,
      config,
    })
  }

  /// Answer a connection. Nothing is sent until the peer identifies itself.
  pub fn acceptor(config: DriverConfig, now: Instant) -> Self {
    Self {
      handshake: Handshake::acceptor(config.logon_timeout, now),
      decoder: FixDecoder::new(config.repo.clone(), config.delimiter),
      encoder: FixEncoder::new(config.delimiter),
      in_buf: BytesMut::with_capacity(8192),
      out_buf: BytesMut::with_capacity(8192),
      config,
    }
  }

  /// When the exchange must have completed by.
  pub fn next_deadline(&self) -> Option<Instant> {
    self.handshake.next_deadline()
  }

  /// The peer's Logon, for applications that authenticate on it. See
  /// [`Handshake::peer_logon`].
  pub fn peer_logon(&self) -> Option<&crate::message::builder::Message> {
    self.handshake.peer_logon()
  }

  /// Give up if the peer has taken too long.
  pub fn on_tick(&mut self, now: Instant) -> Result<DriverStep> {
    let _ = self.handshake.on_timeout(now)?;
    Ok(DriverStep::Continue)
  }

  pub fn pending_writes(&mut self) -> &mut BytesMut {
    &mut self.out_buf
  }

  pub fn has_pending_writes(&self) -> bool {
    !self.out_buf.is_empty()
  }

  /// Feed bytes straight off the socket.
  pub fn on_bytes(
    &mut self,
    now: Instant,
    src: &[u8],
    sink: &mut impl EventSink,
  ) -> Result<DriverStep> {
    self.in_buf.extend_from_slice(src);
    while let Some(msg) = self.decoder.decode(&mut self.in_buf)? {
      let mut out = DriverOutput {
        encoder: &mut self.encoder,
        bytes: &mut self.out_buf,
        clock: self.config.clock,
        sink,
      };
      match self.handshake.on_message(msg, now, &mut out)? {
        Step::Continue => continue,
        Step::NeedsSession(id) => return Ok(DriverStep::NeedsSession(id)),
        Step::Established { state, progress } => {
          return Ok(self.establish(*state, progress));
        }
      }
    }
    Ok(DriverStep::Continue)
  }

  /// Acceptor only: supply the session named by [`DriverStep::NeedsSession`].
  pub fn accept_session(
    &mut self,
    session: Session,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<DriverStep> {
    // The application's session decides the precision of every timestamp from
    // here on, including the Logon reply the handshake is about to send.
    self.encoder = FixEncoder::new(self.config.delimiter)
      .with_precision(session.time_precision);

    let step = {
      let mut out = DriverOutput {
        encoder: &mut self.encoder,
        bytes: &mut self.out_buf,
        clock: self.config.clock,
        sink,
      };
      self.handshake.accept_session(session, now, &mut out)?
    };

    Ok(match step {
      Step::Established { state, progress } => self.establish(*state, progress),
      Step::NeedsSession(id) => DriverStep::NeedsSession(id),
      Step::Continue => DriverStep::Continue,
    })
  }

  /// Carry the codec, its buffers and any unread bytes into the session.
  fn establish(
    &mut self,
    state: SessionState,
    progress: Progress,
  ) -> DriverStep {
    DriverStep::Established {
      progress,
      driver: Box::new(SessionDriver {
        state,
        decoder: std::mem::replace(
          &mut self.decoder,
          FixDecoder::new(self.config.repo.clone(), self.config.delimiter),
        ),
        encoder: std::mem::take(&mut self.encoder),
        in_buf: std::mem::take(&mut self.in_buf),
        out_buf: std::mem::take(&mut self.out_buf),
        clock: self.config.clock,
      }),
    }
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
