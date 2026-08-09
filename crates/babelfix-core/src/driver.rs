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
  AcceptorHandshake, Command, Event, EventSink, InitiatorHandshake, Progress,
  Session, SessionIdentifier, SessionOutput, SessionState,
};
use crate::{Error, Result, repository};

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

/// The codec, buffers and clock a driver needs, independent of any session.
struct Plumbing {
  decoder: FixDecoder,
  encoder: FixEncoder,
  in_buf: BytesMut,
  out_buf: BytesMut,
  config: DriverConfig,
}

impl Plumbing {
  fn new(config: DriverConfig, decoder: FixDecoder) -> Self {
    Self {
      decoder,
      encoder: FixEncoder::new(config.delimiter),
      in_buf: BytesMut::with_capacity(8192),
      out_buf: BytesMut::with_capacity(8192),
      config,
    }
  }

  fn output<'a, E: EventSink>(
    &'a mut self,
    sink: &'a mut E,
  ) -> DriverOutput<'a, E> {
    DriverOutput {
      encoder: &mut self.encoder,
      bytes: &mut self.out_buf,
      clock: self.config.clock,
      sink,
    }
  }

  /// Carry everything into the session, including any bytes that arrived
  /// alongside the Logon.
  fn into_session(self, state: SessionState) -> Box<SessionDriver> {
    Box::new(SessionDriver {
      state,
      decoder: self.decoder,
      encoder: self.encoder,
      in_buf: self.in_buf,
      out_buf: self.out_buf,
      clock: self.config.clock,
    })
  }
}

/// The logon exchange with a codec and buffers attached, for the side that
/// opened the connection.
///
/// [`SessionDriver`] is the session proper and always has a [`Session`]; this
/// is the phase before that. It yields a `SessionDriver` once the peer answers.
pub struct InitiatorDriver {
  handshake: Option<InitiatorHandshake>,
  plumbing: Plumbing,
}

impl InitiatorDriver {
  /// Open a session, putting our Logon in the outbound buffer.
  pub fn start(
    session_id: SessionIdentifier,
    session: Session,
    config: DriverConfig,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    // The version is already known: it is the one we are asking for.
    let decoder = FixDecoder::with_version(
      config.repo.clone(),
      config.delimiter,
      session.fix_version.clone(),
    );
    let mut plumbing = Plumbing::new(config, decoder);
    plumbing.encoder = FixEncoder::new(plumbing.config.delimiter)
      .with_precision(session.time_precision);

    let logon_timeout = plumbing.config.logon_timeout;
    let handshake = {
      let mut out = plumbing.output(sink);
      InitiatorHandshake::start(
        session_id,
        session,
        logon_timeout,
        now,
        &mut out,
      )?
    };

    Ok(Self {
      handshake: Some(handshake),
      plumbing,
    })
  }

  pub fn deadline(&self) -> Option<Instant> {
    self.handshake.as_ref().map(InitiatorHandshake::deadline)
  }

  pub fn on_tick(&self, now: Instant) -> Result<()> {
    match &self.handshake {
      Some(h) => h.on_timeout(now),
      None => Ok(()),
    }
  }

  pub fn pending_writes(&mut self) -> &mut BytesMut {
    &mut self.plumbing.out_buf
  }

  pub fn has_pending_writes(&self) -> bool {
    !self.plumbing.out_buf.is_empty()
  }

  /// Feed bytes straight off the socket. Returns the session once the peer's
  /// Logon completes the exchange.
  pub fn on_bytes(
    &mut self,
    now: Instant,
    src: &[u8],
    sink: &mut impl EventSink,
  ) -> Result<Option<(Box<SessionDriver>, Progress)>> {
    self.plumbing.in_buf.extend_from_slice(src);

    // Exactly one frame completes the exchange, and anything after it belongs
    // to the session — so it stays in `in_buf` and travels there with it.
    if let Some(msg) =
      self.plumbing.decoder.decode(&mut self.plumbing.in_buf)?
    {
      let handshake = self.handshake.take().ok_or_else(|| {
        Error::protocol_violation("logon exchange is already complete")
      })?;
      let established = {
        let mut out = self.plumbing.output(sink);
        handshake.on_peer_logon(msg, now, &mut out)?
      };
      // Swap in a throwaway so the real plumbing — codec, buffers and any
      // bytes that arrived alongside the Logon — can move into the session.
      let config = self.plumbing.config.clone();
      let spare = Plumbing::new(
        config.clone(),
        FixDecoder::new(config.repo.clone(), config.delimiter),
      );
      let plumbing = std::mem::replace(&mut self.plumbing, spare);
      return Ok(Some((
        plumbing.into_session(*established.state),
        established.progress,
      )));
    }

    Ok(None)
  }
}

/// The logon exchange with a codec and buffers attached, for the side that
/// answered the connection.
///
/// Note that [`on_bytes`](Self::on_bytes) takes no event sink: until it names a
/// session there is nothing an event could be about. See
/// [`AcceptorHandshake`](crate::session::AcceptorHandshake).
///
/// ```no_run
/// # use std::time::Instant;
/// # use babelfix_core::driver::{AcceptorDriver, DriverConfig, SessionDriver};
/// # use babelfix_core::session::Session;
/// # fn accept(
/// #   mut hs: AcceptorDriver,
/// #   bytes: &[u8],
/// #   lookup: impl Fn(&babelfix_core::session::SessionIdentifier) -> Session,
/// # ) -> babelfix_core::Result<Option<Box<SessionDriver>>> {
/// let mut sink = ();
/// Ok(match hs.on_bytes(bytes)? {
///   Some(session_id) => {
///     let session = lookup(session_id);
///     let (driver, _progress) = hs.accept(session, Instant::now(), &mut sink)?;
///     Some(driver)
///   }
///   None => None,   // partial frame; read more
/// })
/// # }
/// ```
pub struct AcceptorDriver {
  handshake: AcceptorHandshake,
  plumbing: Plumbing,
}

impl AcceptorDriver {
  /// Answer a connection. Nothing is sent until the peer identifies itself.
  pub fn new(config: DriverConfig, now: Instant) -> Self {
    let decoder = FixDecoder::new(config.repo.clone(), config.delimiter);
    let handshake = AcceptorHandshake::new(config.logon_timeout, now);
    Self {
      handshake,
      plumbing: Plumbing::new(config, decoder),
    }
  }

  pub fn deadline(&self) -> Instant {
    self.handshake.deadline()
  }

  pub fn on_tick(&self, now: Instant) -> Result<()> {
    self.handshake.on_timeout(now)
  }

  pub fn pending_writes(&mut self) -> &mut BytesMut {
    &mut self.plumbing.out_buf
  }

  pub fn has_pending_writes(&self) -> bool {
    !self.plumbing.out_buf.is_empty()
  }

  /// The peer's Logon, for applications that authenticate on it.
  pub fn peer_logon(&self) -> Option<&crate::message::builder::Message> {
    self.handshake.peer_logon()
  }

  /// Feed bytes straight off the socket until the peer names a session.
  ///
  /// No event sink, because nothing can be emitted yet.
  pub fn on_bytes(&mut self, src: &[u8]) -> Result<Option<&SessionIdentifier>> {
    self.plumbing.in_buf.extend_from_slice(src);
    match self.plumbing.decoder.decode(&mut self.plumbing.in_buf)? {
      Some(msg) => Ok(Some(self.handshake.identify(msg)?)),
      None => Ok(None),
    }
  }

  /// Supply the session named by the peer's Logon.
  pub fn accept(
    self,
    session: Session,
    now: Instant,
    sink: &mut impl EventSink,
  ) -> Result<(Box<SessionDriver>, Progress)> {
    let AcceptorDriver {
      handshake,
      mut plumbing,
    } = self;

    // The application's session decides the precision of every timestamp from
    // here on, including the Logon reply about to go out.
    plumbing.encoder = FixEncoder::new(plumbing.config.delimiter)
      .with_precision(session.time_precision);

    let established = {
      let mut out = plumbing.output(sink);
      handshake.accept(session, now, &mut out)?
    };

    let progress = established.progress;
    Ok((plumbing.into_session(*established.state), progress))
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
