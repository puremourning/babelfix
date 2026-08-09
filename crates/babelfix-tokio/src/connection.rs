//! A FIX session driven inline on the caller's task, with no channels and no
//! spawning.
//!
//! [`endpoint::serve`](crate::endpoint::serve) hands you a
//! [`SessionHandle`](crate::session::SessionHandle): the session runs in its own
//! task and you talk to it over two `mpsc` channels. That is the right shape
//! when the session and the application belong to different parts of a program,
//! and it is what most applications want.
//!
//! [`SessionConnection`] is the other shape. It owns the socket and the state
//! machine, and you drive it from your own loop. Events arrive as borrows —
//! nothing is cloned, nothing is queued, no task is woken — and you can run it
//! over anything implementing [`AsyncRead`] + [`AsyncWrite`], which includes
//! TLS streams and [`tokio::io::duplex`] as well as sockets.
//!
//! ```no_run
//! # use babelfix_tokio::connection::SessionConnection;
//! # use babelfix_tokio::session::{Event, Progress};
//! # async fn run<S>(mut conn: SessionConnection<S>) -> babelfix_tokio::Result<()>
//! # where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
//! let mut on_event = |event: Event<'_>| {
//!   if let Event::MessageReceived(msg) = event {
//!     let _ = msg; // business logic
//!   }
//!   Ok(())
//! };
//!
//! conn.run(&mut on_event).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # The catch
//!
//! **Heartbeats only advance while you are in the loop.** [`run`] handles this
//! for you, but if you drive [`step`] yourself and do slow work between calls,
//! the session stops heartbeating for that long and the peer will eventually
//! log you out. The spawned model has no such failure mode, because the session
//! task keeps running whatever the application is doing. If your event handling
//! can block for an appreciable fraction of the heartbeat interval, use
//! [`endpoint`](crate::endpoint) instead.
//!
//! A `SessionConnection` also cannot be split the way a `SessionHandle` can —
//! both halves need `&mut` on the same state machine — so this is "one owner,
//! one loop, send from inside the loop". It is a different shape, not a
//! drop-in.
//!
//! [`run`]: SessionConnection::run
//! [`step`]: SessionConnection::step
//! [`AsyncRead`]: tokio::io::AsyncRead
//! [`AsyncWrite`]: tokio::io::AsyncWrite

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use babelfix_core::codec::FixDecoder;
use babelfix_core::driver::SessionDriver;
use babelfix_core::message::builder;
use babelfix_core::session::{
  Command, EventSink, Progress, Session, SessionIdentifier, SessionState,
};

use crate::repository::FixRepository;
use crate::{Error, Result};

/// How long a peer has to complete the logon exchange.
const LOGON_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Read buffer size. FIX messages are small; this is comfortably several.
const READ_CHUNK: usize = 8192;

/// A logged-on FIX session over `S`, driven by the caller.
pub struct SessionConnection<S> {
  io: S,
  driver: SessionDriver,
  read_buf: BytesMut,
  repo: Arc<FixRepository>,
  delimiter: Option<u8>,
}

/// An accepted connection whose peer has sent its Logon, but for which the
/// application has not yet supplied the persisted sequence numbers.
///
/// The identity comes *from* the Logon, so it cannot be known before the first
/// frame arrives — which is why accepting is two steps rather than one.
pub struct PendingSession<S> {
  io: S,
  repo: Arc<FixRepository>,
  delimiter: Option<u8>,
  session_id: SessionIdentifier,
  logon: builder::Message,
  /// Bytes that arrived after the Logon in the same read.
  leftover: BytesMut,
}

impl<S> PendingSession<S> {
  /// Who the peer says it is. Look up your persisted state with this.
  pub fn session_id(&self) -> &SessionIdentifier {
    &self.session_id
  }

  /// The Logon itself, for applications that authenticate on it.
  pub fn logon(&self) -> &builder::Message {
    &self.logon
  }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PendingSession<S> {
  /// Supply the session state and complete the exchange.
  pub async fn accept(
    self,
    session: Session,
    sink: &mut impl EventSink,
  ) -> Result<SessionConnection<S>> {
    let PendingSession {
      io,
      repo,
      delimiter,
      session_id,
      logon,
      leftover,
    } = self;

    let logon_reply = make_logon_message(&session)?;
    let state = SessionState::new(session_id, session, Instant::now());
    let driver = SessionDriver::new(state, repo.clone(), delimiter, wall_clock);

    let mut conn = SessionConnection {
      io,
      driver,
      read_buf: leftover,
      repo,
      delimiter,
    };

    conn.driver.send_logon(logon_reply, sink)?;
    let progress = conn.driver.start(logon, Instant::now(), sink)?;
    conn.flush().await?;
    if progress.is_close() {
      return Err(Error::connection_failed("session closed during logon"));
    }

    Ok(conn)
  }
}

/// The wall clock the tokio driver stamps `SendingTime` from.
fn wall_clock() -> chrono::DateTime<chrono::Utc> {
  chrono::Utc::now()
}

fn make_logon_message(session: &Session) -> Result<builder::Message> {
  crate::endpoint::logon_message(&session.fix_version, session)
}

impl<S: AsyncRead + AsyncWrite + Unpin> SessionConnection<S> {
  /// Initiate a session: send a Logon and wait for the peer's.
  pub async fn initiate(
    io: S,
    repo: Arc<FixRepository>,
    delimiter: Option<u8>,
    session_id: SessionIdentifier,
    session: Session,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    let logon = make_logon_message(&session)?;
    let state = SessionState::new(session_id, session, Instant::now());
    let driver = SessionDriver::new(state, repo.clone(), delimiter, wall_clock);

    let mut conn = Self {
      io,
      driver,
      read_buf: BytesMut::with_capacity(READ_CHUNK),
      repo,
      delimiter,
    };

    conn.driver.send_logon(logon, sink)?;
    conn.flush().await?;

    let logon = conn.read_logon().await?;

    let progress = conn.driver.start(logon, Instant::now(), sink)?;
    conn.flush().await?;
    if progress.is_close() {
      return Err(Error::connection_failed("session closed during logon"));
    }

    // Anything that arrived alongside the Logon is ordinary traffic — and it
    // can end the session, so the disposition matters even here.
    let progress = conn.drain_read_buf(sink)?;
    conn.flush().await?;
    if progress.is_close() {
      return Err(Error::connection_failed(
        "session closed immediately after logon",
      ));
    }

    Ok(conn)
  }

  /// Accept a session: wait for the peer's Logon and report who it claims to
  /// be, so the application can supply the persisted sequence numbers.
  pub async fn accept(
    io: S,
    repo: Arc<FixRepository>,
    delimiter: Option<u8>,
  ) -> Result<PendingSession<S>> {
    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let mut decoder = FixDecoder::new(repo.clone(), delimiter);
    let mut io = io;

    let logon_msg = read_one_message(&mut io, &mut buf, &mut decoder).await?;
    let logon = builder::Message::from_message(&logon_msg)?;

    if logon.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon.fix_message.msg_type
      )));
    }

    let session_id = session_id_from_logon(&logon)?;

    Ok(PendingSession {
      io,
      repo,
      delimiter,
      session_id,
      logon,
      leftover: buf,
    })
  }

  pub fn session(&self) -> &Session {
    self.driver.session()
  }

  pub fn session_id(&self) -> &SessionIdentifier {
    self.driver.state().session_id()
  }

  /// When [`step`](Self::step) will next act on time alone.
  pub fn deadline(&self) -> Option<Instant> {
    self.driver.next_deadline()
  }

  /// Send an application message.
  pub async fn send(
    &mut self,
    msg: builder::Message,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let progress =
      self
        .driver
        .on_command(Instant::now(), Command::Send(msg), sink)?;
    self.flush().await?;
    Ok(progress)
  }

  /// Apply any [`Command`] — replay, disconnect, and so on.
  pub async fn command(
    &mut self,
    cmd: Command,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let progress = self.driver.on_command(Instant::now(), cmd, sink)?;
    self.flush().await?;
    Ok(progress)
  }

  /// Wait for the socket or the next deadline, whichever comes first, and
  /// process it.
  pub async fn step(&mut self, sink: &mut impl EventSink) -> Result<Progress> {
    let deadline = self
      .deadline()
      .unwrap_or_else(|| Instant::now() + LOGON_TIMEOUT);

    let progress = tokio::select! {
      _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
        self.driver.on_tick(Instant::now(), sink)?
      }
      read = self.io.read_buf(&mut self.read_buf) => {
        match read {
          Ok(0) => self.driver.on_peer_closed(sink)?,
          Ok(_) => self.drain_read_buf(sink)?,
          Err(e) => return Err(Error::Io(e)),
        }
      }
    };

    // Everything the pass produced reaches the peer before the next input is
    // read, which is what keeps backpressure connected.
    self.flush().await?;
    Ok(progress)
  }

  /// Drive the session until it ends.
  pub async fn run(&mut self, sink: &mut impl EventSink) -> Result<()> {
    while self.step(sink).await? == Progress::Continue {}
    Ok(())
  }

  /// Feed whatever is in the read buffer to the state machine.
  fn drain_read_buf(&mut self, sink: &mut impl EventSink) -> Result<Progress> {
    let bytes = std::mem::take(&mut self.read_buf);
    let progress = self.driver.on_bytes(Instant::now(), &bytes, sink)?;
    // `on_bytes` keeps any partial frame itself, so the buffer starts empty
    // again; reuse the allocation.
    self.read_buf = BytesMut::with_capacity(READ_CHUNK);
    Ok(progress)
  }

  async fn flush(&mut self) -> Result<()> {
    if !self.driver.has_pending_writes() {
      return Ok(());
    }
    // Taken rather than borrowed: the borrow cannot be held across the await
    // while `self.io` is also borrowed. The allocation is handed straight back.
    let mut bytes = std::mem::take(self.driver.pending_writes());
    let result = self.io.write_all(&bytes).await;
    bytes.clear();
    *self.driver.pending_writes() = bytes;
    result?;
    self.io.flush().await?;
    Ok(())
  }

  /// Read until the peer's Logon arrives. Anything read alongside it is left
  /// in `read_buf` for the main loop.
  async fn read_logon(&mut self) -> Result<builder::Message> {
    // An initiator already knows the version it asked for.
    let mut decoder = FixDecoder::with_version(
      self.repo.clone(),
      self.delimiter,
      self.driver.session().fix_version.clone(),
    );
    let msg =
      read_one_message(&mut self.io, &mut self.read_buf, &mut decoder).await?;
    let logon = builder::Message::from_message(&msg)?;
    if logon.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon.fix_message.msg_type
      )));
    }
    Ok(logon)
  }
}

/// Derive the session identity from a peer's Logon.
///
/// The peer's `SenderCompID` is our `TargetCompID` and vice versa.
pub fn session_id_from_logon(
  logon: &builder::Message,
) -> Result<SessionIdentifier> {
  use crate::schema::FIX_Latest::Fields;
  Ok(SessionIdentifier {
    begin_string: logon
      .header
      .tag(Fields::BeginString)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing BeginString")
      })?
      .as_string(),
    sender_comp_id: logon
      .header
      .tag(Fields::TargetCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing TargetCompID")
      })?
      .as_string(),
    target_comp_id: logon
      .header
      .tag(Fields::SenderCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing SenderCompID")
      })?
      .as_string(),
  })
}

/// Read from `io` until `decoder` yields a message, or the logon deadline
/// passes.
async fn read_one_message<S: AsyncRead + Unpin>(
  io: &mut S,
  buf: &mut BytesMut,
  decoder: &mut FixDecoder,
) -> Result<babelfix_core::FixMessage> {
  let deadline = tokio::time::Instant::now() + LOGON_TIMEOUT;
  loop {
    if let Some(msg) = decoder.decode(buf)? {
      return Ok(msg);
    }
    let read = tokio::select! {
      _ = tokio::time::sleep_until(deadline) => {
        return Err(Error::connection_failed(
          "Connection timed out after 30 seconds",
        ));
      }
      read = io.read_buf(buf) => read,
    };
    match read {
      Ok(0) => {
        return Err(Error::connection_failed(
          "Connection closed before first message",
        ));
      }
      Ok(_) => {}
      Err(e) => return Err(Error::Io(e)),
    }
  }
}
