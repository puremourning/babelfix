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

use babelfix_core::driver::{
  AcceptorDriver, DriverConfig, InitiatorDriver, SessionDriver,
};
use babelfix_core::message::builder;
use babelfix_core::session::{
  Command, EventSink, Progress, Session, SessionIdentifier,
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
  driver: Box<SessionDriver>,
  read_buf: BytesMut,
}

/// An accepted connection whose peer has sent its Logon, but for which the
/// application has not yet supplied the persisted sequence numbers.
///
/// The identity comes *from* the Logon, so it cannot be known before the first
/// frame arrives — which is why accepting is two steps rather than one.
pub struct PendingSession<S> {
  io: S,
  handshake: AcceptorDriver,
  session_id: SessionIdentifier,
}

impl<S> PendingSession<S> {
  /// Who the peer says it is. Look up your persisted state with this.
  pub fn session_id(&self) -> &SessionIdentifier {
    &self.session_id
  }

  /// The Logon itself, for applications that authenticate on it.
  pub fn logon(&self) -> &builder::Message {
    self
      .handshake
      .peer_logon()
      .expect("a PendingSession always holds the peer's Logon")
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
      mut io, handshake, ..
    } = self;

    let (mut driver, progress) =
      handshake.accept(session, Instant::now(), sink)?;

    // Establishing hands the codec and its buffers to the session, so the
    // Logon reply is now the driver's to flush, not the handshake's.
    flush(&mut io, driver.pending_writes()).await?;

    if progress.is_close() {
      // Unlike the endpoint, the caller has already seen why: its own sink
      // received the events synchronously.
      return Err(Error::connection_failed(
        "session closed during the logon exchange",
      ));
    }

    Ok(SessionConnection {
      io,
      driver,
      read_buf: BytesMut::with_capacity(READ_CHUNK),
    })
  }
}

/// The wall clock the tokio driver stamps `SendingTime` from.
fn wall_clock() -> chrono::DateTime<chrono::Utc> {
  chrono::Utc::now()
}

fn driver_config(
  repo: Arc<FixRepository>,
  delimiter: Option<u8>,
) -> DriverConfig {
  DriverConfig {
    repo,
    delimiter,
    clock: wall_clock,
    logon_timeout: LOGON_TIMEOUT,
  }
}

impl<S: AsyncRead + AsyncWrite + Unpin> SessionConnection<S> {
  /// Initiate a session: send a Logon and wait for the peer's.
  pub async fn initiate(
    mut io: S,
    repo: Arc<FixRepository>,
    delimiter: Option<u8>,
    session_id: SessionIdentifier,
    session: Session,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    let mut handshake = InitiatorDriver::start(
      session_id,
      session,
      driver_config(repo, delimiter),
      Instant::now(),
      sink,
    )?;
    flush(&mut io, handshake.pending_writes()).await?;

    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let deadline = tokio::time::Instant::now() + LOGON_TIMEOUT;
    loop {
      read_more(&mut io, &mut buf, deadline).await?;
      let bytes = std::mem::take(&mut buf);
      buf = BytesMut::with_capacity(READ_CHUNK);

      if let Some((mut driver, progress)) =
        handshake.on_bytes(Instant::now(), &bytes, sink)?
      {
        flush(&mut io, driver.pending_writes()).await?;
        if progress.is_close() {
          return Err(Error::connection_failed(
            "session closed during the logon exchange",
          ));
        }
        return Ok(Self {
          io,
          driver,
          read_buf: BytesMut::with_capacity(READ_CHUNK),
        });
      }
      flush(&mut io, handshake.pending_writes()).await?;
    }
  }

  /// Accept a session: wait for the peer's Logon and report who it claims to
  /// be, so the application can supply the persisted sequence numbers.
  ///
  /// The identity comes out of the Logon, so it cannot be known before the
  /// first frame arrives — which is why accepting is two steps. Note there is
  /// no event sink here: until the session is named, there is nothing an event
  /// could be about.
  pub async fn accept(
    mut io: S,
    repo: Arc<FixRepository>,
    delimiter: Option<u8>,
  ) -> Result<PendingSession<S>> {
    let mut handshake =
      AcceptorDriver::new(driver_config(repo, delimiter), Instant::now());

    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let deadline = tokio::time::Instant::now() + LOGON_TIMEOUT;
    loop {
      read_more(&mut io, &mut buf, deadline).await?;
      let bytes = std::mem::take(&mut buf);
      buf = BytesMut::with_capacity(READ_CHUNK);

      // The handshake validates that this is a Logon before deriving anything
      // from it, so a peer opening with garbage never reaches the application.
      if let Some(session_id) = handshake.on_bytes(&bytes)? {
        let session_id = session_id.clone();
        return Ok(PendingSession {
          io,
          handshake,
          session_id,
        });
      }
    }
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
}

/// Write out whatever a driver has queued, handing the buffer back afterwards.
///
/// Taken rather than borrowed because the borrow cannot be held across the
/// await while the socket is also borrowed.
async fn flush<S: AsyncWrite + Unpin>(
  io: &mut S,
  pending: &mut BytesMut,
) -> Result<()> {
  if pending.is_empty() {
    return Ok(());
  }
  let bytes = std::mem::take(pending);
  let result = io.write_all(&bytes).await;
  *pending = bytes;
  pending.clear();
  result?;
  io.flush().await?;
  Ok(())
}

/// Read at least one more byte into `buf`, or give up on the deadline.
async fn read_more<S: AsyncRead + Unpin>(
  io: &mut S,
  buf: &mut BytesMut,
  deadline: tokio::time::Instant,
) -> Result<()> {
  let read = tokio::select! {
    _ = tokio::time::sleep_until(deadline) => {
      return Err(Error::connection_failed(
        "logon exchange did not complete in time",
      ));
    }
    read = io.read_buf(buf) => read,
  };
  match read {
    Ok(0) => Err(Error::connection_failed(
      "Connection closed before first message",
    )),
    Ok(_) => Ok(()),
    Err(e) => Err(Error::Io(e)),
  }
}
