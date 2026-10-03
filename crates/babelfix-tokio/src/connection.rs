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
//!   if let Event::MessageReceived { msg, .. } = event {
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
//! It takes no [`EndpointConfig`](crate::endpoint::EndpointConfig): the peer
//! has 30 seconds to complete the logon exchange, and frames are limited to
//! [`DEFAULT_MAX_FRAME_LEN`](babelfix_core::codec::DEFAULT_MAX_FRAME_LEN).
//!
//! Like the spawned session, it is sequenced: each outbound message is
//! numbered, persisted to the [`SessionSetup`]'s store, and sent once the
//! write completes. Writes run concurrently with everything else in
//! [`step`]; [`send`](SessionConnection::send) waits only for writes that have
//! already completed.
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
use babelfix_core::message::Message;
use babelfix_core::session::{
  Command, EventSink, Progress, SessionConfig, SessionIdentifier,
};

use crate::message::Dictionaries;
use crate::session::{SessionSetup, SessionStatus, wall_clock};
use crate::store::Sequenced;
use crate::{Error, Result};

/// How long a peer has to complete the logon exchange.
const LOGON_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Read buffer size. FIX messages are small; this is comfortably several.
const READ_CHUNK: usize = 8192;

/// A logged-on FIX session over `S`, driven by the caller.
pub struct SessionConnection<S> {
  io: S,
  driver: Box<SessionDriver>,
  sequenced: Sequenced,
  read_buf: BytesMut,
  /// The session ended outside [`step`](Self::step) — a Logout released by a
  /// completed write, say — and the next `step` reports it.
  closed: bool,
}

/// An accepted connection whose peer has sent its Logon, but for which the
/// application has not yet supplied the session's setup.
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
  pub fn logon(&self) -> &Message {
    self
      .handshake
      .peer_logon()
      .expect("a PendingSession always holds the peer's Logon")
  }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PendingSession<S> {
  /// Supply the session's setup and complete the exchange.
  pub async fn accept(
    self,
    setup: SessionSetup,
    sink: &mut impl EventSink,
  ) -> Result<SessionConnection<S>> {
    let PendingSession {
      mut io,
      handshake,
      session_id,
    } = self;
    let mut sequenced = setup.sequenced(&session_id);

    let mut established = handshake.accept(
      setup.config,
      setup.resume.next_in_seq_num,
      Instant::now(),
      &mut sequenced.sink(sink),
    )?;

    // Our Logon reply is persisted before it is sent, and anything the peer's
    // Logon provoked follows it.
    let mut progress = established.progress();
    if !progress.is_close() {
      progress = sequenced.settle(&mut established, sink).await?;
    }

    // Establishing hands the codec and its buffers on, so the Logon reply is
    // now the established session's to flush, not the handshake's. It goes
    // before `start`, because the peer is owed its answer ahead of anything
    // its own carried message provokes.
    flush(&mut io, established.pending_writes()).await?;

    if progress.is_close() || established.progress().is_close() {
      // Unlike the endpoint, the caller has already seen why: its own sink
      // received the events synchronously.
      return Err(Error::connection_failed(
        "session closed during the logon exchange",
      ));
    }

    // Nothing has been delivered to the sink from the session yet. This is
    // where a message the peer sent alongside its Logon arrives, and where an
    // application with more to set up than this one would do it first.
    let (driver, progress) =
      established.start(Instant::now(), &mut sequenced.sink(sink))?;
    SessionConnection::established(io, driver, sequenced, progress, sink).await
  }
}

fn driver_config(
  dicts: Arc<Dictionaries>,
  delimiter: Option<u8>,
) -> DriverConfig {
  DriverConfig {
    dicts,
    delimiter,
    clock: wall_clock,
    logon_timeout: LOGON_TIMEOUT,
    max_frame_len: babelfix_core::codec::DEFAULT_MAX_FRAME_LEN,
  }
}

impl<S: AsyncRead + AsyncWrite + Unpin> SessionConnection<S> {
  /// Initiate a session: send a Logon and wait for the peer's.
  pub async fn initiate(
    mut io: S,
    dicts: Arc<Dictionaries>,
    delimiter: Option<u8>,
    session_id: SessionIdentifier,
    setup: SessionSetup,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    let mut sequenced = setup.sequenced(&session_id);
    let mut handshake = InitiatorDriver::start(
      session_id,
      setup.config,
      setup.resume.next_in_seq_num,
      driver_config(dicts, delimiter),
      Instant::now(),
      &mut sequenced.sink(sink),
    )?;
    // Our Logon is persisted, then sent. Nothing about sending it can end the
    // session.
    let _ = sequenced.settle(&mut handshake, sink).await?;
    flush(&mut io, handshake.pending_writes()).await?;

    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let deadline = tokio::time::Instant::now() + LOGON_TIMEOUT;
    loop {
      read_more(&mut io, &mut buf, deadline).await?;
      let bytes = std::mem::take(&mut buf);
      buf = BytesMut::with_capacity(READ_CHUNK);

      if let Some(mut established) =
        handshake.on_bytes(Instant::now(), &bytes, &mut sequenced.sink(sink))?
      {
        let mut progress = established.progress();
        if !progress.is_close() {
          progress = sequenced.settle(&mut established, sink).await?;
        }
        flush(&mut io, established.pending_writes()).await?;
        if progress.is_close() || established.progress().is_close() {
          return Err(Error::connection_failed(
            "session closed during the logon exchange",
          ));
        }
        // As on the accepting side: the session delivers nothing until it is
        // started, and this is where anything carried with the peer's Logon
        // reply reaches the sink.
        let (driver, progress) =
          established.start(Instant::now(), &mut sequenced.sink(sink))?;
        return Self::established(io, driver, sequenced, progress, sink).await;
      }
    }
  }

  /// Finish setting up a session the peer has logged on to.
  async fn established(
    mut io: S,
    mut driver: Box<SessionDriver>,
    mut sequenced: Sequenced,
    progress: Progress,
    sink: &mut impl EventSink,
  ) -> Result<Self> {
    let progress = if progress.is_close() {
      progress
    } else {
      sequenced.complete_ready(&mut *driver, sink)?
    };
    flush(&mut io, driver.pending_writes()).await?;
    if progress.is_close() {
      return Err(Error::connection_failed(
        "session closed on the message carried with the logon",
      ));
    }
    Ok(Self {
      io,
      driver,
      sequenced,
      read_buf: BytesMut::with_capacity(READ_CHUNK),
      closed: false,
    })
  }

  /// Accept a session: wait for the peer's Logon and report who it claims to
  /// be, so the application can supply the session's setup.
  ///
  /// The identity comes out of the Logon, so it cannot be known before the
  /// first frame arrives — which is why accepting is two steps. Note there is
  /// no event sink here: until the session is named, there is nothing an event
  /// could be about.
  pub async fn accept(
    mut io: S,
    dicts: Arc<Dictionaries>,
    delimiter: Option<u8>,
  ) -> Result<PendingSession<S>> {
    let mut handshake =
      AcceptorDriver::new(driver_config(dicts, delimiter), Instant::now());

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

  pub fn config(&self) -> &SessionConfig {
    self.driver.config()
  }

  pub fn session_id(&self) -> &SessionIdentifier {
    self.driver.state().session_id()
  }

  /// Where the session's sequence numbers stand.
  pub fn status(&self) -> SessionStatus {
    SessionStatus {
      next_out_seq_num: self.sequenced.seq.next_out_seq_num(),
      next_in_seq_num: self.driver.state().next_in_seq_num(),
      watermark: self.sequenced.seq.watermark(),
    }
  }

  /// When [`step`](Self::step) will next act on time alone.
  pub fn deadline(&self) -> Option<Instant> {
    self.driver.next_deadline()
  }

  /// Send an application message: number it, start its write, and send it if
  /// the write has already completed — otherwise a later [`step`](Self::step)
  /// sends it. Returns its sequence number.
  ///
  /// If the session ends as a result — a completed write releasing a Logout
  /// — the next `step` reports it.
  pub async fn send(
    &mut self,
    msg: Message,
    sink: &mut impl EventSink,
  ) -> Result<u64> {
    let seq_num = self.sequenced.send(msg, sink)?;
    let _ = self.settle_ready(sink).await?;
    Ok(seq_num)
  }

  /// Send a message under the `MsgSeqNum` it carries, header as built. See
  /// [`SessionCommand::SendRaw`](crate::session::SessionCommand::SendRaw).
  pub async fn send_raw(
    &mut self,
    msg: Message,
    sink: &mut impl EventSink,
  ) -> Result<u64> {
    let seq_num = self.sequenced.send_raw(msg, sink)?;
    let _ = self.settle_ready(sink).await?;
    Ok(seq_num)
  }

  /// The application has finished with inbound `seq_num`, under
  /// [`InboundPolicy::Explicit`](crate::session::InboundPolicy::Explicit).
  pub async fn handled(
    &mut self,
    seq_num: u64,
    sink: &mut impl EventSink,
  ) -> Result<()> {
    self.sequenced.handled(seq_num, sink)?;
    let _ = self.settle_ready(sink).await?;
    Ok(())
  }

  /// Apply any other [`Command`] — replay, disconnect, and so on.
  pub async fn command(
    &mut self,
    cmd: Command,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let progress = self.driver.on_command(
      Instant::now(),
      cmd,
      &mut self.sequenced.sink(sink),
    )?;
    if progress.is_close() {
      self.flush().await?;
      return Ok(progress);
    }
    self.settle_ready(sink).await
  }

  /// Wait for the socket, a write completing, or the next deadline, whichever
  /// comes first, and process it.
  pub async fn step(&mut self, sink: &mut impl EventSink) -> Result<Progress> {
    if self.closed {
      return Ok(Progress::Close);
    }
    let deadline = self
      .deadline()
      .unwrap_or_else(|| Instant::now() + LOGON_TIMEOUT);

    let progress = tokio::select! {
      _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
        self.driver.on_tick(Instant::now(), &mut self.sequenced.sink(sink))?
      }
      done = self.sequenced.writes.next() => {
        self.sequenced.complete(done, &mut *self.driver, sink)?
      }
      read = self.io.read_buf(&mut self.read_buf) => {
        match read {
          Ok(0) => self.driver.on_peer_closed(&mut self.sequenced.sink(sink))?,
          Ok(_) => self.drain_read_buf(sink)?,
          Err(e) => return Err(Error::Io(e)),
        }
      }
    };

    if progress.is_close() {
      self.flush().await?;
      return Ok(progress);
    }
    self.settle_ready(sink).await
  }

  /// Drive the session until it ends.
  pub async fn run(&mut self, sink: &mut impl EventSink) -> Result<()> {
    while self.step(sink).await? == Progress::Continue {}
    Ok(())
  }

  /// Feed whatever is in the read buffer to the state machine.
  fn drain_read_buf(&mut self, sink: &mut impl EventSink) -> Result<Progress> {
    let bytes = std::mem::take(&mut self.read_buf);
    let progress = self.driver.on_bytes(
      Instant::now(),
      &bytes,
      &mut self.sequenced.sink(sink),
    )?;
    // `on_bytes` keeps any partial frame itself, so the buffer starts empty
    // again; reuse the allocation.
    self.read_buf = BytesMut::with_capacity(READ_CHUNK);
    Ok(progress)
  }

  /// Feed back the writes that have already completed, then write out what
  /// they released. Everything a pass produced reaches the peer before the
  /// next input is read, which is what keeps backpressure connected.
  async fn settle_ready(
    &mut self,
    sink: &mut impl EventSink,
  ) -> Result<Progress> {
    let progress = self.sequenced.complete_ready(&mut *self.driver, sink)?;
    self.flush().await?;
    self.closed |= progress.is_close();
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
