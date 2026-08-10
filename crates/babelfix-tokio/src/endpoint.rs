//! TCP endpoint layer: accept or initiate FIX connections.
//!
//! The endpoint owns the wire framing — an internal `tokio_util` codec that
//! splits the byte stream into [`FixMessage`]s and verifies
//! `BodyLength`/`CheckSum` — and spawns a [`session`] task per connection.
//!
//! * [`serve`] binds a listener and returns an [`Acceptor`]. Iterate its `events`
//!   receiver: reply to [`EndpointEvent::NewSession`] with a
//!   [`Session`](crate::session::Session) for the negotiated version, then take
//!   the [`SessionHandle`](crate::session::SessionHandle) from
//!   [`EndpointEvent::SessionConnected`].
//! * [`connect`] initiates an outbound connection (with reconnect and backoff)
//!   and returns an [`Initiator`], whose
//!   [`SessionHandle`](crate::session::SessionHandle) is available immediately —
//!   before the peer has been reached, let alone logged on.
//!
//! The asymmetry is deliberate. An acceptor serves many sessions and cannot
//! know a peer's identity until its Logon arrives, so it delivers each one
//! through an event. An initiator has exactly one session and was handed its
//! identity, so there is nothing to wait for — which is what lets a synchronous
//! front end call `connect` and wire up the async half afterwards.
//!
//! # Server
//!
//! ```no_run
//! use std::sync::Arc;
//! use babelfix_tokio::{endpoint, session, repository};
//! use futures::StreamExt;
//!
//! # async fn run() -> babelfix_tokio::Result<()> {
//! let repo = Arc::new(repository::orchestrate()?);
//! let mut endpoint = endpoint::serve(
//!     ("0.0.0.0", 9878),
//!     repo.clone(),
//!     endpoint::EndpointConfig::default(),
//! ).await?;
//!
//! while let Some(event) = endpoint.events.next().await {
//!     match event {
//!         endpoint::EndpointEvent::NewSession { session_id, response } => {
//!             // Answer with the sequence numbers you persisted for this peer.
//!             let session = repo
//!                 .get_version(&session_id.begin_string)
//!                 .map(session::Session::new)
//!                 .ok_or_else(|| babelfix_tokio::Error::unspecified(
//!                     "unknown FIX version",
//!                 ));
//!             let _ = response.send(session);
//!         }
//!         endpoint::EndpointEvent::SessionConnected(handle) => {
//!             // Drive `handle` — see the `session` module docs.
//!             tokio::spawn(async move { let _ = handle; });
//!         }
//!         endpoint::EndpointEvent::SessionInvalid(_peer) => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use futures::SinkExt;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use tracing::{Instrument, error, info, trace};

use super::*;

use std::time::Duration;

/// Everything an endpoint needs beyond the addresses and the repository.
///
/// The defaults are the values that used to be hardcoded, so
/// `EndpointConfig::default()` reproduces the previous behaviour.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct EndpointConfig {
  /// Field separator on the wire. `None` means SOH (`b'\x01'`), which is what
  /// production uses; `Some(b'|')` is convenient in tests and logs.
  pub delimiter: Option<u8>,

  /// How long a peer has to complete the logon exchange before the connection
  /// is dropped.
  pub logon_timeout: Duration,

  /// How long to wait for a TCP connection to be established. Initiator only.
  pub connect_timeout: Duration,

  /// How long to wait between connection attempts, in order. The ladder
  /// restarts for each host, and the host list is cycled indefinitely, so the
  /// last entry is the steady-state retry interval. Initiator only.
  pub backoff: Vec<Duration>,

  /// Depth of the per-session command and event channels.
  ///
  /// This is the application's queue: it is how far the session may run ahead
  /// of a slow consumer before backpressure reaches the peer.
  pub channel_depth: usize,
}

impl Default for EndpointConfig {
  fn default() -> Self {
    Self {
      delimiter: None,
      logon_timeout: Duration::from_secs(30),
      connect_timeout: Duration::from_secs(10),
      backoff: [10, 100, 1000, 5000, 5000]
        .into_iter()
        .map(Duration::from_millis)
        .collect(),
      channel_depth: 100,
    }
  }
}

impl EndpointConfig {
  pub fn delimiter(mut self, delimiter: u8) -> Self {
    self.delimiter = Some(delimiter);
    self
  }

  pub fn logon_timeout(mut self, timeout: Duration) -> Self {
    self.logon_timeout = timeout;
    self
  }

  pub fn connect_timeout(mut self, timeout: Duration) -> Self {
    self.connect_timeout = timeout;
    self
  }

  pub fn backoff(mut self, ladder: impl IntoIterator<Item = Duration>) -> Self {
    self.backoff = ladder.into_iter().collect();
    self
  }

  pub fn channel_depth(mut self, depth: usize) -> Self {
    self.channel_depth = depth;
    self
  }
}

pub enum EndpointEvent {
  NewSession {
    session_id: session::SessionIdentifier,
    response: oneshot::Sender<Result<session::Session>>,
  },
  SessionInvalid(String), // Partner address{
  SessionConnected(session::SessionHandle),
}

pub enum EndpointCommand {
  /// Stop accepting or reconnecting, and let the endpoint's task finish.
  Shutdown,
}

/// A running acceptor, as returned by [`serve`].
///
/// An acceptor serves many sessions and learns each peer's identity from its
/// Logon, which is why it hands them over through [`events`](Self::events)
/// rather than up front. An initiator has exactly one session and already knows
/// who it is talking to, so [`connect`] returns an [`Initiator`] instead, whose
/// handle is available immediately.
pub struct Acceptor {
  pub events: mpsc::Receiver<EndpointEvent>,
  pub commands: mpsc::Sender<EndpointCommand>,
  pub local_addr: std::net::SocketAddr,

  /// The accept loop.
  ///
  /// This is tokio's own handle rather than an opaque future, because the
  /// difference matters and hiding it helped nobody: **dropping a `JoinHandle`
  /// detaches the task, it does not stop it.** To stop accepting, send
  /// [`EndpointCommand::Shutdown`] or drop `commands`; to stop abruptly, call
  /// `abort()`. [`join`] awaits it and flattens tokio's
  /// `JoinError` into this crate's [`Error`].
  pub join_handle: tokio::task::JoinHandle<Result<()>>,
}

/// Adapts [`babelfix_core::codec::FixDecoder`] to [`tokio_util::codec`].
///
/// The framing itself lives in the core crate and is an ordinary synchronous
/// function; this exists only so `FramedRead` can drive it.
#[derive(Default)]
struct FixDecoder(babelfix_core::codec::FixDecoder);

impl tokio_util::codec::Decoder for FixDecoder {
  type Item = crate::message::FixMessage;
  type Error = Error;

  fn decode(
    &mut self,
    data: &mut bytes::BytesMut,
  ) -> std::result::Result<Option<Self::Item>, Self::Error> {
    self.0.decode(data)
  }
}

/// A session being initiated: the handle is available before the peer has
/// answered, or even been reached.
///
/// Unlike an acceptor, an initiator has exactly one session and was told its
/// identity up front, so there is nothing to wait for before handing the
/// [`SessionHandle`](session::SessionHandle) back. That matters for a
/// synchronous front end — a GUI thread, say — which can take the handle from
/// [`connect`] and wire up the async half afterwards.
pub struct Initiator {
  /// The session.
  ///
  /// Commands sent through `session.tx` before the peer has logged on are
  /// buffered and delivered once it has. Watch `session.events` for
  /// [`ConnectionEstablished`](session::SessionEvent::ConnectionEstablished)
  /// and [`Disconnected`](session::SessionEvent::Disconnected) to follow the
  /// connection's progress; dropping `session.tx` ends the session.
  pub session: session::SessionHandle,

  /// Stops the reconnect loop. Sending [`EndpointCommand::Shutdown`], or
  /// dropping this, gives up on connecting.
  pub commands: mpsc::Sender<EndpointCommand>,

  /// The connect-and-run task: it finishes when the session ends, or with the
  /// error that stopped one being established.
  ///
  /// As with [`Acceptor::join_handle`], dropping this detaches the task rather
  /// than stopping it — which for an initiator means it keeps trying to
  /// connect. Send [`EndpointCommand::Shutdown`] or drop `commands` to stop it.
  pub join_handle: tokio::task::JoinHandle<Result<()>>,
}

/// Initiate a session against the first of `endpoints` that answers, retrying
/// with backoff.
///
/// Returns as soon as the attempt is under way — before anything has been
/// connected — so the caller need not be async to get hold of the session.
pub fn connect(
  endpoints: Vec<(String, u16)>,
  repo: Arc<crate::repository::FixRepository>,
  session_id: session::SessionIdentifier,
  session: session::Session,
  config: EndpointConfig,
) -> Result<Initiator> {
  if endpoints.is_empty() {
    return Err(Error::connection_failed("no endpoints to connect to"));
  }

  let (command_sender, mut command_receiver) =
    mpsc::channel::<EndpointCommand>(config.channel_depth);

  // Both session channels exist before the socket does. There is exactly one
  // session and its identity is already known, so deferring these until logon
  // would buy nothing and would force every caller to be async.
  let (session_send, session_recv) =
    mpsc::channel::<session::SessionCommand>(config.channel_depth);
  let (session_event_sender, session_event_recv) =
    mpsc::channel::<session::SessionEvent>(config.channel_depth);

  let handle = session::SessionHandle {
    session_id: session_id.clone(),
    tx: session_send,
    events: session_event_recv,
  };

  let join_handle = tokio::spawn(async move {
    // Shutdown arrives on the same channel the acceptor uses, rather than a
    // separate cancellation token, so both endpoints are driven the same way.
    let mut shutdown = command_receiver.next();

    for (host, port) in endpoints.iter().cycle() {
      for backoff in config.backoff.iter().copied() {
        info!("Connecting to {}:{}", host, port);
        let connect_result = tokio::select! {
          _ = &mut shutdown => return Ok(()),
          result = tokio::time::timeout(
            config.connect_timeout,
            tokio::net::TcpStream::connect((host.as_str(), *port)),
          ) => result,
        };

        match connect_result {
          Ok(Ok(stream)) => {
            trace!("Connected to {host}:{port}; logging in");
            stream.set_nodelay(true)?;
            // TODO: we should maybe handle logon errors differently and retry
            // etc
            return initiate_connection(
              stream,
              repo,
              session_id,
              session,
              config,
              session_recv,
              session_event_sender,
            )
            .await;
          }
          Ok(Err(e)) => trace!("Failed to connect to {host}:{port}: {e}"),
          Err(_) => trace!("Failed to connect to {host}:{port}: timeout"),
        }

        tokio::select! {
          _ = &mut shutdown => return Ok(()),
          _ = tokio::time::sleep(backoff) => {}
        }
      }
    }

    Err(Error::connection_failed(format!(
      "Failed to connect to any endpoint: {:?}",
      endpoints
    )))
  });

  Ok(Initiator {
    session: handle,
    commands: command_sender,
    join_handle,
  })
}

async fn initiate_connection(
  mut stream: tokio::net::TcpStream,
  repo: Arc<crate::repository::FixRepository>,
  session_id: session::SessionIdentifier,
  session: session::Session,
  config: EndpointConfig,
  mut session_recv: mpsc::Receiver<session::SessionCommand>,
  session_event_sender: mpsc::Sender<session::SessionEvent>,
) -> Result<()> {
  let delimiter = config.delimiter;
  let (rx, tx) = stream.split();
  let mut rx = tokio_util::codec::FramedRead::new(
    rx,
    FixDecoder(babelfix_core::codec::FixDecoder::with_version(
      repo.clone(),
      delimiter,
      session.fix_version.clone(),
    )),
  );

  let span = tracing::info_span!(
    "ClientSession",
    local_comp_id = session_id.sender_comp_id,
    remote_comp_id = session_id.target_comp_id,
  );

  async {
    let mut out = session::PendingOutput::new(
      delimiter,
      session.time_precision,
      session_event_sender.clone(),
    );
    let handshake = babelfix_core::session::InitiatorHandshake::start(
      session_id,
      session,
      config.logon_timeout,
      std::time::Instant::now(),
      &mut out,
    )?;

    let mut runner = session::SessionRunner::new(tx, out);
    runner.flush().await?;

    // What to send, in what order, and what to make of the answer is the
    // handshake's business. This only supplies one frame and a deadline.
    let frame = tokio::select! {
      _ = tokio::time::sleep_until(
        tokio::time::Instant::from_std(handshake.deadline()),
      ) => {
        return Err(Error::connection_failed(format!(
          "Logon exchange timed out after {:?}", config.logon_timeout,
        )));
      }
      msg = rx.next() => match msg {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => {
          return Err(Error::connection_failed(format!(
            "Failed to read first message: {e}"
          )));
        }
        None => {
          return Err(Error::connection_failed(
            "Connection closed before first message",
          ));
        }
      },
    };

    let established = handshake.on_peer_logon(
      frame,
      std::time::Instant::now(),
      runner.output(),
    )?;
    runner.flush().await?;
    let mut state = established.state;
    let progress = established.progress;

    // Create a disconnecter to ensure we send a disconnect event when the
    // session is dropped
    let disconnector = Disconnector {
      session_event_sender: session_event_sender.clone(),
    };

    // A session refused during the exchange — a Logon whose sequence number is
    // too low, say — is still a session the application must hear about: the
    // Logout explaining why has already been emitted.
    let result = if progress.is_close() {
      Ok(())
    } else {
      runner.run(&mut state, &mut rx, &mut session_recv).await
    };
    drop(disconnector);
    result
  }
  .instrument(span)
  .await
}

struct Disconnector {
  session_event_sender: mpsc::Sender<session::SessionEvent>,
}

impl Drop for Disconnector {
  fn drop(&mut self) {
    // Send a last-ditch disconnect request and bail.
    self
      .session_event_sender
      .try_send(session::SessionEvent::Disconnected)
      .ok();
  }
}

async fn accept_connection(
  mut stream: tokio::net::TcpStream,
  mut event_sender: mpsc::Sender<EndpointEvent>,
  repo: Arc<crate::repository::FixRepository>,
  config: EndpointConfig,
) -> Result<()> {
  let delimiter = config.delimiter;
  info!("Handling new client connection");
  let partner = stream
    .peer_addr()
    .map_or("Unknown".to_string(), |addr| addr.to_string());
  let (rx, tx) = stream.split();
  let mut rx = tokio_util::codec::FramedRead::new(
    rx,
    FixDecoder(babelfix_core::codec::FixDecoder::new(
      repo.clone(),
      delimiter,
    )),
  );

  let (session_send, mut session_recv) =
    mpsc::channel::<session::SessionCommand>(config.channel_depth);
  let (session_event_sender, session_event_recv) =
    mpsc::channel::<session::SessionEvent>(config.channel_depth);

  let mut handshake = babelfix_core::session::AcceptorHandshake::new(
    config.logon_timeout,
    std::time::Instant::now(),
  );

  // Nothing session-scoped can happen until the peer's Logon names a session,
  // because every session on this port shares the listener. Read exactly one
  // frame; note there is no event sink here, because there is as yet no session
  // for an event to be about.
  let logon_frame = tokio::select! {
    _ = tokio::time::sleep(config.logon_timeout) => {
      return Err(Error::connection_failed(format!(
        "Logon exchange timed out after {:?}", config.logon_timeout,
      )));
    },
    msg_event = rx.next() => {
      match msg_event {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => {
          return Err(Error::connection_failed(format!(
            "Failed to read first message: {e}"
          )));
        }
        None => {
          event_sender
            .send(EndpointEvent::SessionInvalid(partner))
            .await
            .map_err(crate::chan_closed)?;
          return Err(Error::connection_failed(
            "Connection closed before first message",
          ));
        }
      }
    },
  };

  // A first frame that is not a Logon is refused here, before the application
  // is told anything about it. It used to be validated *after* `NewSession` and
  // `SessionConnected` had already gone out, so an application would do its
  // persisted-state lookup, and be handed a session handle, for a connection
  // about to be dropped.
  let session_id = match handshake.identify(logon_frame) {
    Ok(id) => id.clone(),
    Err(e) => {
      // There is no session to report this against — that is the whole point —
      // so an invalid peer is named by its address instead.
      event_sender
        .send(EndpointEvent::SessionInvalid(partner))
        .await
        .map_err(crate::chan_closed)?;
      return Err(e);
    }
  };

  let (set_session, get_session) =
    oneshot::channel::<Result<session::Session>>();
  event_sender
    .send(EndpointEvent::NewSession {
      session_id: session_id.clone(),
      response: set_session,
    })
    .await
    .map_err(crate::chan_closed)?;
  let session = get_session.await.map_err(crate::chan_closed)??;

  let span = tracing::info_span!(
    "ServerSession",
    local_comp_id = session_id.sender_comp_id,
    remote_comp_id = session_id.target_comp_id,
  );

  async {
    // Create a disconnecter to ensure we send a disconnect event when the
    // session is dropped
    let disconnector = Disconnector {
      session_event_sender: session_event_sender.clone(),
    };

    // The session decides the precision of every timestamp from here on,
    // including the Logon reply the handshake is about to send.
    let mut out = session::PendingOutput::new(
      delimiter,
      session.time_precision,
      session_event_sender.clone(),
    );
    let established =
      handshake.accept(session, std::time::Instant::now(), &mut out)?;

    let mut runner = session::SessionRunner::new(tx, out);
    let mut state = established.state;
    let progress = established.progress;

    // The handle is published *before* the first flush. Until the application
    // holds the receiver, nothing is draining the event channel, so a session
    // that emits more events during its logon exchange than the channel is deep
    // would block forever waiting for a reader that does not exist yet.
    //
    // Safe to do this early now that the handshake validated the peer's Logon:
    // by this point there genuinely is a session, even if it is one that is
    // about to be logged out.
    let session_handle = session::SessionHandle {
      session_id,
      tx: session_send,
      events: session_event_recv,
    };
    event_sender
      .send(EndpointEvent::SessionConnected(session_handle))
      .await
      .map_err(crate::chan_closed)?;

    runner.flush().await?;

    // A session refused during the exchange — a Logon whose sequence number is
    // too low, say — is still a session the application must hear about: the
    // Logout explaining why has already been emitted.
    let result = if progress.is_close() {
      Ok(())
    } else {
      runner.run(&mut state, &mut rx, &mut session_recv).await
    };
    drop(disconnector);
    result
  }
  .instrument(span)
  .await
}

/// Accept FIX connections on `addr`.
///
/// Answer each [`EndpointEvent::NewSession`] with the sequence numbers you have
/// persisted for that peer, then take the
/// [`SessionHandle`](session::SessionHandle) from
/// [`EndpointEvent::SessionConnected`].
pub async fn serve(
  addr: impl tokio::net::ToSocketAddrs,
  repo: Arc<crate::repository::FixRepository>,
  config: EndpointConfig,
) -> Result<Acceptor> {
  let (event_sender, event_receiver) =
    mpsc::channel::<EndpointEvent>(config.channel_depth);
  let (command_sender, mut command_receiver) =
    mpsc::channel::<EndpointCommand>(config.channel_depth);

  let listener = tokio::net::TcpListener::bind(addr).await?;

  let local_addr = listener.local_addr()?;

  let join_handle = tokio::spawn(async move {
    loop {
      tokio::select! {
        maybe_command = command_receiver.next() => {
          match maybe_command {
            Some(EndpointCommand::Shutdown) => {
              info!("Shutting down server");
              break;
            },
            None => {
              info!("Command channel closed, shutting down server");
              break;
            },
          }
        }
        maybe_socket = listener.accept() => {
          match maybe_socket {
            Ok((stream, sockaddr)) => {
              stream.set_nodelay(true)?;
              let s = tracing::info_span!("ClientConnection", %sockaddr);
              let event_sender = event_sender.clone();
              let repo = repo.clone();
              let config = config.clone();
              tokio::spawn(crate::util::wrap_and_report(async move {
                accept_connection(
                  stream,
                  event_sender,
                  repo,
                  config).instrument(s).await
              }));
            },
            Err(e) => {
              error!("Failed to accept TCP connection: {e}");
              continue;
            }
          }
        }
      }
    }
    Ok(())
  });

  Ok(Acceptor {
    commands: command_sender,
    events: event_receiver,
    local_addr,
    join_handle,
  })
}

/// Await an endpoint's task, flattening tokio's `JoinError` into [`Error`].
///
/// [`Acceptor`] and [`Initiator`] hand out tokio's `JoinHandle` directly so
/// that `abort()` and detach-on-drop behave the way a tokio user expects. This
/// is the convenience for the common case of only wanting to know how it ended.
/// `what` names the endpoint in the panic message.
///
/// ```no_run
/// # async fn run(acceptor: babelfix_tokio::endpoint::Acceptor)
/// #   -> babelfix_tokio::Result<()> {
/// babelfix_tokio::endpoint::join(acceptor.join_handle, "server")
///   .await
/// # }
/// ```
pub async fn join(
  handle: tokio::task::JoinHandle<Result<()>>,
  what: &'static str,
) -> Result<()> {
  match handle.await {
    Ok(Ok(())) => Ok(()),
    Ok(Err(e)) => Err(e),
    Err(e) => Err(Error::unspecified(format!("{what} task panicked: {e:?}"))),
  }
}
