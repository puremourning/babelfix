//! TCP endpoint layer: accept or initiate FIX connections.
//!
//! The endpoint owns the wire framing — an internal `tokio_util` codec that
//! splits the byte stream into [`FixMessage`]s and verifies
//! `BodyLength`/`CheckSum` — and spawns a [`session`] task per connection.
//!
//! * [`serve`] binds a listener and returns an [`Endpoint`]. Iterate its `events`
//!   receiver: reply to [`EndpointEvent::NewSession`] with a
//!   [`Session`](crate::session::Session) for the negotiated version, then take
//!   the [`SessionHandle`](crate::session::SessionHandle) from
//!   [`EndpointEvent::SessionConnected`].
//! * [`connect`] initiates an outbound connection (with reconnect and backoff),
//!   emitting the same [`EndpointEvent::SessionConnected`] once logged on.
//!
//! Both take an `Arc<`[`FixRepository`](crate::repository::FixRepository)`>` and
//! an [`EndpointConfig`], and both return an [`Endpoint`]: the same events, the
//! same [`EndpointCommand::Shutdown`], the same awaitable `join_handle`.
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
use crate::repository::FieldBlock;
use babelfix_core::session::SessionState;

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

/// A running endpoint, whether it accepts connections or initiates them.
///
/// Both [`serve`] and [`connect`] return one of these: the same events, the
/// same shutdown command, the same awaitable completion. The only asymmetry
/// left is [`local_addr`](Self::local_addr), which an initiator does not have.
pub struct Endpoint<T: Future<Output = Result<()>> + Send + 'static> {
  pub events: mpsc::Receiver<EndpointEvent>,
  pub commands: mpsc::Sender<EndpointCommand>,
  /// The address bound, for an acceptor. `None` for an initiator.
  pub local_addr: Option<std::net::SocketAddr>,
  pub join_handle: T,
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

/// Initiate a session against the first of `endpoints` that answers, retrying
/// with backoff.
///
/// Returns as soon as the attempt is under way; watch `events` for
/// [`EndpointEvent::SessionConnected`], and send [`EndpointCommand::Shutdown`]
/// (or drop the `commands` sender) to stop. Awaiting `join_handle` waits for the
/// session to finish.
pub fn connect(
  endpoints: Vec<(String, u16)>,
  repo: Arc<crate::repository::FixRepository>,
  session_id: session::SessionIdentifier,
  session: session::Session,
  config: EndpointConfig,
) -> Result<Endpoint<impl Future<Output = Result<()>> + Send + 'static>> {
  if endpoints.is_empty() {
    return Err(Error::connection_failed("no endpoints to connect to"));
  }

  let (event_sender, event_receiver) =
    mpsc::channel::<EndpointEvent>(config.channel_depth);
  let (command_sender, mut command_receiver) =
    mpsc::channel::<EndpointCommand>(config.channel_depth);

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
              event_sender,
              repo,
              session_id,
              session,
              config,
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

  Ok(Endpoint {
    events: event_receiver,
    commands: command_sender,
    local_addr: None,
    join_handle: resolve_join_handle(join_handle, "Initiator"),
  })
}

/// Flatten a `JoinHandle` into the endpoint's own error type.
async fn resolve_join_handle(
  handle: tokio::task::JoinHandle<Result<()>>,
  what: &'static str,
) -> Result<()> {
  match handle.await {
    Ok(Ok(())) => Ok(()),
    Ok(Err(e)) => Err(e),
    Err(e) => Err(Error::unspecified(format!("{what} task panicked: {e:?}"))),
  }
}

/// Build a Logon carrying the session's negotiated settings.
pub(crate) fn logon_message(
  fix_version: &Arc<crate::repository::FixVersion>,
  session: &session::Session,
) -> Result<crate::message::builder::Message> {
  let mut logon_msg =
    crate::message::builder::Message::new(fix_version.clone(), "A")?;

  logon_msg.body.set_tag(
    crate::schema::FIX_Latest::Fields::HeartBtInt,
    session.heartbeat_interval.as_secs().to_string(),
  );
  if logon_msg.fix_message.is_member(
    fix_version.as_ref(),
    crate::schema::FIX_Latest::Fields::DefaultApplVerID,
  ) {
    logon_msg.body.set_tag(
      crate::schema::FIX_Latest::Fields::DefaultApplVerID,
      "10", // TODO: Default application version: FIXLatest
    );
  }
  logon_msg.body.set_tag(
    crate::schema::FIX_Latest::Fields::EncryptMethod,
    "0", // No encryption
  );
  Ok(logon_msg)
}

async fn initiate_connection(
  mut stream: tokio::net::TcpStream,
  mut event_sender: mpsc::Sender<EndpointEvent>,
  repo: Arc<crate::repository::FixRepository>,
  session_id: session::SessionIdentifier,
  session: session::Session,
  config: EndpointConfig,
) -> Result<()> {
  let delimiter = config.delimiter;
  let peer_addr = stream
    .peer_addr()
    .map_or("Unknown".to_string(), |addr| addr.to_string());
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
    let (session_send, mut session_recv) =
      mpsc::channel::<session::SessionCommand>(config.channel_depth);
    let (session_event_sender, session_event_recv) =
      mpsc::channel::<session::SessionEvent>(config.channel_depth);

    let logon_message = logon_message(&session.fix_version, &session)?;
    let state = SessionState::new(
      session_id.clone(),
      session,
      std::time::Instant::now(),
    );
    let mut runner = session::SessionRunner::new(
      state,
      tx,
      session_event_sender.clone(),
      delimiter,
    );

    runner
      .emit(session::SessionEvent::ConnectionEstablished)
      .await?;
    runner.send_logon(logon_message).await?;

    // First message has to be a logon
    let logon_fix_msg = tokio::select! {
      _ = tokio::time::sleep(config.logon_timeout) => {
        return Err(Error::connection_failed(format!(
          "Logon exchange timed out after {:?}", config.logon_timeout,
        )));
      },
      msg_event = rx.next() => {
        match msg_event {
          Some(Ok(msg)) => msg,
          Some(Err(e)) => {
            return Err(Error::connection_failed(format!("Failed to read first message: {e}")));
          }
          None => {
            event_sender.send(EndpointEvent::SessionInvalid(
              peer_addr
            )).await.map_err(crate::chan_closed)?;
            return Err(Error::connection_failed("Connection closed before first message"));
          }
        }
      },
    };

    let logon_msg =
      crate::message::builder::Message::from_message(&logon_fix_msg)?;

    let session_snapshot = runner.state().session().clone();
    runner
      .emit(session::SessionEvent::RawMessageReceived(
        logon_fix_msg,
        session_snapshot,
      ))
      .await?;

    let session_handle = session::SessionHandle {
      session_id: session_id.clone(),
      tx: session_send,
      events: session_event_recv,
    };

    event_sender
      .send(EndpointEvent::SessionConnected(session_handle))
      .await.map_err(crate::chan_closed)?;

    // Create a disconnecter to ensure we send a disconnect event when the
    // session is dropped
    let disconnector = Disconnector {
      session_event_sender: session_event_sender.clone(),
    };

    if logon_msg.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon_msg
      )));
    }

    let result = runner.run(&mut rx, &mut session_recv, logon_msg).await;
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
  let (mut session_event_sender, session_event_recv) =
    mpsc::channel::<session::SessionEvent>(config.channel_depth);

  // First message has to be a logon
  let logon_fix_msg = tokio::select! {
    _ = tokio::time::sleep(config.logon_timeout) => {
      return Err(Error::connection_failed(format!(
          "Logon exchange timed out after {:?}", config.logon_timeout,
        )));
    },
    msg_event = rx.next() => {
      match msg_event {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => {
          return Err(Error::connection_failed(format!("Failed to read first message: {e}")));
        }
        None => {
          event_sender.send(EndpointEvent::SessionInvalid(partner)).await.map_err(crate::chan_closed)?;
          return Err(Error::connection_failed("Connection closed before first message"));
        }
      }
    },
  };

  session_event_sender
    .send(session::SessionEvent::ConnectionEstablished)
    .await
    .map_err(crate::chan_closed)?;

  // Create a disconnecter to ensure we send a disconnect event when the
  // session is dropped
  let disconnector = Disconnector {
    session_event_sender: session_event_sender.clone(),
  };

  let logon_msg =
    crate::message::builder::Message::from_message(&logon_fix_msg)?;

  let session_id = session::SessionIdentifier {
    begin_string: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::BeginString)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing BeginString")
      })?
      .as_string(),
    sender_comp_id: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::TargetCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing TargetCompID")
      })?
      .as_string(),
    target_comp_id: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::SenderCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing SenderCompID")
      })?
      .as_string(),
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

  session_event_sender
    .send(session::SessionEvent::RawMessageReceived(
      logon_fix_msg,
      session.clone(),
    ))
    .await
    .map_err(crate::chan_closed)?;

  let span = tracing::info_span!(
    "ServerSession",
    local_comp_id = session_id.sender_comp_id,
    remote_comp_id = session_id.target_comp_id,
  );

  async {
    let session_handle = session::SessionHandle {
      session_id: session_id.clone(),
      tx: session_send,
      events: session_event_recv,
    };

    event_sender
      .send(EndpointEvent::SessionConnected(session_handle))
      .await
      .map_err(crate::chan_closed)?;

    if logon_msg.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon_msg
      )));
    }

    let logon_message = logon_message(&session.fix_version, &session)?;
    let state =
      SessionState::new(session_id.clone(), session, std::time::Instant::now());
    let mut runner =
      session::SessionRunner::new(state, tx, session_event_sender, delimiter);

    runner.send_logon(logon_message).await?;

    let result = runner.run(&mut rx, &mut session_recv, logon_msg).await;
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
) -> Result<Endpoint<impl Future<Output = Result<()>> + Send + 'static>> {
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

  Ok(Endpoint {
    commands: command_sender,
    events: event_receiver,
    local_addr: Some(local_addr),
    join_handle: resolve_join_handle(join_handle, "Server"),
  })
}
