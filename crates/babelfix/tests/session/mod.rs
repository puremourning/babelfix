//! Loopback harness for the session-layer integration tests.
//!
//! [`establish`] wires a real babelfix initiator to a real babelfix acceptor
//! over an ephemeral loopback port and hands back a [`Client`] and a [`Server`],
//! each exposing the session's [`fix::session::SessionEvent`] stream via
//! [`Session::next_event`].
//!
//! Tests that need to be a *non-babelfix* counterparty — sending messages
//! babelfix would never generate, or observing exactly what appears on the wire
//! — use [`raw::RawPeer`] against a [`Server`] created by [`serve`].

use super::fix;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use googletest::matcher::MatcherResult;
use googletest::prelude::*;

pub mod raw;

pub static FIX_REPO: std::sync::LazyLock<Arc<fix::repository::FixRepository>> =
  std::sync::LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

/// How long any single `expect`-style helper waits before giving up. Generous
/// enough to absorb a loaded CI machine, short enough that a genuinely stuck
/// session fails the test rather than the harness timeout.
pub const EVENT_TIMEOUT: std::time::Duration =
  std::time::Duration::from_secs(5);

/// Starting state for one side of a session under test.
///
/// `Default` is a fresh session (both sequence numbers at 1) with a heartbeat
/// interval long enough that no heartbeat fires during a test. Timing tests
/// override `heartbeat_interval` with something in the low hundreds of
/// milliseconds.
#[derive(Debug, Clone)]
pub struct SessionOptions {
  pub next_out_seq_num: u32,
  pub next_in_seq_num: u32,
  pub heartbeat_interval: std::time::Duration,
  pub time_precision: fix::time::TimePrecision,
}

impl Default for SessionOptions {
  fn default() -> Self {
    Self {
      next_out_seq_num: 1,
      next_in_seq_num: 1,
      heartbeat_interval: std::time::Duration::from_secs(30),
      time_precision: fix::time::TimePrecision::default(),
    }
  }
}

impl SessionOptions {
  /// Start mid-session, as if recovering persisted state.
  pub fn at(next_out_seq_num: u32, next_in_seq_num: u32) -> Self {
    Self {
      next_out_seq_num,
      next_in_seq_num,
      ..Default::default()
    }
  }

  pub fn heartbeat(mut self, interval: std::time::Duration) -> Self {
    self.heartbeat_interval = interval;
    self
  }

  fn into_session(
    self,
    fix_version: Arc<fix::repository::FixVersion>,
  ) -> fix::session::Session {
    fix::session::Session {
      fix_version,
      next_in_seq_num: self.next_in_seq_num,
      next_out_seq_num: self.next_out_seq_num,
      heartbeat_interval: self.heartbeat_interval,
      time_precision: self.time_precision,
    }
  }
}

pub struct Session {
  pub handle: fix::session::SessionHandle,
  pub events: VecDeque<fix::session::SessionEvent>,
  pub state: fix::session::Session,
  cancellation_token: tokio_util::sync::CancellationToken,
}

impl std::fmt::Debug for Session {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Session")
      .field("sender_comp_id", &self.handle.session_id.sender_comp_id)
      .field("target_comp_id", &self.handle.session_id.target_comp_id)
      .finish()
  }
}

impl Session {
  /// Return the next event, discarding any that match one of `skipping`.
  ///
  /// Every event seen — skipped or not — is appended to `self.events`, so a
  /// test can inspect the full history afterwards.
  pub async fn next_event(
    &mut self,
    skipping: &[&dyn for<'a> Matcher<&'a fix::session::SessionEvent>],
  ) -> anyhow::Result<fix::session::SessionEvent> {
    let deadline = std::time::Instant::now() + EVENT_TIMEOUT;
    let timeout = tokio::time::sleep_until(deadline.into());
    tokio::pin!(timeout);

    'outer: loop {
      tokio::select! {
        _ = &mut timeout => {
          anyhow::bail!(
            "Timed out waiting for event. Events seen so far: {:#?}",
            self.events);
        }
        event = self.handle.events.recv() => {
          let event = match event {
            Ok(event) => event,
            Err(e) => {
              anyhow::bail!("Session event channel died: {e}");
            }
          };
          self.events.push_back(event.clone());
          for skip in skipping {
            if let MatcherResult::Match = skip.matches(&event)
            {
              continue 'outer;
            }
          }
          return Ok(event);
        }
        _ = self.cancellation_token.cancelled() => {
          anyhow::bail!("Cancelled waiting for event");
        }
      };
    }
  }

  /// Consume events until one matches `matcher`, and return it.
  ///
  /// Use this only where the session genuinely has no defined ordering between
  /// the event of interest and the events preceding it — for example when
  /// waiting for a peer-driven event that races with locally generated
  /// traffic. Prefer [`Session::next_event`] elsewhere: it asserts the order as
  /// well as the content.
  pub async fn next_event_matching(
    &mut self,
    matcher: &dyn for<'a> Matcher<&'a fix::session::SessionEvent>,
  ) -> anyhow::Result<fix::session::SessionEvent> {
    let deadline = std::time::Instant::now() + EVENT_TIMEOUT;
    let timeout = tokio::time::sleep_until(deadline.into());
    tokio::pin!(timeout);

    loop {
      tokio::select! {
        _ = &mut timeout => {
          anyhow::bail!(
            "Timed out waiting for a matching event. Events seen so far: {:#?}",
            self.events);
        }
        event = self.handle.events.recv() => {
          let event = match event {
            Ok(event) => event,
            Err(e) => anyhow::bail!("Session event channel died: {e}"),
          };
          self.events.push_back(event.clone());
          if let MatcherResult::Match = matcher.matches(&event) {
            return Ok(event);
          }
        }
        _ = self.cancellation_token.cancelled() => {
          anyhow::bail!("Cancelled waiting for event");
        }
      };
    }
  }

  /// Assert that no event matching `matcher` arrives within `window`.
  ///
  /// Non-matching events are consumed and recorded as usual, so this doubles as
  /// a way to drain a quiet period.
  pub async fn expect_no_event(
    &mut self,
    window: std::time::Duration,
    matcher: &dyn for<'a> Matcher<&'a fix::session::SessionEvent>,
  ) -> anyhow::Result<()> {
    let timeout = tokio::time::sleep(window);
    tokio::pin!(timeout);

    loop {
      tokio::select! {
        _ = &mut timeout => return Ok(()),
        event = self.handle.events.recv() => {
          let Ok(event) = event else {
            // The session ended; nothing further can arrive.
            return Ok(());
          };
          self.events.push_back(event.clone());
          if let MatcherResult::Match = matcher.matches(&event) {
            anyhow::bail!("Unexpected event: {event:?}");
          }
        }
        _ = self.cancellation_token.cancelled() => return Ok(()),
      };
    }
  }

  /// Consume events up to and including `RecoveryCompleted`.
  ///
  /// Every connection begins with a TestRequest/Heartbeat exchange that
  /// confirms both peers are synchronised, so tests interested in what happens
  /// *after* logon start by settling both sides.
  pub async fn settle(&mut self) -> anyhow::Result<()> {
    loop {
      match self.next_event(&[]).await? {
        fix::session::SessionEvent::RecoveryCompleted => return Ok(()),
        fix::session::SessionEvent::Disconnected => {
          anyhow::bail!(
            "Session disconnected before recovery completed. Events: {:#?}",
            self.events
          );
        }
        _ => {}
      }
    }
  }

  /// Send a command to the session under test.
  pub async fn command(
    &mut self,
    command: fix::session::SessionCommand,
  ) -> anyhow::Result<()> {
    use futures::SinkExt;
    self.handle.tx.send(command).await?;
    Ok(())
  }
}

pub struct Server {
  commands: futures::channel::mpsc::Sender<fix::endpoint::EndpointCommand>,
  configured_sessions:
    Vec<(fix::session::SessionIdentifier, fix::session::Session)>,
  live_sessions: Vec<Session>,
  /// Peers rejected before a session could be identified, by peer address.
  invalid_sessions: Vec<String>,
  sessions_cancel: tokio_util::sync::CancellationToken,
  pub local_addr: std::net::SocketAddr,
}

impl Server {
  pub fn push_session(
    &mut self,
    session_id: fix::session::SessionIdentifier,
    session: fix::session::Session,
  ) {
    self.configured_sessions.push((session_id, session));
  }

  pub fn session(
    &mut self,
    session_id: &fix::session::SessionIdentifier,
  ) -> Option<&mut Session> {
    self
      .live_sessions
      .iter_mut()
      .find(|session| session.handle.session_id == *session_id)
  }

  pub fn invalid_sessions(&self) -> &[String] {
    &self.invalid_sessions
  }

  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    use futures::SinkExt;
    self
      .commands
      .send(fix::endpoint::EndpointCommand::Shutdown)
      .await?;
    Ok(())
  }

  pub async fn new(port: u16) -> anyhow::Result<Arc<Mutex<Self>>> {
    let fix::endpoint::Endpoint {
      commands,
      mut events,
      local_addr,
      join_handle,
    } = fix::endpoint::serve(
      ("127.0.0.1", port),
      Arc::clone(&FIX_REPO),
      fix::endpoint::EndpointConfig::default(),
    )
    .await?;

    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let server = Arc::new(Mutex::new(Self {
      commands,
      sessions_cancel: cancellation_token.clone(),
      configured_sessions: Vec::new(),
      live_sessions: Vec::new(),
      invalid_sessions: Vec::new(),
      local_addr,
    }));

    {
      let server = Arc::clone(&server);
      let server_cancellation_token = cancellation_token.clone();

      tokio::spawn(fix::util::wrap_and_report(async move {
        loop {
          tokio::select! {
            event = events.recv() => {
              let event = match event {
                Ok(event) => event,
                Err(e) => {
                  return Err(fix::Error::connection_failed(format!("Server event channel died: {e}")));
                }
              };
              match event {
                babelfix::endpoint::EndpointEvent::NewSession { session_id, response } => {
                  let server = server.lock().await;
                  let session = server.configured_sessions.iter().find_map(|(id, session)| {
                    if *id == session_id {
                      Some(session.clone())
                    } else {
                      None
                    }
                  });
                  let session =
                    session.ok_or_else(|| fix::Error::connection_failed(
                        format!("No configured session for {:?}", session_id)));
                  response.send(session).unwrap();
                }
                babelfix::endpoint::EndpointEvent::SessionInvalid(peer) => {
                  server.lock().await.invalid_sessions.push(peer);
                }
                babelfix::endpoint::EndpointEvent::SessionConnected(session_handle) => {
                  let mut server = server.lock().await;
                  let state = server.configured_sessions.iter().find_map(|(id, session)| {
                    if *id == session_handle.session_id {
                      Some(session.clone())
                    } else {
                      None
                    }
                  }).unwrap();
                  let session = Session {
                    handle: session_handle,
                    events: VecDeque::new(),
                    state,
                    cancellation_token: server.sessions_cancel.clone(),
                  };
                  server.live_sessions.push(session);
                }
              }
            }
            _ = server_cancellation_token.cancelled() => {
              break;
            }
          }
        }
        join_handle.await?;
        Ok(())
      }))
    };

    Ok(server)
  }
}

pub struct Client {
  /// The initiator's command channel. Dropping it, or sending `Shutdown`,
  /// stops the reconnect loop — the same lever the acceptor uses.
  cancel_signal: futures::channel::mpsc::Sender<fix::endpoint::EndpointCommand>,
  cancellation_token: tokio_util::sync::CancellationToken,
  pub session: Session,
}

impl Client {
  pub async fn new(
    port: u16,
    session_id: fix::session::SessionIdentifier,
    state: fix::session::Session,
  ) -> anyhow::Result<Self> {
    let cancellation_token = tokio_util::sync::CancellationToken::new();

    let fix::endpoint::Initiator {
      session: session_handle,
      commands: cancel_client,
      ..
    } = fix::endpoint::connect(
      vec![("127.0.0.1".to_string(), port)],
      Arc::clone(&FIX_REPO),
      session_id,
      state.clone(),
      fix::endpoint::EndpointConfig::default(),
    )?;

    // No waiting: the handle is live from the moment `connect` returns, and the
    // connection announces itself with `ConnectionEstablished`, which is the
    // first event a test sees. Consuming it here would hide it from them.
    let session = Session {
      handle: session_handle,
      events: VecDeque::new(),
      state,
      cancellation_token: cancellation_token.clone(),
    };

    Ok(Self {
      cancel_signal: cancel_client,
      session,
      cancellation_token,
    })
  }
}

impl Drop for Server {
  fn drop(&mut self) {
    self.sessions_cancel.cancel();
  }
}

impl Drop for Client {
  fn drop(&mut self) {
    self.cancellation_token.cancel();
    // Closing the command channel is what stops the initiator.
    self.cancel_signal.close_channel();
  }
}

/// Wait for the acceptor to register a live session under `session_id`.
///
/// The acceptor publishes a session asynchronously once the peer has logged
/// on, so a test driving a [`raw::RawPeer`] has no natural synchronisation
/// point before its first assertion. Returns `server` so this composes into an
/// expression.
pub async fn wait_for_session<'a>(
  server: &'a Arc<Mutex<Server>>,
  session_id: &fix::session::SessionIdentifier,
) -> anyhow::Result<&'a Arc<Mutex<Server>>> {
  let deadline = std::time::Instant::now() + EVENT_TIMEOUT;
  loop {
    if server.lock().await.session(session_id).is_some() {
      return Ok(server);
    }
    anyhow::ensure!(
      std::time::Instant::now() < deadline,
      "Timed out waiting for the acceptor to publish session {session_id:?}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
  }
}

/// Start an acceptor on an ephemeral port with one session pre-configured.
///
/// Returns the acceptor's view of the session identifier (its own CompID is the
/// sender) alongside the server and the port to connect to. Use this when the
/// counterparty is a [`raw::RawPeer`] rather than a babelfix [`Client`].
pub async fn serve(
  server_comp_id: impl Into<String>,
  server_options: SessionOptions,
  client_comp_id: impl Into<String>,
  fix_version: Arc<fix::repository::FixVersion>,
) -> anyhow::Result<(fix::session::SessionIdentifier, Arc<Mutex<Server>>, u16)>
{
  let server = Server::new(0).await?;
  let port = server.lock().await.local_addr.port();

  let server_session_id = fix::session::SessionIdentifier {
    begin_string: fix_version.begin_string.clone(),
    sender_comp_id: server_comp_id.into(),
    target_comp_id: client_comp_id.into(),
  };

  server.lock().await.push_session(
    server_session_id.clone(),
    server_options.into_session(fix_version),
  );

  tracing::info!("Server listening on port {port}");

  Ok((server_session_id, server, port))
}

/// Connect a babelfix initiator to a babelfix acceptor over loopback.
///
/// Returns each side's session identifier together with its driver. The
/// returned identifiers are from the owner's point of view, so the client's
/// `sender_comp_id` is `client_comp_id` and the server's is `server_comp_id`.
pub async fn establish(
  client_comp_id: impl Into<String>,
  client_options: SessionOptions,
  server_comp_id: impl Into<String>,
  server_options: SessionOptions,
  fix_version: Arc<fix::repository::FixVersion>,
) -> anyhow::Result<(
  (fix::session::SessionIdentifier, Client),
  (fix::session::SessionIdentifier, Arc<Mutex<Server>>),
)> {
  let client_comp_id = client_comp_id.into();
  let server_comp_id = server_comp_id.into();

  let (server_session_id, server, port) = serve(
    server_comp_id.clone(),
    server_options,
    client_comp_id.clone(),
    fix_version.clone(),
  )
  .await?;

  let client_session_id = fix::session::SessionIdentifier {
    begin_string: fix_version.begin_string.clone(),
    sender_comp_id: client_comp_id,
    target_comp_id: server_comp_id,
  };

  tracing::info!("Starting client on port {port}");
  let client = Client::new(
    port,
    client_session_id.clone(),
    client_options.into_session(fix_version),
  )
  .await?;

  tracing::info!("Client connected to server on port {port}");

  Ok(((client_session_id, client), (server_session_id, server)))
}

/// Assert that the next event from a session matches a `matches_pattern!`
/// pattern.
///
/// Four forms, for the two session flavours (a `Server`, which owns many
/// sessions and is behind a mutex, and a `Client`, which owns exactly one):
///
/// ```ignore
/// expect_event!(server(session_id) << SessionEvent::ConnectionEstablished);
/// expect_event!(server(session_id) ignoring_state << SessionEvent::RecoveryCompleted);
/// expect_event!(client << SessionEvent::ConnectionEstablished);
/// expect_event!(client skipping [ SessionEvent::SessionState(anything()) ]
///               << SessionEvent::RecoveryCompleted);
/// ```
///
/// `ignoring_state` is shorthand for skipping `SessionState`, which the session
/// emits on its own schedule and which is therefore almost never what a test
/// wants to synchronise on.
macro_rules! expect_event {
  ( $server:ident ( $session_id:expr ) << $($event:tt)+ ) => {
    let event = crate::session::wait_for_session(&$server, &$session_id)
      .await?
      .lock()
      .await
      .session(&$session_id)
      .ok_or_else(|| anyhow::anyhow!("Session not found"))?
      .next_event(&[])
      .await?;
     verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
       anyhow::anyhow!("Expected server event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $server:ident ( $session_id:expr ) skipping [ $($($skip:tt)+),* ] << $($event:tt)+ ) => {
    let event = crate::session::wait_for_session(&$server, &$session_id)
      .await?
      .lock()
      .await
      .session(&$session_id)
      .ok_or_else(|| anyhow::anyhow!("Session not found"))?
      .next_event(&[$(&matches_pattern!($($skip)+)),*])
      .await?;
     verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
       anyhow::anyhow!("Expected server event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $server:ident ( $session_id:expr ) ignoring_state << $($event:tt)+ ) => {
    expect_event!($server($session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << $($event)+);
  };

  ( $server:ident ( $session_id:expr ) awaiting << $($event:tt)+ ) => {
    crate::session::wait_for_session(&$server, &$session_id)
      .await?
      .lock()
      .await
      .session(&$session_id)
      .ok_or_else(|| anyhow::anyhow!("Session not found"))?
      .next_event_matching(&matches_pattern!($($event)+))
      .await
      .map_err(|e| anyhow::anyhow!(
        "Expected server event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $client:ident << $($event:tt)+ ) => {
    let event = $client
      .session
      .next_event(&[])
      .await?;
    verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
      anyhow::anyhow!("Expected client event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $client:ident skipping [ $($($skip:tt)+),* ] << $($event:tt)+ ) => {
    let event = $client
      .session
      .next_event(&[$(&matches_pattern!($($skip)+)),*])
      .await?;
    verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
      anyhow::anyhow!("Expected client event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $client:ident ignoring_state << $($event:tt)+ ) => {
    expect_event!($client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << $($event)+);
  };

  ( $client:ident awaiting << $($event:tt)+ ) => {
    $client
      .session
      .next_event_matching(&matches_pattern!($($event)+))
      .await
      .map_err(|e| anyhow::anyhow!(
        "Expected client event '{}' not received\n{e}", stringify!($($event)+)))?;
  };
}

/// Apply [`expect_event`] to a sequence of expectations, in order.
macro_rules! expect_events {
  ( $( { $($item:tt)+ } ; )* ) => {
    $( expect_event!($($item)+); )*
  };
}

pub(crate) use expect_event;
pub(crate) use expect_events;
