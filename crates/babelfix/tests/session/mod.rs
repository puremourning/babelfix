use super::fix;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use googletest::matcher::Matcher;
use googletest::{assert_that, expect_that, verify_that};

pub static FIX_REPO: std::sync::LazyLock<Arc<fix::repository::FixRepository>> =
  std::sync::LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

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
  pub async fn expect_next(
    &mut self,
    expected: impl for<'a> Matcher<&'a fix::session::SessionEvent>,
    skipping: &[&dyn for<'a> Matcher<&'a fix::session::SessionEvent>],
  ) -> anyhow::Result<()> {
    'outer: loop {
      let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
      let event = self.handle.events.recv();

      tokio::select! {
        _ = timeout => {
          anyhow::bail!("Timed out waiting for event");
        }
        event = event => {
          let event = match event {
            Ok(event) => event,
            Err(e) => {
              anyhow::bail!("Session event channel died: {e}");
            }
          };
          self.events.push_back(event.clone());
          for skip in skipping {
            if let googletest::matcher::MatcherResult::Match =
              skip.matches(&event)
            {
              continue 'outer;
            }
          }
          expect_that!(&event, &expected);
          return Ok(());
        }
        _ = self.cancellation_token.cancelled() => {
          anyhow::bail!("Cancelled waiting for event");
        }
      };
    }
  }

  // TODO: we want
  // - expect_next(event, skipping [...])
  //   checks for an event matching event while skipping any that match the
  //   list of skpps; equivalent to expect_in_order([event], skipping)
  // = expect_in_order(events, skipping) -
  //   checks for events matching the list of events in order, skipping any
  //   that match the list of skips
  // - expect_in_any_order(events, skipping) -
  //   checks for events matching the list of events in any order, skipping
  //   any that match the list of skips
  //
  // And we should probably adopt googletest-rust for its hamcrest-like
  // matchers. But be careful it looks like it breaks a lot.
  //
  // matching is done either by a matcher fn(event, expected) -> bool
  // and we have a template match for messages which requires any specified
  // fields to match and ignores all others. then again, this quicky gets
  // cmplex. i wonder if there is something like hamcrest for rust.
  pub async fn expect_event(
    &mut self,
    expected: impl for<'a> Matcher<&'a fix::session::SessionEvent>,
  ) -> anyhow::Result<()> {
    // First check/consume buffered events?
    // loop {
    //   let Some(event) = self.events.pop_front() else {
    //     break;
    //   };
    //   if event_matches(&event, &expected) {
    //     return Ok(());
    //   }
    // }

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
    let event = self.handle.events.recv();

    tokio::select! {
      _ = timeout => {
        anyhow::bail!("Timed out waiting for event");
      }
      event = event => {
        let event = match event {
          Ok(event) => event,
          Err(e) => {
            anyhow::bail!("Session event channel died: {e}");
          }
        };
        assert_that!(&event, &expected);
        self.events.push_back(event.clone());
        Ok(())
      }
      _ = self.cancellation_token.cancelled() => {
        anyhow::bail!("Cancelled waiting for event");
      }
    }
  }
}

pub struct Server {
  commands: futures::channel::mpsc::Sender<fix::endpoint::EndpointCommand>,
  configured_sessions:
    Vec<(fix::session::SessionIdentifier, fix::session::Session)>,
  live_sessions: Vec<Session>,
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

  pub async fn new(port: u16) -> anyhow::Result<Arc<Mutex<Self>>> {
    let fix::endpoint::Endpoint {
      commands,
      mut events,
      local_addr,
    } = fix::endpoint::serve(
      port,
      Some(String::from("127.0.0.1")),
      Arc::clone(&FIX_REPO),
      None,
    )
    .await?;

    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let server = Arc::new(Mutex::new(Self {
      commands,
      sessions_cancel: cancellation_token.clone(),
      configured_sessions: Vec::new(),
      live_sessions: Vec::new(),
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
                babelfix::endpoint::EndpointEvent::SessionInvalid(_) => todo!(),
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
        Ok(())
      }))
    };

    Ok(server)
  }
}

pub struct Client {
  cancel_signal: futures::channel::oneshot::Sender<()>,
  cancellation_token: tokio_util::sync::CancellationToken,
  pub session: Session,
}
impl Client {
  pub async fn new(
    port: u16,
    session_id: fix::session::SessionIdentifier,
    state: fix::session::Session,
  ) -> anyhow::Result<Self> {
    let (cancel_client, client_cancellation_token) =
      futures::channel::oneshot::channel();

    let cancellation_token = tokio_util::sync::CancellationToken::new();

    let (client_tx, mut client_rx) = futures::channel::mpsc::channel(100);
    tokio::spawn(fix::endpoint::connect(
      vec![("127.0.0.1".to_string(), port)],
      client_tx,
      Arc::clone(&FIX_REPO),
      None,
      session_id,
      state.clone(),
      client_cancellation_token,
    ));

    let timeout = tokio::time::sleep(std::time::Duration::from_secs(5));
    let session_handle = tokio::select! {
      event = client_rx.recv() => {
          let event = match event {
            Ok(event) => event,
            Err(e) => {
              anyhow::bail!("Client event channel closed unexpectedly: {e}");
            }
          };
          let fix::endpoint::EndpointEvent::SessionConnected(session_handle) = event else {
            anyhow::bail!("Expected SessionConnected event");
          };
          session_handle
      }
      _ = timeout => {
        anyhow::bail!("Timed out waiting for client to connect");
      }
    };

    Ok(Self {
      cancel_signal: cancel_client,
      session: Session {
        handle: session_handle,
        events: VecDeque::new(),
        state,
        cancellation_token: cancellation_token.clone(),
      },
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
    let (tx, _) = futures::channel::oneshot::channel();
    let tx = std::mem::replace(&mut self.cancel_signal, tx);
    let _ = tx.send(());
  }
}

pub async fn establish(
  client_comp_id: impl Into<String>,
  client_seq_no: Option<(u32, u32)>,
  server_comp_id: impl Into<String>,
  server_seq_no: Option<(u32, u32)>,
  fix_version: Arc<fix::repository::FixVersion>,
) -> anyhow::Result<(
  (fix::session::SessionIdentifier, Client),
  (fix::session::SessionIdentifier, Arc<Mutex<Server>>),
)> {
  let client_comp_id = client_comp_id.into();
  let server_comp_id = server_comp_id.into();

  let client_seq_no = client_seq_no.unwrap_or((1, 1));
  let server_seq_no = server_seq_no.unwrap_or((1, 1));

  let server = Server::new(0).await?;
  let port = server.lock().await.local_addr.port();

  tracing::info!("Server listening on port {}", port);

  let server_session_id = fix::session::SessionIdentifier {
    begin_string: fix_version.begin_string.clone(),
    sender_comp_id: server_comp_id.clone(),
    target_comp_id: client_comp_id.clone(),
  };
  let client_session_id = fix::session::SessionIdentifier {
    begin_string: fix_version.begin_string.clone(),
    sender_comp_id: client_comp_id,
    target_comp_id: server_comp_id,
  };

  server.lock().await.push_session(
    server_session_id.clone(),
    fix::session::Session {
      fix_version: fix_version.clone(),
      next_in_seq_num: server_seq_no.0,
      next_out_seq_num: server_seq_no.1,
      heartbeat_interval: std::time::Duration::from_secs(30),
    },
  );

  tracing::info!("Starting client on port {}", port);
  let client = Client::new(
    port,
    client_session_id.clone(),
    fix::session::Session {
      fix_version,
      next_in_seq_num: client_seq_no.0,
      next_out_seq_num: client_seq_no.1,
      heartbeat_interval: std::time::Duration::from_secs(30),
    },
  )
  .await?;

  tracing::info!("Client connected to server on port {}", port);

  Ok(((client_session_id, client), (server_session_id, server)))
}
