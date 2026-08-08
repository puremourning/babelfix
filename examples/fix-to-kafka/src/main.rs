use babelfix as fix;
use futures::SinkExt;
use samsa::prelude::*;
use serde_with::DurationSeconds;
use serde_with::serde_as;
use std::sync::Arc;

// fn assert_send<T: Send>() {}
// fn assert_sync<T: Sync>() {}

async fn run_session(
  mut session_handle: fix::session::SessionHandle,
  cancellation_token: tokio_util::sync::CancellationToken,
  app: Arc<App>,
) -> anyhow::Result<()> {
  loop {
    tokio::select! {
      _ = cancellation_token.cancelled() => {
        println!("Session cancelled");
        break;
      }
      event = session_handle.events.recv() => {
        match event? {
            fix::session::SessionEvent::ConnectionEstablished => {},
            fix::session::SessionEvent::RecoveryCompleted => {},
            fix::session::SessionEvent::SessionState(session) => {
              storage::update_session(
                &session_handle.session_id,
                &session,
                &app)?;
            }
            fix::session::SessionEvent::RawMessageReceived(
              fix_message,
              session
            ) => {
              storage::update_session(
                &session_handle.session_id,
                &session,
                &app)?;
              app.produce(&session_handle, fix_message).await;
            },
            fix::session::SessionEvent::RawMessageSent(
              fix_message,
              session) => {
              storage::update_session(
                &session_handle.session_id,
                &session,
                &app)?;
              app.produce(&session_handle, fix_message).await;
            },
            fix::session::SessionEvent::MessageReceived(_message) => {
              // A real system might actually handle the messages
            },
            fix::session::SessionEvent::ResendRequest {
              resend_request: _,
              begin_seq_no: _,
              end_seq_no: _
            } => {
              // A real system would actually service this replay
              session_handle.tx.send(
                fix::session::SessionCommand::ReplayComplete
              ).await.ok();
            },
            fix::session::SessionEvent::Disconnected => {},
            _ => todo!(),
        }
      }
    }
  }
  Ok(())
}

mod storage {
  use super::*;

  #[derive(serde::Serialize, serde::Deserialize)]
  pub struct SessionConfiguration {
    heartbeat_interval: std::time::Duration,
    next_out_seq_num: u32,
    next_in_seq_num: u32,
  }

  impl SessionConfiguration {
    pub fn new(heartbeat_interval: std::time::Duration) -> Self {
      Self {
        heartbeat_interval,
        next_out_seq_num: 1,
        next_in_seq_num: 1,
      }
    }
  }

  pub fn init_session(
    session_id: &fix::session::SessionIdentifier,
    session_config: SessionConfiguration,
    db: &kv::Store,
  ) {
    use kv::Value;
    let key =
      kv::Bincode::<fix::session::SessionIdentifier>(session_id.clone());
    let key = key.to_raw_value().unwrap();

    let bucket = db
      .bucket::<kv::Raw, kv::Bincode<SessionConfiguration>>(Some("sessions"))
      .unwrap();

    let value = kv::Bincode(session_config);

    bucket
      .transaction(|txn| {
        if txn.get(&key)?.is_none() {
          txn.set(&key, &value)?;
        }
        Ok(())
      })
      .unwrap();
  }

  pub fn look_up_session(
    session_id: &fix::session::SessionIdentifier,
    app: &App,
  ) -> Option<fix::session::Session> {
    use kv::Value;
    let key =
      kv::Bincode::<fix::session::SessionIdentifier>(session_id.clone());
    let key = key.to_raw_value().unwrap();

    app
      .db
      .bucket::<kv::Raw, kv::Bincode<SessionConfiguration>>(Some("sessions"))
      .ok()
      .and_then(|bucket| bucket.get(&key).ok())
      .and_then(|v| {
        v.map(|v| v.0).map(|value| fix::session::Session {
          heartbeat_interval: value.heartbeat_interval,
          next_in_seq_num: value.next_in_seq_num,
          next_out_seq_num: value.next_out_seq_num,
          fix_version: app.repo.get_version(&session_id.begin_string).unwrap(),
        })
      })
  }

  pub fn update_session(
    session_id: &fix::session::SessionIdentifier,
    session: &fix::session::Session,
    app: &App,
  ) -> anyhow::Result<()> {
    use kv::Value;
    let key =
      kv::Bincode::<fix::session::SessionIdentifier>(session_id.clone());
    let key = key.to_raw_value().unwrap();

    let bucket = app
      .db
      .bucket::<kv::Raw, kv::Bincode<SessionConfiguration>>(Some("sessions"))?;

    bucket.transaction(|txn| {
      let value = SessionConfiguration {
        heartbeat_interval: session.heartbeat_interval,
        next_in_seq_num: session.next_in_seq_num,
        next_out_seq_num: session.next_out_seq_num,
      };
      txn.set(&key, &kv::Bincode(value))
    })?;
    Ok(())
  }
}

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize)]
struct ConfiguredSession {
  session_id: fix::session::SessionIdentifier,

  #[serde_as(as = "DurationSeconds")]
  heartbeat_interval: std::time::Duration,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct Address {
  host: String,
  port: u16,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Config {
  sessions: Vec<ConfiguredSession>,

  server: Option<Address>,
  broker: Option<Address>,
}

impl Config {
  fn load(path: &str) -> anyhow::Result<Self> {
    let file = std::fs::File::open(path)?;
    let config: Config = serde_json::from_reader(file)?;
    Ok(config)
  }
}

struct App {
  repo: Arc<fix::repository::FixRepository>,
  db: kv::Store,
  producer: Producer,

  session_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl App {
  fn new(
    config: Config,
    repo: Arc<fix::repository::FixRepository>,
    producer: Producer,
  ) -> anyhow::Result<Self> {
    let db = kv::Store::new(kv::Config::new("fix_app.db"))?;

    for session in &config.sessions {
      storage::init_session(
        &session.session_id,
        storage::SessionConfiguration::new(session.heartbeat_interval),
        &db,
      );
    }

    Ok(Self {
      repo,
      db,
      producer,
      session_tasks: std::sync::Mutex::new(Vec::new()),
    })
  }

  async fn produce(
    &self,
    session_handle: &fix::session::SessionHandle,
    fix_message: fix::message::FixMessage,
  ) {
    let topic = format!(
      "fix-{}-{}-{}",
      session_handle.session_id.begin_string,
      session_handle.session_id.sender_comp_id,
      session_handle.session_id.target_comp_id
    );
    let key = bytes::Bytes::from(format!(
      "{}-{}",
      fix_message
        .get_tag(fix::schema::FIX_Latest::Fields::SenderCompID)
        .unwrap()
        .as_str(&fix_message.data),
      fix_message
        .get_tag(fix::schema::FIX_Latest::Fields::MsgSeqNum)
        .unwrap()
        .as_str(&fix_message.data),
    ));
    let payload = fix_message.into_bytes();
    self
      .producer
      .produce(ProduceMessage {
        headers: vec![],
        key: Some(key),
        partition_id: 0,
        topic,
        value: Some(payload),
      })
      .await;
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();

  let config = Config::load("config.json")?;

  let broker = config
    .broker
    .as_ref()
    .map(|b| BrokerAddress {
      host: b.host.clone(),
      port: b.port,
    })
    .unwrap_or(BrokerAddress {
      host: "localhost".to_string(),
      port: 9092,
    });

  let bootstrap_addrs = vec![broker];

  let topics = config
    .sessions
    .iter()
    .map(|s| {
      format!(
        "fix-{}-{}-{}",
        s.session_id.begin_string,
        s.session_id.sender_comp_id,
        s.session_id.target_comp_id
      )
    })
    .collect::<Vec<_>>();

  let producer = ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, topics)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create producer: {:?}", e))?
    .build()
    .await;

  let server_addr = config
    .server
    .as_ref()
    .map(|s| Address {
      host: s.host.clone(),
      port: s.port,
    })
    .unwrap_or(Address {
      host: "0.0.0.0".into(),
      port: 9797,
    });

  let repo = Arc::new(fix::repository::orchestrate()?);
  let mut endpoint = fix::endpoint::serve(
    server_addr.port,
    Some(server_addr.host),
    Arc::clone(&repo),
    None,
  )
  .await?;

  let app = Arc::new(App::new(config, repo, producer)?);

  let token = tokio_util::sync::CancellationToken::new();

  // Handle endpoint events.
  //
  // NewSession is called when a peer connects and sends a logon. It returns the
  // session deatils including the next expected sequence numbers for the logon
  // exchange, and the FIX protocol settings.
  //
  // SessionInvalid is called when a peer connects and sends garbage.
  //
  // SessionConnected is called after a successful logon exchange. The
  // session_handle contains a channel to send/receive messages.
  //
  loop {
    tokio::select! {
      _ = tokio::signal::ctrl_c() => {
        println!("Shutting down...");
        token.cancel();
        endpoint.commands.send(fix::endpoint::EndpointCommand::Shutdown).await.ok();
        break;
      }
      event = endpoint.events.recv() => {
        match event? {
            fix::endpoint::EndpointEvent::NewSession { session_id, response } => {
                if let Some(session) = storage::look_up_session(&session_id, &app) {
                    response.send(Ok(session)).ok();
                } else {
                    response.send(Err(fix::Error::unspecified("Unable to find session"))).ok();
                }
            },
            fix::endpoint::EndpointEvent::SessionInvalid(_) => {},
            fix::endpoint::EndpointEvent::SessionConnected(session_handle) => {
              let mut session_tasks = app.session_tasks.lock().unwrap();
              let token = token.clone();
              let app  = Arc::clone(&app);
              session_tasks.push(tokio::spawn(async move {
                if let Err(e) = run_session(session_handle, token, app).await {
                  eprintln!("Error in session: {:?}", e);
                }
              }));
            }
        }
      }
    }
  }

  let mut session_tasks = {
    let mut session_tasks = app.session_tasks.lock().unwrap();
    std::mem::take(&mut *session_tasks)
  };

  for task in session_tasks.drain(..) {
    task.await?;
  }

  Ok(())
}
