use babelfix as fix;
use futures::SinkExt;
use serde_with::DurationSeconds;
use serde_with::serde_as;
use std::sync::Arc;

mod kafka;

// fn assert_send<T: Send>() {}
// fn assert_sync<T: Sync>() {}

/// The Kafka topic a session's traffic goes to.
fn topic(session_id: &fix::session::SessionIdentifier) -> String {
  format!(
    "fix-{}-{}-{}",
    session_id.begin_string,
    session_id.sender_comp_id,
    session_id.target_comp_id
  )
}

async fn run_session(
  mut session_handle: fix::session::SessionHandle,
  cancellation_token: tokio_util::sync::CancellationToken,
  app: Arc<App>,
) -> anyhow::Result<()> {
  // Produces in the background, so this task never waits on Kafka — only,
  // if Kafka falls far enough behind, on room in the writer's queue.
  let kafka = kafka::Writer::spawn(
    app.kafka.clone(),
    topic(&session_handle.session_id),
    session_handle.tx.clone(),
  );

  loop {
    tokio::select! {
      _ = cancellation_token.cancelled() => {
        tracing::debug!("Session cancelled");
        break;
      }
      event = session_handle.events.recv() => {
        if event.is_err() {
          break;
        }
        match event? {
            fix::session::SessionEvent::ConnectionEstablished => {},
            fix::session::SessionEvent::LoggedOn => {},
            fix::session::SessionEvent::RecoveryCompleted => {},
            // Every message, either way, goes to Kafka.
            fix::session::SessionEvent::RawMessageReceived(fix_message)
            | fix::session::SessionEvent::RawMessageSent(fix_message) => {
              kafka.record(&fix_message).await;
            },
            // An inbound message is handled once its record — queued just
            // before this, as RawMessageReceived — is in Kafka. Until then the
            // session's watermark stays behind it, so a crash means the peer
            // resends it rather than it being lost.
            fix::session::SessionEvent::MessageReceived { seq_num, .. } => {
              kafka.handled_after_queued(seq_num).await;
            },
            fix::session::SessionEvent::ResendRequest {
              resend_request,
              begin_seq_no,
              end_seq_no,
            } => {
              // Replay what the store holds; anything it does not hold, and
              // every admin message, is gap-filled.
              for wire in storage::outbound_range(
                &session_handle.session_id,
                begin_seq_no..=end_seq_no,
                &app,
              )? {
                let msg = fix::message::Message::parse(
                  resend_request.dict(),
                  wire,
                )?;
                session_handle
                  .tx
                  .send(fix::session::SessionCommand::Replay(msg))
                  .await?;
              }
              session_handle
                .tx
                .send(fix::session::SessionCommand::ReplayComplete)
                .await?;
            },
            fix::session::SessionEvent::Disconnected => {},
            _ => {},
        }
      }
    }
  }
  Ok(())
}

mod storage {
  use super::*;
  use futures::FutureExt;

  /// What is persisted per session: its settings, and where it resumes.
  #[derive(serde::Serialize, serde::Deserialize)]
  pub struct SessionConfiguration {
    heartbeat_interval: std::time::Duration,
    /// One past the highest outbound message stored.
    next_out_seq_num: u64,
    /// The inbound watermark.
    next_in_seq_num: u64,
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

  fn session_key(session_id: &fix::session::SessionIdentifier) -> kv::Raw {
    use kv::Value;
    kv::Bincode::<fix::session::SessionIdentifier>(session_id.clone())
      .to_raw_value()
      .unwrap()
  }

  /// Outbound messages are keyed by session, then sequence number, so a
  /// session's messages sort together and in order.
  fn message_key(session: &kv::Raw, seq_num: u64) -> kv::Raw {
    let mut key = session.to_vec();
    key.extend_from_slice(&seq_num.to_be_bytes());
    kv::Raw::from(key)
  }

  fn sessions(
    db: &kv::Store,
  ) -> kv::Bucket<'_, kv::Raw, kv::Bincode<SessionConfiguration>> {
    db.bucket(Some("sessions")).unwrap()
  }

  fn messages(db: &kv::Store) -> kv::Bucket<'_, kv::Raw, kv::Raw> {
    db.bucket(Some("messages")).unwrap()
  }

  pub fn init_session(
    session_id: &fix::session::SessionIdentifier,
    session_config: SessionConfiguration,
    db: &kv::Store,
  ) {
    let key = session_key(session_id);
    let value = kv::Bincode(session_config);
    sessions(db)
      .transaction(|txn| {
        if txn.get(&key)?.is_none() {
          txn.set(&key, &value)?;
        }
        Ok(())
      })
      .unwrap();
  }

  /// The setup to resume `session_id` with: its settings, where it got to,
  /// and this store to persist to.
  pub fn look_up_session(
    session_id: &fix::session::SessionIdentifier,
    app: &App,
  ) -> Option<fix::session::SessionSetup> {
    let key = session_key(session_id);
    let value = sessions(&app.db).get(&key).ok()??.0;
    let mut config = fix::session::SessionConfig::new(
      app
        .dicts
        .for_begin_string(session_id.begin_string.as_bytes())?
        .clone(),
    );
    config.heartbeat_interval = value.heartbeat_interval;
    Some(
      fix::session::SessionSetup::new(config)
        // Inbound messages are handled once Kafka has them: see `kafka`.
        .inbound(fix::session::InboundPolicy::Explicit(
          fix::session::WatermarkMode::Contiguous,
        ))
        .resume(fix::session::Resume {
          next_out_seq_num: value.next_out_seq_num,
          next_in_seq_num: value.next_in_seq_num,
        })
        .store(Arc::new(KvStore {
          db: app.db.clone(),
          session: key,
        })),
    )
  }

  /// The stored outbound messages in `range`, in order.
  pub fn outbound_range(
    session_id: &fix::session::SessionIdentifier,
    range: std::ops::RangeInclusive<u64>,
    app: &App,
  ) -> anyhow::Result<Vec<bytes::Bytes>> {
    let session = session_key(session_id);
    let bucket = messages(&app.db);
    let mut out = Vec::new();
    for item in bucket.iter_range(
      &message_key(&session, *range.start()),
      &message_key(&session, range.end().saturating_add(1)),
    )? {
      let value: kv::Raw = item?.value()?;
      out.push(bytes::Bytes::copy_from_slice(&value));
    }
    Ok(out)
  }

  /// Persists each outbound message, and where the session resumes, to the
  /// application's kv store — durably, before the session goes on.
  struct KvStore {
    db: kv::Store,
    session: kv::Raw,
  }

  impl KvStore {
    /// Store `message`, if there is one, then apply `update` to this
    /// session's record, and flush both.
    fn write(
      &self,
      message: Option<(kv::Raw, kv::Raw)>,
      update: impl Fn(&mut SessionConfiguration) + Send + 'static,
    ) -> futures::future::BoxFuture<'static, fix::Result<()>> {
      let (db, session) = (self.db.clone(), self.session.clone());
      async move {
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
          if let Some((key, value)) = message {
            messages(&db).set(&key, &value)?;
          }
          // Several writes may be in flight at once, so the record is updated
          // in a transaction: a read-modify-write racing another could
          // otherwise move it backwards. The transaction may be retried, so
          // `update` must be repeatable.
          let bucket = sessions(&db);
          bucket.transaction(|txn| {
            let Some(kv::Bincode(mut record)) = txn.get(&session)? else {
              return Err(kv::abort(kv::Error::Message(
                "session is not configured".into(),
              )));
            };
            update(&mut record);
            txn.set(&session, &kv::Bincode(record))?;
            Ok(())
          })?;
          messages(&db).flush()?;
          bucket.flush()?;
          Ok(())
        })
        .await
        .map_err(|e| fix::Error::persistence(e.to_string()))?
        .map_err(|e| fix::Error::persistence(e.to_string()))
      }
      .boxed()
    }
  }

  impl fix::store::SessionStore for KvStore {
    fn persist_outbound(
      &self,
      seq_num: u64,
      wire: bytes::Bytes,
    ) -> futures::future::BoxFuture<'static, fix::Result<()>> {
      let message = (
        message_key(&self.session, seq_num),
        kv::Raw::from(wire.as_ref()),
      );
      self.write(Some(message), move |record| {
        record.next_out_seq_num = record.next_out_seq_num.max(seq_num + 1);
      })
    }

    fn persist_watermark(
      &self,
      next_in_seq_num: u64,
    ) -> futures::future::BoxFuture<'static, fix::Result<()>> {
      self.write(None, move |record| {
        record.next_in_seq_num = record.next_in_seq_num.max(next_in_seq_num);
      })
    }
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

  #[serde(default = "default_server_address")]
  server: Address,
  #[serde(default = "default_broker_address")]
  broker: Address,

  #[serde(default = "default_db_path")]
  db_path: String,
}

fn default_db_path() -> String {
  "fix_app.db".to_string()
}
fn default_server_address() -> Address {
  Address {
    host: "0.0.0.0".to_string(),
    port: 9797,
  }
}
fn default_broker_address() -> Address {
  Address {
    host: "localhost".to_string(),
    port: 9092,
  }
}

impl Config {
  fn load(path: &str) -> anyhow::Result<Self> {
    let file = std::fs::File::open(path)?;
    let config: Config = serde_json::from_reader(file)?;
    Ok(config)
  }
}

struct App {
  dicts: Arc<fix::message::Dictionaries>,
  db: kv::Store,
  kafka: Arc<rskafka::client::Client>,

  session_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl App {
  fn new(
    config: Config,
    dicts: Arc<fix::message::Dictionaries>,
    kafka: Arc<rskafka::client::Client>,
  ) -> anyhow::Result<Self> {
    let db = kv::Store::new(kv::Config::new(config.db_path))?;

    for session in &config.sessions {
      storage::init_session(
        &session.session_id,
        storage::SessionConfiguration::new(session.heartbeat_interval),
        &db,
      );
    }

    Ok(Self {
      dicts,
      db,
      kafka,
      session_tasks: std::sync::Mutex::new(Vec::new()),
    })
  }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  tracing_subscriber::fmt()
    .with_level(true)
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or(tracing_subscriber::EnvFilter::new("info")),
    )
    .init();

  let config = Config::load("config.json")?;

  let kafka = Arc::new(
    rskafka::client::ClientBuilder::new(vec![format!(
      "{}:{}",
      config.broker.host, config.broker.port
    )])
    .build()
    .await?,
  );

  let dicts = fix::message::Dictionaries::standard()?;
  let mut endpoint = fix::endpoint::serve(
    (config.server.host.clone(), config.server.port),
    Arc::clone(&dicts),
    fix::endpoint::EndpointConfig::default(),
  )
  .await?;

  let app = Arc::new(App::new(config, dicts, kafka)?);

  let token = tokio_util::sync::CancellationToken::new();

  // Handle endpoint events.
  //
  // NewSession is called when a peer connects and sends a logon. It returns the
  // session's setup: its FIX protocol settings, where it resumes, and the
  // store its messages are persisted to before they are sent.
  //
  // SessionInvalid is called when a peer connects and sends garbage.
  //
  // SessionConnected is called after a successful logon exchange. The
  // session_handle contains a channel to send/receive messages.
  //
  loop {
    tokio::select! {
      _ = tokio::signal::ctrl_c() => {
        tracing::info!("Shutting down...");
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
                tracing::warn!("Error in session: {:?}", e);
              }
            }));
          }
        }
      }
    }
  }

  token.cancel();
  endpoint
    .commands
    .send(fix::endpoint::EndpointCommand::Shutdown)
    .await
    .ok();

  let mut session_tasks = {
    let mut session_tasks = app.session_tasks.lock().unwrap();
    std::mem::take(&mut *session_tasks)
  };

  for task in session_tasks.drain(..) {
    task.await?;
  }

  fix::endpoint::join(endpoint.join_handle, "server").await?;

  Ok(())
}
