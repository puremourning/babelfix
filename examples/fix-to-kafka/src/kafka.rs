//! Mirrors a session's traffic to Kafka, and tells the session when an inbound
//! message is safely there.
//!
//! samsa's batching `Producer` cannot do the second part: it defaults to
//! `acks=0`, cuts batches where it likes without saying where, and drops
//! failed batches with a log line. So each session gets a [`Writer`] of its
//! own: a task owning a connection to its topic's partition leader, producing
//! what the session queues — in order, in batches whose contents it knows — and
//! waiting for each to be acknowledged by every in-sync replica.
//!
//! Inbound messages count as handled only once they are in Kafka. The session
//! runs with [`InboundPolicy::Explicit`](fix::session::InboundPolicy), so its
//! inbound watermark — where a restart resumes — never passes a message Kafka
//! has not acknowledged, and a crash in between costs a resend, not the
//! message.

use std::time::Duration;

use babelfix as fix;
use bytes::Bytes;
use futures::SinkExt;
use samsa::prelude::protocol::produce::request::Attributes;
use samsa::prelude::*;
use tokio::sync::mpsc;

/// How many queued records go into one produce request, at most.
const MAX_BATCH: usize = 500;

/// How long the broker may take to replicate a batch before failing it.
const PRODUCE_TIMEOUT_MS: i32 = 5_000;

/// Wait between attempts when Kafka is unavailable.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Wait for every in-sync replica.
const ACKS_ALL: i16 = -1;

/// Something for the writer to do, in order.
enum Item {
  /// Produce a record.
  Record { key: Bytes, value: Bytes },
  /// Tell the session that inbound `seq_num` is handled. Queued behind that
  /// message's own record, so by the time it is reached the record has been
  /// acknowledged.
  Handled(u64),
}

/// The session's end of its writer task.
pub struct Writer {
  queue: mpsc::Sender<Item>,
}

impl Writer {
  /// Start a writer producing to `topic`, reporting handled messages through
  /// `session`.
  pub fn spawn(
    brokers: Vec<BrokerAddress>,
    topic: String,
    session: futures::channel::mpsc::Sender<fix::session::SessionCommand>,
  ) -> Self {
    // Bounded: if Kafka falls behind, the session task waits here, which stops
    // it reading events, which pushes back on the peer.
    let (queue, items) = mpsc::channel(4 * MAX_BATCH);
    tokio::spawn(run(brokers, topic, items, session));
    Self { queue }
  }

  /// Produce `msg`, which went either way on the wire.
  pub async fn record(&self, msg: &fix::message::Message) {
    let (key, value) = record(msg);
    self.push(Item::Record { key, value }).await;
  }

  /// Report inbound `seq_num` handled once everything queued so far — its own
  /// record included — is in Kafka.
  pub async fn handled_after_queued(&self, seq_num: u64) {
    self.push(Item::Handled(seq_num)).await;
  }

  async fn push(&self, item: Item) {
    if self.queue.send(item).await.is_err() {
      tracing::warn!("Kafka writer has stopped");
    }
  }
}

/// The key and value a FIX message is stored under: `SenderCompID-MsgSeqNum`,
/// and its exact bytes.
fn record(msg: &fix::message::Message) -> (Bytes, Bytes) {
  use fix::schema::fields::{MsgSeqNum, SenderCompID};
  let header = msg.header();
  let key = Bytes::from(format!(
    "{}-{}",
    header.get(SenderCompID).ok().flatten().unwrap_or_default(),
    header.get(MsgSeqNum).ok().flatten().unwrap_or_default(),
  ));
  // The exact bytes received, or the message's encoding if it was built.
  let value = msg.wire().cloned().unwrap_or_else(|| msg.to_bytes());
  (key, value)
}

/// The writer task: batch whatever is queued, produce it until Kafka has it,
/// then report what is handled.
async fn run(
  brokers: Vec<BrokerAddress>,
  topic: String,
  mut items: mpsc::Receiver<Item>,
  mut session: futures::channel::mpsc::Sender<fix::session::SessionCommand>,
) {
  let mut leader: Option<TcpConnection> = None;
  let mut batch = Vec::with_capacity(MAX_BATCH);

  while items.recv_many(&mut batch, MAX_BATCH).await > 0 {
    let records: Vec<ProduceMessage> = batch
      .iter()
      .filter_map(|item| match item {
        Item::Record { key, value } => Some(ProduceMessage {
          key: Some(key.clone()),
          value: Some(value.clone()),
          headers: vec![],
          topic: topic.clone(),
          partition_id: 0,
        }),
        Item::Handled(_) => None,
      })
      .collect();

    // Nothing after this batch goes until it is in, so a failure holds the
    // watermark where it is rather than skipping over the batch.
    while !records.is_empty() {
      match produce_to_leader(&brokers, &topic, &mut leader, &records).await {
        Ok(()) => break,
        Err(e) => {
          tracing::warn!("Producing to {topic} failed, retrying: {e:?}");
          leader = None;
          tokio::time::sleep(RETRY_INTERVAL).await;
        }
      }
    }

    for item in batch.drain(..) {
      if let Item::Handled(seq_num) = item {
        // The session may have ended; a later connection resumes from the
        // last watermark persisted, and the peer resends from there.
        let _ = session
          .send(fix::session::SessionCommand::Handled(seq_num))
          .await;
      }
    }
  }
}

/// Produce `records` to partition 0 of `topic` and wait for every in-sync
/// replica to have them, connecting to the partition leader first if need be.
async fn produce_to_leader(
  brokers: &[BrokerAddress],
  topic: &str,
  leader: &mut Option<TcpConnection>,
  records: &Vec<ProduceMessage>,
) -> samsa::prelude::Result<()> {
  let conn = match leader {
    Some(conn) => conn.clone(),
    None => leader
      .insert(connect_to_leader(brokers, topic).await?)
      .clone(),
  };

  // One request at a time on this connection, so the response read here is
  // the one for this request.
  let response = produce(
    conn,
    1,
    "fix-to-kafka",
    ACKS_ALL,
    PRODUCE_TIMEOUT_MS,
    records,
    Attributes::new(None),
  )
  .await?
  .ok_or(Error::MissingBrokerConfigOptions)?;

  for partition in response
    .responses
    .iter()
    .flat_map(|r| &r.partition_responses)
  {
    if partition.error_code != KafkaCode::None {
      return Err(Error::KafkaError(partition.error_code));
    }
  }
  Ok(())
}

/// A connection to the broker leading partition 0 of `topic`.
async fn connect_to_leader(
  brokers: &[BrokerAddress],
  topic: &str,
) -> samsa::prelude::Result<TcpConnection> {
  let metadata = ClusterMetadata::<TcpConnection>::new(
    brokers.to_vec(),
    1,
    "fix-to-kafka".to_string(),
    vec![topic.to_string()],
  )
  .await?;
  let leader = metadata
    .get_leader_id_for_topic_partition(topic, 0)
    .ok_or_else(|| Error::NoLeaderForTopicPartition(topic.to_string(), 0))?;
  metadata
    .broker_connections
    .get(&leader)
    .cloned()
    .ok_or(Error::NoConnectionForBroker(leader))
}
