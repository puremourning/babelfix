//! Mirrors a session's traffic to Kafka, and tells the session when an inbound
//! message is safely there.
//!
//! Each session gets a [`Writer`]: a task producing what the session queues to
//! partition 0 of the session's topic — in order, a batch at a time — and
//! waiting for each batch to be acknowledged by every in-sync replica
//! (rskafka always produces with `acks=all`).
//!
//! Inbound messages count as handled only once they are in Kafka. The session
//! runs with [`InboundPolicy::Explicit`](fix::session::InboundPolicy), so its
//! inbound watermark — where a restart resumes — never passes a message Kafka
//! has not acknowledged, and a crash in between costs a resend, not the
//! message.
//!
//! A batch whose acknowledgement is lost (a timeout, a dropped connection) is
//! sent again, so Kafka may hold a record twice. Records are keyed
//! `SenderCompID-MsgSeqNum`, so a consumer can tell.

use std::sync::Arc;
use std::time::Duration;

use babelfix as fix;
use futures::SinkExt;
use rskafka::client::Client;
use rskafka::client::error::{Error, ProtocolError};
use rskafka::client::partition::{
  Compression, PartitionClient, UnknownTopicHandling,
};
use rskafka::record::Record;
use tokio::sync::mpsc;

/// How many queued records go into one produce request, at most.
const MAX_BATCH: usize = 500;

/// Wait between attempts when Kafka is unavailable.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Something for the writer to do, in order.
enum Item {
  /// Produce a record.
  Record(Record),
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
    client: Arc<Client>,
    topic: String,
    session: futures::channel::mpsc::Sender<fix::session::SessionCommand>,
  ) -> Self {
    // Bounded: if Kafka falls behind, the session task waits here, which stops
    // it reading events, which pushes back on the peer.
    let (queue, items) = mpsc::channel(4 * MAX_BATCH);
    tokio::spawn(run(client, topic, items, session));
    Self { queue }
  }

  /// Produce `msg`, which went either way on the wire.
  pub async fn record(&self, msg: &fix::message::Message) {
    self.push(Item::Record(record(msg))).await;
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

/// A FIX message as a record: keyed `SenderCompID-MsgSeqNum`, its value the
/// exact bytes.
fn record(msg: &fix::message::Message) -> Record {
  use fix::schema::fields::{MsgSeqNum, SenderCompID};
  let header = msg.header();
  let key = format!(
    "{}-{}",
    header.get(SenderCompID).ok().flatten().unwrap_or_default(),
    header.get(MsgSeqNum).ok().flatten().unwrap_or_default(),
  );
  // The exact bytes received, or the message's encoding if it was built.
  let value = msg.wire().cloned().unwrap_or_else(|| msg.to_bytes());
  Record {
    key: Some(key.into_bytes()),
    value: Some(value.to_vec()),
    headers: Default::default(),
    timestamp: chrono::Utc::now(),
  }
}

/// The writer task: batch whatever is queued, produce it until Kafka has it,
/// then report what is handled.
async fn run(
  client: Arc<Client>,
  topic: String,
  mut items: mpsc::Receiver<Item>,
  mut session: futures::channel::mpsc::Sender<fix::session::SessionCommand>,
) {
  let mut partition: Option<PartitionClient> = None;
  let mut batch = Vec::with_capacity(MAX_BATCH);

  while items.recv_many(&mut batch, MAX_BATCH).await > 0 {
    let records: Vec<Record> = batch
      .iter()
      .filter_map(|item| match item {
        Item::Record(record) => Some(record.clone()),
        Item::Handled(_) => None,
      })
      .collect();

    // Nothing after this batch goes until it is in, so a failure holds the
    // watermark where it is rather than skipping over the batch.
    while !records.is_empty() {
      match produce(&client, &topic, &mut partition, records.clone()).await {
        Ok(()) => break,
        Err(e) => {
          tracing::warn!("Producing to {topic} failed, retrying: {e}");
          partition = None;
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

/// Produce `records` to partition 0 of `topic`, creating the topic and the
/// partition client first if need be. Returns once every in-sync replica has
/// them.
async fn produce(
  client: &Client,
  topic: &str,
  partition: &mut Option<PartitionClient>,
  records: Vec<Record>,
) -> Result<(), Error> {
  let partition = match partition {
    Some(partition) => partition,
    None => {
      ensure_topic(client, topic).await?;
      partition.insert(
        client
          .partition_client(topic, 0, UnknownTopicHandling::Retry)
          .await?,
      )
    }
  };
  let n = records.len();
  let offsets = partition
    .produce(records, Compression::NoCompression)
    .await?;
  debug_assert_eq!(offsets.len(), n);
  Ok(())
}

/// Create `topic` with one partition, unless it exists already.
async fn ensure_topic(client: &Client, topic: &str) -> Result<(), Error> {
  match client
    .controller_client()?
    .create_topic(topic, 1, 1, 5_000)
    .await
  {
    Err(Error::ServerError {
      protocol_error: ProtocolError::TopicAlreadyExists,
      ..
    }) => Ok(()),
    result => result,
  }
}
