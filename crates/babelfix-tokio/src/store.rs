//! Where a sequenced session's writes go.
//!
//! A session sends nothing that has not been persisted first: each outbound
//! message is numbered by a [`Sequencer`], written to a [`SessionStore`], and
//! only put on the wire once the write has completed. The inbound watermark —
//! the sequence number to resume from — is written the same way. See
//! [`babelfix_core::sequencer`] for why.
//!
//! The store is the application's. This crate runs its writes, in parallel and
//! without blocking the session, and feeds each completion back.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use tracing::error;

use babelfix_core::sequencer::{
  CommandTarget, Persist, SeqEvent, SeqSink, Sequencer,
};
use babelfix_core::session::{Event, EventSink, Progress};

use crate::Result;

/// Durable storage for a session.
///
/// Each method starts a write and returns a future that completes when it is
/// durable. The futures are `'static`, so several can be in flight at once
/// without borrowing the store: clone whatever handle the write needs into
/// them.
///
/// ```no_run
/// # use babelfix_tokio::store::SessionStore;
/// # use futures::FutureExt;
/// # #[derive(Clone)] struct Db;
/// # impl Db { async fn put(&self, _k: u64, _v: bytes::Bytes) {} }
/// struct MyStore { db: Db }
///
/// impl SessionStore for MyStore {
///   fn persist_outbound(
///     &self,
///     seq_num: u64,
///     wire: bytes::Bytes,
///   ) -> futures::future::BoxFuture<'static, babelfix_tokio::Result<()>> {
///     let db = self.db.clone();
///     async move {
///       db.put(seq_num, wire).await;
///       Ok(())
///     }
///     .boxed()
///   }
///
///   fn persist_watermark(
///     &self,
///     next_in_seq_num: u64,
///   ) -> futures::future::BoxFuture<'static, babelfix_tokio::Result<()>> {
///     futures::future::ready(Ok(())).boxed()
///   }
/// }
/// ```
pub trait SessionStore: Send + Sync + 'static {
  /// Persist an outbound message under `seq_num`. `wire` is exactly what will
  /// be sent: `MsgSeqNum`, `SendingTime` and the CompIDs are set. Store it
  /// to answer the peer's ResendRequests, and resume a later connection one
  /// past the highest `seq_num` stored.
  ///
  /// Admin messages are stored too, so that their numbers are not reused. A
  /// replay gap-fills them whatever was stored, so a placeholder will do.
  fn persist_outbound(
    &self,
    seq_num: u64,
    wire: Bytes,
  ) -> BoxFuture<'static, Result<()>>;

  /// Persist the inbound sequence number to resume a later connection from.
  /// Writes of this are coalesced: at most one is in flight at a time.
  fn persist_watermark(
    &self,
    next_in_seq_num: u64,
  ) -> BoxFuture<'static, Result<()>>;
}

/// Stores nothing, and completes every write at once.
///
/// For tools and tests that do not need to survive a restart. A session
/// using it resumes only from the sequence numbers it is given.
#[derive(Debug, Default, Clone, Copy)]
pub struct VolatileStore;

impl SessionStore for VolatileStore {
  fn persist_outbound(
    &self,
    _seq_num: u64,
    _wire: Bytes,
  ) -> BoxFuture<'static, Result<()>> {
    future::ready(Ok(())).boxed()
  }

  fn persist_watermark(
    &self,
    _next_in_seq_num: u64,
  ) -> BoxFuture<'static, Result<()>> {
    future::ready(Ok(())).boxed()
  }
}

/// A write whose completion the sequencer is waiting for.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Write {
  Outbound(u64),
  Watermark(u64),
}

type InFlight = BoxFuture<'static, (Write, Result<()>)>;

/// The writes a session has in flight, and the store they are going to.
pub(crate) struct Writes {
  store: Arc<dyn SessionStore>,
  in_flight: FuturesUnordered<InFlight>,
  /// Completed writes not yet fed back, for drivers that collect completions
  /// before they can act on them.
  done: VecDeque<(Write, Result<()>)>,
}

impl Writes {
  pub(crate) fn new(store: Arc<dyn SessionStore>) -> Self {
    Self {
      store,
      in_flight: FuturesUnordered::new(),
      done: VecDeque::new(),
    }
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.in_flight.is_empty() && self.done.is_empty()
  }

  /// The next write to complete. Pending forever when there is none, so it can
  /// sit in a `select!` unconditionally.
  pub(crate) async fn next(&mut self) -> (Write, Result<()>) {
    if let Some(done) = self.done.pop_front() {
      return done;
    }
    match self.in_flight.next().await {
      Some(done) => done,
      None => future::pending().await,
    }
  }

  /// Collect every write that has already completed, without waiting.
  fn collect_ready(&mut self) {
    while let Some(Some(done)) = self.in_flight.next().now_or_never() {
      self.done.push_back(done);
    }
  }

  fn start(&mut self, write: Write, future: BoxFuture<'static, Result<()>>) {
    self
      .in_flight
      .push(future.map(move |result| (write, result)).boxed());
  }

  /// Wrap `events` in a [`SeqSink`] that starts each write asked for.
  pub(crate) fn sink<'a, E: EventSink>(
    &'a mut self,
    events: &'a mut E,
  ) -> StoreSink<'a, E> {
    StoreSink {
      writes: self,
      events,
    }
  }
}

/// Starts the writes a sequencer asks for, and passes everything else on.
pub(crate) struct StoreSink<'a, E> {
  writes: &'a mut Writes,
  events: &'a mut E,
}

impl<E: EventSink> SeqSink for StoreSink<'_, E> {
  fn event(&mut self, event: SeqEvent<'_>) -> Result<()> {
    match event {
      SeqEvent::Persist(Persist::Outbound { seq_num, msg }) => {
        let future =
          self.writes.store.persist_outbound(seq_num, msg.to_bytes());
        self.writes.start(Write::Outbound(seq_num), future);
        Ok(())
      }
      SeqEvent::Persist(Persist::Watermark { next_in_seq_num }) => {
        let future = self.writes.store.persist_watermark(next_in_seq_num);
        self.writes.start(Write::Watermark(next_in_seq_num), future);
        Ok(())
      }
      SeqEvent::Session(event) => self.events.event(event),
    }
  }
}

/// A sequencer and the writes it has in flight, delivering session events to
/// `E`.
pub(crate) struct Sequenced {
  pub(crate) seq: Sequencer,
  pub(crate) writes: Writes,
}

impl Sequenced {
  pub(crate) fn new(seq: Sequencer, store: Arc<dyn SessionStore>) -> Self {
    Self {
      seq,
      writes: Writes::new(store),
    }
  }

  /// The sink to hand a driver: session events pass through the sequencer,
  /// writes go to the store, and the rest reaches `events`.
  pub(crate) fn sink<'a, E: EventSink>(
    &'a mut self,
    events: &'a mut E,
  ) -> SequencedSink<'a, E> {
    SequencedSink {
      seq: &mut self.seq,
      writes: &mut self.writes,
      events,
    }
  }

  /// Number an application message and start its write.
  pub(crate) fn send<E: EventSink>(
    &mut self,
    msg: crate::message::Message,
    events: &mut E,
  ) -> Result<u64> {
    self.seq.send(msg, &mut self.writes.sink(events))
  }

  /// Persist and send a message under its own `MsgSeqNum`.
  pub(crate) fn send_raw<E: EventSink>(
    &mut self,
    msg: crate::message::Message,
    events: &mut E,
  ) -> Result<u64> {
    self.seq.send_raw(msg, &mut self.writes.sink(events))
  }

  /// The application has handled inbound `seq_num`.
  pub(crate) fn handled<E: EventSink>(
    &mut self,
    seq_num: u64,
    events: &mut E,
  ) -> Result<()> {
    self.seq.handled(seq_num, &mut self.writes.sink(events))
  }

  /// Feed one completed write back to the sequencer, sending whatever it
  /// releases through `target`.
  pub(crate) fn complete<T: CommandTarget, E: EventSink>(
    &mut self,
    (write, result): (Write, Result<()>),
    target: &mut T,
    events: &mut E,
  ) -> Result<Progress> {
    match (write, result) {
      (Write::Outbound(seq_num), Ok(())) => self.seq.persisted(
        seq_num,
        Instant::now(),
        target,
        &mut self.writes.sink(events),
      ),
      (Write::Watermark(next_in), Ok(())) => {
        self
          .seq
          .watermark_persisted(next_in, &mut self.writes.sink(events))?;
        Ok(Progress::Continue)
      }
      (Write::Outbound(seq_num), Err(e)) => {
        error!("Failed to persist outbound message {seq_num}: {e}");
        Err(self.seq.persist_failed(seq_num))
      }
      (Write::Watermark(next_in), Err(e)) => {
        error!("Failed to persist inbound watermark {next_in}: {e}");
        Err(crate::Error::persistence(format!(
          "inbound watermark {next_in} could not be persisted: {e}"
        )))
      }
    }
  }

  /// Feed back every write that has already completed, without waiting —
  /// which for a store that completes at once is all of them, so its messages
  /// go out in the same pass that numbered them.
  pub(crate) fn complete_ready<T: CommandTarget, E: EventSink>(
    &mut self,
    target: &mut T,
    events: &mut E,
  ) -> Result<Progress> {
    loop {
      self.writes.collect_ready();
      let Some(done) = self.writes.done.pop_front() else {
        return Ok(Progress::Continue);
      };
      if self.complete(done, target, events)?.is_close() {
        return Ok(Progress::Close);
      }
    }
  }

  /// Wait for every write in flight — and those they lead to — to complete.
  /// For the logon exchange, which cannot go on until its Logon is stored.
  pub(crate) async fn settle<T: CommandTarget, E: EventSink>(
    &mut self,
    target: &mut T,
    events: &mut E,
  ) -> Result<Progress> {
    while !self.writes.is_empty() {
      let done = self.writes.next().await;
      if self.complete(done, target, events)?.is_close() {
        return Ok(Progress::Close);
      }
    }
    Ok(Progress::Continue)
  }
}

/// The [`EventSink`] a driver is handed by [`Sequenced::sink`].
pub(crate) struct SequencedSink<'a, E> {
  seq: &'a mut Sequencer,
  writes: &'a mut Writes,
  events: &'a mut E,
}

impl<E: EventSink> EventSink for SequencedSink<'_, E> {
  fn event(&mut self, event: Event<'_>) -> Result<()> {
    let mut store = self.writes.sink(self.events);
    self.seq.sink(&mut store).event(event)
  }
}
