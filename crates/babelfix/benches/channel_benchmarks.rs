//! What it costs to hand a message to the application and get its reply back,
//! and how that cost changes with the tokio executor the session runs on.
//!
//! Every inbound message crosses an `mpsc` channel from the session task to the
//! application, and every reply crosses another one on the way back. Those two
//! hops are pure overhead — no parsing, no business logic — but each of them is
//! a task wakeup, and what a task wakeup costs depends entirely on the runtime:
//! a `current_thread` runtime pushes the woken task onto a local queue the
//! current thread is about to drain, whereas a multi-threaded runtime may have
//! to unpark a sleeping worker on another core.
//!
//! Every group runs on `current_thread` and on multi-threaded runtimes of 1, 2
//! and 4 workers. The single-worker multi-threaded runtime is there to separate
//! the cost of the work-stealing scheduler itself from the cost of actually
//! crossing cores.
//!
//! * `channel_roundtrip` — the two channel hops on their own: no sockets, no
//!   codec, no session logic, one message in flight. `events_1` delivers only
//!   the `MessageReceived` the application acts on; `events_4` also delivers
//!   the `RawMessageSent`, `SessionState` and `RawMessageReceived` events that
//!   a real session emits for the same round trip, which is what an application
//!   actually pays.
//! * `channel_concurrent` — the same hops across 8, 32 and 128 sessions, each
//!   ping-ponging independently in its own pair of tasks. One message in flight
//!   is a strict ping-pong, and a ping-pong never leaves a single thread
//!   whichever runtime it is on: the woken task is run by the worker that woke
//!   it, and the other workers have nothing to steal. This is the group where
//!   the worker count can actually matter.
//! * `channel_pipelined` — one session with 32 messages in flight, so the
//!   application is woken once per batch instead of once per message. The gap
//!   between this and `channel_roundtrip` is the wakeup, not the channel.
//! * `session_roundtrip` — the whole path for comparison: a babelfix initiator
//!   sends a NewOrderSingle to a babelfix acceptor over loopback TCP, the
//!   acceptor's application answers with an ExecutionReport, and the initiator's
//!   application receives it. Framing, checksums, parsing, `write`/`read` and
//!   four channel hops. This is the floor the library imposes on an echo
//!   application. Three baselines sit alongside it and account for all of it:
//!   `codec_only` is the serialising and parsing a round trip forces with no
//!   socket and no executor; `tcp_only` is a bare loopback round trip of the
//!   same size with no FIX in it at all; and `sans_io` is two real sessions
//!   exchanging bytes directly through `babelfix-core`, with no socket, no
//!   executor, no task wakeup and no channel. The socket dominates, and by how
//!   much is a property of the platform, so re-run these on the one you care
//!   about before drawing conclusions from the ratio.
//!
//!   Read them as a decomposition. On the machine this was written on:
//!
//!   | | µs | what it adds |
//!   |---|---|---|
//!   | `codec_only` | 4.8 | serialise and parse, twice each way |
//!   | `sans_io` | 8.3 | + the session layer: sequencing, headers, framing |
//!   | `tcp_only` | 19.6 | (the socket, on its own) |
//!   | `current_thread` | 37.4 | + the socket, the tasks and four channel hops |
//!
//!   So the protocol costs about 3.5µs on top of the codec, and everything
//!   else — roughly 29µs, or four fifths of the round trip — is transport and
//!   delivery. That is the part `babelfix-core` exists to let you replace, and
//!   it is also why removing the channels alone would have been a rounding
//!   error: the two hops are ~1.4µs of the 29.
//! * `session_concurrent` — the whole path again, over 8, 32 and 128
//!   simultaneous loopback sessions. Both ends of every session run on the
//!   runtime under test, so this is the closest thing here to what an engine
//!   carrying real session load has to schedule.
//!
//! # A note on the harness
//!
//! Each measured loop runs in a spawned task ([`Workload`]/[`bench_workload`])
//! rather than directly in `block_on`. This is not tidiness: waking the
//! `block_on` future goes through the runtime's unpark path, which costs a
//! syscall, and paying that on every round trip adds roughly 12µs on a
//! `current_thread` runtime and 4µs on a multi-threaded one — an artefact of
//! the harness some twenty times larger than the thing being measured, and one
//! that reverses the ranking of the executors. Application and session tasks
//! are spawned tasks in production, so the loop is one here too.

use std::hint::black_box;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use babelfix::driver::SessionDriver;
use babelfix::message::builder;
use babelfix::schema::FIX_4_4 as FIX44;
use babelfix::session::{
  Command, Event, Session, SessionCommand, SessionEvent, SessionHandle,
  SessionIdentifier, SessionState,
};
use babelfix::{endpoint, repository};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};

/// Depth the endpoint gives both session channels, mirrored here so the
/// synthetic channels behave like the real ones.
const CHANNEL_DEPTH: usize = 100;

/// A heartbeat firing mid-measurement is an outlier with no relevance to what
/// is being measured, so the benchmark sessions are configured never to send
/// one.
const NO_HEARTBEATS: Duration = Duration::from_secs(3600);

/// Messages in flight for the pipelined group.
const PIPELINE_DEPTH: usize = 32;

/// Session counts for the concurrent groups. One session is a strict ping-pong
/// with no parallelism to exploit; these are the loads at which the runtime has
/// something to spread across workers.
const SESSION_COUNTS: [usize; 3] = [8, 32, 128];

/// Round trips each session performs per sample in `channel_concurrent`, to
/// amortise the cost of releasing and rejoining the session drivers.
const ROUNDS_PER_SESSION: usize = 16;

static REPO: OnceLock<Arc<repository::FixRepository>> = OnceLock::new();

fn load_repo() -> Arc<repository::FixRepository> {
  REPO
    .get_or_init(|| Arc::new(repository::orchestrate().unwrap()))
    .clone()
}

fn fix44() -> Arc<repository::FixVersion> {
  load_repo().get_version("FIX.4.4").unwrap()
}

// ---------------------------------------------------------------------------
// Executors under comparison
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Executor {
  CurrentThread,
  MultiThread(usize),
}

const EXECUTORS: [Executor; 4] = [
  Executor::CurrentThread,
  Executor::MultiThread(1),
  Executor::MultiThread(2),
  Executor::MultiThread(4),
];

impl Executor {
  fn name(self) -> String {
    match self {
      Executor::CurrentThread => "current_thread".to_string(),
      Executor::MultiThread(workers) => format!("multi_thread_{workers}"),
    }
  }

  fn build(self) -> tokio::runtime::Runtime {
    match self {
      Executor::CurrentThread => tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap(),
      Executor::MultiThread(workers) => {
        tokio::runtime::Builder::new_multi_thread()
          .worker_threads(workers)
          .enable_all()
          .build()
          .unwrap()
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One unit of work, repeated for the duration of a measurement.
trait Workload: Send + 'static {
  fn step(&mut self) -> impl Future<Output = ()> + Send;
}

/// Time `iters` steps of `workload` on `rt`.
///
/// The loop is run in a spawned task, and only its own elapsed time is
/// reported, so neither the spawn nor the `block_on` around it is charged to
/// the measurement. See the module documentation for why this matters.
///
/// The workload lives in `slot` between samples because it has to be moved into
/// the spawned task and back out again.
fn bench_workload<W: Workload>(
  rt: &tokio::runtime::Runtime,
  slot: &mut Option<W>,
  iters: u64,
) -> Duration {
  let mut workload = slot.take().expect("workload lost by a previous sample");

  let (elapsed, workload) = rt.block_on(async move {
    tokio::spawn(async move {
      let start = Instant::now();
      for _ in 0..iters {
        workload.step().await;
      }
      (start.elapsed(), workload)
    })
    .await
    .expect("benchmark task panicked")
  });

  *slot = Some(workload);
  elapsed
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn bench_session(fix: &Arc<repository::FixVersion>) -> Session {
  Session {
    next_out_seq_num: 1,
    next_in_seq_num: 1,
    heartbeat_interval: NO_HEARTBEATS,
    fix_version: fix.clone(),
    time_precision: Default::default(),
  }
}

/// A small NewOrderSingle — deliberately modest, so the measurement is
/// dominated by delivery rather than by the size of what is delivered.
///
/// The session populates `MsgSeqNum`, `SendingTime` and the CompIDs itself, so
/// they are left out here.
fn new_order(fix: &Arc<repository::FixVersion>) -> builder::Message {
  let mut msg = builder::Message::new(fix.clone(), "D").unwrap();
  msg.body.set_tag(FIX44::Fields::ClOrdID, "BENCH-ORDER-1");
  msg.body.set_tag(FIX44::Fields::HandlInst, "1");
  msg.body.set_tag(FIX44::Fields::Symbol, "ESH4");
  msg.body.set_tag(FIX44::Fields::Side, "1");
  msg.body.set_tag(FIX44::Fields::OrderQty, 100i64);
  msg.body.set_tag(FIX44::Fields::OrdType, "2");
  msg.body.set_tag(FIX44::Fields::Price, 4782.25f64);
  msg
    .body
    .set_tag(FIX44::Fields::TransactTime, "20231215-14:30:22.119");
  msg
}

/// The reply the application under benchmark sends back for every order.
fn exec_report(fix: &Arc<repository::FixVersion>) -> builder::Message {
  let mut msg = builder::Message::new(fix.clone(), "8").unwrap();
  msg.body.set_tag(FIX44::Fields::OrderID, "BENCH-ORD-1");
  msg.body.set_tag(FIX44::Fields::ClOrdID, "BENCH-ORDER-1");
  msg.body.set_tag(FIX44::Fields::ExecID, "BENCH-EXEC-1");
  msg.body.set_tag(FIX44::Fields::ExecType, "F");
  msg.body.set_tag(FIX44::Fields::OrdStatus, "2");
  msg.body.set_tag(FIX44::Fields::Symbol, "ESH4");
  msg.body.set_tag(FIX44::Fields::Side, "1");
  msg.body.set_tag(FIX44::Fields::LastPx, 4782.25f64);
  msg.body.set_tag(FIX44::Fields::LastQty, 100i64);
  msg.body.set_tag(FIX44::Fields::LeavesQty, 0i64);
  msg.body.set_tag(FIX44::Fields::CumQty, 100i64);
  msg.body.set_tag(FIX44::Fields::AvgPx, 4782.25f64);
  msg
}

/// Answer every `MessageReceived` with a `Send`, ignoring everything else.
///
/// This is the shape of an application written against a
/// [`SessionHandle`](babelfix::session::SessionHandle) with the business logic
/// removed, and it is used for both the synthetic channels and the real
/// session — so the two groups measure the same application behaviour over
/// different amounts of machinery.
fn spawn_echo_app(
  mut events: mpsc::Receiver<SessionEvent>,
  mut commands: mpsc::Sender<SessionCommand>,
  reply: builder::Message,
) {
  tokio::spawn(async move {
    while let Some(event) = events.next().await {
      if let SessionEvent::MessageReceived(msg) = event {
        black_box(&msg);
        if commands
          .send(SessionCommand::Send(reply.clone()))
          .await
          .is_err()
        {
          break;
        }
      }
    }
  });
}

// ---------------------------------------------------------------------------
// Group 1 & 2: the channels on their own
// ---------------------------------------------------------------------------

/// The session's half of a `SessionHandle`: pushes events at the application
/// and reads back the commands it issues, with nothing in between.
struct ChannelPair {
  events: mpsc::Sender<SessionEvent>,
  commands: mpsc::Receiver<SessionCommand>,
  inbound: builder::Message,
  /// The events a real session emits alongside `MessageReceived` for a single
  /// inbound message. Empty when measuring the two hops in isolation.
  filler: Vec<SessionEvent>,
}

impl ChannelPair {
  /// Must be called from within the runtime being measured: it spawns the
  /// application task.
  fn new(fix: &Arc<repository::FixVersion>, events_per_message: usize) -> Self {
    let (events_tx, events_rx) = mpsc::channel(CHANNEL_DEPTH);
    let (commands_tx, commands_rx) = mpsc::channel(CHANNEL_DEPTH);
    spawn_echo_app(events_rx, commands_tx, exec_report(fix));

    let inbound = new_order(fix);
    let filler = match events_per_message {
      1 => Vec::new(),
      4 => {
        let session = bench_session(fix);
        let wire = inbound.as_message().unwrap();
        vec![
          SessionEvent::RawMessageSent(wire.clone(), session.clone()),
          SessionEvent::SessionState(session.clone()),
          SessionEvent::RawMessageReceived(wire, session),
        ]
      }
      n => panic!("unsupported event fan-out: {n}"),
    };

    Self {
      events: events_tx,
      commands: commands_rx,
      inbound,
      filler,
    }
  }

  /// Deliver one message to the application.
  async fn deliver(&mut self) {
    for event in &self.filler {
      self.events.send(event.clone()).await.unwrap();
    }
    self
      .events
      .send(SessionEvent::MessageReceived(self.inbound.clone()))
      .await
      .unwrap();
  }

  /// Collect the reply the application sends back.
  async fn collect(&mut self) {
    black_box(self.commands.next().await.unwrap());
  }

  /// Deliver one message and wait for the application's reply.
  async fn roundtrip(&mut self) {
    self.deliver().await;
    self.collect().await;
  }

  /// Deliver `PIPELINE_DEPTH` messages before collecting any reply. The
  /// application is woken once for a batch rather than once per message, so
  /// what remains is closer to the cost of the channel itself.
  async fn pipelined(&mut self) {
    for _ in 0..PIPELINE_DEPTH {
      self
        .events
        .send(SessionEvent::MessageReceived(self.inbound.clone()))
        .await
        .unwrap();
    }
    for _ in 0..PIPELINE_DEPTH {
      black_box(self.commands.next().await.unwrap());
    }
  }
}

impl Workload for ChannelPair {
  fn step(&mut self) -> impl Future<Output = ()> + Send {
    self.roundtrip()
  }
}

/// A [`ChannelPair`] measured with [`PIPELINE_DEPTH`] messages in flight.
struct Pipelined(ChannelPair);

impl Workload for Pipelined {
  fn step(&mut self) -> impl Future<Output = ()> + Send {
    self.0.pipelined()
  }
}

/// Several sessions' worth of channels, each ping-ponging in its own pair of
/// tasks, all running at the same time.
///
/// One message in flight is a strict ping-pong: exactly one task is runnable at
/// any instant, so a multi-threaded runtime hands the work straight back to the
/// worker that woke it and the extra workers stay parked. Running many sessions
/// at once is what puts independent work in the queues for the other workers to
/// take, and what makes the number of workers mean anything.
///
/// Each session therefore gets its own driver task rather than being fed from
/// the measured task. Driving them all from one task would serialise the group
/// behind that task — every message would pay for its clone and its send in the
/// same place — and no number of workers could improve on it. That measures the
/// harness, not the runtime.
struct ChannelFanOut {
  /// Releases each driver to run [`ROUNDS_PER_SESSION`] round trips.
  go: Vec<mpsc::Sender<()>>,
  /// Signalled by each driver once it has finished its round trips.
  done: Vec<mpsc::Receiver<()>>,
}

impl ChannelFanOut {
  /// Must be called from within the runtime being measured: it spawns two tasks
  /// per session.
  fn new(
    fix: &Arc<repository::FixVersion>,
    sessions: usize,
    events_per_message: usize,
  ) -> Self {
    let mut go = Vec::with_capacity(sessions);
    let mut done = Vec::with_capacity(sessions);

    for _ in 0..sessions {
      let mut pair = ChannelPair::new(fix, events_per_message);
      let (go_tx, mut go_rx) = mpsc::channel::<()>(1);
      let (mut done_tx, done_rx) = mpsc::channel::<()>(1);

      tokio::spawn(async move {
        while go_rx.next().await.is_some() {
          for _ in 0..ROUNDS_PER_SESSION {
            pair.roundtrip().await;
          }
          if done_tx.send(()).await.is_err() {
            break;
          }
        }
      });

      go.push(go_tx);
      done.push(done_rx);
    }

    Self { go, done }
  }
}

impl Workload for ChannelFanOut {
  /// Release every session, then wait for all of them.
  ///
  /// The handshake costs two channel operations per session per sample, which
  /// is why each release is worth [`ROUNDS_PER_SESSION`] round trips: enough to
  /// leave the coordination well under a tenth of what is measured, and it
  /// costs every executor the same.
  async fn step(&mut self) {
    for go in self.go.iter_mut() {
      go.send(()).await.unwrap();
    }
    for done in self.done.iter_mut() {
      done.next().await.expect("session driver stopped");
    }
  }
}

fn bench_channel_roundtrip(c: &mut Criterion) {
  let fix = fix44();
  let mut group = c.benchmark_group("channel_roundtrip");

  // Everything a round trip does to the payload, with no channel and no
  // executor: the inbound message is cloned into the event, the reply is cloned
  // into the command, and both are dropped. `builder::Message` carries a
  // `HashMap` per block, so this is not free, and it is charged to every
  // benchmark below — subtract it to get the cost of the delivery alone.
  {
    let inbound = new_order(&fix);
    let reply = exec_report(&fix);
    group.bench_function("payload_clone_only", |b| {
      b.iter(|| {
        black_box(black_box(&inbound).clone());
        black_box(black_box(&reply).clone());
      })
    });
  }

  for executor in EXECUTORS {
    for events_per_message in [1usize, 4] {
      let rt = executor.build();
      let mut pair =
        Some(rt.block_on(async { ChannelPair::new(&fix, events_per_message) }));

      group.bench_function(
        format!("{}/events_{events_per_message}", executor.name()),
        |b| b.iter_custom(|iters| bench_workload(&rt, &mut pair, iters)),
      );
    }
  }

  group.finish();
}

fn bench_channel_concurrent(c: &mut Criterion) {
  let fix = fix44();
  let mut group = c.benchmark_group("channel_concurrent");

  for sessions in SESSION_COUNTS {
    group
      .throughput(Throughput::Elements((sessions * ROUNDS_PER_SESSION) as u64));
    for executor in EXECUTORS {
      let rt = executor.build();
      let mut fanout =
        Some(rt.block_on(async { ChannelFanOut::new(&fix, sessions, 4) }));

      group.bench_function(
        format!("{sessions}_sessions/{}", executor.name()),
        |b| b.iter_custom(|iters| bench_workload(&rt, &mut fanout, iters)),
      );
    }
  }

  group.finish();
}

fn bench_channel_pipelined(c: &mut Criterion) {
  let fix = fix44();
  let mut group = c.benchmark_group("channel_pipelined");
  group.throughput(Throughput::Elements(PIPELINE_DEPTH as u64));

  for executor in EXECUTORS {
    let rt = executor.build();
    let mut pair =
      Some(Pipelined(rt.block_on(async { ChannelPair::new(&fix, 1) })));

    group.bench_function(executor.name(), |b| {
      b.iter_custom(|iters| bench_workload(&rt, &mut pair, iters))
    });
  }

  group.finish();
}

// ---------------------------------------------------------------------------
// Group 3: the same round trip over a real session
// ---------------------------------------------------------------------------

/// One or more babelfix initiators connected to a babelfix acceptor over
/// loopback, each acceptor session driven by an echo application.
///
/// Both ends of every session run on the runtime under test, so a measurement
/// covers the acceptor's session task, its application, the initiator's session
/// task and the initiator's application — all competing for the same workers.
struct Loopback {
  clients: Vec<SessionHandle>,
  order: builder::Message,
  /// Dropping the endpoint's command sender closes its accept loop, and
  /// dropping an initiator's command sender closes its reconnect loop, so both
  /// are held for the lifetime of the benchmark.
  _endpoint_commands: mpsc::Sender<endpoint::EndpointCommand>,
  _client_cancels: Vec<mpsc::Sender<endpoint::EndpointCommand>>,
}

impl Loopback {
  /// Must be called from within the runtime being measured: both sides run on
  /// it, which is the whole point of the comparison.
  async fn establish(
    fix: &Arc<repository::FixVersion>,
    sessions: usize,
  ) -> Self {
    let repo = load_repo();

    let endpoint::Endpoint {
      commands,
      mut events,
      local_addr,
      join_handle,
    } = endpoint::serve(
      ("127.0.0.1", 0),
      repo.clone(),
      endpoint::EndpointConfig::default(),
    )
    .await
    .unwrap();
    // `serve` has already spawned the accept loop; this handle exists only to
    // observe its exit, which the benchmark has no use for.
    drop(join_handle);

    let acceptor_fix = fix.clone();
    tokio::spawn(async move {
      while let Some(event) = events.next().await {
        match event {
          endpoint::EndpointEvent::NewSession { response, .. } => {
            let _ = response.send(Ok(bench_session(&acceptor_fix)));
          }
          endpoint::EndpointEvent::SessionConnected(handle) => {
            spawn_echo_app(
              handle.events,
              handle.tx,
              exec_report(&acceptor_fix),
            );
          }
          _ => {}
        }
      }
    });

    let mut clients = Vec::with_capacity(sessions);
    let mut cancels = Vec::with_capacity(sessions);

    for i in 0..sessions {
      let endpoint::Initiator {
        session: mut client,
        commands: cancel,
        ..
      } = endpoint::connect(
        vec![("127.0.0.1".to_string(), local_addr.port())],
        repo.clone(),
        SessionIdentifier {
          begin_string: fix.begin_string.clone(),
          sender_comp_id: format!("BENCHCLIENT{i}"),
          target_comp_id: "BENCHSERVER".to_string(),
        },
        bench_session(fix),
        endpoint::EndpointConfig::default(),
      )
      .unwrap();

      // Logon is followed by a TestRequest/Heartbeat exchange; measure settled
      // sessions rather than ones still recovering.
      loop {
        match client
          .events
          .next()
          .await
          .expect("session ended during logon")
        {
          SessionEvent::RecoveryCompleted => break,
          SessionEvent::Disconnected => panic!("disconnected during logon"),
          _ => {}
        }
      }

      clients.push(client);
      cancels.push(cancel);
    }

    Self {
      clients,
      order: new_order(fix),
      _endpoint_commands: commands,
      _client_cancels: cancels,
    }
  }

  /// Put an order on every session, then wait for every ExecutionReport.
  ///
  /// Sending all the orders before collecting any reply is what keeps every
  /// session busy at once; collecting each one before sending the next would
  /// serialise the whole thing back down to a single ping-pong however many
  /// sessions there are.
  ///
  /// The intervening events — the initiator's own `RawMessageSent`, then
  /// `SessionState` and `RawMessageReceived` for the reply — are drained here
  /// because a real application has to drain them too: they share the channel
  /// with the event it cares about.
  async fn roundtrip(&mut self) {
    for client in self.clients.iter_mut() {
      client
        .tx
        .send(SessionCommand::Send(self.order.clone()))
        .await
        .unwrap();
    }

    for client in self.clients.iter_mut() {
      loop {
        match client.events.next().await.expect("session ended") {
          SessionEvent::MessageReceived(msg) => {
            black_box(msg);
            break;
          }
          SessionEvent::Disconnected => panic!("session disconnected"),
          _ => {}
        }
      }
    }
  }
}

impl Workload for Loopback {
  fn step(&mut self) -> impl Future<Output = ()> + Send {
    self.roundtrip()
  }
}

/// The message work one round trip forces on the two sessions, with no
/// channels, no sockets and no executor.
///
/// Each side serialises what it sends — [`babelfix::session::Session::send`]
/// calls `as_message`, the encoder calls `write_to` — and parses what it
/// receives, where the decoder's `from_bytes_delimited` is followed by
/// `builder::Message::from_message` before the application sees anything.
///
/// That second conversion is not optional and not something an application can
/// decline. `SessionManager` performs it on every inbound message, and
/// `SessionCommand::Send` accepts nothing but a `builder::Message`, so the flat
/// `FixMessage` representation is never what a session hands to, or takes from,
/// application code. Both are on this path twice per round trip.
///
/// This is a lower bound on the real cost: it leaves out the decoder's separate
/// version/length and checksum passes over the same bytes, the checksum sum
/// itself, and the `FixMessage` clones carried by the `RawMessageSent` and
/// `RawMessageReceived` events.
fn codec_roundtrip(
  fix: &Arc<repository::FixVersion>,
  order: &builder::Message,
  reply: &builder::Message,
) {
  for msg in [order, reply] {
    let wire = msg.as_message().unwrap();
    let mut buf = bytes::BytesMut::new();
    wire.write_to(&mut buf, b'\x01').unwrap();

    let (decoded, _) = babelfix::FixMessage::from_bytes_delimited(
      fix.clone(),
      buf.freeze(),
      b'\x01',
    )
    .unwrap();
    black_box(builder::Message::from_message(&decoded).unwrap());
  }
}

/// The bytes a message occupies on the wire.
fn wire_bytes(msg: &builder::Message) -> Vec<u8> {
  let mut buf = bytes::BytesMut::new();
  msg
    .as_message()
    .unwrap()
    .write_to(&mut buf, b'\x01')
    .unwrap();
  buf.to_vec()
}

/// A bare loopback TCP round trip, carrying the same number of bytes each way
/// as a session round trip and with the same `TCP_NODELAY` setting, but no FIX
/// anywhere in it: no codec, no session, no session channels.
///
/// Whatever this costs, every session round trip pays it before doing anything
/// useful. It is the floor under `session_roundtrip`.
struct TcpEcho {
  client: tokio::net::TcpStream,
  request: Vec<u8>,
  /// Reused, so the baseline is not inflated by an allocation per round trip.
  reply_buf: Vec<u8>,
}

impl TcpEcho {
  async fn establish(request: Vec<u8>, reply: Vec<u8>) -> Self {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();

    let request_len = request.len();
    let server_reply = reply.clone();
    tokio::spawn(async move {
      use tokio::io::{AsyncReadExt, AsyncWriteExt};
      let mut buf = vec![0u8; request_len];
      loop {
        if server.read_exact(&mut buf).await.is_err() {
          break;
        }
        if server.write_all(&server_reply).await.is_err() {
          break;
        }
      }
    });

    Self {
      client,
      request,
      reply_buf: vec![0u8; reply.len()],
    }
  }
}

impl Workload for TcpEcho {
  async fn step(&mut self) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    self.client.write_all(&self.request).await.unwrap();
    self.client.read_exact(&mut self.reply_buf).await.unwrap();
    black_box(&self.reply_buf);
  }
}

fn bench_session_roundtrip(c: &mut Criterion) {
  let fix = fix44();
  let order = new_order(&fix);
  let reply = exec_report(&fix);
  let mut group = c.benchmark_group("session_roundtrip");
  group.throughput(Throughput::Elements(1));

  // Two baselines for the full path below. Neither involves a session: the
  // first is the message work a round trip forces, the second is the socket
  // it forces, and between them they account for most of it.
  {
    group.bench_function("codec_only", |b| {
      b.iter(|| codec_roundtrip(&fix, black_box(&order), black_box(&reply)))
    });

    // Any executor will do: the full path below is the same on all four, so
    // the socket, not the scheduler, is what this is isolating.
    let rt = Executor::CurrentThread.build();
    let mut echo = Some(
      rt.block_on(TcpEcho::establish(wire_bytes(&order), wire_bytes(&reply))),
    );
    group.bench_function("tcp_only", |b| {
      b.iter_custom(|iters| bench_workload(&rt, &mut echo, iters))
    });
  }

  // The same round trip through the sans-io core: two real sessions, real
  // framing, real sequence checking, but no socket, no executor, no task
  // wakeup and no channel. Everything `session_roundtrip` measures except the
  // transport and the scheduling.
  //
  // The gap between this and `codec_only` is what the session layer itself
  // costs. The gap between this and the executor rows below is what the
  // transport and the delivery machinery cost — and it is the part a latency
  // user is buying the right to replace.
  {
    let mut pair = SansIo::establish(&fix);
    group.bench_function("sans_io", |b| b.iter(|| pair.roundtrip()));
  }

  for executor in EXECUTORS {
    let rt = executor.build();
    let mut loopback = Some(rt.block_on(Loopback::establish(&fix, 1)));

    group.bench_function(executor.name(), |b| {
      b.iter_custom(|iters| bench_workload(&rt, &mut loopback, iters))
    });
  }

  group.finish();
}

// ---------------------------------------------------------------------------
// The same round trip with no I/O at all
// ---------------------------------------------------------------------------

/// Two [`SessionDriver`]s exchanging bytes directly, with an echo application
/// wired straight into the event sink.
///
/// This is the shape from `babelfix-core`: the caller owns the file descriptor,
/// so here there simply isn't one. A round trip is the initiator sending a
/// NewOrderSingle, the acceptor's application answering with an ExecutionReport,
/// and the initiator receiving it — the same work `session_roundtrip` measures,
/// minus everything the transport imposes.
struct SansIo {
  initiator: SessionDriver,
  acceptor: SessionDriver,
  order: builder::Message,
  reply: builder::Message,
  wire: Vec<u8>,
}

impl SansIo {
  fn establish(fix: &Arc<repository::FixVersion>) -> Self {
    let mut initiator = Self::driver(fix, "CLIENT", "SERVER");
    let mut acceptor = Self::driver(fix, "SERVER", "CLIENT");
    let now = Instant::now();
    let mut ignore = ();

    // Logon exchange. The acceptor reads the first frame to learn who is
    // calling, which is the part of the handshake that still lives outside the
    // state machine.
    initiator.send_logon(Self::logon(fix), &mut ignore).unwrap();
    let wire = Self::take(&mut initiator);
    let (logon_from_client, rest) = Self::split_one(fix, &wire);
    assert!(rest.is_empty());

    acceptor.send_logon(Self::logon(fix), &mut ignore).unwrap();
    let _ = acceptor.start(logon_from_client, now, &mut ignore).unwrap();

    let wire = Self::take(&mut acceptor);
    let (logon_from_server, rest) = Self::split_one(fix, &wire);
    let _ = initiator
      .start(logon_from_server, now, &mut ignore)
      .unwrap();
    let _ = initiator.on_bytes(now, &rest, &mut ignore).unwrap();

    // Settle the synchronisation TestRequests both sides send after logon, so
    // the measured loop is pure application traffic.
    for _ in 0..2 {
      let wire = Self::take(&mut initiator);
      let _ = acceptor.on_bytes(now, &wire, &mut ignore).unwrap();
      let wire = Self::take(&mut acceptor);
      let _ = initiator.on_bytes(now, &wire, &mut ignore).unwrap();
    }

    Self {
      initiator,
      acceptor,
      order: new_order(fix),
      reply: exec_report(fix),
      wire: Vec::with_capacity(1024),
    }
  }

  fn driver(
    fix: &Arc<repository::FixVersion>,
    us: &str,
    them: &str,
  ) -> SessionDriver {
    let session_id = SessionIdentifier {
      begin_string: fix.begin_string.clone(),
      sender_comp_id: us.to_string(),
      target_comp_id: them.to_string(),
    };
    let state =
      SessionState::new(session_id, bench_session(fix), Instant::now());
    SessionDriver::new(state, load_repo(), None, chrono::Utc::now)
  }

  fn logon(fix: &Arc<repository::FixVersion>) -> builder::Message {
    let mut msg = builder::Message::new(fix.clone(), "A").unwrap();
    msg.body.set_tag(FIX44::Fields::HeartBtInt, "3600");
    msg.body.set_tag(FIX44::Fields::EncryptMethod, "0");
    msg
  }

  fn take(d: &mut SessionDriver) -> Vec<u8> {
    let buf = d.pending_writes();
    let bytes = buf.to_vec();
    buf.clear();
    bytes
  }

  fn split_one(
    fix: &Arc<repository::FixVersion>,
    bytes: &[u8],
  ) -> (builder::Message, Vec<u8>) {
    let mut decoder =
      babelfix::codec::FixDecoder::with_version(load_repo(), None, fix.clone());
    let mut buf = bytes::BytesMut::from(bytes);
    let msg = decoder.decode(&mut buf).unwrap().unwrap();
    (builder::Message::from_message(&msg).unwrap(), buf.to_vec())
  }

  /// One request and one reply, application to application.
  fn roundtrip(&mut self) {
    let now = Instant::now();

    // Initiator sends; the bytes go straight into its buffer.
    let _ = self
      .initiator
      .on_command(now, Command::Send(self.order.clone()), &mut ())
      .unwrap();
    self.wire.clear();
    self.wire.extend_from_slice(self.initiator.pending_writes());
    self.initiator.pending_writes().clear();

    // The acceptor's "application" answers inline from the event sink, which is
    // the whole point: no queue, no wakeup, no clone.
    let reply = self.reply.clone();
    let mut answered = None;
    {
      let mut sink = |event: Event<'_>| {
        if matches!(event, Event::MessageReceived(_)) {
          answered = Some(reply.clone());
        }
        Ok(())
      };
      let _ = self.acceptor.on_bytes(now, &self.wire, &mut sink).unwrap();
    }
    let _ = self
      .acceptor
      .on_command(now, Command::Send(answered.unwrap()), &mut ())
      .unwrap();

    self.wire.clear();
    self.wire.extend_from_slice(self.acceptor.pending_writes());
    self.acceptor.pending_writes().clear();

    let mut received = false;
    {
      let mut sink = |event: Event<'_>| {
        if matches!(event, Event::MessageReceived(_)) {
          received = true;
        }
        Ok(())
      };
      let _ = self.initiator.on_bytes(now, &self.wire, &mut sink).unwrap();
    }
    assert!(black_box(received));
  }
}

fn bench_session_concurrent(c: &mut Criterion) {
  let fix = fix44();
  let mut group = c.benchmark_group("session_concurrent");
  // A sample is one message on each session, so time-per-iteration is the
  // latency of a whole round of traffic and the throughput figure is the
  // per-message cost.
  group.sample_size(30);

  for sessions in SESSION_COUNTS {
    group.throughput(Throughput::Elements(sessions as u64));
    for executor in EXECUTORS {
      let rt = executor.build();
      let mut loopback = Some(rt.block_on(Loopback::establish(&fix, sessions)));

      group.bench_function(
        format!("{sessions}_sessions/{}", executor.name()),
        |b| b.iter_custom(|iters| bench_workload(&rt, &mut loopback, iters)),
      );
    }
  }

  group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
  benches,
  bench_channel_roundtrip,
  bench_channel_concurrent,
  bench_channel_pipelined,
  bench_session_roundtrip,
  bench_session_concurrent,
);

criterion_main!(benches);
