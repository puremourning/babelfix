//! Two sequenced sessions wired together over byte slices: each side a core
//! driver, a [`Sequencer`], and an application whose store completes every
//! write the moment it is asked.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use babelfix_schema::codesets::Side;
use babelfix_schema::fields::{ClOrdID, OrderQty, Side, Symbol};
use babelfix_schema::tags;
use fix::driver::{
  AcceptorDriver, DriverConfig, InitiatorDriver, SessionDriver,
};
use fix::message::{Dictionaries, Dictionary, Message};
use fix::sequencer::{
  CommandTarget, InboundPolicy, Persist, Resume, SeqEvent, SeqSink, Sequencer,
};
use fix::session::{Event, Progress, SessionConfig, SessionIdentifier};

pub static DICTS: LazyLock<Arc<Dictionaries>> =
  LazyLock::new(|| Dictionaries::standard().unwrap());

pub fn fix44() -> Arc<Dictionary> {
  DICTS.get("FIX.4.4").unwrap().clone()
}

pub const DELIM: u8 = b'|';
pub const HEARTBEAT: Duration = Duration::from_secs(30);

/// A fixed clock. Real drivers pass `Utc::now`; a test wants determinism, and
/// the core cannot read a clock itself in any case.
pub fn clock() -> chrono::DateTime<chrono::Utc> {
  chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

pub fn session() -> SessionConfig {
  let mut session = SessionConfig::new(fix44());
  session.heartbeat_interval = HEARTBEAT;
  session
}

pub fn id(us: &str, them: &str) -> SessionIdentifier {
  SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: us.into(),
    target_comp_id: them.into(),
  }
}

pub fn config() -> DriverConfig {
  DriverConfig {
    dicts: DICTS.clone(),
    delimiter: Some(DELIM),
    clock,
    logon_timeout: HEARTBEAT,
    max_frame_len: fix::codec::DEFAULT_MAX_FRAME_LEN,
  }
}

pub fn order(cl_ord_id: &str) -> Message {
  let mut msg = Message::new(&fix44(), "D");
  msg
    .body_mut()
    .set(ClOrdID, cl_ord_id)
    .set(Symbol, "AAPL")
    .set(Side, Side::Buy)
    .set(OrderQty, 100u64);
  msg
}

/// A sequencer for the client side, `CLIENT` to `SERVER`; which side it is
/// matters only to the CompIDs it stamps, which the session overwrites anyway.
pub fn sequencer(resume: Resume, policy: InboundPolicy) -> Sequencer {
  sequencer_for(&id("CLIENT", "SERVER"), resume, policy)
}

pub fn sequencer_for(
  session_id: &SessionIdentifier,
  resume: Resume,
  policy: InboundPolicy,
) -> Sequencer {
  Sequencer::new(session_id, &session(), resume, policy, clock)
}

/// The application: records what it is told, and the writes it is asked for.
#[derive(Default)]
pub struct App {
  /// Outbound writes asked for and not yet completed.
  pub pending: VecDeque<u64>,
  /// Every outbound write asked for: `(seq_num, msg_type)`.
  pub persisted: Vec<(u64, String)>,
  /// Watermark writes asked for, in order.
  pub watermarks: Vec<u64>,
  /// Watermark writes asked for and not yet completed.
  pub pending_watermarks: VecDeque<u64>,
  /// Hold writes rather than completing them in [`settle`].
  pub hold: bool,
  pub app_messages: Vec<String>,
  pub received_seq_nums: Vec<u64>,
  pub recovery_completed: bool,
  pub logged_on: bool,
  pub resend_requested: Option<(u64, u64)>,
}

impl SeqSink for App {
  fn event(&mut self, event: SeqEvent<'_>) -> fix::Result<()> {
    match event {
      SeqEvent::Persist(Persist::Outbound { seq_num, msg }) => {
        self.pending.push_back(seq_num);
        self.persisted.push((seq_num, msg.msg_type().to_string()));
      }
      SeqEvent::Persist(Persist::Watermark { next_in_seq_num }) => {
        self.watermarks.push(next_in_seq_num);
        self.pending_watermarks.push_back(next_in_seq_num);
      }
      SeqEvent::Session(Event::MessageReceived { seq_num, msg }) => {
        self.received_seq_nums.push(seq_num);
        self.app_messages.push(
          String::from_utf8_lossy(
            msg.body().raw(tags::ClOrdID).unwrap_or_default(),
          )
          .into_owned(),
        );
      }
      SeqEvent::Session(Event::RecoveryCompleted) => {
        self.recovery_completed = true
      }
      SeqEvent::Session(Event::LoggedOn) => self.logged_on = true,
      SeqEvent::Session(Event::ResendRequest {
        begin_seq_no,
        end_seq_no,
        ..
      }) => self.resend_requested = Some((begin_seq_no, end_seq_no)),
      SeqEvent::Session(_) => {}
    }
    Ok(())
  }
}

/// Complete every write asked for — including those that completing earlier
/// ones leads to — unless the application is holding them.
pub fn settle<T: CommandTarget>(
  seq: &mut Sequencer,
  target: &mut T,
  app: &mut App,
  now: Instant,
) -> Progress {
  if app.hold {
    return Progress::Continue;
  }
  while let Some(seq_num) = app.pending.pop_front() {
    if seq.persisted(seq_num, now, target, app).unwrap().is_close() {
      return Progress::Close;
    }
  }
  while let Some(w) = app.pending_watermarks.pop_front() {
    seq.watermark_persisted(w, app).unwrap();
  }
  Progress::Continue
}

/// One end of an established session.
pub struct Peer {
  pub driver: Box<SessionDriver>,
  pub seq: Sequencer,
  pub app: App,
}

impl Peer {
  pub fn take_wire(&mut self) -> Vec<u8> {
    let buf = self.driver.pending_writes();
    let bytes = buf.to_vec();
    buf.clear();
    bytes
  }

  pub fn settle(&mut self, now: Instant) -> Progress {
    settle(&mut self.seq, &mut *self.driver, &mut self.app, now)
  }

  pub fn on_bytes(&mut self, now: Instant, bytes: &[u8]) -> Progress {
    let progress = self
      .driver
      .on_bytes(now, bytes, &mut self.seq.sink(&mut self.app))
      .unwrap();
    if progress.is_close() {
      return progress;
    }
    self.settle(now)
  }

  pub fn send(&mut self, msg: Message, now: Instant) -> u64 {
    let seq_num = self.seq.send(msg, &mut self.app).unwrap();
    let _ = self.settle(now);
    seq_num
  }

  pub fn tick(&mut self, now: Instant) -> Progress {
    let progress = self
      .driver
      .on_tick(now, &mut self.seq.sink(&mut self.app))
      .unwrap();
    if progress.is_close() {
      return progress;
    }
    self.settle(now)
  }
}

/// Hand everything `from` has queued to `to`.
pub fn pump(from: &mut Peer, to: &mut Peer, now: Instant) -> Progress {
  let wire = from.take_wire();
  to.on_bytes(now, &wire)
}

/// Two sequenced sessions that have exchanged Logons, with the inbound policy
/// given.
pub fn established_with(policy: InboundPolicy) -> (Peer, Peer, Instant) {
  let now = Instant::now();

  // The initiator asks for its Logon, which is persisted and sent.
  let mut i_app = App::default();
  let mut i_seq = sequencer(Resume::new(), policy);
  let mut initiator = InitiatorDriver::start(
    id("CLIENT", "SERVER"),
    session(),
    1,
    config(),
    now,
    &mut i_seq.sink(&mut i_app),
  )
  .unwrap();
  let _ = settle(&mut i_seq, &mut initiator, &mut i_app, now);
  let wire = initiator.pending_writes().split().to_vec();

  // The acceptor learns who is calling, and is asked for its reply.
  let mut a_app = App::default();
  let mut a_seq = sequencer_for(&id("SERVER", "CLIENT"), Resume::new(), policy);
  let mut acceptor = AcceptorDriver::new(config(), now);
  acceptor.on_bytes(&wire).unwrap().expect("a Logon");
  let mut a_est = acceptor
    .accept(session(), 1, now, &mut a_seq.sink(&mut a_app))
    .unwrap();
  let _ = settle(&mut a_seq, &mut a_est, &mut a_app, now);
  let wire = a_est.pending_writes().split().to_vec();
  let (a_driver, progress) =
    a_est.start(now, &mut a_seq.sink(&mut a_app)).unwrap();
  assert_eq!(progress, Progress::Continue);

  // The initiator completes the exchange.
  let mut i_est = initiator
    .on_bytes(now, &wire, &mut i_seq.sink(&mut i_app))
    .unwrap()
    .expect("the acceptor's Logon");
  let _ = settle(&mut i_seq, &mut i_est, &mut i_app, now);
  let (i_driver, progress) =
    i_est.start(now, &mut i_seq.sink(&mut i_app)).unwrap();
  assert_eq!(progress, Progress::Continue);

  let mut initiator = Peer {
    driver: i_driver,
    seq: i_seq,
    app: i_app,
  };
  let mut acceptor = Peer {
    driver: a_driver,
    seq: a_seq,
    app: a_app,
  };
  let _ = initiator.settle(now);
  let _ = acceptor.settle(now);
  assert!(initiator.app.logged_on && acceptor.app.logged_on);
  (initiator, acceptor, now)
}

pub fn established() -> (Peer, Peer, Instant) {
  established_with(InboundPolicy::OnDelivery)
}

/// Established, and each side's synchronisation TestRequest answered.
pub fn synchronised() -> (Peer, Peer, Instant) {
  let (mut initiator, mut acceptor, now) = established();
  let _ = pump(&mut initiator, &mut acceptor, now);
  let _ = pump(&mut acceptor, &mut initiator, now);
  let _ = pump(&mut initiator, &mut acceptor, now);
  initiator.take_wire();
  acceptor.take_wire();
  (initiator, acceptor, now)
}
