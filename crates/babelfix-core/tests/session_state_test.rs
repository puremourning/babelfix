//! Protocol logic, driven directly with a clock the test advances by hand.
//!
//! These exercise [`SessionState`] with no socket, no runtime and no real time,
//! so they can assert things the loopback integration tests cannot: what
//! happens at exactly the third missed heartbeat, or that a message rejected
//! for a bad sequence number still counts as evidence the peer is alive.
//!
//! The session allocates no outbound sequence numbers: it asks for each admin
//! message with `AdminSendRequired`. [`Harness`] plays the application's part,
//! numbering each one and sending it straight back, which is what a sequencer
//! with an instant store does.

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use babelfix_schema::fields::{
  ClOrdID, MsgSeqNum, OrigSendingTime, SenderCompID, SendingTime, TargetCompID,
};
use babelfix_schema::tags;
use fix::message::{Dictionaries, Dictionary, Message};
use fix::session::{
  Command, Event, Progress, SessionConfig, SessionIdentifier, SessionOutput,
  SessionState, Unstamped,
};

static DICTS: LazyLock<Arc<Dictionaries>> =
  LazyLock::new(|| Dictionaries::standard().unwrap());

fn fix44() -> Arc<Dictionary> {
  DICTS.get("FIX.4.4").unwrap().clone()
}

const HEARTBEAT: Duration = Duration::from_secs(30);

/// An owned snapshot of an [`Event`], since the real one borrows.
#[derive(Debug, PartialEq)]
enum Seen {
  ConnectionEstablished,
  AdminSendRequired(String),
  LoggedOn,
  RecoveryCompleted,
  RawMessageReceived,
  RawMessageSent(String),
  MessageReceived { seq_num: u64, msg_type: String },
  InboundAdvanced(u64),
  ResendRequest { begin_seq_no: u64, end_seq_no: u64 },
  Disconnected,
}

/// Records what the session emits, and stamps `SendingTime` the way a real
/// driver would — from a clock the test controls.
#[derive(Default)]
struct Recorder {
  /// `(msg_type, MsgSeqNum)` for everything transmitted, in order.
  sent: Vec<(String, u64)>,
  /// Raw `SendingTime`/`OrigSendingTime` of everything transmitted.
  sent_times: Vec<(String, String)>,
  events: Vec<Seen>,
  /// Admin messages the session asked for, not yet numbered.
  requested: VecDeque<Message>,
  /// Advanced by hand so successive stamps differ.
  clock: u32,
}

impl Recorder {
  /// Message types transmitted since the last [`take_sent`](Self::take_sent).
  fn sent_types(&self) -> Vec<&str> {
    self.sent.iter().map(|(t, _)| t.as_str()).collect()
  }

  fn take_sent(&mut self) -> Vec<(String, u64)> {
    std::mem::take(&mut self.sent)
  }
}

impl SessionOutput for Recorder {
  fn transmit(&mut self, msg: Unstamped<'_>) -> fix::Result<()> {
    // A real driver reads a wall clock here; the important part is that it
    // happens once per message, so the test just needs distinct values.
    let clock = &mut self.clock;
    msg.stamp(|| {
      *clock += 1;
      chrono::DateTime::from_timestamp(1_700_000_000, *clock)
        .expect("valid timestamp")
    });
    Ok(())
  }

  fn event(&mut self, event: Event<'_>) -> fix::Result<()> {
    self.events.push(match event {
      Event::ConnectionEstablished => Seen::ConnectionEstablished,
      Event::AdminSendRequired(msg) => {
        let seen = Seen::AdminSendRequired(msg.msg_type().to_string());
        self.requested.push_back(msg);
        seen
      }
      Event::LoggedOn => Seen::LoggedOn,
      Event::RecoveryCompleted => Seen::RecoveryCompleted,
      Event::RawMessageReceived(..) => Seen::RawMessageReceived,
      Event::RawMessageSent(msg) => {
        // Record the message as it goes on the wire, so the tests can check
        // the SendingTime the output stamped.
        self.sent.push((
          msg.msg_type().to_string(),
          msg.header().req(MsgSeqNum).unwrap(),
        ));
        let sending_time = text(msg.header().raw(tags::SendingTime));
        self.sent_times.push((
          sending_time.clone(),
          text(msg.header().raw(tags::OrigSendingTime)),
        ));
        Seen::RawMessageSent(sending_time)
      }
      Event::MessageReceived { seq_num, msg } => Seen::MessageReceived {
        seq_num,
        msg_type: msg.msg_type().to_string(),
      },
      Event::InboundAdvanced { next_in_seq_num } => {
        Seen::InboundAdvanced(next_in_seq_num)
      }
      Event::ResendRequest {
        begin_seq_no,
        end_seq_no,
        ..
      } => Seen::ResendRequest {
        begin_seq_no,
        end_seq_no,
      },
      Event::Disconnected => Seen::Disconnected,
      _ => unreachable!("unhandled event variant"),
    });
    Ok(())
  }
}

fn text(v: Option<&[u8]>) -> String {
  String::from_utf8_lossy(v.unwrap_or_default()).into_owned()
}

/// Build an inbound frame as if it had arrived off the wire.
fn inbound(msg_type: &str, seq: u64) -> Message {
  inbound_with(msg_type, seq, &[])
}

fn inbound_with(msg_type: &str, seq: u64, fields: &[(u32, &str)]) -> Message {
  let mut msg = Message::new(&fix44(), msg_type);
  msg
    .header_mut()
    .set(MsgSeqNum, seq)
    .set(SenderCompID, "PEER")
    .set(TargetCompID, "US")
    .set_raw(tags::SendingTime, b"20231114-22:13:20.000000000");
  for (tag, value) in fields {
    msg.body_mut().set_raw(*tag, value.as_bytes());
  }
  Message::parse(&fix44(), msg.to_bytes()).unwrap()
}

fn order(id: &str) -> Message {
  let mut order = Message::new(&fix44(), "D");
  order.body_mut().set(ClOrdID, id);
  order
}

/// The state machine plus an application that numbers whatever it is asked to
/// send, from `next_out`, and sends it at once.
struct Harness {
  state: SessionState,
  out: Recorder,
  next_out: u64,
}

impl Harness {
  /// A session whose state machine exists but has exchanged nothing.
  fn new(start: Instant) -> Self {
    let session_id = SessionIdentifier {
      begin_string: "FIX.4.4".into(),
      sender_comp_id: "US".into(),
      target_comp_id: "PEER".into(),
    };
    let mut config = SessionConfig::new(fix44());
    config.heartbeat_interval = HEARTBEAT;
    Self {
      state: SessionState::new(session_id, config, 1, start),
      out: Recorder::default(),
      next_out: 1,
    }
  }

  /// Number and send everything the session asked for.
  fn pump(&mut self, at: Instant) -> Progress {
    while let Some(mut msg) = self.out.requested.pop_front() {
      msg.header_mut().set(MsgSeqNum, self.next_out);
      self.next_out += 1;
      if self
        .state
        .on_command(Command::Send(msg), at, &mut self.out)
        .unwrap()
        .is_close()
      {
        return Progress::Close;
      }
    }
    Progress::Continue
  }

  fn then_pump(&mut self, progress: Progress, at: Instant) -> Progress {
    if progress.is_close() {
      return progress;
    }
    self.pump(at)
  }

  fn message(&mut self, msg: Message, at: Instant) -> Progress {
    let progress = self.state.on_message(msg, at, &mut self.out).unwrap();
    self.then_pump(progress, at)
  }

  fn timeout(&mut self, at: Instant) -> Progress {
    let progress = self.state.on_timeout(at, &mut self.out).unwrap();
    self.then_pump(progress, at)
  }

  fn command(&mut self, cmd: Command, at: Instant) -> Progress {
    let progress = self.state.on_command(cmd, at, &mut self.out).unwrap();
    self.then_pump(progress, at)
  }

  /// Send an application message under the next sequence number.
  fn send(&mut self, mut msg: Message, at: Instant) -> Progress {
    msg.header_mut().set(MsgSeqNum, self.next_out);
    self.next_out += 1;
    self.command(Command::Send(msg), at)
  }

  /// Exchange Logons, as an acceptor would: ask for ours, process theirs, and
  /// send what that asked for.
  fn logon(&mut self, at: Instant) {
    self.state.request_logon(&mut self.out).unwrap();
    let progress = self
      .state
      .start(inbound("A", 1), at, &mut self.out)
      .unwrap();
    assert_eq!(progress, Progress::Continue);
    assert_eq!(self.pump(at), Progress::Continue);
  }
}

/// A session that has exchanged Logons and sent its synchronisation
/// TestRequest, which is where the integration tests all begin.
fn established() -> (Harness, Instant) {
  let start = Instant::now();
  let mut h = Harness::new(start);
  h.logon(start);
  assert_eq!(h.out.sent_types(), vec!["A", "1"]);
  h.out.take_sent();
  h.out.sent_times.clear();
  h.out.events.clear();
  (h, start)
}

#[test]
fn logon_is_followed_by_a_synchronisation_test_request() {
  let start = Instant::now();
  let mut h = Harness::new(start);
  h.logon(start);

  assert_eq!(h.out.sent, vec![("A".into(), 1), ("1".into(), 2)]);
  assert_eq!(h.state.next_in_seq_num(), 2);
  assert!(h.state.is_logged_on());
  assert!(h.out.events.contains(&Seen::LoggedOn));
}

#[test]
fn the_session_numbers_nothing_itself() {
  let start = Instant::now();
  let mut h = Harness::new(start);
  h.state.request_logon(&mut h.out).unwrap();

  // Asked for, not sent: the application has not numbered it yet.
  assert_eq!(h.out.events, vec![Seen::AdminSendRequired("A".into())]);
  assert!(h.out.sent.is_empty());
}

#[test]
fn the_first_message_must_be_the_logon() {
  let start = Instant::now();
  let mut h = Harness::new(start);

  let mut msg = order("too-early");
  msg.header_mut().set(MsgSeqNum, 1u64);
  let err = h
    .state
    .on_command(Command::Send(msg), start, &mut h.out)
    .unwrap_err();
  assert!(
    err.to_string().contains("first message must be a Logon"),
    "{err}"
  );
}

#[test]
fn a_message_without_a_seq_num_is_rejected() {
  let (mut h, start) = established();

  let err = h
    .state
    .on_command(Command::Send(order("unnumbered")), start, &mut h.out)
    .unwrap_err();
  assert!(err.to_string().contains("no MsgSeqNum"), "{err}");
  assert!(h.out.sent.is_empty());
}

#[test]
fn a_seq_num_already_used_is_rejected_and_leaves_the_session_alone() {
  let (mut h, start) = established();

  let mut msg = order("reused");
  msg.header_mut().set(MsgSeqNum, 2u64);
  let err = h
    .state
    .on_command(Command::Send(msg), start, &mut h.out)
    .unwrap_err();
  assert!(err.to_string().contains("not beyond 2"), "{err}");

  // The session carries on, from where it was.
  assert_eq!(h.send(order("next"), start), Progress::Continue);
  assert_eq!(h.out.sent, vec![("D".into(), 3)]);
}

#[test]
fn a_seq_num_may_jump_forward() {
  let (mut h, start) = established();

  h.next_out = 10;
  assert_eq!(h.send(order("jumped"), start), Progress::Continue);
  assert_eq!(h.out.sent, vec![("D".into(), 10)]);
  assert_eq!(h.state.highest_sent(), 10);
}

#[test]
fn a_supplied_sending_time_is_sent_as_is() {
  let (mut h, start) = established();

  let mut msg = order("timed");
  msg
    .header_mut()
    .set_raw(SendingTime, b"20240101-00:00:00.000000000");
  let _ = h.send(msg, start);

  assert_eq!(
    h.out.sent_times[0].0, "20240101-00:00:00.000000000",
    "the SendingTime the application supplied was replaced"
  );
}

#[test]
fn the_outbound_heartbeat_fires_on_its_deadline() {
  let (mut h, start) = established();

  // Nothing is due before the interval elapses.
  assert_eq!(h.state.next_deadline(), Some(start + HEARTBEAT));
  assert_eq!(
    h.timeout(start + HEARTBEAT - Duration::from_millis(1)),
    Progress::Continue
  );
  assert!(h.out.sent_types().is_empty());

  let _ = h.timeout(start + HEARTBEAT);
  assert_eq!(h.out.sent_types(), vec!["0"]);
}

/// While a heartbeat is being persisted, the timer must not ask for another.
#[test]
fn a_heartbeat_is_not_asked_for_twice() {
  let (mut h, start) = established();

  let _ = h.state.on_timeout(start + HEARTBEAT, &mut h.out).unwrap();
  let _ = h
    .state
    .on_timeout(start + 2 * HEARTBEAT, &mut h.out)
    .unwrap();
  let asked: Vec<_> = h
    .out
    .events
    .iter()
    .filter(|e| **e == Seen::AdminSendRequired("0".into()))
    .collect();
  assert_eq!(asked.len(), 1, "{:?}", h.out.events);

  // Once it has been sent, the next one may be asked for.
  let _ = h.pump(start + 2 * HEARTBEAT);
  h.out.events.clear();
  let _ = h
    .state
    .on_timeout(start + 3 * HEARTBEAT, &mut h.out)
    .unwrap();
  assert!(h.out.events.contains(&Seen::AdminSendRequired("0".into())));
}

/// The escalation ladder, at exactly the boundaries. Real time makes this
/// awkward to test; a hand-advanced clock makes it exact.
#[test]
fn three_missed_heartbeats_end_the_session() {
  let (mut h, start) = established();

  // First: noted, nothing sent. The outbound heartbeat fires at the same
  // instant, which is the "0".
  let _ = h.timeout(start + HEARTBEAT);
  assert_eq!(h.out.take_sent().len(), 1);

  // Second: a TestRequest, probing whether the peer is alive.
  let _ = h.timeout(start + 2 * HEARTBEAT);
  let sent = h.out.take_sent();
  let types: Vec<&str> = sent.iter().map(|(t, _)| t.as_str()).collect();
  assert_eq!(types, vec!["0", "1"]);

  // Third: Logout, and the session is over.
  let progress = h.timeout(start + 3 * HEARTBEAT);
  assert_eq!(progress, Progress::Close);
  let sent = h.out.take_sent();
  let types: Vec<&str> = sent.iter().map(|(t, _)| t.as_str()).collect();
  assert_eq!(types, vec!["0", "5"]);
}

/// A Logout the application never sends — a dead store, say — must not leave
/// the session half-closed: it ends after a heartbeat interval regardless.
#[test]
fn a_logout_never_sent_closes_the_session_after_a_heartbeat_interval() {
  let (mut h, start) = established();

  // Ask for a disconnect, and never send the Logout it asks for.
  let _ = h
    .state
    .on_command(Command::Disconnect, start, &mut h.out)
    .unwrap();
  assert!(h.out.events.contains(&Seen::AdminSendRequired("5".into())));
  assert_eq!(h.state.next_deadline(), Some(start + HEARTBEAT));

  // Inbound traffic is ignored while closing.
  let progress = h
    .state
    .on_message(inbound("D", 2), start, &mut h.out)
    .unwrap();
  assert_eq!(progress, Progress::Continue);
  assert!(
    !h.out
      .events
      .iter()
      .any(|e| matches!(e, Seen::MessageReceived { .. })),
    "an application message was delivered while closing"
  );

  let progress = h
    .state
    .on_timeout(start + HEARTBEAT - Duration::from_millis(1), &mut h.out)
    .unwrap();
  assert_eq!(progress, Progress::Continue);
  let progress = h.state.on_timeout(start + HEARTBEAT, &mut h.out).unwrap();
  assert_eq!(progress, Progress::Close);
}

/// Rule one: inbound liveness is credited when the frame arrives, *before* it
/// is validated. A message rejected for a bad sequence number still proves the
/// peer is there, so it must not count towards the missed-heartbeat ladder.
#[test]
fn a_rejected_message_still_counts_as_liveness() {
  let (mut h, start) = established();

  // Two intervals of silence: the session is one step from a TestRequest.
  let _ = h.timeout(start + HEARTBEAT);
  h.out.take_sent();

  // A message arrives with a sequence number far in the future — it will be
  // discarded and provoke a ResendRequest, but it is still evidence of life.
  let at = start + HEARTBEAT + Duration::from_secs(1);
  let _ = h.message(inbound("D", 99), at);
  h.out.take_sent();

  // The inbound clock restarted, so a full interval from *now* passes without
  // escalating to the second-miss TestRequest.
  let _ = h.timeout(at + HEARTBEAT - Duration::from_millis(1));
  let types = h.out.sent_types();
  assert!(
    !types.contains(&"1"),
    "escalated to a TestRequest despite the peer having just spoken: {types:?}"
  );
}

/// Rule two: the outbound heartbeat is deferred only by application messages.
/// A heartbeat sent in reply to the peer's TestRequest is not the session
/// asserting its own liveness on a schedule, and must not reset the timer —
/// otherwise a peer polling us could suppress our heartbeats entirely.
#[test]
fn answering_a_test_request_does_not_defer_our_own_heartbeat() {
  let (mut h, start) = established();

  // Just before the outbound deadline, the peer asks us to prove we are alive.
  let at = start + HEARTBEAT - Duration::from_millis(1);
  let _ = h.message(inbound_with("1", 2, &[(tags::TestReqID, "PING")]), at);
  assert_eq!(
    h.out.sent_types(),
    vec!["0"],
    "should answer with a Heartbeat"
  );
  h.out.take_sent();

  // The scheduled heartbeat must still be due on its original deadline.
  assert_eq!(
    h.state.next_deadline(),
    Some(start + HEARTBEAT),
    "answering a TestRequest moved the outbound heartbeat deadline"
  );
  let _ = h.timeout(start + HEARTBEAT);
  assert_eq!(h.out.sent_types(), vec!["0"]);
}

/// By contrast, an application send *is* evidence of liveness on the wire, so
/// it does defer the next heartbeat.
#[test]
fn an_application_send_defers_the_next_heartbeat() {
  let (mut h, start) = established();

  let at = start + HEARTBEAT / 2;
  let _ = h.send(order("order-1"), at);
  h.out.take_sent();

  // The original outbound deadline passes without a heartbeat: the order
  // already told the peer we are alive. (The inbound timer fires here, which
  // only increments the missed counter and sends nothing.)
  let _ = h.timeout(start + HEARTBEAT);
  assert!(
    !h.out.sent_types().contains(&"0"),
    "sent a heartbeat despite an application message having just gone out"
  );

  // It fires an interval after the send instead.
  let _ = h.timeout(at + HEARTBEAT);
  assert!(h.out.sent_types().contains(&"0"));
}

#[test]
fn a_sequence_gap_provokes_one_open_ended_resend_request() {
  let (mut h, start) = established();

  let _ = h.message(inbound("D", 5), start);
  assert_eq!(h.out.sent_types(), vec!["2"], "expected a ResendRequest");
  h.out.take_sent();

  // A second out-of-sequence message must not provoke a second request.
  let _ = h.message(inbound("D", 6), start);
  assert!(
    h.out.sent_types().is_empty(),
    "a second ResendRequest was sent while one was outstanding"
  );
}

#[test]
fn a_sequence_number_below_the_expected_one_ends_the_session() {
  let (mut h, start) = established();

  // Expecting 2; 1 means one side has lost state and cannot recover.
  let progress = h.message(inbound("D", 1), start);
  assert_eq!(progress, Progress::Close);
  assert_eq!(h.out.sent_types(), vec!["5"], "expected a Logout");
}

#[test]
fn a_test_request_is_answered_with_the_same_test_req_id() {
  let (mut h, start) = established();

  let _ =
    h.message(inbound_with("1", 2, &[(tags::TestReqID, "PING-1")]), start);

  assert_eq!(h.out.sent_types(), vec!["0"]);
}

/// Application messages carry their sequence number, which is what the
/// application records to resume from; admin traffic it never sees advances
/// the inbound sequence too, and says so.
#[test]
fn inbound_messages_report_their_sequence_numbers() {
  let (mut h, start) = established();

  let _ = h.message(inbound("D", 2), start);
  let _ = h.message(inbound("0", 3), start);
  let gap_fill =
    inbound_with("4", 4, &[(tags::GapFillFlag, "Y"), (tags::NewSeqNo, "7")]);
  let _ = h.message(gap_fill, start);
  let _ = h.message(inbound("3", 7), start);

  let seen: Vec<&Seen> = h
    .out
    .events
    .iter()
    .filter(|e| {
      matches!(e, Seen::MessageReceived { .. } | Seen::InboundAdvanced(_))
    })
    .collect();
  assert_eq!(
    seen,
    vec![
      &Seen::MessageReceived {
        seq_num: 2,
        msg_type: "D".into()
      },
      &Seen::InboundAdvanced(4),
      &Seen::InboundAdvanced(7),
      // A session-level Reject is about something we sent: the application
      // needs to see it.
      &Seen::MessageReceived {
        seq_num: 7,
        msg_type: "3".into()
      },
    ]
  );
}

#[test]
fn an_inbound_logout_is_acknowledged_and_closes_the_session() {
  let (mut h, start) = established();

  let progress = h.message(inbound("5", 2), start);
  assert_eq!(progress, Progress::Close);
  assert_eq!(h.out.sent_types(), vec!["5"]);
}

/// A Logout arriving inside a gap must not be acknowledged until the gap is
/// recovered, or the messages still missing are abandoned.
#[test]
fn a_logout_inside_a_gap_is_deferred_until_recovery() {
  let (mut h, start) = established();

  // Open a gap.
  let _ = h.message(inbound("D", 5), start);
  assert_eq!(h.out.take_sent().len(), 1); // the ResendRequest

  // The Logout arrives while messages 2..4 are still missing.
  let progress = h.message(inbound("5", 6), start);
  assert_eq!(
    progress,
    Progress::Continue,
    "the session ended before recovering the gap"
  );
  assert!(
    h.out.sent_types().is_empty(),
    "the Logout was acknowledged before the gap closed"
  );

  // The peer gap-fills over the missing range, which closes it.
  let gap_fill =
    inbound_with("4", 2, &[(tags::GapFillFlag, "Y"), (tags::NewSeqNo, "6")]);
  let progress = h.message(gap_fill, start);
  assert_eq!(progress, Progress::Close);
  assert_eq!(
    h.out.sent_types(),
    vec!["5"],
    "the deferred Logout was not acknowledged once the gap closed"
  );
}

#[test]
fn a_resend_request_reports_a_concrete_end_sequence_number() {
  let (mut h, start) = established();

  // Send three messages so there is something to resend: 3, 4 and 5, after
  // the Logon and the synchronisation TestRequest.
  for i in 0..3 {
    let _ = h.send(order(&format!("order-{i}")), start);
  }
  h.out.take_sent();
  h.out.events.clear();

  // An open-ended request must be resolved against what we have actually sent.
  let _ = h.message(
    inbound_with("2", 2, &[(tags::BeginSeqNo, "2"), (tags::EndSeqNo, "0")]),
    start,
  );

  let resend = h
    .out
    .events
    .iter()
    .find(|e| matches!(e, Seen::ResendRequest { .. }))
    .expect("expected a ResendRequest event");
  assert_eq!(
    resend,
    &Seen::ResendRequest {
      begin_seq_no: 2,
      end_seq_no: 5,
    }
  );
}

/// Every message gets its own `SendingTime`, including several emitted from a
/// single call. This is why the clock is read inside the output rather than
/// handed to the state machine once per pass.
#[test]
fn messages_emitted_together_get_distinct_sending_times() {
  let (mut h, start) = established();

  // Get the peer to one missed heartbeat, so the next timeout crosses both
  // deadlines and emits two messages: the scheduled Heartbeat and the
  // TestRequest probing the silent peer.
  let _ = h.timeout(start + HEARTBEAT);
  h.out.take_sent();
  h.out.events.clear();

  let _ = h.timeout(start + 2 * HEARTBEAT);
  assert_eq!(h.out.sent_types(), vec!["0", "1"]);

  let stamps: Vec<&String> = h
    .out
    .events
    .iter()
    .filter_map(|e| match e {
      Seen::RawMessageSent(t) => Some(t),
      _ => None,
    })
    .collect();

  assert_eq!(stamps.len(), 2, "expected a Heartbeat and a TestRequest");
  assert_ne!(
    stamps[0], stamps[1],
    "two messages from one call shared a SendingTime"
  );
  assert!(
    stamps.iter().all(|s| !s.is_empty()),
    "the output left a SendingTime unstamped: {stamps:?}"
  );
}

/// Start a replay of 2..=5 after sending orders up to 6.
fn replaying() -> (Harness, Instant) {
  let (mut h, start) = established();
  for i in 0..4 {
    let _ = h.send(order(&format!("order-{i}")), start);
  }
  h.out.take_sent();
  let _ = h.message(
    inbound_with("2", 2, &[(tags::BeginSeqNo, "2"), (tags::EndSeqNo, "5")]),
    start,
  );
  h.out.take_sent();
  h.out.sent_times.clear();
  (h, start)
}

fn stored(msg_type: &str, seq: u64) -> Message {
  let mut msg = Message::new(&fix44(), msg_type);
  msg
    .header_mut()
    .set(MsgSeqNum, seq)
    .set_raw(tags::SendingTime, b"20231114-22:13:20.000");
  if msg_type == "V" {
    msg.body_mut().set_raw(tags::MDReqID, b"md-1");
  }
  msg
}

/// On replay, session messages are gap-filled over — except Reject and
/// XMLnonFIX, "the only session messages which may be retransmitted" (FIX
/// Session Layer §4.8.5). Application messages are resent whatever their type,
/// including those whose MsgType looks admin-like (V, h, Y, j).
#[test]
fn replay_resends_reject_and_application_messages_but_gap_fills_the_rest() {
  let (mut h, start) = replaying();

  for (msg_type, seq) in [("V", 2), ("3", 3), ("0", 4), ("j", 5)] {
    let _ = h.command(Command::Replay(stored(msg_type, seq)), start);
  }
  let _ = h.command(Command::ReplayComplete, start);

  let sent: Vec<(String, u64)> = h.out.take_sent();
  let types: Vec<&str> = sent.iter().map(|(t, _)| t.as_str()).collect();
  // V and the Reject resent; the Heartbeat gap-filled (35=4); j resent; then
  // the post-replay TestRequest.
  assert_eq!(types, ["V", "3", "4", "j", "1"], "{sent:?}");
}

/// A replayed message's original `SendingTime` becomes its `OrigSendingTime`,
/// unless the application supplied one, and it is restamped.
#[test]
fn replay_preserves_the_original_sending_time() {
  let (mut h, start) = replaying();

  let _ = h.command(Command::Replay(stored("D", 2)), start);
  let mut with_orig = stored("D", 3);
  with_orig
    .header_mut()
    .set_raw(OrigSendingTime, b"20200101-00:00:00.000");
  let _ = h.command(Command::Replay(with_orig), start);

  let (sending, orig) = &h.out.sent_times[0];
  assert_eq!(orig, "20231114-22:13:20.000");
  assert_ne!(
    sending, "20231114-22:13:20.000",
    "SendingTime was not restamped"
  );
  assert_eq!(h.out.sent_times[1].1, "20200101-00:00:00.000");
}

#[test]
fn a_replayed_message_with_no_sending_time_is_rejected() {
  let (mut h, start) = replaying();

  let mut msg = order("no-time");
  msg.header_mut().set(MsgSeqNum, 2u64);
  let err = h
    .state
    .on_command(Command::Replay(msg), start, &mut h.out)
    .unwrap_err();
  assert!(err.to_string().contains("neither OrigSendingTime"), "{err}");
}

/// Messages sent during a replay wait for it to finish, and may not reuse a
/// number inside the replayed range.
#[test]
fn sends_during_a_replay_wait_for_it_to_finish() {
  let (mut h, start) = replaying();

  let mut inside = order("inside");
  inside.header_mut().set(MsgSeqNum, 5u64);
  assert!(
    h.state
      .on_command(Command::Send(inside), start, &mut h.out)
      .is_err()
  );

  let _ = h.send(order("after"), start); // 7
  assert!(h.out.sent.is_empty(), "sent mid-replay: {:?}", h.out.sent);

  let _ = h.command(Command::ReplayComplete, start);
  let sent = h.out.take_sent();
  // Gap fill over 2..=5, then the queued order, then the TestRequest.
  assert_eq!(
    sent,
    vec![("4".into(), 2), ("D".into(), 7), ("1".into(), 8)],
  );
}
