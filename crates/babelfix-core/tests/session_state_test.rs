//! Protocol logic, driven directly with a clock the test advances by hand.
//!
//! These exercise [`SessionState`] with no socket, no runtime and no real time,
//! so they can assert things the loopback integration tests cannot: what
//! happens at exactly the third missed heartbeat, or that a message rejected
//! for a bad sequence number still counts as evidence the peer is alive.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use fix::message::builder;
use fix::schema::FIX_Latest::Fields;
use fix::session::{
  Command, Event, Progress, Session, SessionIdentifier, SessionOutput,
  SessionState,
};

static FIX_REPO: LazyLock<Arc<fix::repository::FixRepository>> =
  LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

const HEARTBEAT: Duration = Duration::from_secs(30);

/// An owned snapshot of an [`Event`], since the real one borrows.
#[derive(Debug, PartialEq)]
enum Seen {
  ConnectionEstablished,
  RecoveryCompleted,
  SessionState,
  RawMessageReceived,
  RawMessageSent(String),
  MessageReceived(String),
  ResendRequest { begin_seq_no: u32, end_seq_no: u32 },
  Disconnected,
}

/// Records what the session emits, and stamps `SendingTime` the way a real
/// driver would — from a clock the test controls.
#[derive(Default)]
struct Recorder {
  /// `(msg_type, MsgSeqNum)` for everything transmitted, in order.
  sent: Vec<(String, u32)>,
  events: Vec<Seen>,
  /// Advanced by hand so successive stamps differ.
  clock: u32,
}

impl Recorder {
  /// Message types transmitted since the last [`take_sent`](Self::take_sent).
  fn sent_types(&self) -> Vec<&str> {
    self.sent.iter().map(|(t, _)| t.as_str()).collect()
  }

  fn take_sent(&mut self) -> Vec<(String, u32)> {
    std::mem::take(&mut self.sent)
  }
}

impl SessionOutput for Recorder {
  fn transmit(
    &mut self,
    msg: &mut fix::FixMessage,
    _session: &Session,
  ) -> fix::Result<()> {
    // A real driver reads a wall clock here; the important part is that it
    // happens once per message, so the test just needs distinct values.
    self.clock += 1;
    let when = chrono::DateTime::from_timestamp(1_700_000_000, self.clock)
      .expect("valid timestamp");
    fix::codec::stamp_sending_time(msg, when, Default::default())?;
    Ok(())
  }

  fn event(&mut self, event: Event<'_>) -> fix::Result<()> {
    self.events.push(match event {
      Event::ConnectionEstablished => Seen::ConnectionEstablished,
      Event::RecoveryCompleted => Seen::RecoveryCompleted,
      Event::SessionState(_) => Seen::SessionState,
      Event::RawMessageReceived(..) => Seen::RawMessageReceived,
      Event::RawMessageSent(msg, session) => {
        // Record the message as it goes on the wire, so the tests can check
        // the SendingTime the output stamped.
        self.sent.push((
          msg.get_type().to_string(),
          session.next_out_seq_num.saturating_sub(1),
        ));
        Seen::RawMessageSent(
          msg
            .get_tag(Fields::SendingTime)
            .map(|v| v.to_string(&msg.data))
            .unwrap_or_default(),
        )
      }
      Event::MessageReceived(msg) => Seen::MessageReceived(
        msg
          .body
          .tag(Fields::ClOrdID)
          .map(|v| v.as_string())
          .unwrap_or_default(),
      ),
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

/// Build an inbound frame as if it had arrived off the wire.
fn inbound(msg_type: &str, seq: u32) -> fix::FixMessage {
  let mut msg = builder::Message::new(fix44(), msg_type).unwrap();
  msg.header.set_tag(Fields::MsgSeqNum, seq);
  msg.header.set_tag(Fields::SenderCompID, "PEER");
  msg.header.set_tag(Fields::TargetCompID, "US");
  msg
    .header
    .set_tag(Fields::SendingTime, "20231114-22:13:20.000000000");
  msg.as_message().unwrap()
}

fn inbound_with(
  msg_type: &str,
  seq: u32,
  tags: &[(u32, &str)],
) -> fix::FixMessage {
  let mut msg = builder::Message::new(fix44(), msg_type).unwrap();
  msg.header.set_tag(Fields::MsgSeqNum, seq);
  msg.header.set_tag(Fields::SenderCompID, "PEER");
  msg.header.set_tag(Fields::TargetCompID, "US");
  msg
    .header
    .set_tag(Fields::SendingTime, "20231114-22:13:20.000000000");
  for (tag, value) in tags {
    msg.body.set_tag(*tag, *value);
  }
  msg.as_message().unwrap()
}

/// A session that has just accepted a Logon and sent its synchronisation
/// TestRequest, which is where the integration tests all begin.
fn established() -> (SessionState, Recorder, Instant) {
  let start = Instant::now();
  let session_id = SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: "US".into(),
    target_comp_id: "PEER".into(),
  };
  let mut session = Session::new(fix44());
  session.heartbeat_interval = HEARTBEAT;

  let mut state = SessionState::new(session_id, session, start);
  let mut out = Recorder::default();

  let logon = builder::Message::from_message(&inbound("A", 1)).unwrap();
  let progress = state.start(logon, start, &mut out).unwrap();
  assert_eq!(progress, Progress::Continue);
  // Logon acknowledgement is the driver's job; the state machine emits only
  // the synchronisation TestRequest.
  assert_eq!(out.sent_types(), vec!["1"]);
  out.take_sent();
  out.events.clear();

  (state, out, start)
}

#[test]
fn logon_is_followed_by_a_synchronisation_test_request() {
  let start = Instant::now();
  let session_id = SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: "US".into(),
    target_comp_id: "PEER".into(),
  };
  let mut session = Session::new(fix44());
  session.heartbeat_interval = HEARTBEAT;
  let mut state = SessionState::new(session_id, session, start);
  let mut out = Recorder::default();

  let logon = builder::Message::from_message(&inbound("A", 1)).unwrap();
  let _ = state.start(logon, start, &mut out).unwrap();

  assert_eq!(out.sent_types(), vec!["1"]);
  assert_eq!(state.session().next_in_seq_num, 2);
}

#[test]
fn the_outbound_heartbeat_fires_on_its_deadline() {
  let (mut state, mut out, start) = established();

  // Nothing is due before the interval elapses.
  assert_eq!(state.next_deadline(), Some(start + HEARTBEAT));
  assert_eq!(
    state
      .on_timeout(start + HEARTBEAT - Duration::from_millis(1), &mut out)
      .unwrap(),
    Progress::Continue
  );
  assert!(out.sent_types().is_empty());

  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  assert_eq!(out.sent_types(), vec!["0"]);
}

/// The escalation ladder, at exactly the boundaries. Real time makes this
/// awkward to test; a hand-advanced clock makes it exact.
#[test]
fn three_missed_heartbeats_end_the_session() {
  let (mut state, mut out, start) = established();

  // First: noted, nothing sent. The outbound heartbeat fires at the same
  // instant, which is the "0".
  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  assert_eq!(
    out
      .take_sent()
      .iter()
      .map(|(t, _)| t.clone())
      .collect::<Vec<_>>(),
    vec!["0"]
  );

  // Second: a TestRequest, probing whether the peer is alive.
  let _ = state.on_timeout(start + 2 * HEARTBEAT, &mut out).unwrap();
  let sent = out.take_sent();
  let types: Vec<&str> = sent.iter().map(|(t, _)| t.as_str()).collect();
  assert_eq!(types, vec!["0", "1"]);

  // Third: Logout, and the session is over.
  let progress = state.on_timeout(start + 3 * HEARTBEAT, &mut out).unwrap();
  assert_eq!(progress, Progress::Close);
  let sent = out.take_sent();
  let types: Vec<&str> = sent.iter().map(|(t, _)| t.as_str()).collect();
  assert_eq!(types, vec!["0", "5"]);
}

/// Rule one: inbound liveness is credited when the frame arrives, *before* it
/// is validated. A message rejected for a bad sequence number still proves the
/// peer is there, so it must not count towards the missed-heartbeat ladder.
#[test]
fn a_rejected_message_still_counts_as_liveness() {
  let (mut state, mut out, start) = established();

  // Two intervals of silence: the session is one step from a TestRequest.
  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  out.take_sent();

  // A message arrives with a sequence number far in the future — it will be
  // discarded and provoke a ResendRequest, but it is still evidence of life.
  let at = start + HEARTBEAT + Duration::from_secs(1);
  let _ = state.on_message(inbound("D", 99), at, &mut out).unwrap();
  out.take_sent();

  // The inbound clock restarted, so a full interval from *now* passes without
  // escalating to the second-miss TestRequest.
  let _ = state
    .on_timeout(at + HEARTBEAT - Duration::from_millis(1), &mut out)
    .unwrap();
  let types = out.sent_types();
  assert!(
    !types.contains(&"1"),
    "escalated to a TestRequest despite the peer having just spoken: {types:?}"
  );
}

/// Rule two: the outbound heartbeat is deferred only by application commands.
/// A heartbeat sent in reply to the peer's TestRequest is not the session
/// asserting its own liveness on a schedule, and must not reset the timer —
/// otherwise a peer polling us could suppress our heartbeats entirely.
#[test]
fn answering_a_test_request_does_not_defer_our_own_heartbeat() {
  let (mut state, mut out, start) = established();

  // Just before the outbound deadline, the peer asks us to prove we are alive.
  let at = start + HEARTBEAT - Duration::from_millis(1);
  let _ = state
    .on_message(
      inbound_with("1", 2, &[(Fields::TestReqID, "PING")]),
      at,
      &mut out,
    )
    .unwrap();
  assert_eq!(
    out.sent_types(),
    vec!["0"],
    "should answer with a Heartbeat"
  );
  out.take_sent();

  // The scheduled heartbeat must still be due on its original deadline.
  assert_eq!(
    state.next_deadline(),
    Some(start + HEARTBEAT),
    "answering a TestRequest moved the outbound heartbeat deadline"
  );
  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  assert_eq!(out.sent_types(), vec!["0"]);
}

/// By contrast, an application send *is* evidence of liveness on the wire, so
/// it does defer the next heartbeat.
#[test]
fn an_application_send_defers_the_next_heartbeat() {
  let (mut state, mut out, start) = established();

  let at = start + HEARTBEAT / 2;
  let mut order = builder::Message::new(fix44(), "D").unwrap();
  order.body.set_tag(Fields::ClOrdID, "order-1");
  let _ = state
    .on_command(Command::Send(order), at, &mut out)
    .unwrap();
  out.take_sent();

  // The original outbound deadline passes without a heartbeat: the order
  // already told the peer we are alive. (The inbound timer fires here, which
  // only increments the missed counter and sends nothing.)
  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  assert!(
    !out.sent_types().contains(&"0"),
    "sent a heartbeat despite an application message having just gone out"
  );

  // It fires an interval after the send instead.
  let _ = state.on_timeout(at + HEARTBEAT, &mut out).unwrap();
  assert!(out.sent_types().contains(&"0"));
}

#[test]
fn a_sequence_gap_provokes_one_open_ended_resend_request() {
  let (mut state, mut out, start) = established();

  let _ = state.on_message(inbound("D", 5), start, &mut out).unwrap();
  assert_eq!(out.sent_types(), vec!["2"], "expected a ResendRequest");
  out.take_sent();

  // A second out-of-sequence message must not provoke a second request.
  let _ = state.on_message(inbound("D", 6), start, &mut out).unwrap();
  assert!(
    out.sent_types().is_empty(),
    "a second ResendRequest was sent while one was outstanding"
  );
}

#[test]
fn a_sequence_number_below_the_expected_one_ends_the_session() {
  let (mut state, mut out, start) = established();

  // Expecting 2; 1 means one side has lost state and cannot recover.
  let progress = state.on_message(inbound("D", 1), start, &mut out).unwrap();
  assert_eq!(progress, Progress::Close);
  assert_eq!(out.sent_types(), vec!["5"], "expected a Logout");
}

#[test]
fn a_test_request_is_answered_with_the_same_test_req_id() {
  let (mut state, mut out, start) = established();

  let _ = state
    .on_message(
      inbound_with("1", 2, &[(Fields::TestReqID, "PING-1")]),
      start,
      &mut out,
    )
    .unwrap();

  assert_eq!(out.sent_types(), vec!["0"]);
}

#[test]
fn an_inbound_logout_is_acknowledged_and_closes_the_session() {
  let (mut state, mut out, start) = established();

  let progress = state.on_message(inbound("5", 2), start, &mut out).unwrap();
  assert_eq!(progress, Progress::Close);
  assert_eq!(out.sent_types(), vec!["5"]);
}

/// A Logout arriving inside a gap must not be acknowledged until the gap is
/// recovered, or the messages still missing are abandoned.
#[test]
fn a_logout_inside_a_gap_is_deferred_until_recovery() {
  let (mut state, mut out, start) = established();

  // Open a gap.
  let _ = state.on_message(inbound("D", 5), start, &mut out).unwrap();
  assert_eq!(out.take_sent().len(), 1); // the ResendRequest

  // The Logout arrives while messages 2..4 are still missing.
  let progress = state.on_message(inbound("5", 6), start, &mut out).unwrap();
  assert_eq!(
    progress,
    Progress::Continue,
    "the session ended before recovering the gap"
  );
  assert!(
    out.sent_types().is_empty(),
    "the Logout was acknowledged before the gap closed"
  );

  // The peer gap-fills over the missing range, which closes it.
  let gap_fill = inbound_with(
    "4",
    2,
    &[(Fields::GapFillFlag, "Y"), (Fields::NewSeqNo, "6")],
  );
  let progress = state.on_message(gap_fill, start, &mut out).unwrap();
  assert_eq!(progress, Progress::Close);
  assert_eq!(
    out.sent_types(),
    vec!["5"],
    "the deferred Logout was not acknowledged once the gap closed"
  );
}

#[test]
fn a_resend_request_reports_a_concrete_end_sequence_number() {
  let (mut state, mut out, start) = established();

  // Send three messages so there is something to resend.
  for i in 0..3 {
    let mut order = builder::Message::new(fix44(), "D").unwrap();
    order.body.set_tag(Fields::ClOrdID, format!("order-{i}"));
    let _ = state
      .on_command(Command::Send(order), start, &mut out)
      .unwrap();
  }
  out.take_sent();
  out.events.clear();

  // An open-ended request must be resolved against what we have actually sent.
  let _ = state
    .on_message(
      inbound_with(
        "2",
        2,
        &[(Fields::BeginSeqNo, "2"), (Fields::EndSeqNo, "0")],
      ),
      start,
      &mut out,
    )
    .unwrap();

  let resend = out
    .events
    .iter()
    .find(|e| matches!(e, Seen::ResendRequest { .. }))
    .expect("expected a ResendRequest event");
  assert_eq!(
    resend,
    &Seen::ResendRequest {
      begin_seq_no: 2,
      end_seq_no: 4,
    }
  );
}

/// Every message gets its own `SendingTime`, including several emitted from a
/// single call. This is why the clock is read inside the output rather than
/// handed to the state machine once per pass.
#[test]
fn messages_emitted_together_get_distinct_sending_times() {
  let (mut state, mut out, start) = established();

  // Get the peer to one missed heartbeat, so the next timeout crosses both
  // deadlines and emits two messages: the scheduled Heartbeat and the
  // TestRequest probing the silent peer.
  let _ = state.on_timeout(start + HEARTBEAT, &mut out).unwrap();
  out.take_sent();
  out.events.clear();

  let _ = state.on_timeout(start + 2 * HEARTBEAT, &mut out).unwrap();
  assert_eq!(out.sent_types(), vec!["0", "1"]);

  let stamps: Vec<&String> = out
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
