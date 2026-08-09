//! Two FIX sessions talking to each other with no socket, no runtime and no
//! real time — just byte slices handed between two [`SessionDriver`]s.
//!
//! This is the shape a latency-sensitive application uses: it owns the file
//! descriptor, feeds whatever `read()` returned, and writes whatever the driver
//! leaves in its buffer. If this works, so does an `epoll` loop.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use fix::codec::FixDecoder;
use fix::driver::SessionDriver;
use fix::message::builder;
use fix::schema::FIX_Latest::Fields;
use fix::session::{
  Command, Event, Progress, Session, SessionIdentifier, SessionState,
};

static FIX_REPO: LazyLock<Arc<fix::repository::FixRepository>> =
  LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

const DELIM: u8 = b'|';
const HEARTBEAT: Duration = Duration::from_secs(30);

/// A fixed clock. Real drivers pass `Utc::now`; a test wants determinism, and
/// the core cannot read a clock itself in any case.
fn clock() -> chrono::DateTime<chrono::Utc> {
  chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

/// Collects the message types delivered to the application.
#[derive(Default)]
struct Seen {
  app_messages: Vec<String>,
  recovery_completed: bool,
  resend_requested: Option<(u32, u32)>,
}

impl Seen {
  /// A sink closure that records into `self`.
  fn sink(&mut self) -> impl FnMut(Event<'_>) -> fix::Result<()> + '_ {
    move |event: Event<'_>| {
      match event {
        Event::MessageReceived(msg) => {
          self.app_messages.push(
            msg
              .body
              .tag(Fields::ClOrdID)
              .map(|v| v.as_string())
              .unwrap_or_default(),
          );
        }
        Event::RecoveryCompleted => self.recovery_completed = true,
        Event::ResendRequest {
          begin_seq_no,
          end_seq_no,
          ..
        } => self.resend_requested = Some((begin_seq_no, end_seq_no)),
        _ => {}
      }
      Ok(())
    }
  }
}

fn logon_message() -> builder::Message {
  let mut msg = builder::Message::new(fix44(), "A").unwrap();
  msg.body.set_tag(Fields::HeartBtInt, "30");
  msg.body.set_tag(Fields::EncryptMethod, "0");
  msg
}

fn order(cl_ord_id: &str) -> builder::Message {
  let mut msg = builder::Message::new(fix44(), "D").unwrap();
  msg.body.set_tag(Fields::ClOrdID, cl_ord_id);
  msg.body.set_tag(Fields::Symbol, "AAPL");
  msg.body.set_tag(Fields::Side, "1");
  msg.body.set_tag(Fields::OrderQty, 100i64);
  msg
}

fn driver(us: &str, them: &str) -> SessionDriver {
  let session_id = SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: us.into(),
    target_comp_id: them.into(),
  };
  let mut session = Session::new(fix44());
  session.heartbeat_interval = HEARTBEAT;
  let state = SessionState::new(session_id, session, Instant::now());
  SessionDriver::new(state, FIX_REPO.clone(), Some(DELIM), clock)
}

/// Take everything a driver has queued for the wire.
fn take_wire(d: &mut SessionDriver) -> Vec<u8> {
  let buf = d.pending_writes();
  let bytes = buf.to_vec();
  buf.clear();
  bytes
}

/// Decode exactly one message off the front of `bytes`, returning it and the
/// remainder. Stands in for the part of the handshake that still lives outside
/// the state machine: reading the first frame to learn who the peer is.
fn split_one(bytes: &[u8]) -> (builder::Message, Vec<u8>) {
  let mut decoder = FixDecoder::new(FIX_REPO.clone(), Some(DELIM));
  let mut buf = bytes::BytesMut::from(bytes);
  let msg = decoder
    .decode(&mut buf)
    .expect("decodes")
    .expect("a complete message");
  (builder::Message::from_message(&msg).unwrap(), buf.to_vec())
}

/// Two logged-on drivers, plus the instant they started.
fn established() -> (SessionDriver, SessionDriver, Instant) {
  let now = Instant::now();
  let mut initiator = driver("CLIENT", "SERVER");
  let mut acceptor = driver("SERVER", "CLIENT");
  let mut ignore = ();

  // Initiator opens with a Logon.
  initiator.send_logon(logon_message(), &mut ignore).unwrap();
  let wire = take_wire(&mut initiator);

  // The acceptor reads that first frame to learn who is calling, answers with
  // its own Logon, then hands the peer's Logon to the session.
  let (logon_from_client, rest) = split_one(&wire);
  assert!(rest.is_empty());
  acceptor.send_logon(logon_message(), &mut ignore).unwrap();
  let progress = acceptor.start(logon_from_client, now, &mut ignore).unwrap();
  assert_eq!(progress, Progress::Continue);

  // The initiator does the mirror image: the acceptor's Logon starts its
  // session, and everything after it is ordinary traffic.
  let wire = take_wire(&mut acceptor);
  let (logon_from_server, rest) = split_one(&wire);
  let progress = initiator
    .start(logon_from_server, now, &mut ignore)
    .unwrap();
  assert_eq!(progress, Progress::Continue);
  let _ = initiator.on_bytes(now, &rest, &mut ignore).unwrap();

  (initiator, acceptor, now)
}

/// Hand everything `from` has queued to `to`, and return what `to` says.
fn pump(
  from: &mut SessionDriver,
  to: &mut SessionDriver,
  now: Instant,
  seen: &mut Seen,
) {
  let wire = take_wire(from);
  let mut sink = seen.sink();
  let _ = to.on_bytes(now, &wire, &mut sink).unwrap();
}

#[test]
fn two_drivers_complete_a_logon_exchange() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();

  // Each side answers the other's synchronisation TestRequest, which is what
  // marks recovery complete.
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);

  assert!(
    seen.recovery_completed,
    "neither side reported recovery complete"
  );
  // Both ends have consumed the same stream, so their views must agree, and
  // both must be past the Logon and the synchronisation TestRequest.
  assert_eq!(
    initiator.session().next_in_seq_num,
    acceptor.session().next_in_seq_num,
    "the two sides disagree about how far the conversation got"
  );
  assert!(initiator.session().next_in_seq_num > 2);
}

#[test]
fn an_application_message_crosses_with_no_socket_involved() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  take_wire(&mut initiator);
  take_wire(&mut acceptor);
  seen.app_messages.clear();

  let mut sink = seen.sink();
  let _ = initiator
    .on_command(now, Command::Send(order("order-1")), &mut sink)
    .unwrap();
  drop(sink);

  assert!(
    initiator.has_pending_writes(),
    "the order was not queued for the wire"
  );
  pump(&mut initiator, &mut acceptor, now, &mut seen);

  assert_eq!(seen.app_messages, vec!["order-1"]);
}

/// Bytes arriving split across reads must frame correctly — the driver holds a
/// partial message until the rest turns up. A real socket does this constantly.
#[test]
fn a_message_split_across_reads_is_reassembled() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  take_wire(&mut initiator);
  take_wire(&mut acceptor);
  seen.app_messages.clear();

  let mut sink = seen.sink();
  let _ = initiator
    .on_command(now, Command::Send(order("order-1")), &mut sink)
    .unwrap();
  drop(sink);
  let wire = take_wire(&mut initiator);

  // Deliver it a byte at a time.
  for byte in &wire {
    let mut sink = seen.sink();
    let _ = acceptor
      .on_bytes(now, std::slice::from_ref(byte), &mut sink)
      .unwrap();
  }

  assert_eq!(seen.app_messages, vec!["order-1"]);
}

/// Several messages arriving in one read are all delivered, in order.
#[test]
fn a_batched_read_delivers_every_message() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  take_wire(&mut initiator);
  take_wire(&mut acceptor);
  seen.app_messages.clear();

  for i in 1..=3 {
    let mut sink = seen.sink();
    let _ = initiator
      .on_command(now, Command::Send(order(&format!("order-{i}"))), &mut sink)
      .unwrap();
  }

  // One write, one read, three messages.
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  assert_eq!(seen.app_messages, vec!["order-1", "order-2", "order-3"]);
}

/// A gap provokes a ResendRequest that the peer actually sees.
#[test]
fn a_dropped_message_is_detected_across_the_pair() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  take_wire(&mut initiator);
  take_wire(&mut acceptor);

  // Send two orders but deliver only the second, so one sequence number is
  // missing from the acceptor's point of view.
  let mut sink = seen.sink();
  let _ = initiator
    .on_command(now, Command::Send(order("order-1")), &mut sink)
    .unwrap();
  drop(sink);
  let _dropped = take_wire(&mut initiator);

  let mut sink = seen.sink();
  let _ = initiator
    .on_command(now, Command::Send(order("order-2")), &mut sink)
    .unwrap();
  drop(sink);

  // Whatever the acceptor expects next is what it should ask to have resent.
  let expected_begin = acceptor.session().next_in_seq_num;
  pump(&mut initiator, &mut acceptor, now, &mut seen);

  // The acceptor discards the out-of-sequence message and asks for the gap.
  assert!(seen.app_messages.is_empty());
  assert!(
    acceptor.has_pending_writes(),
    "no ResendRequest was queued for the gap"
  );

  // And the initiator recognises it as a resend request.
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  let (begin, end) = seen.resend_requested.expect("no ResendRequest event");
  assert_eq!(
    begin, expected_begin,
    "resend began at the wrong sequence number"
  );
  assert!(end >= begin, "resend range is inverted: {begin}..={end}");
}

/// The driver reports when it next needs attention, and produces a heartbeat
/// when that moment arrives — with no timer anywhere in sight.
#[test]
fn heartbeats_come_from_on_tick_alone() {
  let (mut initiator, mut acceptor, now) = established();
  let mut seen = Seen::default();
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  pump(&mut acceptor, &mut initiator, now, &mut seen);
  take_wire(&mut initiator);

  let deadline = initiator.next_deadline().expect("a live session has one");
  assert!(deadline > now, "a deadline already in the past");

  // Nothing fires early, however often you ask.
  let mut sink = seen.sink();
  let _ = initiator
    .on_tick(deadline - Duration::from_millis(1), &mut sink)
    .unwrap();
  drop(sink);
  assert!(
    !initiator.has_pending_writes(),
    "something fired before its deadline"
  );

  // Past a full interval the outbound heartbeat is certainly due. The exact
  // deadline above may belong to the *inbound* timer, which only counts a
  // missed beat and sends nothing.
  let mut sink = seen.sink();
  let _ = initiator.on_tick(now + HEARTBEAT * 2, &mut sink).unwrap();
  drop(sink);
  assert!(
    initiator.has_pending_writes(),
    "no heartbeat after two intervals of silence"
  );

  // And the peer accepts it as an ordinary in-sequence message.
  pump(&mut initiator, &mut acceptor, now, &mut seen);
  assert!(
    seen.app_messages.is_empty(),
    "a heartbeat reached the application"
  );
}
