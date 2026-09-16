//! The driver-level handoff from the Logon exchange to an established session.
//!
//! A socket read is not aligned to FIX frames. The Logon and the first
//! application message can therefore arrive together, and the handshake must
//! hand both to the session without waiting for another read.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use fix::driver::{
  AcceptorDriver, DriverConfig, InitiatorDriver, SessionDriver,
};
use fix::message::builder;
use fix::schema::FIX_Latest::Fields;
use fix::session::{Command, Event, Session, SessionIdentifier, SessionState};

static FIX_REPO: LazyLock<Arc<fix::repository::FixRepository>> =
  LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

const DELIM: u8 = b'|';
const HEARTBEAT: Duration = Duration::from_secs(30);

/// A fixed clock keeps every encoded frame deterministic.
fn clock() -> chrono::DateTime<chrono::Utc> {
  chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

#[derive(Default)]
struct Seen {
  app_messages: Vec<String>,
}

impl Seen {
  fn sink(&mut self) -> impl FnMut(Event<'_>) -> fix::Result<()> + '_ {
    move |event: Event<'_>| {
      if let Event::MessageReceived(msg) = event {
        self.app_messages.push(
          msg
            .body
            .tag(Fields::ClOrdID)
            .map(|value| value.as_string())
            .unwrap_or_default(),
        );
      }
      Ok(())
    }
  }
}

fn session() -> Session {
  let mut session = Session::new(fix44());
  session.heartbeat_interval = HEARTBEAT;
  session
}

fn id(us: &str, them: &str) -> SessionIdentifier {
  SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: us.into(),
    target_comp_id: them.into(),
  }
}

fn config() -> DriverConfig {
  DriverConfig {
    repo: FIX_REPO.clone(),
    delimiter: Some(DELIM),
    clock,
    logon_timeout: HEARTBEAT,
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

fn driver(us: &str, them: &str, now: Instant) -> SessionDriver {
  let state = SessionState::new(id(us, them), session(), now);
  SessionDriver::new(state, FIX_REPO.clone(), Some(DELIM), clock)
}

/// Encode a Logon followed immediately by an application message, as one
/// socket read could present them to the receiver.
fn logon_and_order(us: &str, them: &str, now: Instant) -> Vec<u8> {
  let mut peer = driver(us, them, now);
  let mut ignore = ();
  peer.send_logon(logon_message(), &mut ignore).unwrap();
  let _ = peer
    .on_command(now, Command::Send(order("order-1")), &mut ignore)
    .unwrap();
  peer.pending_writes().to_vec()
}

#[test]
fn initiator_delivers_a_message_carried_with_the_logon() {
  let now = Instant::now();
  let mut ignore = ();
  let mut handshake = InitiatorDriver::start(
    id("CLIENT", "SERVER"),
    session(),
    config(),
    now,
    &mut ignore,
  )
  .unwrap();
  let wire = logon_and_order("SERVER", "CLIENT", now);
  let mut seen = Seen::default();

  let mut sink = seen.sink();
  let established = handshake
    .on_bytes(now, &wire, &mut sink)
    .unwrap()
    .expect("the peer's Logon establishes a session");
  drop(sink);

  assert!(
    seen.app_messages.is_empty(),
    "the initiator delivered the carried message before it was started, \
     so an application had nowhere to reply to it"
  );

  let mut sink = seen.sink();
  let _ = established.start(now, &mut sink).unwrap();
  drop(sink);

  assert_eq!(
    seen.app_messages,
    vec!["order-1"],
    "the initiator left the message following Logon buffered"
  );
}

#[test]
fn acceptor_delivers_a_message_carried_with_the_logon() {
  let now = Instant::now();
  let wire = logon_and_order("CLIENT", "SERVER", now);
  let mut handshake = AcceptorDriver::new(config(), now);
  let session_id = handshake
    .on_bytes(&wire)
    .unwrap()
    .expect("the peer's Logon identifies a session");
  assert_eq!(session_id, &id("SERVER", "CLIENT"));
  let mut seen = Seen::default();

  let mut sink = seen.sink();
  let established = handshake.accept(session(), now, &mut sink).unwrap();
  drop(sink);

  assert!(
    seen.app_messages.is_empty(),
    "the acceptor delivered the carried message before it was started, \
     so an application had nowhere to reply to it"
  );

  let mut sink = seen.sink();
  let _ = established.start(now, &mut sink).unwrap();
  drop(sink);

  assert_eq!(
    seen.app_messages,
    vec!["order-1"],
    "the acceptor left the message following Logon buffered"
  );
}
