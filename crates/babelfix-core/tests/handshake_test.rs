//! The logon exchange, with no socket and no runtime.
//!
//! These pin down the part that is genuinely protocol rather than transport:
//! the two roles' orderings, and what happens to a peer that opens with
//! something other than a Logon. Before the handshake moved into the core this
//! could only be tested through a real acceptor on a real port.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix_core as fix;
use babelfix_schema::codesets::EncryptMethod;
use babelfix_schema::fields::{
  EncryptMethod, HeartBtInt, MsgSeqNum, SenderCompID, TargetCompID,
};
use babelfix_schema::tags;
use fix::message::{Dictionaries, Dictionary, Message};
use fix::session::{
  AcceptorHandshake, Event, InitiatorHandshake, Progress, Session,
  SessionIdentifier, SessionOutput,
};

static DICTS: LazyLock<Arc<Dictionaries>> =
  LazyLock::new(|| Dictionaries::standard().unwrap());

fn fix44() -> Arc<Dictionary> {
  DICTS.get("FIX.4.4").unwrap().clone()
}

const LOGON_TIMEOUT: Duration = Duration::from_secs(30);

/// An owned trace of what the handshake emitted, in order.
#[derive(Default)]
struct Trace {
  events: Vec<String>,
  clock: u32,
}

impl SessionOutput for Trace {
  fn transmit(
    &mut self,
    msg: fix::session::Unstamped<'_>,
    _session: &Session,
  ) -> fix::Result<()> {
    self.clock += 1;
    let when = chrono::DateTime::from_timestamp(1_700_000_000, self.clock)
      .expect("valid timestamp");
    msg.stamp(when);
    Ok(())
  }

  fn event(&mut self, event: Event<'_>) -> fix::Result<()> {
    self.events.push(match event {
      Event::ConnectionEstablished => "ConnectionEstablished".into(),
      Event::RawMessageReceived(m, _) => {
        format!("RawMessageReceived({})", m.msg_type())
      }
      Event::RawMessageSent(m, _) => {
        format!("RawMessageSent({})", m.msg_type())
      }
      Event::SessionState(_) => "SessionState".into(),
      Event::MessageReceived(_) => "MessageReceived".into(),
      Event::RecoveryCompleted => "RecoveryCompleted".into(),
      Event::Disconnected => "Disconnected".into(),
      _ => "other".into(),
    });
    Ok(())
  }
}

fn session() -> Session {
  let mut s = Session::new(fix44());
  s.heartbeat_interval = Duration::from_secs(30);
  s
}

/// A frame as it would arrive from the peer named `from`, addressed to `to`.
fn frame(msg_type: &str, seq: u32, from: &str, to: &str) -> Message {
  let mut msg = Message::new(&fix44(), msg_type);
  msg
    .header_mut()
    .set(MsgSeqNum, seq)
    .set(SenderCompID, from)
    .set(TargetCompID, to)
    .set_raw(tags::SendingTime, b"20231114-22:13:20.000000000");
  if msg_type == "A" {
    msg
      .body_mut()
      .set(HeartBtInt, 30u64)
      .set(EncryptMethod, EncryptMethod::None);
  }
  Message::parse(&fix44(), msg.to_bytes()).unwrap()
}

fn id(us: &str, them: &str) -> SessionIdentifier {
  SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: us.into(),
    target_comp_id: them.into(),
  }
}

/// An initiator knows who it is talking to, so it announces the session and
/// puts its Logon on the wire before anything arrives.
#[test]
fn an_initiator_opens_with_its_own_logon() {
  let mut out = Trace::default();
  let _hs = InitiatorHandshake::start(
    id("CLIENT", "SERVER"),
    session(),
    LOGON_TIMEOUT,
    Instant::now(),
    &mut out,
  )
  .unwrap();

  assert_eq!(
    out.events,
    vec!["ConnectionEstablished", "RawMessageSent(A)"],
    "an initiator announces the session, then sends its Logon"
  );
}

/// An acceptor says nothing at all until the peer names a session. It cannot:
/// every session on the listening port shares it, so there is nothing yet to
/// attach an event to.
#[test]
fn an_acceptor_says_nothing_until_the_peer_identifies_itself() {
  let out = Trace::default();
  let mut hs = AcceptorHandshake::new(LOGON_TIMEOUT, Instant::now());
  assert!(out.events.is_empty());

  // Note there is no output to pass: the signature says an acceptor cannot
  // emit anything at this point, because there is no session yet.
  let session_id = hs
    .identify(frame("A", 1, "CLIENT", "SERVER"))
    .unwrap()
    .clone();
  // Identity is expressed from our point of view: the peer's SenderCompID is
  // our TargetCompID.
  assert_eq!(session_id.sender_comp_id, "SERVER");
  assert_eq!(session_id.target_comp_id, "CLIENT");
  assert_eq!(session_id.begin_string, "FIX.4.4");

  assert!(
    out.events.is_empty(),
    "an acceptor emitted {:?} before it knew which session it was serving",
    out.events
  );
}

/// And once the application supplies the session, the ordering is fixed: the
/// session is announced, the Logon that opened it is reported, and only then
/// does the reply go out.
#[test]
fn an_acceptor_reports_the_logon_before_answering_it() {
  let mut out = Trace::default();
  let mut hs = AcceptorHandshake::new(LOGON_TIMEOUT, Instant::now());
  hs.identify(frame("A", 1, "CLIENT", "SERVER")).unwrap();

  let established = hs.accept(session(), Instant::now(), &mut out).unwrap();
  assert_eq!(established.progress, Progress::Continue);

  assert_eq!(
    &out.events[..3],
    &[
      "ConnectionEstablished",
      "RawMessageReceived(A)",
      "RawMessageSent(A)",
    ],
    "an application persisting from these must see what arrived before what \
     it answered with"
  );
}

/// The peer's Logon is available for inspection between being told about it
/// and accepting it — which is where authentication belongs.
#[test]
fn the_peer_logon_is_available_before_accepting() {
  let mut hs = AcceptorHandshake::new(LOGON_TIMEOUT, Instant::now());
  assert!(hs.peer_logon().is_none());

  hs.identify(frame("A", 1, "CLIENT", "SERVER")).unwrap();

  let logon = hs.peer_logon().expect("the Logon that named the session");
  assert_eq!(logon.msg_type(), "A");
  assert_eq!(logon.body().get(HeartBtInt).unwrap(), Some(30));
}

/// A first frame that is not a Logon is refused before anything is derived
/// from it. Nothing is emitted, so no application ever hears about a session
/// that does not exist.
#[test]
fn a_first_frame_that_is_not_a_logon_is_refused_silently() {
  // Acceptor: nothing has been emitted, and nothing can be — there is no
  // output to emit into.
  let mut acceptor = AcceptorHandshake::new(LOGON_TIMEOUT, Instant::now());
  let err = acceptor
    .identify(frame("0", 1, "CLIENT", "SERVER"))
    .expect_err("a Heartbeat must not open a session");
  assert!(format!("{err}").contains("not a logon"), "{err}");
  assert!(acceptor.session_id().is_none());
  assert!(acceptor.peer_logon().is_none());

  // Initiator: the Logon it already sent is the only thing emitted.
  let mut out = Trace::default();
  let initiator = InitiatorHandshake::start(
    id("CLIENT", "SERVER"),
    session(),
    LOGON_TIMEOUT,
    Instant::now(),
    &mut out,
  )
  .unwrap();
  let before = out.events.len();

  let err = initiator
    .on_peer_logon(frame("0", 1, "CLIENT", "SERVER"), Instant::now(), &mut out)
    .expect_err("a Heartbeat must not complete the exchange");
  assert!(format!("{err}").contains("not a logon"), "{err}");
  assert_eq!(
    out.events.len(),
    before,
    "something was emitted for a frame that was never a Logon"
  );
}

/// A Logon whose sequence number is too low ends the session — but the state
/// still comes back, because the application has to be given the Logout that
/// explains why.
#[test]
fn a_session_refused_during_logon_still_comes_back() {
  let mut out = Trace::default();
  let mut hs = AcceptorHandshake::new(LOGON_TIMEOUT, Instant::now());
  hs.identify(frame("A", 1, "CLIENT", "SERVER")).unwrap();

  // Expecting inbound 5; the peer opened at 1, so one side has lost state.
  let mut stale = session();
  stale.next_in_seq_num = 5;

  let established = hs.accept(stale, Instant::now(), &mut out).unwrap();
  assert_eq!(established.progress, Progress::Close);
  assert!(
    out.events.iter().any(|e| e == "RawMessageSent(5)"),
    "no Logout explaining the refusal: {:?}",
    out.events
  );
}

/// The exchange has a deadline of its own, independent of the heartbeats that
/// only start once a session exists.
#[test]
fn the_logon_deadline_is_enforced() {
  let start = Instant::now();
  let hs = AcceptorHandshake::new(LOGON_TIMEOUT, start);

  assert_eq!(hs.deadline(), start + LOGON_TIMEOUT);
  assert!(
    hs.on_timeout(start + LOGON_TIMEOUT - Duration::from_millis(1))
      .is_ok()
  );
  assert!(
    hs.on_timeout(start + LOGON_TIMEOUT).is_err(),
    "a peer that never completed the exchange was not given up on"
  );
}
