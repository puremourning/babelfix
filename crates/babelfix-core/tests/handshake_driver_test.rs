//! The driver-level handoff from the Logon exchange to an established session.
//!
//! A socket read is not aligned to FIX frames. The Logon and the first
//! application message can therefore arrive together, and the handshake must
//! hand both to the session without waiting for another read.

mod support;

use std::time::Instant;

use babelfix_core as fix;
use babelfix_schema::codesets::EncryptMethod;
use babelfix_schema::fields::{
  EncryptMethod, HeartBtInt, MsgSeqNum, SenderCompID, SendingTime, TargetCompID,
};
use fix::codec::FixEncoder;
use fix::driver::{AcceptorDriver, InitiatorDriver};
use fix::message::Message;
use fix::sequencer::{InboundPolicy, Resume};
use support::*;

/// A frame as the peer `us` would put it on the wire to `them`.
fn frame(mut msg: Message, seq: u64, us: &str, them: &str) -> Message {
  msg
    .header_mut()
    .set(MsgSeqNum, seq)
    .set(SenderCompID, us)
    .set(TargetCompID, them)
    .set_raw(SendingTime, b"20231114-22:13:20.000000000");
  msg
}

/// Encode a Logon followed immediately by an application message, as one
/// socket read could present them to the receiver.
fn logon_and_order(us: &str, them: &str) -> Vec<u8> {
  let mut logon = Message::new(&fix44(), "A");
  logon
    .body_mut()
    .set(HeartBtInt, 30u64)
    .set(EncryptMethod, EncryptMethod::None);

  let mut encoder = FixEncoder::new(Some(DELIM));
  let mut wire = bytes::BytesMut::new();
  encoder
    .encode(&frame(logon, 1, us, them), &mut wire)
    .unwrap();
  encoder
    .encode(&frame(order("order-1"), 2, us, them), &mut wire)
    .unwrap();
  wire.to_vec()
}

#[test]
fn initiator_delivers_a_message_carried_with_the_logon() {
  let now = Instant::now();
  let mut app = App::default();
  let mut seq = sequencer(Resume::new(), InboundPolicy::OnDelivery);
  let mut handshake = InitiatorDriver::start(
    id("CLIENT", "SERVER"),
    session(),
    1,
    config(),
    now,
    &mut seq.sink(&mut app),
  )
  .unwrap();
  // Our Logon goes out first: the peer is answering it.
  let _ = settle(&mut seq, &mut handshake, &mut app, now);
  assert!(handshake.has_pending_writes(), "our Logon was not sent");

  let wire = logon_and_order("SERVER", "CLIENT");
  let mut established = handshake
    .on_bytes(now, &wire, &mut seq.sink(&mut app))
    .unwrap()
    .expect("the peer's Logon establishes a session");
  let _ = settle(&mut seq, &mut established, &mut app, now);

  assert!(
    app.app_messages.is_empty(),
    "the initiator delivered the carried message before it was started, \
     so an application had nowhere to reply to it"
  );

  let _ = established.start(now, &mut seq.sink(&mut app)).unwrap();

  assert_eq!(
    app.app_messages,
    vec!["order-1"],
    "the initiator left the message following Logon buffered"
  );
}

#[test]
fn acceptor_delivers_a_message_carried_with_the_logon() {
  let now = Instant::now();
  let wire = logon_and_order("CLIENT", "SERVER");
  let mut handshake = AcceptorDriver::new(config(), now);
  let session_id = handshake
    .on_bytes(&wire)
    .unwrap()
    .expect("the peer's Logon identifies a session");
  assert_eq!(session_id, &id("SERVER", "CLIENT"));

  let mut app = App::default();
  let mut seq = sequencer(Resume::new(), InboundPolicy::OnDelivery);
  let mut established = handshake
    .accept(session(), 1, now, &mut seq.sink(&mut app))
    .unwrap();
  let _ = settle(&mut seq, &mut established, &mut app, now);

  assert!(
    app.app_messages.is_empty(),
    "the acceptor delivered the carried message before it was started, \
     so an application had nowhere to reply to it"
  );
  assert!(
    established.has_pending_writes(),
    "the Logon reply was not sent"
  );

  let _ = established.start(now, &mut seq.sink(&mut app)).unwrap();

  assert_eq!(
    app.app_messages,
    vec!["order-1"],
    "the acceptor left the message following Logon buffered"
  );
}
