//! The message-centric policy: a message reaches the wire only once its write
//! has completed, in sequence order, and the inbound watermark follows what the
//! application has handled.

mod support;

use std::time::Instant;

use babelfix_core as fix;
use babelfix_schema::fields::{MsgSeqNum, PossDupFlag};
use fix::driver::InitiatorDriver;
use fix::sequencer::{InboundPolicy, Resume, WatermarkMode};
use support::*;

/// Hold the initiator's writes from here on.
fn holding() -> (Peer, Peer, Instant) {
  let (mut initiator, acceptor, now) = synchronised();
  initiator.app.hold = true;
  initiator.app.persisted.clear();
  (initiator, acceptor, now)
}

#[test]
fn nothing_reaches_the_wire_until_it_is_persisted() {
  let (mut initiator, mut acceptor, now) = holding();

  let seq_num = initiator.send(order("order-1"), now);
  assert_eq!(initiator.app.persisted, [(seq_num, "D".to_string())]);
  assert!(
    !initiator.driver.has_pending_writes(),
    "the order was sent before its write completed"
  );

  let _ = initiator
    .seq
    .persisted(seq_num, now, &mut *initiator.driver, &mut initiator.app)
    .unwrap();
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(acceptor.app.app_messages, ["order-1"]);
}

/// Writes may complete out of order; messages may not go out of order.
#[test]
fn completions_out_of_order_are_released_in_order() {
  let (mut initiator, mut acceptor, now) = holding();

  let first = initiator.send(order("order-1"), now);
  let second = initiator.send(order("order-2"), now);
  assert_eq!(second, first + 1);

  let _ = initiator
    .seq
    .persisted(second, now, &mut *initiator.driver, &mut initiator.app)
    .unwrap();
  assert!(
    !initiator.driver.has_pending_writes(),
    "order-2 overtook order-1, whose write is still outstanding"
  );

  let _ = initiator
    .seq
    .persisted(first, now, &mut *initiator.driver, &mut initiator.app)
    .unwrap();
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(acceptor.app.app_messages, ["order-1", "order-2"]);
  assert_eq!(acceptor.app.received_seq_nums, [first, second]);
}

/// Before the peer has answered our Logon, only the Logon may go out. Anything
/// persisted meanwhile follows the session's own synchronisation TestRequest,
/// which logging on always asks for.
#[test]
fn messages_wait_for_the_session_to_log_on() {
  let now = Instant::now();
  let mut app = App::default();
  let mut seq = sequencer(Resume::new(), InboundPolicy::OnDelivery);
  let mut initiator = InitiatorDriver::start(
    id("CLIENT", "SERVER"),
    session(),
    1,
    config(),
    now,
    &mut seq.sink(&mut app),
  )
  .unwrap();
  let _ = settle(&mut seq, &mut initiator, &mut app, now);
  initiator.pending_writes().clear();

  seq.send(order("early"), &mut app).unwrap();
  let _ = settle(&mut seq, &mut initiator, &mut app, now);
  assert!(
    !initiator.has_pending_writes(),
    "an order went out before the peer had logged on"
  );
  assert_eq!(app.persisted.last(), Some(&(2, "D".to_string())));
}

#[test]
fn too_many_unpersisted_messages_are_refused() {
  let (mut initiator, _acceptor, now) = holding();
  // Swap the live sequencer out to configure it, then back in.
  let live = std::mem::replace(
    &mut initiator.seq,
    sequencer(Resume::new(), InboundPolicy::OnDelivery),
  );
  initiator.seq = live.with_max_in_flight(1);

  initiator
    .seq
    .send(order("one"), &mut initiator.app)
    .unwrap();
  let err = initiator
    .seq
    .send(order("two"), &mut initiator.app)
    .unwrap_err();
  assert!(matches!(err, fix::Error::Busy), "{err}");

  // The session's own messages are never refused.
  let _ = initiator.tick(now + HEARTBEAT);
  assert_eq!(initiator.seq.in_flight(), 2);
}

#[test]
fn a_failed_write_stops_the_session_sending() {
  let (mut initiator, _acceptor, now) = holding();

  let seq_num = initiator.send(order("doomed"), now);
  let err = initiator.seq.persist_failed(seq_num);
  assert!(matches!(err, fix::Error::Persistence(_)), "{err}");

  assert!(
    initiator
      .seq
      .send(order("next"), &mut initiator.app)
      .is_err()
  );
  assert!(
    initiator
      .seq
      .persisted(seq_num, now, &mut *initiator.driver, &mut initiator.app)
      .is_err()
  );
  assert!(!initiator.driver.has_pending_writes());
}

/// A test tool may send exactly what it built: its own sequence number,
/// PossDupFlag and all. Numbering carries on after it.
#[test]
fn send_raw_keeps_the_header_it_was_given() {
  let (mut initiator, mut acceptor, now) = synchronised();

  let next = initiator.seq.next_out_seq_num();
  let mut msg = order("raw");
  msg
    .header_mut()
    .set(MsgSeqNum, next + 5)
    .set(PossDupFlag, true);
  let sent = initiator.seq.send_raw(msg, &mut initiator.app).unwrap();
  let _ = initiator.settle(now);
  assert_eq!(sent, next + 5);
  assert_eq!(initiator.seq.next_out_seq_num(), next + 6);

  let wire =
    String::from_utf8_lossy(initiator.driver.pending_writes()).into_owned();
  assert!(wire.contains("|43=Y|"), "PossDupFlag was dropped: {wire}");
  let _ = pump(&mut initiator, &mut acceptor, now);

  // Going back is never allowed.
  let mut stale = order("stale");
  stale.header_mut().set(MsgSeqNum, next);
  assert!(initiator.seq.send_raw(stale, &mut initiator.app).is_err());
}

/// Delivered is handled, for an application that does not care.
#[test]
fn on_delivery_advances_the_watermark_as_messages_arrive() {
  let (mut initiator, mut acceptor, now) = synchronised();
  acceptor.app.watermarks.clear();

  let seq_num = initiator.send(order("order-1"), now);
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(acceptor.app.watermarks, [seq_num + 1]);
  assert_eq!(acceptor.seq.watermark(), seq_num + 1);
}

/// Under `Contiguous`, the watermark waits for the earliest message still
/// being handled; admin traffic needs no handling.
#[test]
fn contiguous_waits_for_the_earliest_unhandled_message() {
  let (mut initiator, mut acceptor, now) =
    established_with(InboundPolicy::Explicit(WatermarkMode::Contiguous));
  let _ = pump(&mut initiator, &mut acceptor, now);
  let _ = pump(&mut acceptor, &mut initiator, now);
  let _ = pump(&mut initiator, &mut acceptor, now);
  let before = acceptor.seq.watermark();
  assert_eq!(before, acceptor.driver.state().next_in_seq_num());

  let first = initiator.send(order("order-1"), now);
  let second = initiator.send(order("order-2"), now);
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(acceptor.seq.watermark(), first);

  acceptor.seq.handled(second, &mut acceptor.app).unwrap();
  assert_eq!(acceptor.seq.watermark(), first, "skipped past order-1");

  acceptor.seq.handled(first, &mut acceptor.app).unwrap();
  assert_eq!(acceptor.seq.watermark(), second + 1);
}

/// Under `Highest`, the watermark is just past the latest message handled.
#[test]
fn highest_follows_the_latest_message_handled() {
  let (mut initiator, mut acceptor, now) =
    established_with(InboundPolicy::Explicit(WatermarkMode::Highest));
  let _ = pump(&mut initiator, &mut acceptor, now);

  let first = initiator.send(order("order-1"), now);
  let second = initiator.send(order("order-2"), now);
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert!(acceptor.seq.watermark() <= first);

  acceptor.seq.handled(second, &mut acceptor.app).unwrap();
  assert_eq!(acceptor.seq.watermark(), second + 1);
}

/// One watermark write at a time; when it completes, the latest value is
/// asked for, not every value in between.
#[test]
fn watermark_writes_are_coalesced() {
  let (mut initiator, mut acceptor, now) = synchronised();
  acceptor.app.hold = true;
  acceptor.app.watermarks.clear();
  acceptor.app.pending_watermarks.clear();

  for i in 0..3 {
    initiator.send(order(&format!("order-{i}")), now);
  }
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(
    acceptor.app.watermarks.len(),
    1,
    "{:?}",
    acceptor.app.watermarks
  );

  let first = acceptor.app.watermarks[0];
  acceptor
    .seq
    .watermark_persisted(first, &mut acceptor.app)
    .unwrap();
  assert_eq!(
    acceptor.app.watermarks,
    [first, acceptor.seq.watermark()],
    "the latest watermark was not asked for once the first completed"
  );
}
