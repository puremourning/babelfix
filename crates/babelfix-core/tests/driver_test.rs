//! Two FIX sessions talking to each other with no socket, no runtime and no
//! real time — just byte slices handed between two [`SessionDriver`]s, each
//! numbered by a [`Sequencer`] whose store completes writes at once.
//!
//! This is the shape a latency-sensitive application uses: it owns the file
//! descriptor, feeds whatever `read()` returned, and writes whatever the driver
//! leaves in its buffer. If this works, so does an `epoll` loop.
//!
//! [`SessionDriver`]: babelfix_core::driver::SessionDriver
//! [`Sequencer`]: babelfix_core::sequencer::Sequencer

mod support;

use std::time::Duration;

use support::*;

#[test]
fn two_drivers_complete_a_logon_exchange() {
  let (mut initiator, mut acceptor, now) = established();

  // Each side answers the other's synchronisation TestRequest, which is what
  // marks recovery complete.
  let _ = pump(&mut initiator, &mut acceptor, now);
  let _ = pump(&mut acceptor, &mut initiator, now);
  let _ = pump(&mut initiator, &mut acceptor, now);

  assert!(
    initiator.app.recovery_completed && acceptor.app.recovery_completed,
    "a side did not report recovery complete"
  );
  // Both ends have consumed the same stream, so their views must agree, and
  // both must be past the Logon and the synchronisation TestRequest.
  assert_eq!(
    initiator.driver.state().next_in_seq_num(),
    acceptor.driver.state().next_in_seq_num(),
    "the two sides disagree about how far the conversation got"
  );
  assert!(initiator.driver.state().next_in_seq_num() > 2);
}

/// Everything sent was persisted first, under the number it went out with.
#[test]
fn every_message_sent_was_persisted_first() {
  let (mut initiator, mut acceptor, now) = synchronised();
  initiator.send(order("order-1"), now);
  let _ = pump(&mut initiator, &mut acceptor, now);

  let persisted: Vec<(u64, &str)> = initiator
    .app
    .persisted
    .iter()
    .map(|(n, t)| (*n, t.as_str()))
    .collect();
  // Logon, synchronisation TestRequest, the Heartbeat answering the peer's,
  // then the order.
  assert_eq!(persisted, [(1, "A"), (2, "1"), (3, "0"), (4, "D")]);
  assert_eq!(acceptor.app.received_seq_nums, [4]);
}

#[test]
fn an_application_message_crosses_with_no_socket_involved() {
  let (mut initiator, mut acceptor, now) = synchronised();

  initiator.send(order("order-1"), now);
  assert!(
    initiator.driver.has_pending_writes(),
    "the order was not queued for the wire"
  );
  let _ = pump(&mut initiator, &mut acceptor, now);

  assert_eq!(acceptor.app.app_messages, vec!["order-1"]);
}

/// Bytes arriving split across reads must frame correctly — the driver holds a
/// partial message until the rest turns up. A real socket does this constantly.
#[test]
fn a_message_split_across_reads_is_reassembled() {
  let (mut initiator, mut acceptor, now) = synchronised();

  initiator.send(order("order-1"), now);
  let wire = initiator.take_wire();

  // Deliver it a byte at a time.
  for byte in &wire {
    let _ = acceptor.on_bytes(now, std::slice::from_ref(byte));
  }

  assert_eq!(acceptor.app.app_messages, vec!["order-1"]);
}

/// Several messages arriving in one read are all delivered, in order.
#[test]
fn a_batched_read_delivers_every_message() {
  let (mut initiator, mut acceptor, now) = synchronised();

  for i in 1..=3 {
    initiator.send(order(&format!("order-{i}")), now);
  }

  // One write, one read, three messages.
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert_eq!(
    acceptor.app.app_messages,
    vec!["order-1", "order-2", "order-3"]
  );
}

/// A gap provokes a ResendRequest that the peer actually sees.
#[test]
fn a_dropped_message_is_detected_across_the_pair() {
  let (mut initiator, mut acceptor, now) = synchronised();

  // Send two orders but deliver only the second, so one sequence number is
  // missing from the acceptor's point of view.
  initiator.send(order("order-1"), now);
  let _dropped = initiator.take_wire();
  initiator.send(order("order-2"), now);

  // Whatever the acceptor expects next is what it should ask to have resent.
  let expected_begin = acceptor.driver.state().next_in_seq_num();
  let _ = pump(&mut initiator, &mut acceptor, now);

  // The acceptor discards the out-of-sequence message and asks for the gap.
  assert!(acceptor.app.app_messages.is_empty());
  assert!(
    acceptor.driver.has_pending_writes(),
    "no ResendRequest was queued for the gap"
  );

  // And the initiator recognises it as a resend request.
  let _ = pump(&mut acceptor, &mut initiator, now);
  let (begin, end) = initiator
    .app
    .resend_requested
    .expect("no ResendRequest event");
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
  let (mut initiator, mut acceptor, now) = synchronised();

  let deadline = initiator
    .driver
    .next_deadline()
    .expect("a live session has one");
  assert!(deadline > now, "a deadline already in the past");

  // Nothing fires early, however often you ask.
  let _ = initiator.tick(deadline - Duration::from_millis(1));
  assert!(
    !initiator.driver.has_pending_writes(),
    "something fired before its deadline"
  );

  // Past a full interval the outbound heartbeat is certainly due. The exact
  // deadline above may belong to the *inbound* timer, which only counts a
  // missed beat and sends nothing.
  let _ = initiator.tick(now + HEARTBEAT * 2);
  assert!(
    initiator.driver.has_pending_writes(),
    "no heartbeat after two intervals of silence"
  );

  // And the peer accepts it as an ordinary in-sequence message.
  let _ = pump(&mut initiator, &mut acceptor, now);
  assert!(
    acceptor.app.app_messages.is_empty(),
    "a heartbeat reached the application"
  );
}
