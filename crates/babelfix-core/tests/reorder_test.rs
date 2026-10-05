//! The event-centric reorder window: messages numbered by several senders go
//! out in number order, and a gap is waited for only so long.

mod support;

use std::time::{Duration, Instant};

use babelfix_core as fix;
use babelfix_schema::fields::MsgSeqNum;
use fix::Message;
use fix::reorder::{Offered, ReorderConfig, ReorderWindow};
use fix::session::Progress;
use support::order;

const WAIT: Duration = Duration::from_millis(1);

fn numbered(seq_num: u64) -> Message {
  let mut msg = order(&format!("order-{seq_num}"));
  msg.header_mut().set(MsgSeqNum, seq_num);
  msg
}

/// A window and the numbers it has released, in release order.
struct Wire {
  window: ReorderWindow,
  sent: Vec<u64>,
  close_on: Option<u64>,
}

impl Wire {
  fn new(max_held: usize) -> Self {
    Self {
      window: ReorderWindow::new(ReorderConfig {
        max_held,
        max_wait: WAIT,
      }),
      sent: Vec::new(),
      close_on: None,
    }
  }

  fn offer(&mut self, seq_num: u64, now: Instant) -> Offered {
    let (sent, close_on) = (&mut self.sent, self.close_on);
    self
      .window
      .offer(numbered(seq_num), now, |msg| {
        let n = msg.header().req(MsgSeqNum)?;
        sent.push(n);
        Ok(if close_on == Some(n) {
          Progress::Close
        } else {
          Progress::Continue
        })
      })
      .unwrap()
  }

  fn expire(&mut self, now: Instant) -> Progress {
    let sent = &mut self.sent;
    self
      .window
      .expire(now, |msg| {
        sent.push(msg.header().req(MsgSeqNum)?);
        Ok(Progress::Continue)
      })
      .unwrap()
  }
}

fn taken(offered: Offered) -> Progress {
  match offered {
    Offered::Taken(progress) => progress,
    Offered::Late(msg) => panic!("{msg} came back late"),
  }
}

fn late(offered: Offered) -> u64 {
  match offered {
    Offered::Late(msg) => msg.header().req(MsgSeqNum).unwrap(),
    Offered::Taken(_) => panic!("expected the message back"),
  }
}

#[test]
fn in_order_goes_straight_through() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  for n in 5..10 {
    let _ = taken(wire.offer(n, now));
  }
  assert_eq!(wire.sent, [5, 6, 7, 8, 9]);
  assert_eq!(wire.window.held(), 0);
  assert_eq!(wire.window.next_seq_num(), Some(10));
}

#[test]
fn a_message_ahead_waits_for_the_gap() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(4, now));
  let _ = taken(wire.offer(3, now));
  assert_eq!(wire.sent, [1]);
  assert_eq!(wire.window.held(), 2);
  assert_eq!(wire.window.next_expiry(), Some(now + WAIT));

  let _ = taken(wire.offer(2, now));
  assert_eq!(wire.sent, [1, 2, 3, 4]);
  assert_eq!(wire.window.held(), 0);
  assert_eq!(wire.window.next_expiry(), None);
}

#[test]
fn an_expired_gap_is_given_up_on_by_the_next_offer() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let _ = taken(wire.offer(5, now + WAIT / 2));

  // 3 has expired; 5 has not, but cannot overtake it, and 4 is still missing.
  let _ = taken(wire.offer(6, now + WAIT));
  assert_eq!(wire.sent, [1, 3]);
  assert_eq!(wire.window.next_seq_num(), Some(4));

  let _ = taken(wire.offer(4, now + WAIT));
  assert_eq!(wire.sent, [1, 3, 4, 5, 6]);

  assert_eq!(late(wire.offer(2, now + WAIT)), 2);
}

#[test]
fn the_message_a_gap_waited_for_is_not_late_however_late_it_comes() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let _ = taken(wire.offer(5, now));

  // 3 and 5 have both expired, but 2 is what 3 was waiting for: it goes
  // first, and 3 with it; 5 has waited for 4 long enough.
  let _ = taken(wire.offer(2, now + WAIT));
  assert_eq!(wire.sent, [1, 2, 3, 5]);
  assert_eq!(wire.window.held(), 0);
  assert_eq!(wire.window.next_seq_num(), Some(6));
}

#[test]
fn expire_releases_without_an_offer() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let _ = taken(wire.offer(4, now));

  assert_eq!(wire.expire(now + WAIT / 2), Progress::Continue);
  assert_eq!(wire.sent, [1]);

  assert_eq!(wire.expire(now + WAIT), Progress::Continue);
  assert_eq!(wire.sent, [1, 3, 4]);
  assert_eq!(wire.window.next_seq_num(), Some(5));
  assert_eq!(wire.window.next_expiry(), None);
}

#[test]
fn a_message_beyond_the_window_gives_up_on_the_gap() {
  let mut wire = Wire::new(3);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let _ = taken(wire.offer(4, now));
  // Expecting 2, so 3..=5 fit and 6 does not.
  let _ = taken(wire.offer(6, now));
  assert_eq!(wire.sent, [1, 3, 4, 6]);
  assert_eq!(wire.window.next_seq_num(), Some(7));
  assert_eq!(late(wire.offer(2, now)), 2);
  assert_eq!(late(wire.offer(5, now)), 5);
}

#[test]
fn the_furthest_message_that_fits_is_held() {
  let mut wire = Wire::new(3);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(5, now));
  assert_eq!(wire.sent, [1]);
  let _ = taken(wire.offer(2, now));
  let _ = taken(wire.offer(4, now));
  let _ = taken(wire.offer(3, now));
  assert_eq!(wire.sent, [1, 2, 3, 4, 5]);
}

#[test]
fn a_duplicate_comes_back() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  assert_eq!(late(wire.offer(3, now)), 3);
  assert_eq!(late(wire.offer(1, now)), 1);
  let _ = taken(wire.offer(2, now));
  assert_eq!(wire.sent, [1, 2, 3]);
}

#[test]
fn close_stops_the_release() {
  let mut wire = Wire::new(10);
  wire.close_on = Some(3);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let _ = taken(wire.offer(4, now));
  assert_eq!(taken(wire.offer(2, now)), Progress::Close);
  assert_eq!(wire.sent, [1, 2, 3]);
}

#[test]
fn disabled_passes_everything_through_in_offer_order() {
  let mut wire = Wire::new(0);
  let now = Instant::now();
  for n in [1, 3, 2, 2] {
    let _ = taken(wire.offer(n, now));
  }
  assert_eq!(wire.sent, [1, 3, 2, 2]);
  assert_eq!(wire.window.next_seq_num(), None);
}

#[test]
fn an_unnumbered_message_goes_through_for_the_session_to_refuse() {
  let mut wire = Wire::new(10);
  let now = Instant::now();
  let _ = taken(wire.offer(1, now));
  let _ = taken(wire.offer(3, now));
  let mut sent = 0;
  let _ = taken(
    wire
      .window
      .offer(order("unnumbered"), now, |_| {
        sent += 1;
        Ok(Progress::Continue)
      })
      .unwrap(),
  );
  assert_eq!(sent, 1);
  assert_eq!(wire.window.held(), 1);
}
