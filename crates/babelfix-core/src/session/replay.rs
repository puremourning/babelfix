//! Bookkeeping for answering a peer's ResendRequest.
//!
//! The peer asks for `begin_seq_no..=end_seq_no`. The application hands back
//! whichever of those messages it kept, in order, and anything it skips —
//! because it never stored the message, or because retransmitting a stale order
//! would be worse than not — has to be covered by a SequenceReset-GapFill so
//! the peer's inbound sequence still lines up.
//!
//! This type tracks how far through the range we are and how many consecutive
//! sequence numbers are currently owed a gap fill. It decides *what* to send;
//! the caller does the sending.

use crate::message::builder;
use crate::{Error, Result};

/// What the replay wants done for one message the application offered.
#[derive(Debug, PartialEq)]
pub(crate) enum ReplayStep {
  /// The message falls outside the requested range, or has already been
  /// covered. Drop it.
  Skip,
  /// An admin message: never retransmitted, so it is gap-filled along with any
  /// other skipped numbers rather than sent.
  Absorb,
  /// Retransmit it, first closing off any preceding run of skipped numbers with
  /// a gap fill over `gap_fill` (inclusive bounds).
  Retransmit { gap_fill: Option<(u32, u32)> },
}

#[derive(Debug, Default, Clone)]
pub struct Replay {
  pub begin_seq_no: u32,
  pub end_seq_no: u32,

  /// The next sequence number in the requested range we have not yet accounted
  /// for.
  next_expected_seq_num: u32,
  /// How many consecutive numbers immediately before `next_expected_seq_num`
  /// are owed a gap fill.
  gap_fill_count: u32,

  /// Messages the application asked to send *normally* while the replay was in
  /// progress. They cannot go out mid-retransmission without corrupting the
  /// sequence, so they wait here until the replay finishes.
  queue: Vec<builder::Message>,
}

impl Replay {
  /// Begin a replay for the range the peer asked for.
  ///
  /// `next_out_seq_num` is the session's current outbound counter, used to
  /// resolve an open-ended request (`EndSeqNo` of 0) into a concrete end.
  pub(crate) fn start(
    begin_seq_no: u32,
    end_seq_no: u32,
    next_out_seq_num: u32,
  ) -> Result<Self> {
    if end_seq_no > 0 && begin_seq_no > end_seq_no {
      return Err(Error::protocol_violation("Invalid ResendRequest"));
    }
    // Sequence numbers start at 1, so a request beginning at 0 is malformed.
    // Left unchecked it would set `next_expected_seq_num` to 0 and could put a
    // gap fill carrying `MsgSeqNum=0` on the wire.
    if begin_seq_no == 0 {
      return Err(Error::protocol_violation(
        "Invalid ResendRequest; BeginSeqNo must be at least 1",
      ));
    }

    Ok(Self {
      begin_seq_no,
      // EndSeqNo of 0 means "everything since BeginSeqNo", which is everything
      // we have sent so far.
      end_seq_no: if end_seq_no > 0 {
        end_seq_no
      } else {
        next_out_seq_num.saturating_sub(1)
      },
      next_expected_seq_num: begin_seq_no,
      gap_fill_count: 0,
      queue: Vec::new(),
    })
  }

  /// Hold an ordinary outbound message until the replay finishes.
  pub(crate) fn defer(&mut self, msg: builder::Message) {
    self.queue.push(msg);
  }

  /// Take the deferred messages, leaving the queue empty.
  pub(crate) fn take_queue(&mut self) -> Vec<builder::Message> {
    std::mem::take(&mut self.queue)
  }

  /// Account for a message the application offered for retransmission.
  pub(crate) fn offer(
    &mut self,
    msg_seq_num: u32,
    is_admin: bool,
  ) -> ReplayStep {
    if msg_seq_num < self.next_expected_seq_num {
      // Already accounted for.
      return ReplayStep::Skip;
    }
    if msg_seq_num > self.end_seq_no {
      // Beyond what the peer asked for. Retransmitting it would put a sequence
      // number on the wire that we are about to reuse for a new message.
      return ReplayStep::Skip;
    }

    self.gap_fill_count += msg_seq_num - self.next_expected_seq_num;
    self.next_expected_seq_num = msg_seq_num + 1;

    if is_admin {
      // Admin messages are never retransmitted; the gap fill stands in.
      self.gap_fill_count += 1;
      return ReplayStep::Absorb;
    }

    // Close off the run of skipped messages preceding this one. The gap fill
    // must stop at msg_seq_num - 1, because msg_seq_num itself is about to be
    // retransmitted.
    let gap_fill = if self.gap_fill_count > 0 {
      let first_skipped = self.next_expected_seq_num - self.gap_fill_count - 1;
      Some((first_skipped, msg_seq_num - 1))
    } else {
      None
    };
    self.gap_fill_count = 0;

    ReplayStep::Retransmit { gap_fill }
  }

  /// The gap fill needed to cover whatever is left of the requested range, if
  /// anything is.
  pub(crate) fn trailing_gap_fill(&self) -> Option<(u32, u32)> {
    let begin = self.next_expected_seq_num - self.gap_fill_count;
    (begin <= self.end_seq_no).then_some((begin, self.end_seq_no))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn replay(begin: u32, end: u32) -> Replay {
    Replay::start(begin, end, 100).unwrap()
  }

  #[test]
  fn open_ended_request_resolves_to_everything_sent() {
    // next_out_seq_num of 8 means 7 was the last number used.
    let r = Replay::start(3, 0, 8).unwrap();
    assert_eq!(r.begin_seq_no, 3);
    assert_eq!(r.end_seq_no, 7);
  }

  #[test]
  fn inverted_range_is_rejected() {
    assert!(Replay::start(9, 4, 100).is_err());
  }

  #[test]
  fn begin_seq_no_of_zero_is_rejected() {
    // Sequence numbers are 1-based. Accepting 0 here would eventually put a
    // gap fill carrying MsgSeqNum=0 on the wire.
    assert!(Replay::start(0, 5, 100).is_err());
    assert!(Replay::start(0, 0, 100).is_err());
  }

  #[test]
  fn a_contiguous_run_needs_no_gap_fill() {
    let mut r = replay(1, 3);
    for seq in 1..=3 {
      assert_eq!(
        r.offer(seq, false),
        ReplayStep::Retransmit { gap_fill: None }
      );
    }
    assert_eq!(r.trailing_gap_fill(), None);
  }

  #[test]
  fn skipped_numbers_are_gap_filled_before_the_next_retransmit() {
    let mut r = replay(1, 5);
    assert_eq!(r.offer(1, false), ReplayStep::Retransmit { gap_fill: None });
    // 2 and 3 are never offered; 4 arrives next.
    assert_eq!(
      r.offer(4, false),
      ReplayStep::Retransmit {
        gap_fill: Some((2, 3))
      }
    );
    assert_eq!(r.trailing_gap_fill(), Some((5, 5)));
  }

  #[test]
  fn admin_messages_are_absorbed_into_the_gap_fill() {
    let mut r = replay(1, 4);
    assert_eq!(r.offer(1, true), ReplayStep::Absorb);
    assert_eq!(r.offer(2, true), ReplayStep::Absorb);
    // The two admin messages become part of the gap fill preceding 3.
    assert_eq!(
      r.offer(3, false),
      ReplayStep::Retransmit {
        gap_fill: Some((1, 2))
      }
    );
    assert_eq!(r.trailing_gap_fill(), Some((4, 4)));
  }

  #[test]
  fn messages_outside_the_range_are_skipped() {
    let mut r = replay(3, 5);
    assert_eq!(r.offer(2, false), ReplayStep::Skip);
    assert_eq!(r.offer(6, false), ReplayStep::Skip);
    // Skipping must not disturb the bookkeeping.
    assert_eq!(r.offer(3, false), ReplayStep::Retransmit { gap_fill: None });
  }

  #[test]
  fn a_message_offered_twice_is_only_counted_once() {
    let mut r = replay(1, 3);
    // Starting at 2 leaves 1 unaccounted for, so it is gap-filled first.
    assert_eq!(
      r.offer(2, false),
      ReplayStep::Retransmit {
        gap_fill: Some((1, 1))
      }
    );
    // The repeat is behind the cursor and must not shift the bookkeeping.
    assert_eq!(r.offer(2, false), ReplayStep::Skip);
    assert_eq!(r.trailing_gap_fill(), Some((3, 3)));
  }

  #[test]
  fn an_entirely_unanswered_replay_gap_fills_the_whole_range() {
    let r = replay(4, 9);
    assert_eq!(r.trailing_gap_fill(), Some((4, 9)));
  }

  #[test]
  fn a_fully_answered_replay_needs_no_trailing_gap_fill() {
    let mut r = replay(1, 2);
    r.offer(1, false);
    r.offer(2, false);
    assert_eq!(r.trailing_gap_fill(), None);
  }

  #[test]
  fn trailing_admin_messages_extend_the_final_gap_fill() {
    let mut r = replay(1, 4);
    assert_eq!(r.offer(1, false), ReplayStep::Retransmit { gap_fill: None });
    assert_eq!(r.offer(2, true), ReplayStep::Absorb);
    assert_eq!(r.offer(3, true), ReplayStep::Absorb);
    // 2, 3 absorbed and 4 never offered: all three are covered at the end.
    assert_eq!(r.trailing_gap_fill(), Some((2, 4)));
  }

  /// The invariant every subtraction above depends on: the first skipped
  /// number is never below `begin_seq_no`, so no gap fill can name a sequence
  /// number the peer did not ask for, and none of the `u32` arithmetic can
  /// underflow.
  #[test]
  fn gap_fill_bounds_stay_inside_the_requested_range() {
    for begin in 1u32..=4 {
      for end in begin..=8 {
        for offered in begin..=end {
          let mut r = replay(begin, end);
          if let ReplayStep::Retransmit {
            gap_fill: Some((lo, hi)),
          } = r.offer(offered, false)
          {
            assert!(lo >= begin, "{lo} < {begin}");
            assert!(hi < offered, "{hi} >= {offered}");
            assert!(lo <= hi, "{lo} > {hi}");
          }
          if let Some((lo, hi)) = r.trailing_gap_fill() {
            assert!(lo >= begin, "trailing {lo} < {begin}");
            assert!(hi <= end, "trailing {hi} > {end}");
            assert!(lo <= hi, "trailing {lo} > {hi}");
          }
        }
      }
    }
  }
}
