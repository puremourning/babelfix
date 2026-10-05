//! Event-centric ordering: put messages on the wire in `MsgSeqNum` order when
//! more than one sender numbers them.
//!
//! An application that numbers its own messages (see
//! [`session`](crate::session)) and sends from more than one thread can hand
//! them to the session out of order: one thread numbers 9, another 10, and 10
//! arrives first. The session sends 10, notes the jump, and then refuses 9 as
//! already sent. Nothing is lost — the peer asks for 9 and the application
//! replays it — but every such race costs a resend, and the peer sees messages
//! out of order that were produced in order.
//!
//! A [`ReorderWindow`] sits in front of the session and holds a message that
//! arrives ahead of the next expected number until the missing ones arrive,
//! then releases the run in order. It waits only so long:
//!
//! * at most [`max_held`](ReorderConfig::max_held) numbers ahead of the gap. A
//!   message further ahead gives up on the gap: everything held goes out, gap
//!   and all, and the peer recovers the gap with a ResendRequest as before;
//! * at most [`max_wait`](ReorderConfig::max_wait) for any one message. The
//!   window reads no clock and arms no timer: expiry is checked whenever it is
//!   called, against the `now` it is given. [`ReorderWindow::expire`] is for
//!   calls that offer nothing, and [`ReorderWindow::next_expiry`] is there for
//!   a driver that wants a timer anyway.
//!
//! A message whose number has already been passed, because the gap it would
//! have filled was given up on, comes back as [`Offered::Late`]: the peer will
//! ask for it, and the application replays it then.
//!
//! The common case costs one comparison: a message numbered one past the last
//! goes straight through, as does everything when only one thread sends.
//!
//! ```no_run
//! # use std::time::Instant;
//! # use babelfix_core::driver::SessionDriver;
//! # use babelfix_core::reorder::{Offered, ReorderConfig, ReorderWindow};
//! # use babelfix_core::session::Command;
//! # fn run(driver: &mut SessionDriver, msg: babelfix_core::Message)
//! #     -> babelfix_core::Result<()> {
//! let mut window = ReorderWindow::new(ReorderConfig::default());
//! let now = Instant::now();
//! let mut sink = ();
//! let offered = window.offer(msg, now, |msg| {
//!   driver.on_command(now, Command::Send(msg), &mut sink)
//! })?;
//! if let Offered::Late(msg) = offered {
//!   // Its number went out as a gap; the peer will ask for it.
//! }
//! # Ok(())
//! # }
//! ```

use std::time::{Duration, Instant};

use crate::Result;
use crate::message::Message;
use crate::session::Progress;
use crate::session::fields::MsgSeqNum;

/// How long a [`ReorderWindow`] waits for a missing message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReorderConfig {
  /// How far ahead of the next expected `MsgSeqNum` a message may be and
  /// still be held. Zero disables the window: everything goes straight
  /// through, in the order offered.
  pub max_held: usize,
  /// How long a message is held before the gap ahead of it is given up on.
  pub max_wait: Duration,
}

impl ReorderConfig {
  /// No reordering: messages go out in the order they are offered.
  pub const DISABLED: Self = Self {
    max_held: 0,
    max_wait: Duration::ZERO,
  };
}

/// Ten messages, for a millisecond: enough to absorb the skew between threads
/// numbering consecutive messages, and short enough that a peer barely notices
/// a sender that has stalled.
impl Default for ReorderConfig {
  fn default() -> Self {
    Self {
      max_held: 10,
      max_wait: Duration::from_millis(1),
    }
  }
}

/// What became of an offered message.
#[must_use]
#[derive(Debug)]
pub enum Offered {
  /// Sent, or held to be sent in order. The [`Progress`] is that of the last
  /// message released; once it is [`Progress::Close`] nothing more is
  /// released, and whatever is still held is never sent.
  Taken(Progress),
  /// Its number was already passed, or is already held. It is handed back
  /// unsent.
  Late(Message),
}

#[derive(Debug)]
struct Held {
  seq_num: u64,
  expires: Instant,
  msg: Message,
}

/// Holds messages that arrive ahead of their turn. See the
/// [module docs](self).
#[derive(Debug)]
pub struct ReorderWindow {
  max_wait: Duration,
  /// Indexed by `MsgSeqNum % len`. Held numbers lie in
  /// `next + 1 ..= next + len`, so no two share a slot.
  slots: Box<[Option<Held>]>,
  held: usize,
  /// The number expected next: everything below it has been released or
  /// given up on. `None` until the first message, which goes straight through.
  next: Option<u64>,
  /// The earliest expiry of anything held.
  earliest: Option<Instant>,
}

impl ReorderWindow {
  pub fn new(config: ReorderConfig) -> Self {
    Self {
      max_wait: config.max_wait,
      slots: (0..config.max_held).map(|_| None).collect(),
      held: 0,
      next: None,
      earliest: None,
    }
  }

  /// Release `msg` through `release` if it is next, along with anything held
  /// that follows it, or hold it until it is.
  ///
  /// Anything that has waited past `max_wait` by `now` is released first. A
  /// message without a usable `MsgSeqNum` goes straight through, for the
  /// session to refuse.
  ///
  /// An error from `release` is returned at once. The message it was given is
  /// gone, and anything still held stays held.
  pub fn offer(
    &mut self,
    msg: Message,
    now: Instant,
    mut release: impl FnMut(Message) -> Result<Progress>,
  ) -> Result<Offered> {
    if self.slots.is_empty() {
      return release(msg).map(Offered::Taken);
    }
    let seq_num = match msg.header().get(MsgSeqNum) {
      Ok(Some(n)) if n > 0 => n,
      _ => return release(msg).map(Offered::Taken),
    };
    if self.expire(now, &mut release)?.is_close() {
      return Ok(Offered::Taken(Progress::Close));
    }
    let Some(next) = self.next else {
      self.next = Some(seq_num + 1);
      return release(msg).map(Offered::Taken);
    };

    if seq_num < next {
      return Ok(Offered::Late(msg));
    }
    if seq_num == next {
      self.next = Some(next + 1);
      if release(msg)?.is_close() {
        return Ok(Offered::Taken(Progress::Close));
      }
      return self.release_run(&mut release).map(Offered::Taken);
    }

    let len = self.slots.len() as u64;
    if seq_num - next > len {
      // Too far ahead to wait for the gap: give up on everything up to it.
      if self.release_through(seq_num - 1, &mut release)?.is_close() {
        return Ok(Offered::Taken(Progress::Close));
      }
      self.next = Some(seq_num + 1);
      if release(msg)?.is_close() {
        return Ok(Offered::Taken(Progress::Close));
      }
      return self.release_run(&mut release).map(Offered::Taken);
    }

    let slot = &mut self.slots[(seq_num % len) as usize];
    if slot.is_some() {
      return Ok(Offered::Late(msg));
    }
    let expires = now + self.max_wait;
    *slot = Some(Held {
      seq_num,
      expires,
      msg,
    });
    self.held += 1;
    self.earliest = Some(self.earliest.map_or(expires, |e| e.min(expires)));
    Ok(Offered::Taken(Progress::Continue))
  }

  /// Release, gaps and all, everything held that has waited past `max_wait`
  /// by `now`, and everything held behind it.
  ///
  /// [`offer`](Self::offer) does this itself. Call it from anything else that
  /// happens on the session — inbound bytes, a tick — so a held message does
  /// not wait for the next offer.
  pub fn expire(
    &mut self,
    now: Instant,
    mut release: impl FnMut(Message) -> Result<Progress>,
  ) -> Result<Progress> {
    if self.earliest.is_none_or(|e| e > now) {
      return Ok(Progress::Continue);
    }
    let through = self
      .slots
      .iter()
      .flatten()
      .filter(|h| h.expires <= now)
      .map(|h| h.seq_num)
      .max();
    let Some(through) = through else {
      return Ok(Progress::Continue);
    };
    if self.release_through(through, &mut release)?.is_close() {
      return Ok(Progress::Close);
    }
    self.next = Some(through + 1);
    self.release_run(&mut release)
  }

  /// When [`expire`](Self::expire) next has something to do, if anything is
  /// held.
  pub fn next_expiry(&self) -> Option<Instant> {
    self.earliest
  }

  /// How many messages are held.
  pub fn held(&self) -> usize {
    self.held
  }

  /// The `MsgSeqNum` the window expects next, once it has seen a message.
  pub fn next_seq_num(&self) -> Option<u64> {
    self.next
  }

  /// Take the message held under `seq_num`, if there is one.
  fn take(&mut self, seq_num: u64) -> Option<Message> {
    let len = self.slots.len() as u64;
    let slot = &mut self.slots[(seq_num % len) as usize];
    if slot.as_ref().is_none_or(|h| h.seq_num != seq_num) {
      return None;
    }
    let held = slot.take()?;
    self.held -= 1;
    if self.held == 0 {
      self.earliest = None;
    } else if self.earliest == Some(held.expires) {
      self.earliest = self.slots.iter().flatten().map(|h| h.expires).min();
    }
    Some(held.msg)
  }

  /// Release everything held up to and including `through`, in order,
  /// skipping the gaps. Leaves `next` for the caller.
  fn release_through(
    &mut self,
    through: u64,
    release: &mut impl FnMut(Message) -> Result<Progress>,
  ) -> Result<Progress> {
    let Some(next) = self.next else {
      return Ok(Progress::Continue);
    };
    let last = through.min(next + self.slots.len() as u64);
    for seq_num in next..=last {
      if self.held == 0 {
        break;
      }
      if let Some(msg) = self.take(seq_num)
        && release(msg)?.is_close()
      {
        return Ok(Progress::Close);
      }
    }
    Ok(Progress::Continue)
  }

  /// Release the run of held messages starting at `next`.
  fn release_run(
    &mut self,
    release: &mut impl FnMut(Message) -> Result<Progress>,
  ) -> Result<Progress> {
    while self.held > 0 {
      let Some(next) = self.next else { break };
      let Some(msg) = self.take(next) else { break };
      self.next = Some(next + 1);
      if release(msg)?.is_close() {
        return Ok(Progress::Close);
      }
    }
    Ok(Progress::Continue)
  }
}
