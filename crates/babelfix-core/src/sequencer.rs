//! Message-centric sequencing: number each outbound message, have the
//! application persist it, and only then let it reach the wire.
//!
//! The session layer allocates no sequence numbers of its own: every outbound
//! message arrives numbered, and every admin message it wants sent is handed to
//! the application to number (see [`session`](crate::session)). That suits an
//! application whose own event stream defines the order of what it sends. Most
//! applications want something simpler, which is what most FIX engines do:
//!
//! 1. give the message the next sequence number, and a `SendingTime`;
//! 2. persist it;
//! 3. send it once — and only once — the write has completed.
//!
//! A crash between any two of those steps is recoverable. Before 2 completes
//! nothing was sent, so the number is free to reuse; after it, the message is
//! in the store, and a peer that missed it will ask for it.
//!
//! [`Sequencer`] is that policy, as a sans-io state machine. It stores
//! nothing: it asks for writes with [`Persist`] and is told when they complete.
//! It also tracks which inbound messages have been handled, and asks for the
//! resulting watermark — the inbound sequence number to resume from — to be
//! persisted too.
//!
//! # Driving it
//!
//! Everything the session emits goes through the sequencer on its way to the
//! application, so that it can number admin messages and follow the inbound
//! sequence. [`Sequencer::sink`] wraps the application's [`SeqSink`] for that:
//!
//! ```no_run
//! # use std::time::Instant;
//! # use babelfix_core::driver::SessionDriver;
//! # use babelfix_core::sequencer::{Persist, SeqEvent, Sequencer};
//! # fn run(driver: &mut SessionDriver, seq: &mut Sequencer, bytes: &[u8],
//! #        msg: babelfix_core::Message) -> babelfix_core::Result<()> {
//! let mut writes = Vec::new();
//! let mut app = |event: SeqEvent<'_>| {
//!   if let SeqEvent::Persist(Persist::Outbound { seq_num, msg }) = event {
//!     writes.push((seq_num, msg.to_bytes()));  // start the write
//!   }
//!   Ok(())
//! };
//!
//! driver.on_bytes(Instant::now(), bytes, &mut seq.sink(&mut app))?;
//! seq.send(msg, &mut app)?;
//!
//! // ... later, as each write completes:
//! # let seq_num = 1;
//! seq.persisted(seq_num, Instant::now(), driver, &mut app)?;
//! // write(fd, driver.pending_writes())...
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeSet, VecDeque};
use std::time::Instant;

use chrono::{DateTime, Utc};

use crate::driver::Clock;
use crate::message::Message;
use crate::session::fields::{
  MsgSeqNum, OrigSendingTime, PossDupFlag, SenderCompID, SendingTime,
  TargetCompID,
};
use crate::session::{
  Command, Event, EventSink, Progress, SessionConfig, SessionIdentifier,
};
use crate::time::TimePrecision;
use crate::{Error, Result};

/// Where a session resumes: what the application persisted last time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resume {
  /// One past the highest outbound sequence number persisted.
  pub next_out_seq_num: u64,
  /// The persisted inbound watermark.
  pub next_in_seq_num: u64,
}

impl Resume {
  /// A session starting from scratch.
  pub fn new() -> Self {
    Self {
      next_out_seq_num: 1,
      next_in_seq_num: 1,
    }
  }
}

impl Default for Resume {
  fn default() -> Self {
    Self::new()
  }
}

/// How the inbound watermark — the sequence number to resume from — follows
/// the messages the application handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundPolicy {
  /// A message counts as handled as soon as it is delivered. At most once: a
  /// crash while processing it loses it. Fine for tools.
  OnDelivery,
  /// A message counts as handled when the application says so with
  /// [`Sequencer::handled`].
  Explicit(WatermarkMode),
}

/// Where the watermark sits when messages are handled out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkMode {
  /// At the earliest message not yet handled. An application that handles
  /// messages in order needs nothing else; one that does not may see a message
  /// again after a restart, if a later one was handled first.
  Contiguous,
  /// Just past the latest message handled. For applications handling messages
  /// concurrently that are idempotent on the sequence number. A message still
  /// in progress when a later one completes is *not* asked for again after a
  /// restart, so this is only safe if its effects are durable some other way.
  Highest,
}

/// A write the sequencer needs before it can go on.
#[derive(Debug)]
pub enum Persist<'a> {
  /// Store this message. It is exactly what will go on the wire: `MsgSeqNum`,
  /// `SendingTime` and the CompIDs are in its header. It is not sent until
  /// [`Sequencer::persisted`] says the write has completed.
  ///
  /// Admin messages are persisted too, because the next session resumes one
  /// past the highest number persisted. Their content does not matter for
  /// recovery — a replay gap-fills them — so storing a placeholder is enough.
  Outbound { seq_num: u64, msg: &'a Message },
  /// Store the inbound sequence number to resume from. At most one is in
  /// flight at a time; report it with [`Sequencer::watermark_persisted`].
  Watermark { next_in_seq_num: u64 },
}

/// What a sequenced session tells the application about.
#[derive(Debug)]
pub enum SeqEvent<'a> {
  /// A write the session depends on.
  Persist(Persist<'a>),
  /// Everything else, as the session emitted it. Admin requests
  /// ([`Event::AdminSendRequired`]) never appear here: the sequencer numbers
  /// and persists them itself.
  Session(Event<'a>),
}

/// The application-facing half of a sequenced session. A closure will do.
pub trait SeqSink {
  fn event(&mut self, event: SeqEvent<'_>) -> Result<()>;
}

impl<F> SeqSink for F
where
  F: FnMut(SeqEvent<'_>) -> Result<()>,
{
  fn event(&mut self, event: SeqEvent<'_>) -> Result<()> {
    self(event)
  }
}

/// Something a [`Command`] can be applied to: each stage of a session's life,
/// in the [`driver`](crate::driver) module.
pub trait CommandTarget {
  fn on_command(
    &mut self,
    now: Instant,
    cmd: Command,
    sink: &mut impl EventSink,
  ) -> Result<Progress>;
}

#[derive(Debug)]
struct InFlight {
  seq_num: u64,
  msg: Message,
  persisted: bool,
}

/// The message-centric sequencing policy. See the [module docs](self).
#[derive(Debug)]
pub struct Sequencer {
  sender_comp_id: String,
  target_comp_id: String,
  next_out_seq_num: u64,
  precision: TimePrecision,
  clock: Clock,
  max_in_flight: usize,

  /// Numbered and waiting for their writes, oldest first.
  in_flight: VecDeque<InFlight>,
  /// Persisted, in order, and waiting to be sent.
  ready: VecDeque<Message>,
  /// The session has logged on, so more than its Logon may be sent.
  logged_on: bool,
  /// A write failed: nothing more may be sent.
  failed: bool,

  policy: InboundPolicy,
  /// The next inbound sequence number the session expects.
  session_next_in: u64,
  /// Delivered application messages not yet handled, for
  /// [`WatermarkMode::Contiguous`].
  unhandled: BTreeSet<u64>,
  /// Just past the latest message handled, for [`WatermarkMode::Highest`].
  highest_handled: u64,
  watermark_persisted: u64,
  watermark_in_flight: Option<u64>,
}

impl Sequencer {
  /// A sequencer for one connection of the session `session_id`.
  ///
  /// `clock` stamps the `SendingTime` of messages that have none, at the
  /// session's precision, at the moment they are numbered: the stored message
  /// is then exactly the one sent, so a replay always knows its original
  /// `SendingTime`.
  pub fn new(
    session_id: &SessionIdentifier,
    config: &SessionConfig,
    resume: Resume,
    policy: InboundPolicy,
    clock: Clock,
  ) -> Self {
    Self {
      sender_comp_id: session_id.sender_comp_id.clone(),
      target_comp_id: session_id.target_comp_id.clone(),
      next_out_seq_num: resume.next_out_seq_num,
      precision: config.time_precision,
      clock,
      max_in_flight: 1024,
      in_flight: VecDeque::new(),
      ready: VecDeque::new(),
      logged_on: false,
      failed: false,
      policy,
      session_next_in: resume.next_in_seq_num,
      unhandled: BTreeSet::new(),
      highest_handled: resume.next_in_seq_num,
      watermark_persisted: resume.next_in_seq_num,
      watermark_in_flight: None,
    }
  }

  /// Limit how many application messages may await their writes at once.
  /// Beyond it, [`send`](Self::send) returns [`Error::Busy`]. Admin messages
  /// are never refused. Defaults to 1024.
  pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
    self.max_in_flight = max_in_flight;
    self
  }

  /// Whether [`send`](Self::send) would refuse another message.
  pub fn is_full(&self) -> bool {
    self.in_flight.len() >= self.max_in_flight
  }

  /// The sequence number the next message will be given.
  pub fn next_out_seq_num(&self) -> u64 {
    self.next_out_seq_num
  }

  /// The inbound sequence number to resume from, as things stand in memory.
  pub fn watermark(&self) -> u64 {
    match self.policy {
      InboundPolicy::OnDelivery => self.session_next_in,
      InboundPolicy::Explicit(WatermarkMode::Contiguous) => self
        .unhandled
        .first()
        .copied()
        .unwrap_or(self.session_next_in),
      InboundPolicy::Explicit(WatermarkMode::Highest) => self.highest_handled,
    }
  }

  /// How many messages are numbered but not yet persisted.
  pub fn in_flight(&self) -> usize {
    self.in_flight.len()
  }

  /// Wrap the application's sink, so that what the session emits passes
  /// through the sequencer on its way.
  pub fn sink<'a, S: SeqSink>(
    &'a mut self,
    app: &'a mut S,
  ) -> Intercept<'a, S> {
    Intercept { seq: self, app }
  }

  /// Number an application message, stamp its `SendingTime` if it has none,
  /// and ask for it to be persisted. Returns its sequence number.
  ///
  /// It is a new message under a new number, so it cannot be a possible
  /// duplicate: any `PossDupFlag` or `OrigSendingTime` is removed. A
  /// `SendingTime` it already has is kept. To send exactly what was built, use
  /// [`send_raw`](Self::send_raw).
  pub fn send(
    &mut self,
    mut msg: Message,
    app: &mut impl SeqSink,
  ) -> Result<u64> {
    if self.in_flight.len() >= self.max_in_flight {
      return Err(Error::Busy);
    }
    msg.header_mut().remove(PossDupFlag).remove(OrigSendingTime);
    self.assign(msg, app)
  }

  /// Persist and send a message with the `MsgSeqNum` it already carries,
  /// along with any `PossDupFlag`, `OrigSendingTime` or other header field —
  /// for test tools that need to send exactly what they built.
  ///
  /// The number may jump forward, and numbering continues after it; it may not
  /// go back.
  pub fn send_raw(
    &mut self,
    mut msg: Message,
    app: &mut impl SeqSink,
  ) -> Result<u64> {
    self.check_live()?;
    let seq_num = msg
      .header()
      .get(MsgSeqNum)?
      .ok_or_else(|| Error::protocol_violation("send_raw needs a MsgSeqNum"))?;
    if seq_num < self.next_out_seq_num {
      return Err(Error::protocol_violation(format!(
        "MsgSeqNum {seq_num} is behind the next sequence number, {}",
        self.next_out_seq_num
      )));
    }
    self.next_out_seq_num = seq_num + 1;
    self.stamp(&mut msg);
    self.request_write(seq_num, msg, app)
  }

  /// The write of `seq_num` has completed. Whatever is now persisted, in
  /// order, is sent through `target`.
  pub fn persisted<T: CommandTarget, S: SeqSink>(
    &mut self,
    seq_num: u64,
    now: Instant,
    target: &mut T,
    app: &mut S,
  ) -> Result<Progress> {
    self.check_live()?;
    let entry = self
      .in_flight
      .iter_mut()
      .find(|e| e.seq_num == seq_num)
      .ok_or_else(|| {
        Error::protocol_violation(format!(
          "MsgSeqNum {seq_num} is not awaiting persistence"
        ))
      })?;
    entry.persisted = true;

    while self.in_flight.front().is_some_and(|e| e.persisted) {
      let entry = self.in_flight.pop_front().expect("checked above");
      self.ready.push_back(entry.msg);
    }

    self.release(now, target, app)
  }

  /// The write of `seq_num` failed. Nothing more is sent: the session cannot
  /// carry on without the message, or reuse its number. The error returned
  /// is for the driver to end the session with.
  pub fn persist_failed(&mut self, seq_num: u64) -> Error {
    self.failed = true;
    Error::persistence(format!("MsgSeqNum {seq_num} could not be persisted"))
  }

  /// Send whatever is persisted and may now go out: everything, once the
  /// session has logged on, and only our Logon before that.
  ///
  /// [`persisted`](Self::persisted) does this itself. Messages persisted
  /// before the session logged on need no separate call either: logging on
  /// always asks for the synchronisation TestRequest, and its completion
  /// releases them, ahead of it. This is for drivers that want to release
  /// explicitly anyway.
  pub fn release<T: CommandTarget, S: SeqSink>(
    &mut self,
    now: Instant,
    target: &mut T,
    app: &mut S,
  ) -> Result<Progress> {
    while let Some(msg) = self.pop_ready() {
      let progress =
        target.on_command(now, Command::Send(msg), &mut self.sink(app))?;
      if progress.is_close() {
        return Ok(Progress::Close);
      }
    }
    Ok(Progress::Continue)
  }

  /// The application has finished with inbound `seq_num`, under
  /// [`InboundPolicy::Explicit`].
  pub fn handled(
    &mut self,
    seq_num: u64,
    app: &mut impl SeqSink,
  ) -> Result<()> {
    match self.policy {
      InboundPolicy::OnDelivery => {}
      InboundPolicy::Explicit(WatermarkMode::Contiguous) => {
        self.unhandled.remove(&seq_num);
      }
      InboundPolicy::Explicit(WatermarkMode::Highest) => {
        self.highest_handled = self.highest_handled.max(seq_num + 1);
      }
    }
    self.persist_watermark(app)
  }

  /// The watermark write of `next_in_seq_num` has completed.
  pub fn watermark_persisted(
    &mut self,
    next_in_seq_num: u64,
    app: &mut impl SeqSink,
  ) -> Result<()> {
    self.watermark_persisted = self.watermark_persisted.max(next_in_seq_num);
    if self.watermark_in_flight == Some(next_in_seq_num) {
      self.watermark_in_flight = None;
    }
    self.persist_watermark(app)
  }

  fn check_live(&self) -> Result<()> {
    if self.failed {
      return Err(Error::persistence(
        "an earlier write failed; the session cannot continue",
      ));
    }
    Ok(())
  }

  fn pop_ready(&mut self) -> Option<Message> {
    let front = self.ready.front()?;
    if !self.logged_on && front.msg_type() != "A" {
      return None;
    }
    self.ready.pop_front()
  }

  /// Fill in the header the way it will go on the wire.
  fn stamp(&self, msg: &mut Message) {
    let stamp = !msg.header().has(SendingTime);
    let mut header = msg.header_mut();
    header
      .set(SenderCompID, self.sender_comp_id.as_str())
      .set(TargetCompID, self.target_comp_id.as_str());
    if stamp {
      let now: DateTime<Utc> = (self.clock)();
      header.set(SendingTime, (now, self.precision));
    }
  }

  fn assign(
    &mut self,
    mut msg: Message,
    app: &mut impl SeqSink,
  ) -> Result<u64> {
    self.check_live()?;
    let seq_num = self.next_out_seq_num;
    self.next_out_seq_num += 1;
    msg.header_mut().set(MsgSeqNum, seq_num);
    self.stamp(&mut msg);
    self.request_write(seq_num, msg, app)
  }

  fn request_write(
    &mut self,
    seq_num: u64,
    msg: Message,
    app: &mut impl SeqSink,
  ) -> Result<u64> {
    self.in_flight.push_back(InFlight {
      seq_num,
      msg,
      persisted: false,
    });
    let msg = &self.in_flight.back().expect("just pushed").msg;
    app.event(SeqEvent::Persist(Persist::Outbound { seq_num, msg }))?;
    Ok(seq_num)
  }

  fn persist_watermark(&mut self, app: &mut impl SeqSink) -> Result<()> {
    let watermark = self.watermark();
    if watermark > self.watermark_persisted
      && self.watermark_in_flight.is_none()
    {
      self.watermark_in_flight = Some(watermark);
      app.event(SeqEvent::Persist(Persist::Watermark {
        next_in_seq_num: watermark,
      }))?;
    }
    Ok(())
  }

  fn on_session_event(
    &mut self,
    event: Event<'_>,
    app: &mut impl SeqSink,
  ) -> Result<()> {
    match event {
      Event::AdminSendRequired(msg) => {
        self.assign(msg, app)?;
        Ok(())
      }
      Event::LoggedOn => {
        self.logged_on = true;
        app.event(SeqEvent::Session(Event::LoggedOn))
      }
      Event::MessageReceived { seq_num, msg } => {
        self.session_next_in = self.session_next_in.max(seq_num + 1);
        if self.policy == InboundPolicy::Explicit(WatermarkMode::Contiguous) {
          self.unhandled.insert(seq_num);
        }
        app
          .event(SeqEvent::Session(Event::MessageReceived { seq_num, msg }))?;
        self.persist_watermark(app)
      }
      Event::InboundAdvanced { next_in_seq_num } => {
        self.session_next_in = self.session_next_in.max(next_in_seq_num);
        // Admin traffic needs no handling: it is done with on arrival.
        if self.policy == InboundPolicy::Explicit(WatermarkMode::Highest) {
          self.highest_handled = self.highest_handled.max(next_in_seq_num);
        }
        app.event(SeqEvent::Session(Event::InboundAdvanced {
          next_in_seq_num,
        }))?;
        self.persist_watermark(app)
      }
      event => app.event(SeqEvent::Session(event)),
    }
  }
}

/// The application's sink, wrapped by [`Sequencer::sink`] so that the session's
/// events pass through the sequencer.
pub struct Intercept<'a, S> {
  seq: &'a mut Sequencer,
  app: &'a mut S,
}

impl<S: SeqSink> EventSink for Intercept<'_, S> {
  fn event(&mut self, event: Event<'_>) -> Result<()> {
    self.seq.on_session_event(event, self.app)
  }
}
