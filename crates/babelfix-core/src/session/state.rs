//! The session state machine proper. It performs no I/O: it writes messages
//! and events into a [`SessionOutput`].

use std::time::Instant;

use tracing::{debug, error, info};

use super::fields::*;
use super::replay::{Replay, ReplayStep};
use super::{
  Command, Event, Progress, SessionConfig, SessionIdentifier, SessionOutput,
  Unstamped,
};
use crate::message::Message;
use crate::time::MAX_LEN;
use crate::{Error, Result};

/// A `SendingTime` the application did not supply is stamped by the
/// [`SessionOutput`], as late as it can be. The state machine reserves the
/// field — a placeholder of exactly the stamp's width — so the stamp is an
/// in-place write.
const SENDING_TIME_PLACEHOLDER: &[u8; MAX_LEN] =
  b"00000000-00:00:00.000000000000";

/// How many heartbeat intervals may pass without a word from the peer before
/// the session gives up on it.
const MISSED_HEARTBEATS_BEFORE_LOGOUT: u32 = 3;

/// Heartbeat deadlines, held as absolute instants.
///
/// Deadlines rather than intervals: a session blocked past two deadlines sees
/// one late timeout, not two back to back, so it cannot jump straight to two
/// missed heartbeats.
#[derive(Debug)]
struct Timers {
  interval: std::time::Duration,
  /// When to send the next heartbeat, absent other outbound traffic.
  next_out: Instant,
  /// When to count the peer as having missed one.
  next_in: Instant,
  missed_heartbeats: u32,
}

impl Timers {
  fn new(interval: std::time::Duration, now: Instant) -> Self {
    Self {
      interval,
      next_out: now + interval,
      next_in: now + interval,
      missed_heartbeats: 0,
    }
  }

  fn reset_out(&mut self, now: Instant) {
    self.next_out = now + self.interval;
  }

  fn reset_in(&mut self, now: Instant) {
    self.next_in = now + self.interval;
    self.missed_heartbeats = 0;
  }
}

/// The FIX session state machine: sequence checking, heartbeats, recovery.
///
/// It allocates no outbound sequence numbers. Every message it wants sent —
/// the Logon, heartbeats, TestRequests, ResendRequests, Logouts — it hands to
/// the application as an [`Event::AdminSendRequired`], and the application
/// sends it back numbered with [`Command::Send`]. The only exceptions are gap
/// fills and retransmissions, which reuse numbers already sent.
///
/// See the [module docs](super) for how a driver is expected to call this.
pub struct SessionState {
  session_id: SessionIdentifier,
  config: SessionConfig,

  /// The `MsgSeqNum` expected on the next message from the peer. Held only in
  /// memory: what the application persists, and resumes from, is its own
  /// business.
  next_in_seq_num: u64,
  /// The highest `MsgSeqNum` put on the wire, or 0 before anything has been.
  /// Gap fills and retransmissions sit below it.
  highest_sent: u64,
  /// The highest `MsgSeqNum` accepted for sending, which includes messages
  /// queued behind a replay. A new message must be beyond it.
  highest_accepted: u64,

  /// Our Logon has gone out.
  logon_sent: bool,
  /// The peer's Logon has been processed.
  peer_logon_received: bool,

  /// A heartbeat has been asked for and not yet sent, so the timer does not ask
  /// again while the application is persisting it.
  heartbeat_requested: bool,
  /// A Logout has been asked for: the session is ending. Inbound traffic is
  /// ignored, and if the Logout has not been sent by this instant the session
  /// closes without it.
  closing: Option<Instant>,

  /// Set while we are waiting for the peer to close a gap: holds the sequence
  /// number after which recovery is complete.
  rerequest_in_progress: Option<u64>,
  /// `TestReqID` of the synchronisation TestRequest sent after logon; cleared
  /// when the peer echoes it back.
  recovery_tr_id: Option<String>,
  replay: Option<Replay>,
  /// The peer sent a Logout that arrived inside a sequence gap. It is
  /// acknowledged, and the session ended, once the gap has been recovered.
  peer_logout_pending: bool,

  timers: Timers,
  /// Makes `TestReqID`s unique without reading a clock, which also makes them
  /// reproducible under test.
  test_request_seq: u64,
}

impl std::fmt::Debug for SessionState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SessionState")
      .field("session_id", &self.session_id)
      .field("config", &self.config)
      .field("next_in_seq_num", &self.next_in_seq_num)
      .field("highest_sent", &self.highest_sent)
      .field("highest_accepted", &self.highest_accepted)
      .field("logon_sent", &self.logon_sent)
      .field("peer_logon_received", &self.peer_logon_received)
      .field("closing", &self.closing)
      .field("rerequest_in_progress", &self.rerequest_in_progress)
      .field("recovery_tr_id", &self.recovery_tr_id)
      .field("replay", &self.replay)
      .field("peer_logout_pending", &self.peer_logout_pending)
      .field("timers", &self.timers)
      .finish()
  }
}

impl SessionState {
  /// Build a state machine for a session about to exchange Logons.
  ///
  /// `next_in_seq_num` is the `MsgSeqNum` expected on the peer's next message,
  /// which is its Logon. `now` starts the heartbeat clocks.
  ///
  /// The [`AcceptorHandshake`](super::AcceptorHandshake) and
  /// [`InitiatorHandshake`](super::InitiatorHandshake) drive the logon
  /// exchange; a hand-rolled one calls [`request_logon`](Self::request_logon)
  /// and [`start`](Self::start).
  pub fn new(
    session_id: SessionIdentifier,
    config: SessionConfig,
    next_in_seq_num: u64,
    now: Instant,
  ) -> Self {
    let timers = Timers::new(config.heartbeat_interval, now);
    Self {
      session_id,
      config,
      next_in_seq_num,
      highest_sent: 0,
      highest_accepted: 0,
      logon_sent: false,
      peer_logon_received: false,
      heartbeat_requested: false,
      closing: None,
      rerequest_in_progress: None,
      recovery_tr_id: None,
      replay: None,
      peer_logout_pending: false,
      timers,
      test_request_seq: 0,
    }
  }

  /// The negotiated settings.
  pub fn config(&self) -> &SessionConfig {
    &self.config
  }

  pub fn session_id(&self) -> &SessionIdentifier {
    &self.session_id
  }

  /// The `MsgSeqNum` expected on the peer's next message.
  pub fn next_in_seq_num(&self) -> u64 {
    self.next_in_seq_num
  }

  /// The highest `MsgSeqNum` sent so far, or 0.
  pub fn highest_sent(&self) -> u64 {
    self.highest_sent
  }

  /// Whether both Logons have been exchanged.
  pub fn is_logged_on(&self) -> bool {
    self.logon_sent && self.peer_logon_received
  }

  /// Whether a replay is currently in progress.
  pub fn replay_in_progress(&self) -> bool {
    self.replay.is_some()
  }

  /// The next instant at which [`on_timeout`](Self::on_timeout) has something
  /// to do.
  ///
  /// Never `None` for a live session: the outbound heartbeat always has a
  /// deadline. It is an `Option` so a driver can hold a closed session without
  /// arming a timer.
  pub fn next_deadline(&self) -> Option<Instant> {
    let heartbeat = self.timers.next_out.min(self.timers.next_in);
    Some(self.closing.map_or(heartbeat, |c| c.min(heartbeat)))
  }

  /// Ask the application to send our Logon, carrying the session's negotiated
  /// settings.
  pub fn request_logon(&mut self, out: &mut impl SessionOutput) -> Result<()> {
    let logon = super::logon_message(&self.config)?;
    self.request(logon, out)
  }

  /// Process the peer's Logon and ask for the synchronisation TestRequest that
  /// establishes whether either side has missed anything.
  pub fn start(
    &mut self,
    logon: Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    if self.handle_session_message(logon, now, out)?.is_close() {
      return Ok(Progress::Close);
    }
    self.peer_logon_received = true;
    if self.closing.is_some() {
      return Ok(Progress::Continue);
    }
    if self.logon_sent {
      out.event(Event::LoggedOn)?;
    }

    self.request_sync_test_request(out)?;
    Ok(Progress::Continue)
  }

  /// Feed a decoded inbound frame.
  pub fn on_message(
    &mut self,
    msg: Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    // Liveness is evidenced by the frame arriving at all, so this happens
    // before any validation: a message rejected for a bad sequence number still
    // proves the peer is there.
    self.timers.reset_in(now);

    out.event(Event::RawMessageReceived(&msg))?;
    if self.closing.is_some() {
      debug!("Ignoring {} while closing", msg.msg_type());
      return Ok(Progress::Continue);
    }
    self.handle_session_message(msg, now, out)
  }

  /// Feed an application command.
  pub fn on_command(
    &mut self,
    cmd: Command,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    match cmd {
      Command::Send(msg) => self.send(msg, now, out),
      Command::Replay(msg) => {
        self.timers.reset_out(now);
        self.replay_message(msg, out)?;
        Ok(Progress::Continue)
      }
      Command::ReplayComplete => {
        self.timers.reset_out(now);
        self.complete_replay(out)
      }
      Command::Disconnect => {
        info!("Session disconnect requested");
        // Always announce the intent to disconnect, so the peer can tell an
        // orderly shutdown from a network failure.
        self.request_logout("Disconnect requested by application", now, out)?;
        Ok(Progress::Continue)
      }
    }
  }

  /// Advance time. Call when [`next_deadline`](Self::next_deadline) has passed.
  pub fn on_timeout(
    &mut self,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    if let Some(deadline) = self.closing {
      if now >= deadline {
        error!("Logout was not sent in time; closing without it");
        return Ok(Progress::Close);
      }
      return Ok(Progress::Continue);
    }

    if now >= self.timers.next_out {
      self.timers.reset_out(now);
      // A heartbeat already asked for is still on its way: asking again would
      // only queue a second one behind it.
      if !self.heartbeat_requested {
        self.heartbeat_requested = true;
        let heartbeat = self.message(msg_type::Heartbeat);
        self.request(heartbeat, out)?;
      }
    }

    if now >= self.timers.next_in {
      self.timers.next_in = now + self.timers.interval;
      self.timers.missed_heartbeats += 1;
      match self.timers.missed_heartbeats {
        1 => {
          debug!("Missed first heartbeat");
        }
        2 => {
          debug!("Missed second heartbeat, sending TestRequest");
          let tr_id = self.next_test_request_id("HB");
          let mut test_request = self.message(msg_type::TestRequest);
          test_request.body_mut().set(TestReqID, tr_id.as_str());
          self.request(test_request, out)?;
        }
        MISSED_HEARTBEATS_BEFORE_LOGOUT.. => {
          error!("Missed third heartbeat, logging out");
          self.request_logout("Heartbeat timeout", now, out)?;
        }
        _ => unreachable!(),
      }
    }

    Ok(Progress::Continue)
  }

  /// The peer went away without logging out.
  pub fn on_peer_closed(
    &mut self,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    info!("Client disconnected");
    out.event(Event::Disconnected)?;
    Ok(Progress::Close)
  }

  /// An empty message of the session's version.
  fn message(&self, msg_type: crate::message::MsgType) -> Message {
    Message::new(&self.config.dict, msg_type)
  }

  /// A `TestReqID` unique within the session, without consulting a clock.
  fn next_test_request_id(&mut self, prefix: &str) -> String {
    self.test_request_seq += 1;
    format!("{prefix}{}", self.test_request_seq)
  }

  // ---------------------------------------------------------------------
  // Admin requests
  // ---------------------------------------------------------------------

  /// Hand an admin message to the application to number and send.
  fn request(
    &mut self,
    msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    debug!("Requesting {} from the application", msg.msg_type());
    out.event(Event::AdminSendRequired(msg))
  }

  /// Ask for the TestRequest whose answer tells us the peer has caught up.
  fn request_sync_test_request(
    &mut self,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let tr_id = self.next_test_request_id("HELO-");
    let mut test_request = self.message(msg_type::TestRequest);
    test_request.body_mut().set(TestReqID, tr_id.as_str());
    self.recovery_tr_id = Some(tr_id);
    self.request(test_request, out)
  }

  /// Ask for a Logout carrying a diagnostic reason, and start closing.
  ///
  /// The session ends when the Logout is sent. Until then inbound traffic is
  /// ignored; if the application has not sent it within a heartbeat interval —
  /// a dead store, say — the session closes without it.
  fn request_logout(
    &mut self,
    text: &str,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    if self.closing.is_some() {
      return Ok(());
    }
    self.closing = Some(now + self.config.heartbeat_interval);
    let mut logout = self.message(msg_type::Logout);
    logout.body_mut().set(Text, text);
    self.request(logout, out)
  }

  // ---------------------------------------------------------------------
  // Transmission
  // ---------------------------------------------------------------------

  /// Accept a message the application numbered, and send it unless a replay
  /// is running.
  fn send(
    &mut self,
    msg: Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    let msg_seq_num = msg.header().get(MsgSeqNum)?.ok_or_else(|| {
      Error::protocol_violation("Outbound message has no MsgSeqNum")
    })?;
    if msg_seq_num == 0 {
      return Err(Error::protocol_violation("MsgSeqNum must be at least 1"));
    }

    let is_logon = msg.msg_type() == "A";
    if is_logon {
      if self.logon_sent {
        return Err(Error::protocol_violation(
          "Logon sent outside the logon exchange",
        ));
      }
      self.check_logon(&msg)?;
    } else if !self.logon_sent {
      return Err(Error::protocol_violation(format!(
        "The first message must be a Logon, not {}",
        msg.msg_type()
      )));
    } else if !self.peer_logon_received {
      return Err(Error::protocol_violation(format!(
        "Cannot send {} before the peer has logged on",
        msg.msg_type()
      )));
    }

    if msg_seq_num <= self.highest_accepted {
      return Err(Error::protocol_violation(format!(
        "MsgSeqNum {msg_seq_num} is not beyond {}, already sent",
        self.highest_accepted
      )));
    }
    if msg_seq_num > self.highest_accepted + 1 {
      debug!(
        "MsgSeqNum jumps from {} to {msg_seq_num}; the peer will ask for the gap",
        self.highest_accepted
      );
    }
    self.highest_accepted = msg_seq_num;
    // An application message is evidence of liveness on the wire, so it defers
    // the next heartbeat. The session's own traffic does not: a heartbeat
    // answering the peer's TestRequest is not us asserting liveness on our own
    // schedule, and letting it reset the timer would let a peer polling us
    // suppress our heartbeats. The scheduled heartbeat reset the timer when it
    // was asked for.
    if !msg.is_admin() {
      self.timers.reset_out(now);
    }

    if let Some(replay) = self.replay.as_mut() {
      replay.defer(msg);
      return Ok(Progress::Continue);
    }
    self.dispatch_outbound(msg, out)
  }

  /// The application's Logon must still describe the session that was
  /// configured: it may add to it, but not contradict it.
  fn check_logon(&self, logon: &Message) -> Result<()> {
    let heartbeat = logon.body().get(HeartBtInt)?;
    let expected = self.config.heartbeat_interval.as_secs();
    if heartbeat.and_then(|h| u64::try_from(h).ok()) != Some(expected) {
      return Err(Error::protocol_violation(format!(
        "Logon HeartBtInt {heartbeat:?} does not match the configured {expected}"
      )));
    }
    if logon.begin_string() != self.session_id.begin_string {
      return Err(Error::protocol_violation(format!(
        "Logon BeginString {} does not match the session's {}",
        logon.begin_string(),
        self.session_id.begin_string
      )));
    }
    Ok(())
  }

  /// Put a newly sent message on the wire, and note what sending it means.
  fn dispatch_outbound(
    &mut self,
    msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    let msg = self.transmit(msg, out)?;
    match msg.msg_type() {
      "A" => {
        self.logon_sent = true;
        if self.peer_logon_received {
          out.event(Event::LoggedOn)?;
        }
      }
      "0" => self.heartbeat_requested = false,
      "5" => return Ok(Progress::Close),
      _ => {}
    }
    Ok(Progress::Continue)
  }

  /// Put the session header fields onto `msg` and hand it to `out`.
  ///
  /// A `SendingTime` the message already carries is kept. Otherwise a slot is
  /// reserved for the output to fill: one clock read per message, taken as
  /// close to the wire as the sans-io boundary allows.
  fn transmit(
    &mut self,
    mut msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<Message> {
    let msg_seq_num = msg.header().req(MsgSeqNum)?;
    let precision = if msg.header().has(SendingTime) {
      None
    } else {
      Some(self.config.time_precision)
    };
    {
      let mut header = msg.header_mut();
      header
        .set(SenderCompID, self.session_id.sender_comp_id.as_str())
        .set(TargetCompID, self.session_id.target_comp_id.as_str());
      if let Some(precision) = precision {
        header
          .set_raw(SendingTime, &SENDING_TIME_PLACEHOLDER[..precision.width()]);
      }
    }
    self.highest_sent = self.highest_sent.max(msg_seq_num);

    out.transmit(Unstamped::new(&mut msg, precision))?;
    debug!("Sent message: {msg}");
    out.event(Event::RawMessageSent(&msg))?;
    Ok(msg)
  }

  /// Skip over `begin_seq_no..=end_seq_no` with a single SequenceReset-GapFill.
  ///
  /// Both bounds are inclusive and name messages that will *not* be
  /// retransmitted. `NewSeqNo` is therefore `end_seq_no + 1`: the sequence
  /// number of the next message the peer should expect, which must be the one
  /// transmitted immediately after this gap fill.
  fn send_gap_fill(
    &mut self,
    begin_seq_no: u64,
    end_seq_no: u64,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let mut gap_fill = self.message(msg_type::SequenceReset);
    gap_fill.header_mut().set(MsgSeqNum, begin_seq_no);
    gap_fill
      .body_mut()
      .set(GapFillFlag, true)
      .set(NewSeqNo, end_seq_no + 1);
    self.transmit(gap_fill, out).map(|_| ())
  }

  // ---------------------------------------------------------------------
  // Replay
  // ---------------------------------------------------------------------

  fn replay_message(
    &mut self,
    mut message: Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let msg_seq_num = message.header().req(MsgSeqNum)?;
    // Session messages are gap-filled rather than resent, except Reject and
    // XMLnonFIX, "the only session messages which may be retransmitted"
    // (FIX Session Layer §4.8.5).
    let gap_filled =
      message.is_admin() && !matches!(message.msg_type(), "3" | "n");
    let replay = self
      .replay
      .as_mut()
      .ok_or_else(|| Error::protocol_violation("No replay in progress"))?;

    match replay.offer(msg_seq_num, gap_filled) {
      ReplayStep::Skip | ReplayStep::Absorb => Ok(()),
      ReplayStep::Retransmit { gap_fill } => {
        // The peer needs to know when the message was *originally* sent. An
        // OrigSendingTime the application supplied wins; otherwise it is the
        // stored SendingTime. A fresh SendingTime is then stamped.
        let mut header = message.header_mut();
        if !header.as_block().has(OrigSendingTime) {
          header.copy_value(SendingTime, OrigSendingTime).map_err(|_| {
            Error::protocol_violation(format!(
              "Replayed message {msg_seq_num} has neither OrigSendingTime nor \
               SendingTime"
            ))
          })?;
        }
        header.remove(SendingTime).set(PossDupFlag, true);

        if let Some((begin, end)) = gap_fill {
          self.send_gap_fill(begin, end, out)?;
        }
        self.transmit(message, out).map(|_| ())
      }
    }
  }

  fn complete_replay(
    &mut self,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    let replay = self
      .replay
      .as_mut()
      .ok_or_else(|| Error::protocol_violation("No replay in progress"))?;

    let queue = replay.take_queue();
    let trailing = replay.trailing_gap_fill();

    if let Some((begin, end)) = trailing {
      self.send_gap_fill(begin, end, out)?;
    }
    self.replay = None;

    for msg in queue {
      if self.dispatch_outbound(msg, out)?.is_close() {
        return Ok(Progress::Close);
      }
    }

    // Re-synchronise: the peer's answer to this tells us the replay landed.
    if self.closing.is_none() {
      self.request_sync_test_request(out)?;
    }
    Ok(Progress::Continue)
  }

  // ---------------------------------------------------------------------
  // Inbound protocol handling
  // ---------------------------------------------------------------------

  /// Read `NewSeqNo` from an inbound SequenceReset(35=4).
  ///
  /// Only the gap fill form is supported. A SequenceReset-Reset — GapFillFlag
  /// of "N", or absent, which the specification defines as the default — asks
  /// the peer to accept a new sequence number without regard to the message's
  /// own, and is rejected.
  fn gap_fill_new_seq_num(msg: &Message) -> Result<u64> {
    if msg.body().get(GapFillFlag)? != Some(true) {
      return Err(Error::protocol_violation(
        "Sequence reset message is garbage and not supported",
      ));
    }
    Ok(msg.body().req(NewSeqNo)?)
  }

  fn handle_session_message(
    &mut self,
    msg: Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    let msg_seq_num = msg.header().req(MsgSeqNum)?;

    // A SequenceReset-GapFill stands in for the messages it skips over, so it
    // occupies a slot in the stream and is subject to the same sequence checks
    // as any other message. Accepting one differs only in where the expected
    // inbound sequence number lands: at NewSeqNo rather than one further on.
    let gap_fill_new_seq_num = if msg.msg_type() == "4" {
      Some(Self::gap_fill_new_seq_num(&msg)?)
    } else {
      None
    };

    match msg_seq_num.cmp(&self.next_in_seq_num) {
      std::cmp::Ordering::Equal => {
        if let Some(new_seq_num) = gap_fill_new_seq_num {
          if new_seq_num <= msg_seq_num {
            return Err(Error::protocol_violation(format!(
              "Invalid NewSeqNo in GapFill; expected greater than {} but got {}",
              msg_seq_num, new_seq_num
            )));
          }
          info!(
            "GapFill received, advancing next_in_seq_num to {}",
            new_seq_num
          );
          self.next_in_seq_num = new_seq_num;
        } else {
          self.next_in_seq_num += 1;
        }
      }
      std::cmp::Ordering::Greater => {
        // A gap. Request everything from the first missing message onwards
        // with an open-ended EndSeqNo of 0, and discard this message and every
        // subsequent one until the gap closes: they all fall inside the range
        // just requested, so the peer will retransmit them in order. This is
        // the approach the session layer specification recommends, and it
        // avoids holding an unbounded queue of out-of-order messages.
        if self.rerequest_in_progress.is_none() {
          let mut rr = self.message(msg_type::ResendRequest);
          rr.body_mut()
            .set(BeginSeqNo, self.next_in_seq_num)
            .set(EndSeqNo, 0u64);
          self.rerequest_in_progress = Some(msg_seq_num);
          self.request(rr, out)?;
        }

        // ResendRequest and Logout are the exceptions to discarding. Neither
        // is ever retransmitted — the peer gap fills over them instead — so
        // discarding one loses it for good, and this is the only opportunity
        // to act on it. Neither consumes the sequence number: the gap fill
        // that eventually covers it will.
        return match msg.msg_type() {
          // Service the retransmission the peer asked for. Our own request for
          // the messages we are missing has already been asked for above.
          "2" => self.dispatch_message(msg, false, now, out),
          // The messages still missing precede the Logout, so acknowledging it
          // now would abandon them. Recover first, acknowledge afterwards.
          "5" => {
            info!("Logout received inside a gap; deferring acknowledgement");
            self.peer_logout_pending = true;
            Ok(Progress::Continue)
          }
          _ => Ok(Progress::Continue),
        };
      }
      std::cmp::Ordering::Less => {
        // One of the two peers has lost session state and the connection is no
        // longer recoverable.
        self.request_logout(
          &format!(
            "Invalid MsgSeqNum; too low. Expected {} but got {}.",
            self.next_in_seq_num, msg_seq_num
          ),
          now,
          out,
        )?;
        return Ok(Progress::Continue);
      }
    }

    if self
      .rerequest_in_progress
      .is_some_and(|replay_seq| self.next_in_seq_num >= replay_seq)
    {
      self.rerequest_in_progress = None;
    }

    // TODO: Validate message matches session_id
    // The dispatch result decides whether the session continues.
    if self.dispatch_message(msg, true, now, out)?.is_close() {
      return Ok(Progress::Close);
    }

    // The gap that deferred a Logout acknowledgement has now closed.
    if self.peer_logout_pending && self.rerequest_in_progress.is_none() {
      self.request_logout(
        "Logout message received. Closing session.",
        now,
        out,
      )?;
    }

    Ok(Progress::Continue)
  }

  /// Act on a message. `in_sequence` is false for the ResendRequest serviced
  /// from inside a gap, which does not consume its sequence number.
  fn dispatch_message(
    &mut self,
    msg: Message,
    in_sequence: bool,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    match msg.msg_type() {
      "A" => {
        // we already mostly handled this
      }
      // Heartbeat
      "0" => {
        if let Some(test_req_id) = msg.body().get(TestReqID)?
          && let Some(recovery_tr_id) = &self.recovery_tr_id
          && test_req_id == recovery_tr_id.as_str()
        {
          debug!(
            "Received heartbeat for recovery test request, session is now established"
          );
          self.recovery_tr_id = None;
          out.event(Event::RecoveryCompleted)?;
        }
      }
      // TestRequest
      "1" => {
        let mut heartbeat = self.message(msg_type::Heartbeat);
        heartbeat
          .body_mut()
          .set(TestReqID, msg.body().req(TestReqID)?);
        self.request(heartbeat, out)?;
      }
      // ResendRequest
      "2" => {
        let begin_seq_no = msg.body().req(BeginSeqNo)?;
        let end_seq_no = msg.body().req(EndSeqNo)?;
        if self.replay.is_some() {
          return Err(Error::protocol_violation(
            "ResendRequest while a resend is already in progress",
          ));
        }
        let replay =
          Replay::start(begin_seq_no, end_seq_no, self.highest_sent + 1)?;
        let (begin_seq_no, end_seq_no) =
          (replay.begin_seq_no, replay.end_seq_no);
        // Nothing queued behind the replay may reuse a number inside it.
        self.highest_accepted = self.highest_accepted.max(end_seq_no);
        self.replay = Some(replay);
        out.event(Event::ResendRequest {
          resend_request: &msg,
          begin_seq_no,
          end_seq_no,
        })?;
      }
      // Logout
      "5" => {
        self.request_logout(
          "Logout message received. Closing session.",
          now,
          out,
        )?;
      }
      // Reject: about a message of ours, so the application needs to see it.
      "3" => {
        out.event(Event::MessageReceived {
          seq_num: msg.header().req(MsgSeqNum)?,
          msg: &msg,
        })?;
        return Ok(Progress::Continue);
      }
      _ if msg.is_admin() => {
        // Nothing to do for other admin messages.
      }
      &_ => {
        out.event(Event::MessageReceived {
          seq_num: msg.header().req(MsgSeqNum)?,
          msg: &msg,
        })?;
        return Ok(Progress::Continue);
      }
    }

    if in_sequence {
      out.event(Event::InboundAdvanced {
        next_in_seq_num: self.next_in_seq_num,
      })?;
    }
    Ok(Progress::Continue)
  }
}
