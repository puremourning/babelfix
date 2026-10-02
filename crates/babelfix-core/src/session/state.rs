//! The session state machine proper. It performs no I/O: it writes messages
//! and events into a [`SessionOutput`].

use std::time::Instant;

use tracing::{debug, error, info};

use super::fields::*;
use super::replay::{Replay, ReplayStep};
use super::{
  Command, Event, Progress, Session, SessionIdentifier, SessionOutput,
  Unstamped,
};
use crate::message::Message;
use crate::time::MAX_LEN;
use crate::{Error, Result};

/// `SendingTime` is stamped by the [`SessionOutput`], as late as it can be. The
/// state machine reserves the field — a placeholder of exactly the stamp's
/// width — so the stamp is an in-place write.
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

/// The FIX session state machine: sequence numbers, heartbeats, recovery.
///
/// See the [module docs](super) for how a driver is expected to call this.
pub struct SessionState {
  session_id: SessionIdentifier,
  session: Session,

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
      .field("session", &self.session)
      .field("rerequest_in_progress", &self.rerequest_in_progress)
      .field("recovery_tr_id", &self.recovery_tr_id)
      .field("replay", &self.replay)
      .field("peer_logout_pending", &self.peer_logout_pending)
      .field("timers", &self.timers)
      .finish()
  }
}

impl SessionState {
  /// Build a state machine for a session whose logon exchange has completed.
  ///
  /// `now` starts the heartbeat clocks. Feed the peer's Logon to [`start`] next.
  ///
  /// [`start`]: SessionState::start
  pub fn new(
    session_id: SessionIdentifier,
    session: Session,
    now: Instant,
  ) -> Self {
    let timers = Timers::new(session.heartbeat_interval, now);
    Self {
      session_id,
      session,
      rerequest_in_progress: None,
      recovery_tr_id: None,
      replay: None,
      peer_logout_pending: false,
      timers,
      test_request_seq: 0,
    }
  }

  /// The current sequence numbers and settings. Reading them puts nothing on
  /// the wire and does not touch the heartbeat timers.
  pub fn session(&self) -> &Session {
    &self.session
  }

  pub fn session_id(&self) -> &SessionIdentifier {
    &self.session_id
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
    Some(self.timers.next_out.min(self.timers.next_in))
  }

  /// Transmit a message belonging to the logon exchange, with the session's
  /// sequence number and header fields applied.
  ///
  /// The [`AcceptorHandshake`](super::AcceptorHandshake) and
  /// [`InitiatorHandshake`](super::InitiatorHandshake) use this to send their
  /// Logon; a hand-rolled handshake can too.
  pub fn send_logon(
    &mut self,
    msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    self.transmit(msg, out)
  }

  /// Process the peer's Logon and send the synchronisation TestRequest that
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

    let tr_id = self.next_test_request_id("HELO-");
    let mut test_request = self.message(msg_type::TestRequest);
    test_request.body_mut().set(TestReqID, tr_id.as_str());
    self.transmit(test_request, out)?;
    self.recovery_tr_id = Some(tr_id);

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

    out.event(Event::RawMessageReceived(&msg, &self.session))?;
    self.handle_session_message(msg, now, out)
  }

  /// Feed an application command.
  pub fn on_command(
    &mut self,
    cmd: Command,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    // Each of these puts a message on the wire, and that message is itself
    // evidence of liveness, so the outbound heartbeat is deferred. Note this
    // happens *only* here: heartbeats answering a TestRequest, ResendRequests
    // raised on gap detection, gap fills, replay traffic and Logouts all leave
    // the timer alone, exactly as they did before.
    self.timers.reset_out(now);

    match cmd {
      Command::Send(msg) => {
        self.send(msg, out)?;
        Ok(Progress::Continue)
      }
      Command::Replay(msg) => {
        self.replay_message(msg, out)?;
        Ok(Progress::Continue)
      }
      Command::ReplayComplete => {
        self.complete_replay(out)?;
        Ok(Progress::Continue)
      }
      Command::Disconnect => {
        info!("Session disconnect requested");
        // Always announce the intent to disconnect, so the peer can tell an
        // orderly shutdown from a network failure.
        let mut logout = self.message(msg_type::Logout);
        logout
          .body_mut()
          .set(Text, "Disconnect requested by application");
        self.send(logout, out)?;
        Ok(Progress::Close)
      }
    }
  }

  /// Advance time. Call when [`next_deadline`](Self::next_deadline) has passed.
  pub fn on_timeout(
    &mut self,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    if now >= self.timers.next_out {
      self.timers.reset_out(now);
      let heartbeat = self.message(msg_type::Heartbeat);
      self.send(heartbeat, out)?;
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
          self.send(test_request, out)?;
        }
        MISSED_HEARTBEATS_BEFORE_LOGOUT.. => {
          error!("Missed third heartbeat, logging out");
          let mut logout = self.message(msg_type::Logout);
          logout.body_mut().set(Text, "Heartbeat timeout");
          self.send(logout, out)?;
          return Ok(Progress::Close);
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
    Message::new(&self.session.dict, msg_type)
  }

  /// A `TestReqID` unique within the session, without consulting a clock.
  fn next_test_request_id(&mut self, prefix: &str) -> String {
    self.test_request_seq += 1;
    format!("{prefix}{}", self.test_request_seq)
  }

  // ---------------------------------------------------------------------
  // Transmission
  // ---------------------------------------------------------------------

  /// Stamp the session header fields onto `msg` and hand it to `out`.
  ///
  /// The `SendingTime` is deliberately left for the output to fill: one clock
  /// read per message, taken as close to the wire as the sans-io boundary
  /// allows.
  fn transmit(
    &mut self,
    mut msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let precision = self.session.time_precision;
    {
      let mut header = msg.header_mut();
      // A gap fill and a replayed message carry their own sequence number.
      if !header.as_block().has(MsgSeqNum) {
        header.set(MsgSeqNum, self.session.next_out_seq_num);
        self.session.next_out_seq_num += 1;
      }
      header
        .set(SenderCompID, self.session_id.sender_comp_id.as_str())
        .set(TargetCompID, self.session_id.target_comp_id.as_str())
        .set_raw(SendingTime, &SENDING_TIME_PLACEHOLDER[..precision.width()]);
    }

    // The snapshot handed out alongside the message is taken here, after this
    // message's sequence number has been consumed and before the next one is.
    // A single call can emit several messages; labelling them from the state at
    // the end would give them all the last sequence number.
    out.transmit(Unstamped::new(&mut msg, precision), &self.session)?;
    debug!("Sent message: {msg}");
    out.event(Event::RawMessageSent(&msg, &self.session))
  }

  /// Send an application or admin message, deferring it if a replay is running.
  fn send(
    &mut self,
    mut msg: Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    if let Some(replay) = self.replay.as_mut() {
      replay.defer(msg);
      return Ok(());
    }

    msg.header_mut().remove(MsgSeqNum).remove(PossDupFlag);
    self.transmit(msg, out)
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
    self.transmit(gap_fill, out)
  }

  /// Send a Logout(35=5) carrying a diagnostic reason.
  fn send_logout(
    &mut self,
    text: &str,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let mut logout = self.message(msg_type::Logout);
    logout.body_mut().set(Text, text);
    self.transmit(logout, out)
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
        if let Some((begin, end)) = gap_fill {
          self.send_gap_fill(begin, end, out)?;
        }

        // The peer needs to know when the message was *originally* sent, so the
        // stored SendingTime is preserved as OrigSendingTime before the output
        // stamps a fresh one.
        message
          .header_mut()
          .copy_value(SendingTime, OrigSendingTime)?
          .set(PossDupFlag, true);
        self.transmit(message, out)
      }
    }
  }

  fn complete_replay(&mut self, out: &mut impl SessionOutput) -> Result<()> {
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
      self.send(msg, out)?;
    }

    // Re-synchronise: the peer's answer to this tells us the replay landed.
    let tr_id = self.next_test_request_id("HELO-");
    let mut test_request = self.message(msg_type::TestRequest);
    test_request.body_mut().set(TestReqID, tr_id.as_str());
    self.transmit(test_request, out)?;
    self.recovery_tr_id = Some(tr_id);

    Ok(())
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

    match msg_seq_num.cmp(&self.session.next_in_seq_num) {
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
          self.session.next_in_seq_num = new_seq_num;
        } else {
          self.session.next_in_seq_num += 1;
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
            .set(BeginSeqNo, self.session.next_in_seq_num)
            .set(EndSeqNo, 0u64);
          self.rerequest_in_progress = Some(msg_seq_num);
          self.transmit(rr, out)?;
        }

        // ResendRequest and Logout are the exceptions to discarding. Neither
        // is ever retransmitted — the peer gap fills over them instead — so
        // discarding one loses it for good, and this is the only opportunity
        // to act on it. Neither consumes the sequence number: the gap fill
        // that eventually covers it will.
        return match msg.msg_type() {
          // Service the retransmission the peer asked for. Our own request for
          // the messages we are missing has already gone out above.
          "2" => self.dispatch_message(msg, now, out),
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
        self.send_logout(
          &format!(
            "Invalid MsgSeqNum; too low. Expected {} but got {}.",
            self.session.next_in_seq_num, msg_seq_num
          ),
          out,
        )?;
        return Ok(Progress::Close);
      }
    }

    if self
      .rerequest_in_progress
      .is_some_and(|replay_seq| self.session.next_in_seq_num >= replay_seq)
    {
      self.rerequest_in_progress = None;
    }

    // TODO: Validate message matches session_id
    // The dispatch result decides whether the session continues: an inbound
    // Logout ends it once acknowledged.
    if self.dispatch_message(msg, now, out)?.is_close() {
      return Ok(Progress::Close);
    }

    // The gap that deferred a Logout acknowledgement has now closed.
    if self.peer_logout_pending && self.rerequest_in_progress.is_none() {
      self.send_logout("Logout message received. Closing session.", out)?;
      return Ok(Progress::Close);
    }

    Ok(Progress::Continue)
  }

  fn dispatch_message(
    &mut self,
    msg: Message,
    _now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    out.event(Event::SessionState(&self.session))?;

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
        self.send(heartbeat, out)?;
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
        let replay = Replay::start(
          begin_seq_no,
          end_seq_no,
          self.session.next_out_seq_num,
        )?;
        let (begin_seq_no, end_seq_no) =
          (replay.begin_seq_no, replay.end_seq_no);
        self.replay = Some(replay);
        out.event(Event::ResendRequest {
          resend_request: &msg,
          begin_seq_no,
          end_seq_no,
        })?;
      }
      // Logout
      "5" => {
        self.send_logout("Logout message received. Closing session.", out)?;
        return Ok(Progress::Close);
      }
      _ if msg.is_admin() => {
        // Ignore other admin messages
        // FIXME: Not Reject and BusinessMessageReject!
      }
      &_ => {
        out.event(Event::MessageReceived(&msg))?;
      }
    }
    Ok(Progress::Continue)
  }
}
