//! The session state machine proper.
//!
//! Ported from the async `SessionManager` this replaces. Every `.await` in that
//! version was either a write to the socket sink or a send on the event
//! channel, so the protocol logic underneath was already synchronous; here it
//! writes into a [`SessionOutput`] instead.

use std::time::Instant;

use tracing::{debug, error, info};

use super::replay::{Replay, ReplayStep};
use super::{
  Command, Event, Progress, Session, SessionIdentifier, SessionOutput,
};
use crate::message::{FixMessage, builder};
use crate::schema::FIX_Latest::Fields;
use crate::{Error, Result};

/// `SendingTime` is stamped by the [`SessionOutput`], as late as it can be. The
/// state machine reserves the field so it occupies its proper place in the
/// header, and leaves the value for the output to fill in.
const SENDING_TIME_PLACEHOLDER: &str = "";

/// How many heartbeat intervals may pass without a word from the peer before
/// the session gives up on it.
const MISSED_HEARTBEATS_BEFORE_LOGOUT: u32 = 3;

/// Heartbeat deadlines, held as absolute instants.
///
/// The async version used a pair of `tokio::time::Interval`s. Those default to
/// `MissedTickBehavior::Burst`, so a session that was blocked past two
/// deadlines would get two ticks back to back and could jump straight to two
/// missed heartbeats. Deadline arithmetic cannot burst.
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
  rerequest_in_progress: Option<u32>,
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

  /// The current sequence numbers and settings.
  ///
  /// This replaces the old `GetSessionState` command: with the state machine no
  /// longer hidden inside a task, asking it what it thinks is a method call.
  /// That also removes a hazard by construction — polling the session used to
  /// travel the same path as commands that transmit, and had to be careful not
  /// to reset the outbound heartbeat timer on the way past.
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

  /// Process the peer's Logon and send the synchronisation TestRequest that
  /// establishes whether either side has missed anything.
  pub fn start(
    &mut self,
    logon: builder::Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    if self.handle_session_message(logon, now, out)?.is_close() {
      return Ok(Progress::Close);
    }

    let tr_id = self.next_test_request_id("HELO-");
    let mut test_request =
      builder::Message::new(self.session.fix_version.clone(), "1")?;
    test_request.body.set_tag(Fields::TestReqID, tr_id.clone());
    self.transmit(test_request, out)?;
    self.recovery_tr_id = Some(tr_id);

    Ok(Progress::Continue)
  }

  /// Feed a decoded inbound frame.
  pub fn on_message(
    &mut self,
    fix_message: FixMessage,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    // Liveness is evidenced by the frame arriving at all, so this happens
    // before any validation: a message rejected for a bad sequence number still
    // proves the peer is there.
    self.timers.reset_in(now);

    let msg = builder::Message::from_message(&fix_message)?;
    out.event(Event::RawMessageReceived(&fix_message, &self.session))?;
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
        let mut logout =
          builder::Message::new(self.session.fix_version.clone(), "5")?;
        logout
          .body
          .set_tag(Fields::Text, "Disconnect requested by application");
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
      let heartbeat =
        builder::Message::new(self.session.fix_version.clone(), "0")?;
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
          let mut test_request =
            builder::Message::new(self.session.fix_version.clone(), "1")?;
          test_request.body.set_tag(Fields::TestReqID, tr_id);
          self.send(test_request, out)?;
        }
        MISSED_HEARTBEATS_BEFORE_LOGOUT.. => {
          error!("Missed third heartbeat, logging out");
          let mut logout =
            builder::Message::new(self.session.fix_version.clone(), "5")?;
          logout.body.set_tag(Fields::Text, "Heartbeat timeout");
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
  /// This was `Session::send`. The `SendingTime` is deliberately left empty for
  /// the output to fill: one clock read per message, taken as close to the wire
  /// as the sans-io boundary allows.
  fn transmit(
    &mut self,
    mut msg: builder::Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    // Only set the seqnum if it's not a gap fill
    if !msg.header.has_tag(Fields::MsgSeqNum) {
      msg
        .header
        .set_tag(Fields::MsgSeqNum, self.session.next_out_seq_num);
      self.session.next_out_seq_num += 1;
    }

    msg
      .header
      .set_tag(Fields::SenderCompID, self.session_id.sender_comp_id.clone());
    msg
      .header
      .set_tag(Fields::TargetCompID, self.session_id.target_comp_id.clone());
    msg
      .header
      .set_tag(Fields::SendingTime, SENDING_TIME_PLACEHOLDER);

    let mut msg = msg.as_message()?;
    debug!("Sending message: {:?}", msg);

    // The snapshot handed out alongside the message is taken here, after this
    // message's sequence number has been consumed and before the next one is.
    // A single call can emit several messages; labelling them from the state at
    // the end would give them all the last sequence number.
    out.transmit(&mut msg, &self.session)?;
    out.event(Event::RawMessageSent(&msg, &self.session))
  }

  /// Send an application or admin message, deferring it if a replay is running.
  fn send(
    &mut self,
    mut msg: builder::Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    if let Some(replay) = self.replay.as_mut() {
      replay.defer(msg);
      return Ok(());
    }

    msg.header.remove_tag(Fields::MsgSeqNum);
    msg.header.remove_tag(Fields::PossDupFlag);

    debug!("Sending message to session: {:?}", msg);
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
    begin_seq_no: u32,
    end_seq_no: u32,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let mut gap_fill =
      builder::Message::new(self.session.fix_version.clone(), "4")?;
    gap_fill.body.set_tag(Fields::GapFillFlag, "Y");
    gap_fill.header.set_tag(Fields::MsgSeqNum, begin_seq_no);
    gap_fill.body.set_tag(Fields::NewSeqNo, end_seq_no + 1);
    self.transmit(gap_fill, out)
  }

  /// Send a Logout(35=5) carrying a diagnostic reason.
  fn send_logout(
    &mut self,
    text: impl Into<builder::TypedValue>,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let mut logout =
      builder::Message::new(self.session.fix_version.clone(), "5")?;
    logout.body.set_tag(Fields::Text, text);
    self.transmit(logout, out)
  }

  // ---------------------------------------------------------------------
  // Replay
  // ---------------------------------------------------------------------

  fn replay_message(
    &mut self,
    mut message: builder::Message,
    out: &mut impl SessionOutput,
  ) -> Result<()> {
    let msg_seq_num = message
      .header
      .tag(Fields::MsgSeqNum)
      .ok_or_else(|| {
        Error::protocol_violation("No MsgSeqNum in replay message")
      })?
      .as_int()
      .ok_or_else(|| Error::protocol_violation("MsgSeqNum is not an integer"))?
      as u32;

    let is_admin = message.is_admin_message();
    let replay = self
      .replay
      .as_mut()
      .ok_or_else(|| Error::protocol_violation("No replay in progress"))?;

    match replay.offer(msg_seq_num, is_admin) {
      ReplayStep::Skip | ReplayStep::Absorb => Ok(()),
      ReplayStep::Retransmit { gap_fill } => {
        if let Some((begin, end)) = gap_fill {
          self.send_gap_fill(begin, end, out)?;
        }

        // The peer needs to know when the message was *originally* sent, so the
        // stored SendingTime is preserved as OrigSendingTime before the output
        // stamps a fresh one.
        message.header.set_tag(
          Fields::OrigSendingTime,
          message
            .header
            .tag(Fields::SendingTime)
            .ok_or_else(|| Error::protocol_violation("Missing SendingTime"))?
            .clone(),
        );
        message.header.set_tag(Fields::PossDupFlag, "Y");
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
    let mut test_request =
      builder::Message::new(self.session.fix_version.clone(), "1")?;
    test_request.body.set_tag(Fields::TestReqID, tr_id.clone());
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
  fn gap_fill_new_seq_num(msg: &builder::Message) -> Result<u32> {
    if msg
      .body
      .tag(Fields::GapFillFlag)
      .ok_or_else(|| Error::protocol_violation("Missing GapFillFlag"))?
      .as_string()
      != "Y"
    {
      return Err(Error::protocol_violation(
        "Sequence reset message is garbage and not supported",
      ));
    }

    Ok(
      msg
        .body
        .tag(Fields::NewSeqNo)
        .ok_or_else(|| Error::protocol_violation("Missing NewSeqNo"))?
        .as_int()
        .ok_or_else(|| Error::protocol_violation("Expected integer"))?
        as u32,
    )
  }

  fn handle_session_message(
    &mut self,
    msg: builder::Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    let msg_seq_num = msg
      .header
      .tag(Fields::MsgSeqNum)
      .ok_or_else(|| Error::protocol_violation("Missing MsgSeqNum"))?
      .as_int()
      .ok_or_else(|| Error::protocol_violation("MsgSeqNum is not an integer"))?
      as u32;

    // A SequenceReset-GapFill stands in for the messages it skips over, so it
    // occupies a slot in the stream and is subject to the same sequence checks
    // as any other message. Accepting one differs only in where the expected
    // inbound sequence number lands: at NewSeqNo rather than one further on.
    let gap_fill_new_seq_num = if msg.fix_message.msg_type == "4" {
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
          let mut rr = builder::Message::new(msg.fix_version.clone(), "2")?;
          rr.body
            .set_tag(Fields::BeginSeqNo, self.session.next_in_seq_num);
          rr.body.set_tag(Fields::EndSeqNo, 0);
          self.rerequest_in_progress = Some(msg_seq_num);
          self.transmit(rr, out)?;
        }

        // ResendRequest and Logout are the exceptions to discarding. Neither
        // is ever retransmitted — the peer gap fills over them instead — so
        // discarding one loses it for good, and this is the only opportunity
        // to act on it. Neither consumes the sequence number: the gap fill
        // that eventually covers it will.
        return match msg.fix_message.msg_type.as_str() {
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
          format!(
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
    msg: builder::Message,
    _now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    out.event(Event::SessionState(&self.session))?;

    match msg.fix_message.msg_type.as_str() {
      "A" => {
        // we already mostly handled this
      }
      // Heartbeat
      "0" => {
        if let Some(test_req_id) =
          msg.body.tag(Fields::TestReqID).map(|v| v.as_string())
        {
          if let Some(recovery_tr_id) = &self.recovery_tr_id {
            if &test_req_id == recovery_tr_id {
              debug!(
                "Received heartbeat for recovery test request, session is now established"
              );
              self.recovery_tr_id = None;
              out.event(Event::RecoveryCompleted)?;
            }
          }
        }
      }
      // TestRequest
      "1" => {
        let mut heartbeat =
          builder::Message::new(msg.fix_version.clone(), "0")?;
        heartbeat.body.set_tag(
          Fields::TestReqID,
          msg
            .body
            .tag(Fields::TestReqID)
            .ok_or_else(|| Error::protocol_violation("Missing TestReqID"))?
            .as_string(),
        );
        self.send(heartbeat, out)?;
      }
      // ResendRequest
      "2" => {
        let begin_seq_no = msg
          .body
          .tag(Fields::BeginSeqNo)
          .ok_or_else(|| Error::protocol_violation("Missing BeginSeqNo"))?
          .as_int()
          .ok_or_else(|| Error::protocol_violation("Expected integer"))?
          as u32;
        let end_seq_no = msg
          .body
          .tag(Fields::EndSeqNo)
          .ok_or_else(|| Error::protocol_violation("Missing EndSeqNo"))?
          .as_int()
          .ok_or_else(|| Error::protocol_violation("Expected integer"))?
          as u32;
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
      _ if msg.is_admin_message() => {
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
