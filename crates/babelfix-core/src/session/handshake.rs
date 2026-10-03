//! The logon exchange, as a state machine — one type per role.
//!
//! The two roles do not behave the same, and the difference is protocol rather
//! than transport:
//!
//! * An **initiator** was told who it is talking to. It opens by asking the
//!   application for its Logon, sends it, and waits for the answer.
//! * An **acceptor** is multiplexing sessions over one listening port, so it
//!   has no idea which session a connection belongs to until the peer's Logon
//!   names one. Nothing session-scoped can be said before that: the identity
//!   comes out of the Logon, the application is then asked for the settings
//!   and inbound sequence number for that identity, and only then can it be
//!   asked for a reply.
//!
//! That ordering is the same whether the bytes arrive from tokio, from `epoll`,
//! or from a test, so it lives here rather than in a driver.
//!
//! # Why two types
//!
//! [`AcceptorHandshake::identify`] takes no [`SessionOutput`], because at that
//! point there is no session for an event to be *about*. Making that a property
//! of the signature rather than a rule to remember is why the roles are separate
//! types: every caller already knows which end it is, so nothing has to branch
//! on a role, and nothing is handed an output it must not use.
//!
//! ```no_run
//! # use std::time::{Duration, Instant};
//! # use babelfix_core::session::{AcceptorHandshake, SessionOutput, SessionConfig};
//! # fn accept(
//! #   out: &mut impl SessionOutput,
//! #   frame: babelfix_core::message::Message,
//! #   lookup: impl Fn(&babelfix_core::session::SessionIdentifier) -> (SessionConfig, u64),
//! # ) -> babelfix_core::Result<()> {
//! let mut hs = AcceptorHandshake::new(Duration::from_secs(30), Instant::now());
//!
//! // No output here: nothing can be said about a session that has no name yet.
//! let session_id = hs.identify(frame)?;
//! let (config, next_in) = lookup(session_id);   // however you persist them
//!
//! // From here there is a session, so there is somewhere to put events — the
//! // first of which asks for our Logon reply.
//! let established = hs.accept(config, next_in, Instant::now(), out)?;
//! let _ = established;
//! # Ok(())
//! # }
//! ```

use std::time::{Duration, Instant};

use tracing::debug;

use super::fields::*;
use super::{
  Command, Event, Progress, SessionConfig, SessionIdentifier, SessionOutput,
  SessionState,
};
use crate::message::Message;
use crate::repository::FieldBlock;
use crate::{Error, Result};

/// A logon exchange the peer's side of which is complete: the session, and
/// whether it survived it.
///
/// For an acceptor our Logon reply has only been asked for, and goes out when
/// the application sends it through the state; anything else the exchange
/// asked for — a ResendRequest, the synchronisation TestRequest, or a Logout
/// for a Logon whose sequence number is too low — follows it.
///
/// `progress` is [`Progress::Close`] when the session ended during the
/// exchange. The state comes back even then, because the application still has
/// to be given its handle: the events explaining *why* it ended have already
/// been emitted, and dropping the handle would strand them.
#[must_use]
#[derive(Debug)]
pub struct Established {
  pub state: Box<SessionState>,
  pub progress: Progress,
}

/// The peer's Logon, held until the application accepts the session it
/// names.
#[derive(Debug)]
struct PendingLogon {
  session_id: SessionIdentifier,
  logon: Message,
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// The logon exchange from the side that opened the connection.
#[derive(Debug)]
pub struct InitiatorHandshake {
  /// Exists from the start: the identity was never in doubt, and our Logon is
  /// asked for straight away.
  state: Box<SessionState>,
  deadline: Instant,
}

impl InitiatorHandshake {
  /// Announce the session and ask the application for our Logon.
  ///
  /// The Logon goes out when the application sends it back, numbered, through
  /// [`on_command`](Self::on_command). `next_in_seq_num` is the `MsgSeqNum`
  /// expected on the peer's Logon.
  pub fn start(
    session_id: SessionIdentifier,
    config: SessionConfig,
    next_in_seq_num: u64,
    logon_timeout: Duration,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Self> {
    // The socket already exists and the identity is known, so the session can
    // be announced before anything is exchanged.
    out.event(Event::ConnectionEstablished)?;

    let mut state = SessionState::new(session_id, config, next_in_seq_num, now);
    state.request_logon(out)?;

    Ok(Self {
      state: Box::new(state),
      deadline: now + logon_timeout,
    })
  }

  /// The session being established.
  pub fn state(&self) -> &SessionState {
    &self.state
  }

  /// When the peer's Logon must have arrived by.
  pub fn deadline(&self) -> Instant {
    self.deadline
  }

  /// Give up if the peer has taken too long.
  pub fn on_timeout(&self, now: Instant) -> Result<()> {
    expired(self.deadline, now)
  }

  /// Feed an application command. Until the peer answers, the only one with
  /// anything to do is the [`Command::Send`] of our Logon; a
  /// [`Command::Disconnect`] simply ends the exchange.
  pub fn on_command(
    &mut self,
    cmd: Command,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Progress> {
    match cmd {
      Command::Disconnect => Ok(Progress::Close),
      cmd => self.state.on_command(cmd, now, out),
    }
  }

  /// The peer's Logon completes the exchange.
  pub fn on_peer_logon(
    mut self,
    logon: Message,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Established> {
    expect_logon(&logon)?;
    if self.state.highest_sent() == 0 {
      return Err(Error::protocol_violation(
        "peer sent its Logon before ours went out",
      ));
    }

    out.event(Event::RawMessageReceived(&logon))?;
    let progress = self.state.start(logon, now, out)?;
    Ok(Established {
      state: self.state,
      progress,
    })
  }
}

// ---------------------------------------------------------------------------
// Acceptor
// ---------------------------------------------------------------------------

/// The logon exchange from the side that answered the connection.
#[derive(Debug)]
pub struct AcceptorHandshake {
  deadline: Instant,
  /// Set once the peer has named a session.
  pending: Option<Box<PendingLogon>>,
}

impl AcceptorHandshake {
  /// Wait for the peer to introduce itself. Nothing is sent.
  pub fn new(logon_timeout: Duration, now: Instant) -> Self {
    Self {
      deadline: now + logon_timeout,
      pending: None,
    }
  }

  /// When the peer's Logon must have arrived by.
  pub fn deadline(&self) -> Instant {
    self.deadline
  }

  /// Give up if the peer has taken too long.
  pub fn on_timeout(&self, now: Instant) -> Result<()> {
    expired(self.deadline, now)
  }

  /// Read the peer's Logon and work out which session it names.
  ///
  /// There is deliberately no [`SessionOutput`] here. Until this returns there
  /// is no session, so there is nothing an event could be *about* — and a first
  /// frame that turns out not to be a Logon must be refused without the
  /// application having been told anything about it at all.
  pub fn identify(&mut self, logon: Message) -> Result<&SessionIdentifier> {
    if self.pending.is_some() {
      return Err(Error::protocol_violation(
        "peer sent a second message before the session was accepted",
      ));
    }

    // Checked before anything is derived from the message, so a peer opening
    // with garbage cannot cause an identity to be read out of it.
    expect_logon(&logon)?;

    let session_id = session_id_from_logon(&logon)?;
    debug!("Logon received from {session_id:?}");

    Ok(
      &self
        .pending
        .insert(Box::new(PendingLogon { session_id, logon }))
        .session_id,
    )
  }

  /// The session this connection named, once [`identify`](Self::identify) has
  /// run.
  pub fn session_id(&self) -> Option<&SessionIdentifier> {
    self.pending.as_ref().map(|p| &p.session_id)
  }

  /// The peer's Logon, for applications that authenticate on it —
  /// `Username`/`Password`, or whatever else the peer put in there. Available
  /// between [`identify`](Self::identify) and [`accept`](Self::accept).
  pub fn peer_logon(&self) -> Option<&Message> {
    self.pending.as_ref().map(|p| &p.logon)
  }

  /// Supply the settings for the identified session, and the `MsgSeqNum`
  /// expected on the peer's Logon.
  ///
  /// The application is asked for our Logon reply with
  /// [`Event::AdminSendRequired`]; the session is logged on once it has been
  /// sent, through the returned state.
  pub fn accept(
    self,
    config: SessionConfig,
    next_in_seq_num: u64,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Established> {
    let Some(pending) = self.pending else {
      return Err(Error::protocol_violation(
        "accept called before the peer identified itself",
      ));
    };
    let PendingLogon { session_id, logon } = *pending;

    // Only now is there a session to attach anything to.
    out.event(Event::ConnectionEstablished)?;

    let mut state = SessionState::new(session_id, config, next_in_seq_num, now);

    // The peer's Logon is reported before our reply is asked for. An
    // application persisting from these events must see what arrived before
    // what it answered with.
    out.event(Event::RawMessageReceived(&logon))?;

    // Our reply is asked for before the peer's Logon is processed, so it is
    // numbered ahead of anything processing it asks for: a ResendRequest for a
    // gap, the synchronisation TestRequest, or a Logout.
    state.request_logon(out)?;

    let progress = state.start(logon, now, out)?;
    Ok(Established {
      state: Box::new(state),
      progress,
    })
  }
}

fn expired(deadline: Instant, now: Instant) -> Result<()> {
  if now >= deadline {
    return Err(Error::connection_failed(
      "logon exchange did not complete in time",
    ));
  }
  Ok(())
}

// ---------------------------------------------------------------------------
// Shared protocol helpers
// ---------------------------------------------------------------------------

/// Build a Logon carrying the session's negotiated settings.
pub fn logon_message(session: &SessionConfig) -> Result<Message> {
  let fix = session.dict.version();
  let mut logon = Message::new(&session.dict, msg_type::Logon);
  let mut body = logon.body_mut();

  body.set(HeartBtInt, session.heartbeat_interval.as_secs());
  if fix
    .get_message("A")
    .is_some_and(|m| m.is_member(fix, DefaultApplVerID))
  {
    // TODO: Default application version: FIXLatest
    body.set_raw(DefaultApplVerID, b"10");
  }
  body.set_raw(EncryptMethod, b"0"); // None
  Ok(logon)
}

/// Derive our session identity from a peer's Logon.
///
/// The peer's `SenderCompID` is our `TargetCompID` and vice versa — the
/// identity is always expressed from the point of view of the side holding it.
pub fn session_id_from_logon(logon: &Message) -> Result<SessionIdentifier> {
  Ok(SessionIdentifier {
    begin_string: logon.begin_string().to_owned(),
    sender_comp_id: comp_id(logon, TargetCompID)?,
    target_comp_id: comp_id(logon, SenderCompID)?,
  })
}

/// Reject anything that is not a Logon(35=A).
pub fn expect_logon(msg: &Message) -> Result<()> {
  if msg.msg_type() != "A" {
    return Err(Error::protocol_violation(format!(
      "First message was not a logon, got: {}",
      msg.msg_type()
    )));
  }
  Ok(())
}

fn comp_id(
  msg: &Message,
  field: crate::message::Field<crate::message::datatypes::Str>,
) -> Result<String> {
  let value = msg.header().get(field)?.ok_or_else(|| {
    Error::protocol_violation(format!(
      "Logon message missing {}",
      msg.dict().field_name(field.tag()).unwrap_or("CompID")
    ))
  })?;
  Ok(value.to_str().into_owned())
}
