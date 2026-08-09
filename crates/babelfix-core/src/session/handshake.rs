//! The logon exchange, as a state machine — one type per role.
//!
//! The two roles do not behave the same, and the difference is protocol rather
//! than transport:
//!
//! * An **initiator** was told who it is talking to, and holds the sequence
//!   numbers already. It opens with its own Logon and waits for the answer.
//! * An **acceptor** is multiplexing sessions over one listening port, so it
//!   has no idea which session a connection belongs to until the peer's Logon
//!   names one. Nothing session-scoped can be said before that: the identity
//!   comes out of the Logon, the application is then asked for the sequence
//!   numbers it persisted for that identity, and only then can a reply go out.
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
//! # use babelfix_core::session::{AcceptorHandshake, SessionOutput, Session};
//! # fn accept(
//! #   out: &mut impl SessionOutput,
//! #   frame: babelfix_core::FixMessage,
//! #   lookup: impl Fn(&babelfix_core::session::SessionIdentifier) -> Session,
//! # ) -> babelfix_core::Result<()> {
//! let mut hs = AcceptorHandshake::new(Duration::from_secs(30), Instant::now());
//!
//! // No output here: nothing can be said about a session that has no name yet.
//! let session_id = hs.identify(frame)?;
//! let session = lookup(session_id);          // however you persist them
//!
//! // From here there is a session, so there is somewhere to put events.
//! let established = hs.accept(session, Instant::now(), out)?;
//! let _ = established;
//! # Ok(())
//! # }
//! ```

use std::time::{Duration, Instant};

use tracing::debug;

use super::{
  Event, Progress, Session, SessionIdentifier, SessionOutput, SessionState,
};
use crate::message::{FixMessage, builder};
use crate::repository::{FieldBlock, FixVersion};
use crate::schema::FIX_Latest::Fields;
use crate::{Error, Result};

/// A completed logon exchange: the session, and whether it survived it.
///
/// `progress` is [`Progress::Close`] when the session ended during the exchange
/// — a Logon whose sequence number is too low, say, which is answered with a
/// Logout and terminated. The state comes back even then, because the
/// application still has to be given its handle: the events explaining *why* it
/// ended have already been emitted, and dropping the handle would strand them.
#[must_use]
#[derive(Debug)]
pub struct Established {
  pub state: Box<SessionState>,
  pub progress: Progress,
}

/// The peer's Logon, in both the forms still needed: parsed, to feed the
/// session, and as it arrived, to report as `RawMessageReceived`.
#[derive(Debug)]
struct PendingLogon {
  session_id: SessionIdentifier,
  logon: builder::Message,
  raw: FixMessage,
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// The logon exchange from the side that opened the connection.
#[derive(Debug)]
pub struct InitiatorHandshake {
  /// Exists from the start: the identity was never in doubt, and sending our
  /// Logon consumed an outbound sequence number.
  state: Box<SessionState>,
  deadline: Instant,
}

impl InitiatorHandshake {
  /// Announce the session and put our Logon on the wire.
  pub fn start(
    session_id: SessionIdentifier,
    session: Session,
    logon_timeout: Duration,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Self> {
    // The socket already exists and the identity is known, so the session can
    // be announced before anything is exchanged.
    out.event(Event::ConnectionEstablished)?;

    let logon = logon_message(&session)?;
    let mut state = SessionState::new(session_id, session, now);
    state.send_logon(logon, out)?;

    Ok(Self {
      state: Box::new(state),
      deadline: now + logon_timeout,
    })
  }

  /// When the peer's Logon must have arrived by.
  pub fn deadline(&self) -> Instant {
    self.deadline
  }

  /// Give up if the peer has taken too long.
  pub fn on_timeout(&self, now: Instant) -> Result<()> {
    expired(self.deadline, now)
  }

  /// The peer's Logon completes the exchange.
  pub fn on_peer_logon(
    mut self,
    msg: FixMessage,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Established> {
    let logon = builder::Message::from_message(&msg)?;
    expect_logon(&logon)?;

    out.event(Event::RawMessageReceived(&msg, self.state.session()))?;
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
  pub fn identify(&mut self, msg: FixMessage) -> Result<&SessionIdentifier> {
    if self.pending.is_some() {
      return Err(Error::protocol_violation(
        "peer sent a second message before the session was accepted",
      ));
    }

    let logon = builder::Message::from_message(&msg)?;
    // Checked before anything is derived from the message, so a peer opening
    // with garbage cannot cause an identity to be read out of it.
    expect_logon(&logon)?;

    let session_id = session_id_from_logon(&logon)?;
    debug!("Logon received from {session_id:?}");

    Ok(
      &self
        .pending
        .insert(Box::new(PendingLogon {
          session_id,
          logon,
          raw: msg,
        }))
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
  pub fn peer_logon(&self) -> Option<&builder::Message> {
    self.pending.as_ref().map(|p| &p.logon)
  }

  /// Supply the sequence numbers persisted for the identified session.
  pub fn accept(
    self,
    session: Session,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Established> {
    let Some(pending) = self.pending else {
      return Err(Error::protocol_violation(
        "accept called before the peer identified itself",
      ));
    };
    let PendingLogon {
      session_id,
      logon,
      raw,
    } = *pending;

    // Only now is there a session to attach anything to.
    out.event(Event::ConnectionEstablished)?;

    let mut state = SessionState::new(session_id, session, now);

    // The peer's Logon is reported before our reply goes out. An application
    // persisting from these events must see what arrived before what it
    // answered with, or a crash between the two leaves it believing it sent a
    // Logon in response to nothing.
    out.event(Event::RawMessageReceived(&raw, state.session()))?;

    let reply = logon_message(state.session())?;
    state.send_logon(reply, out)?;

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
pub fn logon_message(session: &Session) -> Result<builder::Message> {
  let fix_version: &std::sync::Arc<FixVersion> = &session.fix_version;
  let mut logon = builder::Message::new(fix_version.clone(), "A")?;

  logon.body.set_tag(
    Fields::HeartBtInt,
    session.heartbeat_interval.as_secs().to_string(),
  );
  if logon
    .fix_message
    .is_member(fix_version.as_ref(), Fields::DefaultApplVerID)
  {
    logon.body.set_tag(
      Fields::DefaultApplVerID,
      "10", // TODO: Default application version: FIXLatest
    );
  }
  logon.body.set_tag(
    Fields::EncryptMethod,
    "0", // No encryption
  );
  Ok(logon)
}

/// Derive our session identity from a peer's Logon.
///
/// The peer's `SenderCompID` is our `TargetCompID` and vice versa — the
/// identity is always expressed from the point of view of the side holding it.
pub fn session_id_from_logon(
  logon: &builder::Message,
) -> Result<SessionIdentifier> {
  Ok(SessionIdentifier {
    begin_string: tag(logon, Fields::BeginString, "BeginString")?,
    sender_comp_id: tag(logon, Fields::TargetCompID, "TargetCompID")?,
    target_comp_id: tag(logon, Fields::SenderCompID, "SenderCompID")?,
  })
}

/// Reject anything that is not a Logon(35=A).
pub fn expect_logon(msg: &builder::Message) -> Result<()> {
  if msg.fix_message.msg_type.as_str() != "A" {
    return Err(Error::protocol_violation(format!(
      "First message was not a logon, got: {}",
      msg.fix_message.msg_type
    )));
  }
  Ok(())
}

fn tag(msg: &builder::Message, tag: u32, name: &str) -> Result<String> {
  Ok(
    msg
      .header
      .tag(tag)
      .ok_or_else(|| {
        Error::protocol_violation(format!("Logon message missing {name}"))
      })?
      .as_string(),
  )
}
