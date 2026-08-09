//! The logon exchange, as a state machine.
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
//! or from a test — so it lives here, and a driver's only job is to feed frames
//! in and answer [`Step::NeedsSession`] when asked.
//!
//! ```no_run
//! # use std::time::{Duration, Instant};
//! # use babelfix_core::session::{Handshake, Step, SessionOutput, Session};
//! # fn drive(
//! #   mut hs: Handshake,
//! #   out: &mut impl SessionOutput,
//! #   frame: babelfix_core::FixMessage,
//! #   lookup: impl Fn(&babelfix_core::session::SessionIdentifier) -> Session,
//! # ) -> babelfix_core::Result<()> {
//! let now = Instant::now();
//! let mut step = hs.on_message(frame, now, out)?;
//!
//! if let Step::NeedsSession(id) = &step {
//!   let session = lookup(id);              // however you persist them
//!   step = hs.accept_session(session, now, out)?;
//! }
//!
//! if let Step::Established { state, progress } = step {
//!   // hand `state` the rest of the stream, unless it is already over
//!   let _ = (state, progress);
//! }
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

/// Which end of the connection this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
  /// Opened the connection, and knows the session's identity already.
  Initiator,
  /// Answered the connection, and learns the identity from the peer's Logon.
  Acceptor,
}

/// What a driver should do next.
#[must_use]
#[derive(Debug)]
pub enum Step {
  /// Keep feeding frames.
  Continue,

  /// Acceptor only: the peer's Logon named this session. Look up whatever
  /// sequence numbers you persisted for it and answer with
  /// [`Handshake::accept_session`]. Refusing is a matter of dropping the
  /// connection.
  NeedsSession(SessionIdentifier),

  /// The logon exchange is complete. Feed everything after this to the
  /// [`SessionState`].
  Established {
    state: Box<SessionState>,
    /// `Close` when the session ended during the exchange itself — a Logon
    /// whose sequence number is too low, say, which is answered with a Logout
    /// and terminated.
    ///
    /// The state comes back even then, because the application still has to be
    /// given its handle: the events explaining *why* the session ended have
    /// already been emitted, and dropping the handle would strand them.
    progress: Progress,
  },
}

#[derive(Debug)]
enum Phase {
  /// Our Logon is on the wire; waiting for the peer's. The state exists
  /// already because sending that Logon consumed a sequence number.
  InitiatorAwaitingLogon { state: Box<SessionState> },
  /// Waiting for the peer to introduce itself.
  AcceptorAwaitingLogon,
  /// Identity known; waiting for the application to supply the session. Both
  /// forms of the peer's Logon are kept: the parsed one to feed the session,
  /// and the frame as it arrived to report as `RawMessageReceived`.
  AcceptorAwaitingSession(Box<PendingLogon>),
  /// [`Step::Established`] has been returned and the state moved out.
  Done,
}

/// The peer's Logon, in both the forms the handshake still needs: parsed, to
/// feed the session, and as it arrived, to report as `RawMessageReceived`.
#[derive(Debug)]
struct PendingLogon {
  session_id: SessionIdentifier,
  logon: builder::Message,
  raw: FixMessage,
}

/// The logon exchange. See the [module docs](self).
#[derive(Debug)]
pub struct Handshake {
  role: Role,
  /// When to give up waiting for the peer to complete the exchange.
  deadline: Instant,
  phase: Phase,
}

impl Handshake {
  /// Open a session: emit `ConnectionEstablished` and put our Logon on the
  /// wire.
  pub fn initiator(
    session_id: SessionIdentifier,
    session: Session,
    logon_timeout: Duration,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Self> {
    // The socket already exists, and the identity was never in doubt, so the
    // session can be announced before anything is exchanged.
    out.event(Event::ConnectionEstablished)?;

    let logon = logon_message(&session)?;
    let mut state = SessionState::new(session_id, session, now);
    state.send_logon(logon, out)?;

    Ok(Self {
      role: Role::Initiator,
      deadline: now + logon_timeout,
      phase: Phase::InitiatorAwaitingLogon {
        state: Box::new(state),
      },
    })
  }

  /// Answer a connection: say nothing until the peer identifies itself.
  pub fn acceptor(logon_timeout: Duration, now: Instant) -> Self {
    Self {
      role: Role::Acceptor,
      deadline: now + logon_timeout,
      phase: Phase::AcceptorAwaitingLogon,
    }
  }

  pub fn role(&self) -> Role {
    self.role
  }

  /// The peer's Logon, once it has arrived and before the session is accepted.
  ///
  /// Applications that authenticate — on `Username`/`Password`, or on anything
  /// else the peer put in its Logon — inspect it here, between
  /// [`Step::NeedsSession`] and [`accept_session`](Self::accept_session).
  pub fn peer_logon(&self) -> Option<&builder::Message> {
    match &self.phase {
      Phase::AcceptorAwaitingSession(pending) => Some(&pending.logon),
      _ => None,
    }
  }

  /// When the exchange must have completed by. `None` once it has.
  pub fn next_deadline(&self) -> Option<Instant> {
    match self.phase {
      Phase::Done => None,
      _ => Some(self.deadline),
    }
  }

  /// Give up if the peer has taken too long.
  pub fn on_timeout(&mut self, now: Instant) -> Result<Step> {
    if now >= self.deadline {
      return Err(Error::connection_failed(
        "logon exchange did not complete in time",
      ));
    }
    Ok(Step::Continue)
  }

  /// Feed a decoded frame.
  pub fn on_message(
    &mut self,
    msg: FixMessage,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Step> {
    let logon = builder::Message::from_message(&msg)?;

    // Nothing but a Logon may open a session. This is checked before the
    // message is looked at in any other way, so a peer that opens with garbage
    // cannot cause an identity to be derived from it, or the application to be
    // asked about a session that will never exist.
    expect_logon(&logon)?;

    match std::mem::replace(&mut self.phase, Phase::Done) {
      Phase::InitiatorAwaitingLogon { mut state } => {
        out.event(Event::RawMessageReceived(&msg, state.session()))?;
        let progress = state.start(logon, now, out)?;
        Ok(Step::Established { state, progress })
      }

      Phase::AcceptorAwaitingLogon => {
        let session_id = session_id_from_logon(&logon)?;
        debug!("Logon received from {session_id:?}");
        self.phase = Phase::AcceptorAwaitingSession(Box::new(PendingLogon {
          session_id: session_id.clone(),
          logon,
          raw: msg,
        }));
        Ok(Step::NeedsSession(session_id))
      }

      other @ Phase::AcceptorAwaitingSession(_) => {
        // Put it back; the caller owes us a session first.
        self.phase = other;
        Err(Error::protocol_violation(
          "peer sent a second message before the session was accepted",
        ))
      }

      Phase::Done => Err(Error::protocol_violation(
        "handshake already complete; feed the SessionState instead",
      )),
    }
  }

  /// Acceptor only: supply the sequence numbers persisted for the identity
  /// reported by [`Step::NeedsSession`].
  pub fn accept_session(
    &mut self,
    session: Session,
    now: Instant,
    out: &mut impl SessionOutput,
  ) -> Result<Step> {
    let Phase::AcceptorAwaitingSession(pending) =
      std::mem::replace(&mut self.phase, Phase::Done)
    else {
      return Err(Error::protocol_violation(
        "accept_session called when no session was being awaited",
      ));
    };

    // Only now is there a session to attach anything to.
    out.event(Event::ConnectionEstablished)?;

    let PendingLogon {
      session_id,
      logon,
      raw,
    } = *pending;
    let mut state = SessionState::new(session_id, session, now);

    // The peer's Logon is reported before our reply goes out. An application
    // persisting from these events must see what arrived before what it
    // answered with, or a crash between the two leaves it believing it sent a
    // Logon in response to nothing.
    out.event(Event::RawMessageReceived(&raw, state.session()))?;

    let reply = logon_message(state.session())?;
    state.send_logon(reply, out)?;

    let progress = state.start(logon, now, out)?;
    Ok(Step::Established {
      state: Box::new(state),
      progress,
    })
  }
}

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
