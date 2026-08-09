//! # babelfix-tokio
//!
//! The batteries-included tokio driver for [`babelfix_core`]: TCP transport,
//! heartbeat timers, per-connection tasks, and channels for talking to a
//! session from elsewhere in your application.
//!
//! The protocol itself lives in `babelfix-core` as a sans-io state machine and
//! knows nothing about any of this. This crate supplies the four things it
//! deliberately does without — a socket, a clock, a scheduler and a way to
//! reach the application — and nothing more.
//!
//! * [`endpoint::serve`] accepts connections; [`endpoint::connect`] initiates
//!   them with reconnect and backoff. Both surface a
//!   [`session::SessionHandle`] once a peer has logged on.
//! * [`session`] is the driver, plus the owned command and event types the
//!   handle carries.
//!
//! Most applications should depend on the `babelfix` umbrella crate, which
//! re-exports this alongside the core. Depend on this one directly if you want
//! the tokio transport without the umbrella.

pub use babelfix_core::{
  Error, FixMessage, Result, Value, codec, message, repository, schema, time,
};

pub mod endpoint;
pub mod session;
pub mod util;

/// Map a channel failure onto [`Error::ConnectionFailed`].
///
/// [`Error`] lives in `babelfix-core`, which knows nothing about `futures`, so
/// the `From<mpsc::SendError>` / `From<oneshot::Canceled>` impls this crate
/// used to carry would break the orphan rule. Channel failures are mapped
/// explicitly at each call site instead.
pub(crate) fn chan_closed<E>(_: E) -> Error {
  Error::connection_failed("session channel closed")
}
