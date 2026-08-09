//! # babelfix-core
//!
//! The sans-io core of the [babelfix](https://docs.rs/babelfix) FIX engine:
//! message representation, wire codec and the session state machine, with no
//! I/O, no timers, no tasks and no async runtime.
//!
//! Nothing here performs I/O or reads a clock. Bytes are fed in, bytes and
//! events come out, and the caller decides when — from an async task, from an
//! `epoll` loop, or from a test with a hand-advanced clock.
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|----------------|
//! | Schema | [`schema`] | Compile-time FIX tag-number constants |
//! | Repository | [`repository`] | Parsed FIX Orchestra metadata |
//! | Message | [`message`] | Parsing, building and serialising individual messages |
//! | Codec | [`codec`] | Framing a byte stream into messages and back |
//! | Time | [`time`] | FIX UTC timestamp formatting (no clock) |
//!
//! Most applications should depend on `babelfix` instead, which re-exports this
//! crate alongside a batteries-included tokio driver. Depend on `babelfix-core`
//! directly when you want to drive the protocol from your own event loop.

pub use babelfix_repo as repository;
pub use babelfix_repogen as schema;

pub mod codec;
pub mod message;
pub mod time;

pub use message::{FixMessage, Value};

/// The error type returned by all fallible babelfix operations.
///
/// Most variants carry a human-readable description; the `#[from]` variants wrap
/// the underlying error so it can be propagated with `?`. Downstream code can
/// match on the semantic variants or simply format the error via [`Display`].
///
/// [`Display`]: std::fmt::Display
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
  /// An underlying I/O error (socket, framing).
  #[error("I/O error: {0}")]
  Io(#[from] std::io::Error),

  /// An error parsing or loading the FIX repository / Orchestra data.
  #[error("FIX repository error: {0}")]
  Repository(#[from] repository::FixRepoError),

  /// A message could not be parsed or is structurally invalid.
  #[error("Invalid message: {0}")]
  InvalidMessage(std::borrow::Cow<'static, str>),

  /// A well-formed message violated a FIX session/protocol rule
  /// (e.g. an unexpected message type or a bad sequence number).
  #[error("Protocol violation: {0}")]
  ProtocolViolation(std::borrow::Cow<'static, str>),

  /// A connection could not be established, or was lost.
  #[error("Connection failed: {0}")]
  ConnectionFailed(std::borrow::Cow<'static, str>),

  /// Any error that does not fit a more specific variant.
  #[error("Unspecified FIX error: {0}")]
  Unspecified(std::borrow::Cow<'static, str>),
}

// A malformed numeric field in a message surfaces as an invalid message.
impl From<std::num::ParseIntError> for Error {
  fn from(e: std::num::ParseIntError) -> Self {
    Error::InvalidMessage(format!("invalid integer field: {e}").into())
  }
}

// Formatting failures while serialising a message are not expected in practice.
impl From<std::fmt::Error> for Error {
  fn from(e: std::fmt::Error) -> Self {
    Error::Unspecified(std::borrow::Cow::Owned(format!(
      "formatting error: {e}"
    )))
  }
}

impl Error {
  pub fn unspecified<S: Into<std::borrow::Cow<'static, str>>>(msg: S) -> Self {
    Error::Unspecified(msg.into())
  }

  pub fn invalid_message<S: Into<std::borrow::Cow<'static, str>>>(
    msg: S,
  ) -> Self {
    Error::InvalidMessage(msg.into())
  }

  pub fn connection_failed<S: Into<std::borrow::Cow<'static, str>>>(
    msg: S,
  ) -> Self {
    Error::ConnectionFailed(msg.into())
  }

  pub fn protocol_violation<S: Into<std::borrow::Cow<'static, str>>>(
    msg: S,
  ) -> Self {
    Error::ProtocolViolation(msg.into())
  }
}

/// Convenience alias for a `Result` whose error type is [`enum@Error`].
pub type Result<T> = std::result::Result<T, Error>;
