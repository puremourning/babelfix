//! # babelfix
//!
//! An asynchronous [FIX protocol](https://www.fixtrading.org/) engine for Rust,
//! driven by the FIX Orchestra metadata repository.
//!
//! babelfix is a small stack of layers, each usable on its own:
//!
//! | Layer | Module / crate | Responsibility |
//! |-------|----------------|----------------|
//! | Schema | [`schema`] (`babelfix-repogen`) | Compile-time FIX tag-number constants, e.g. [`schema::FIX_Latest::Fields`] |
//! | Repository | [`repository`] (`babelfix-repo`) | Parsed FIX Orchestra metadata: versions, messages, fields, components, groups |
//! | Message | [`message`] | Parsing, building and serialising individual FIX messages |
//! | Session | [`session`] | Sequence numbers, heartbeats, test requests, resend/replay |
//! | Endpoint | [`endpoint`] | TCP acceptor/initiator that frames the wire protocol and spawns sessions |
//!
//! ## Getting a repository
//!
//! The FIX Orchestra data for FIX 4.2, 4.4 and FIX.Latest is embedded in the
//! `babelfix-repo` crate, so nothing needs to be loaded from disk at runtime.
//! Build a [`repository::FixRepository`] once and share it as an `Arc`; select a
//! concrete version by its begin-string:
//!
//! ```no_run
//! use std::sync::Arc;
//! use babelfix::repository;
//!
//! let repo = Arc::new(repository::orchestrate().expect("load FIX repository"));
//! let fix44 = repo.get_version("FIX.4.4").expect("FIX.4.4 is available");
//! ```
//!
//! ## Building and serialising a message
//!
//! ```no_run
//! use babelfix::{repository, message::builder};
//! use babelfix::schema::FIX_Latest::Fields;
//!
//! let repo = repository::orchestrate().unwrap();
//! let fix44 = repo.get_version("FIX.4.4").unwrap();
//!
//! // NewOrderSingle (MsgType = "D"). `new` presets BeginString (8) and MsgType (35).
//! let mut order = builder::Message::new(fix44, "D").unwrap();
//! order.body.set_tag(Fields::ClOrdID, "order-1");   // set_tag takes anything Into<TypedValue>
//! order.body.set_tag(Fields::Symbol, "AAPL");
//! order.body.set_tag(Fields::Side, "1");            // 1 = Buy
//! order.body.set_tag(Fields::OrderQty, 100i64);
//! order.body.set_tag(Fields::Price, 42.5f64);
//!
//! // Convert to a wire message; BodyLength (9) and CheckSum (10) are computed.
//! let msg = order.into_message().unwrap();
//! // SOH (b'\x01') is the real field separator; b'|' is convenient for logging.
//! println!("{}", msg.to_string_delimited(b'|'));
//! ```
//!
//! Field constants live under a per-version module (see [`schema`]), but the tag
//! *numbers* are shared across versions, so [`schema::FIX_Latest::Fields`] is
//! used by convention regardless of the runtime version.
//!
//! The builder shown above is the convenient representation. For hot paths there
//! is also [`FixMessage`], a flat, buffer-backed tag list built for very fast
//! reading (and, rarely, fast append-only construction); see the [`message`]
//! module docs for when to use which.
//!
//! ## Running a session over TCP
//!
//! [`endpoint::serve`] accepts connections and [`endpoint::connect`] initiates
//! them; both surface a [`session::SessionHandle`] once a peer has logged on. See
//! the [`endpoint`] and [`session`] module docs for complete server and client
//! loops.
//!
//! ## Licensing
//!
//! babelfix is MIT licensed. The `babelfix-repo` and `babelfix-repogen` crates
//! additionally bundle and derive from the Apache-2.0 licensed FIX Orchestra
//! data, and are therefore released under `MIT AND Apache-2.0`.

pub use babelfix_repo as repository;
pub use babelfix_repogen as schema;

pub mod endpoint;
pub mod message;
pub mod session;
pub mod util;

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

// Internal channels closing is treated as the session/connection going away.
impl From<futures::channel::mpsc::SendError> for Error {
  fn from(_: futures::channel::mpsc::SendError) -> Self {
    Error::ConnectionFailed(std::borrow::Cow::Borrowed(
      "session channel closed",
    ))
  }
}

impl From<futures::channel::oneshot::Canceled> for Error {
  fn from(_: futures::channel::oneshot::Canceled) -> Self {
    Error::ConnectionFailed(std::borrow::Cow::Borrowed(
      "session channel closed",
    ))
  }
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
