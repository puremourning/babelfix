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
//! | Message | [`message`] (`babelfix-core`) | Parsing, building and serialising individual FIX messages |
//! | Session | [`session`] | Sequence numbers, heartbeats, test requests, resend/replay |
//! | Endpoint | [`endpoint`] | TCP acceptor/initiator that frames the wire protocol and spawns sessions |
//!
//! The message representation, the wire codec and (eventually) the session
//! state machine live in `babelfix-core`, which has no I/O, no timers and no
//! async runtime. This crate adds the tokio-based transport on top. Depend on
//! `babelfix-core` directly if you would rather drive the protocol from your
//! own event loop.
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

pub use babelfix_core::repository;
pub use babelfix_core::schema;
pub use babelfix_core::{Error, FixMessage, Result, Value, message, time};

pub mod endpoint;
pub mod session;
pub mod util;

/// Map a channel failure onto [`Error::ConnectionFailed`].
///
/// [`Error`] lives in `babelfix-core`, which knows nothing about `futures`, so
/// the `From<mpsc::SendError>` / `From<oneshot::Canceled>` impls this crate used
/// to carry would now break the orphan rule. Channel failures are mapped
/// explicitly at each call site instead.
pub(crate) fn chan_closed<E>(_: E) -> Error {
  Error::connection_failed("session channel closed")
}
