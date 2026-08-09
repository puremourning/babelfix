//! # babelfix
//!
//! An asynchronous [FIX protocol](https://www.fixtrading.org/) engine for Rust,
//! driven by the FIX Orchestra metadata repository.
//!
//! This crate is an umbrella: it re-exports [`babelfix_core`], which holds the
//! protocol, and — under the default `tokio` feature — [`babelfix_tokio`],
//! which holds the transport.
//!
//! | Layer | Module / crate | Responsibility |
//! |-------|----------------|----------------|
//! | Schema | [`schema`] (`babelfix-repogen`) | Compile-time FIX tag-number constants, e.g. [`schema::FIX_Latest::Fields`] |
//! | Repository | [`repository`] (`babelfix-repo`) | Parsed FIX Orchestra metadata: versions, messages, fields, components, groups |
//! | Message | [`message`] (`babelfix-core`) | Parsing, building and serialising individual FIX messages |
//! | Codec | [`codec`] (`babelfix-core`) | Framing a byte stream into messages and back |
//! | Session | [`session`] (`babelfix-core`) | Sequence numbers, heartbeats, test requests, resend/replay |
//! | Endpoint | [`endpoint`] (`babelfix-tokio`) | TCP acceptor/initiator that spawns sessions |
//! | Connection | [`connection`] (`babelfix-tokio`) | The same session driven inline, without channels |
//!
//! ## Which crate do I want?
//!
//! Everything through `babelfix::endpoint` is the batteries-included path: give
//! it a port and a repository and it runs sessions for you.
//!
//! If you want to own the event loop — a busy-polled socket, `io_uring`, a
//! runtime other than tokio — depend on `babelfix-core` directly. It has no
//! I/O, no timers, no tasks and no async runtime in its dependency tree; you
//! feed it bytes and it tells you what to send. See
//! [`babelfix_core::session`].
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

pub use babelfix_core::{
  Error, FixMessage, Result, Value, codec, message, repository, schema, time,
};

/// The session layer.
///
/// With the default `tokio` feature this is `babelfix-tokio`'s module, which
/// adds the driver and the owned [`SessionCommand`]/[`SessionEvent`] types on
/// top of the core's sans-io state machine and re-exports both.
///
/// [`SessionCommand`]: session::SessionCommand
/// [`SessionEvent`]: session::SessionEvent
#[cfg(feature = "tokio")]
pub use babelfix_tokio::session;

/// The session layer: `babelfix-core`'s sans-io state machine.
#[cfg(not(feature = "tokio"))]
pub use babelfix_core::session;

#[cfg(feature = "tokio")]
pub use babelfix_tokio::{connection, endpoint, util};
