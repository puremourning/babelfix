//! # babelfix
//!
//! An asynchronous [FIX protocol](https://www.fixtrading.org/) engine for Rust,
//! driven by the FIX Orchestra metadata repository.
//!
//! This crate is an umbrella: it re-exports [`babelfix_core`], which holds the
//! protocol, and — under the default `tokio` feature — `babelfix_tokio`, which
//! holds the transport.
//!
//! | Layer | Module / crate | Responsibility |
//! |-------|----------------|----------------|
//! | Schema | [`schema`] (`babelfix-schema`) | Typed field constants, message types and codeset enums, generated from FIX.Latest |
//! | Repository | [`repository`] (`babelfix-repo`) | Parsed FIX Orchestra metadata: versions, messages, fields, components, groups |
//! | Message | [`message`] (`babelfix-core`) | Parsing, reading, building, editing and serialising FIX messages |
//! | Codec | [`codec`] (`babelfix-core`) | Framing a byte stream into messages and back |
//! | Session | [`session`] (`babelfix-core`) | Sequence numbers, heartbeats, test requests, resend/replay |
//! | Driver | [`driver`] (`babelfix-core`) | The above assembled: feed bytes, drain bytes, no I/O |
//! | Connection | `connection` (`babelfix-tokio`) | The same session driven inline, without channels |
//! | Endpoint | `endpoint` (`babelfix-tokio`) | TCP acceptor/initiator that spawns sessions |
//!
//! ## Which crate do I want?
//!
//! Everything through `babelfix::endpoint` is the batteries-included path: give
//! it a port and the dictionaries and it runs sessions for you.
//!
//! If you want to own the event loop — a busy-polled socket, `io_uring`, a
//! runtime other than tokio — depend on `babelfix-core` directly. It has no
//! I/O, no timers, no tasks and no async runtime in its dependency tree; you
//! feed it bytes and it tells you what to send. See
//! [`babelfix_core::session`].
//!
//! ## Dictionaries
//!
//! The FIX Orchestra data for FIX 4.2, 4.4 and FIX.Latest is embedded in the
//! `babelfix-repo` crate, so nothing needs to be loaded from disk at runtime.
//! Parsing and building messages use a [`message::Dictionary`] per version — a
//! flattened, table-driven view of the Orchestra model. Compile them once and
//! share them:
//!
//! ```no_run
//! use babelfix::message::Dictionaries;
//!
//! let dicts = Dictionaries::standard().expect("load FIX repository");
//! let fix44 = dicts.get("FIX.4.4").expect("FIX.4.4 is available");
//! ```
//!
//! Dictionaries are named by version — `FIX.4.2`, `FIX.4.4`, `FIX.Latest` — and
//! found from a message's BeginString with
//! [`for_begin_string`](message::Dictionaries::for_begin_string), which knows
//! FIX.Latest as `FIXT.1.1`. To use your own Orchestra files, load them with
//! [`repository::load_orchestration`] and compile them with
//! [`Dictionaries::new`](message::Dictionaries::new).
//!
//! The full Orchestra model — components, documentation, everything a tool
//! might show — stays available as [`repository`], and from each dictionary
//! through [`message::Dictionary::version`].
//!
//! ## Building and reading a message
//!
//! ```no_run
//! use babelfix::message::{Dictionaries, Message};
//! use babelfix::schema::{codesets, fields::*, msg_type};
//!
//! let dicts = Dictionaries::standard().unwrap();
//! let fix44 = dicts.get("FIX.4.4").unwrap();
//!
//! let mut order = Message::new(fix44, msg_type::NewOrderSingle);
//! order
//!     .body_mut()
//!     .set(ClOrdID, "order-1")
//!     .set(Symbol, "AAPL")
//!     .set(Side, codesets::Side::Buy)
//!     .set(OrderQty, 100u64);
//!
//! // Fields are typed: Price decodes to a `Decimal`, the text as sent.
//! let price = order.body().get(Price).unwrap();
//! assert!(price.is_none());
//!
//! // BodyLength (9) and CheckSum (10) are computed when encoding. `Display`
//! // shows the wire form with `|` for SOH.
//! println!("{order}");
//! let wire = order.to_bytes();
//! # let _ = wire;
//! ```
//!
//! Every constant in [`schema::fields`] carries its field's datatype, so `get`
//! decodes to the right type and `set` takes only values of it. See the
//! [`message`] module docs for groups, editing and the representation.
//!
//! ## Running a session over TCP
//!
//! With the `tokio` feature, `endpoint::serve` accepts connections and
//! `endpoint::connect` initiates them; both surface a `session::SessionHandle`.
//! See the `endpoint` and `session` module docs for complete server and client
//! loops.
//!
//! ## Features
//!
//! * `tokio` (default): the transport — `endpoint`, `connection`, and the
//!   async `session` driver. Without it, `babelfix` is the sans-io core and
//!   the schema.
//! * `decimix`: read and write decimal fields as `decimix::Dec19`/`UDec19`.
//! * `decimix-finance`: as `decimix`, plus the `decimix-finance` newtypes.
//! * `serde`: `Serialize`/`Deserialize` for `SessionIdentifier` and
//!   `TimePrecision`.
//!
//! ## Licensing
//!
//! babelfix is MIT licensed. The `babelfix-repo` and `babelfix-schema` crates
//! additionally bundle or derive from the Apache-2.0 licensed FIX Orchestra
//! data, and are therefore released under `MIT AND Apache-2.0`.

pub use babelfix_core::{
  Error, Result, codec, driver, message, repository, time,
};

/// Typed FIX field constants, message types and codeset enums, generated from
/// the FIX.Latest Orchestra data. See [`babelfix_schema`].
pub use babelfix_schema as schema;

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
