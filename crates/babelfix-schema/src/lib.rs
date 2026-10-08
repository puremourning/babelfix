//! FIX field, message-type and codeset definitions for babelfix, generated
//! from the FIX Orchestra data.
//!
//! Each FIX version has its own module:
//!  - [`fixlatest`]: definitions from FIX.Latest. Always built, and re-exported
//!    at the top level of this crate, so `babelfix_schema::fields` is
//!    `babelfix_schema::fixlatest::fields`.
//!  - `fix44`: definitions from FIX 4.4, with the `fix44` feature.
//!  - `fix42`: definitions from FIX 4.2, with the `fix42` feature.
//!
//! To work in another version without its prefix, alias it:
//! `use babelfix_schema::fix44 as fix;`.
//!
//! Each version's module contains:
//! - [`fields`]: typed field constants — `Field<M>` for a field of datatype
//!   `M`, `GroupField` for a NumInGroup — for reading and writing
//!   [`Message`](babelfix_core::message::Message)s.
//! - [`tags`]: the same names as plain `u32` tag numbers, for `match` and for
//!   loops over several fields.
//! - [`msg_type`]: `MsgType(35)` values by message name.
//! - [`codesets`]: an enum per codeset, each with an `Unlisted` variant for
//!   values the specification does not list.
//!
//! Versions' definitions are separate types: a FIX 4.2 `ExecType` is not a
//! FIX.Latest `ExecType`, since the codes differ.
//!
//! This crate sits on top of `babelfix-core`, which defines the types the
//! constants are made of; `babelfix-core` does not depend on this crate. It is re-exported from the
//! `babelfix` crate as `babelfix::schema`.
//!
//! ```no_run
//! use babelfix_core::message::{Dictionaries, Message};
//! use babelfix_schema::{codesets::Side, fields::*, msg_type};
//!
//! let dicts = Dictionaries::standard().unwrap();
//! let mut order = Message::new(dicts.get("FIX.4.4").unwrap(), msg_type::NewOrderSingle);
//! order.body_mut().set(ClOrdID, "order-1").set(Side, Side::Buy);
//! ```

#![allow(non_upper_case_globals, non_snake_case, non_camel_case_types)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/schema.rs"));
