//! FIX field, message-type and codeset definitions for babelfix, generated
//! from the FIX.Latest Orchestra data.
//!
//! - [`fields`]: typed field constants — `Field<M>` for a field of datatype
//!   `M`, `GroupField` for a NumInGroup — for reading and writing
//!   [`Message`](babelfix_core::message::Message)s.
//! - [`tags`]: the same names as plain `u32` tag numbers, for `match` and for
//!   loops over several fields.
//! - [`msg_type`]: `MsgType(35)` values by message name.
//! - [`codesets`]: an enum per codeset, each with an `Unlisted` variant for
//!   values the specification does not list.
//!
//! One set of constants serves every FIX version: tag numbers, datatypes and
//! codes are shared across versions, and FIX.Latest is the superset.
//!
//! This crate sits on top of `babelfix-core`, which defines the types the
//! constants are made of and does not depend on it. It is re-exported from the
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
