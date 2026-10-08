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
//! - [`messages`], [`components`] and [`groups`]: a module per message,
//!   component and repeating group, holding just the fields and groups that
//!   belong to it. See [Scoped messages](#scoped-messages).
//! - [`MessageInstance`]: a variant per message type, for handling received
//!   messages. See [Received messages](#received-messages).
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
//!
//! # Scoped messages
//!
//! Each message's module has a `fields` module and a `groups` module with only
//! the fields and groups the message may contain, its components' included —
//! so an editor completing `new_order_single::fields::` offers just those.
//! Each group in `groups` is also a module of the same name, with its
//! instances' `fields` and `groups`. `header` and `trailer` are the
//! StandardHeader's and StandardTrailer's modules.
//!
//! Those constants are *scoped*: each names the message, component or group
//! it belongs to. [`TypedMessage`](babelfix_core::message::TypedMessage) gives a message
//! blocks of its own scope, which accept only its fields:
//!
//! ```no_run
//! use babelfix_core::message::{Dictionaries, TypedMessage};
//! use babelfix_schema::codesets::Side;
//! use babelfix_schema::messages::new_order_single::{self as nos, NewOrderSingle};
//!
//! let dicts = Dictionaries::standard().unwrap();
//! let mut order = TypedMessage::<NewOrderSingle>::new(dicts.get("FIX.Latest").unwrap());
//! let mut body = order.body_mut();
//! body
//!   .set(nos::fields::ClOrdID, "order-1")
//!   .set(nos::fields::Side, Side::Buy)
//!   .set(nos::fields::Symbol, "VOD.L"); // from the Instrument component
//! body
//!   .group_mut(nos::groups::NoPartyIDs)
//!   .push()
//!   .set(nos::groups::NoPartyIDs::fields::PartyID, "ABC");
//! ```
//!
//! A field from another message does not compile:
//!
//! ```compile_fail,E0277
//! # use babelfix_core::message::{Dictionaries, TypedMessage};
//! # use babelfix_schema::messages::{execution_report as er, new_order_single::NewOrderSingle};
//! # let dicts = Dictionaries::standard().unwrap();
//! # let mut order = TypedMessage::<NewOrderSingle>::new(dicts.get("FIX.Latest").unwrap());
//! order.body_mut().set(er::fields::ExecID, "exec-1");
//! ```
//!
//! Nor does one of the message's own fields in a group instance:
//!
//! ```compile_fail,E0277
//! # use babelfix_core::message::{Dictionaries, TypedMessage};
//! # use babelfix_schema::messages::new_order_single::{self as nos, NewOrderSingle};
//! # let dicts = Dictionaries::standard().unwrap();
//! # let mut order = TypedMessage::<NewOrderSingle>::new(dicts.get("FIX.Latest").unwrap());
//! order.body_mut().group_mut(nos::groups::NoPartyIDs).push().set(nos::fields::ClOrdID, "x");
//! ```
//!
//! Nor does an unscoped field, from [`fields`]:
//!
//! ```compile_fail,E0277
//! # use babelfix_core::message::{Dictionaries, TypedMessage};
//! # use babelfix_schema::messages::new_order_single::NewOrderSingle;
//! # let dicts = Dictionaries::standard().unwrap();
//! # let mut order = TypedMessage::<NewOrderSingle>::new(dicts.get("FIX.Latest").unwrap());
//! order.body_mut().set(babelfix_schema::fields::ClOrdID, "order-1");
//! ```
//!
//! — though an unscoped block (a plain `Message`'s, or a typed block's
//! `unscoped()` view) accepts any field, scoped or not.
//!
//! # Received messages
//!
//! [`MessageInstance`] has a variant per message of the version, each holding
//! it as a `TypedMessage` of that type, and `Unknown` for any other. Match on
//! it to handle each type with its own fields:
//!
//! ```no_run
//! # use babelfix_core::message::Message;
//! use babelfix_schema::MessageInstance;
//! use babelfix_schema::messages::{new_order_single as nos, order_cancel_request as ocr};
//!
//! # fn f(received: &Message) -> Result<(), babelfix_core::message::FieldError> {
//! match MessageInstance::from(received) {
//!   MessageInstance::NewOrderSingle(order) => {
//!     let id = order.body().req(nos::fields::ClOrdID)?;
//!   }
//!   MessageInstance::OrderCancelRequest(cancel) => {
//!     let id = cancel.body().req(ocr::fields::OrigClOrdID)?;
//!   }
//!   _ => {}
//! }
//! # Ok(()) }
//! ```
//!
//! `MessageInstance::from(&msg)` borrows the message; `from(msg)` takes it,
//! and `from(&mut msg)` edits it in place. For one type, use
//! [`Message::as_typed`](babelfix_core::message::Message::as_typed).

#![allow(non_upper_case_globals, non_snake_case, non_camel_case_types)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/schema.rs"));
