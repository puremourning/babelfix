//! FIX messages: parsing, reading, building, editing and serialising.
//!
//! One type, [`Message`], whether it was parsed from the wire or built by hand.
//! Read it through [`Block`]s — [`header`](Message::header),
//! [`body`](Message::body), and the instances of repeating [`Group`]s — and edit
//! it through the same shapes, mutably: [`BlockMut`], [`GroupMut`], and
//! [`InstanceMut`] for an instance, which removes itself if it is left empty.
//!
//! ```no_run
//! # use babelfix_core::message::{Message, Dictionary};
//! # use babelfix_schema::fields::*;
//! # fn f(dict: &std::sync::Arc<Dictionary>, order: &Message) -> Result<(), babelfix_core::message::FieldError> {
//! let body = order.body();
//! let symbol = body.req(Symbol)?;                 // FixStr, borrowed from `order`
//! for party in body.group(NoPartyIDs) {
//!     let id = party.req(PartyID)?;
//! }
//!
//! let mut ack = Message::new(dict, "8");
//! ack.body_mut()
//!     .set(ClOrdID, body.req(ClOrdID)?)          // bytes copied, nothing decoded
//!     .set(Symbol, symbol);
//! # Ok(()) }
//! ```
//!
//! Fields are typed: every constant in the `babelfix-schema` crate's `fields`
//! (`babelfix::schema::fields`) carries its datatype, so `get` decodes to the right type and `set` accepts
//! only values of it. See [`types`] for the datatypes, and [`Decimal`],
//! [`FixStr`] for the two you meet most.
//!
//! The representation — a flat tape of entries over the message's bytes — is
//! described in [`tape`].

mod dict;
mod edit;
mod encode;
mod error;
mod hash;
mod normalize;
mod parse;
mod path;
pub mod tape;
pub mod types;
mod validate;
mod view;

pub use dict::{Dictionaries, Dictionary};
pub use edit::{BlockMut, CursorMut, GroupMut, InstanceMut};
pub use error::{
  FieldError, FieldErrorKind, ParseError, ValueError, reject_reason,
};
pub use path::{FieldPath, ParsePathError};
pub use tape::{Message, Region};
pub use types::{
  Date, Decimal, Field, FieldType, FixStr, FromFix, GroupField, MsgType, Tag,
  Time, Timestamp, ToFix, ValueWriter, datatypes,
};
pub use view::{Block, Cursor, EntryKind, Group, GroupIter, Pos};

/// The FIX field delimiter, Start of Heading.
pub const SOH: u8 = 0x01;

#[cfg(test)]
pub(crate) mod test_support {
  use std::sync::{Arc, OnceLock};

  use super::Dictionary;
  use crate::repository;

  static REPO: OnceLock<repository::FixRepository> = OnceLock::new();

  pub fn dict_for(version: &str) -> Arc<Dictionary> {
    let repo = REPO.get_or_init(|| repository::orchestrate().unwrap());
    Dictionary::new(repo.get_version(version).unwrap())
  }

  /// FIX 4.4.
  pub fn dict() -> Arc<Dictionary> {
    static DICT: OnceLock<Arc<Dictionary>> = OnceLock::new();
    DICT.get_or_init(|| dict_for("FIX.4.4")).clone()
  }
}
