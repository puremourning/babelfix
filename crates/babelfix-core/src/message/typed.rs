//! TypedMessage messages: a [`Message`] known to be of one message type, whose blocks
//! accept only that message's fields.
//!
//! `babelfix-schema` generates a [`MessageScope`] for every message of every
//! FIX version, with the field and group constants that belong to it:
//!
//! ```ignore
//! use babelfix_schema::messages::new_order_single::{self as nos, NewOrderSingle};
//!
//! let mut order = TypedMessage::<NewOrderSingle>::new(dict);
//! order.body_mut().set(nos::fields::ClOrdID, "order-1");
//! order.body_mut().set(nos::fields::Symbol, "VOD.L");     // from Instrument
//! let mut parties = order.body_mut().group_mut(nos::groups::NoPartyIDs);
//! parties.push().set(nos::groups::NoPartyIDs::fields::PartyID, "ABC");
//!
//! order.body_mut().set(execution_report::fields::ExecID, "x"); // does not compile
//! ```

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::dict::Dictionary;
use super::edit::BlockMut;
use super::tape::Message;
use super::types::{MsgType, Scope};
use super::view::Block;

/// The scope of a message's body, with what else a [`TypedMessage`] message needs to
/// know about it.
pub trait MessageScope: Scope {
  /// Its `MsgType(35)`.
  const MSG_TYPE: MsgType;
  /// The FIX version that defines it, by [`Dictionary`] name: `FIX.4.4`,
  /// `FIX.Latest`.
  const VERSION: &'static str;
  /// The scope of its header: the version's StandardHeader.
  type Header: Scope;
  /// The scope of its trailer: the version's StandardTrailer.
  type Trailer: Scope;
}

/// A [`Message`] of type `S`: its [`header`](Self::header),
/// [`body`](Self::body) and [`trailer`](Self::trailer) are blocks of `S`'s
/// scopes, so reading and writing them accepts only `S`'s fields.
///
/// It dereferences to the [`Message`] for everything else. To set fields its
/// type does not define — bilaterally agreed ones, say — use a block's
/// `unscoped()` view, or edit the whole message unchecked with
/// [`as_untyped_mut`](Self::as_untyped_mut).
pub struct TypedMessage<S> {
  msg: Message,
  _s: PhantomData<fn() -> S>,
}

impl<S: MessageScope> TypedMessage<S> {
  /// An empty message of type `S`. See [`Message::new`].
  ///
  /// # Panics
  ///
  /// If `dict` is not for the FIX version that defines `S`.
  pub fn new(dict: &Arc<Dictionary>) -> Self {
    assert_eq!(
      dict.version().name,
      S::VERSION,
      "a {} message needs a {} dictionary",
      S::MSG_TYPE.as_str(),
      S::VERSION
    );
    Self {
      msg: Message::new(dict, S::MSG_TYPE),
      _s: PhantomData,
    }
  }

  /// Whether `msg` is of type `S`: its MsgType, from the version that defines
  /// `S`.
  pub fn is(msg: &Message) -> bool {
    msg.msg_type() == S::MSG_TYPE.as_str()
      && msg.dict().version().name == S::VERSION
  }

  pub fn header(&self) -> Block<'_, S::Header> {
    self.msg.header().scoped()
  }

  pub fn body(&self) -> Block<'_, S> {
    self.msg.body().scoped()
  }

  pub fn trailer(&self) -> Block<'_, S::Trailer> {
    self.msg.trailer().scoped()
  }

  pub fn header_mut(&mut self) -> BlockMut<'_, S::Header> {
    self.msg.header_mut().scoped()
  }

  pub fn body_mut(&mut self) -> BlockMut<'_, S> {
    self.msg.body_mut().scoped()
  }

  /// The message, unchecked.
  pub fn as_untyped(&self) -> &Message {
    &self.msg
  }

  /// The message, to edit unchecked: its blocks accept any field.
  ///
  /// It stays a `TypedMessage<S>`, so keep its MsgType:
  /// [`clear`](Message::clear) it to another type and its typed blocks are
  /// wrong.
  pub fn as_untyped_mut(&mut self) -> &mut Message {
    &mut self.msg
  }

  pub fn into_untyped(self) -> Message {
    self.msg
  }
}

impl<S: MessageScope> TryFrom<Message> for TypedMessage<S> {
  /// The message, given back: it is not of type `S`.
  type Error = Message;

  fn try_from(msg: Message) -> Result<Self, Message> {
    if Self::is(&msg) {
      Ok(Self {
        msg,
        _s: PhantomData,
      })
    } else {
      Err(msg)
    }
  }
}

impl<S> From<TypedMessage<S>> for Message {
  fn from(typed: TypedMessage<S>) -> Message {
    typed.msg
  }
}

impl<S> Deref for TypedMessage<S> {
  type Target = Message;
  fn deref(&self) -> &Message {
    &self.msg
  }
}

impl<S> AsRef<Message> for TypedMessage<S> {
  fn as_ref(&self) -> &Message {
    &self.msg
  }
}

impl<S> Clone for TypedMessage<S> {
  fn clone(&self) -> Self {
    Self {
      msg: self.msg.clone(),
      _s: PhantomData,
    }
  }
}

impl<S> PartialEq for TypedMessage<S> {
  fn eq(&self, other: &Self) -> bool {
    self.msg == other.msg
  }
}

impl<S> fmt::Display for TypedMessage<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(&self.msg, f)
  }
}

impl<S> fmt::Debug for TypedMessage<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Debug::fmt(&self.msg, f)
  }
}

impl Message {
  /// The body, as message type `S`'s, if this message is one. For reading a
  /// message you hold by reference; see [`TypedMessage`] to own one.
  pub fn body_as<S: MessageScope>(&self) -> Option<Block<'_, S>> {
    TypedMessage::<S>::is(self).then(|| self.body().scoped())
  }

  /// [`body_as`](Self::body_as), to edit.
  pub fn body_mut_as<S: MessageScope>(&mut self) -> Option<BlockMut<'_, S>> {
    if TypedMessage::<S>::is(self) {
      Some(self.body_mut().scoped())
    } else {
      None
    }
  }
}
