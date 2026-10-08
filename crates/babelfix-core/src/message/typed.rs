//! Typed messages: a [`Message`] known to be of one message type, whose blocks
//! accept only that message's fields.
//!
//! `babelfix-schema` generates a [`MessageScope`] for every message of every
//! FIX version, with the field and group constants that belong to it, and a
//! `MessageInstance` enum per version to tell them apart:
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
//!
//! match MessageInstance::from(&received) {
//!   MessageInstance::NewOrderSingle(order) => { order.body().req(nos::fields::ClOrdID)?; }
//!   _ => {}
//! }
//! ```

use std::borrow::{Borrow, BorrowMut};
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::dict::Dictionary;
use super::edit::BlockMut;
use super::tape::Message;
use super::types::{MsgType, Scope};
use super::view::Block;

/// The scope of a message's body, with what else a [`TypedMessage`] needs to
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
/// `M` is how it holds the message: owned (the default), or anything that
/// borrows one — `&Message` to read one you were lent, `&mut Message` to edit
/// it, `Arc<Message>` to share it.
///
/// It dereferences to the [`Message`] for everything else. To set fields its
/// type does not define — bilaterally agreed ones, say — use a block's
/// `unscoped()` view, or edit the whole message unchecked with
/// [`as_untyped_mut`](Self::as_untyped_mut).
pub struct TypedMessage<S, M = Message> {
  msg: M,
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
}

impl<S: MessageScope, M: Borrow<Message>> TypedMessage<S, M> {
  /// `msg` as type `S`, if it is one; otherwise `msg`, given back.
  pub fn try_from_message(msg: M) -> Result<Self, M> {
    if TypedMessage::<S>::is(msg.borrow()) {
      Ok(Self {
        msg,
        _s: PhantomData,
      })
    } else {
      Err(msg)
    }
  }
}

impl<S: MessageScope, M: Borrow<Message>> TypedMessage<S, M> {
  pub fn header(&self) -> Block<'_, S::Header> {
    self.msg.borrow().header().scoped()
  }

  pub fn body(&self) -> Block<'_, S> {
    self.msg.borrow().body().scoped()
  }

  pub fn trailer(&self) -> Block<'_, S::Trailer> {
    self.msg.borrow().trailer().scoped()
  }
}

impl<S: MessageScope, M: BorrowMut<Message>> TypedMessage<S, M> {
  pub fn header_mut(&mut self) -> BlockMut<'_, S::Header> {
    self.msg.borrow_mut().header_mut().scoped()
  }

  pub fn body_mut(&mut self) -> BlockMut<'_, S> {
    self.msg.borrow_mut().body_mut().scoped()
  }

  /// The message, to edit unchecked: its blocks accept any field.
  ///
  /// It stays a `TypedMessage<S>`, so keep its MsgType:
  /// [`clear`](Message::clear) it to another type and its typed blocks are
  /// wrong.
  pub fn as_untyped_mut(&mut self) -> &mut Message {
    self.msg.borrow_mut()
  }
}

impl<S, M: Borrow<Message>> TypedMessage<S, M> {
  /// The message, unchecked.
  pub fn as_untyped(&self) -> &Message {
    self.msg.borrow()
  }

  /// The message, as it was held.
  pub fn into_untyped(self) -> M {
    self.msg
  }
}

impl<S: MessageScope> TryFrom<Message> for TypedMessage<S> {
  /// The message, given back: it is not of type `S`.
  type Error = Message;

  fn try_from(msg: Message) -> Result<Self, Message> {
    Self::try_from_message(msg)
  }
}

impl<'a, S: MessageScope> TryFrom<&'a Message> for TypedMessage<S, &'a Message> {
  /// The message, given back: it is not of type `S`.
  type Error = &'a Message;

  fn try_from(msg: &'a Message) -> Result<Self, &'a Message> {
    Self::try_from_message(msg)
  }
}

impl<'a, S: MessageScope> TryFrom<&'a mut Message>
  for TypedMessage<S, &'a mut Message>
{
  /// The message, given back: it is not of type `S`.
  type Error = &'a mut Message;

  fn try_from(msg: &'a mut Message) -> Result<Self, &'a mut Message> {
    Self::try_from_message(msg)
  }
}

impl<S> From<TypedMessage<S>> for Message {
  fn from(typed: TypedMessage<S>) -> Message {
    typed.msg
  }
}

impl<S, M: Borrow<Message>> Deref for TypedMessage<S, M> {
  type Target = Message;
  fn deref(&self) -> &Message {
    self.msg.borrow()
  }
}

impl<S, M: Borrow<Message>> AsRef<Message> for TypedMessage<S, M> {
  fn as_ref(&self) -> &Message {
    self.msg.borrow()
  }
}

impl<S, M: Clone> Clone for TypedMessage<S, M> {
  fn clone(&self) -> Self {
    Self {
      msg: self.msg.clone(),
      _s: PhantomData,
    }
  }
}

impl<S, M: Borrow<Message>> PartialEq for TypedMessage<S, M> {
  fn eq(&self, other: &Self) -> bool {
    self.msg.borrow() == other.msg.borrow()
  }
}

impl<S, M: Borrow<Message>> fmt::Display for TypedMessage<S, M> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Display::fmt(self.msg.borrow(), f)
  }
}

impl<S, M: Borrow<Message>> fmt::Debug for TypedMessage<S, M> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fmt::Debug::fmt(self.msg.borrow(), f)
  }
}

impl Message {
  /// This message as type `S`, to read, if it is one.
  pub fn as_typed<S: MessageScope>(&self) -> Option<TypedMessage<S, &Message>> {
    TypedMessage::try_from_message(self).ok()
  }

  /// This message as type `S`, to edit, if it is one.
  pub fn as_typed_mut<S: MessageScope>(
    &mut self,
  ) -> Option<TypedMessage<S, &mut Message>> {
    TypedMessage::try_from_message(self).ok()
  }
}
