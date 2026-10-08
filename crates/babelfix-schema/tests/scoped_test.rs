//! Scoped messages: building and reading through the generated message,
//! component and group modules, and `TypedMessage`.

use std::sync::Arc;

use babelfix_core::message::{Dictionaries, Dictionary, Message, TypedMessage};
use babelfix_schema::codesets::{PartyRole, Side};
use babelfix_schema::messages::execution_report::ExecutionReport;
use babelfix_schema::messages::new_order_single::{self as nos, NewOrderSingle};

fn dict(version: &str) -> Arc<Dictionary> {
  Dictionaries::standard().unwrap().get(version).unwrap().clone()
}

fn order() -> TypedMessage<NewOrderSingle> {
  let mut order = TypedMessage::<NewOrderSingle>::new(&dict("FIX.Latest"));
  let mut body = order.body_mut();
  body
    .set(nos::fields::ClOrdID, "order-1")
    .set(nos::fields::Side, Side::Buy)
    // From the Instrument component.
    .set(nos::fields::Symbol, "VOD.L")
    .set(nos::fields::SecurityID, "GB00BH4HKS39");
  let mut parties = body.group_mut(nos::groups::NoPartyIDs);
  let mut party = parties.push();
  party
    .set(nos::groups::NoPartyIDs::fields::PartyID, "ABC")
    .set(nos::groups::NoPartyIDs::fields::PartyRole, PartyRole::ExecutingFirm);
  // A group within the group.
  party
    .group_mut(nos::groups::NoPartyIDs::groups::NoPartySubIDs)
    .push()
    .set(
      nos::groups::NoPartyIDs::groups::NoPartySubIDs::fields::PartySubID,
      "desk-1",
    );
  drop(party);
  order
    .header_mut()
    .set(nos::header::fields::SenderCompID, "ME");
  order
}

#[test]
fn builds_and_reads_back() {
  let order = order();
  let body = order.body();
  assert_eq!(body.req(nos::fields::ClOrdID).unwrap(), "order-1");
  assert_eq!(body.req(nos::fields::Side).unwrap(), Side::Buy);
  assert_eq!(body.req(nos::fields::Symbol).unwrap(), "VOD.L");

  let parties = body.group(nos::groups::NoPartyIDs);
  assert_eq!(parties.len(), 1);
  let party = parties.get(0).unwrap();
  assert_eq!(
    party.req(nos::groups::NoPartyIDs::fields::PartyID).unwrap(),
    "ABC"
  );
  let sub = party
    .group(nos::groups::NoPartyIDs::groups::NoPartySubIDs)
    .get(0)
    .unwrap();
  assert_eq!(
    sub
      .req(nos::groups::NoPartyIDs::groups::NoPartySubIDs::fields::PartySubID)
      .unwrap(),
    "desk-1"
  );
  assert_eq!(
    order.header().req(nos::header::fields::SenderCompID).unwrap(),
    "ME"
  );
}

#[test]
fn is_the_same_message_unscoped() {
  use babelfix_schema::fields::*;

  let order = order();
  let msg: &Message = &order;
  assert_eq!(msg.msg_type(), "D");
  assert_eq!(msg.body().req(ClOrdID).unwrap(), "order-1");
  assert_eq!(msg.body().group(NoPartyIDs).len(), 1);
  // Scoped constants are accepted by unscoped blocks.
  assert_eq!(msg.body().req(nos::fields::Symbol).unwrap(), "VOD.L");
  // And the other way round, through the unscoped view.
  assert_eq!(order.body().unscoped().req(Symbol).unwrap(), "VOD.L");
}

#[test]
fn views_a_message_by_type() {
  let msg: Message = order().into();
  let body = msg.body_as::<NewOrderSingle>().unwrap();
  assert_eq!(body.req(nos::fields::ClOrdID).unwrap(), "order-1");
  assert!(msg.body_as::<ExecutionReport>().is_none());

  let msg = TypedMessage::<ExecutionReport>::try_from(msg).unwrap_err();
  let order = TypedMessage::<NewOrderSingle>::try_from(msg).unwrap();
  assert_eq!(order.body().req(nos::fields::Symbol).unwrap(), "VOD.L");
}

#[test]
fn unscoped_view_edits_anything() {
  let mut order = order();
  order
    .body_mut()
    .unscoped()
    .set(babelfix_schema::fields::ExecID, "not-in-an-order");
  assert_eq!(
    order
      .body()
      .unscoped()
      .req(babelfix_schema::fields::ExecID)
      .unwrap(),
    "not-in-an-order"
  );
}

#[test]
#[should_panic(expected = "needs a FIX.Latest dictionary")]
fn needs_its_versions_dictionary() {
  TypedMessage::<NewOrderSingle>::new(&dict("FIX.4.4"));
}

#[test]
fn other_versions_messages_are_not_this_ones() {
  let msg = Message::new(&dict("FIX.4.4"), "D");
  assert!(msg.body_as::<NewOrderSingle>().is_none());
}

#[cfg(feature = "fix44")]
#[test]
fn fix44() {
  use babelfix_schema::fix44::codesets::Side;
  use babelfix_schema::fix44::messages::new_order_single::{
    self as nos, NewOrderSingle,
  };

  let mut order = TypedMessage::<NewOrderSingle>::new(&dict("FIX.4.4"));
  order
    .body_mut()
    .set(nos::fields::ClOrdID, "order-1")
    .set(nos::fields::Side, Side::Buy)
    .set(nos::fields::Symbol, "VOD.L")
    .group_mut(nos::groups::NoPartyIDs)
    .push()
    .set(nos::groups::NoPartyIDs::fields::PartyID, "ABC");
  assert_eq!(order.body().group(nos::groups::NoPartyIDs).len(), 1);
}

#[test]
fn untyped_message_edits_anything() {
  use babelfix_schema::fields::{ExecID, NoPartyIDs};

  let mut order = order();
  let msg = order.as_untyped_mut();
  msg.body_mut().set(ExecID, "not-in-an-order").set_raw(9999u32, b"custom");
  msg.body_mut().group_mut(NoPartyIDs).push().set_raw(9998u32, b"x");

  let body = order.body().unscoped();
  assert_eq!(body.req(ExecID).unwrap(), "not-in-an-order");
  assert_eq!(body.raw(9999u32), Some(&b"custom"[..]));
  // Still the typed message it was.
  assert_eq!(order.body().group(nos::groups::NoPartyIDs).len(), 2);
}
