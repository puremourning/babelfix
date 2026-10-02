//! The `decimix` feature, enabled on the umbrella crate, reaches the core's
//! conversions. Run with `cargo test -p babelfix --features decimix`.
#![cfg(feature = "decimix")]

use babelfix::message::{Dictionaries, Message};
use babelfix::schema::fields::{OrderQty, Price};
use decimix::{Dec19, UDec19};

#[test]
fn decimals_convert_through_the_umbrella_crate() {
  let dicts = Dictionaries::standard().unwrap();
  let dict = dicts.get("FIX.4.4").unwrap();

  let mut msg = Message::new(dict, "D");
  msg
    .body_mut()
    .set(Price, Dec19::from_ascii(b"-12.50").unwrap())
    .set(OrderQty, UDec19::from(100u64));

  let msg = Message::parse(dict, msg.to_bytes()).unwrap();
  let px: Dec19 = msg.body().req_as(Price).unwrap();
  let qty: UDec19 = msg.body().req_as(OrderQty).unwrap();
  assert_eq!(px, Dec19::from_ascii(b"-12.5").unwrap());
  assert_eq!(qty, UDec19::from(100u64));
}
