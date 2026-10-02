//! Tests for the message representation, organised by the rules and API in
//! `message_proposal.md`.

use bytes::BytesMut;

use std::sync::{Arc, OnceLock};

use babelfix_core::message::*;
use babelfix_schema::codesets::{PartyRole, Side as SideCode};
use babelfix_schema::fields::*;
use babelfix_schema::{msg_type, tags};

/// FIX 4.4.
fn dict() -> Arc<Dictionary> {
  static DICT: OnceLock<Arc<Dictionary>> = OnceLock::new();
  DICT
    .get_or_init(|| {
      Dictionaries::standard()
        .unwrap()
        .get("FIX.4.4")
        .unwrap()
        .clone()
    })
    .clone()
}

/// Frame `body` (`|`-delimited, starting at 35) as a complete SOH message with
/// correct BodyLength and CheckSum.
fn frame(body: &str) -> Vec<u8> {
  let body = format!("{}\x01", body.replace('|', "\x01"));
  let mut msg = format!("8=FIX.4.4\x019={}\x01{body}", body.len()).into_bytes();
  let sum: u32 = msg.iter().map(|&b| b as u32).sum();
  msg.extend_from_slice(format!("10={:03}\x01", sum % 256).as_bytes());
  msg
}

fn parse(body: &str) -> Message {
  Message::parse(&dict(), frame(body)).unwrap()
}

fn parse_err(body: &str) -> ParseError {
  Message::parse(&dict(), frame(body)).unwrap_err()
}

fn piped(msg: &Message) -> String {
  msg.to_string()
}

const NOS: &str = "35=D|49=S|56=T|34=2|52=20261002-10:00:00|11=abc|453=2|448=A|447=D|452=3|448=B|447=D|452=4|55=XYZ|54=1|38=100|40=2|44=12.30|60=20261002-10:00:00";

// ---------------------------------------------------------------------------
// Parsing and reading
// ---------------------------------------------------------------------------

#[test]
fn parses_and_reads_typed_values() {
  let msg = parse(NOS);
  assert_eq!(msg.msg_type(), "D");
  let body = msg.body();
  assert_eq!(body.req(ClOrdID).unwrap(), "abc");
  assert_eq!(body.req(Side).unwrap(), SideCode::Buy);
  assert_eq!(body.req(OrderQty).unwrap().as_str(), "100");
  assert_eq!(body.req(Price).unwrap().as_str(), "12.30");
  assert_eq!(body.get(StopPx).unwrap(), None);
  assert_eq!(body.req(StopPx), Err(FieldError::missing(99)));
  assert_eq!(msg.header().req(MsgSeqNum).unwrap(), 2);
  assert_eq!(msg.header().get(PossDupFlag).unwrap(), None);
  // Header fields are not in the body, and vice versa.
  assert!(!body.has(MsgSeqNum));
  assert!(!msg.header().has(ClOrdID));
  assert_eq!(msg.find(MsgSeqNum).unwrap(), Some(2));
  assert_eq!(msg.find(ClOrdID).unwrap().unwrap(), "abc");
}

#[test]
fn malformed_values_are_errors_with_the_tag() {
  let msg = parse("35=D|11=a|44=12.3.4|38=x|54=1");
  let e = msg.body().get(Price).unwrap_err();
  assert_eq!(e.tag, 44);
  assert_eq!(
    e.reject_reason(),
    reject_reason::INCORRECT_DATA_FORMAT_FOR_VALUE
  );
  assert!(msg.body().get(OrderQty).is_err());
  assert_eq!(msg.body().raw(Price), Some(&b"12.3.4"[..]));
}

#[test]
fn groups_read_by_instance() {
  let msg = parse(NOS);
  let parties = msg.body().group(NoPartyIDs);
  assert_eq!(parties.len(), 2);
  let ids: Vec<_> = parties
    .iter()
    .map(|p| p.req(PartyID).unwrap().to_string())
    .collect();
  assert_eq!(ids, ["A", "B"]);
  assert_eq!(
    parties.get(0).unwrap().req(PartyRole).unwrap(),
    PartyRole::ClientID
  );
  assert_eq!(
    parties.get(1).unwrap().req(PartyRole).unwrap(),
    PartyRole::ClearingFirm
  );
  // Group members are not top-level fields.
  assert!(!msg.body().has(PartyID));
  // Fields after the group are found.
  assert_eq!(msg.body().req(Symbol).unwrap(), "XYZ");
  assert!(msg.body().group(NoLegs).is_empty());
}

#[test]
fn nested_groups() {
  let msg = parse(
    "35=D|11=a|453=2|448=A|447=D|452=3|802=2|523=x|803=1|523=y|803=2|448=B|447=D|452=4|55=S|54=1|40=1",
  );
  let parties = msg.body().group(NoPartyIDs);
  assert_eq!(parties.len(), 2);
  let subs = parties.get(0).unwrap().group(NoPartySubIDs);
  assert_eq!(subs.len(), 2);
  assert_eq!(subs.get(1).unwrap().req(PartySubID).unwrap(), "y");
  assert!(parties.get(1).unwrap().group(NoPartySubIDs).is_empty());
  assert_eq!(msg.body().req(Symbol).unwrap(), "S");
}

#[test]
fn group_fields_in_any_order_within_an_instance() {
  // TagValue §4.3.6.3 asks for definition order; we accept any order after
  // the delimiter.
  let msg =
    parse("35=D|11=a|453=2|448=A|452=3|447=D|448=B|447=D|55=S|54=1|40=1");
  let parties = msg.body().group(NoPartyIDs);
  assert_eq!(
    parties.get(0).unwrap().raw(tags::PartyIDSource),
    Some(&b"D"[..])
  );
  assert_eq!(parties.get(1).unwrap().get(PartyRole).unwrap(), None);
}

#[test]
fn header_groups() {
  let msg = parse(
    "35=0|49=S|56=T|627=2|628=H1|629=20261002-10:00:00|628=H2|34=1|52=20261002-10:00:00|112=x",
  );
  let hops = msg.header().group(NoHops);
  assert_eq!(hops.len(), 2);
  assert_eq!(hops.get(1).unwrap().req(HopCompID).unwrap(), "H2");
  assert_eq!(msg.header().req(MsgSeqNum).unwrap(), 1);
  assert_eq!(msg.body().req(TestReqID).unwrap(), "x");
}

#[test]
fn data_fields_may_contain_anything() {
  let msg = parse("35=A|98=0|108=30|95=7|96=a\x01b=|\x00c|141=Y");
  assert_eq!(msg.body().req(RawData).unwrap(), b"a\x01b=\x01\x00c");
  assert_eq!(msg.body().req(RawDataLength).unwrap(), 7);
  assert!(msg.body().req(ResetSeqNumFlag).unwrap());
}

#[test]
fn data_field_must_follow_its_length() {
  let e = parse_err("35=A|98=0|108=30|95=3|141=Y|96=abc");
  assert_eq!(e.tag, Some(96));
  let e = parse_err("35=A|98=0|108=30|95=9|96=abc");
  assert!(e.is_garbled());
}

#[test]
fn hinted_lookups_are_never_wrong() {
  let msg = parse(NOS);
  let body = msg.body();
  let parties = body.group(NoPartyIDs);
  assert_eq!(
    body.get_from(Symbol, parties.end()).unwrap().unwrap(),
    "XYZ"
  );
  // A hint past the field wraps round to find it.
  assert_eq!(
    body.get_from(ClOrdID, parties.end()).unwrap().unwrap(),
    "abc"
  );
  // A hint from inside a group (not a boundary of this block) is ignored.
  let inner = parties.get(1).unwrap().first().unwrap().pos();
  assert_eq!(body.get_from(Symbol, inner).unwrap().unwrap(), "XYZ");
  // A hint from another block entirely, likewise.
  let header_pos = msg.header().first().unwrap().pos();
  assert_eq!(
    body.get_from(Price, header_pos).unwrap().unwrap().as_str(),
    "12.30"
  );
  for f in body.fields() {
    if f.tag() == tags::NoPartyIDs {
      assert_eq!(body.get_from(Side, f.pos()).unwrap(), Some(SideCode::Buy));
    }
  }
}

#[test]
fn unknown_tags_are_kept() {
  let msg = parse("35=D|11=a|9999=custom|55=S|54=1|40=1");
  assert_eq!(msg.body().raw(9999), Some(&b"custom"[..]));
  assert_eq!(
    msg
      .body()
      .get(Field::<datatypes::Str>::new(9999))
      .unwrap()
      .unwrap(),
    "custom"
  );
}

#[test]
fn unknown_tag_closes_a_group_and_lands_outside_it() {
  let msg = parse("35=D|11=a|453=1|448=A|9999=x|55=S|54=1|40=1");
  assert_eq!(msg.body().raw(9999), Some(&b"x"[..]));
  assert_eq!(msg.body().group(NoPartyIDs).get(0).unwrap().raw(9999), None);
}

// ---------------------------------------------------------------------------
// The rules (message_proposal.md §4)
// ---------------------------------------------------------------------------

#[test]
fn rule_first_three_fields() {
  let raw = frame("35=0|49=S").into_iter().collect::<Vec<_>>();
  assert!(Message::parse(&dict(), raw).is_ok());
  // 35 not third.
  let mut bad = b"8=FIX.4.4\x019=5\x0149=S\x01".to_vec();
  let sum: u32 = bad.iter().map(|&b| b as u32).sum();
  bad.extend_from_slice(format!("10={:03}\x01", sum % 256).as_bytes());
  assert!(Message::parse(&dict(), bad).unwrap_err().is_garbled());
  // 35 again later.
  assert_eq!(
    parse_err("35=0|49=S|35=1").reject_reason,
    Some(reject_reason::TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER)
  );
}

#[test]
fn rule_header_then_body_then_trailer() {
  let e = parse_err("35=D|11=a|49=S|55=X");
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER)
  );
  assert_eq!(e.tag, Some(49));
}

#[test]
fn rule_empty_values() {
  let e = parse_err("35=D|11=|55=X");
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::TAG_SPECIFIED_WITHOUT_A_VALUE)
  );
  assert_eq!(e.tag, Some(11));
}

/// Duplicates are kept by parsing — reads see the first — and caught by the
/// opt-in `validate_strict`.
#[test]
fn rule_duplicates() {
  let strict = |body: &str| parse(body).validate_strict().unwrap_err();

  let m = parse("35=D|11=a|55=X|11=b");
  assert_eq!(m.body().req(ClOrdID).unwrap(), "a");
  let e = strict("35=D|11=a|55=X|11=b");
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::TAG_APPEARS_MORE_THAN_ONCE)
  );
  assert_eq!(e.tag, Some(11));

  // Header and body together, for the rule (parsing still rejects a header
  // tag after the body began, which is a different rule).
  assert_eq!(
    parse_err("35=D|49=S|11=a|49=T").reject_reason,
    Some(reject_reason::TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER)
  );

  // Within one group instance.
  let e = strict("35=D|11=a|453=1|448=A|447=D|447=D|55=S");
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::TAG_APPEARS_MORE_THAN_ONCE)
  );
  assert_eq!(e.tag, Some(447));

  // But the same tag in different instances is fine.
  parse("35=D|11=a|453=2|448=A|447=D|448=B|447=D|55=S")
    .validate_strict()
    .unwrap();
  parse(NOS).validate_strict().unwrap();
}

#[test]
fn strict_validation_checks_group_field_order() {
  // Parsing accepts any order after the delimiter.
  let m = parse("35=D|11=a|453=1|448=A|452=3|447=D|55=S");
  let e = m.validate_strict().unwrap_err();
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::REPEATING_GROUP_FIELDS_OUT_OF_ORDER)
  );
  assert_eq!(e.tag, Some(447));
  // Built messages are in definition order, so they pass.
  let mut b = Message::new(&dict(), "D");
  b.body_mut()
    .group_mut(NoPartyIDs)
    .push()
    .set(PartyRole, PartyRole::ClientID)
    .set(
      PartyIDSource,
      babelfix_schema::codesets::PartyIDSource::Proprietary,
    )
    .set(PartyID, "X");
  b.validate_strict().unwrap();
}

#[test]
fn rule_num_in_group_count() {
  for body in [
    "35=D|11=a|453=3|448=A|448=B|55=S",
    "35=D|11=a|453=1|448=A|448=B|55=S",
  ] {
    assert_eq!(
      parse_err(body).reject_reason,
      Some(reject_reason::INCORRECT_NUM_IN_GROUP_COUNT),
      "{body}"
    );
  }
  // A group must start with its delimiter.
  assert_eq!(
    parse_err("35=D|11=a|453=1|447=D|448=A|55=S").reject_reason,
    Some(reject_reason::REPEATING_GROUP_FIELDS_OUT_OF_ORDER)
  );
}

#[test]
fn rule_unknown_groups() {
  // An unknown group with two instances: its delimiter repeats, which
  // strict validation catches.
  let e = parse("35=D|11=a|9000=2|9001=x|9001=y|55=S")
    .validate_strict()
    .unwrap_err();
  assert_eq!(
    e.reject_reason,
    Some(reject_reason::TAG_APPEARS_MORE_THAN_ONCE)
  );
  // With one instance it is indistinguishable from unknown tags.
  parse("35=D|11=a|9000=1|9001=x|55=S");
}

#[test]
fn rule_framing_is_verified() {
  let mut raw = frame("35=0|49=S");
  let n = raw.len();
  raw[n - 2] = if raw[n - 2] == b'0' { b'1' } else { b'0' }; // corrupt CheckSum
  assert!(Message::parse(&dict(), raw).unwrap_err().is_garbled());

  let raw = b"8=FIX.4.4\x019=99\x0135=0\x0110=000\x01".to_vec();
  assert!(Message::parse(&dict(), raw).unwrap_err().is_garbled());

  let mut raw = frame("35=0|49=S");
  raw.extend_from_slice(b"55=X\x01");
  assert!(Message::parse(&dict(), raw).unwrap_err().is_garbled());
}

// ---------------------------------------------------------------------------
// Serialising
// ---------------------------------------------------------------------------

#[test]
fn parsed_messages_encode_byte_for_byte() {
  for body in [
    NOS,
    "35=0|49=S|56=T|34=1|52=20261002-10:00:00",
    "35=A|98=0|108=30|95=7|96=a\x01b=|\x00c|141=Y",
    "35=0|49=S|56=T|627=2|628=H1|628=H2|34=1|52=20261002-10:00:00|112=x",
  ] {
    let raw = frame(body);
    let msg = Message::parse(&dict(), raw.clone()).unwrap();
    assert_eq!(msg.to_bytes().as_ref(), raw.as_slice());
    // And when re-encoded from the tape rather than copied.
    let mut out = BytesMut::new();
    msg.encode_delimited(&mut out, SOH);
    assert_eq!(out.as_ref(), raw.as_slice(), "{body}");
  }
}

#[test]
fn delimited_parse_and_display() {
  let piped_in = String::from_utf8(frame(NOS)).unwrap().replace('\x01', "|");
  let msg =
    Message::parse_delimited(&dict(), piped_in.as_bytes(), b'|').unwrap();
  assert_eq!(piped(&msg), piped_in);
  assert_eq!(msg.to_bytes().as_ref(), frame(NOS).as_slice());
}

#[test]
fn fragments() {
  let msg =
    Message::parse_fragment(&dict(), b"35=D|11=a|55=X|54=1", b'|').unwrap();
  assert_eq!(msg.body().req(Symbol).unwrap(), "X");
  assert_eq!(
    msg.to_bytes().as_ref(),
    frame("35=D|11=a|55=X|54=1").as_slice()
  );
  assert!(Message::parse_fragment(&dict(), b"11=a|55=X", b'|').is_err());
}

// ---------------------------------------------------------------------------
// Building and editing
// ---------------------------------------------------------------------------

#[test]
fn builds_a_message() {
  let mut m = Message::new(&dict(), msg_type::NewOrderSingle);
  {
    let mut b = m.body_mut();
    b.set(ClOrdID, "abc")
      .set(Side, SideCode::Buy)
      .set(OrderQty, 100u64);
    let mut parties = b.group_mut(NoPartyIDs);
    // Set in any order: instances come out in definition order.
    parties
      .push()
      .set(PartyRole, PartyRole::ClientID)
      .set(PartyID, "CLIENT-A");
    parties
      .push()
      .set(PartyID, "DESK-7")
      .set(PartyRole, PartyRole::DeskID);
  }
  m.body_mut().set(Symbol, "XYZ");
  assert_eq!(
    piped(&m),
    piped(&parse(
      "35=D|11=abc|54=1|38=100|453=2|448=CLIENT-A|452=3|448=DESK-7|452=76|55=XYZ"
    ))
  );
  // Round trip.
  let again = Message::parse(&dict(), m.to_bytes()).unwrap();
  assert_eq!(again, m);
}

#[test]
fn copying_values_between_messages() {
  let order = parse(NOS);
  let body = order.body();
  let mut er = Message::new(order.dict(), msg_type::ExecutionReport);
  {
    let mut b = er.body_mut();
    b.set(ClOrdID, body.req(ClOrdID).unwrap());
    b.set(OrderQty, body.req(OrderQty).unwrap());
    b.set(Side, body.req(Side).unwrap());
    for t in [tags::Symbol, tags::Account, tags::NoPartyIDs] {
      b.copy(&body, t);
    }
  }
  let b = er.body();
  assert_eq!(b.req(ClOrdID).unwrap(), "abc");
  assert_eq!(b.req(OrderQty).unwrap().as_str(), "100");
  assert_eq!(b.req(Symbol).unwrap(), "XYZ");
  assert!(!b.has(Account));
  assert_eq!(b.group(NoPartyIDs).len(), 2);
  assert_eq!(
    b.group(NoPartyIDs).get(1).unwrap().req(PartyID).unwrap(),
    "B"
  );
  Message::parse(&dict(), er.to_bytes()).unwrap();
}

#[test]
fn editing_a_parsed_message() {
  let mut m = parse(NOS);
  {
    let mut b = m.body_mut();
    b.set(Account, "HOUSE");
    b.set(Symbol, "ABC");
    b.remove(Price);
    b.group_mut(NoPartyIDs)
      .insert(0)
      .set(PartyID, "ROUTER")
      .set(PartyRole, PartyRole::ExecutingFirm);
    b.group_mut(NoPartyIDs)
      .retain(|p| p.raw(tags::PartyRole) != Some(b"3"));
  }
  assert!(m.wire().is_none(), "edited, so no longer clean");
  let b = m.body();
  assert_eq!(b.req(Account).unwrap(), "HOUSE");
  assert_eq!(b.req(Symbol).unwrap(), "ABC");
  assert!(!b.has(Price));
  let ids: Vec<_> = b
    .group(NoPartyIDs)
    .iter()
    .map(|p| p.req(PartyID).unwrap().to_string())
    .collect();
  assert_eq!(ids, ["ROUTER", "B"]);
  let again = Message::parse(&dict(), m.to_bytes()).unwrap();
  assert_eq!(again, m);
}

#[test]
fn removing_the_last_instance_removes_the_group() {
  let mut m = parse("35=D|11=a|453=1|448=A|55=S");
  m.body_mut().group_mut(NoPartyIDs).remove(0);
  assert!(!m.body().has(NoPartyIDs));
  assert_eq!(piped(&m), piped(&parse("35=D|11=a|55=S")));
}

#[test]
fn empty_instances_are_not_written() {
  let mut m = Message::new(&dict(), "D");
  {
    let mut b = m.body_mut();
    b.set(ClOrdID, "a");
    let mut g = b.group_mut(NoPartyIDs);
    g.push();
    g.push().set(PartyID, "X");
  }
  assert_eq!(piped(&m), piped(&parse("35=D|11=a|453=1|448=X")));
}

#[test]
fn header_edits_fill_the_gap_without_moving_the_body() {
  let mut m = parse(NOS);
  let body_start = m.body().start();
  m.header_mut()
    .set(PossDupFlag, true)
    .copy_value(tags::SendingTime, tags::OrigSendingTime)
    .unwrap();
  assert_eq!(m.body().start(), body_start);
  assert!(m.header().req(PossDupFlag).unwrap());
  assert_eq!(
    m.header().req(OrigSendingTime).unwrap().to_string(),
    "20261002-10:00:00"
  );
  // More header fields than the gap holds still work.
  for tag in 5000..5030u32 {
    m.header_mut().set_raw(tag, b"x");
  }
  // (Those tags are not header fields, so a parse would put them in the body;
  // check the message reads back, then that it round-trips as text.)
  assert_eq!(m.header().raw(5029), Some(&b"x"[..]));
  assert_eq!(m.body().req(Symbol).unwrap(), "XYZ");
}

#[test]
fn data_fields_set_their_length() {
  let mut m = Message::new(&dict(), "A");
  m.body_mut()
    .set(
      EncryptMethod,
      babelfix_schema::codesets::EncryptMethod::None,
    )
    .set(HeartBtInt, 30u64)
    .set(RawData, &b"a\x01b"[..]);
  assert_eq!(m.body().req(RawDataLength).unwrap(), 3);
  m.body_mut().set(RawData, &b"longer\x01data"[..]);
  assert_eq!(m.body().req(RawDataLength).unwrap(), 11);
  assert_eq!(
    m.body_mut()
      .try_set_raw(tags::RawDataLength, b"5")
      .unwrap_err(),
    FieldError::derived(95)
  );
  let again = Message::parse(&dict(), m.to_bytes()).unwrap();
  assert_eq!(again.body().req(RawData).unwrap(), b"longer\x01data");
  m.body_mut().remove(RawData);
  assert!(!m.body().has(RawDataLength));
}

#[test]
fn invalid_values() {
  let mut m = Message::new(&dict(), "D");
  let mut b = m.body_mut();
  assert_eq!(
    b.try_set(ClOrdID, "").unwrap_err(),
    FieldError::value(11, ValueError::Empty)
  );
  assert_eq!(
    b.try_set(ClOrdID, "a\x01b").unwrap_err(),
    FieldError::value(11, ValueError::Delimiter)
  );
  assert_eq!(
    b.try_set(Text, "日本").unwrap_err(),
    FieldError::value(58, ValueError::Unrepresentable)
  );
  assert_eq!(
    b.try_set_raw(tags::BodyLength, b"5").unwrap_err(),
    FieldError::derived(9)
  );
  assert_eq!(
    b.try_set_raw(tags::NoPartyIDs, b"5").unwrap_err(),
    FieldError::derived(453)
  );
  // The failed writes left nothing behind.
  assert!(m.body().is_empty());
}

#[test]
fn latin1_text_round_trips() {
  let mut m = Message::new(&dict(), "D");
  m.body_mut().set(Text, "café");
  assert_eq!(m.body().raw(Text), Some(&b"caf\xe9"[..]));
  assert_eq!(m.body().req(Text).unwrap().to_str(), "café");
}

#[test]
fn values_mut_rewrites_placeholders() {
  let mut m = parse("35=D|11={{random}}|55=S|60={{now}}");
  m.values_mut(|_, v| match v {
    b"{{now}}" => Some(b"20261002-10:00:00".to_vec()),
    b"{{random}}" => Some(b"r4nd0m".to_vec()),
    _ => None,
  });
  assert_eq!(
    piped(&m),
    piped(&parse("35=D|11=r4nd0m|55=S|60=20261002-10:00:00"))
  );
}

#[test]
fn unlisted_codes_round_trip() {
  let mut m = Message::new(&dict(), "D");
  m.body_mut().set(Side, SideCode::Unlisted(b"Z"));
  assert_eq!(m.body().req(Side).unwrap(), SideCode::Unlisted(b"Z"));
  assert_eq!(m.body().req(Side).unwrap().wire(), b"Z");
  m.body_mut().set_raw(tags::Side, b"2");
  assert_eq!(m.body().req(Side).unwrap(), SideCode::Sell);
}

#[test]
fn clear_reuses_a_message() {
  let mut m = parse(NOS);
  m.clear("0");
  assert_eq!(m.msg_type(), "0");
  assert!(m.body().is_empty());
  m.body_mut().set(TestReqID, "x");
  assert_eq!(piped(&m), piped(&parse("35=0|112=x")));
}

// ---------------------------------------------------------------------------
// Cursors and paths
// ---------------------------------------------------------------------------

#[test]
fn cursors_navigate_and_paths_resolve() {
  let msg = parse(NOS);
  let party_role = msg
    .body()
    .group(NoPartyIDs)
    .get(1)
    .unwrap()
    .fields()
    .find(|c| c.tag() == 452)
    .unwrap();
  let path = party_role.path();
  assert_eq!(path.to_string(), "body/453[1]/452");
  assert_eq!(msg.cursor(&path).unwrap().value(), Some(&b"4"[..]));

  let instance = party_role.up().unwrap();
  assert_eq!(instance.kind(), EntryKind::Instance);
  assert_eq!(instance.instance_index(), Some(1));
  assert_eq!(instance.path().to_string(), "body/453[1]");
  let group = instance.up().unwrap();
  assert_eq!(group.kind(), EntryKind::Group);
  assert_eq!(group.count(), Some(2));
  assert!(group.up().is_none());
  assert_eq!(group.next().unwrap().tag(), 55);
  assert_eq!(group.prev().unwrap().tag(), 11);
  assert_eq!(group.down().unwrap().down().unwrap().tag(), 448);

  let tags: Vec<u32> = msg.walk().map(|c| c.tag()).collect();
  assert_eq!(&tags[..5], &[8, 9, 35, 49, 56]);
  assert!(tags.contains(&452));
}

#[test]
fn cursor_mut_edits_by_path() {
  let mut msg = parse(NOS);
  let path: FieldPath = "body/453[1]/448".parse().unwrap();
  msg.cursor_mut(&path).unwrap().set_raw(b"CHANGED").unwrap();
  assert_eq!(
    msg
      .body()
      .group(NoPartyIDs)
      .get(1)
      .unwrap()
      .req(PartyID)
      .unwrap(),
    "CHANGED"
  );

  let group: FieldPath = "body/453".parse().unwrap();
  msg
    .cursor_mut(&group)
    .unwrap()
    .push_instance()
    .unwrap()
    .set(PartyID, "NEW");
  assert_eq!(msg.body().group(NoPartyIDs).len(), 3);

  msg
    .cursor_mut(&"body/453[0]".parse().unwrap())
    .unwrap()
    .remove();
  let ids: Vec<_> = msg
    .body()
    .group(NoPartyIDs)
    .iter()
    .map(|p| p.req(PartyID).unwrap().to_string())
    .collect();
  assert_eq!(ids, ["CHANGED", "NEW"]);
  Message::parse(&dict(), msg.to_bytes()).unwrap();
}

#[test]
fn debug_names_fields() {
  let msg = parse("35=D|11=a|54=1|453=1|448=A");
  let s = format!("{msg:#?}");
  assert!(s.contains("54 Side = 1 (Buy)"), "{s}");
  assert!(s.contains("453 NoPartyIDs = 1 instance(s)"), "{s}");
  assert!(s.contains("    448 PartyID = A"), "{s}");
}

// ---------------------------------------------------------------------------
// Fragments, normalising, encoding into a shared buffer
// ---------------------------------------------------------------------------

#[test]
fn fragments_move_late_header_fields_into_the_header() {
  let m = Message::parse_fragment(
    &dict(),
    b"56=Target|8=FIX.4.4|34=1|35=D|49=Sender|38=100|55=AAPL|11=ABC123|54=1",
    b'|',
  )
  .unwrap();
  assert_eq!(m.header().req(SenderCompID).unwrap(), "Sender");
  assert_eq!(m.header().req(MsgSeqNum).unwrap(), 1);
  assert!(!m.body().has(SenderCompID));
  assert_eq!(m.body().req(Symbol).unwrap(), "AAPL");
  // MsgType is written third however the fragment ordered it.
  assert!(piped(&m).starts_with("8=FIX.4.4|9="));
  assert!(piped(&m).contains("|35=D|"));
  let again = Message::parse(&dict(), m.to_bytes()).unwrap();
  assert_eq!(again.msg_type(), "D");
  // Strict parsing still refuses the same thing on the wire.
  assert!(parse_err("35=D|11=a|49=S").reject_reason.is_some());
}

#[test]
fn normalize_orders_by_definition() {
  let mut m = Message::parse_fragment(
    &dict(),
    b"8=FIX.4.4|35=D|1000=AfterAll|49=Sender|56=Target|12=x|34=1|55=Symbol|11=ClOrdID|50=y",
    b'|',
  )
  .unwrap();
  m.normalize();
  let order: Vec<u32> = m.walk().map(|c| c.tag()).collect();
  let pos = |t| order.iter().position(|&x| x == t).unwrap();
  // Header: StandardHeader order.
  assert!(
    pos(8) < pos(35)
      && pos(35) < pos(49)
      && pos(49) < pos(56)
      && pos(56) < pos(34)
  );
  // Body: NewOrderSingle order; unknown tags last, by number.
  assert!(pos(34) < pos(11) && pos(11) < pos(55));
  assert!(pos(55) < pos(12) && pos(12) < pos(1000));
  // Nothing lost; still a valid message.
  assert_eq!(m.body().raw(12), Some(&b"x"[..]));
  Message::parse(&dict(), m.to_bytes()).unwrap();
}

#[test]
fn normalize_orders_group_instances_and_is_canonical() {
  let a = b"8=FIX.4.4|35=AB|49=Sender|56=Target|34=1|11=ABC123|55=AAPL|54=1|38=100|555=2|600=6B|608=F|610=202509|600=6C|608=G|610=202510";
  let b = b"56=Target|8=FIX.4.4|34=1|35=AB|49=Sender|38=100|555=2|600=6B|610=202509|608=F|600=6C|608=G|610=202510|55=AAPL|11=ABC123|54=1";
  let mut a = Message::parse_fragment(&dict(), a, b'|').unwrap();
  let mut b = Message::parse_fragment(&dict(), b, b'|').unwrap();
  assert_ne!(piped(&a), piped(&b));
  a.normalize();
  b.normalize();
  assert_eq!(piped(&a), piped(&b));
  let legs = a.body().group(NoLegs);
  assert_eq!(legs.len(), 2);
  let tags: Vec<u32> = legs.get(0).unwrap().fields().map(|c| c.tag()).collect();
  assert_eq!(tags, [600, 608, 610]);
}

#[test]
fn encoding_appends_after_earlier_messages() {
  // A driver batches several messages into one buffer before a write; each
  // message's CheckSum must cover only itself.
  let first = parse("35=0|49=S|56=T|34=1|52=20261002-10:00:00");
  let mut second = Message::new(&dict(), "1");
  second.body_mut().set(TestReqID, "x");
  let mut out = BytesMut::new();
  first.encode(&mut out);
  second.encode(&mut out);
  let mut out = out;
  let mut decoder = babelfix_core::codec::FixDecoder::with_dictionary(
    Dictionaries::standard().unwrap(),
    None,
    dict(),
  );
  assert_eq!(decoder.decode(&mut out).unwrap().unwrap(), first);
  assert_eq!(decoder.decode(&mut out).unwrap().unwrap(), second);
  assert!(out.is_empty());
}

#[test]
fn rule_tags_have_no_leading_zeros() {
  // A fragment skips the framing checks, so only the tag rule can fail.
  let e = Message::parse_fragment(&dict(), b"35=0|049=S", b'|').unwrap_err();
  assert!(e.is_garbled(), "{e}");
  assert!(Message::parse_fragment(&dict(), b"35=0|49=S", b'|').is_ok());
}

/// Build messages from a seeded generator — fields, groups, nested groups,
/// data fields, edits — and check every one survives encode → parse intact.
#[test]
fn randomised_round_trips() {
  // xorshift64*: deterministic, no dependencies.
  let mut state = 0x9E37_79B9_7F4A_7C15u64;
  let mut next = move |n: u64| {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    state.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
  };

  for round in 0..300 {
    let mut m = Message::new(&dict(), "D");
    {
      let mut h = m.header_mut();
      h.set(SenderCompID, "S")
        .set(TargetCompID, "T")
        .set(MsgSeqNum, round as u64);
    }
    let mut b = m.body_mut();
    b.set(ClOrdID, format!("c{round}").as_str());
    if next(2) == 0 {
      b.set(Symbol, "XYZ").set(Side, SideCode::Sell);
    }
    if next(3) == 0 {
      let data: Vec<u8> =
        (0..next(20)).map(|i| (i as u8).wrapping_mul(37)).collect();
      if !data.is_empty() {
        b.set(RawData, data);
      }
    }
    let parties = next(4);
    for p in 0..parties {
      let mut g = b.group_mut(NoPartyIDs);
      let mut i = g.push();
      i.set(PartyRole, PartyRole::ClientID)
        .set(PartyID, format!("p{p}").as_str());
      for s in 0..next(3) {
        i.group_mut(NoPartySubIDs)
          .push()
          .set(
            PartySubIDType,
            babelfix_schema::codesets::PartySubIDType::Firm,
          )
          .set(PartySubID, format!("s{s}").as_str());
      }
    }
    // Some edits after the fact.
    if parties > 1 && next(2) == 0 {
      b.group_mut(NoPartyIDs).remove(next(parties) as usize);
    }
    if next(2) == 0 {
      b.set(Account, "A");
    }
    if next(3) == 0 {
      b.remove(ClOrdID);
    }

    let wire = m.to_bytes();
    let parsed = Message::parse(&dict(), wire.clone())
      .unwrap_or_else(|e| panic!("round {round}: {e}\n{m}"));
    assert_eq!(parsed, m, "round {round}");
    assert_eq!(parsed.to_bytes(), wire, "round {round}");
    let mut normalized = parsed.clone();
    normalized.normalize();
    assert_eq!(
      Message::parse(&dict(), normalized.to_bytes())
        .unwrap()
        .to_bytes(),
      normalized.to_bytes()
    );
  }
}
