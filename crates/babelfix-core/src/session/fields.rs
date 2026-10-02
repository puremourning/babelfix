//! The fields and message types the session layer itself reads and writes.
//!
//! Applications get the full, generated schema from `babelfix-schema`, which
//! builds on this crate; the core does not depend on it, so the few
//! session-layer definitions it needs are written out here. Their tags and
//! datatypes match the generated ones.

#![allow(non_upper_case_globals)]

use crate::message::{Field, MsgType, datatypes::*};

pub(crate) const BeginSeqNo: Field<UInt> = Field::new(7);
pub(crate) const EndSeqNo: Field<UInt> = Field::new(16);
pub(crate) const MsgSeqNum: Field<UInt> = Field::new(34);
pub(crate) const NewSeqNo: Field<UInt> = Field::new(36);
pub(crate) const PossDupFlag: Field<Boolean> = Field::new(43);
pub(crate) const SenderCompID: Field<Str> = Field::new(49);
pub(crate) const SendingTime: Field<UtcTimestamp> = Field::new(52);
pub(crate) const TargetCompID: Field<Str> = Field::new(56);
pub(crate) const Text: Field<Str> = Field::new(58);
/// A codeset (`EncryptMethodCodeSet`); written raw.
pub(crate) const EncryptMethod: u32 = 98;
pub(crate) const HeartBtInt: Field<Int> = Field::new(108);
pub(crate) const TestReqID: Field<Str> = Field::new(112);
pub(crate) const OrigSendingTime: Field<UtcTimestamp> = Field::new(122);
pub(crate) const GapFillFlag: Field<Boolean> = Field::new(123);
/// A codeset (`ApplVerIDCodeSet`); written raw.
pub(crate) const DefaultApplVerID: u32 = 1137;

pub(crate) mod msg_type {
  use super::MsgType;

  pub(crate) const Heartbeat: MsgType = MsgType::new("0");
  pub(crate) const TestRequest: MsgType = MsgType::new("1");
  pub(crate) const ResendRequest: MsgType = MsgType::new("2");
  pub(crate) const SequenceReset: MsgType = MsgType::new("4");
  pub(crate) const Logout: MsgType = MsgType::new("5");
  pub(crate) const Logon: MsgType = MsgType::new("A");
}
