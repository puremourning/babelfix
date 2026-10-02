//! Errors from reading, writing and parsing fields.

use std::fmt;

/// `SessionRejectReason(373)` codes (FIX Session Layer §11.11) that message
/// handling can produce.
pub mod reject_reason {
  pub const INVALID_TAG_NUMBER: u32 = 0;
  pub const REQUIRED_TAG_MISSING: u32 = 1;
  pub const TAG_SPECIFIED_WITHOUT_A_VALUE: u32 = 4;
  pub const VALUE_IS_INCORRECT: u32 = 5;
  pub const INCORRECT_DATA_FORMAT_FOR_VALUE: u32 = 6;
  pub const INVALID_MSG_TYPE: u32 = 11;
  pub const TAG_APPEARS_MORE_THAN_ONCE: u32 = 13;
  pub const TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER: u32 = 14;
  pub const REPEATING_GROUP_FIELDS_OUT_OF_ORDER: u32 = 15;
  pub const INCORRECT_NUM_IN_GROUP_COUNT: u32 = 16;
  pub const FIELD_DELIMITER_IN_FIELD_VALUE: u32 = 17;
}

/// Why a value could not be decoded or encoded, before it is attributed to a
/// tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueError {
  /// The bytes are not in the datatype's lexical space (`44=abc`).
  Malformed,
  /// Well formed, but outside what the target type can hold (a negative
  /// quantity read as unsigned, more than 19 decimal places, ...).
  OutOfRange,
  /// Empty. FIX has no empty values: absent fields are omitted.
  Empty,
  /// Contains the SOH field delimiter, which only data fields may.
  Delimiter,
  /// Text that cannot be written in the Latin-1 character set FIX strings use.
  Unrepresentable,
}

/// A problem with one field, carrying the tag so it can become a Reject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FieldError {
  pub tag: u32,
  pub kind: FieldErrorKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldErrorKind {
  /// Required, but absent.
  Missing,
  /// Present, but its value is unusable.
  Value(ValueError),
  /// The field is derived from others and cannot be set directly: a data
  /// field's Length, `BodyLength(9)`, `CheckSum(10)`, a NumInGroup.
  Derived,
}

impl FieldError {
  pub fn missing(tag: u32) -> Self {
    Self {
      tag,
      kind: FieldErrorKind::Missing,
    }
  }

  pub fn value(tag: u32, e: ValueError) -> Self {
    Self {
      tag,
      kind: FieldErrorKind::Value(e),
    }
  }

  pub fn derived(tag: u32) -> Self {
    Self {
      tag,
      kind: FieldErrorKind::Derived,
    }
  }

  /// The `SessionRejectReason(373)` a Reject for this error carries, with
  /// [`tag`](Self::tag) as its `RefTagID(371)`.
  pub fn reject_reason(&self) -> u32 {
    use reject_reason::*;
    match self.kind {
      FieldErrorKind::Missing => REQUIRED_TAG_MISSING,
      FieldErrorKind::Value(ValueError::Malformed) => {
        INCORRECT_DATA_FORMAT_FOR_VALUE
      }
      FieldErrorKind::Value(ValueError::OutOfRange) => VALUE_IS_INCORRECT,
      FieldErrorKind::Value(ValueError::Empty) => TAG_SPECIFIED_WITHOUT_A_VALUE,
      FieldErrorKind::Value(ValueError::Delimiter) => {
        FIELD_DELIMITER_IN_FIELD_VALUE
      }
      FieldErrorKind::Value(ValueError::Unrepresentable) => {
        INCORRECT_DATA_FORMAT_FOR_VALUE
      }
      FieldErrorKind::Derived => VALUE_IS_INCORRECT,
    }
  }
}

impl fmt::Display for ValueError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      ValueError::Malformed => "malformed value",
      ValueError::OutOfRange => "value out of range",
      ValueError::Empty => "empty value",
      ValueError::Delimiter => "value contains the field delimiter",
      ValueError::Unrepresentable => "value not representable in Latin-1",
    })
  }
}

impl fmt::Display for FieldError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self.kind {
      FieldErrorKind::Missing => write!(f, "tag {} is missing", self.tag),
      FieldErrorKind::Value(e) => write!(f, "tag {}: {e}", self.tag),
      FieldErrorKind::Derived => {
        write!(f, "tag {} is derived and cannot be set", self.tag)
      }
    }
  }
}

impl std::error::Error for ValueError {}
impl std::error::Error for FieldError {}

impl From<FieldError> for crate::Error {
  fn from(e: FieldError) -> Self {
    crate::Error::invalid_message(e.to_string())
  }
}

/// A message that could not be parsed.
///
/// `reject_reason` is set when the message was framed well enough to be
/// rejected with a `Reject(35=3)`; when it is `None` the message is garbled
/// (FIX Session Layer §4.5.3) and should be dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
  pub reject_reason: Option<u32>,
  pub tag: Option<u32>,
  pub message: std::borrow::Cow<'static, str>,
}

impl ParseError {
  pub(crate) fn garbled(
    message: impl Into<std::borrow::Cow<'static, str>>,
  ) -> Self {
    Self {
      reject_reason: None,
      tag: None,
      message: message.into(),
    }
  }

  pub(crate) fn reject(
    reason: u32,
    tag: u32,
    message: impl Into<std::borrow::Cow<'static, str>>,
  ) -> Self {
    Self {
      reject_reason: Some(reason),
      tag: Some(tag),
      message: message.into(),
    }
  }

  /// Whether the message is too broken to reject, and should be dropped.
  pub fn is_garbled(&self) -> bool {
    self.reject_reason.is_none()
  }
}

impl fmt::Display for ParseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match (self.tag, self.reject_reason) {
      (Some(tag), Some(reason)) => {
        write!(f, "{} (tag {tag}, reject reason {reason})", self.message)
      }
      _ => write!(f, "garbled message: {}", self.message),
    }
  }
}

impl std::error::Error for ParseError {}

impl From<ParseError> for crate::Error {
  fn from(e: ParseError) -> Self {
    crate::Error::invalid_message(e.to_string())
  }
}
