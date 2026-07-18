//! FIX message parsing, building and serialisation.
//!
//! # Two representations
//!
//! babelfix models a message two ways, a deliberate speed/convenience trade-off.
//! Choose based on what you are doing.
//!
//! ## [`FixMessage`] — fast, flat, buffer-backed
//!
//! A [`FixMessage`] is barely more than the message's byte buffer (`data`) plus a
//! flat, ordered `Vec` of `(tag, `[`Value`]`)` pairs (`tags`). Parsing is trivial
//! and cheap: each [`Value`] is normally just a *slice* into the original buffer
//! ([`Value::StringRef`] / [`Value::DataRef`]), so there is no per-field
//! allocation or copying. This is the representation the [`crate::endpoint`]
//! codec produces and consumes.
//!
//! Reach for it when you need **very fast reading**: scan `tags` and resolve each
//! value against `data`. It also supports **fast construction by appending body
//! tags** — start from [`FixMessage::of_type`] (which presets `BeginString` and
//! `MsgType`) and push owned `(tag, `[`Value`]`)` pairs straight onto `tags`.
//! There is no field-ordering, repeating-group, or duplicate-tag bookkeeping —
//! that is exactly what makes it fast, and also what makes it easy to produce a
//! malformed message, so appending by hand is comparatively rare.
//!
//! ```no_run
//! use babelfix::{repository, FixMessage, Value};
//! use babelfix::schema::FIX_Latest::Fields;
//!
//! let repo = repository::orchestrate().unwrap();
//! let fix = repo.get_version("FIX.4.4").unwrap();
//!
//! // Fast construction: append owned body tags directly onto `tags`.
//! let mut msg = FixMessage::of_type(fix, "D"); // NewOrderSingle
//! msg.tags.push((Fields::Symbol, Value::String("AAPL".into())));
//! msg.tags.push((Fields::OrderQty, Value::String("100".into())));
//!
//! // Fast reading: scan the flat tag list, resolving each value against `data`.
//! for (tag, value) in &msg.tags {
//!     if *tag == Fields::Symbol {
//!         println!("symbol = {}", value.as_str(&msg.data));
//!     }
//! }
//! ```
//!
//! ## [`builder::Message`] — convenient, structured, slower
//!
//! A [`builder::Message`] is an ergonomic, typed representation keyed by tag
//! number: separate [`builder::Block`]s for header and body, [`builder::TypedValue`]
//! accessors, duplicate-tag checks, and first-class repeating groups
//! ([`builder::Block::group_mut`]). It owns its data and does bookkeeping on every
//! mutation, so it is **not especially fast** — but it is the right choice for
//! assembling non-trivial messages or navigating one by structure. This is the
//! type the [`crate::session`] layer works with.
//!
//! Convert between the two with [`builder::Message::from_message`] (`FixMessage` →
//! builder) and [`builder::Message::as_message`] / [`builder::Message::into_message`]
//! (builder → `FixMessage`).
//!
//! # Building
//!
//! ```no_run
//! use babelfix::{repository, message::builder};
//! use babelfix::schema::FIX_Latest::Fields;
//!
//! let repo = repository::orchestrate().unwrap();
//! let fix = repo.get_version("FIX.4.4").unwrap();
//!
//! let mut order = builder::Message::new(fix, "D").unwrap(); // NewOrderSingle
//! order.body.set_tag(Fields::ClOrdID, "abc");
//! order.body.set_tag(Fields::OrderQty, 100i64);
//! order.body.set_tag(Fields::Price, 12.30f64);
//!
//! let wire = order.into_message().unwrap();
//! ```
//!
//! # Parsing
//!
//! ```no_run
//! use babelfix::{repository, message::builder};
//! use babelfix::schema::FIX_Latest::Fields;
//!
//! let repo = repository::orchestrate().unwrap();
//! let fix = repo.get_version("FIX.4.4").unwrap();
//!
//! // `|`-delimited for readability; real wire data uses SOH (b'\x01').
//! let raw = b"8=FIX.4.4|9=...|35=D|11=abc|55=AAPL|10=000|";
//! let msg = builder::Message::from_bytes_delimited(fix, raw, b'|').unwrap();
//!
//! if let Some(symbol) = msg.body.tag(Fields::Symbol) {
//!     println!("symbol = {}", symbol.as_string());
//! }
//! ```
//!
//! # Repeating groups
//!
//! Groups are addressed by their `NumInGroup` tag; each entry is a
//! [`builder::Block`] pushed onto the group:
//!
//! ```no_run
//! use babelfix::{repository, message::builder};
//! use babelfix::schema::FIX_Latest::Fields;
//!
//! let repo = repository::orchestrate().unwrap();
//! let fix = repo.get_version("FIX.4.4").unwrap();
//! let mut msg = builder::Message::new(fix, "D").unwrap();
//!
//! let mut party = builder::Block::new();
//! party.push_tag(Fields::PartyID, "CLIENT-A").unwrap();
//! party.push_tag(Fields::PartyRole, 3i64).unwrap();
//! msg.body.group_mut(Fields::NoPartyIDs).push(party);
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use crate::repository;
use crate::repository::FieldBlock;

const DEFAULT_MSG_CAPACITY: usize = 128;
const DEFAULT_GROUP_CAPACITY: usize = 8;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ValRef {
  start: usize,
  len: usize,
}

impl<'a> ValRef {
  pub fn as_str(&self, data: &'a [u8]) -> &'a str {
    std::str::from_utf8(&data[self.start..self.start + self.len]).unwrap()
  }
  pub fn as_bytes(&self, data: &'a [u8]) -> &'a [u8] {
    &data[self.start..self.start + self.len]
  }
}

/// Represents a FIX field value
///
/// FIX Protocol requires that field values are US-ASCII except:
/// - Raw data fields (type="data") which can contain arbitrary binary data
/// - Text fields with associated encoding tags (not yet implemented)
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Value {
  /// Reference to a string value in the message buffer (US-ASCII)
  StringRef(ValRef),
  /// Owned string value (US-ASCII)
  String(String),
  /// Reference to raw binary data in the message buffer (arbitrary bytes)
  DataRef(ValRef),
  /// Owned raw binary data (arbitrary bytes)
  Data(Vec<u8>),
}

impl Default for Value {
  fn default() -> Self {
    Value::String("".into())
  }
}

impl Value {
  /// Convert value to String. For Data variants, converts bytes to lossy UTF-8.
  pub fn to_string(&self, data: &[u8]) -> String {
    match self {
      Value::StringRef(v) => v.as_str(data).to_string(),
      Value::String(v) => v.clone(),
      Value::DataRef(v) => {
        String::from_utf8_lossy(v.as_bytes(data)).to_string()
      }
      Value::Data(v) => String::from_utf8_lossy(v).to_string(),
    }
  }

  /// Get value as string slice. Panics if called on Data/OwnedData variant.
  pub fn as_str<'a>(&'a self, data: &'a [u8]) -> &'a str {
    match self {
      Value::StringRef(v) => v.as_str(data),
      Value::String(v) => v,
      Value::DataRef(_) | Value::Data(_) => {
        panic!("Cannot convert Data value to str - use as_bytes() instead")
      }
    }
  }

  /// Get value as byte slice. Works for all variants.
  pub fn as_bytes<'a>(&'a self, data: &'a [u8]) -> &'a [u8] {
    match self {
      Value::StringRef(v) => v.as_bytes(data),
      Value::String(v) => v.as_bytes(),
      Value::DataRef(v) => v.as_bytes(data),
      Value::Data(v) => v,
    }
  }

  /// Parse value as integer. Only works for string variants.
  pub fn parse<T>(&self, data: &[u8]) -> Result<T, std::num::ParseIntError>
  where
    T: std::str::FromStr<Err = std::num::ParseIntError>,
  {
    self.as_str(data).parse::<T>()
  }

  pub fn is_empty(&self) -> bool {
    match self {
      Value::StringRef(v) => v.len == 0,
      Value::String(v) => v.is_empty(),
      Value::DataRef(v) => v.len == 0,
      Value::Data(v) => v.is_empty(),
    }
  }

  /// Returns true if this is a Data or OwnedData variant (raw binary data)
  pub fn is_data(&self) -> bool {
    matches!(self, Value::DataRef(_) | Value::Data(_))
  }
}

impl<T> From<T> for Value
where
  T: ToString,
{
  fn from(s: T) -> Self {
    Value::String(s.to_string())
  }
}

type TagValue = (u32, Value);

#[derive(Default, Eq, Clone)]
pub struct FixMessage {
  pub fix_version: Arc<repository::FixVersion>,
  pub data: Bytes,
  pub tags: Vec<TagValue>,
}

impl PartialEq for FixMessage {
  fn eq(&self, other: &Self) -> bool {
    if self.fix_version != other.fix_version {
      return false;
    }
    if self.tags.len() != other.tags.len() {
      return false;
    }
    for (a, b) in self.tags.iter().zip(other.tags.iter()) {
      if a.0 != b.0 {
        return false;
      }
      if a.1.as_str(&self.data) != b.1.as_str(&other.data) {
        return false;
      }
    }
    true
  }
}

impl fmt::Debug for FixMessage {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut s = f.debug_struct("FixMessage");
    s.field("data", &self.data);
    s.finish()?;
    writeln!(f, "\ntags:")?;
    for (tag, val) in &self.tags {
      writeln!(f, "  {}={}", tag, val.as_str(&self.data))?;
    }
    Ok(())
  }
}

pub(crate) fn peek_infer_version_and_length(
  repo: &repository::FixRepository,
  s: &[u8],
  delimiter: u8,
) -> crate::Result<(Option<Arc<repository::FixVersion>>, Option<usize>, usize)>
{
  let Some(first_tag_delim) = s.iter().position(|c| *c == delimiter) else {
    return Ok((None, None, 0));
  };

  let first_tag = std::str::from_utf8(&s[..first_tag_delim])
    .map_err(|_| crate::Error::invalid_message("Invalid BeginString value"))?;

  let delimiter = delimiter as char;
  let (tag, begin_string) = first_tag.split_once('=').ok_or_else(|| {
    crate::Error::invalid_message(format!(
      "Invalid BeginString tag format in {}",
      first_tag
    ))
  })?;

  if tag != "8" {
    return Err(crate::Error::invalid_message(format!(
      "Expected BeginString tag (8), found {}",
      tag
    )));
  }

  let fix_version = repo
    .versions
    .values()
    .find(|v| v.begin_string == begin_string)
    .cloned()
    .ok_or_else(|| crate::Error::invalid_message("Unknown FIX version"))?;

  let Some(second_tag_delim) = s[first_tag_delim + 1..]
    .iter()
    .position(|c| *c == delimiter as u8)
  else {
    return Ok((Some(fix_version), None, first_tag_delim + 1));
  };

  let second_tag = std::str::from_utf8(
    &s[first_tag_delim + 1..first_tag_delim + 1 + second_tag_delim],
  )
  .map_err(|_| crate::Error::invalid_message("Invalid BodyLength encoding"))?;

  let (tag, body_length) = second_tag.split_once('=').ok_or_else(|| {
    crate::Error::invalid_message("Invalid BodyLength tag format")
  })?;

  if tag != "9" {
    return Err(crate::Error::invalid_message(format!(
      "Expected BodyLength tag (9), found {}",
      tag
    )));
  }

  let body_length = body_length
    .parse::<usize>()
    .map_err(|_| crate::Error::invalid_message("Invalid BodyLength value"))?;

  Ok((
    Some(fix_version),
    Some(body_length),
    first_tag_delim + 1 + second_tag_delim + 1,
  ))
}

pub(crate) fn peek_checksum(
  _repo: &repository::FixRepository,
  s: &[u8],
  delimiter: u8,
) -> crate::Result<(Option<u8>, usize)> {
  let Some(first_tag_delim) = s.iter().position(|c| *c == delimiter) else {
    return Ok((None, 0));
  };

  let first_tag = std::str::from_utf8(&s[..first_tag_delim])
    .map_err(|_| crate::Error::invalid_message("Invalid Checksum value"))?;

  let (tag, checksum) = first_tag.split_once('=').ok_or_else(|| {
    crate::Error::invalid_message(format!(
      "Invalid Checksum tag format in {}",
      first_tag
    ))
  })?;

  if tag != "10" {
    return Err(crate::Error::invalid_message(format!(
      "Expected Checksum tag (8), found {}",
      tag
    )));
  }

  let checksum = checksum
    .parse::<u8>()
    .map_err(|_| crate::Error::invalid_message("Invalid Checksum value"))?;

  Ok((Some(checksum), first_tag_delim + 1))
}

impl FixMessage {
  pub fn new(fix_version: Arc<repository::FixVersion>) -> Self {
    Self {
      fix_version,
      ..FixMessage::default()
    }
  }

  pub fn of_type(fix: Arc<repository::FixVersion>, msg_type: &str) -> Self {
    let mut msg = FixMessage::new(fix.clone());
    msg.tags.push((8, Value::String(fix.begin_string.clone())));
    msg.tags.push((35, Value::String(msg_type.to_string())));
    msg
  }

  pub fn is_complete(&self) -> bool {
    if self.tags.is_empty() {
      return false;
    }
    if self.tags.first().unwrap().0 != 8 {
      return false;
    }
    if self.tags.last().unwrap().0 != 10 {
      return false;
    }
    true
  }

  /// Parse from Bytes (zero-copy when coming from network)
  pub fn from_bytes(
    fix: Arc<repository::FixVersion>,
    data: Bytes,
  ) -> Result<(Self, usize), crate::Error> {
    Self::from_bytes_delimited(fix, data, b'\x01')
  }

  /// Parse from Bytes with custom delimiter (zero-copy when coming from network)
  pub fn from_bytes_delimited(
    fix: Arc<repository::FixVersion>,
    data: Bytes,
    delimiter: u8,
  ) -> Result<(Self, usize), crate::Error> {
    let mut msg = FixMessage {
      fix_version: fix,
      data: data.clone(),
      tags: Vec::with_capacity(std::cmp::max(
        DEFAULT_MSG_CAPACITY,
        data.len() / 10,
      )),
    };

    msg.tags.reserve(data.len() / 10);

    let mut tag_start: usize = 0;
    let mut tag: u32 = 0;
    let mut raw_data_len: usize = 0;

    let mut i: usize = 0;
    while i < data.len() {
      let c = data[i];
      i += 1;

      if tag == 0 && c == b'=' {
        if i < tag_start {
          return Err(crate::Error::invalid_message("Invalid tag"));
        }
        tag = std::str::from_utf8(&data[tag_start..i - 1])
          .unwrap()
          .parse::<u32>()
          .map_err(|_| {
            crate::Error::invalid_message(format!(
              "Invalid message: Unable to parse a tag number from {}",
              String::from_utf8_lossy(&data[tag_start..i - 1])
            ))
          })?;
        tag_start = i;

        if raw_data_len > 0 {
          if let Some(field) = msg.fix_version.get_field(tag) {
            if field.field_type == "data" {
              msg.tags.push((
                tag,
                Value::DataRef(ValRef {
                  start: tag_start,
                  len: raw_data_len,
                }),
              ));
              i += raw_data_len;
              raw_data_len = 0;
            }
          }
        }
      } else if c == delimiter {
        if tag == 0 {
          return Err(crate::Error::invalid_message("Missing tag"));
        }
        msg.tags.push((
          tag,
          Value::StringRef(ValRef {
            start: tag_start,
            len: i - tag_start - 1,
          }),
        ));

        if tag == 8 {
          if msg.get_being_string() != msg.fix_version.begin_string {
            return Err(crate::Error::invalid_message(format!(
              "Invalid FIX version: expected {}, got {}",
              msg.fix_version.begin_string,
              msg.get_being_string()
            )));
          }
        } else if tag == 10 {
          // end of the message
          tag = 0;
          break;
        }

        if let Some(field) = msg.fix_version.get_field(tag) {
          if field.field_type == "Length" {
            let len = std::str::from_utf8(&data[tag_start..i - 1])
              .unwrap()
              .parse::<usize>()
              .map_err(|_| {
                crate::Error::invalid_message(format!(
                  "Invalid message: Unable to parse a data length from {}",
                  String::from_utf8_lossy(&data[tag_start..i - 1])
                ))
              })?;
            raw_data_len = len;
          }
        }
        tag_start = i;
        tag = 0;
      }
    }

    if tag != 0 {
      // Missing trailing delimiter; accept this
      msg.tags.push((
        tag,
        Value::StringRef(ValRef {
          start: tag_start,
          len: data.len() - tag_start,
        }),
      ));
    }

    Ok((msg, i))
  }

  pub fn is_admin_message(&self) -> bool {
    matches!(
      self.get_type(),
      "0" | "A" | "1" | "2" | "3" | "4" | "5" | "j" | "h" | "Y" | "V"
    )
  }

  pub fn to_generic_string_delimited(&self, delimiter: u8) -> String {
    use std::fmt::Write;
    let delim = delimiter as char;
    let mut s = String::with_capacity(self.tags.len() * 20);
    write!(s, "35={}{}", self.get_type(), delim).unwrap();
    for (tag, val) in &self.tags {
      // Skip checksum, begin_string and body_length
      if *tag == 10 || *tag == 8 || *tag == 9 || *tag == 35 {
        continue;
      }

      match val {
        Value::DataRef(v) => {
          let bytes = v.as_bytes(&self.data);
          if !bytes.is_empty() {
            // FIXME: This is going to be compeltely wrong if the data does not
            // happen to be valid utf-8, which of course it probably won't be.
            // OK this fn is "to string" but should we just not provide such a
            // thing?
            write!(s, "{}={}{}", tag, String::from_utf8_lossy(bytes), delim)
              .unwrap();
          }
        }
        Value::Data(v) => {
          if !v.is_empty() {
            write!(s, "{}={}{}", tag, String::from_utf8_lossy(v), delim)
              .unwrap();
          }
        }
        Value::StringRef(v) => {
          let str_val = v.as_str(&self.data);
          if !str_val.is_empty() {
            write!(s, "{}={}{}", tag, str_val, delim).unwrap();
          }
        }
        Value::String(v) => {
          if !v.is_empty() {
            write!(s, "{}={}{}", tag, v, delim).unwrap();
          }
        }
      }
    }
    s
  }

  /// NOTE: Data tags get messed up because they can't actually safely be
  /// converted to String which must be valid UTF-8
  pub fn to_string_delimited(&self, delimiter: u8) -> String {
    use std::fmt::Write;
    let delim = delimiter as char;
    let body = self.to_generic_string_delimited(delimiter);
    let mut s = String::with_capacity(body.len() + 30);
    write!(
      s,
      "8={}{delim}9={}{delim}",
      self.fix_version.begin_string,
      body.len()
    )
    .unwrap();
    s.push_str(&body);
    let mut checksum: u8 = 0;
    for ch in s.as_bytes() {
      checksum = checksum.wrapping_add(*ch);
    }
    write!(s, "10={:03}{}", checksum, delim).unwrap();
    s
  }

  pub fn write_to(
    &self,
    out: &mut bytes::BytesMut,
    delimiter: u8,
  ) -> crate::Result<()> {
    use std::fmt::Write;

    use bytes::BufMut;

    out.reserve(self.tags.len() * 20);

    let delim = delimiter as char;
    // Write the header but with the body length set to 000000, then come back
    // and fill in the actual body length after we know it.
    write!(out, "8={}{delim}9=", self.fix_version.begin_string,)?;
    let body_length_pos = out.len();
    out.extend_from_slice(b"000000");
    out.put_u8(delimiter);
    let body_start = out.len();

    write!(out, "35={}{}", self.get_type(), delim)?;

    for (tag, val) in &self.tags {
      // Skip checksum, begin_string and body_length
      if *tag == 10 || *tag == 8 || *tag == 9 || *tag == 35 {
        continue;
      }

      match val {
        Value::DataRef(v) => {
          let bytes = v.as_bytes(&self.data);
          if !bytes.is_empty() {
            write!(out, "{}=", tag)?;
            out.extend_from_slice(bytes);
            out.put_u8(delimiter);
          }
        }
        Value::Data(v) => {
          if !v.is_empty() {
            write!(out, "{}=", tag)?;
            out.extend_from_slice(v.as_slice());
            out.put_u8(delimiter);
          }
        }
        Value::StringRef(v) => {
          let str_val = v.as_str(&self.data);
          if !str_val.is_empty() {
            write!(out, "{}=", tag)?;
            out.extend_from_slice(v.as_bytes(&self.data));
            out.put_u8(delimiter);
          }
        }
        Value::String(v) => {
          if !v.is_empty() {
            write!(out, "{}=", tag)?;
            out.extend_from_slice(v.as_bytes());
            out.put_u8(delimiter);
          }
        }
      }
    }

    let body_length = out.len() - body_start;
    out[body_length_pos..body_length_pos + 6]
      .copy_from_slice(format!("{:06}", body_length).as_bytes());
    let mut checksum: u8 = 0;
    for ch in &out[..] {
      checksum = checksum.wrapping_add(*ch);
    }
    out.extend_from_slice(b"10=");
    write!(out, "{:03}", checksum)?;
    out.put_u8(delimiter);

    Ok(())
  }

  pub fn get_being_string(&self) -> &str {
    for (tag, val) in &self.tags {
      if *tag == 8 {
        return match val {
          Value::StringRef(v) => v.as_str(&self.data),
          Value::String(v) => v,
          Value::DataRef(_) | Value::Data(_) => {
            panic!("BeginString (tag 8) should not be a Data field")
          }
        };
      }
    }
    &self.fix_version.begin_string
  }

  pub fn get_type(&self) -> &str {
    for (tag, val) in &self.tags {
      if *tag == 35 {
        return match val {
          Value::StringRef(v) => v.as_str(&self.data),
          Value::String(v) => v,
          Value::DataRef(_) | Value::Data(_) => {
            panic!("MsgType (tag 35) should not be a Data field")
          }
        };
      }
    }
    ""
  }
}

impl std::fmt::Display for FixMessage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.to_string_delimited(b'\x01'))
  }
}

pub mod builder {
  use super::*;

  /// Type-safe value representation for builder::Message
  #[derive(Clone, Debug, PartialEq, Default)]
  pub enum TypedValue {
    // Numeric types
    Int(i64),
    Float(f64),

    // String types
    String(String),
    Char(char),

    // Binary data (for data fields)
    Data(Vec<u8>),

    // Boolean
    Boolean(bool),

    // Empty/unset value
    #[default]
    Empty,
  }

  impl TypedValue {
    pub fn as_string(&self) -> String {
      match self {
        TypedValue::Int(v) => v.to_string(),
        TypedValue::Float(v) => v.to_string(),
        TypedValue::String(v) => v.clone(),
        TypedValue::Char(v) => v.to_string(),
        TypedValue::Data(v) => String::from_utf8_lossy(v).to_string(),
        TypedValue::Boolean(v) => (if *v { "Y" } else { "N" }).to_string(),
        TypedValue::Empty => String::new(),
      }
    }

    pub fn into_string(self) -> String {
      match self {
        TypedValue::String(v) => v,
        TypedValue::Int(v) => v.to_string(),
        TypedValue::Float(v) => v.to_string(),
        TypedValue::Char(v) => v.to_string(),
        TypedValue::Data(v) => String::from_utf8_lossy(&v).into_owned(),
        TypedValue::Boolean(v) => (if v { "Y" } else { "N" }).to_string(),
        TypedValue::Empty => String::new(),
      }
    }

    pub fn as_int(&self) -> Option<i64> {
      match self {
        TypedValue::Int(v) => Some(*v),
        TypedValue::String(s) => s.parse().ok(),
        _ => None,
      }
    }

    pub fn as_float(&self) -> Option<f64> {
      match self {
        TypedValue::Float(v) => Some(*v),
        TypedValue::String(s) => s.parse().ok(),
        TypedValue::Int(v) => Some(*v as f64),
        _ => None,
      }
    }

    pub fn as_bool(&self) -> Option<bool> {
      match self {
        TypedValue::Boolean(v) => Some(*v),
        TypedValue::String(s) => match s.as_str() {
          "Y" => Some(true),
          "N" => Some(false),
          _ => None,
        },
        _ => None,
      }
    }

    pub fn is_empty(&self) -> bool {
      match self {
        TypedValue::Empty => true,
        TypedValue::String(s) => s.is_empty(),
        _ => false,
      }
    }
  }

  impl std::fmt::Display for TypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "{}", self.as_string())
    }
  }

  impl From<String> for TypedValue {
    fn from(s: String) -> Self {
      TypedValue::String(s)
    }
  }

  impl From<&str> for TypedValue {
    fn from(s: &str) -> Self {
      TypedValue::String(s.to_string())
    }
  }

  impl From<i64> for TypedValue {
    fn from(i: i64) -> Self {
      TypedValue::Int(i)
    }
  }

  impl From<i32> for TypedValue {
    fn from(i: i32) -> Self {
      TypedValue::Int(i as i64)
    }
  }

  impl From<u32> for TypedValue {
    fn from(i: u32) -> Self {
      TypedValue::Int(i as i64)
    }
  }

  impl From<usize> for TypedValue {
    fn from(i: usize) -> Self {
      TypedValue::Int(i as i64)
    }
  }

  impl From<f64> for TypedValue {
    fn from(f: f64) -> Self {
      TypedValue::Float(f)
    }
  }

  impl From<bool> for TypedValue {
    fn from(b: bool) -> Self {
      TypedValue::Boolean(b)
    }
  }

  #[derive(Clone, Debug)]
  pub enum Element {
    Tag((u32, TypedValue)),
    Group(u32 /* NumInGroup */, Vec<Block>), // actual size is len of vec
    RawDataTag(
      u32, /* Length Tag */
      u32, /* Data Tag */
      TypedValue,
    ), /* actual size is len of data */
  }

  impl Element {
    pub fn is_set(&self) -> bool {
      match self {
        Element::Tag((_, val)) => !val.is_empty(),
        Element::Group(_, group) => !group.is_empty(),
        Element::RawDataTag(_, _, data) => !data.is_empty(),
      }
    }
  }

  #[derive(Clone, Debug, Default)]
  pub struct Block(Vec<Element>, HashMap<u32, usize>);

  #[derive(Clone)]
  pub struct Message {
    pub fix_message: Arc<repository::Message>,
    pub fix_version: Arc<repository::FixVersion>,

    pub header: Block,
    pub body: Block,
  }

  impl Message {
    pub fn is_admin_message(&self) -> bool {
      matches!(
        self.fix_message.msg_type.as_str(),
        "0" | "A" | "1" | "2" | "3" | "4" | "5" | "j" | "h" | "Y" | "V"
      )
    }

    pub fn new(
      repo: Arc<repository::FixVersion>,
      msg_type: &str,
    ) -> crate::Result<Self> {
      let mut s = Self {
        fix_message: repo.get_message(msg_type).ok_or_else(|| {
          crate::Error::invalid_message("Invalid message type")
        })?,
        fix_version: repo,
        body: Block::default(),
        header: Block::default(),
      };
      s.header.set_tag(8, s.fix_version.begin_string.clone());
      s.header.set_tag(35, s.fix_message.msg_type.clone());
      Ok(s)
    }

    /// Parse from borrowed slice (convenience wrapper, copies data)
    pub fn from_bytes(
      fix: Arc<repository::FixVersion>,
      s: &[u8],
    ) -> Result<Self, crate::Error> {
      let (msg, _) =
        FixMessage::from_bytes(fix.clone(), Bytes::copy_from_slice(s))?;
      Self::from_message(&msg)
    }

    /// Parse from borrowed slice with delimiter (convenience wrapper, copies data)
    pub fn from_bytes_delimited(
      fix: Arc<repository::FixVersion>,
      s: &[u8],
      delimiter: u8,
    ) -> Result<Self, crate::Error> {
      let (msg, _) = FixMessage::from_bytes_delimited(
        fix.clone(),
        Bytes::copy_from_slice(s),
        delimiter,
      )?;
      Self::from_message(&msg)
    }

    pub fn from_message(msg: &FixMessage) -> Result<Self, crate::Error> {
      fn parse_typed_value(
        fix: &repository::FixVersion,
        tag: u32,
        value_str: &str,
      ) -> TypedValue {
        if value_str.is_empty() {
          return TypedValue::Empty;
        }

        if let Some(field) = fix.get_field(tag) {
          match field.field_type.as_str() {
            "int" | "SeqNum" | "NumInGroup" | "DayOfMonth" | "Length" => {
              value_str
                .parse::<i64>()
                .map(TypedValue::Int)
                .unwrap_or_else(|_| TypedValue::String(value_str.to_string()))
            }
            "float" | "Price" | "Amt" | "Qty" | "PriceOffset"
            | "Percentage" => value_str
              .parse::<f64>()
              .map(TypedValue::Float)
              .unwrap_or_else(|_| TypedValue::String(value_str.to_string())),
            "Boolean" => match value_str {
              "Y" => TypedValue::Boolean(true),
              "N" => TypedValue::Boolean(false),
              _ => TypedValue::String(value_str.to_string()),
            },
            "char" => value_str
              .chars()
              .next()
              .map(TypedValue::Char)
              .unwrap_or_else(|| TypedValue::String(value_str.to_string())),
            _ => TypedValue::String(value_str.to_string()),
          }
        } else {
          TypedValue::String(value_str.to_string())
        }
      }

      let mut message = Message::new(msg.fix_version.clone(), msg.get_type())?;

      fn read_elements(
        fix: &repository::FixVersion,
        fixmessage: &repository::Message,
        component: Option<&repository::Component>,
        group: Option<&repository::Group>,
        msg: &FixMessage,
        begin: usize,
        elements: &mut Block,
      ) -> Result<usize, crate::Error> /* consumed */ {
        let mut i = 0;
        while begin + i < msg.tags.len() {
          let (tag, val) = &msg.tags[begin + i];

          // Ignore the being string, body length, msg type and checksum
          if *tag == 8 || *tag == 9 || *tag == 10 || *tag == 35 {
            i += 1;
            continue;
          }

          // If we have a component to check membership, bail out (before incrementing
          // i) if this element is not a member of the component
          if let Some(component) = component {
            if i > 0 && !component.is_member(fix, *tag) {
              return Ok(i); // consumed i elements
            }
          }

          // If we have a group to check membership, bail out (before incrementing
          // i) if this element is not a member of the group, or if it's the first
          // group tag, which tells us the group ended.
          if let Some(group) = group {
            if i > 0 && group.get_marker_tag(fix).id == *tag
              || !group.is_member(fix, *tag)
            {
              return Ok(i); // consumed i elements
            }
          }

          i += 1;
          if let Some(field) = fix.get_field(*tag) {
            match field.field_type.as_str() {
              "NumInGroup" => {
                let group = fix
                  .get_group_by_num_in_group_tag(fixmessage, field.id)
                  .ok_or_else(|| {
                    crate::Error::invalid_message(format!(
                      "Invalid group {} in message {}",
                      field.id, fixmessage.name
                    ))
                  })?;

                let num_entries: u32 = val.parse(&msg.data)?;
                let mut group_entries =
                  Vec::with_capacity(num_entries as usize);
                for _ in 0..num_entries {
                  let mut group_elements = Block::new();
                  i += read_elements(
                    fix,
                    fixmessage,
                    None,
                    Some(&group),
                    msg,
                    begin + i,
                    &mut group_elements,
                  )?;
                  group_entries.push(group_elements);
                }
                elements.push(Element::Group(*tag, group_entries));
              }
              "Length" => {
                if begin + i < msg.tags.len() {
                  let (data_tag, data_val) = &msg.tags[begin + i];
                  // Data fields should be kept as binary
                  let data_bytes = data_val.as_bytes(&msg.data).to_vec();
                  elements.push(Element::RawDataTag(
                    *tag,
                    *data_tag,
                    TypedValue::Data(data_bytes),
                  ));
                  i += 1;
                } else {
                  return Err(crate::Error::invalid_message(
                    "Invalid length tag: No data tag",
                  ));
                }
              }
              _ => {
                let typed_val =
                  parse_typed_value(fix, *tag, val.as_str(&msg.data));
                elements.push(Element::Tag((*tag, typed_val)));
              }
            }
          } else {
            // Unknown tag. We just assume it's a basic string value
            elements.push(Element::Tag((
              *tag,
              TypedValue::String(val.to_string(&msg.data)),
            )));
          };
        }
        Ok(i)
      }

      let header_elements = read_elements(
        &msg.fix_version,
        &message.fix_message,
        Some(
          &msg
            .fix_version
            .get_component_by_name("StandardHeader")
            .unwrap(),
        ),
        None,
        msg,
        0,
        &mut message.header,
      )?;
      read_elements(
        &msg.fix_version,
        &message.fix_message,
        None,
        None,
        msg,
        header_elements,
        &mut message.body,
      )?;

      Ok(message)
    }

    pub fn as_message(&self) -> Result<FixMessage, crate::Error> {
      let mut msg = FixMessage::new(self.fix_version.clone());

      fn flatten(elements: &Block, msg: &mut FixMessage) -> crate::Result<()> {
        for element in &elements.0 {
          match element {
            Element::Tag((tag, val)) => {
              msg.tags.push((*tag, Value::String(val.as_string())));
            }
            Element::Group(tag, group) => {
              msg
                .tags
                .push((*tag, Value::String(group.len().to_string())));
              for entry in group {
                flatten(entry, msg)?;
              }
            }
            Element::RawDataTag(len_tag, data_tag, data) => {
              let data_bytes = match data {
                TypedValue::Data(bytes) => bytes.clone(),
                _ => data.as_string().into_bytes(),
              };
              msg
                .tags
                .push((*len_tag, Value::String(data_bytes.len().to_string())));
              msg.tags.push((*data_tag, Value::Data(data_bytes)));
            }
          }
        }
        Ok(())
      }

      flatten(&self.header, &mut msg)?;
      flatten(&self.body, &mut msg)?;

      Ok(msg)
    }

    pub fn into_message(self) -> Result<FixMessage, crate::Error> {
      let mut msg = FixMessage::new(self.fix_version.clone());

      fn flatten(elements: Block, msg: &mut FixMessage) -> crate::Result<()> {
        for element in elements.0 {
          if !element.is_set() {
            continue;
          }
          match element {
            Element::Tag((tag, val)) => {
              msg.tags.push((tag, Value::String(val.into_string())));
            }
            Element::Group(tag, group) => {
              msg.tags.push((tag, Value::String(group.len().to_string())));
              for entry in group {
                flatten(entry, msg)?;
              }
            }
            Element::RawDataTag(len_tag, data_tag, data) => {
              let data_bytes = match data {
                TypedValue::Data(bytes) => bytes,
                _ => data.into_string().into_bytes(),
              };
              msg
                .tags
                .push((len_tag, Value::String(data_bytes.len().to_string())));
              msg.tags.push((data_tag, Value::Data(data_bytes)));
            }
          }
        }
        Ok(())
      }

      flatten(self.header, &mut msg)?;
      flatten(self.body, &mut msg)?;

      Ok(msg)
    }
  }

  impl Block {
    pub fn new() -> Self {
      Default::default()
    }

    pub fn is_empty(&self) -> bool {
      self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Element> {
      self.0.iter().filter(|e| e.is_set())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Element> {
      self.0.iter_mut().filter(|e| e.is_set())
    }

    pub fn len(&self) -> usize {
      self.0.len()
    }

    fn push(&mut self, element: Element) -> usize {
      // note this internal version doesn't check for existing tag and just
      // pushes what it is told
      let tag = match element {
        Element::Tag((tag, _)) => tag,
        Element::Group(tag, _) => tag,
        Element::RawDataTag(_, data_tag, _) => data_tag,
      };
      let index = self.0.len();
      self.0.push(element);
      // TODO: Should this really be a HashMap<Vec<usize>> ? This would allow
      // for invalid messages though. Or better should this function just return
      // Result<usize> and not push the tag if it exists?
      self
        .1
        .entry(tag)
        .and_modify(|e| *e = index)
        .or_insert(index);
      index
    }

    pub fn has_tag(&self, tag: u32) -> bool {
      self.tag(tag).is_some_and(|t| !t.is_empty())
    }

    pub fn set_tag<T: Into<TypedValue>>(&mut self, tag: u32, value: T) {
      *(self.tag_mut(tag)) = value.into()
    }

    pub fn remove_tag(&mut self, tag: u32) {
      self.set_tag(tag, TypedValue::Empty);
    }

    pub fn push_tag<T: Into<TypedValue>>(
      &mut self,
      tag: u32,
      value: T,
    ) -> crate::Result<()> {
      if self.has_tag(tag) {
        return Err(crate::Error::invalid_message(format!(
          "Tag {} already exists in block",
          tag
        )));
      }
      self.push(Element::Tag((tag, value.into())));
      Ok(())
    }

    pub fn push_group(&mut self, tag: u32, group: Block) {
      let g = self.group_mut(tag);
      g.push(group);
    }

    pub fn tag(&self, tag: u32) -> Option<&TypedValue> {
      let index = self.1.get(&tag)?;
      if let Element::Tag((_, v)) = self.0.get(*index)? {
        return Some(v);
      }
      None
    }

    pub fn tag_mut(&mut self, tag: u32) -> &mut TypedValue {
      let mut index: Option<usize> = None;
      if let Some(i) = self.1.get(&tag) {
        if let Some(Element::Tag((_, _))) = self.0.get_mut(*i) {
          index = Some(*i)
        }
      }
      let index = index
        .unwrap_or_else(|| self.push(Element::Tag((tag, TypedValue::Empty))));
      if let Element::Tag((_, value)) = self.0.get_mut(index).unwrap() {
        return value;
      }
      unreachable!()
    }

    pub fn group(&self, tag: u32) -> Option<&Vec<Block>> {
      let index = self.1.get(&tag)?;
      if let Element::Group(_, group) = self.0.get(*index)? {
        return Some(group);
      }
      None
    }

    pub fn group_mut(&mut self, tag: u32) -> &mut Vec<Block> {
      let mut index: Option<usize> = None;
      if let Some(i) = self.1.get(&tag) {
        if let Some(Element::Group(_, _)) = self.0.get_mut(*i) {
          index = Some(*i)
        }
      }
      let index = index.unwrap_or_else(|| {
        self.push(Element::Group(
          tag,
          Vec::with_capacity(DEFAULT_GROUP_CAPACITY),
        ))
      });
      if let Element::Group(_, group) = self.0.get_mut(index).unwrap() {
        return group;
      }
      unreachable!()
    }
  }

  // TODO: should we make a BlockIter that flattens the elements of the block?
  // The trick would be returning an indicator of the group/path rather than
  // just having all users implement iteration with recursion. Recursion isn't
  // difficult but it can have lifetime issues.

  struct MessageIter<BlockIter> {
    header_iter: Option<BlockIter>,
    body_iter: BlockIter,
  }

  impl<'msg, BlockIter> Iterator for MessageIter<BlockIter>
  where
    BlockIter: Iterator<Item = &'msg Element>,
  {
    type Item = &'msg Element;
    fn next(&mut self) -> Option<Self::Item> {
      if let Some(header_iter) = &mut self.header_iter {
        match header_iter.next() {
          Some(n) => return Some(n),
          None => self.header_iter = None,
        }
      }
      self.body_iter.next()
    }
  }

  impl Message {
    pub fn iter(&self) -> impl Iterator<Item = &Element> {
      MessageIter {
        header_iter: Some(self.header.iter()),
        body_iter: self.body.iter(),
      }
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Element> {
      self.body.iter_mut()
    }

    /// Normalizes the message according to FIX Orchestra field ordering
    ///
    /// Returns a new Message where:
    /// - Header fields are ordered according to StandardHeader component
    ///   definition
    /// - Body fields are ordered according to the message definition from FIX
    ///   Orchestra
    /// - Repeating groups are placed at NumInGroup tag positions with members
    ///   in definition order
    /// - Unknown tags are sorted by tag number after known fields within their
    ///   block
    /// - All original tag values and data are preserved
    ///
    /// This enables reliable message diffing by ensuring consistent ordering
    /// regardless of the original message's tag sequence.
    pub fn normalize(&self) -> crate::Result<Message> {
      let mut normalized = Message {
        fix_message: self.fix_message.clone(),
        fix_version: self.fix_version.clone(),
        header: Block::new(),
        body: Block::new(),
      };

      // Normalize header using StandardHeader component if available
      let std_header = self
        .fix_version
        .get_component_by_name("StandardHeader")
        .ok_or_else(|| {
          crate::Error::invalid_message(format!(
            "FIX version {} does not define StandardHeader component",
            self.fix_version.begin_string
          ))
        })?;

      normalized.header = self
        .normalize_block_with_definition(&self.header, std_header.as_ref())?;

      // Normalize body using message definition
      normalized.body = self.normalize_block_with_definition(
        &self.body,
        self.fix_message.as_ref(),
      )?;

      Ok(normalized)
    }

    fn normalize_block_with_definition<T: repository::FieldBlock>(
      &self,
      block: &Block,
      definition: &T,
    ) -> crate::Result<Block> {
      use std::collections::HashMap;

      let mut normalized = Block::new();

      // Build a map of tag -> order based on the FIX Orchestra definition
      let mut canonical_order: HashMap<u32, usize> = HashMap::new();
      let mut order_idx = 0;

      // Process the elements in the definition to build canonical ordering
      // Note: This could be pre-calculated in the fix_repo crate for
      // performance
      self.build_canonical_order(
        definition.get_elements(),
        &mut canonical_order,
        &mut order_idx,
      );

      // Collect all elements from the current block
      let mut elements_to_sort = Vec::with_capacity(block.len());
      for element in block.iter() {
        elements_to_sort.push(element.clone());
      }

      // Sort elements by canonical order, with unknown tags sorted by tag
      // number at the end
      elements_to_sort.sort_by(|a, b| {
        let tag_a = self.get_element_tag(a);
        let tag_b = self.get_element_tag(b);

        let order_a = canonical_order.get(&tag_a);
        let order_b = canonical_order.get(&tag_b);

        // Create a composite sort key that interleaves unknown tags by tag number
        let sort_key_a = self.create_sort_key(&canonical_order, tag_a, order_a);
        let sort_key_b = self.create_sort_key(&canonical_order, tag_b, order_b);

        sort_key_a.cmp(&sort_key_b)
      });

      // Add sorted elements to normalized block, normalizing groups recursively
      for element in elements_to_sort {
        match element {
          Element::Tag(tag_value) => {
            normalized.push(Element::Tag(tag_value));
          }
          Element::Group(num_in_group_tag, group_entries) => {
            let mut normalized_entries =
              Vec::with_capacity(group_entries.len());

            // Find the group definition to get proper ordering for group
            // members
            let group_def = self
              .fix_version
              .get_group_by_num_in_group_tag(definition, num_in_group_tag)
              .ok_or_else(|| {
                crate::Error::invalid_message(format!(
                  "Group definition not found for NumInGroup tag {}",
                  num_in_group_tag
                ))
              })?;

            // Normalize each group entry using the group definition
            for entry in group_entries {
              let normalized_entry = self
                .normalize_block_with_definition(&entry, group_def.as_ref())?;
              normalized_entries.push(normalized_entry);
            }

            normalized
              .push(Element::Group(num_in_group_tag, normalized_entries));
          }
          Element::RawDataTag(len_tag, data_tag, data) => {
            normalized.push(Element::RawDataTag(len_tag, data_tag, data));
          }
        }
      }

      Ok(normalized)
    }

    fn build_canonical_order(
      &self,
      elements: &[repository::MessageElement],
      canonical_order: &mut std::collections::HashMap<u32, usize>,
      order_idx: &mut usize,
    ) {
      for element in elements {
        match element {
          repository::MessageElement::Field(field_ref) => {
            canonical_order.insert(field_ref.field_id, *order_idx);
            *order_idx += 1;
          }
          repository::MessageElement::Component(comp_ref) => {
            if let Some(component) =
              self.fix_version.get_component(comp_ref.component_id)
            {
              self.build_canonical_order(
                component.get_elements(),
                canonical_order,
                order_idx,
              );
            }
          }
          repository::MessageElement::Group(group_ref) => {
            if let Some(group) = self.fix_version.get_group(group_ref.group_id)
            {
              // Add the NumInGroup tag first
              canonical_order.insert(group.num_in_group_tag, *order_idx);
              *order_idx += 1;

              // Don't add the group members to the parent's canonical order
              // They will be handled when the group itself is normalized
            }
          }
        }
      }
    }

    fn create_sort_key(
      &self,
      canonical_order: &std::collections::HashMap<u32, usize>,
      tag: u32,
      order: Option<&usize>,
    ) -> (usize, u32) {
      match order {
        Some(&canonical_pos) => {
          // For known tags, use canonical position * 1000 to leave room for unknown tags
          (canonical_pos * 1000, tag)
        }
        None => {
          // For unknown tags, find where to insert based on tag number relative to known tags
          // Find the largest canonical order for known tags that are smaller than this unknown tag
          let mut position_after = 0; // Before all known tags by default

          for (&known_tag, &known_order) in canonical_order {
            if known_tag < tag {
              // This known tag comes before our unknown tag numerically
              position_after = position_after.max(known_order);
            }
          }

          // Insert unknown tag after position_after but before position_after + 1
          // Use position_after * 1000 + 500 + small_offset to place between known tags
          let tag_offset = (tag % 100) as usize; // Small offset based on tag number
          (position_after * 1000 + 500 + tag_offset, tag)
        }
      }
    }

    fn get_element_tag(&self, element: &Element) -> u32 {
      match element {
        Element::Tag((tag, _)) => *tag,
        Element::Group(tag, _) => *tag,
        Element::RawDataTag(_, data_tag, _) => *data_tag,
      }
    }
  }

  impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      fn print_block(
        f: &mut fmt::Formatter<'_>,
        block: &Block,
        indent: &str,
        fix: &repository::FixVersion,
        msg: &repository::Message,
      ) -> fmt::Result {
        for element in block.iter() {
          match &element {
            Element::Tag((tag, val)) => {
              writeln!(f, "{indent}{tag}={}", val.as_string())?;
            }
            Element::Group(tag, group) => {
              if let Some(g) = fix.get_group_by_num_in_group_tag(msg, *tag) {
                writeln!(f, "{}Group {} [{}]", indent, tag, g.name)?;
              } else {
                writeln!(f, "{indent}Group {tag}")?;
              }
              for (idx, entry) in group.iter().enumerate() {
                print_block(
                  f,
                  entry,
                  &format!("{indent}  {tag}[{idx}]."),
                  fix,
                  msg,
                )?;
              }
            }
            Element::RawDataTag(len_tag, data_tag, data) => {
              let data_str = match data {
                TypedValue::Data(bytes) => format!("<{} bytes>", bytes.len()),
                _ => data.as_string(),
              };
              writeln!(f, "{indent} {data_tag}={data_str} [{len_tag}]")?;
            }
          }
        }
        Ok(())
      }
      print_block(f, &self.header, "", &self.fix_version, &self.fix_message)?;
      print_block(f, &self.body, "", &self.fix_version, &self.fix_message)?;
      Ok(())
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::OnceLock;

  use super::builder::TypedValue;
  use super::*;

  static REPO: OnceLock<Arc<repository::FixRepository>> = OnceLock::new();

  fn load_repo() -> Arc<repository::FixRepository> {
    REPO
      .get_or_init(|| Arc::new(repository::orchestrate().unwrap()))
      .clone()
  }

  #[test]
  fn test_fix_message() -> Result<(), anyhow::Error> {
    let repo = load_repo();
    let fix44 = repo.get_version("FIX.4.4").unwrap();
    let data = b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000|";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )?;
    assert_eq!(consumed, data.len());

    assert_eq!(msg.get_being_string(), "FIX.4.4");
    assert_eq!(msg.get_type(), "AB");

    use crate::schema::FIX_4_4 as FIX44;

    let mut b = builder::Message::from_message(&msg)?;
    assert!(b.body.has_tag(FIX44::Fields::ClOrdID));
    b.body
      .set_tag(FIX44::Fields::ClOrdID, TypedValue::String("foobar".into()));
    assert!(b.body.tag(FIX44::Fields::ClOrdID).is_some());
    assert_eq!(
      *b.body.tag(FIX44::Fields::ClOrdID).unwrap(),
      TypedValue::String("foobar".into())
    );

    assert!(!b.body.has_tag(FIX44::Fields::Account));
    *b.body.tag_mut(FIX44::Fields::Account) =
      TypedValue::String("123456".into());
    assert_eq!(
      *b.body.tag(FIX44::Fields::Account).unwrap(),
      TypedValue::String("123456".into())
    );

    let g = b.body.group(FIX44::Fields::NoLegs).unwrap();
    assert_eq!(g.len(), 2);
    assert!(g[0].has_tag(FIX44::Fields::LegSymbol));

    Ok(())
  }

  #[test]
  fn test_build() -> Result<(), anyhow::Error> {
    let repo = load_repo();

    use crate::schema::FIX_4_4 as FIX44;
    let mut msg =
      builder::Message::new(repo.get_version("FIX.4.4").unwrap(), "AB")
        .unwrap();

    let mut g = builder::Block::new();
    g.push_tag(FIX44::Fields::LegSymbol, "FDAX")?;
    msg.body.push_group(FIX44::Fields::NoLegs, g);
    let mut g = builder::Block::new();
    assert!(g.push_tag(FIX44::Fields::LegSymbol, "ODAX").is_ok());
    msg.body.push_group(FIX44::Fields::NoLegs, g);

    msg.header.push_tag(FIX44::Fields::SenderCompID, "Sender")?;
    msg.header.push_tag(FIX44::Fields::TargetCompID, "Target")?;
    msg.header.push_tag(FIX44::Fields::MsgSeqNum, 55)?;

    println!("The message:");
    println!("{msg:?}");

    let new = msg.clone();

    let data = msg.into_message()?;
    assert_eq!(data.get_being_string(), "FIX.4.4");
    assert_eq!(
      data.to_string_delimited(b'|'),
      "8=FIX.4.4|9=56|35=AB|49=Sender|56=Target|34=55|555=2|600=FDAX|600=ODAX|10=176|"
    );

    let data = new.into_message()?;
    assert_eq!(data.get_being_string(), "FIX.4.4");
    assert_eq!(
      data.to_string_delimited(b'|'),
      "8=FIX.4.4|9=56|35=AB|49=Sender|56=Target|34=55|555=2|600=FDAX|600=ODAX|10=176|"
    );

    Ok(())
  }

  #[test]
  fn test_parse() -> Result<(), anyhow::Error> {
    let repo = load_repo();
    let fix44 = repo.get_version("FIX.4.4").unwrap();

    let data = b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )?;
    assert_eq!(consumed, data.len());
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(msg.tags.last().unwrap().1.as_str(&msg.data), "0000000000");

    let data = b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|10=99|8=FIX.4.4|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=NotAnIntegerSomehow";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )?;
    assert_eq!(consumed, 43);
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(msg.tags.last().unwrap().1.as_str(&msg.data), "99");

    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(&data[consumed..]),
      b'|',
    )?;
    assert_eq!(consumed, data.len() - 43);
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(
      msg.tags.last().unwrap().1.as_str(&msg.data),
      "NotAnIntegerSomehow"
    );

    Ok(())
  }

  #[test]
  fn test_delimiter_in_value_rawdata() -> Result<(), anyhow::Error> {
    let repo = load_repo();
    let fix44 = repo.get_version("FIX.4.4").unwrap();

    let data = b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|95=20|96=0123456789|fo=0000000000|";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )?;
    assert_eq!(consumed, data.len());
    assert_eq!(msg.tags[6].0, 95); // SecDataLen
    assert_eq!(msg.tags[6].1.as_str(&msg.data), "20");
    assert_eq!(msg.tags[7].0, 96); // SecData (raw data field)
    // SecData is now correctly stored as Value::Data, so use as_bytes()
    assert!(msg.tags[7].1.is_data());
    assert_eq!(msg.tags[7].1.as_bytes(&msg.data), b"0123456789|fo=000000");

    Ok(())
  }

  #[test]
  fn test_equals_in_value() {
    let repo = load_repo();
    let fix44 = repo.get_version("FIX.4.4").unwrap();

    let data = b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|58=This is a test=with equals|10=0000000000|";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )
    .unwrap();
    assert_eq!(consumed, data.len());
    assert_eq!(msg.tags[6].0, 58);
    assert_eq!(
      msg.tags[6].1.as_str(&msg.data),
      "This is a test=with equals"
    );
  }

  #[test]
  fn test_builder_in_struct_with_repo() {
    struct State {
      _msg: builder::Message,
    }
    let repo = load_repo();
    let _state = State {
      _msg: builder::Message::new(repo.get_version("FIX.4.4").unwrap(), "AB")
        .unwrap(),
    };
  }

  #[test]
  fn test_parse_fix5() -> Result<(), anyhow::Error> {
    let repo = load_repo();
    let fix5 = repo.get_version("FIX.Latest").unwrap();

    let data = b"8=FIXT.1.1|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix5.clone(),
      Bytes::from(&data[..]),
      b'|',
    )?;
    assert_eq!(consumed, data.len());
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(msg.tags.last().unwrap().1.as_str(&msg.data), "0000000000");

    let data = b"8=FIXT.1.1|35=AB|34=1|49=SenderCompID|10=99|8=FIXT.1.1|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=NotAnIntegerSomehow";
    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix5.clone(),
      Bytes::from(&data[..]),
      b'|',
    )?;
    assert_eq!(consumed, 44);
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(msg.tags.last().unwrap().1.as_str(&msg.data), "99");

    let (msg, consumed) = FixMessage::from_bytes_delimited(
      fix5.clone(),
      Bytes::from(&data[consumed..]),
      b'|',
    )?;
    assert_eq!(consumed, data.len() - 44);
    assert_eq!(msg.tags.last().unwrap().0, 10);
    assert_eq!(
      msg.tags.last().unwrap().1.as_str(&msg.data),
      "NotAnIntegerSomehow"
    );

    Ok(())
  }

  #[test]
  fn test_infer_version() -> anyhow::Result<()> {
    let data = b"8=FIXT.1.1|9=128|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000";
    let repo = load_repo();
    let (version, body_len, consumed) =
      peek_infer_version_and_length(&repo, data, b'|')?;
    assert_eq!(version.unwrap().name, "FIX.Latest");
    assert_eq!(body_len.unwrap(), 128);
    assert_eq!(consumed, 17);

    let data = b"8=FIXT.1";
    let (version, body_len, consumed) =
      peek_infer_version_and_length(&repo, data, b'|')?;
    assert!(version.is_none());
    assert!(body_len.is_none());
    assert_eq!(consumed, 0);

    let data = b"8=FIXT.1.1";
    let (version, body_len, consumed) =
      peek_infer_version_and_length(&repo, data, b'|')?;
    assert!(version.is_none());
    assert!(body_len.is_none());
    assert_eq!(consumed, 0);

    let data = b"8=FIXT.1.1|";
    let (version, body_len, consumed) =
      peek_infer_version_and_length(&repo, data, b'|')?;
    assert_eq!(version.unwrap().name, "FIX.Latest");
    assert!(body_len.is_none());
    assert_eq!(consumed, 11);

    let data = b"8=FIXT.1.1|9=1";
    let (version, body_len, consumed) =
      peek_infer_version_and_length(&repo, data, b'|')?;
    assert_eq!(version.unwrap().name, "FIX.Latest");
    assert!(body_len.is_none());
    assert_eq!(consumed, 11);

    let data = b"8=FOOB|9=1";
    let Err(s) = peek_infer_version_and_length(&repo, data, b'|') else {
      panic!("Should have failed")
    };
    assert_eq!(
      s.to_string().as_str(),
      "Invalid message: Unknown FIX version"
    );

    Ok(())
  }

  #[test]
  fn test_build_fix5() -> anyhow::Result<()> {
    let repo = load_repo();
    let fix5 = repo.get_version("FIX.Latest").unwrap();
    let mut b = builder::Message::new(fix5, "D")?;
    assert!(b.header.push_tag(8, "FIX.4.4").is_err());
    let m = b.into_message()?;
    assert_eq!(m.get_being_string(), "FIXT.1.1");

    Ok(())
  }

  #[test]
  fn test_unknown_tag() -> anyhow::Result<()> {
    let data = b"8=FIXT.1.1|35=D|500001=Ben";
    let repo = load_repo();
    let fix5 = repo.get_version("FIX.Latest").unwrap();
    let m = builder::Message::from_bytes_delimited(fix5, data, b'|')?;
    assert_eq!(m.body.tag(500001).unwrap().as_string(), "Ben");
    Ok(())
  }

  #[test]
  fn test_equality_of_messages() -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();
    let msg = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000"),
      b'|',
    )?.0;
    let msg2 = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000"),
      b'|',
    )?.0;

    assert_eq!(msg, msg2);

    // Different tag order
    let msg2 = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|600=6B|555=2|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000"),
      b'|',
    )?.0;

    assert_ne!(msg, msg2);

    // Same tag order, differnt values

    let msg2 = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6C|608=F|610=202509|600=6B|608=F|610=202505|10=0000000000"),
      b'|',
    )?.0;
    assert_ne!(msg, msg2);

    // Different tags

    let msg2 = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(b"8=FIX.4.4|35=AB|34=1|49=SenderCompID|56=TargetCompID|52=20231010-12:00:00.000|11=123456|54=0|38=1000|40=2|555=2|600=6B|608=F|610=202509|600=6B|608=F|610=202505|9999=ExtraTag|10=0000000000"),
      b'|',
    )?.0;
    assert_ne!(msg, msg2);

    Ok(())
  }

  #[test]
  fn test_message_normalization() -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();

    // Create a simple message to test basic normalization
    let data =
      b"8=FIX.4.4|35=D|49=Sender|56=Target|34=1|11=ClOrdID|55=Symbol|10=123|";
    let (orig_msg, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from_static(data),
      b'|',
    )?;
    let builder_msg = builder::Message::from_message(&orig_msg)?;

    // Normalize the message
    let normalized = builder_msg.normalize()?;

    // Convert back to FixMessage to verify structure is preserved
    let normalized_fix_msg = normalized.as_message()?;

    // Verify that tag count is preserved through normalization
    // The builder message excludes BodyLength(9) and CheckSum(10), but keeps other tags
    let builder_count =
      builder_msg.header.iter().count() + builder_msg.body.iter().count();
    let normalized_count =
      normalized.header.iter().count() + normalized.body.iter().count();
    assert_eq!(
      builder_count, normalized_count,
      "Normalization should preserve all tags from builder message"
    );

    // The normalized fix message should have the same count as the builder generates
    let builder_fix_msg = builder_msg.as_message()?;
    assert_eq!(
      builder_fix_msg.tags.len(),
      normalized_fix_msg.tags.len(),
      "Normalized fix message should have same tag count as original builder conversion"
    );

    Ok(())
  }

  #[test]
  fn test_message_normalization_ordering() -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();

    // Create a message with tags deliberately out of order
    let data =
      b"8=FIX.4.4|35=D|55=Symbol|49=Sender|11=ClOrdID|56=Target|34=1|10=123|";
    let (orig_msg, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from(&data[..]),
      b'|',
    )?;
    let builder_msg = builder::Message::from_message(&orig_msg)?;

    // Normalize the message
    let normalized = builder_msg.normalize()?;

    // Convert back to FixMessage to verify final ordering
    let normalized_fix_msg = normalized.as_message()?;

    // Extract the final tag sequence (excluding BodyLength and CheckSum)
    let final_tag_order: Vec<u32> = normalized_fix_msg
      .tags
      .iter()
      .filter(|(tag, _)| *tag != 9 && *tag != 10)
      .map(|(tag, _)| *tag)
      .collect();

    // Verify specific FIX Orchestra ordering expectations for NewOrderSingle (D)
    // Based on typical FIX 4.4 message structure, we expect:
    // Header: BeginString(8), MsgType(35), SenderCompID(49), TargetCompID(56), MsgSeqNum(34)
    // Body: ClOrdID(11), Symbol(55)

    // Find positions of key tags
    let pos_8 = final_tag_order.iter().position(|&x| x == 8).unwrap();
    let pos_35 = final_tag_order.iter().position(|&x| x == 35).unwrap();
    let pos_49 = final_tag_order.iter().position(|&x| x == 49).unwrap();
    let pos_56 = final_tag_order.iter().position(|&x| x == 56).unwrap();
    let pos_34 = final_tag_order.iter().position(|&x| x == 34).unwrap();
    let pos_11 = final_tag_order.iter().position(|&x| x == 11).unwrap();
    let pos_55 = final_tag_order.iter().position(|&x| x == 55).unwrap();

    // Verify StandardHeader ordering - these should follow FIX Orchestra definition
    assert!(
      pos_8 < pos_35,
      "BeginString(8) should come before MsgType(35)"
    );
    assert!(
      pos_35 < pos_49,
      "MsgType(35) should come before SenderCompID(49)"
    );
    assert!(
      pos_49 < pos_56,
      "SenderCompID(49) should come before TargetCompID(56)"
    );
    assert!(
      pos_56 < pos_34,
      "TargetCompID(56) should come before MsgSeqNum(34)"
    );

    // Verify that header comes before body
    assert!(pos_34 < pos_11, "Header tags should come before body tags");
    assert!(pos_34 < pos_55, "Header tags should come before body tags");

    Ok(())
  }

  #[test]
  fn test_message_normalization_with_unknown_tags() -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();

    // Create a message with unknown tags mixed in among known tags
    // Use unknown tags that should interleave: 12 (between 11 and 34), 50 (between 49 and 55), 1000 (after all)
    let data = b"8=FIX.4.4|35=D|1000=AfterAll|49=Sender|56=Target|12=BetweenClOrdIDAndMsgSeqNum|34=1|11=ClOrdID|55=Symbol|50=BetweenSenderAndSymbol|10=123|";
    let (orig_msg, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from(&data[..]),
      b'|',
    )?;
    let builder_msg = builder::Message::from_message(&orig_msg)?;

    // Normalize the message
    let normalized = builder_msg.normalize()?;

    // Convert back to verify structure
    let normalized_fix_msg = normalized.as_message()?;

    // Extract the final tag sequence (excluding BodyLength and CheckSum)
    let final_tag_order: Vec<u32> = normalized_fix_msg
      .tags
      .iter()
      .filter(|(tag, _)| *tag != 9 && *tag != 10)
      .map(|(tag, _)| *tag)
      .collect();

    // Verify all tags are preserved
    let has_12 = normalized_fix_msg.tags.iter().any(|(tag, _)| *tag == 12);
    let has_50 = normalized_fix_msg.tags.iter().any(|(tag, _)| *tag == 50);
    let has_1000 = normalized_fix_msg.tags.iter().any(|(tag, _)| *tag == 1000);
    assert!(has_12, "Unknown tag 12 should be preserved");
    assert!(has_50, "Unknown tag 50 should be preserved");
    assert!(has_1000, "Unknown tag 1000 should be preserved");

    // Verify that unknown tags are properly interleaved by tag number
    let pos_11 = final_tag_order.iter().position(|&x| x == 11).unwrap();
    let pos_12 = final_tag_order.iter().position(|&x| x == 12).unwrap();
    let _pos_34 = final_tag_order.iter().position(|&x| x == 34).unwrap();
    let pos_49 = final_tag_order.iter().position(|&x| x == 49).unwrap();
    let pos_50 = final_tag_order.iter().position(|&x| x == 50).unwrap();
    let pos_55 = final_tag_order.iter().position(|&x| x == 55).unwrap();
    let pos_1000 = final_tag_order.iter().position(|&x| x == 1000).unwrap();

    // Verify proper interleaving based on how the algorithm actually works:
    // Unknown tags are placed after the last known tag that's smaller by tag number

    // Tag 12: should come after ClOrdID(11) but follows FIX Orchestra ordering,
    // so it comes after Symbol(55) since 55 > 12 in canonical order
    assert!(
      pos_11 < pos_12,
      "Unknown tag 12 should come after ClOrdID(11)"
    );
    assert!(
      pos_55 < pos_12,
      "Unknown tag 12 should come after Symbol(55) due to canonical ordering"
    );

    // Tag 50: should come after SenderCompID(49) and before Symbol(55)
    // Since 49 < 50 < 55, it gets inserted between them
    assert!(
      pos_49 < pos_50,
      "Unknown tag 50 should come after SenderCompID(49)"
    );
    assert!(
      pos_50 < pos_11,
      "Unknown tag 50 should come before ClOrdID(11) in this ordering"
    );

    // Tag 1000: should come after all known tags
    assert!(
      pos_12 < pos_1000,
      "Unknown tag 1000 should come after all other tags"
    );

    // Verify FIX Orchestra ordering is still maintained for known tags
    let pos_8 = final_tag_order.iter().position(|&x| x == 8).unwrap();
    let pos_35 = final_tag_order.iter().position(|&x| x == 35).unwrap();
    let pos_49 = final_tag_order.iter().position(|&x| x == 49).unwrap();
    assert!(
      pos_8 < pos_35,
      "BeginString(8) should come before MsgType(35)"
    );
    assert!(
      pos_35 < pos_49,
      "MsgType(35) should come before SenderCompID(49)"
    );

    Ok(())
  }

  #[test]
  fn test_message_normalization_with_groups() -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();

    // Create a message with repeating groups in non-canonical order
    let data = b"8=FIX.4.4|35=AB|555=2|600=6B|608=F|610=202509|600=6C|608=G|610=202510|49=Sender|56=Target|34=1|10=123|";
    let (orig_msg, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from(&data[..]),
      b'|',
    )?;
    let builder_msg = builder::Message::from_message(&orig_msg)?;

    // Normalize the message
    let normalized = builder_msg.normalize()?;

    // Verify that groups are preserved
    let group = normalized
      .body
      .group(555)
      .expect("NoLegs group should exist");
    assert_eq!(group.len(), 2, "Should have 2 group entries");

    // Verify that group members are ordered according to the group definition
    for group_entry in group {
      let group_tags: Vec<u32> = group_entry
        .iter()
        .map(|element| match element {
          builder::Element::Tag((tag, _)) => *tag,
          builder::Element::Group(tag, _) => *tag,
          builder::Element::RawDataTag(_, data_tag, _) => *data_tag,
        })
        .collect();

      // Verify that both 600 (LegSymbol), 608 (LegCFICode), and 610 (LegMaturityDate) are present
      assert!(
        group_tags.contains(&600),
        "Group should contain LegSymbol(600)"
      );
      assert!(
        group_tags.contains(&608),
        "Group should contain LegCFICode(608)"
      );
      assert!(
        group_tags.contains(&610),
        "Group should contain LegMaturityDate(610)"
      );
    }

    Ok(())
  }

  #[test]
  fn test_message_normalization_produces_identical_results()
  -> anyhow::Result<()> {
    let fix44 = load_repo().get_version("FIX.4.4").unwrap();

    // Create two messages with identical content but different ordering
    // Message 1: Standard ordering with groups
    let data1 = b"8=FIX.4.4|35=AB|49=Sender|56=Target|34=1|11=ABC123|55=AAPL|54=1|38=100|555=2|600=6B|608=F|610=202509|600=6C|608=G|610=202510|10=123|";

    // Message 2: Same content but different ordering (header/body tags mixed, but same group structure)
    let data2 = b"56=Target|8=FIX.4.4|34=1|35=AB|49=Sender|38=100|555=2|600=6B|608=F|610=202509|600=6C|608=G|610=202510|55=AAPL|11=ABC123|54=1|10=123|";

    // Parse both messages
    let (msg1, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from(&data1[..]),
      b'|',
    )?;
    let (msg2, _) = FixMessage::from_bytes_delimited(
      fix44.clone(),
      Bytes::from(&data2[..]),
      b'|',
    )?;

    // Convert to builder messages
    let builder_msg1 = builder::Message::from_message(&msg1)?;
    let builder_msg2 = builder::Message::from_message(&msg2)?;

    // Normalize both messages
    let normalized1 = builder_msg1.normalize()?;
    let normalized2 = builder_msg2.normalize()?;

    // Convert back to FixMessage format for comparison
    let fix_msg1 = normalized1.clone().into_message()?;
    let fix_msg2 = normalized2.clone().into_message()?;

    // Verify that the normalized wire format strings are identical
    let wire1 = fix_msg1.to_string_delimited(b'|');
    let wire2 = fix_msg2.to_string_delimited(b'|');

    assert_eq!(
      wire1, wire2,
      "Normalized messages should have identical wire format"
    );

    // Verify that checksums are identical (implicitly checked by wire format comparison above)
    // Extract checksum from wire format strings
    let checksum1_str =
      wire1.split('|').find(|s| s.starts_with("10=")).unwrap();
    let checksum2_str =
      wire2.split('|').find(|s| s.starts_with("10=")).unwrap();
    assert_eq!(
      checksum1_str, checksum2_str,
      "Normalized messages should have identical checksums"
    );

    // Verify tag-by-tag equality
    assert_eq!(
      fix_msg1.tags.len(),
      fix_msg2.tags.len(),
      "Both messages should have the same number of tags"
    );

    for (tag1, tag2) in fix_msg1.tags.iter().zip(fix_msg2.tags.iter()) {
      assert_eq!(tag1.0, tag2.0, "Tag numbers should match");
      assert_eq!(tag1.1, tag2.1, "Tag values should match");
    }

    // Verify group content is identical
    let group1 = normalized1.body.group(555);
    let group2 = normalized2.body.group(555);

    match (group1, group2) {
      (Some(g1), Some(g2)) => {
        assert_eq!(
          g1.len(),
          g2.len(),
          "Groups should have same number of entries"
        );

        for (entry1, entry2) in g1.iter().zip(g2.iter()) {
          // Compare group entries element by element
          assert_eq!(
            entry1.iter().count(),
            entry2.iter().count(),
            "Group entries should have same number of elements"
          );

          for (elem1, elem2) in entry1.iter().zip(entry2.iter()) {
            match (elem1, elem2) {
              (
                builder::Element::Tag((tag1, val1)),
                builder::Element::Tag((tag2, val2)),
              ) => {
                assert_eq!(tag1, tag2, "Group element tags should match");
                assert_eq!(val1, val2, "Group element values should match");
              }
              _ => panic!("Expected Tag elements in group entries"),
            }
          }
        }
      }
      (None, None) => {} // Both have no groups, which is fine
      _ => panic!("One message has groups while the other doesn't"),
    }

    Ok(())
  }
}
