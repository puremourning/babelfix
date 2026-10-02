//! Typed fields: what a tag's value decodes to, and what can be written to it.
//!
//! Every field constant in `babelfix-schema`'s `fields` module (re-exported as
//! `babelfix::schema::fields`) is a [`Field<M>`], where `M` is a *datatype
//! marker* from [`datatypes`]. The marker
//! says how the value's bytes decode ([`FieldType::Value`]) and, through
//! [`ToFix`] and [`FromFix`] impls keyed on it, what may be written to the field
//! and what it may be converted to:
//!
//! ```ignore
//! let px: Option<Decimal> = body.get(Price)?;      // Field<datatypes::Price>: the text, validated
//! let px: Option<Dec19>   = body.get_as(Price)?;   // FromFix<datatypes::Price> for Dec19
//! b.set(OrderQty, udec!(100));                     // ToFix<datatypes::Qty> for UDec19
//! ```
//!
//! Decoding is lazy: parsing a message only finds where each value is, and a
//! value is decoded when it is asked for. The decoded forms borrow from the
//! message — [`FixStr`], [`Decimal`] and the date and time views are views of
//! the value's bytes, not copies.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use super::error::ValueError;

/// How a datatype's values decode.
pub trait FieldType: 'static {
  /// The decoded value, usually borrowing from the message.
  type Value<'a>: Copy;

  /// Decode a value from its bytes, which are never empty.
  fn decode(raw: &[u8]) -> Result<Self::Value<'_>, ValueError>;
}

/// Writes a value of datatype `M`.
pub trait ToFix<M: FieldType> {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError>;
}

/// Converts a decoded value of datatype `M` into `Self`.
pub trait FromFix<M: FieldType>: Sized {
  fn from_fix(v: M::Value<'_>) -> Result<Self, ValueError>;
}

/// Anything that names a tag: a typed [`Field`], a [`GroupField`], or a bare
/// tag number.
pub trait Tag: Copy {
  fn tag(self) -> u32;
}

impl Tag for u32 {
  fn tag(self) -> u32 {
    self
  }
}

/// A tag whose value is of datatype `M`.
pub struct Field<M> {
  tag: u32,
  _m: PhantomData<fn() -> M>,
}

impl<M> Field<M> {
  pub const fn new(tag: u32) -> Self {
    Self {
      tag,
      _m: PhantomData,
    }
  }

  pub const fn tag(&self) -> u32 {
    self.tag
  }
}

impl<M> Clone for Field<M> {
  fn clone(&self) -> Self {
    *self
  }
}
impl<M> Copy for Field<M> {}

impl<M> fmt::Debug for Field<M> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Field({})", self.tag)
  }
}

impl<M> Tag for Field<M> {
  fn tag(self) -> u32 {
    self.tag
  }
}

/// A repeating group, named by its NumInGroup tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupField {
  tag: u32,
}

impl GroupField {
  pub const fn new(tag: u32) -> Self {
    Self { tag }
  }

  pub const fn tag(&self) -> u32 {
    self.tag
  }
}

impl Tag for GroupField {
  fn tag(self) -> u32 {
    self.tag
  }
}

/// A `MsgType(35)` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MsgType(&'static str);

impl MsgType {
  pub const fn new(msg_type: &'static str) -> Self {
    Self(msg_type)
  }

  pub const fn as_str(&self) -> &'static str {
    self.0
  }
}

impl AsRef<[u8]> for MsgType {
  fn as_ref(&self) -> &[u8] {
    self.0.as_bytes()
  }
}

impl PartialEq<str> for MsgType {
  fn eq(&self, other: &str) -> bool {
    self.0 == other
  }
}

impl PartialEq<&str> for MsgType {
  fn eq(&self, other: &&str) -> bool {
    self.0 == *other
  }
}

/// Where a [`ToFix`] impl writes its value: the message's byte arena.
pub struct ValueWriter<'a> {
  buf: &'a mut Vec<u8>,
}

impl<'a> ValueWriter<'a> {
  pub(crate) fn new(buf: &'a mut Vec<u8>) -> Self {
    Self { buf }
  }

  /// Append bytes verbatim.
  pub fn put(&mut self, bytes: &[u8]) {
    self.buf.extend_from_slice(bytes);
  }

  pub fn put_u8(&mut self, b: u8) {
    self.buf.push(b);
  }

  /// Append an unsigned integer in decimal.
  pub fn put_uint(&mut self, mut v: u64) {
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    loop {
      i -= 1;
      digits[i] = b'0' + (v % 10) as u8;
      v /= 10;
      if v == 0 {
        break;
      }
    }
    self.put(&digits[i..]);
  }

  /// Append a signed integer in decimal.
  pub fn put_int(&mut self, v: i64) {
    if v < 0 {
      self.put_u8(b'-');
    }
    self.put_uint(v.unsigned_abs());
  }

  /// Append text, encoding it as Latin-1 (TagValue §4.1). ASCII, the
  /// overwhelmingly common case, is a plain copy.
  pub fn put_str(&mut self, s: &str) -> Result<(), ValueError> {
    if s.is_ascii() {
      self.put(s.as_bytes());
      return Ok(());
    }
    for c in s.chars() {
      let c = u32::from(c);
      if c > 0xFF {
        return Err(ValueError::Unrepresentable);
      }
      self.put_u8(c as u8);
    }
    Ok(())
  }
}

/// The datatype markers. Each names a FIX datatype and is the `M` in a
/// [`Field<M>`]. Codeset markers are generated alongside their enums in
/// `babelfix-schema`'s `codesets` module.
pub mod datatypes {
  /// String, and the many datatypes derived from it: Currency, Exchange,
  /// Country, MultipleCharValue, MonthYear, TZTimestamp, XID, ...
  pub enum Str {}
  pub enum Char {}
  pub enum Boolean {}
  /// `int`: signed.
  pub enum Int {}
  /// SeqNum, NumInGroup, DayOfMonth, TagNum, and `Length` when it is not a data
  /// field's length (`BodyLength`).
  pub enum UInt {}
  pub enum Float {}
  pub enum Qty {}
  pub enum Price {}
  pub enum PriceOffset {}
  pub enum Amt {}
  pub enum Percentage {}
  pub enum UtcTimestamp {}
  pub enum UtcDateOnly {}
  pub enum UtcTimeOnly {}
  pub enum LocalMktDate {}
  pub enum LocalMktTime {}
  /// `data` and `XMLData`: raw bytes, which may contain anything.
  pub enum Data {}
  /// The Length field of a data field. Readable; never set directly — it is
  /// written along with its data field.
  pub enum DataLength {}
}

use datatypes::*;

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// A string value, borrowed from the message.
///
/// FIX strings are Latin-1 (TagValue §4.1), not UTF-8, so this is a view of
/// bytes rather than a `&str`. In practice nearly every value is ASCII, which
/// [`as_ascii`](Self::as_ascii) and [`to_str`](Self::to_str) hand out without
/// copying.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FixStr<'a>(&'a [u8]);

impl<'a> FixStr<'a> {
  pub const fn new(bytes: &'a [u8]) -> Self {
    Self(bytes)
  }

  pub const fn as_bytes(&self) -> &'a [u8] {
    self.0
  }

  pub const fn len(&self) -> usize {
    self.0.len()
  }

  pub const fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// The value as `&str`, if it is ASCII.
  pub fn as_ascii(&self) -> Option<&'a str> {
    if self.0.is_ascii() {
      std::str::from_utf8(self.0).ok()
    } else {
      None
    }
  }

  /// The value as text: borrowed if ASCII, otherwise decoded from Latin-1.
  pub fn to_str(&self) -> Cow<'a, str> {
    match self.as_ascii() {
      Some(s) => Cow::Borrowed(s),
      None => Cow::Owned(self.0.iter().map(|&b| char::from(b)).collect()),
    }
  }
}

impl PartialEq<str> for FixStr<'_> {
  fn eq(&self, other: &str) -> bool {
    self.0 == other.as_bytes()
  }
}

impl PartialEq<&str> for FixStr<'_> {
  fn eq(&self, other: &&str) -> bool {
    self.0 == other.as_bytes()
  }
}

impl fmt::Display for FixStr<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self.as_ascii() {
      Some(s) => f.write_str(s),
      None => {
        for &b in self.0 {
          fmt::Write::write_char(f, char::from(b))?;
        }
        Ok(())
      }
    }
  }
}

impl fmt::Debug for FixStr<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self.to_str())
  }
}

impl FieldType for Str {
  type Value<'a> = FixStr<'a>;
  fn decode(raw: &[u8]) -> Result<FixStr<'_>, ValueError> {
    Ok(FixStr(raw))
  }
}

impl ToFix<Str> for FixStr<'_> {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put(self.0);
    Ok(())
  }
}

impl ToFix<Str> for str {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put_str(self)
  }
}

impl ToFix<Str> for String {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put_str(self)
  }
}

impl<M: FieldType, T: ToFix<M> + ?Sized> ToFix<M> for &T {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    (**self).to_fix(w)
  }
}

impl FromFix<Str> for String {
  fn from_fix(v: FixStr<'_>) -> Result<Self, ValueError> {
    Ok(v.to_str().into_owned())
  }
}

// ---------------------------------------------------------------------------
// char, Boolean
// ---------------------------------------------------------------------------

impl FieldType for Char {
  type Value<'a> = u8;
  fn decode(raw: &[u8]) -> Result<u8, ValueError> {
    match raw {
      [c] => Ok(*c),
      _ => Err(ValueError::Malformed),
    }
  }
}

impl ToFix<Char> for u8 {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put_u8(*self);
    Ok(())
  }
}

impl ToFix<Char> for char {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    let c = u32::from(*self);
    if c > 0xFF {
      return Err(ValueError::Unrepresentable);
    }
    w.put_u8(c as u8);
    Ok(())
  }
}

impl FieldType for Boolean {
  type Value<'a> = bool;
  fn decode(raw: &[u8]) -> Result<bool, ValueError> {
    match raw {
      b"Y" => Ok(true),
      b"N" => Ok(false),
      _ => Err(ValueError::Malformed),
    }
  }
}

impl ToFix<Boolean> for bool {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put_u8(if *self { b'Y' } else { b'N' });
    Ok(())
  }
}

// ---------------------------------------------------------------------------
// Integers
// ---------------------------------------------------------------------------

/// Digits only; leading zeros allowed.
fn parse_uint(raw: &[u8]) -> Result<u64, ValueError> {
  if raw.is_empty() {
    return Err(ValueError::Malformed);
  }
  let mut v: u64 = 0;
  for &c in raw {
    let d = c.wrapping_sub(b'0');
    if d > 9 {
      return Err(ValueError::Malformed);
    }
    v = v
      .checked_mul(10)
      .and_then(|v| v.checked_add(d as u64))
      .ok_or(ValueError::OutOfRange)?;
  }
  Ok(v)
}

pub(crate) fn parse_uint_value(raw: &[u8]) -> Result<u64, ValueError> {
  parse_uint(raw)
}

impl FieldType for UInt {
  type Value<'a> = u64;
  fn decode(raw: &[u8]) -> Result<u64, ValueError> {
    parse_uint(raw)
  }
}

impl FieldType for Int {
  type Value<'a> = i64;
  fn decode(raw: &[u8]) -> Result<i64, ValueError> {
    let (negative, digits) = match raw {
      [b'-', rest @ ..] => (true, rest),
      _ => (false, raw),
    };
    let magnitude = parse_uint(digits)?;
    if negative {
      0i64
        .checked_sub_unsigned(magnitude)
        .ok_or(ValueError::OutOfRange)
    } else {
      i64::try_from(magnitude).map_err(|_| ValueError::OutOfRange)
    }
  }
}

impl FieldType for DataLength {
  type Value<'a> = u64;
  fn decode(raw: &[u8]) -> Result<u64, ValueError> {
    parse_uint(raw)
  }
}

macro_rules! unsigned_to_fix {
  ($($t:ty),*) => {$(
    impl ToFix<UInt> for $t {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put_uint(*self as u64);
        Ok(())
      }
    }
    impl ToFix<Int> for $t {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put_uint(*self as u64);
        Ok(())
      }
    }
    impl FromFix<UInt> for $t {
      fn from_fix(v: u64) -> Result<Self, ValueError> {
        <$t>::try_from(v).map_err(|_| ValueError::OutOfRange)
      }
    }
  )*};
}
unsigned_to_fix!(u8, u16, u32, u64, usize);

macro_rules! signed_to_fix {
  ($($t:ty),*) => {$(
    impl ToFix<Int> for $t {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put_int(*self as i64);
        Ok(())
      }
    }
    impl FromFix<Int> for $t {
      fn from_fix(v: i64) -> Result<Self, ValueError> {
        <$t>::try_from(v).map_err(|_| ValueError::OutOfRange)
      }
    }
  )*};
}
signed_to_fix!(i8, i16, i32, i64, isize);

// ---------------------------------------------------------------------------
// Decimals
// ---------------------------------------------------------------------------

/// A decimal value (`float`, `Price`, `Qty`, `Amt`, ...), borrowed from the
/// message as validated text.
///
/// The text is in FIX's `float` lexical space: an optional `-`, digits, an
/// optional `.` and more digits, with at least one digit — `23`, `-23.5`,
/// `00023.23`, `23.`, `.5`. No `+`, no exponent. Convert it with `get_as` to a
/// decimal type (`decimix::Dec19` with the `decimix` feature), or pass it
/// straight to another message's `set`, which copies the text.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal<'a>(&'a [u8]);

impl<'a> Decimal<'a> {
  /// Validate `raw` as a FIX float.
  pub fn new(raw: &'a [u8]) -> Result<Self, ValueError> {
    let digits = raw.strip_prefix(b"-").unwrap_or(raw);
    let mut seen_digit = false;
    let mut seen_point = false;
    for &c in digits {
      match c {
        b'0'..=b'9' => seen_digit = true,
        b'.' if !seen_point => seen_point = true,
        _ => return Err(ValueError::Malformed),
      }
    }
    if !seen_digit {
      return Err(ValueError::Malformed);
    }
    Ok(Self(raw))
  }

  pub fn as_bytes(&self) -> &'a [u8] {
    self.0
  }

  pub fn as_str(&self) -> &'a str {
    // Validated as ASCII digits, '-' and '.'.
    std::str::from_utf8(self.0).unwrap_or_default()
  }

  pub fn is_negative(&self) -> bool {
    self.0.first() == Some(&b'-')
  }

  /// The nearest `f64`. Lossy — for display and tooling, never for prices.
  pub fn to_f64_lossy(&self) -> f64 {
    self.as_str().parse().unwrap_or(f64::NAN)
  }
}

impl fmt::Display for Decimal<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl fmt::Debug for Decimal<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Decimal({})", self.as_str())
  }
}

macro_rules! decimal_type {
  ($($m:ty),*) => {$(
    impl FieldType for $m {
      type Value<'a> = Decimal<'a>;
      fn decode(raw: &[u8]) -> Result<Decimal<'_>, ValueError> {
        Decimal::new(raw)
      }
    }
    impl ToFix<$m> for Decimal<'_> {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put(self.0);
        Ok(())
      }
    }
    // Integers are decimals too: `set(OrderQty, 100u64)`.
    impl ToFix<$m> for u64 {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put_uint(*self);
        Ok(())
      }
    }
    impl ToFix<$m> for i64 {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put_int(*self);
        Ok(())
      }
    }
  )*};
}
decimal_type!(Float, Qty, Price, PriceOffset, Amt, Percentage);

#[cfg(feature = "decimix")]
mod decimix_impls {
  use decimix::{Dec19, UDec19};

  use super::*;

  fn write_ascii(
    w: &mut ValueWriter<'_>,
    f: impl FnOnce(&mut [u8]) -> Result<usize, decimix::BufferTooSmall>,
  ) -> Result<(), ValueError> {
    let mut buf = [0u8; 64];
    let n = f(&mut buf).map_err(|_| ValueError::OutOfRange)?;
    w.put(&buf[..n]);
    Ok(())
  }

  fn parse_err(e: decimix::ParseError) -> ValueError {
    match e {
      decimix::ParseError::Invalid => ValueError::Malformed,
      _ => ValueError::OutOfRange,
    }
  }

  macro_rules! dec19 {
    ($($m:ty),*) => {$(
      impl FromFix<$m> for Dec19 {
        fn from_fix(v: Decimal<'_>) -> Result<Self, ValueError> {
          Dec19::from_ascii(v.as_bytes()).map_err(parse_err)
        }
      }
      impl ToFix<$m> for Dec19 {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          write_ascii(w, |b| self.write_ascii(b))
        }
      }
    )*};
  }
  // Every float datatype may be negative "unless explicitly specified
  // otherwise" (TagValue datatypes), Qty included.
  dec19!(Float, Qty, Price, PriceOffset, Amt, Percentage);

  // Qty's natural type is unsigned; a negative quantity is out of range.
  impl FromFix<Qty> for UDec19 {
    fn from_fix(v: Decimal<'_>) -> Result<Self, ValueError> {
      if v.is_negative() {
        return Err(ValueError::OutOfRange);
      }
      UDec19::from_ascii(v.as_bytes()).map_err(parse_err)
    }
  }
  impl ToFix<Qty> for UDec19 {
    fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
      write_ascii(w, |b| self.write_ascii(b))
    }
  }
}

#[cfg(feature = "decimix-finance")]
mod decimix_finance_impls {
  use decimix_finance as finance;

  use super::*;

  macro_rules! newtype {
    ($t:ty, $inner:ty, $($m:ty),*) => {$(
      impl FromFix<$m> for $t {
        fn from_fix(v: Decimal<'_>) -> Result<Self, ValueError> {
          <$inner as FromFix<$m>>::from_fix(v).map(<$t>::new)
        }
      }
      impl ToFix<$m> for $t {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          <$inner as ToFix<$m>>::to_fix(&self.get(), w)
        }
      }
    )*};
  }
  newtype!(finance::Price, decimix::Dec19, Price, PriceOffset);
  newtype!(finance::Qty, decimix::UDec19, Qty);
  newtype!(finance::DeltaQty, decimix::Dec19, Qty);
  newtype!(finance::Amt, decimix::Dec19, Amt);
  newtype!(finance::Percentage, decimix::Dec19, Percentage);
}

// ---------------------------------------------------------------------------
// Dates and times
// ---------------------------------------------------------------------------

macro_rules! text_view {
  ($(#[$doc:meta])* $name:ident) => {
    $(#[$doc])*
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct $name<'a>(FixStr<'a>);

    impl<'a> $name<'a> {
      pub fn as_fix_str(&self) -> FixStr<'a> {
        self.0
      }
      pub fn as_bytes(&self) -> &'a [u8] {
        self.0.as_bytes()
      }
    }

    impl fmt::Display for $name<'_> {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
      }
    }

    impl fmt::Debug for $name<'_> {
      fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", stringify!($name), self.0)
      }
    }
  };
}

text_view!(
  /// A `UTCTimestamp` value: `YYYYMMDD-HH:MM:SS[.s…]`. Unchecked until
  /// converted with `get_as::<DateTime<Utc>>`.
  Timestamp
);
text_view!(
  /// A `UTCDateOnly` or `LocalMktDate` value: `YYYYMMDD`. Unchecked until
  /// converted with `get_as::<NaiveDate>`.
  Date
);
text_view!(
  /// A `UTCTimeOnly` or `LocalMktTime` value: `HH:MM:SS[.s…]`. Unchecked until
  /// converted with `get_as::<NaiveTime>`.
  Time
);

impl FieldType for UtcTimestamp {
  type Value<'a> = Timestamp<'a>;
  fn decode(raw: &[u8]) -> Result<Timestamp<'_>, ValueError> {
    Ok(Timestamp(FixStr(raw)))
  }
}

macro_rules! view_type {
  ($view:ident, $($m:ty),*) => {$(
    impl FieldType for $m {
      type Value<'a> = $view<'a>;
      fn decode(raw: &[u8]) -> Result<$view<'_>, ValueError> {
        Ok($view(FixStr(raw)))
      }
    }
    impl ToFix<$m> for $view<'_> {
      fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
        w.put(self.as_bytes());
        Ok(())
      }
    }
  )*};
}
view_type!(Date, UtcDateOnly, LocalMktDate);
view_type!(Time, UtcTimeOnly, LocalMktTime);

impl ToFix<UtcTimestamp> for Timestamp<'_> {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put(self.as_bytes());
    Ok(())
  }
}

mod chrono_impls {
  use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};

  use super::*;
  use crate::time::{TimePrecision, fix_time};

  /// Exactly `n` ASCII digits.
  fn digits(raw: &[u8], n: usize) -> Result<u32, ValueError> {
    if raw.len() != n {
      return Err(ValueError::Malformed);
    }
    parse_uint(raw).map(|v| v as u32)
  }

  fn date(raw: &[u8]) -> Result<NaiveDate, ValueError> {
    if raw.len() != 8 {
      return Err(ValueError::Malformed);
    }
    NaiveDate::from_ymd_opt(
      digits(&raw[0..4], 4)? as i32,
      digits(&raw[4..6], 2)?,
      digits(&raw[6..8], 2)?,
    )
    .ok_or(ValueError::OutOfRange)
  }

  /// `HH:MM:SS` optionally followed by `.` and 1 to 12 digits (picoseconds are
  /// truncated to the nanosecond). A leap second, `:60`, is folded into the
  /// last nanosecond of the minute, as `fix_time` does when formatting.
  fn time(raw: &[u8]) -> Result<NaiveTime, ValueError> {
    if raw.len() < 8 || raw[2] != b':' || raw[5] != b':' {
      return Err(ValueError::Malformed);
    }
    let h = digits(&raw[0..2], 2)?;
    let m = digits(&raw[3..5], 2)?;
    let mut s = digits(&raw[6..8], 2)?;
    let mut nanos = match &raw[8..] {
      [] => 0,
      [b'.', frac @ ..] if (1..=12).contains(&frac.len()) => {
        let kept = &frac[..frac.len().min(9)];
        parse_uint(kept)? as u32 * 10u32.pow(9 - kept.len() as u32)
      }
      _ => return Err(ValueError::Malformed),
    };
    if s == 60 {
      s = 59;
      nanos = 999_999_999;
    }
    NaiveTime::from_hms_nano_opt(h, m, s, nanos).ok_or(ValueError::OutOfRange)
  }

  impl FromFix<UtcTimestamp> for DateTime<Utc> {
    fn from_fix(v: Timestamp<'_>) -> Result<Self, ValueError> {
      let raw = v.as_bytes();
      if raw.len() < 17 || raw[8] != b'-' {
        return Err(ValueError::Malformed);
      }
      Ok(date(&raw[..8])?.and_time(time(&raw[9..])?).and_utc())
    }
  }

  /// A UTC timestamp at a chosen precision: `set(TransactTime, (now,
  /// TimePrecision::Micros))`.
  impl ToFix<UtcTimestamp> for (DateTime<Utc>, TimePrecision) {
    fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
      w.put(fix_time(self.0, self.1).as_bytes());
      Ok(())
    }
  }

  macro_rules! date_impls {
    ($($m:ty),*) => {$(
      impl FromFix<$m> for NaiveDate {
        fn from_fix(v: Date<'_>) -> Result<Self, ValueError> {
          date(v.as_bytes())
        }
      }
      impl ToFix<$m> for NaiveDate {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          let y = self.year();
          if !(0..=9999).contains(&y) {
            return Err(ValueError::OutOfRange);
          }
          let mut buf = [0u8; 8];
          write_padded(&mut buf[0..4], y as u32);
          write_padded(&mut buf[4..6], self.month());
          write_padded(&mut buf[6..8], self.day());
          w.put(&buf);
          Ok(())
        }
      }
    )*};
  }
  date_impls!(UtcDateOnly, LocalMktDate);

  macro_rules! time_impls {
    ($($m:ty),*) => {$(
      impl FromFix<$m> for NaiveTime {
        fn from_fix(v: Time<'_>) -> Result<Self, ValueError> {
          time(v.as_bytes())
        }
      }
      /// Whole seconds: `HH:MM:SS`.
      impl ToFix<$m> for NaiveTime {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          write_time(w, self, None)
        }
      }
      impl ToFix<$m> for (NaiveTime, TimePrecision) {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          write_time(w, &self.0, Some(self.1))
        }
      }
    )*};
  }
  time_impls!(UtcTimeOnly, LocalMktTime);

  fn write_padded(out: &mut [u8], mut v: u32) {
    for b in out.iter_mut().rev() {
      *b = b'0' + (v % 10) as u8;
      v /= 10;
    }
  }

  fn write_time(
    w: &mut ValueWriter<'_>,
    t: &NaiveTime,
    precision: Option<TimePrecision>,
  ) -> Result<(), ValueError> {
    let mut buf = [0u8; 8];
    write_padded(&mut buf[0..2], t.hour());
    buf[2] = b':';
    write_padded(&mut buf[3..5], t.minute());
    buf[5] = b':';
    write_padded(&mut buf[6..8], t.second());
    w.put(&buf);
    if let Some(p) = precision {
      let nanos = t.nanosecond().min(999_999_999);
      let mut frac = [b'0'; 12];
      write_padded(&mut frac[..9], nanos);
      w.put_u8(b'.');
      w.put(&frac[..p.digits()]);
    }
    Ok(())
  }
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

impl FieldType for Data {
  type Value<'a> = &'a [u8];
  fn decode(raw: &[u8]) -> Result<&[u8], ValueError> {
    Ok(raw)
  }
}

impl ToFix<Data> for [u8] {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put(self);
    Ok(())
  }
}

impl ToFix<Data> for Vec<u8> {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put(self);
    Ok(())
  }
}

impl<const N: usize> ToFix<Data> for [u8; N] {
  fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
    w.put(self);
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn write<M: FieldType>(v: impl ToFix<M>) -> Result<Vec<u8>, ValueError> {
    let mut buf = Vec::new();
    v.to_fix(&mut ValueWriter::new(&mut buf))?;
    Ok(buf)
  }

  #[test]
  fn decimal_lexical_space_is_fix_float() {
    for ok in ["23", "-23.5", "00023.23", "23.", "23.0000", ".5", "-0", "0"] {
      assert!(Decimal::new(ok.as_bytes()).is_ok(), "{ok}");
    }
    for bad in ["+1", "1e5", "", "-", ".", "1.2.3", " 1", "1,000", "--1"] {
      assert_eq!(
        Decimal::new(bad.as_bytes()),
        Err(ValueError::Malformed),
        "{bad}"
      );
    }
  }

  #[test]
  fn integers() {
    assert_eq!(Int::decode(b"-42"), Ok(-42));
    assert_eq!(Int::decode(b"007"), Ok(7));
    assert_eq!(Int::decode(b"-9223372036854775808"), Ok(i64::MIN));
    assert_eq!(
      Int::decode(b"9223372036854775808"),
      Err(ValueError::OutOfRange)
    );
    assert_eq!(Int::decode(b"+1"), Err(ValueError::Malformed));
    assert_eq!(UInt::decode(b"-1"), Err(ValueError::Malformed));
    assert_eq!(write::<Int>(-12i32).unwrap(), b"-12");
    assert_eq!(write::<UInt>(0u64).unwrap(), b"0");
    assert_eq!(write::<Int>(i64::MIN).unwrap(), b"-9223372036854775808");
  }

  #[test]
  fn strings_are_latin1() {
    assert_eq!(write::<Str>("abc").unwrap(), b"abc");
    assert_eq!(write::<Str>("café").unwrap(), b"caf\xe9");
    assert_eq!(write::<Str>("日本"), Err(ValueError::Unrepresentable));

    let s = FixStr::new(b"caf\xe9");
    assert_eq!(s.as_ascii(), None);
    assert_eq!(s.to_str(), "café");
    assert_eq!(s.to_string(), "café");
    let s = FixStr::new(b"abc");
    assert_eq!(s.as_ascii(), Some("abc"));
    assert!(matches!(s.to_str(), Cow::Borrowed("abc")));
    assert_eq!(s, "abc");
  }

  #[test]
  fn booleans_and_chars() {
    assert_eq!(Boolean::decode(b"Y"), Ok(true));
    assert_eq!(Boolean::decode(b"y"), Err(ValueError::Malformed));
    assert_eq!(Char::decode(b"12"), Err(ValueError::Malformed));
    assert_eq!(write::<Boolean>(false).unwrap(), b"N");
  }

  #[test]
  fn timestamps_round_trip_through_chrono() {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

    use crate::time::TimePrecision;

    let t = Timestamp(FixStr(b"20261002-13:45:01.123456"));
    let dt = <DateTime<Utc> as FromFix<UtcTimestamp>>::from_fix(t).unwrap();
    assert_eq!(
      write::<UtcTimestamp>((dt, TimePrecision::Micros)).unwrap(),
      b"20261002-13:45:01.123456"
    );
    let t = Timestamp(FixStr(b"20261002-13:45:01"));
    assert!(<DateTime<Utc> as FromFix<UtcTimestamp>>::from_fix(t).is_ok());
    for bad in [
      &b"20261002 13:45:01"[..],
      b"2026100-13:45:01",
      b"20261302-13:45:01",
    ] {
      assert!(
        <DateTime<Utc> as FromFix<UtcTimestamp>>::from_fix(Timestamp(FixStr(
          bad
        )))
        .is_err()
      );
    }

    let d = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
    assert_eq!(write::<LocalMktDate>(d).unwrap(), b"20260102");
    assert_eq!(
      <NaiveDate as FromFix<LocalMktDate>>::from_fix(Date(FixStr(b"20260102"))),
      Ok(d)
    );

    let t = NaiveTime::from_hms_milli_opt(9, 5, 3, 7).unwrap();
    assert_eq!(write::<UtcTimeOnly>(t).unwrap(), b"09:05:03");
    assert_eq!(
      write::<UtcTimeOnly>((t, TimePrecision::Millis)).unwrap(),
      b"09:05:03.007"
    );
    assert_eq!(
      <NaiveTime as FromFix<UtcTimeOnly>>::from_fix(Time(FixStr(
        b"09:05:03.007"
      ))),
      Ok(t)
    );
  }

  #[cfg(feature = "decimix")]
  #[test]
  fn decimix_conversions() {
    use decimix::{Dec19, UDec19};

    let px = Decimal::new(b"-12.30").unwrap();
    let d = <Dec19 as FromFix<Price>>::from_fix(px).unwrap();
    assert_eq!(write::<Price>(d).unwrap(), b"-12.3");
    let qty = Decimal::new(b"100").unwrap();
    assert_eq!(
      <UDec19 as FromFix<Qty>>::from_fix(qty).unwrap(),
      UDec19::from(100u64)
    );
    assert_eq!(
      <UDec19 as FromFix<Qty>>::from_fix(Decimal::new(b"-1").unwrap()),
      Err(ValueError::OutOfRange)
    );
    assert!(
      <Dec19 as FromFix<Qty>>::from_fix(Decimal::new(b"-1").unwrap()).is_ok()
    );
  }
}
