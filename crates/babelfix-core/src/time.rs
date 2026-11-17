//! FIX UTC timestamp formatting.
//!
//! FIX renders `SendingTime`, `OrigSendingTime` and `TransactTime` as
//! `YYYYMMDD-HH:MM:SS.sss`, with the fractional part carrying anywhere from
//! milliseconds to picoseconds depending on what the counterparties agreed.
//! [`TimePrecision`] selects how many fractional digits are emitted.
//!
//! **This module reads no clock.** It formats an instant it is handed. Built on
//! its own, the crate does not even enable jiff's `std` feature, so
//! `Timestamp::now()` is not in scope here — the driver owns the clock, and stamps
//! `SendingTime` as late as it can. See [`crate::message`] for where the result is used.

use std::fmt;

use jiff::{Timestamp, tz::Offset};

/// `"YYYYMMDD-HH:MM:SS."` — everything before the fractional digits.
const PREFIX_LEN: usize = 18;

/// Longest a formatted timestamp can be: [`TimePrecision::Picos`].
pub const MAX_LEN: usize = PREFIX_LEN + 12;

/// How many fractional-second digits to emit in a FIX timestamp.
///
/// Every variant is fixed-width, which keeps `BodyLength` predictable and makes
/// it possible to stamp the field into an already-serialised buffer.
///
/// Note that the underlying clock resolution is nanoseconds, so [`Picos`]
/// always emits three trailing zeroes. It exists because some venues require
/// the field to be picosecond-shaped, not because the extra digits carry
/// information.
///
/// [`Picos`]: TimePrecision::Picos
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TimePrecision {
  /// `YYYYMMDD-HH:MM:SS.sss` — the FIX 4.x default.
  Millis,
  /// `YYYYMMDD-HH:MM:SS.ssssss`
  Micros,
  /// `YYYYMMDD-HH:MM:SS.sssssssss`
  #[default]
  Nanos,
  /// `YYYYMMDD-HH:MM:SS.ssssssssssss`, the last three digits always zero.
  Picos,
}

impl TimePrecision {
  /// Number of fractional-second digits.
  pub const fn digits(self) -> usize {
    match self {
      TimePrecision::Millis => 3,
      TimePrecision::Micros => 6,
      TimePrecision::Nanos => 9,
      TimePrecision::Picos => 12,
    }
  }

  /// Total width, in characters, of a timestamp at this precision.
  pub const fn width(self) -> usize {
    PREFIX_LEN + self.digits()
  }
}

/// A formatted FIX timestamp held inline, with no heap allocation.
///
/// Produced by [`fix_time`]. Deref to `&str` via [`as_str`](Self::as_str), or
/// write it straight out with [`as_bytes`](Self::as_bytes).
#[derive(Clone, Copy)]
pub struct FixTime {
  buf: [u8; MAX_LEN],
  len: usize,
}

impl FixTime {
  pub fn as_str(&self) -> &str {
    // Every byte written below is ASCII, so this cannot fail.
    std::str::from_utf8(self.as_bytes()).unwrap_or("")
  }

  pub fn as_bytes(&self) -> &[u8] {
    &self.buf[..self.len]
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn is_empty(&self) -> bool {
    self.len == 0
  }
}

impl fmt::Display for FixTime {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

impl fmt::Debug for FixTime {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self.as_str())
  }
}

impl From<FixTime> for String {
  fn from(t: FixTime) -> String {
    t.as_str().to_owned()
  }
}

/// Write `value` as exactly `width` zero-padded decimal digits at `buf[at..]`,
/// returning the position just past them.
fn write_digits(buf: &mut [u8], at: usize, value: u32, width: usize) -> usize {
  let mut v = value;
  for i in (0..width).rev() {
    buf[at + i] = b'0' + (v % 10) as u8;
    v /= 10;
  }
  at + width
}

/// Format `when` as a FIX UTC timestamp at the given precision.
///
/// The formatting is done by hand rather than through jiff's `strftime`
/// machinery: the layout is fixed, this sits on the send path, and it lets the
/// result live on the stack. To format straight into a buffer of your own, see
/// [`write_fix_time`].
pub fn fix_time(when: Timestamp, precision: TimePrecision) -> FixTime {
  let mut buf = [0u8; MAX_LEN];
  let len = write_fix_time(when, precision, &mut buf);
  FixTime { buf, len }
}

/// Format `when` as a FIX UTC timestamp at the start of `buf`, returning the
/// bytes written — always [`TimePrecision::width`].
///
/// # Panics
///
/// If `buf` is shorter than `precision.width()`.
pub fn write_fix_time(
  when: Timestamp,
  precision: TimePrecision,
  buf: &mut [u8],
) -> usize {
  let buf = &mut buf[..precision.width()];
  let dt = Offset::UTC.to_datetime(when);

  // A year outside 0..=9999 cannot be rendered in the four digits FIX allows.
  // Clamping keeps the field fixed-width; such a timestamp is nonsense anyway.
  let year = dt.year().clamp(0, 9999) as u32;

  let mut at = write_digits(buf, 0, year, 4);
  at = write_digits(buf, at, dt.month() as u32, 2);
  at = write_digits(buf, at, dt.day() as u32, 2);
  buf[at] = b'-';
  at += 1;
  at = write_digits(buf, at, dt.hour() as u32, 2);
  buf[at] = b':';
  at += 1;
  at = write_digits(buf, at, dt.minute() as u32, 2);
  buf[at] = b':';
  at += 1;
  at = write_digits(buf, at, dt.second() as u32, 2);
  buf[at] = b'.';
  at += 1;

  // jiff has no leap seconds, and `subsec_nanosecond` of a civil datetime is
  // always in 0..1_000_000_000.
  let nanos = dt.subsec_nanosecond() as u32;

  let at = match precision {
    TimePrecision::Millis => write_digits(buf, at, nanos / 1_000_000, 3),
    TimePrecision::Micros => write_digits(buf, at, nanos / 1_000, 6),
    TimePrecision::Nanos => write_digits(buf, at, nanos, 9),
    TimePrecision::Picos => {
      let at = write_digits(buf, at, nanos, 9);
      write_digits(buf, at, 0, 3)
    }
  };

  debug_assert_eq!(at, precision.width());
  at
}

#[cfg(test)]
mod tests {
  use super::*;

  fn at(y: i16, mo: i8, d: i8, h: i8, mi: i8, s: i8, nanos: i32) -> Timestamp {
    Offset::UTC
      .to_timestamp(jiff::civil::datetime(y, mo, d, h, mi, s, nanos))
      .unwrap()
  }

  #[test]
  fn formats_each_precision() {
    let t = at(2026, 8, 9, 14, 3, 7, 123_456_789);
    assert_eq!(
      fix_time(t, TimePrecision::Millis).as_str(),
      "20260809-14:03:07.123"
    );
    assert_eq!(
      fix_time(t, TimePrecision::Micros).as_str(),
      "20260809-14:03:07.123456"
    );
    assert_eq!(
      fix_time(t, TimePrecision::Nanos).as_str(),
      "20260809-14:03:07.123456789"
    );
    assert_eq!(
      fix_time(t, TimePrecision::Picos).as_str(),
      "20260809-14:03:07.123456789000"
    );
  }

  #[test]
  fn pads_every_field() {
    let t = at(2026, 1, 2, 3, 4, 5, 6);
    assert_eq!(
      fix_time(t, TimePrecision::Nanos).as_str(),
      "20260102-03:04:05.000000006"
    );
    assert_eq!(
      fix_time(t, TimePrecision::Millis).as_str(),
      "20260102-03:04:05.000"
    );
  }

  #[test]
  fn width_is_fixed_per_precision() {
    // Stamping into a pre-serialised buffer depends on this.
    for p in [
      TimePrecision::Millis,
      TimePrecision::Micros,
      TimePrecision::Nanos,
      TimePrecision::Picos,
    ] {
      for nanos in [0, 1, 999_999_999] {
        let t = at(2026, 12, 31, 23, 59, 59, nanos);
        assert_eq!(fix_time(t, p).len(), p.width(), "{p:?} nanos={nanos}");
      }
    }
  }

  #[test]
  fn matches_jiff_reference_formatting() {
    // The hand-rolled formatter must agree with the `%Y%m%d-%H:%M:%S.%f` form
    // this replaced.
    let t = at(2026, 8, 9, 14, 3, 7, 123_456_789);
    assert_eq!(
      fix_time(t, TimePrecision::Millis).as_str(),
      t.strftime("%Y%m%d-%H:%M:%S.%3f").to_string()
    );
    assert_eq!(
      fix_time(t, TimePrecision::Nanos).as_str(),
      t.strftime("%Y%m%d-%H:%M:%S.%9f").to_string()
    );
  }

  #[test]
  fn out_of_range_years_are_clamped_to_four_digits() {
    let t = Timestamp::MIN;
    let formatted = fix_time(t, TimePrecision::Nanos);
    assert_eq!(formatted.len(), TimePrecision::Nanos.width());
    assert!(formatted.as_str().starts_with("0000"));
  }
}
