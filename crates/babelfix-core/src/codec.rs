//! Wire framing: splitting a byte stream into [`FixMessage`]s and back again.
//!
//! Both halves are ordinary synchronous functions over [`bytes::BytesMut`].
//! There is no runtime here and no trait to implement — [`FixDecoder::decode`]
//! takes whatever bytes you have and returns a message once one is complete,
//! and [`FixEncoder::encode`] appends a message's wire form to a buffer.
//!
//! `babelfix-tokio` wraps these in [`tokio_util::codec`] `Decoder`/`Encoder`
//! impls so they can be used with `Framed`, but nothing about them requires it.
//!
//! # Framing
//!
//! A FIX message is `BeginString(8)`, `BodyLength(9)`, the body, then
//! `CheckSum(10)`. `BodyLength` covers everything between the end of tag 9 and
//! the start of tag 10, so the frame boundary is known once the header has been
//! read. The decoder verifies the checksum before handing a message back;
//! a mismatch is an [`Error::InvalidMessage`] and, at the session layer, fatal
//! to the connection.
//!
//! [`tokio_util::codec`]: https://docs.rs/tokio-util/latest/tokio_util/codec/

use std::sync::Arc;

use bytes::BytesMut;
use chrono::{DateTime, Utc};
use tracing::debug;

use crate::message::{self, FixMessage, Value};
use crate::repository::{FixRepository, FixVersion};
use crate::schema::FIX_Latest::Fields::SendingTime as SENDING_TIME;
use crate::time::TimePrecision;
use crate::{Error, Result};

/// The field separator FIX uses on the wire: ASCII SOH.
pub const SOH: u8 = b'\x01';

/// Splits a byte stream into [`FixMessage`]s, verifying `BodyLength` and
/// `CheckSum`.
///
/// The decoder latches the FIX version inferred from the first message's
/// `BeginString` and reuses it thereafter, so a session cannot silently change
/// version mid-stream.
#[derive(Default, Clone)]
pub struct FixDecoder {
  repo: Arc<FixRepository>,
  delimiter: u8,
  fix_version: Option<Arc<FixVersion>>,
}

impl FixDecoder {
  /// A decoder that infers the FIX version from the first message it sees.
  ///
  /// `delimiter` is the field separator; pass `None` for the wire default
  /// [`SOH`]. A printable delimiter such as `b'|'` is convenient in tests and
  /// logs.
  pub fn new(repo: Arc<FixRepository>, delimiter: Option<u8>) -> Self {
    Self {
      repo,
      delimiter: delimiter.unwrap_or(SOH),
      fix_version: None,
    }
  }

  /// A decoder pinned to a known version, for a session that has already
  /// negotiated one.
  pub fn with_version(
    repo: Arc<FixRepository>,
    delimiter: Option<u8>,
    fix_version: Arc<FixVersion>,
  ) -> Self {
    Self {
      repo,
      delimiter: delimiter.unwrap_or(SOH),
      fix_version: Some(fix_version),
    }
  }

  /// The FIX version this decoder has latched onto, if it has seen a message.
  pub fn fix_version(&self) -> Option<&Arc<FixVersion>> {
    self.fix_version.as_ref()
  }

  pub fn delimiter(&self) -> u8 {
    self.delimiter
  }

  /// Take one complete message off the front of `data`, if there is one.
  ///
  /// Returns `Ok(None)` when `data` holds only a partial frame — call again
  /// once more bytes have arrived. Consumed bytes are split off `data`;
  /// anything left is the start of the next frame.
  pub fn decode(&mut self, data: &mut BytesMut) -> Result<Option<FixMessage>> {
    // FIXME: PIGGY PIGGY PIGGY PORKER SO MANY UNNECESSARY PARSES IN THE CASE OF
    // FRAGMENT
    //  We can store the begin_string_len and body_len after parsing and skip
    //  it if we already parsed it once, rather than re-parsing the fragment
    //  every time
    let begin_string_len;
    let body_len;
    if self.fix_version.is_none() {
      (self.fix_version, body_len, begin_string_len) =
        message::peek_infer_version_and_length(
          self.repo.as_ref(),
          data,
          self.delimiter,
        )?;
    } else {
      (_, body_len, begin_string_len) = message::peek_infer_version_and_length(
        self.repo.as_ref(),
        data,
        self.delimiter,
      )?;
    }

    let (Some(fix_version), Some(msg_len)) =
      (self.fix_version.as_ref(), body_len)
    else {
      return Ok(None);
    };

    if data.len() <= begin_string_len + msg_len {
      // Either part message or no additional checksum
      return Ok(None);
    }

    let (Some(checksum), checksum_len) = message::peek_checksum(
      self.repo.as_ref(),
      &data[begin_string_len + msg_len..],
      self.delimiter,
    )?
    else {
      return Ok(None);
    };

    let buf = data.split_to(begin_string_len + msg_len + checksum_len);
    let mut calc_checksum: u8 = 0;
    for ch in buf[..begin_string_len + msg_len].iter() {
      calc_checksum = calc_checksum.wrapping_add(*ch);
    }

    if checksum != calc_checksum {
      return Err(Error::invalid_message(format!(
        "Invalid checksum; expected {calc_checksum} but got {checksum}",
      )));
    }

    // Convert BytesMut to Bytes (zero-copy)
    let bytes = buf.freeze();
    let (fix_msg, consumed) = FixMessage::from_bytes_delimited(
      fix_version.clone(),
      bytes,
      self.delimiter,
    )?;

    if consumed != begin_string_len + msg_len + checksum_len {
      return Err(Error::invalid_message(
        "Consumed length does not match expected length",
      ));
    }

    debug!("Decoded FIX message: {:?}", fix_msg);
    Ok(Some(fix_msg))
  }
}

/// Serialises [`FixMessage`]s onto the wire, computing `BodyLength` and
/// `CheckSum`.
#[derive(Default, Clone)]
pub struct FixEncoder {
  delimiter: u8,
  precision: TimePrecision,
}

impl FixEncoder {
  pub fn new(delimiter: Option<u8>) -> Self {
    Self {
      delimiter: delimiter.unwrap_or(SOH),
      precision: TimePrecision::default(),
    }
  }

  /// Set the precision used by [`encode_stamped`](Self::encode_stamped).
  pub fn with_precision(mut self, precision: TimePrecision) -> Self {
    self.precision = precision;
    self
  }

  pub fn delimiter(&self) -> u8 {
    self.delimiter
  }

  pub fn precision(&self) -> TimePrecision {
    self.precision
  }

  /// Append `msg`'s wire form to `dst`.
  ///
  /// Takes the message by reference: the caller usually still needs it, to hand
  /// to the application as a record of what was sent.
  pub fn encode(&mut self, msg: &FixMessage, dst: &mut BytesMut) -> Result<()> {
    msg.write_to(dst, self.delimiter)?;

    debug!("Encoded FIX message: {:?}", dst);
    Ok(())
  }

  /// Stamp `SendingTime`, then serialise.
  ///
  /// This is the form the session layer's output should use: it fills in the
  /// slot the state machine reserved, so the clock is read once per message and
  /// as late as possible, and `BodyLength`/`CheckSum` are computed over the
  /// stamped message rather than the placeholder.
  pub fn encode_stamped(
    &mut self,
    msg: &mut FixMessage,
    sending_time: DateTime<Utc>,
    dst: &mut BytesMut,
  ) -> Result<()> {
    stamp_sending_time(msg, sending_time, self.precision)?;
    self.encode(msg, dst)
  }
}

/// Overwrite `msg`'s `SendingTime` with `when`.
///
/// Always overwrites, never fills-only-if-absent: a replayed message arrives
/// carrying the timestamp from when it was *originally* sent (which the session
/// has already copied into `OrigSendingTime`), and it must go back out with a
/// fresh one.
///
/// The field must already be present. The session layer reserves it so it sits
/// in its proper place in the header; erroring here rather than inserting keeps
/// a driver that forgets to stamp from silently emitting messages with no
/// `SendingTime` at all.
pub fn stamp_sending_time(
  msg: &mut FixMessage,
  when: DateTime<Utc>,
  precision: TimePrecision,
) -> Result<()> {
  let stamp = crate::time::fix_time(when, precision);
  for (tag, value) in msg.tags.iter_mut() {
    if *tag == SENDING_TIME {
      *value = Value::String(stamp.as_str().to_owned());
      return Ok(());
    }
  }
  Err(Error::invalid_message(
    "outbound message has no SendingTime field to stamp",
  ))
}
