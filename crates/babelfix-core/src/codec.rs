//! Wire framing: splitting a byte stream into [`Message`]s and back again.
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
//! read. The frame is then parsed, which verifies `BodyLength` and `CheckSum`;
//! a message that fails is an [`Error::InvalidMessage`] and, at the session
//! layer, fatal to the connection.
//!
//! A decoded message keeps the frame's bytes — split off the input buffer and
//! frozen, not copied — and its fields point into them.
//!
//! [`tokio_util::codec`]: https://docs.rs/tokio-util/latest/tokio_util/codec/

use std::sync::Arc;

use bytes::BytesMut;
use tracing::debug;

use crate::message::{Dictionaries, Dictionary, Message};
use crate::{Error, Result};

/// The field separator FIX uses on the wire: ASCII SOH.
pub const SOH: u8 = crate::message::SOH;

/// `10=nnn<SOH>`
const CHECKSUM_FIELD_LEN: usize = 7;

/// The largest frame a [`FixDecoder`] accepts unless told otherwise: 4 MiB.
pub const DEFAULT_MAX_FRAME_LEN: usize = 4 << 20;

/// Splits a byte stream into [`Message`]s.
///
/// The decoder latches the FIX version named by the first message's
/// `BeginString` and holds every later message to it, so a session cannot
/// silently change version mid-stream.
#[derive(Clone)]
pub struct FixDecoder {
  dicts: Arc<Dictionaries>,
  delimiter: u8,
  dict: Option<Arc<Dictionary>>,
  max_frame_len: usize,
}

impl FixDecoder {
  /// A decoder that takes its FIX version from the first message it sees.
  ///
  /// `delimiter` is the field separator; pass `None` for the wire default
  /// [`SOH`]. A printable delimiter such as `b'|'` is convenient in tests and
  /// logs.
  pub fn new(dicts: Arc<Dictionaries>, delimiter: Option<u8>) -> Self {
    Self {
      dicts,
      delimiter: delimiter.unwrap_or(SOH),
      dict: None,
      max_frame_len: DEFAULT_MAX_FRAME_LEN,
    }
  }

  /// A decoder pinned to a known version, for a session that has already
  /// negotiated one.
  pub fn with_dictionary(
    dicts: Arc<Dictionaries>,
    delimiter: Option<u8>,
    dict: Arc<Dictionary>,
  ) -> Self {
    Self {
      dicts,
      delimiter: delimiter.unwrap_or(SOH),
      dict: Some(dict),
      max_frame_len: DEFAULT_MAX_FRAME_LEN,
    }
  }

  /// Refuse frames longer than `max` bytes, BeginString to CheckSum, rather
  /// than buffering whatever a peer's `BodyLength(9)` claims. The default is
  /// [`DEFAULT_MAX_FRAME_LEN`].
  pub fn with_max_frame_len(mut self, max: usize) -> Self {
    self.max_frame_len = max;
    self
  }

  /// The largest frame this decoder accepts.
  pub fn max_frame_len(&self) -> usize {
    self.max_frame_len
  }

  /// The dictionary this decoder has latched onto, if it has seen a message.
  pub fn dictionary(&self) -> Option<&Arc<Dictionary>> {
    self.dict.as_ref()
  }

  pub fn delimiter(&self) -> u8 {
    self.delimiter
  }

  /// Take one complete message off the front of `data`, if there is one.
  ///
  /// Returns `Ok(None)` when `data` holds only a partial frame — call again
  /// once more bytes have arrived. Consumed bytes are split off `data`;
  /// anything left is the start of the next frame.
  pub fn decode(&mut self, data: &mut BytesMut) -> Result<Option<Message>> {
    let Some(frame) = frame(data, self.delimiter, self.max_frame_len)? else {
      return Ok(None);
    };
    if data.len() < frame.len {
      return Ok(None);
    }

    let dict = match &self.dict {
      Some(dict) => dict.clone(),
      None => {
        let dict = self
          .dicts
          .for_begin_string(&data[frame.begin_string.clone()])
          .ok_or_else(|| {
            Error::invalid_message(format!(
              "Unknown FIX version {}",
              String::from_utf8_lossy(&data[frame.begin_string.clone()])
            ))
          })?
          .clone();
        self.dict = Some(dict.clone());
        dict
      }
    };

    let bytes = data.split_to(frame.len).freeze();
    let msg = if self.delimiter == SOH {
      Message::parse(&dict, bytes)?
    } else {
      Message::parse_delimited(&dict, &bytes, self.delimiter)?
    };

    debug!("Decoded FIX message: {msg}");
    Ok(Some(msg))
  }
}

/// Where the first frame in a buffer is.
struct Frame {
  begin_string: std::ops::Range<usize>,
  /// The whole frame, through CheckSum's delimiter.
  len: usize,
}

/// Find the first frame's extent from its `8=...|9=...|` prefix. `None` if
/// the prefix has not fully arrived yet. A frame longer than `max` — or a
/// buffer that has grown past `max` without a prefix — is an error, so a peer
/// cannot make the caller buffer without bound.
fn frame(data: &[u8], delimiter: u8, max: usize) -> Result<Option<Frame>> {
  let too_long =
    || Error::invalid_message(format!("frame exceeds the {max} byte limit"));
  let incomplete = || {
    if data.len() > max {
      Err(too_long())
    } else {
      Ok(None)
    }
  };
  let Some(begin_end) = data.iter().position(|&c| c == delimiter) else {
    return incomplete();
  };
  let begin_string = data[..begin_end]
    .strip_prefix(b"8=")
    .ok_or_else(|| Error::invalid_message("Expected BeginString(8) first"))?;
  let begin_string = 2..2 + begin_string.len();

  let rest = &data[begin_end + 1..];
  let Some(length_end) = rest.iter().position(|&c| c == delimiter) else {
    return incomplete();
  };
  let body_length = rest[..length_end]
    .strip_prefix(b"9=")
    .ok_or_else(|| Error::invalid_message("Expected BodyLength(9) second"))?;
  let body_length = std::str::from_utf8(body_length)
    .ok()
    .and_then(|s| s.parse::<usize>().ok())
    .ok_or_else(|| Error::invalid_message("Invalid BodyLength(9)"))?;

  let header_len = begin_end + 1 + length_end + 1;
  // BodyLength comes off the wire: never trust it to stay in bounds.
  let len = header_len
    .checked_add(body_length)
    .and_then(|l| l.checked_add(CHECKSUM_FIELD_LEN))
    .filter(|&l| l <= max)
    .ok_or_else(too_long)?;
  Ok(Some(Frame { begin_string, len }))
}

/// Serialises [`Message`]s onto the wire, computing `BodyLength` and
/// `CheckSum`.
#[derive(Default, Clone)]
pub struct FixEncoder {
  delimiter: u8,
}

impl FixEncoder {
  pub fn new(delimiter: Option<u8>) -> Self {
    Self {
      delimiter: delimiter.unwrap_or(SOH),
    }
  }

  /// Append `msg`'s wire form to `dst`.
  ///
  /// Takes the message by reference: the caller usually still needs it, to hand
  /// to the application as a record of what was sent.
  pub fn encode(&mut self, msg: &Message, dst: &mut BytesMut) -> Result<()> {
    if self.delimiter == SOH {
      msg.encode(dst);
    } else {
      msg.encode_delimited(dst, self.delimiter);
    }
    debug!("Encoded FIX message: {msg}");
    Ok(())
  }
}
