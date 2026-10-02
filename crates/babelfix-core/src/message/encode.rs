//! Serialising: walk the tape, copy each field's bytes, frame with 8, 9 and 10.

use std::fmt;

use bytes::{BufMut, BytesMut};

use super::SOH;
use super::parse::checksum_of;
use super::tape::{Entry, Kind, Message, Seg, digits};

/// One piece of output: a run of field bytes from a segment, or a NumInGroup
/// field written from its count.
enum Piece<'a> {
  Bytes(&'a [u8]),
  Group { tag: u32, count: u64 },
}

impl Message {
  /// Append the message's wire form to `out`, computing `BodyLength(9)` and
  /// `CheckSum(10)`.
  ///
  /// A message parsed from the wire and not edited since is copied as it
  /// arrived.
  pub fn encode(&self, out: &mut BytesMut) {
    if let Some(wire) = self.wire() {
      out.extend_from_slice(wire);
      return;
    }
    self.encode_delimited(out, SOH);
  }

  /// [`encode`](Self::encode) with `delimiter` in place of SOH — `b'|'` for
  /// logs and tests. Data fields are written as they are.
  pub fn encode_delimited(&self, out: &mut BytesMut, delimiter: u8) {
    let body_len: usize = {
      let mut n = 0;
      self.pieces(|p| n += piece_len(&p));
      n
    };

    let start = out.len();
    out.reserve(body_len + 40);
    out.put_slice(b"8=");
    out.put_slice(self.dict.begin_string().as_bytes());
    out.put_u8(delimiter);
    out.put_slice(b"9=");
    put_uint(out, body_len as u64);
    out.put_u8(delimiter);

    // 8 and 9 so far.
    let mut fields = 2;
    if delimiter == SOH {
      self.pieces(|p| match p {
        Piece::Bytes(b) => out.put_slice(b),
        Piece::Group { tag, count } => put_group(out, tag, count, SOH),
      });
    } else {
      self.visit(|idx, e| {
        fields += 1;
        match e.kind {
          Kind::Group => {
            put_group(out, e.tag, self.live_instances(idx) as u64, delimiter)
          }
          _ => {
            let bytes = self.span_bytes(e);
            out.put_slice(&bytes[..bytes.len() - 1]);
            out.put_u8(delimiter);
          }
        }
      });
    }

    // The checksum is always the wire's: each delimiter counts as SOH.
    let checksum = soh_checksum(&out[start..], fields, delimiter);
    out.put_slice(b"10=");
    out.put_slice(&[
      b'0' + checksum / 100,
      b'0' + checksum / 10 % 10,
      b'0' + checksum % 10,
    ]);
    out.put_u8(delimiter);
  }

  /// The wire form as a new buffer.
  pub fn to_bytes(&self) -> bytes::Bytes {
    let mut out = BytesMut::new();
    self.encode(&mut out);
    out.freeze()
  }

  /// Visit the body's output pieces — everything between `9=...` and
  /// `10=...` — coalescing field bytes that are adjacent in the same segment.
  fn pieces<'a>(&'a self, mut f: impl FnMut(Piece<'a>)) {
    let mut run: Option<(Seg, usize, usize)> = None;
    let flush = |run: &mut Option<(Seg, usize, usize)>,
                 f: &mut dyn FnMut(Piece<'a>)| {
      if let Some((seg, a, b)) = run.take() {
        f(Piece::Bytes(&self.seg(seg)[a..b]));
      }
    };
    self.visit(|idx, e| match e.kind {
      Kind::Group => {
        flush(&mut run, &mut f);
        f(Piece::Group {
          tag: e.tag,
          count: self.live_instances(idx) as u64,
        });
      }
      _ => {
        let r = e.span_bytes();
        match &mut run {
          Some((seg, _, end)) if *seg == e.seg && *end == r.start => {
            *end = r.end
          }
          _ => {
            flush(&mut run, &mut f);
            run = Some((e.seg, r.start, r.end));
          }
        }
      }
    });
    flush(&mut run, &mut f);
  }

  /// Visit the fields and groups that are written between `9=` and `10=`, in
  /// order: everything but BeginString, BodyLength and CheckSum, and gaps,
  /// instances, and groups with no non-empty instances.
  fn visit<'a>(&'a self, mut f: impl FnMut(u32, &'a Entry)) {
    // MsgType is the third field (after 8 and 9, which are written
    // separately). A parsed or built message already has it first; a fragment
    // may not.
    let msg_type = self.find_in(0, self.body_start, 0, 35, None);
    if let Some(i) = msg_type {
      f(i, &self.tape[i as usize]);
    }
    let mut i = 0u32;
    while (i as usize) < self.tape.len() {
      let e = &self.tape[i as usize];
      match e.kind {
        Kind::Gap => {
          i += e.span();
          continue;
        }
        Kind::Instance => {}
        Kind::Group => {
          if self.live_instances(i) == 0 {
            i += e.span();
            continue;
          }
          f(i, e);
        }
        _ if e.depth == 0 && matches!(e.tag, 8..=10 | 35) => {}
        _ => f(i, e),
      }
      i += 1;
    }
  }

  /// Instances that hold at least one field. An instance with nothing in it is
  /// not written, and not counted: empty means absent.
  pub(crate) fn live_instances(&self, idx: u32) -> usize {
    let end = self.next_sibling(idx);
    let mut i = idx + 1;
    let mut n = 0;
    while i < end {
      if self.instance_writes(i) {
        n += 1;
      }
      i = self.next_sibling(i);
    }
    n
  }

  /// Whether the instance at `idx` writes anything: a field, or a nested group
  /// that does. An instance holding only empty groups writes nothing.
  fn instance_writes(&self, idx: u32) -> bool {
    let end = self.next_sibling(idx);
    let mut i = idx + 1;
    while i < end {
      let e = &self.tape[i as usize];
      match e.kind {
        Kind::Group if self.live_instances(i) > 0 => return true,
        Kind::Group | Kind::Gap | Kind::Instance => {}
        _ => return true,
      }
      i = self.next_sibling(i);
    }
    false
  }
}

/// The checksum `bytes` would have with each of its `fields` delimiters SOH.
pub(crate) fn soh_checksum(bytes: &[u8], fields: usize, delimiter: u8) -> u8 {
  let sum = checksum_of(bytes) as i64
    - (fields as i64 * (delimiter as i64 - SOH as i64));
  sum.rem_euclid(256) as u8
}

fn piece_len(p: &Piece<'_>) -> usize {
  match p {
    Piece::Bytes(b) => b.len(),
    Piece::Group { tag, count } => digits(*tag as u64) + 1 + digits(*count) + 1,
  }
}

fn put_uint(out: &mut BytesMut, v: u64) {
  let n = super::tape::digits(v);
  let start = out.len();
  out.resize(start + n, 0);
  super::types::write_uint(&mut out[start..], v);
}

fn put_group(out: &mut BytesMut, tag: u32, count: u64, delimiter: u8) {
  put_uint(out, tag as u64);
  out.put_u8(b'=');
  put_uint(out, count);
  out.put_u8(delimiter);
}

/// `8=FIX.4.4|9=...|35=D|...|10=...|`, with `|` for SOH.
impl fmt::Display for Message {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut out = BytesMut::new();
    self.encode_delimited(&mut out, b'|');
    write!(f, "{}", super::types::FixStr::new(&out))
  }
}

/// One field per line, indented by depth, with names from the dictionary:
/// `{:#?}`. Plain `{:?}` is the `Display` form.
impl fmt::Debug for Message {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if !f.alternate() {
      return write!(f, "Message({self})");
    }
    for c in self.walk() {
      // An instance's marker sits at its depth; its fields one level in.
      let level = match c.kind() {
        super::EntryKind::Instance => c.depth(),
        _ if c.depth() > 0 => c.depth() + 1,
        _ => 0,
      };
      let indent = "  ".repeat(level as usize);
      let tag = c.tag();
      let name = self.dict.field_name(tag).unwrap_or("?");
      match c.kind() {
        super::EntryKind::Instance => {
          writeln!(f, "{indent}[{}]", c.instance_index().unwrap_or(0))?
        }
        super::EntryKind::Group => writeln!(
          f,
          "{indent}{tag} {name} = {} instance(s)",
          c.count().unwrap_or(0)
        )?,
        super::EntryKind::Data => writeln!(
          f,
          "{indent}{tag} {name} = <{} bytes>",
          c.value().map_or(0, <[u8]>::len)
        )?,
        _ => {
          let value = c.value().unwrap_or_default();
          let s = super::types::FixStr::new(value);
          match self.dict.codeset_name(tag, value) {
            Some(code) => writeln!(f, "{indent}{tag} {name} = {s} ({code})")?,
            None => writeln!(f, "{indent}{tag} {name} = {s}")?,
          }
        }
      }
    }
    Ok(())
  }
}

/// Two messages are equal when they have the same fields with the same values
/// in the same structure — wherever their bytes happen to live.
impl PartialEq for Message {
  fn eq(&self, other: &Self) -> bool {
    if self.dict.begin_string() != other.dict.begin_string() {
      return false;
    }
    let mut a = self.walk().filter(|c| !matches!(c.tag(), 9 | 10));
    let mut b = other.walk().filter(|c| !matches!(c.tag(), 9 | 10));
    loop {
      match (a.next(), b.next()) {
        (None, None) => return true,
        (Some(x), Some(y)) => {
          if x.kind() != y.kind()
            || x.tag() != y.tag()
            || x.depth() != y.depth()
            || x.value() != y.value()
            || x.count() != y.count()
          {
            return false;
          }
        }
        _ => return false,
      }
    }
  }
}

impl Eq for Message {}
