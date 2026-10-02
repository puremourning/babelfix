//! The message representation: a flat tape of entries over two byte segments.
//!
//! A message is a list of `Entry`s in wire order. A field entry records where
//! its `tag=value<SOH>` bytes are; a repeating group is a `Group` entry followed
//! by its `Instance`s, each followed by its fields — the structure of a tree,
//! laid out flat, with no allocation per group. Group, instance and gap entries
//! carry a *span*: how many entries they cover, themselves included, so a reader
//! skips a whole group in one step.
//!
//! ```text
//! 35=D|453=2|448=A|447=D|448=B|447=D|55=X
//!
//!   0  35   Field
//!   1  453  Group     span=7  count=2
//!   2       Instance  span=3
//!   3  448  Field
//!   4  447  Field
//!   5       Instance  span=3
//!   6  448  Field
//!   7  447  Field
//!   8  55   Field
//! ```
//!
//! Field bytes live in one of two segments: `wire`, the received message,
//! shared with the codec and never copied; or `arena`, where building and
//! editing append. Editing a field appends its new bytes to the arena and
//! repoints the entry, so a received message can be edited without copying the
//! rest of it.
//!
//! The tape has three regions — header, body, trailer — because the spec
//! requires fields in that order (TagValue §4.3.3). At the end of the header
//! sits a *gap*: `HEADER_GAP` (20) reserved slots, so that the header fields the
//! session adds (CompIDs, MsgSeqNum, SendingTime) and those a replay adds
//! (PossDupFlag, OrigSendingTime) never shift the body.

use std::sync::Arc;

use bytes::Bytes;

use super::dict::{Dictionary, MsgIdx};

/// Slots reserved at the end of the header.
pub(crate) const HEADER_GAP: u32 = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
  /// Unused slots. A gap's first entry carries the gap's span; the rest are
  /// never looked at.
  #[default]
  Gap,
  Field,
  /// The Length field of a data field. Always immediately followed by its
  /// `Data` entry.
  DataLen,
  Data,
  /// A NumInGroup field. `off` holds the instance count; the field's text is
  /// written from it when serialising.
  Group,
  Instance,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Seg {
  #[default]
  Wire,
  Arena,
}

/// One tape entry. 16 bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct Entry {
  pub tag: u32,
  pub kind: Kind,
  pub depth: u8,
  pub seg: Seg,
  /// Where the value starts, relative to `off`: the tag's digits plus `=`. A
  /// `u32` tag has at most 10 digits, so this is at most 11.
  pub val_off: u8,
  /// Fields: where `tag=` starts in `seg`. Groups: the instance count.
  pub off: u32,
  /// Fields: the value's length. Groups, instances and gaps: the span.
  pub len: u32,
}

const _: () = assert!(std::mem::size_of::<Entry>() == 16);

impl Entry {
  pub fn gap(span: u32) -> Self {
    Self {
      kind: Kind::Gap,
      len: span,
      ..Default::default()
    }
  }

  pub fn is_field(&self) -> bool {
    matches!(self.kind, Kind::Field | Kind::DataLen | Kind::Data)
  }

  /// Whether this entry names a tag at its level: a field or a group.
  pub fn is_tagged(&self) -> bool {
    !matches!(self.kind, Kind::Gap | Kind::Instance)
  }

  /// How many entries this one covers, itself included.
  pub fn span(&self) -> u32 {
    match self.kind {
      Kind::Gap | Kind::Group | Kind::Instance => self.len.max(1),
      _ => 1,
    }
  }

  /// The `tag=value<SOH>` byte range, for fields.
  pub fn span_bytes(&self) -> std::ops::Range<usize> {
    let start = self.off as usize;
    start..start + self.val_off as usize + self.len as usize + 1
  }

  pub fn value_range(&self) -> std::ops::Range<usize> {
    let start = self.off as usize + self.val_off as usize;
    start..start + self.len as usize
  }
}

/// The number of decimal digits in `n`.
pub(crate) fn digits(n: u64) -> usize {
  n.checked_ilog10().map_or(1, |d| d as usize + 1)
}

/// Which of the three regions a block or position is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Region {
  Header,
  Body,
  Trailer,
}

/// A FIX message: parsed, built, or edited. See the module documentation for
/// the representation, and [`crate::message`] for the API.
#[derive(Clone)]
pub struct Message {
  pub(crate) dict: Arc<Dictionary>,
  pub(crate) msg_def: Option<MsgIdx>,
  pub(crate) wire: Bytes,
  pub(crate) arena: Vec<u8>,
  pub(crate) tape: Vec<Entry>,
  pub(crate) body_start: u32,
  pub(crate) trailer_start: u32,
  /// `wire` is exactly this message's encoding: it was parsed from SOH-delimited
  /// bytes and has not been edited since.
  pub(crate) clean: bool,
}

impl Message {
  /// An empty message of type `msg_type`, holding just `BeginString(8)` and
  /// `MsgType(35)`. The session fills in the rest of the header when it sends.
  pub fn new(dict: &Arc<Dictionary>, msg_type: impl AsRef<[u8]>) -> Message {
    let mut msg = Message {
      dict: dict.clone(),
      msg_def: None,
      wire: Bytes::new(),
      arena: Vec::with_capacity(256),
      tape: Vec::with_capacity(64),
      body_start: 0,
      trailer_start: 0,
      clean: false,
    };
    msg.reset(msg_type.as_ref());
    msg
  }

  /// Empty the message for reuse as a new `msg_type`, keeping its allocations.
  pub fn clear(&mut self, msg_type: impl AsRef<[u8]>) {
    self.reset(msg_type.as_ref());
  }

  fn reset(&mut self, msg_type: &[u8]) {
    self.wire = Bytes::new();
    self.arena.clear();
    self.tape.clear();
    self.clean = false;
    let begin_string = self.dict.clone();
    let begin_string = begin_string.begin_string().as_bytes();
    let e = self.append_field(8, begin_string);
    self.tape.push(e);
    let e = self.append_field(35, msg_type);
    self.tape.push(e);
    self.push_gap();
    self.body_start = self.tape.len() as u32;
    self.trailer_start = self.body_start;
    self.msg_def = self.dict.message(msg_type);
  }

  pub(crate) fn push_gap(&mut self) {
    self.tape.push(Entry::gap(HEADER_GAP));
    self
      .tape
      .resize(self.tape.len() + HEADER_GAP as usize - 1, Entry::default());
  }

  /// The dictionary this message was parsed or built with.
  pub fn dict(&self) -> &Arc<Dictionary> {
    &self.dict
  }

  /// Append `tag=value<SOH>` to the arena; returns an entry for it.
  pub(crate) fn append_field(&mut self, tag: u32, value: &[u8]) -> Entry {
    let off = self.start_field(tag);
    self.arena.extend_from_slice(value);
    self.finish_field(tag, off)
  }

  /// Start writing a field into the arena: `tag=`. Returns its offset.
  pub(crate) fn start_field(&mut self, tag: u32) -> usize {
    let off = self.arena.len();
    let mut w = super::types::ValueWriter::new(&mut self.arena);
    w.put_uint(tag as u64);
    w.put_u8(b'=');
    off
  }

  /// Finish a field started with [`start_field`](Self::start_field) once its
  /// value has been written.
  pub(crate) fn finish_field(&mut self, tag: u32, off: usize) -> Entry {
    let val_off = digits(tag as u64) + 1;
    let len = self.arena.len() - off - val_off;
    self.arena.push(super::SOH);
    Entry {
      tag,
      kind: Kind::Field,
      depth: 0,
      seg: Seg::Arena,
      val_off: val_off as u8,
      off: off as u32,
      len: len as u32,
    }
  }

  pub(crate) fn seg(&self, seg: Seg) -> &[u8] {
    match seg {
      Seg::Wire => &self.wire,
      Seg::Arena => &self.arena,
    }
  }

  /// A field entry's value bytes.
  pub(crate) fn value(&self, e: &Entry) -> &[u8] {
    &self.seg(e.seg)[e.value_range()]
  }

  /// A field entry's `tag=value<SOH>` bytes.
  pub(crate) fn span_bytes(&self, e: &Entry) -> &[u8] {
    &self.seg(e.seg)[e.span_bytes()]
  }

  /// The tape range of a region's contents.
  pub(crate) fn region_range(&self, region: Region) -> (u32, u32) {
    match region {
      Region::Header => (0, self.body_start),
      Region::Body => (self.body_start, self.trailer_start),
      Region::Trailer => (self.trailer_start, self.tape.len() as u32),
    }
  }

  pub(crate) fn region_of(&self, idx: u32) -> Region {
    if idx < self.body_start {
      Region::Header
    } else if idx < self.trailer_start {
      Region::Body
    } else {
      Region::Trailer
    }
  }

  /// Index of the next sibling of the entry at `idx`.
  pub(crate) fn next_sibling(&self, idx: u32) -> u32 {
    idx + self.tape[idx as usize].span()
  }

  /// Find `tag` among the direct children of `[start, end)`, starting the
  /// search at `from` and wrapping round to `start`. `from` is trusted only if
  /// it is a child boundary of this block; otherwise the search starts at
  /// `start`, so a bad hint costs time, never correctness.
  pub(crate) fn find_in(
    &self,
    start: u32,
    end: u32,
    depth: u8,
    tag: u32,
    from: Option<u32>,
  ) -> Option<u32> {
    let from = from
      .filter(|&f| f > start && f < end && self.is_boundary(f, depth))
      .unwrap_or(start);
    self.scan(from, end, tag).or_else(|| {
      if from > start {
        self.scan(start, from, tag)
      } else {
        None
      }
    })
  }

  fn scan(&self, mut i: u32, end: u32, tag: u32) -> Option<u32> {
    while i < end {
      let e = &self.tape[i as usize];
      if e.tag == tag && e.is_tagged() {
        return Some(i);
      }
      i += e.span();
    }
    None
  }

  /// Whether `idx` is a direct child of a block at `depth`.
  fn is_boundary(&self, idx: u32, depth: u8) -> bool {
    let e = &self.tape[idx as usize];
    e.depth == depth && e.kind != Kind::Instance
  }

  /// The gap at the end of the header, if there is one.
  pub(crate) fn header_gap(&self) -> Option<u32> {
    let mut i = 0;
    while i < self.body_start {
      let e = &self.tape[i as usize];
      if e.kind == Kind::Gap && i + e.span() == self.body_start {
        return Some(i);
      }
      i += e.span();
    }
    None
  }
}
