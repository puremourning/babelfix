//! Reading: blocks, groups and cursors over a message's tape.

use std::fmt;
use std::marker::PhantomData;

use bytes::Bytes;

use super::error::{FieldError, ValueError};
use super::path::FieldPath;
use super::tape::{Kind, Message, Region};
use super::types::{
  Field, FieldType, FromFix, GroupTag, Scope, Tag, Unscoped, Within,
};

/// A position in a message, for hinted lookups ([`Block::get_from`]). Produced
/// by [`Cursor::pos`], [`Group::end`] and [`Block::start`]/[`Block::end`].
///
/// A hint is only ever a starting point: a search from a wrong or stale hint
/// still finds the field, just less quickly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pos(pub(crate) u32);

/// A set of fields in which each tag appears at most once: a region of the
/// message (header, body, trailer) or one instance of a repeating group.
///
/// Its scope `S` says which fields it holds: [`get`](Self::get) accepts only
/// fields [`Within`] `S`. An [`Unscoped`] block accepts any field.
pub struct Block<'a, S = Unscoped> {
  pub(crate) msg: &'a Message,
  pub(crate) start: u32,
  pub(crate) end: u32,
  pub(crate) depth: u8,
  pub(crate) _s: PhantomData<fn() -> S>,
}

impl<S> Clone for Block<'_, S> {
  fn clone(&self) -> Self {
    *self
  }
}
impl<S> Copy for Block<'_, S> {}

impl<'a> Block<'a> {
  pub(crate) fn region(msg: &'a Message, region: Region) -> Self {
    let (start, end) = msg.region_range(region);
    Self {
      msg,
      start,
      end,
      depth: 0,
      _s: PhantomData,
    }
  }

  /// The block holding the contents of the instance entry at `idx`.
  pub(crate) fn instance(msg: &'a Message, idx: u32) -> Self {
    let e = &msg.tape[idx as usize];
    Self {
      msg,
      start: idx + 1,
      end: idx + e.span(),
      depth: e.depth,
      _s: PhantomData,
    }
  }
}

impl<'a, S> Block<'a, S> {
  /// The same block, as scope `T`; the caller vouches that it holds `T`.
  pub(crate) fn scoped<T>(self) -> Block<'a, T> {
    Block {
      msg: self.msg,
      start: self.start,
      end: self.end,
      depth: self.depth,
      _s: PhantomData,
    }
  }

  /// The same block, unchecked: it accepts any field.
  pub fn unscoped(self) -> Block<'a> {
    self.scoped()
  }

  fn find(&self, tag: u32, hint: Option<Pos>) -> Option<u32> {
    self
      .msg
      .find_in(self.start, self.end, self.depth, tag, hint.map(|p| p.0))
  }

  fn field_bytes(&self, tag: u32, hint: Option<Pos>) -> Option<&'a [u8]> {
    let idx = self.find(tag, hint)?;
    let e = &self.msg.tape[idx as usize];
    e.is_field().then(|| self.msg.value(e))
  }

  fn decode<M: FieldType>(
    tag: u32,
    raw: Option<&'a [u8]>,
  ) -> Result<Option<M::Value<'a>>, FieldError> {
    raw
      .map(|raw| M::decode(raw).map_err(|e| FieldError::value(tag, e)))
      .transpose()
  }

  /// The field's decoded value: `Ok(None)` if it is absent, `Err` if it is
  /// present but malformed.
  pub fn get<M: FieldType>(
    &self,
    field: Field<M, impl Within<S>>,
  ) -> Result<Option<M::Value<'a>>, FieldError> {
    Self::decode::<M>(field.tag(), self.field_bytes(field.tag(), None))
  }

  /// [`get`](Self::get), searching from `hint` first.
  pub fn get_from<M: FieldType>(
    &self,
    field: Field<M, impl Within<S>>,
    hint: Pos,
  ) -> Result<Option<M::Value<'a>>, FieldError> {
    Self::decode::<M>(field.tag(), self.field_bytes(field.tag(), Some(hint)))
  }

  /// The field's decoded value; absent is an error too.
  pub fn req<M: FieldType>(
    &self,
    field: Field<M, impl Within<S>>,
  ) -> Result<M::Value<'a>, FieldError> {
    self.get(field)?.ok_or(FieldError::missing(field.tag()))
  }

  /// The field's value converted to `T`: `get_as::<Dec19>(Price)`.
  pub fn get_as<T: FromFix<M>, M: FieldType>(
    &self,
    field: Field<M, impl Within<S>>,
  ) -> Result<Option<T>, FieldError> {
    self
      .get(field)?
      .map(|v| T::from_fix(v).map_err(|e| FieldError::value(field.tag(), e)))
      .transpose()
  }

  /// [`get_as`](Self::get_as); absent is an error too.
  pub fn req_as<T: FromFix<M>, M: FieldType>(
    &self,
    field: Field<M, impl Within<S>>,
  ) -> Result<T, FieldError> {
    self.get_as(field)?.ok_or(FieldError::missing(field.tag()))
  }

  /// The field's value bytes, undecoded. `None` if absent, or if `tag` is a
  /// group.
  pub fn raw(&self, tag: impl Tag) -> Option<&'a [u8]> {
    self.field_bytes(tag.tag(), None)
  }

  /// [`raw`](Self::raw), searching from `hint` first.
  pub fn raw_from(&self, tag: impl Tag, hint: Pos) -> Option<&'a [u8]> {
    self.field_bytes(tag.tag(), Some(hint))
  }

  /// Whether the block holds the field or group.
  pub fn has(&self, tag: impl Tag) -> bool {
    self.find(tag.tag(), None).is_some()
  }

  /// The repeating group: empty if absent.
  pub fn group<G: GroupTag<S>>(&self, group: G) -> Group<'a, G::Instance> {
    let idx = self
      .find(group.tag(), None)
      .filter(|&i| self.msg.tape[i as usize].kind == Kind::Group);
    Group {
      msg: self.msg,
      idx,
      _s: PhantomData,
    }
  }

  /// The block's own fields and groups, in order. Groups are single entries
  /// here; go into them with [`Cursor::down`] or [`group`](Self::group).
  pub fn fields(&self) -> impl Iterator<Item = Cursor<'a>> + 'a {
    let msg = self.msg;
    let end = self.end;
    let mut i = self.start;
    std::iter::from_fn(move || {
      while i < end {
        let idx = i;
        let e = &msg.tape[idx as usize];
        i += e.span();
        if e.kind != Kind::Gap {
          return Some(Cursor { msg, idx });
        }
      }
      None
    })
  }

  /// The block's first entry, if it has one.
  pub fn first(&self) -> Option<Cursor<'a>> {
    self.fields().next()
  }

  pub fn start(&self) -> Pos {
    Pos(self.start)
  }

  pub fn end(&self) -> Pos {
    Pos(self.end)
  }

  pub fn is_empty(&self) -> bool {
    self.first().is_none()
  }
}

impl<S> fmt::Debug for Block<'_, S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut list = f.debug_list();
    for c in self.fields() {
      list.entry(&c);
    }
    list.finish()
  }
}

/// A repeating group within a block, whose instances are blocks of scope `S`.
/// Absent groups are empty.
pub struct Group<'a, S = Unscoped> {
  pub(crate) msg: &'a Message,
  pub(crate) idx: Option<u32>,
  pub(crate) _s: PhantomData<fn() -> S>,
}

impl<S> Clone for Group<'_, S> {
  fn clone(&self) -> Self {
    *self
  }
}
impl<S> Copy for Group<'_, S> {}

impl<'a, S> Group<'a, S> {
  /// Number of instances.
  pub fn len(&self) -> usize {
    self
      .idx
      .map_or(0, |i| self.msg.tape[i as usize].off as usize)
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// The `n`th instance.
  pub fn get(&self, n: usize) -> Option<Block<'a, S>> {
    self.iter().nth(n)
  }

  pub fn iter(&self) -> GroupIter<'a, S> {
    let (next, end) = match self.idx {
      Some(idx) => (idx + 1, self.msg.next_sibling(idx)),
      None => (0, 0),
    };
    GroupIter {
      msg: self.msg,
      next,
      end,
      _s: PhantomData,
    }
  }

  /// The position just after the group: a hint for fields that follow it.
  pub fn end(&self) -> Pos {
    Pos(self.idx.map_or(0, |i| self.msg.next_sibling(i)))
  }

  /// The group's own position, if it is present.
  pub fn pos(&self) -> Option<Pos> {
    self.idx.map(Pos)
  }
}

impl<'a, S> IntoIterator for Group<'a, S> {
  type Item = Block<'a, S>;
  type IntoIter = GroupIter<'a, S>;
  fn into_iter(self) -> GroupIter<'a, S> {
    self.iter()
  }
}

/// The instances of a [`Group`].
pub struct GroupIter<'a, S = Unscoped> {
  msg: &'a Message,
  next: u32,
  end: u32,
  _s: PhantomData<fn() -> S>,
}

impl<'a, S> Iterator for GroupIter<'a, S> {
  type Item = Block<'a, S>;
  fn next(&mut self) -> Option<Block<'a, S>> {
    if self.next >= self.end {
      return None;
    }
    let block = Block::instance(self.msg, self.next).scoped();
    self.next = self.msg.next_sibling(self.next);
    Some(block)
  }
}

impl<S> fmt::Debug for Group<'_, S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_list().entries(self.iter()).finish()
  }
}

/// What a [`Cursor`] is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
  /// An ordinary field.
  Field,
  /// The Length field of a data field; always followed by the data field.
  DataLength,
  /// A data field: arbitrary bytes, SOH included.
  Data,
  /// A NumInGroup field: the start of a repeating group.
  Group,
  /// One instance of a repeating group.
  Instance,
}

/// A position in a message: where you are now, for navigating. Borrowed from
/// the message and valid only while that borrow lives; for an address that
/// survives editing, take its [`path`](Self::path).
#[derive(Clone, Copy)]
pub struct Cursor<'a> {
  pub(crate) msg: &'a Message,
  pub(crate) idx: u32,
}

impl<'a> Cursor<'a> {
  fn entry(&self) -> &'a super::tape::Entry {
    &self.msg.tape[self.idx as usize]
  }

  pub fn kind(&self) -> EntryKind {
    match self.entry().kind {
      Kind::Field | Kind::Gap => EntryKind::Field,
      Kind::DataLen => EntryKind::DataLength,
      Kind::Data => EntryKind::Data,
      Kind::Group => EntryKind::Group,
      Kind::Instance => EntryKind::Instance,
    }
  }

  /// The tag; for an instance, its group's NumInGroup tag.
  pub fn tag(&self) -> u32 {
    match self.entry().kind {
      Kind::Instance => self.up().map_or(0, |g| g.tag()),
      _ => self.entry().tag,
    }
  }

  /// A field's value bytes. `None` for groups and instances.
  pub fn value(&self) -> Option<&'a [u8]> {
    let e = self.entry();
    e.is_field().then(|| self.msg.value(e))
  }

  /// A field's value, decoded as datatype `M`.
  pub fn decode<M: FieldType>(&self) -> Result<M::Value<'a>, FieldError> {
    let raw = self
      .value()
      .ok_or(FieldError::value(self.tag(), ValueError::Malformed))?;
    M::decode(raw).map_err(|e| FieldError::value(self.tag(), e))
  }

  /// A group's instance count. `None` for anything else.
  pub fn count(&self) -> Option<usize> {
    let e = self.entry();
    (e.kind == Kind::Group).then_some(e.off as usize)
  }

  /// Nesting depth: 0 for the message's own fields, 1 inside a group instance,
  /// and so on.
  pub fn depth(&self) -> u8 {
    self.entry().depth
  }

  pub fn pos(&self) -> Pos {
    Pos(self.idx)
  }

  /// The block this position is in.
  pub fn block(&self) -> Block<'a> {
    match self.enclosing() {
      Some(instance) => Block::instance(self.msg, instance),
      None => Block::region(self.msg, self.msg.region_of(self.idx)),
    }
  }

  /// The index of the entry immediately enclosing this one: the instance a
  /// field is in, or the group an instance is in.
  fn enclosing(&self) -> Option<u32> {
    let (start, _) = self.msg.region_range(self.msg.region_of(self.idx));
    let mut i = start;
    let mut found = None;
    // Walk down the tree towards idx: each step either skips a sibling or
    // enters the group/instance that contains idx.
    while i < self.idx {
      let e = &self.msg.tape[i as usize];
      let next = i + e.span();
      if next > self.idx {
        found = Some(i);
        i += 1;
      } else {
        i = next;
      }
    }
    found
  }

  /// The next entry in the same block, skipping over groups.
  pub fn next(&self) -> Option<Cursor<'a>> {
    let end = self.block().end;
    let mut i = self.msg.next_sibling(self.idx);
    while i < end {
      if self.msg.tape[i as usize].kind != Kind::Gap {
        return Some(Cursor {
          msg: self.msg,
          idx: i,
        });
      }
      i = self.msg.next_sibling(i);
    }
    None
  }

  /// The previous entry in the same block.
  pub fn prev(&self) -> Option<Cursor<'a>> {
    self
      .block()
      .fields()
      .take_while(|c| c.idx < self.idx)
      .last()
  }

  /// Into a group (its first instance) or an instance (its first field).
  pub fn down(&self) -> Option<Cursor<'a>> {
    let e = self.entry();
    match e.kind {
      Kind::Group | Kind::Instance if e.span() > 1 => Some(Cursor {
        msg: self.msg,
        idx: self.idx + 1,
      }),
      _ => None,
    }
  }

  /// Out to the enclosing instance (from a field) or group (from an instance).
  pub fn up(&self) -> Option<Cursor<'a>> {
    self.enclosing().map(|idx| Cursor { msg: self.msg, idx })
  }

  /// The instance's index within its group.
  pub fn instance_index(&self) -> Option<usize> {
    if self.entry().kind != Kind::Instance {
      return None;
    }
    let group = self.up()?;
    let mut i = group.idx + 1;
    let mut n = 0;
    while i < self.idx {
      i = self.msg.next_sibling(i);
      n += 1;
    }
    Some(n)
  }

  /// A stable address for this position, which survives edits elsewhere.
  pub fn path(&self) -> FieldPath {
    let mut chain = Vec::new();
    let mut c = *self;
    let tag = match self.entry().kind {
      Kind::Instance => None,
      _ => Some(self.entry().tag),
    };
    if self.entry().kind == Kind::Instance {
      chain.push((self.tag(), self.instance_index().unwrap_or(0) as u32));
    }
    while let Some(up) = c.up() {
      if up.entry().kind == Kind::Instance {
        chain.push((up.tag(), up.instance_index().unwrap_or(0) as u32));
      }
      c = up;
    }
    chain.reverse();
    FieldPath::new(self.msg.region_of(self.idx), chain, tag)
  }
}

impl fmt::Debug for Cursor<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self.entry().kind {
      Kind::Group => {
        write!(f, "{}=[{}]", self.tag(), self.count().unwrap_or(0))
      }
      Kind::Instance => {
        write!(f, "{}[{}]", self.tag(), self.instance_index().unwrap_or(0))
      }
      Kind::Data => write!(f, "{}=<{} bytes>", self.tag(), self.entry().len),
      _ => write!(
        f,
        "{}={}",
        self.tag(),
        super::types::FixStr::new(self.value().unwrap_or_default())
      ),
    }
  }
}

impl Message {
  pub fn header(&self) -> Block<'_> {
    Block::region(self, Region::Header)
  }

  pub fn body(&self) -> Block<'_> {
    Block::region(self, Region::Body)
  }

  pub fn trailer(&self) -> Block<'_> {
    Block::region(self, Region::Trailer)
  }

  /// The header and body together, as one block: for a field when you do not
  /// care which it is in.
  fn header_and_body(&self) -> Block<'_> {
    Block {
      msg: self,
      start: 0,
      end: self.trailer_start,
      depth: 0,
      _s: PhantomData,
    }
  }

  /// A top-level field from the header or the body.
  pub fn find<M: FieldType>(
    &self,
    field: Field<M, impl Scope>,
  ) -> Result<Option<M::Value<'_>>, FieldError> {
    self.header_and_body().get(field)
  }

  /// [`find`](Self::find), searching from `hint` first.
  pub fn find_from<M: FieldType>(
    &self,
    field: Field<M, impl Scope>,
    hint: Pos,
  ) -> Result<Option<M::Value<'_>>, FieldError> {
    self.header_and_body().get_from(field, hint)
  }

  /// The `MsgType(35)`. The parser guarantees it is present and ASCII.
  pub fn msg_type(&self) -> &str {
    self
      .header()
      .raw(35)
      .and_then(|v| std::str::from_utf8(v).ok())
      .unwrap_or_default()
  }

  /// The `BeginString(8)`.
  pub fn begin_string(&self) -> &str {
    self.dict.begin_string()
  }

  /// Whether this is a session-level (admin) message: Heartbeat, TestRequest,
  /// ResendRequest, Reject, SequenceReset, Logout, Logon or XMLnonFIX (FIX
  /// Session Layer §9). Everything else — BusinessMessageReject and market
  /// data included — is an application message.
  pub fn is_admin(&self) -> bool {
    matches!(
      self.msg_type(),
      "0" | "1" | "2" | "3" | "4" | "5" | "A" | "n"
    )
  }

  /// The received bytes, if this message was parsed from SOH-delimited wire
  /// bytes and has not been edited since: exactly what arrived.
  pub fn wire(&self) -> Option<&Bytes> {
    (self.clean && !self.wire.is_empty()).then_some(&self.wire)
  }

  /// Every entry, depth first, in wire order.
  pub fn walk(&self) -> impl Iterator<Item = Cursor<'_>> {
    let mut i = 0;
    std::iter::from_fn(move || {
      while (i as usize) < self.tape.len() {
        let idx = i;
        let e = &self.tape[idx as usize];
        i += match e.kind {
          Kind::Gap => e.span(),
          _ => 1,
        };
        if e.kind != Kind::Gap {
          return Some(Cursor { msg: self, idx });
        }
      }
      None
    })
  }

  /// Resolve a path to a position, if it still exists.
  pub fn cursor(&self, path: &FieldPath) -> Option<Cursor<'_>> {
    self.resolve(path).map(|idx| Cursor { msg: self, idx })
  }

  pub(crate) fn resolve(&self, path: &FieldPath) -> Option<u32> {
    let mut block = Block::region(self, path.region());
    let mut last = None;
    for &(group, n) in path.instances() {
      let g = block.group(group);
      let instance = g.iter().nth(n as usize)?;
      last = Some(instance.start - 1);
      block = instance;
    }
    match path.tag() {
      Some(tag) => block.find(tag, None),
      None => last,
    }
  }
}
