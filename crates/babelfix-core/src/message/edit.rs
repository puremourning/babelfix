//! Building and editing: mutable views over a message's tape.
//!
//! An edit never moves message bytes. A new or changed value is appended to the
//! arena and an entry points at it; inserting a field splices one 16-byte entry
//! into the tape (or fills a slot in the header gap), and the spans of the
//! groups and instances around it grow by one.
//!
//! Mutable views record only *which* block they are — a region, or the index of
//! an instance entry — and work out its extent from the tape when they need it.
//! Entries before an insertion point never move, so a view's own index stays
//! valid however much is inserted into it.

use super::SOH;
use super::dict::{FieldKind, GroupIdx};
use super::error::{FieldError, FieldErrorKind, ValueError};
use super::path::FieldPath;
use super::tape::{Entry, Kind, Message, Region, Seg};
use super::types::{Field, FieldType, Tag, ToFix, ValueWriter};
use super::view::{Block, Cursor};

/// Which block a mutable view edits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
  Region(Region),
  /// The instance entry at this index.
  Instance(u32),
}

/// A block (a region, or a group instance) being built or edited.
pub struct BlockMut<'a> {
  msg: &'a mut Message,
  owner: Owner,
}

/// A repeating group being built or edited.
pub struct GroupMut<'a> {
  msg: &'a mut Message,
  /// The block the group is in.
  parent: Owner,
  tag: u32,
  /// The Group entry, once it exists.
  idx: Option<u32>,
}

/// A position in a message, for editing. See [`Cursor`].
pub struct CursorMut<'a> {
  msg: &'a mut Message,
  idx: u32,
}

impl Message {
  pub fn header_mut(&mut self) -> BlockMut<'_> {
    BlockMut {
      msg: self,
      owner: Owner::Region(Region::Header),
    }
  }

  pub fn body_mut(&mut self) -> BlockMut<'_> {
    BlockMut {
      msg: self,
      owner: Owner::Region(Region::Body),
    }
  }

  /// Resolve a path for editing, if it still exists.
  pub fn cursor_mut(&mut self, path: &FieldPath) -> Option<CursorMut<'_>> {
    let idx = self.resolve(path)?;
    Some(CursorMut { msg: self, idx })
  }

  /// Visit every field value (not data fields, and not BeginString, BodyLength,
  /// MsgType or CheckSum); where `f` returns new bytes, the value is replaced.
  pub fn values_mut(
    &mut self,
    mut f: impl FnMut(u32, &[u8]) -> Option<Vec<u8>>,
  ) {
    for i in 0..self.tape.len() {
      let e = self.tape[i];
      if e.kind != Kind::Field || matches!(e.tag, 8 | 9 | 10 | 35) {
        continue;
      }
      let Some(new) = f(e.tag, self.value(&e)) else {
        continue;
      };
      if new.is_empty() || new.contains(&SOH) {
        debug_assert!(false, "values_mut: invalid value for tag {}", e.tag);
        continue;
      }
      let mut fresh = self.append_field(e.tag, &new);
      fresh.depth = e.depth;
      self.tape[i] = fresh;
      self.clean = false;
    }
  }

  /// Write `SendingTime(52)` — `width` bytes, formatted by `write` — straight
  /// into the slot the session reserved in the header, so stamping allocates
  /// nothing, moves nothing and copies nothing. Without such a slot, it is set
  /// like any other field.
  pub(crate) fn stamp_sending_time(
    &mut self,
    width: usize,
    write: impl FnOnce(&mut [u8]) -> usize,
  ) {
    let (start, end) = self.region_range(Region::Header);
    if let Some(i) = self.find_in(start, end, 0, 52, None) {
      let e = self.tape[i as usize];
      if e.seg == Seg::Arena && e.len as usize == width {
        let written = write(&mut self.arena[e.value_range()]);
        debug_assert_eq!(written, width);
        self.clean = false;
        return;
      }
    }
    let result = self
      .header_mut()
      .put(52, |w| {
        w.put_with(width, write);
        Ok(())
      })
      .map(|_| ());
    debug_assert!(result.is_ok());
  }

  // -------------------------------------------------------------------------
  // Tape surgery
  // -------------------------------------------------------------------------

  fn owner_range(&self, owner: Owner) -> (u32, u32, u8) {
    match owner {
      Owner::Region(r) => {
        let (s, e) = self.region_range(r);
        (s, e, 0)
      }
      Owner::Instance(q) => {
        let e = &self.tape[q as usize];
        (q + 1, q + e.span(), e.depth)
      }
    }
  }

  fn owner_region(&self, owner: Owner) -> Region {
    match owner {
      Owner::Region(r) => r,
      Owner::Instance(q) => self.region_of(q),
    }
  }

  /// Insert `entries` at `at`, inside the container `[start, end)` — a block or
  /// a group — in `region`. Fills the header gap when inserting at its start;
  /// otherwise shifts what follows, growing every enclosing group and instance
  /// and moving the region boundaries after `region`.
  fn insert(
    &mut self,
    region: Region,
    container: Option<(u32, u32)>,
    at: u32,
    entries: &[Entry],
  ) {
    self.clean = false;
    let n = entries.len() as u32;
    let a = at as usize;

    // The header gap takes region-level inserts only. Something inserted into
    // a group or instance that happens to end at the gap must grow that group
    // and instance, which only a splice does.
    if let Some(gap) = self
      .tape
      .get(a)
      .filter(|e| e.kind == Kind::Gap && container.is_none())
    {
      let room = gap.span();
      if room >= n {
        self.tape[a..a + n as usize].copy_from_slice(entries);
        if room > n {
          self.tape[a + n as usize] = Entry::gap(room - n);
        }
        return;
      }
    }

    if let Some((start, end)) = container {
      self.grow_ancestors(region, start, end, n as i64);
    }
    self.tape.splice(a..a, entries.iter().copied());
    self.shift_boundaries(region, n as i64);
  }

  /// Remove the entry at `at` and everything it spans, from the container
  /// `[start, end)` in `region`.
  fn remove_at(
    &mut self,
    region: Region,
    container: Option<(u32, u32)>,
    at: u32,
  ) {
    self.clean = false;
    let n = self.tape[at as usize].span();
    if let Some((start, end)) = container {
      self.grow_ancestors(region, start, end, -(n as i64));
    }
    self.tape.drain(at as usize..(at + n) as usize);
    self.shift_boundaries(region, -(n as i64));
  }

  /// Every group and instance enclosing the container `[start, end)` grows by
  /// `n` entries.
  fn grow_ancestors(&mut self, region: Region, start: u32, end: u32, n: i64) {
    let (region_start, _) = self.region_range(region);
    for j in region_start..start {
      let e = &mut self.tape[j as usize];
      if matches!(e.kind, Kind::Group | Kind::Instance) && j + e.span() >= end {
        e.len = (e.len as i64 + n) as u32;
      }
    }
  }

  fn shift_boundaries(&mut self, region: Region, n: i64) {
    let shift = |b: &mut u32| *b = (*b as i64 + n) as u32;
    match region {
      Region::Header => {
        shift(&mut self.body_start);
        shift(&mut self.trailer_start);
      }
      Region::Body => shift(&mut self.trailer_start),
      Region::Trailer => {}
    }
  }

  /// The group definition of the Group entry at `g`, if the dictionary knows
  /// it in this position.
  pub(crate) fn group_def_at(&self, g: u32) -> Option<GroupIdx> {
    let tag = self.tape[g as usize].tag;
    match (Cursor { msg: self, idx: g }).up() {
      // The group is inside an instance, of a group whose definition says
      // what this tag is there.
      Some(instance) => {
        let outer = instance.up()?;
        let outer = self.group_def_at(outer.idx)?;
        self.dict.nested_group(outer, tag)
      }
      None => self.dict.top_level_group(
        self.msg_def,
        self.region_of(g) == Region::Header,
        tag,
      ),
    }
  }

  /// The group definition governing a block, if it is a group instance.
  fn owner_group_def(&self, owner: Owner) -> Option<GroupIdx> {
    match owner {
      Owner::Region(_) => None,
      Owner::Instance(q) => {
        let g = (Cursor { msg: self, idx: q }).up()?;
        self.group_def_at(g.idx)
      }
    }
  }

  /// Whether `tag` is a NumInGroup in this block.
  fn is_group_tag(&self, owner: Owner, tag: u32) -> Option<GroupIdx> {
    match owner {
      Owner::Region(r) => {
        self
          .dict
          .top_level_group(self.msg_def, r == Region::Header, tag)
      }
      Owner::Instance(_) => self
        .owner_group_def(owner)
        .and_then(|g| self.dict.nested_group(g, tag)),
    }
  }

  /// Where a new entry for `tag` goes in a block: in a group instance, at its
  /// place in the group's definition order (so instances we build conform to
  /// TagValue §4.3.6.3); otherwise at the end.
  fn insert_pos(&self, owner: Owner, tag: u32) -> u32 {
    let (start, end, _) = self.owner_range(owner);
    if let Some(def) = self.owner_group_def(owner) {
      let members = &self.dict.group(def).members;
      if let Some(&order) = members.get(&tag) {
        let mut i = start;
        while i < end {
          let e = &self.tape[i as usize];
          if e.is_tagged() && members.get(&e.tag).is_some_and(|&o| o > order) {
            return i;
          }
          i += e.span();
        }
      }
      return end;
    }
    match owner {
      Owner::Region(Region::Header) => self.header_gap().unwrap_or(end),
      _ => end,
    }
  }

  /// Write `tag=value<SOH>` to the arena with `write` producing the value;
  /// checks the value is non-empty and (unless `data`) SOH-free. On error the
  /// arena is left as it was.
  fn write_value(
    &mut self,
    tag: u32,
    data: bool,
    write: impl FnOnce(&mut ValueWriter<'_>) -> Result<(), ValueError>,
  ) -> Result<Entry, FieldError> {
    let off = self.start_field(tag);
    let value_start = self.arena.len();
    let result = write(&mut ValueWriter::new(&mut self.arena)).and_then(|()| {
      let value = &self.arena[value_start..];
      if value.is_empty() {
        Err(ValueError::Empty)
      } else if !data && value.contains(&SOH) {
        Err(ValueError::Delimiter)
      } else {
        Ok(())
      }
    });
    match result {
      Ok(()) => {
        let mut e = self.finish_field(tag, off);
        if data {
          e.kind = Kind::Data;
        }
        Ok(e)
      }
      Err(err) => {
        self.arena.truncate(off);
        Err(FieldError::value(tag, err))
      }
    }
  }
}

impl<'a> BlockMut<'a> {
  /// Read the block as it stands.
  pub fn as_block(&self) -> Block<'_> {
    let (start, end, depth) = self.msg.owner_range(self.owner);
    Block {
      msg: self.msg,
      start,
      end,
      depth,
    }
  }

  fn region(&self) -> Region {
    self.msg.owner_region(self.owner)
  }

  fn container(&self) -> Option<(u32, u32)> {
    match self.owner {
      Owner::Region(_) => None,
      Owner::Instance(_) => {
        let (s, e, _) = self.msg.owner_range(self.owner);
        Some((s, e))
      }
    }
  }

  fn find(&self, tag: u32) -> Option<u32> {
    let (start, end, depth) = self.msg.owner_range(self.owner);
    self.msg.find_in(start, end, depth, tag, None)
  }

  /// Set a field, replacing any value it had. Invalid values are a bug: they
  /// fail a `debug_assert!`, and in release builds the field is removed
  /// (empty means absent). Use [`try_set`](Self::try_set) to handle them.
  pub fn set<M: FieldType, V: ToFix<M>>(
    &mut self,
    field: Field<M>,
    value: V,
  ) -> &mut Self {
    let result = self.try_set(field, value).map(|_| ());
    self.settle(field.tag(), result)
  }

  /// Set a field, or say why its value is unusable.
  pub fn try_set<M: FieldType, V: ToFix<M>>(
    &mut self,
    field: Field<M>,
    value: V,
  ) -> Result<&mut Self, FieldError> {
    self.put(field.tag(), |w| value.to_fix(w))
  }

  /// Set a field to these exact bytes, whatever its datatype — for values the
  /// dictionary does not describe, such as bilaterally agreed codes.
  pub fn set_raw(&mut self, tag: impl Tag, value: &[u8]) -> &mut Self {
    let tag = tag.tag();
    let result = self.try_set_raw(tag, value).map(|_| ());
    self.settle(tag, result)
  }

  pub fn try_set_raw(
    &mut self,
    tag: impl Tag,
    value: &[u8],
  ) -> Result<&mut Self, FieldError> {
    self.put(tag.tag(), |w| {
      w.put(value);
      Ok(())
    })
  }

  fn settle(&mut self, tag: u32, result: Result<(), FieldError>) -> &mut Self {
    if let Err(e) = result {
      debug_assert!(false, "{e}");
      tracing::error!("{e}");
      if matches!(e.kind, FieldErrorKind::Value(_)) {
        self.remove(tag);
      }
    }
    self
  }

  fn put(
    &mut self,
    tag: u32,
    write: impl FnOnce(&mut ValueWriter<'_>) -> Result<(), ValueError>,
  ) -> Result<&mut Self, FieldError> {
    if matches!(tag, 8 | 9 | 10 | 35)
      || self.msg.is_group_tag(self.owner, tag).is_some()
    {
      return Err(FieldError::derived(tag));
    }
    match self.msg.dict.kind(tag) {
      FieldKind::DataLength { .. } => Err(FieldError::derived(tag)),
      FieldKind::Data { length } => {
        let data = self.msg.write_value(tag, true, write)?;
        let off = self.msg.start_field(length);
        ValueWriter::new(&mut self.msg.arena).put_uint(data.len as u64);
        let mut len = self.msg.finish_field(length, off);
        len.kind = Kind::DataLen;
        self.place(&[len, data]);
        Ok(self)
      }
      FieldKind::Plain => {
        let e = self.msg.write_value(tag, false, write)?;
        self.place(&[e]);
        Ok(self)
      }
    }
  }

  /// Put freshly written entries (a field, or a Length and data pair) in the
  /// block, replacing the existing ones for the same tag.
  fn place(&mut self, entries: &[Entry]) {
    let (_, _, depth) = self.msg.owner_range(self.owner);
    let mut entries = entries.to_vec();
    for e in &mut entries {
      e.depth = depth;
    }
    let tag = entries.last().map_or(0, |e| e.tag);
    self.msg.clean = false;
    match self.find(tag) {
      Some(i) if entries.len() == 1 => self.msg.tape[i as usize] = entries[0],
      Some(i) => {
        // A data field: its Length is the entry before it.
        let i = i as usize;
        self.msg.tape[i - 1..=i].copy_from_slice(&entries);
      }
      None => {
        let at = self.msg.insert_pos(self.owner, entries[0].tag);
        let container = self.container();
        self.msg.insert(self.region(), container, at, &entries);
      }
    }
  }

  /// Remove a field (a data field and its Length together) or a whole group.
  pub fn remove(&mut self, tag: impl Tag) -> &mut Self {
    let tag = tag.tag();
    let Some(mut i) = self.find(tag) else {
      return self;
    };
    let e = self.msg.tape[i as usize];
    let pair = match e.kind {
      Kind::Data => {
        i -= 1;
        true
      }
      Kind::DataLen => true,
      _ => false,
    };
    let region = self.region();
    let container = self.container();
    self.msg.remove_at(region, container, i);
    if pair {
      let container = self.container();
      self.msg.remove_at(region, container, i);
    }
    self
  }

  /// Copy a field, a data field (with its Length), or a whole group from
  /// another message's block, replacing any here. Bytes are copied; nothing is
  /// decoded. Returns whether `src` had it.
  pub fn copy(&mut self, src: &Block<'_>, tag: impl Tag) -> bool {
    let tag = tag.tag();
    let Some(i) = src.msg.find_in(src.start, src.end, src.depth, tag, None)
    else {
      return false;
    };
    let first = match src.msg.tape[i as usize].kind {
      Kind::Data => i - 1,
      _ => i,
    };
    let last = match src.msg.tape[first as usize].kind {
      Kind::DataLen => first + 2,
      _ => src.msg.next_sibling(first),
    };
    let (_, _, depth) = self.msg.owner_range(self.owner);
    let base_depth = src.msg.tape[first as usize].depth;
    let mut entries = Vec::with_capacity((last - first) as usize);
    for e in &src.msg.tape[first as usize..last as usize] {
      let mut e = *e;
      if e.is_field() {
        let bytes = src.msg.span_bytes(&e);
        e.off = self.msg.arena.len() as u32;
        e.seg = Seg::Arena;
        self.msg.arena.extend_from_slice(bytes);
      }
      e.depth = e.depth - base_depth + depth;
      entries.push(e);
    }
    self.remove(tag);
    let at = self.msg.insert_pos(self.owner, entries[0].tag);
    let container = self.container();
    self.msg.insert(self.region(), container, at, &entries);
    true
  }

  /// Give field `to` the value field `from` has in this block — within one
  /// message, where borrowing the value and setting it at once is not
  /// possible. `OrigSendingTime` from `SendingTime`, say.
  pub fn copy_value(
    &mut self,
    from: impl Tag,
    to: impl Tag,
  ) -> Result<&mut Self, FieldError> {
    let (from, to) = (from.tag(), to.tag());
    let i = self.find(from).ok_or(FieldError::missing(from))?;
    let e = self.msg.tape[i as usize];
    if e.kind != Kind::Field {
      return Err(FieldError::derived(from));
    }
    if matches!(to, 8 | 9 | 10 | 35)
      || self.msg.dict.kind(to) != FieldKind::Plain
      || self.msg.is_group_tag(self.owner, to).is_some()
    {
      return Err(FieldError::derived(to));
    }
    let off = self.msg.start_field(to);
    let msg = &mut *self.msg;
    match e.seg {
      Seg::Wire => msg.arena.extend_from_slice(&msg.wire[e.value_range()]),
      Seg::Arena => msg.arena.extend_from_within(e.value_range()),
    }
    let entry = self.msg.finish_field(to, off);
    self.place(&[entry]);
    Ok(self)
  }

  /// A repeating group in this block, to build or edit.
  pub fn group_mut(&mut self, group: impl Tag) -> GroupMut<'_> {
    let tag = group.tag();
    let idx = self
      .find(tag)
      .filter(|&i| self.msg.tape[i as usize].kind == Kind::Group);
    GroupMut {
      msg: self.msg,
      parent: self.owner,
      tag,
      idx,
    }
  }
}

impl std::fmt::Debug for BlockMut<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    std::fmt::Debug::fmt(&self.as_block(), f)
  }
}

impl<'a> GroupMut<'a> {
  /// Number of instances.
  pub fn len(&self) -> usize {
    self
      .idx
      .map_or(0, |g| self.msg.tape[g as usize].off as usize)
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  fn region(&self) -> Region {
    self.msg.owner_region(self.parent)
  }

  /// The tape index of the `n`th instance, or of the end of the group when
  /// `n == len()`.
  fn instance_at(&self, n: usize) -> Option<u32> {
    let g = self.idx?;
    let end = self.msg.next_sibling(g);
    let mut i = g + 1;
    for _ in 0..n {
      if i >= end {
        return None;
      }
      i = self.msg.next_sibling(i);
    }
    Some(i)
  }

  /// Make sure the Group entry exists; returns its index.
  fn ensure(&mut self) -> u32 {
    if let Some(g) = self.idx {
      return g;
    }
    let (_, _, depth) = self.msg.owner_range(self.parent);
    let at = self.msg.insert_pos(self.parent, self.tag);
    let container = match self.parent {
      Owner::Region(_) => None,
      Owner::Instance(_) => {
        let (s, e, _) = self.msg.owner_range(self.parent);
        Some((s, e))
      }
    };
    let entry = Entry {
      tag: self.tag,
      kind: Kind::Group,
      depth,
      off: 0,
      len: 1,
      ..Default::default()
    };
    self.msg.insert(self.region(), container, at, &[entry]);
    self.idx = Some(at);
    at
  }

  /// Add an instance at the end; returns it, empty, to fill in. An instance
  /// left empty is not written (empty means absent).
  pub fn push(&mut self) -> BlockMut<'_> {
    let n = self.len();
    self.insert(n)
  }

  /// Add an instance before the `n`th (or at the end, for `n == len()`).
  ///
  /// # Panics
  ///
  /// If `n > len()`.
  pub fn insert(&mut self, n: usize) -> BlockMut<'_> {
    assert!(n <= self.len(), "instance {n} of {}", self.len());
    let g = self.ensure();
    let at = self.instance_at(n).expect("checked above");
    let depth = self.msg.tape[g as usize].depth + 1;
    let container = Some((g + 1, self.msg.next_sibling(g)));
    let instance = Entry {
      kind: Kind::Instance,
      depth,
      len: 1,
      ..Default::default()
    };
    self.msg.insert(self.region(), container, at, &[instance]);
    self.msg.tape[g as usize].off += 1;
    BlockMut {
      msg: &mut *self.msg,
      owner: Owner::Instance(at),
    }
  }

  /// The `n`th instance, to edit.
  pub fn get_mut(&mut self, n: usize) -> Option<BlockMut<'_>> {
    let i = self.instance_at(n).filter(|_| n < self.len())?;
    Some(BlockMut {
      msg: &mut *self.msg,
      owner: Owner::Instance(i),
    })
  }

  /// Remove the `n`th instance. Removing the last removes the group.
  pub fn remove(&mut self, n: usize) -> &mut Self {
    let (Some(g), Some(i)) = (self.idx, self.instance_at(n)) else {
      return self;
    };
    if n >= self.len() {
      return self;
    }
    let container = Some((g + 1, self.msg.next_sibling(g)));
    self.msg.remove_at(self.region(), container, i);
    self.msg.tape[g as usize].off -= 1;
    if self.msg.tape[g as usize].off == 0 {
      self.clear();
    }
    self
  }

  /// Keep only the instances for which `keep` returns true.
  pub fn retain(
    &mut self,
    mut keep: impl FnMut(Block<'_>) -> bool,
  ) -> &mut Self {
    let mut n = 0;
    while n < self.len() {
      let i = self.instance_at(n).expect("in range");
      if keep(Block::instance(self.msg, i)) {
        n += 1;
      } else {
        self.remove(n);
      }
    }
    self
  }

  /// Remove the whole group.
  pub fn clear(&mut self) -> &mut Self {
    if let Some(g) = self.idx.take() {
      let container = match self.parent {
        Owner::Region(_) => None,
        Owner::Instance(_) => {
          let (s, e, _) = self.msg.owner_range(self.parent);
          Some((s, e))
        }
      };
      self.msg.remove_at(self.region(), container, g);
    }
    self
  }
}

impl<'a> CursorMut<'a> {
  /// Read the position as it stands.
  pub fn as_cursor(&self) -> Cursor<'_> {
    Cursor {
      msg: self.msg,
      idx: self.idx,
    }
  }

  /// The block this position is in, to edit.
  pub fn block_mut(&mut self) -> BlockMut<'_> {
    let owner = match self.as_cursor().up() {
      Some(c) if c.kind() == super::EntryKind::Instance => {
        Owner::Instance(c.idx)
      }
      _ => Owner::Region(self.msg.region_of(self.idx)),
    };
    BlockMut {
      msg: &mut *self.msg,
      owner,
    }
  }

  /// Set this field's value to these bytes.
  pub fn set_raw(&mut self, value: &[u8]) -> Result<(), FieldError> {
    let tag = self.as_cursor().tag();
    self.block_mut().try_set_raw(tag, value).map(|_| ())
  }

  /// Remove this field, group or instance.
  pub fn remove(mut self) {
    let c = self.as_cursor();
    let tag = c.tag();
    match c.kind() {
      super::EntryKind::Instance => {
        let n = c.instance_index().unwrap_or(0);
        let group = c.up().expect("an instance is in a group").idx;
        let mut owner = CursorMut {
          msg: &mut *self.msg,
          idx: group,
        };
        owner.block_mut().group_mut(tag).remove(n);
      }
      _ => {
        self.block_mut().remove(tag);
      }
    }
  }

  /// On a group: add an instance at the end, and return it to fill in.
  pub fn push_instance(&mut self) -> Option<BlockMut<'_>> {
    let c = self.as_cursor();
    if c.kind() != super::EntryKind::Group {
      return None;
    }
    let tag = c.tag();
    let mut block = self.block_mut();
    let mut group = block.group_mut(tag);
    let n = group.len();
    let at = {
      let b = group.insert(n);
      match b.owner {
        Owner::Instance(i) => i,
        Owner::Region(_) => unreachable!(),
      }
    };
    Some(BlockMut {
      msg: &mut *self.msg,
      owner: Owner::Instance(at),
    })
  }
}
