//! Building and editing: mutable views over a message's tape.
//!
//! An edit never moves message bytes. A new or changed value is appended to the
//! arena and an entry points at it; inserting a field splices one 16-byte entry
//! into the tape (or fills a slot in the header gap), and the spans of the
//! groups and instances around it grow by one.
//!
//! A mutable view records its `Place` once, when it is made: which block it is
//! (a region, or the index of an instance entry), the group definition that
//! governs it, and the indices of every group and instance enclosing it. An
//! insert or remove then grows or shrinks exactly those ancestors, with no
//! search. Entries before an insertion point never move, so none of those
//! indices go stale however much is inserted into the view.

use super::SOH;
use super::dict::{FieldKind, GroupIdx};
use super::error::{FieldError, FieldErrorKind, ValueError};
use super::path::FieldPath;
use super::tape::{Entry, Kind, Message, Region, Seg};
use super::types::{Field, FieldType, Tag, ToFix, ValueWriter};
use super::view::{Block, Cursor};

/// BeginString, BodyLength, CheckSum and MsgType: derived or fixed when the
/// message is created, never edited as fields.
fn is_framing(tag: u32) -> bool {
  matches!(tag, 8 | 9 | 10 | 35)
}

/// Which block a mutable view edits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
  Region(Region),
  /// The instance entry at this index.
  Instance(u32),
}

/// Where a mutable view's block is.
#[derive(Clone, Debug)]
struct Place {
  owner: Owner,
  region: Region,
  /// For an instance, the definition of its group, if the dictionary has one.
  def: Option<GroupIdx>,
  /// Every Group and Instance entry enclosing the block, outermost first; an
  /// instance's own entry is last.
  ancestors: Vec<u32>,
}

impl Place {
  fn region(region: Region) -> Self {
    Self {
      owner: Owner::Region(region),
      region,
      def: None,
      ancestors: Vec::new(),
    }
  }
}

/// A block (a region, or a group instance) being built or edited.
pub struct BlockMut<'a> {
  msg: &'a mut Message,
  place: Place,
}

/// A group instance being built or edited: a [`BlockMut`] (which it
/// dereferences to) that tidies up after itself.
///
/// If it is dropped with nothing in the instance, the instance goes — and its
/// group, if that was the last one — so an instance only exists once something
/// is in it, and a group's length always matches what is written.
pub struct InstanceMut<'a> {
  block: BlockMut<'a>,
}

impl<'a> std::ops::Deref for InstanceMut<'a> {
  type Target = BlockMut<'a>;
  fn deref(&self) -> &BlockMut<'a> {
    &self.block
  }
}

impl<'a> std::ops::DerefMut for InstanceMut<'a> {
  fn deref_mut(&mut self) -> &mut BlockMut<'a> {
    &mut self.block
  }
}

impl Drop for InstanceMut<'_> {
  fn drop(&mut self) {
    let BlockMut { msg, place } = &mut self.block;
    msg.prune(place);
  }
}

impl std::fmt::Debug for InstanceMut<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    std::fmt::Debug::fmt(&self.block, f)
  }
}

/// A repeating group being built or edited.
pub struct GroupMut<'a> {
  msg: &'a mut Message,
  /// The block the group is in.
  parent: Place,
  tag: u32,
  /// The Group entry, once it exists.
  idx: Option<u32>,
  /// The group's definition, if the dictionary has one.
  def: Option<GroupIdx>,
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
      place: Place::region(Region::Header),
    }
  }

  pub fn body_mut(&mut self) -> BlockMut<'_> {
    BlockMut {
      msg: self,
      place: Place::region(Region::Body),
    }
  }

  /// If `place` is an instance with nothing in it, remove it, and its group if
  /// that was the group's last instance.
  fn prune(&mut self, place: &Place) {
    let Owner::Instance(q) = place.owner else {
      return;
    };
    if self.instance_writes(q) {
      return;
    }
    // An instance's ancestors end `.., group, instance`.
    let [outer @ .., g, _] = place.ancestors.as_slice() else {
      return;
    };
    let mut to_group = outer.to_vec();
    to_group.push(*g);
    self.remove_at(place.region, &to_group, q);
    self.tape[*g as usize].off -= 1;
    if self.tape[*g as usize].off == 0 {
      self.remove_at(place.region, outer, *g);
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
      if e.kind != Kind::Field || is_framing(e.tag) {
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

  /// The place of the block that holds the entry at `idx`, found by walking
  /// the tape. Views made from a parent view inherit their place instead; this
  /// is for starting from a position, as [`CursorMut`] does.
  fn place_of(&self, idx: u32) -> Place {
    let region = self.region_of(idx);
    let mut ancestors = Vec::new();
    let mut c = Cursor { msg: self, idx };
    while let Some(up) = c.up() {
      ancestors.push(up.idx);
      c = up;
    }
    ancestors.reverse();
    match *ancestors.as_slice() {
      [.., g, q] if self.tape[q as usize].kind == Kind::Instance => Place {
        owner: Owner::Instance(q),
        region,
        def: self.group_def_at(g),
        ancestors,
      },
      _ => Place::region(region),
    }
  }

  /// Insert `entries` at `at`, inside the groups and instances `ancestors`, in
  /// `region`. A region-level insert at the start of the header gap fills gap
  /// slots; anything else shifts what follows, grows each ancestor, and moves
  /// the region boundaries after `region`.
  fn insert(
    &mut self,
    region: Region,
    ancestors: &[u32],
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
      .filter(|e| e.kind == Kind::Gap && ancestors.is_empty())
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

    self.grow(ancestors, n as i64);
    self.tape.splice(a..a, entries.iter().copied());
    self.shift_boundaries(region, n as i64);
  }

  /// Remove the entry at `at` and everything it spans, from inside the groups
  /// and instances `ancestors`, in `region`.
  fn remove_at(&mut self, region: Region, ancestors: &[u32], at: u32) {
    self.clean = false;
    let n = self.tape[at as usize].span();
    self.grow(ancestors, -(n as i64));
    self.tape.drain(at as usize..(at + n) as usize);
    self.shift_boundaries(region, -(n as i64));
  }

  /// Each of `ancestors` grows by `n` entries.
  fn grow(&mut self, ancestors: &[u32], n: i64) {
    for &a in ancestors {
      let e = &mut self.tape[a as usize];
      e.len = (e.len as i64 + n) as u32;
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
  /// it in this position. Walks up the tape; views avoid it by carrying the
  /// definition down from where they were made.
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

  /// The group `tag` opens in a block, if it is a NumInGroup there.
  fn is_group_tag(&self, place: &Place, tag: u32) -> Option<GroupIdx> {
    match place.owner {
      Owner::Region(r) => {
        self
          .dict
          .top_level_group(self.msg_def, r == Region::Header, tag)
      }
      Owner::Instance(_) => {
        place.def.and_then(|g| self.dict.nested_group(g, tag))
      }
    }
  }

  /// Where a new entry for `tag` goes in a block: in a group instance, at its
  /// place in the group's definition order (so instances we build conform to
  /// TagValue §4.3.6.3); otherwise at the end.
  fn insert_pos(&self, place: &Place, tag: u32) -> u32 {
    let (start, end, _) = self.owner_range(place.owner);
    if let Some(def) = place.def {
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
    match place.owner {
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
    let (start, end, depth) = self.msg.owner_range(self.place.owner);
    Block {
      msg: self.msg,
      start,
      end,
      depth,
    }
  }

  fn find(&self, tag: u32) -> Option<u32> {
    let (start, end, depth) = self.msg.owner_range(self.place.owner);
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
    if is_framing(tag) || self.msg.is_group_tag(&self.place, tag).is_some() {
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
        self.place_entries(&mut [len, data]);
        Ok(self)
      }
      FieldKind::Plain => {
        let mut e = [self.msg.write_value(tag, false, write)?];
        self.place_entries(&mut e);
        Ok(self)
      }
    }
  }

  /// Put freshly written entries (a field, or a Length and data pair) in the
  /// block, replacing the existing ones for the same tag.
  fn place_entries(&mut self, entries: &mut [Entry]) {
    let (_, _, depth) = self.msg.owner_range(self.place.owner);
    for e in entries.iter_mut() {
      e.depth = depth;
    }
    let tag = entries.last().map_or(0, |e| e.tag);
    self.msg.clean = false;
    match self.find(tag) {
      Some(i) if entries.len() == 1 => self.msg.tape[i as usize] = entries[0],
      Some(i) => {
        // A data field: its Length is the entry before it.
        let i = i as usize;
        debug_assert_eq!(self.msg.tape[i - 1].kind, Kind::DataLen);
        self.msg.tape[i - 1..=i].copy_from_slice(entries);
      }
      None => {
        let at = self.msg.insert_pos(&self.place, entries[0].tag);
        self
          .msg
          .insert(self.place.region, &self.place.ancestors, at, entries);
      }
    }
  }

  /// Remove a field (a data field and its Length together) or a whole group.
  ///
  /// BeginString, BodyLength, CheckSum and MsgType cannot be removed: like
  /// setting them, trying is a bug (a `debug_assert!`), and in release builds
  /// nothing happens.
  pub fn remove(&mut self, tag: impl Tag) -> &mut Self {
    let tag = tag.tag();
    if is_framing(tag) {
      debug_assert!(false, "tag {tag} is derived and cannot be removed");
      return self;
    }
    let Some(mut i) = self.find(tag) else {
      return self;
    };
    let pair = match self.msg.tape[i as usize].kind {
      Kind::Data => {
        i -= 1;
        true
      }
      Kind::DataLen => true,
      _ => false,
    };
    let Place {
      region, ancestors, ..
    } = &self.place;
    self.msg.remove_at(*region, ancestors, i);
    if pair {
      self.msg.remove_at(*region, ancestors, i);
    }
    self
  }

  /// Copy a field, a data field (with its Length), or a whole group from
  /// another message's block, replacing any here. Bytes are copied; nothing is
  /// decoded. Returns whether `src` had it.
  ///
  /// BeginString, BodyLength, CheckSum and MsgType are each message's own, and
  /// are not copied (`false`, after a `debug_assert!`).
  pub fn copy(&mut self, src: &Block<'_>, tag: impl Tag) -> bool {
    let tag = tag.tag();
    if is_framing(tag) {
      debug_assert!(false, "tag {tag} is derived and cannot be copied");
      return false;
    }
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
    let (_, _, depth) = self.msg.owner_range(self.place.owner);
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
    let at = self.msg.insert_pos(&self.place, entries[0].tag);
    self
      .msg
      .insert(self.place.region, &self.place.ancestors, at, &entries);
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
    if is_framing(to)
      || self.msg.dict.kind(to) != FieldKind::Plain
      || self.msg.is_group_tag(&self.place, to).is_some()
    {
      return Err(FieldError::derived(to));
    }
    let off = self.msg.start_field(to);
    let msg = &mut *self.msg;
    match e.seg {
      Seg::Wire => msg.arena.extend_from_slice(&msg.wire[e.value_range()]),
      Seg::Arena => msg.arena.extend_from_within(e.value_range()),
    }
    let mut entry = [self.msg.finish_field(to, off)];
    self.place_entries(&mut entry);
    Ok(self)
  }

  /// A repeating group in this block, to build or edit.
  pub fn group_mut(&mut self, group: impl Tag) -> GroupMut<'_> {
    let tag = group.tag();
    let idx = self
      .find(tag)
      .filter(|&i| self.msg.tape[i as usize].kind == Kind::Group);
    let def = self.msg.is_group_tag(&self.place, tag);
    GroupMut {
      msg: self.msg,
      parent: self.place.clone(),
      tag,
      idx,
      def,
    }
  }
}

impl std::fmt::Debug for BlockMut<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    std::fmt::Debug::fmt(&self.as_block(), f)
  }
}

impl<'a> GroupMut<'a> {
  /// The Group entry, if it still exists: an instance view dropped empty can
  /// remove the group along with its last instance.
  fn group(&self) -> Option<u32> {
    let (_, _, depth) = self.msg.owner_range(self.parent.owner);
    self.idx.filter(|&g| {
      self.msg.tape.get(g as usize).is_some_and(|e| {
        e.kind == Kind::Group && e.tag == self.tag && e.depth == depth
      })
    })
  }

  /// Number of instances.
  pub fn len(&self) -> usize {
    self
      .group()
      .map_or(0, |g| self.msg.tape[g as usize].off as usize)
  }

  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// The ancestors of the group's own contents: the parent block's, and the
  /// group itself.
  fn inner_ancestors(&self, g: u32) -> Vec<u32> {
    let mut ancestors = Vec::with_capacity(self.parent.ancestors.len() + 2);
    ancestors.extend_from_slice(&self.parent.ancestors);
    ancestors.push(g);
    ancestors
  }

  /// The tape index of the `n`th instance, or of the end of the group when
  /// `n == len()`.
  fn instance_at(&self, n: usize) -> Option<u32> {
    let g = self.group()?;
    let end = self.msg.next_sibling(g);
    if n == self.len() {
      return Some(end);
    }
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
    if let Some(g) = self.group() {
      return g;
    }
    let (_, _, depth) = self.msg.owner_range(self.parent.owner);
    let at = self.msg.insert_pos(&self.parent, self.tag);
    let entry = Entry {
      tag: self.tag,
      kind: Kind::Group,
      depth,
      off: 0,
      len: 1,
      ..Default::default()
    };
    self
      .msg
      .insert(self.parent.region, &self.parent.ancestors, at, &[entry]);
    self.idx = Some(at);
    at
  }

  /// A view of the instance entry at `q`.
  fn instance_view(&mut self, g: u32, q: u32) -> InstanceMut<'_> {
    let mut ancestors = self.inner_ancestors(g);
    ancestors.push(q);
    InstanceMut {
      block: BlockMut {
        msg: &mut *self.msg,
        place: Place {
          owner: Owner::Instance(q),
          region: self.parent.region,
          def: self.def,
          ancestors,
        },
      },
    }
  }

  /// Add an instance at the end; returns it, empty, to fill in. If the view is
  /// dropped with nothing in the instance, the instance goes too.
  pub fn push(&mut self) -> InstanceMut<'_> {
    let n = self.len();
    self.insert(n)
  }

  /// Add an instance before the `n`th (or at the end, for `n == len()`).
  ///
  /// # Panics
  ///
  /// If `n > len()`.
  pub fn insert(&mut self, n: usize) -> InstanceMut<'_> {
    assert!(n <= self.len(), "instance {n} of {}", self.len());
    let g = self.ensure();
    let at = self.instance_at(n).expect("checked above");
    let instance = Entry {
      kind: Kind::Instance,
      depth: self.msg.tape[g as usize].depth + 1,
      len: 1,
      ..Default::default()
    };
    let ancestors = self.inner_ancestors(g);
    self
      .msg
      .insert(self.parent.region, &ancestors, at, &[instance]);
    self.msg.tape[g as usize].off += 1;
    self.instance_view(g, at)
  }

  /// The `n`th instance, to edit.
  pub fn get_mut(&mut self, n: usize) -> Option<InstanceMut<'_>> {
    let g = self.group()?;
    let q = self.instance_at(n).filter(|_| n < self.len())?;
    Some(self.instance_view(g, q))
  }

  /// Remove the `n`th instance. Removing the last removes the group.
  pub fn remove(&mut self, n: usize) -> &mut Self {
    let Some(g) = self.group() else {
      return self;
    };
    if n >= self.len() {
      return self;
    }
    let q = self.instance_at(n).expect("in range");
    let ancestors = self.inner_ancestors(g);
    self.msg.remove_at(self.parent.region, &ancestors, q);
    self.msg.tape[g as usize].off -= 1;
    if self.msg.tape[g as usize].off == 0 {
      self.clear();
    }
    self
  }

  /// Keep only the instances for which `keep` returns true. One pass over the
  /// group, however many go.
  pub fn retain(
    &mut self,
    mut keep: impl FnMut(Block<'_>) -> bool,
  ) -> &mut Self {
    let Some(g) = self.group() else {
      return self;
    };
    let end = self.msg.next_sibling(g);
    let mut kept: Vec<Entry> = Vec::with_capacity((end - g - 1) as usize);
    let mut count = 0;
    let mut q = g + 1;
    while q < end {
      let next = self.msg.next_sibling(q);
      if keep(Block::instance(self.msg, q)) {
        kept.extend_from_slice(&self.msg.tape[q as usize..next as usize]);
        count += 1;
      }
      q = next;
    }
    let removed = (end - g - 1) as i64 - kept.len() as i64;
    if removed == 0 {
      return self;
    }
    if count == 0 {
      return self.clear();
    }
    self.msg.clean = false;
    self.msg.tape.splice((g + 1) as usize..end as usize, kept);
    let ancestors = self.inner_ancestors(g);
    self.msg.grow(&ancestors, -removed);
    self.msg.shift_boundaries(self.parent.region, -removed);
    self.msg.tape[g as usize].off = count;
    self
  }

  /// Remove the whole group.
  pub fn clear(&mut self) -> &mut Self {
    if let Some(g) = self.group() {
      self.idx = None;
      self
        .msg
        .remove_at(self.parent.region, &self.parent.ancestors, g);
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
    let place = self.msg.place_of(self.idx);
    BlockMut {
      msg: &mut *self.msg,
      place,
    }
  }

  /// Set this field's value to these bytes.
  pub fn set_raw(&mut self, value: &[u8]) -> Result<(), FieldError> {
    let tag = self.as_cursor().tag();
    self.block_mut().try_set_raw(tag, value).map(|_| ())
  }

  /// Remove this field, group or instance.
  pub fn remove(self) {
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
        let place = self.msg.place_of(self.idx);
        let mut block = BlockMut {
          msg: &mut *self.msg,
          place,
        };
        block.remove(tag);
        // Removing an instance's last field removes the instance, as
        // dropping an empty InstanceMut does.
        let BlockMut { msg, place } = &mut block;
        msg.prune(place);
      }
    }
  }

  /// On a group: add an instance at the end, and return it to fill in.
  pub fn push_instance(&mut self) -> Option<InstanceMut<'_>> {
    let c = self.as_cursor();
    if c.kind() != super::EntryKind::Group {
      return None;
    }
    let tag = c.tag();
    let mut block = self.block_mut();
    let mut group = block.group_mut(tag);
    let mut view = group.push();
    // The view is replaced by the one returned: take its place so it lets go
    // without pruning the new, still-empty instance.
    let place =
      std::mem::replace(&mut view.block.place, Place::region(Region::Body));
    drop(view);
    Some(InstanceMut {
      block: BlockMut {
        msg: &mut *self.msg,
        place,
      },
    })
  }
}
