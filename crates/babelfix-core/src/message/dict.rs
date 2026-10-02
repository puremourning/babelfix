//! The flattened view of a FIX version that parsing and building need.
//!
//! [`FixVersion`] is the full Orchestra model: messages built from components
//! built from groups and fields, with names and documentation. That is the
//! right shape for tooling, which shows components to people. A parser asks
//! narrower questions, once per field, and wants them answered by a table
//! lookup:
//!
//! - is this tag a header field? a trailer field?
//! - is it the Length of a data field, and which one?
//! - in this message, or this group, is it a NumInGroup — and of which group?
//! - is it a member of the group instance currently open, and where does it
//!   come in the group's definition order?
//!
//! A [`Dictionary`] answers those. It is compiled once from a [`FixVersion`]
//! and shared by every message of that version. The version itself is kept
//! alongside — [`Dictionary::version`] — for anything that needs the full model.
//!
//! Structure comes from group definitions and Orchestra's `lengthId`, never from
//! datatype names: FIX 4.2 predates the `NumInGroup` and `Length` datatypes, and
//! declares both as plain `int`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::repository::{
  Component, FieldBlock, FixRepository, FixVersion, Group, MessageElement,
};

use super::hash::TagMap;

/// What a tag is, structurally, wherever it appears.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FieldKind {
  Plain,
  /// The Length field of a data field: `95` for `96`.
  DataLength {
    data: u32,
  },
  /// A data field, whose value is exactly `length`'s value in bytes and may
  /// contain anything, SOH included.
  Data {
    length: u32,
  },
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FieldInfo {
  pub kind: FieldKind,
  pub header: bool,
  pub trailer: bool,
}

/// Index of a group in [`Dictionary::groups`].
pub(crate) type GroupIdx = u32;

/// Index of a message in [`Dictionary::messages`].
pub(crate) type MsgIdx = u32;

#[derive(Debug)]
pub(crate) struct GroupDef {
  pub num_in_group: u32,
  /// The first field of every instance: the group's first field, or the first
  /// field of its first component, or the NumInGroup of its first nested group
  /// (TagValue §4.3.6.4).
  pub delimiter: u32,
  /// Member tag → position in the group's definition order. Covers fields
  /// reached through components, and nested groups' NumInGroup tags — but not
  /// nested groups' own members, which belong to the nested group.
  pub members: TagMap<u16>,
  /// NumInGroup tag → nested group.
  pub nested: TagMap<GroupIdx>,
}

#[derive(Debug)]
pub(crate) struct MessageDef {
  pub name: String,
  /// NumInGroup tag → group, for groups directly in the body (possibly via
  /// components).
  pub groups: TagMap<GroupIdx>,
}

/// The parsing and building view of one FIX version. See the module docs.
#[derive(Debug)]
pub struct Dictionary {
  version: Arc<FixVersion>,
  fields: TagMap<FieldInfo>,
  pub(crate) groups: Vec<GroupDef>,
  pub(crate) messages: Vec<MessageDef>,
  messages_by_type: HashMap<Box<[u8]>, MsgIdx>,
  /// Groups in the StandardHeader (NoHops).
  header_groups: TagMap<GroupIdx>,
  /// NumInGroup tag → group, for tags that mean the same group in every message
  /// that uses them. Used for a group in a message whose type is not defined,
  /// or which does not define that group.
  global_groups: TagMap<GroupIdx>,
}

impl Dictionary {
  /// Compile the tables for `version`.
  pub fn new(version: Arc<FixVersion>) -> Arc<Dictionary> {
    Arc::new(Builder::new(&version).build(version.clone()))
  }

  /// The full Orchestra model this dictionary was compiled from.
  pub fn version(&self) -> &Arc<FixVersion> {
    &self.version
  }

  /// `BeginString(8)` for messages of this version: `FIX.4.4`, `FIXT.1.1`, ...
  pub fn begin_string(&self) -> &str {
    &self.version.begin_string
  }

  /// The field's name, if the dictionary defines it.
  pub fn field_name(&self, tag: u32) -> Option<&str> {
    self.version.fields.get(&tag).map(|f| f.name.as_str())
  }

  /// The name of `value` in the field's codeset, if it has one: `Buy` for
  /// `54=1`.
  pub fn codeset_name(&self, tag: u32, value: &[u8]) -> Option<&str> {
    let field = self.version.fields.get(&tag)?;
    let codes = self.version.codesets.get(&field.field_type)?;
    codes
      .iter()
      .find(|c| c.value.as_bytes() == value)
      .map(|c| c.name.as_str())
  }

  /// The message's name, if its type is defined: `NewOrderSingle` for `D`.
  pub fn message_name(&self, msg_type: &[u8]) -> Option<&str> {
    self
      .message(msg_type)
      .map(|m| self.messages[m as usize].name.as_str())
  }

  pub(crate) fn kind(&self, tag: u32) -> FieldKind {
    self.fields.get(&tag).map_or(FieldKind::Plain, |f| f.kind)
  }

  pub(crate) fn is_header(&self, tag: u32) -> bool {
    self.fields.get(&tag).is_some_and(|f| f.header)
  }

  pub(crate) fn is_trailer(&self, tag: u32) -> bool {
    self.fields.get(&tag).is_some_and(|f| f.trailer)
  }

  pub(crate) fn message(&self, msg_type: &[u8]) -> Option<MsgIdx> {
    self.messages_by_type.get(msg_type).copied()
  }

  pub(crate) fn group(&self, idx: GroupIdx) -> &GroupDef {
    &self.groups[idx as usize]
  }

  /// The group `tag` opens at the top level of a message, if it is a NumInGroup
  /// there.
  pub(crate) fn top_level_group(
    &self,
    msg: Option<MsgIdx>,
    header: bool,
    tag: u32,
  ) -> Option<GroupIdx> {
    if header {
      return self.header_groups.get(&tag).copied();
    }
    msg
      .and_then(|m| self.messages[m as usize].groups.get(&tag).copied())
      .or_else(|| self.global_groups.get(&tag).copied())
  }

  /// The group `tag` opens inside an instance of `parent`, if it is a nested
  /// NumInGroup there.
  pub(crate) fn nested_group(
    &self,
    parent: GroupIdx,
    tag: u32,
  ) -> Option<GroupIdx> {
    self.group(parent).nested.get(&tag).copied()
  }
}

/// A [`Dictionary`] for each version in a repository, compiled once and shared
/// by every connection: an acceptor does not know which version a peer speaks
/// until its Logon arrives.
///
/// Dictionaries are named by Orchestra version, which is not always what goes
/// on the wire. [`standard`](Self::standard) has `FIX.4.2`, `FIX.4.4` and
/// `FIX.Latest`; FIX.Latest's BeginString is `FIXT.1.1`, so
/// `get("FIX.Latest")` and `for_begin_string(b"FIXT.1.1")` find the same
/// dictionary, and `get("FIXT.1.1")` finds nothing.
///
/// To use Orchestra files of your own, load them with
/// [`repository::load_orchestration`](crate::repository::load_orchestration)
/// and compile with [`new`](Self::new).
#[derive(Debug, Default)]
pub struct Dictionaries {
  by_name: std::collections::BTreeMap<String, Arc<Dictionary>>,
}

impl Dictionaries {
  /// Compile every version in `repo`.
  pub fn new(repo: &FixRepository) -> Arc<Dictionaries> {
    Arc::new(Dictionaries {
      by_name: repo
        .versions
        .iter()
        .map(|(name, v)| (name.clone(), Dictionary::new(v.clone())))
        .collect(),
    })
  }

  /// The dictionaries for the FIX versions babelfix embeds.
  pub fn standard() -> crate::Result<Arc<Dictionaries>> {
    Ok(Self::new(&crate::repository::orchestrate()?))
  }

  /// By version name: `FIX.4.4`, `FIX.Latest`.
  pub fn get(&self, version: &str) -> Option<&Arc<Dictionary>> {
    self.by_name.get(version)
  }

  /// The dictionary for messages with this `BeginString(8)`.
  pub fn for_begin_string(
    &self,
    begin_string: &[u8],
  ) -> Option<&Arc<Dictionary>> {
    self
      .by_name
      .values()
      .find(|d| d.begin_string().as_bytes() == begin_string)
  }

  /// Every dictionary, in version name order.
  pub fn iter(&self) -> impl Iterator<Item = &Arc<Dictionary>> {
    self.by_name.values()
  }
}

/// Compiles a [`FixVersion`] into a [`Dictionary`]. Groups are interned by their
/// Orchestra id, so a group reached from many messages is compiled once.
struct Builder<'v> {
  fix: &'v FixVersion,
  groups: Vec<GroupDef>,
  group_ids: HashMap<u32, GroupIdx>,
}

impl<'v> Builder<'v> {
  fn new(fix: &'v FixVersion) -> Self {
    Self {
      fix,
      groups: Vec::new(),
      group_ids: HashMap::new(),
    }
  }

  fn build(mut self, version: Arc<FixVersion>) -> Dictionary {
    let fix = self.fix;

    let mut fields = TagMap::default();
    for (tag, field) in &fix.fields {
      fields.insert(
        *tag,
        FieldInfo {
          kind: FieldKind::Plain,
          header: false,
          trailer: false,
        },
      );
      if let Some(length) = field.length_id {
        fields.insert(
          *tag,
          FieldInfo {
            kind: FieldKind::Data { length },
            header: false,
            trailer: false,
          },
        );
      }
    }
    // Second pass, so the Length side is set however the HashMap iterates.
    for (tag, field) in &fix.fields {
      if let Some(length) = field.length_id
        && let Some(info) = fields.get_mut(&length)
      {
        info.kind = FieldKind::DataLength { data: *tag };
      }
    }

    let mut header_groups = TagMap::default();
    if let Some(header) = fix.get_component_by_name("StandardHeader") {
      for tag in &header.members {
        if let Some(info) = fields.get_mut(tag) {
          info.header = true;
        }
      }
      self.collect_groups(&header.elements, &mut header_groups);
    }
    if let Some(trailer) = fix.get_component_by_name("StandardTrailer") {
      for tag in &trailer.members {
        if let Some(info) = fields.get_mut(tag) {
          info.trailer = true;
        }
      }
    }

    let mut messages = Vec::new();
    let mut messages_by_type = HashMap::new();
    let mut by_num_in_group: TagMap<Option<GroupIdx>> = TagMap::default();
    let mut defs: Vec<_> = fix.messages.values().collect();
    // Deterministic indices, whatever order the HashMap hands them out.
    defs.sort_by(|a, b| a.msg_type.cmp(&b.msg_type));
    for def in defs {
      let mut groups = TagMap::default();
      self.collect_groups(def.get_elements(), &mut groups);
      for (tag, idx) in &groups {
        by_num_in_group
          .entry(*tag)
          .and_modify(|g| {
            if *g != Some(*idx) {
              *g = None // ambiguous: means different groups in different messages
            }
          })
          .or_insert(Some(*idx));
      }
      messages_by_type.insert(
        def.msg_type.as_bytes().to_vec().into_boxed_slice(),
        messages.len() as MsgIdx,
      );
      messages.push(MessageDef {
        name: def.name.clone(),
        groups,
      });
    }
    let global_groups = by_num_in_group
      .into_iter()
      .filter_map(|(tag, g)| Some((tag, g?)))
      .collect();

    Dictionary {
      version,
      fields,
      groups: self.groups,
      messages,
      messages_by_type,
      header_groups,
      global_groups,
    }
  }

  /// Collect the groups directly in `elements` — looking through components,
  /// but not into the groups themselves — keyed by NumInGroup tag.
  fn collect_groups(
    &mut self,
    elements: &[MessageElement],
    out: &mut TagMap<GroupIdx>,
  ) {
    for element in elements {
      match element {
        MessageElement::Field(_) => {}
        MessageElement::Component(c) => {
          if let Some(component) = self.fix.get_component(c.component_id) {
            self.collect_groups(&component.elements, out);
          }
        }
        MessageElement::Group(g) => {
          if let Some(group) = self.fix.get_group(g.group_id) {
            let idx = self.intern(&group);
            out.insert(group.num_in_group_tag, idx);
          }
        }
      }
    }
  }

  fn intern(&mut self, group: &Group) -> GroupIdx {
    if let Some(idx) = self.group_ids.get(&group.id) {
      return *idx;
    }
    // Reserve the slot first: a group may (in principle) contain itself.
    let idx = self.groups.len() as GroupIdx;
    self.group_ids.insert(group.id, idx);
    self.groups.push(GroupDef {
      num_in_group: group.num_in_group_tag,
      delimiter: 0,
      members: TagMap::default(),
      nested: TagMap::default(),
    });

    let mut order = Vec::new();
    self.member_order(&group.elements, &mut order);
    let mut nested = TagMap::default();
    self.collect_groups(&group.elements, &mut nested);

    let def = &mut self.groups[idx as usize];
    def.delimiter = order.first().copied().unwrap_or(0);
    for (i, tag) in order.into_iter().enumerate() {
      def.members.entry(tag).or_insert(i as u16);
    }
    def.nested = nested;
    idx
  }

  /// The group's member tags in definition order, flattening components; a
  /// nested group contributes its NumInGroup tag only.
  fn member_order(&self, elements: &[MessageElement], out: &mut Vec<u32>) {
    for element in elements {
      match element {
        MessageElement::Field(f) => out.push(f.field_id),
        MessageElement::Component(c) => {
          if let Some(component) = self.fix.get_component(c.component_id) {
            let component: &Component = &component;
            self.member_order(&component.elements, out);
          }
        }
        MessageElement::Group(g) => {
          if let Some(group) = self.fix.get_group(g.group_id) {
            out.push(group.num_in_group_tag);
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::test_support::{dict, dict_for};

  #[test]
  fn data_fields_pair_with_their_length() {
    for version in ["FIX.4.2", "FIX.4.4", "FIX.Latest"] {
      let d = dict_for(version);
      assert_eq!(d.kind(95), FieldKind::DataLength { data: 96 }, "{version}");
      assert_eq!(d.kind(96), FieldKind::Data { length: 95 }, "{version}");
      // BodyLength is a Length, but belongs to no data field.
      assert_eq!(d.kind(9), FieldKind::Plain, "{version}");
    }
  }

  #[test]
  fn header_and_trailer_membership() {
    let d = dict();
    for tag in [8, 9, 35, 49, 56, 34, 52, 43, 122, 627] {
      assert!(d.is_header(tag), "{tag}");
    }
    assert!(!d.is_header(55));
    assert!(d.is_trailer(10));
    assert!(d.is_trailer(93));
    assert!(!d.is_trailer(55));
  }

  #[test]
  fn groups_resolve_per_message() {
    let d = dict();
    let nos = d.message(b"D");
    let parties = d.top_level_group(nos, false, 453).unwrap();
    let parties = d.group(parties);
    assert_eq!(parties.num_in_group, 453);
    assert_eq!(parties.delimiter, 448);
    assert!(parties.members.contains_key(&447));
    assert!(parties.members.contains_key(&452));
    // Nested group: its NumInGroup is a member, its own members are not.
    assert!(parties.members.contains_key(&802));
    assert!(!parties.members.contains_key(&523));
    assert!(parties.members[&448] < parties.members[&447]);

    let hops = d.top_level_group(nos, true, 627).unwrap();
    assert_eq!(d.group(hops).delimiter, 628);

    // NoLegs means a different group in different messages, so it has no
    // global meaning; within a message it resolves.
    let multileg = d.message(b"AB");
    let legs = d.top_level_group(multileg, false, 555).unwrap();
    assert!(d.group(legs).members.contains_key(&600));
    assert!(!d.global_groups.contains_key(&555));
  }

  #[test]
  fn nested_groups_resolve_within_their_parent() {
    let d = dict();
    let nos = d.message(b"D");
    let parties = d.top_level_group(nos, false, 453).unwrap();
    let sub = d.nested_group(parties, 802).unwrap();
    assert_eq!(d.group(sub).delimiter, 523);
    assert_eq!(d.nested_group(parties, 453), None);
  }

  #[test]
  fn names() {
    let d = dict();
    assert_eq!(d.field_name(44), Some("Price"));
    assert_eq!(d.codeset_name(54, b"1"), Some("Buy"));
    assert_eq!(d.codeset_name(54, b"?"), None);
    assert_eq!(d.message_name(b"D"), Some("NewOrderSingle"));
  }
}
