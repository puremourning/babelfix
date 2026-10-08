//! Generates the scoped definitions: a module per message, component and
//! repeating group, each with a scope marker type and the field and group
//! constants that belong to it.
//!
//! ```text
//! messages::new_order_single {
//!   NewOrderSingle                   scope marker; impls MessageScope
//!   MSG_TYPE, header, trailer
//!   fields { ClOrdID: Field<dt::Str, NewOrderSingle>, ...
//!            pub use components::instrument::fields::*, ... }
//!   groups { NoPartyIDs: GroupField<NewOrderSingle, PartyIDGrp>,
//!            NoPartyIDs = groups::party_id_grp (a module), ...
//!            pub use components::parties::groups::*, ... }
//! }
//! components::instrument { Instrument, fields, groups }
//! groups::party_id_grp   { PartyIDGrp, fields, groups }
//! ```
//!
//! A field or group a scope lists itself is a constant of that scope; those of
//! its components come through glob re-exports, so each is generated once,
//! where it is defined. `Within` impls say which components' fields a scope's
//! blocks accept: its own, and those of every component it includes, however
//! deeply — but not through groups, whose fields belong to their instances.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write;

use babelfix_repo::{FixVersion, MessageElement};

use super::{Marker, marker};

/// A message, component or group: something whose blocks have a scope.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ScopeId<'a> {
  Message(&'a str),
  Component(u32),
  Group(u32),
}

/// What a name in a scope's `fields` or `groups` module is: where it is
/// defined, and its tag (for a group, also its definition).
#[derive(Clone, Copy, PartialEq, Eq)]
struct Item<'a> {
  defined_in: ScopeId<'a>,
  tag: u32,
  group: Option<u32>,
}

/// The names a scope's `fields` and `groups` modules provide.
#[derive(Default, Clone)]
struct Provided<'a> {
  fields: BTreeMap<String, Item<'a>>,
  groups: BTreeMap<String, Item<'a>>,
}

struct Gen<'a> {
  fix: &'a FixVersion,
  /// `crate::fixlatest`.
  root: String,
  header: u32,
  trailer: u32,
  length_fields: &'a HashSet<u32>,
  group_fields: &'a HashSet<u32>,
  provided: BTreeMap<ScopeId<'a>, Provided<'a>>,
}

/// `NewOrderSingle` -> `new_order_single`, `PartyIDGrp` -> `party_id_grp`.
pub fn snake(name: &str) -> String {
  let c: Vec<char> = name.chars().collect();
  let mut out = String::new();
  for (i, &ch) in c.iter().enumerate() {
    let boundary = i > 0
      && ch.is_uppercase()
      && (c[i - 1].is_lowercase()
        || c[i - 1].is_ascii_digit()
        || (c[i - 1].is_uppercase()
          && c.get(i + 1).is_some_and(|n| n.is_lowercase())));
    if boundary {
      out.push('_');
    }
    out.extend(ch.to_lowercase());
  }
  match out.as_str() {
    "as" | "break" | "const" | "continue" | "crate" | "else" | "enum"
    | "extern" | "false" | "fn" | "for" | "if" | "impl" | "in" | "let"
    | "loop" | "match" | "mod" | "move" | "mut" | "pub" | "ref" | "return"
    | "self" | "static" | "struct" | "super" | "trait" | "true" | "type"
    | "unsafe" | "use" | "where" | "while" | "async" | "await" | "dyn"
    | "abstract" | "become" | "box" | "do" | "final" | "macro" | "override"
    | "priv" | "typeof" | "unsized" | "virtual" | "yield" | "try" | "gen" => {
      format!("r#{out}")
    }
    _ => out,
  }
}

impl<'a> Gen<'a> {
  fn name(&self, s: ScopeId<'a>) -> &'a str {
    match s {
      ScopeId::Message(name) => name,
      ScopeId::Component(id) => &self.fix.components[&id].name,
      ScopeId::Group(id) => &self.fix.groups[&id].name,
    }
  }

  /// The scope's module, from the crate root.
  fn module(&self, s: ScopeId<'a>) -> String {
    let kind = match s {
      ScopeId::Message(_) => "messages",
      ScopeId::Component(_) => "components",
      ScopeId::Group(_) => "groups",
    };
    format!("{}::{kind}::{}", self.root, snake(self.name(s)))
  }

  /// The scope's marker type, from the crate root.
  fn marker_type(&self, s: ScopeId<'a>) -> String {
    format!("{}::{}", self.module(s), self.name(s))
  }

  fn elements(&self, s: ScopeId<'a>) -> &'a [MessageElement] {
    match s {
      ScopeId::Message(name) => {
        &self.fix.messages.values().find(|m| m.name == name).unwrap().elements
      }
      ScopeId::Component(id) => &self.fix.components[&id].elements,
      ScopeId::Group(id) => &self.fix.groups[&id].elements,
    }
  }

  /// The components a scope lists itself, less a message's header and
  /// trailer: those are blocks of their own.
  fn components(&self, s: ScopeId<'a>) -> Vec<u32> {
    let mut out = Vec::new();
    for e in self.elements(s) {
      if let MessageElement::Component(c) = e
        && c.component_id != self.header
        && c.component_id != self.trailer
        && self.fix.components.contains_key(&c.component_id)
        && !out.contains(&c.component_id)
      {
        out.push(c.component_id);
      }
    }
    out
  }

  /// Every component whose fields a block of `s` holds: those it lists, and
  /// theirs, and so on.
  fn within(&self, s: ScopeId<'a>, out: &mut BTreeSet<u32>) {
    for c in self.components(s) {
      if out.insert(c) {
        self.within(ScopeId::Component(c), out);
      }
    }
  }

  /// The names `s` provides: its own, and its components' unless shadowed.
  /// A name two components provide differently is ambiguous, so `s` defines
  /// it itself.
  fn provided(&mut self, s: ScopeId<'a>) -> Provided<'a> {
    if let Some(p) = self.provided.get(&s) {
      return p.clone();
    }
    let mut own = Provided::default();
    for e in self.elements(s) {
      match e {
        MessageElement::Field(f) => {
          let Some(field) = self.fix.fields.get(&f.field_id) else {
            continue;
          };
          if matches!(
            marker(self.fix, field, self.length_fields, self.group_fields),
            Marker::Group
          ) {
            continue;
          }
          own.fields.entry(field.name.clone()).or_insert(Item {
            defined_in: s,
            tag: field.id,
            group: None,
          });
        }
        MessageElement::Group(g) => {
          let Some(group) = self.fix.groups.get(&g.group_id) else {
            continue;
          };
          let tag = group.num_in_group_tag;
          own.groups.entry(self.fix.fields[&tag].name.clone()).or_insert(
            Item {
              defined_in: s,
              tag,
              group: Some(group.id),
            },
          );
        }
        MessageElement::Component(_) => {}
      }
    }
    let mut from_components = Provided::default();
    let mut ambiguous = Provided::default();
    for c in self.components(s) {
      let theirs = self.provided(ScopeId::Component(c));
      for (mine, from, clash, items) in [
        (
          &own.fields,
          &mut from_components.fields,
          &mut ambiguous.fields,
          theirs.fields,
        ),
        (
          &own.groups,
          &mut from_components.groups,
          &mut ambiguous.groups,
          theirs.groups,
        ),
      ] {
        for (name, item) in items {
          if mine.contains_key(&name) {
            continue;
          }
          match from.get(&name) {
            Some(prev) if *prev != item => {
              clash.insert(name, item);
            }
            _ => {
              from.insert(name, item);
            }
          }
        }
      }
    }
    // Ambiguous names become the scope's own.
    for (name, item) in ambiguous.fields {
      from_components.fields.remove(&name);
      own.fields.insert(
        name,
        Item {
          defined_in: s,
          ..item
        },
      );
    }
    for (name, item) in ambiguous.groups {
      from_components.groups.remove(&name);
      own.groups.insert(
        name,
        Item {
          defined_in: s,
          ..item
        },
      );
    }
    let mut all = own;
    all.fields.extend(from_components.fields);
    all.groups.extend(from_components.groups);
    self.provided.insert(s, all.clone());
    all
  }

  fn emit(&mut self, s: ScopeId<'a>, src: &mut String) {
    let name = self.name(s);
    let ty = name;
    let provided = self.provided(s);
    let root = self.root.clone();

    let doc = match s {
      ScopeId::Message(_) => {
        let m = self.fix.messages.values().find(|m| m.name == name).unwrap();
        format!("The {name} message (MsgType `{}`).", m.msg_type)
      }
      ScopeId::Component(_) => format!("The {name} component."),
      ScopeId::Group(id) => {
        let tag = self.fix.groups[&id].num_in_group_tag;
        format!(
          "The {name} repeating group, counted by {} ({tag}).",
          self.fix.fields[&tag].name
        )
      }
    };
    writeln!(src, "  /// {doc}").unwrap();
    writeln!(src, "  pub mod {} {{", snake(name)).unwrap();
    writeln!(
      src,
      "    #[allow(unused_imports)]\n    \
       use babelfix_core::message::{{MessageScope, MsgType, Scope, Within}};"
    )
    .unwrap();
    writeln!(
      src,
      "    /// The scope of {name}'s fields: a block of it accepts them, and \
       those of the components it includes.\n    \
       pub enum {ty} {{}}\n    \
       impl Scope for {ty} {{}}\n    \
       impl Within<{ty}> for {ty} {{}}"
    )
    .unwrap();
    let mut within = BTreeSet::new();
    self.within(s, &mut within);
    for c in within {
      let c = self.marker_type(ScopeId::Component(c));
      writeln!(src, "    impl Within<{ty}> for {c} {{}}").unwrap();
    }

    if let ScopeId::Message(_) = s {
      let m = self.fix.messages.values().find(|m| m.name == name).unwrap();
      let header = ScopeId::Component(self.header);
      let trailer = ScopeId::Component(self.trailer);
      writeln!(
        src,
        "    impl MessageScope for {ty} {{\n      \
         const MSG_TYPE: MsgType = {root}::msg_type::{ty};\n      \
         const VERSION: &'static str = {:?};\n      \
         type Header = {};\n      \
         type Trailer = {};\n    }}\n    \
         /// `MsgType(35)`: `{}`.\n    \
         pub const MSG_TYPE: MsgType = {root}::msg_type::{ty};\n    \
         /// The header's fields and groups.\n    \
         pub use {} as header;\n    \
         /// The trailer's fields.\n    \
         pub use {} as trailer;",
        self.fix.name,
        self.marker_type(header),
        self.marker_type(trailer),
        m.msg_type,
        self.module(header),
        self.module(trailer),
      )
      .unwrap();
    }

    // fields
    if !provided.fields.is_empty() {
      writeln!(
        src,
        "    /// {name}'s fields, and those of the components it includes.\n    \
         pub mod fields {{\n      \
         #[allow(unused_imports)]\n      \
         use babelfix_core::message::types::{{Field, datatypes as dt}};\n      \
         #[allow(unused_imports)]\n      \
         use {root}::codesets as cs;"
      )
      .unwrap();
      let mut globs = BTreeSet::new();
      for (fname, item) in &provided.fields {
        if item.defined_in != s {
          continue;
        }
        let f = &self.fix.fields[&item.tag];
        let m = match marker(self.fix, f, self.length_fields, self.group_fields)
        {
          Marker::Datatype(m) => format!("dt::{m}"),
          Marker::Codeset(cs) => format!("cs::{cs}"),
          Marker::Group => unreachable!("groups are not fields"),
        };
        let required = self.required(s, item.tag);
        writeln!(
          src,
          "      /// {fname} ({}): `{}`{}\n      \
           pub const {fname}: Field<{m}, super::{ty}> = Field::new({});",
          f.id,
          f.field_type,
          if required { ", required" } else { "" },
          f.id
        )
        .unwrap();
      }
      for c in self.components(s) {
        if !self.provided(ScopeId::Component(c)).fields.is_empty() {
          globs.insert(self.module(ScopeId::Component(c)));
        }
      }
      for g in globs {
        writeln!(src, "      pub use {g}::fields::*;").unwrap();
      }
      src.push_str("    }\n");
    }

    // groups
    if !provided.groups.is_empty() {
      writeln!(
        src,
        "    /// {name}'s repeating groups, and those of the components it \
         includes: each a `GroupField`, and a module of the same name with \
         its instances' fields and groups.\n    \
         pub mod groups {{\n      \
         #[allow(unused_imports)]\n      \
         use babelfix_core::message::types::GroupField;"
      )
      .unwrap();
      let mut globs = BTreeSet::new();
      for (gname, item) in &provided.groups {
        if item.defined_in != s {
          continue;
        }
        let group = ScopeId::Group(item.group.unwrap());
        let required = self.required(s, item.tag);
        writeln!(
          src,
          "      /// {gname} ({}): the {} group{}\n      \
           pub const {gname}: GroupField<super::{ty}, {}> = GroupField::new({});\n      \
           pub use {} as {gname};",
          item.tag,
          self.name(group),
          if required { ", required" } else { "" },
          self.marker_type(group),
          item.tag,
          self.module(group),
        )
        .unwrap();
      }
      for c in self.components(s) {
        if !self.provided(ScopeId::Component(c)).groups.is_empty() {
          globs.insert(self.module(ScopeId::Component(c)));
        }
      }
      for g in globs {
        writeln!(src, "      pub use {g}::groups::*;").unwrap();
      }
      src.push_str("    }\n");
    }
    src.push_str("  }\n");
  }

  /// Whether `s` lists `tag` (a field, or a group's NumInGroup) as required.
  fn required(&self, s: ScopeId<'a>, tag: u32) -> bool {
    self.elements(s).iter().any(|e| match e {
      MessageElement::Field(f) => f.field_id == tag && f.required,
      MessageElement::Group(g) => {
        g.required
          && self
            .fix
            .groups
            .get(&g.group_id)
            .is_some_and(|d| d.num_in_group_tag == tag)
      }
      MessageElement::Component(_) => false,
    })
  }
}

/// The `messages`, `components` and `groups` modules for `fix`, which is
/// generated as module `module` of the crate.
pub fn generate_scopes(
  fix: &FixVersion,
  module: &str,
  length_fields: &HashSet<u32>,
  group_fields: &HashSet<u32>,
) -> String {
  let find = |name: &str| {
    fix
      .components
      .values()
      .find(|c| c.name == name)
      .unwrap_or_else(|| panic!("{}: no {name} component", fix.name))
      .id
  };
  let mut g = Gen {
    fix,
    root: format!("crate::{module}"),
    header: find("StandardHeader"),
    trailer: find("StandardTrailer"),
    length_fields,
    group_fields,
    provided: BTreeMap::new(),
  };

  let mut src = String::new();
  let mut sections: [(&str, &str, Vec<ScopeId>); 3] = [
    (
      "messages",
      "Messages: a module per message, with its scope and its fields and \
       groups.",
      fix
        .messages
        .values()
        .map(|m| ScopeId::Message(m.name.as_str()))
        .collect(),
    ),
    (
      "components",
      "Components: a module per component, with its scope and its fields \
       and groups.",
      fix.components.keys().map(|&id| ScopeId::Component(id)).collect(),
    ),
    (
      "groups",
      "Repeating groups: a module per group definition, with the scope of \
       its instances and their fields and groups.",
      fix.groups.keys().map(|&id| ScopeId::Group(id)).collect(),
    ),
  ];
  src.push_str(&message_instance(&g));
  for (module, doc, scopes) in &mut sections {
    scopes.sort_by_key(|&s| snake(g.name(s)));
    let mut seen = HashSet::new();
    for &s in scopes.iter() {
      assert!(
        seen.insert(snake(g.name(s))),
        "{}: two {module} are named {}",
        fix.name,
        snake(g.name(s))
      );
    }
    writeln!(src, "/// {doc}\npub mod {module} {{").unwrap();
    for &s in scopes.iter() {
      g.emit(s, &mut src);
    }
    src.push_str("}\n\n");
  }
  src
}

/// The `MessageInstance` enum: a variant per message, holding it as a
/// `TypedMessage` of that type.
fn message_instance(g: &Gen) -> String {
  let fix = g.fix;
  let mut messages: Vec<_> = fix.messages.values().collect();
  messages.sort_by(|a, b| a.name.cmp(&b.name));
  assert!(
    messages.iter().all(|m| m.name != "Unknown"),
    "{}: a message named Unknown",
    fix.name
  );

  let mut src = String::new();
  writeln!(
    src,
    "/// A {version} message, by type: match on it to handle each type with \
     its own fields.\n\
     ///\n\
     /// `M` is how the message is held, as in \
     /// [`TypedMessage`](babelfix_core::message::TypedMessage): \
     /// `MessageInstance::from(msg)` takes it, `MessageInstance::from(&msg)` \
     /// borrows it. See the [crate documentation](crate#received-messages).\n\
     #[non_exhaustive]\n\
     pub enum MessageInstance<M = babelfix_core::message::Message> {{",
    version = fix.name
  )
  .unwrap();
  for m in &messages {
    writeln!(
      src,
      "  /// {} (`{}`).\n  \
       {}(babelfix_core::message::TypedMessage<{}, M>),",
      m.name,
      m.msg_type,
      m.name,
      g.marker_type(ScopeId::Message(&m.name))
    )
    .unwrap();
  }
  writeln!(
    src,
    "  /// A message of a type {} does not define, or of another version.\n  \
     Unknown(M),\n}}\n",
    fix.name
  )
  .unwrap();

  // From: one MsgType match, then the checked conversion.
  writeln!(
    src,
    "impl<M: std::borrow::Borrow<babelfix_core::message::Message>> From<M> \
     for MessageInstance<M> {{\n  \
     fn from(msg: M) -> Self {{\n    \
     use babelfix_core::message::TypedMessage;\n    \
     let msg_type = msg.borrow().msg_type();\n    \
     match msg_type {{"
  )
  .unwrap();
  for m in &messages {
    writeln!(
      src,
      "      {:?} => match TypedMessage::try_from_message(msg) {{\n        \
       Ok(m) => Self::{}(m),\n        \
       Err(m) => Self::Unknown(m),\n      }},",
      m.msg_type, m.name
    )
    .unwrap();
  }
  src.push_str("      _ => Self::Unknown(msg),\n    }\n  }\n}\n\n");

  writeln!(
    src,
    "impl<M: std::borrow::Borrow<babelfix_core::message::Message>> \
     MessageInstance<M> {{\n  \
     /// The message, unchecked.\n  \
     pub fn as_untyped(&self) -> &babelfix_core::message::Message {{\n    \
     match self {{"
  )
  .unwrap();
  for m in &messages {
    writeln!(src, "      Self::{}(m) => m.as_untyped(),", m.name).unwrap();
  }
  src.push_str(
    "      Self::Unknown(m) => m.borrow(),\n    }\n  }\n\n  \
     /// The message, as it was held.\n  \
     pub fn into_untyped(self) -> M {\n    match self {\n",
  );
  for m in &messages {
    writeln!(src, "      Self::{}(m) => m.into_untyped(),", m.name).unwrap();
  }
  src.push_str("      Self::Unknown(m) => m,\n    }\n  }\n}\n\n");
  src
}
