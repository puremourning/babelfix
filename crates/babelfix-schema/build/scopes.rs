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
//! MessageInstance          a variant per message
//! ```
//!
//! A field or group a scope lists itself is a constant of that scope; those of
//! its components come through glob re-exports, so each is generated once,
//! where it is defined. `Within` impls say which components' fields a scope's
//! blocks accept: its own, and those of every component it includes, however
//! deeply — but not through groups, whose fields belong to their instances.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use babelfix_repo::{FixVersion, MessageElement};
use proc_macro2::TokenStream;
use quote::quote;

use super::{Marker, doc, ident, marker, number};

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
  root: TokenStream,
  header: u32,
  trailer: u32,
  length_fields: &'a HashSet<u32>,
  group_fields: &'a HashSet<u32>,
  provided: BTreeMap<ScopeId<'a>, Provided<'a>>,
}

/// `NewOrderSingle` -> `new_order_single`, `PartyIDGrp` -> `party_id_grp`.
fn snake(name: &str) -> String {
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
  out
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
  fn module(&self, s: ScopeId<'a>) -> TokenStream {
    let root = &self.root;
    let kind = match s {
      ScopeId::Message(_) => quote! { messages },
      ScopeId::Component(_) => quote! { components },
      ScopeId::Group(_) => quote! { groups },
    };
    let module = ident(&snake(self.name(s)));
    quote! { #root::#kind::#module }
  }

  /// The scope's marker type, from the crate root.
  fn marker_type(&self, s: ScopeId<'a>) -> TokenStream {
    let module = self.module(s);
    let name = ident(self.name(s));
    quote! { #module::#name }
  }

  fn elements(&self, s: ScopeId<'a>) -> &'a [MessageElement] {
    match s {
      ScopeId::Message(name) => {
        &self
          .fix
          .messages
          .values()
          .find(|m| m.name == name)
          .unwrap()
          .elements
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
          own
            .groups
            .entry(self.fix.fields[&tag].name.clone())
            .or_insert(Item {
              defined_in: s,
              tag,
              group: Some(group.id),
            });
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

  /// The modules of `s`'s components that provide something in `fields` (or
  /// `groups`): the glob re-exports, in name order.
  fn component_globs(
    &mut self,
    s: ScopeId<'a>,
    groups: bool,
  ) -> Vec<TokenStream> {
    let mut globs = BTreeMap::new();
    for c in self.components(s) {
      let theirs = self.provided(ScopeId::Component(c));
      let provides = if groups {
        !theirs.groups.is_empty()
      } else {
        !theirs.fields.is_empty()
      };
      if provides {
        let c = ScopeId::Component(c);
        globs.insert(snake(self.name(c)), self.module(c));
      }
    }
    globs.into_values().collect()
  }

  /// The scope's module.
  fn emit(&mut self, s: ScopeId<'a>) -> TokenStream {
    let name = self.name(s);
    let ty = ident(name);
    let module = ident(&snake(name));
    let provided = self.provided(s);
    let root = self.root.clone();

    let doc = doc(match s {
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
    });
    let scope_doc = super::doc(format!(
      "The scope of {name}'s fields: a block of it accepts them, and those of \
       the components it includes."
    ));

    let mut within = BTreeSet::new();
    self.within(s, &mut within);
    let within = within.into_iter().map(|c| {
      let c = self.marker_type(ScopeId::Component(c));
      quote! { impl Within<#ty> for #c {} }
    });
    let within: TokenStream = within.collect();

    let message = match s {
      ScopeId::Message(_) => {
        let m = self.fix.messages.values().find(|m| m.name == name).unwrap();
        let version = &self.fix.name;
        let header = self.marker_type(ScopeId::Component(self.header));
        let trailer = self.marker_type(ScopeId::Component(self.trailer));
        let header_module = self.module(ScopeId::Component(self.header));
        let trailer_module = self.module(ScopeId::Component(self.trailer));
        let msg_type_doc =
          super::doc(format!("`MsgType(35)`: `{}`.", m.msg_type));
        quote! {
          impl MessageScope for #ty {
            const MSG_TYPE: MsgType = #root::msg_type::#ty;
            const VERSION: &'static str = #version;
            type Header = #header;
            type Trailer = #trailer;
          }
          #msg_type_doc
          pub const MSG_TYPE: MsgType = #root::msg_type::#ty;
          /// The header's fields and groups.
          pub use #header_module as header;
          /// The trailer's fields.
          pub use #trailer_module as trailer;
        }
      }
      _ => TokenStream::new(),
    };

    let fields = if provided.fields.is_empty() {
      TokenStream::new()
    } else {
      let consts = provided
        .fields
        .iter()
        .filter(|(_, item)| item.defined_in == s)
        .map(|(fname, item)| {
          let f = &self.fix.fields[&item.tag];
          let m =
            marker(self.fix, f, self.length_fields, self.group_fields).tokens();
          let doc = super::doc(format!(
            "{fname} ({}): `{}`{}",
            f.id,
            f.field_type,
            if self.required(s, item.tag) {
              ", required"
            } else {
              ""
            }
          ));
          let fname = ident(fname);
          let tag = number(f.id);
          quote! {
            #doc
            pub const #fname: Field<#m, super::#ty> = Field::new(#tag);
          }
        })
        .collect::<TokenStream>();
      let globs = self.component_globs(s, false);
      let doc = super::doc(format!(
        "{name}'s fields, and those of the components it includes."
      ));
      quote! {
        #doc
        pub mod fields {
          #[allow(unused_imports)]
          use babelfix_core::message::types::{Field, datatypes as dt};
          #[allow(unused_imports)]
          use #root::codesets as cs;
          #consts
          #(pub use #globs::fields::*;)*
        }
      }
    };

    let groups = if provided.groups.is_empty() {
      TokenStream::new()
    } else {
      let consts = provided
        .groups
        .iter()
        .filter(|(_, item)| item.defined_in == s)
        .map(|(gname, item)| {
          let group = ScopeId::Group(item.group.unwrap());
          let doc = super::doc(format!(
            "{gname} ({}): the {} group{}",
            item.tag,
            self.name(group),
            if self.required(s, item.tag) {
              ", required"
            } else {
              ""
            }
          ));
          let gname = ident(gname);
          let tag = number(item.tag);
          let instance = self.marker_type(group);
          let module = self.module(group);
          quote! {
            #doc
            pub const #gname: GroupField<super::#ty, #instance> =
              GroupField::new(#tag);
            pub use #module as #gname;
          }
        })
        .collect::<TokenStream>();
      let globs = self.component_globs(s, true);
      let doc = super::doc(format!(
        "{name}'s repeating groups, and those of the components it includes: \
         each a `GroupField`, and a module of the same name with its \
         instances' fields and groups."
      ));
      quote! {
        #doc
        pub mod groups {
          #[allow(unused_imports)]
          use babelfix_core::message::types::GroupField;
          #consts
          #(pub use #globs::groups::*;)*
        }
      }
    };

    quote! {
      #doc
      pub mod #module {
        #[allow(unused_imports)]
        use babelfix_core::message::{MessageScope, MsgType, Scope, Within};
        #scope_doc
        pub enum #ty {}
        impl Scope for #ty {}
        impl Within<#ty> for #ty {}
        #within
        #message
        #fields
        #groups
      }
    }
  }
}

/// The `messages`, `components` and `groups` modules for `fix`, which is
/// generated as module `module` of the crate, and its `MessageInstance`.
pub fn generate_scopes(
  fix: &FixVersion,
  module: &str,
  length_fields: &HashSet<u32>,
  group_fields: &HashSet<u32>,
) -> TokenStream {
  let find = |name: &str| {
    fix
      .components
      .values()
      .find(|c| c.name == name)
      .unwrap_or_else(|| panic!("{}: no {name} component", fix.name))
      .id
  };
  let module = ident(module);
  let mut g = Gen {
    fix,
    root: quote! { crate::#module },
    header: find("StandardHeader"),
    trailer: find("StandardTrailer"),
    length_fields,
    group_fields,
    provided: BTreeMap::new(),
  };

  let mut src = message_instance(&g);
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
      fix
        .components
        .keys()
        .map(|&id| ScopeId::Component(id))
        .collect(),
    ),
    (
      "groups",
      "Repeating groups: a module per group definition, with the scope of \
       its instances and their fields and groups.",
      fix.groups.keys().map(|&id| ScopeId::Group(id)).collect(),
    ),
  ];
  for (module, section_doc, scopes) in &mut sections {
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
    let modules: TokenStream = scopes.iter().map(|&s| g.emit(s)).collect();
    let section_doc = doc(*section_doc);
    let module = ident(module);
    src.extend(quote! {
      #section_doc
      pub mod #module {
        #modules
      }
    });
  }
  src
}

/// The `MessageInstance` enum: a variant per message, holding it as a
/// `TypedMessage` of that type.
fn message_instance(g: &Gen) -> TokenStream {
  let fix = g.fix;
  let mut messages: Vec<_> = fix.messages.values().collect();
  messages.sort_by(|a, b| a.name.cmp(&b.name));
  assert!(
    messages.iter().all(|m| m.name != "Unknown"),
    "{}: a message named Unknown",
    fix.name
  );

  let enum_doc = doc(format!(
    "A {} message, by type: match on it to handle each type with its own \
     fields.\n\
     \n\
     `M` is how the message is held, as in\n\
     [`TypedMessage`](babelfix_core::message::TypedMessage):\n\
     `MessageInstance::from(msg)` takes it, `MessageInstance::from(&msg)`\n\
     borrows it. See the [crate documentation](crate#received-messages).",
    fix.name
  ));
  let unknown_doc = doc(format!(
    "A message of a type {} does not define, or of another version.",
    fix.name
  ));
  let names: Vec<_> = messages.iter().map(|m| ident(&m.name)).collect();
  let variants = messages.iter().zip(&names).map(|(m, name)| {
    let doc = doc(format!("{} (`{}`).", m.name, m.msg_type));
    let scope = g.marker_type(ScopeId::Message(&m.name));
    quote! {
      #doc
      #name(babelfix_core::message::TypedMessage<#scope, M>),
    }
  });
  // One MsgType match, then the checked conversion.
  let arms = messages.iter().zip(&names).map(|(m, name)| {
    let msg_type = &m.msg_type;
    quote! {
      #msg_type => match TypedMessage::try_from_message(msg) {
        Ok(m) => Self::#name(m),
        Err(m) => Self::Unknown(m),
      },
    }
  });

  quote! {
    #enum_doc
    #[non_exhaustive]
    pub enum MessageInstance<M = babelfix_core::message::Message> {
      #(#variants)*
      #unknown_doc
      Unknown(M),
    }

    impl<M: std::borrow::Borrow<babelfix_core::message::Message>> From<M>
      for MessageInstance<M>
    {
      fn from(msg: M) -> Self {
        use babelfix_core::message::TypedMessage;
        let msg_type = msg.borrow().msg_type();
        match msg_type {
          #(#arms)*
          _ => Self::Unknown(msg),
        }
      }
    }

    impl<M: std::borrow::Borrow<babelfix_core::message::Message>>
      MessageInstance<M>
    {
      /// The message, unchecked.
      pub fn as_untyped(&self) -> &babelfix_core::message::Message {
        match self {
          #(Self::#names(m) => m.as_untyped(),)*
          Self::Unknown(m) => m.borrow(),
        }
      }

      /// The message, as it was held.
      pub fn into_untyped(self) -> M {
        match self {
          #(Self::#names(m) => m.into_untyped(),)*
          Self::Unknown(m) => m,
        }
      }
    }
  }
}
