//! Generates the typed schema (`fields`, `tags`, `msg_type`, `codesets`)
//! from the Orchestra data: a module per FIX version, FIX.Latest always and
//! the others when their feature is enabled.
//!
//! Code is built as tokens with `quote!` and formatted with `prettyplease`, so
//! the files in `OUT_DIR` are readable when a compile error points into them.

use std::collections::{BTreeSet, HashSet};

use babelfix_repo::{Field, FixVersion};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

#[path = "build/scopes.rs"]
mod scopes;

/// The module name for a version: `FIX.4.4` -> `fix44`.
fn identifier_from_version(version: &str) -> String {
  version.to_lowercase().replace('.', "")
}

fn main() {
  println!("cargo:rerun-if-changed=build.rs");
  println!("cargo:rerun-if-changed=build");
  let repo = babelfix_repo::orchestrate().expect("embedded Orchestra data");
  let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

  // FIX.Latest is always built; other versions only when their feature is on.
  let mut versions: Vec<_> = repo
    .versions
    .iter()
    .map(|(version, fix)| (identifier_from_version(version), fix))
    .filter(|(module, _)| {
      module == "fixlatest"
        || std::env::var_os(format!(
          "CARGO_FEATURE_{}",
          module.to_uppercase()
        ))
        .is_some()
    })
    .collect();
  versions.sort_by(|a, b| a.0.cmp(&b.0));

  let mut lib = TokenStream::new();
  for (module, fix) in versions {
    write(&out.join(format!("{module}.rs")), generate(fix, &module));
    let file = format!("/{module}.rs");
    let module = ident(&module);
    lib.extend(quote! {
      pub mod #module { include!(concat!(env!("OUT_DIR"), #file)); }
    });
  }
  lib.extend(quote! { pub use fixlatest::*; });
  write(&out.join("schema.rs"), lib);
}

/// Format `tokens` and write them to `path`.
fn write(path: &std::path::Path, tokens: TokenStream) {
  let file = syn::parse2(tokens).unwrap_or_else(|e| {
    panic!("generated invalid code for {}: {e}", path.display())
  });
  std::fs::write(path, prettyplease::unparse(&file)).unwrap();
}

/// An identifier, raw if it is a keyword: `type` -> `r#type`.
fn ident(name: &str) -> Ident {
  if syn::parse_str::<Ident>(name).is_ok() {
    Ident::new(name, Span::call_site())
  } else {
    Ident::new_raw(name, Span::call_site())
  }
}

/// A doc comment: one `#[doc]` per line, as `///` lines would be.
fn doc(text: impl AsRef<str>) -> TokenStream {
  text
    .as_ref()
    .lines()
    .map(|line| {
      let line = if line.is_empty() {
        String::new()
      } else {
        format!(" {line}")
      };
      quote! { #[doc = #line] }
    })
    .collect()
}

/// A tag or other number, without a type suffix: `11`, not `11u32`.
fn number(n: u32) -> Literal {
  Literal::u32_unsuffixed(n)
}

/// Where a field's value type comes from.
enum Marker {
  Datatype(&'static str),
  Codeset(String),
  Group,
}

impl Marker {
  /// The field's datatype marker, from inside a module that imports the
  /// datatypes as `dt` and the codesets as `cs`.
  fn tokens(&self) -> TokenStream {
    match self {
      Marker::Datatype(m) => {
        let m = ident(m);
        quote! { dt::#m }
      }
      Marker::Codeset(cs) => {
        let cs = ident(cs);
        quote! { cs::#cs }
      }
      Marker::Group => unreachable!("a group has no datatype marker"),
    }
  }
}

fn generate(fix: &FixVersion, module: &str) -> TokenStream {
  let mut fields: Vec<&Field> =
    fix.fields.values().map(|f| f.as_ref()).collect();
  fields.sort_by_key(|f| f.id);

  let length_fields: HashSet<u32> =
    fields.iter().filter_map(|f| f.length_id).collect();
  // Before FIX 4.4 there is no NumInGroup datatype: a group's count field is
  // an `int`, so find them from the groups themselves.
  let group_fields: HashSet<u32> =
    fix.groups.values().map(|g| g.num_in_group_tag).collect();

  let mut codesets_used = BTreeSet::new();

  // fields
  let field_consts = fields.iter().map(|f| {
    let doc = doc(format!("{} ({}): `{}`", f.name, f.id, f.field_type));
    let name = ident(&f.name);
    let tag = number(f.id);
    let marker = marker(fix, f, &length_fields, &group_fields);
    if let Marker::Codeset(cs) = &marker {
      codesets_used.insert(cs.clone());
    }
    match marker {
      Marker::Group => quote! {
        #doc
        pub const #name: GroupField = GroupField::new(#tag);
      },
      m => {
        let m = m.tokens();
        quote! {
          #doc
          pub const #name: Field<#m> = Field::new(#tag);
        }
      }
    }
  });
  let field_consts: TokenStream = field_consts.collect();
  let fields_doc = doc(
    "Typed field constants: `Field<M>` for a field of datatype `M`, \
     `GroupField` for a NumInGroup.",
  );

  // tags
  let tag_consts = fields.iter().map(|f| {
    let name = ident(&f.name);
    let tag = number(f.id);
    quote! { pub const #name: u32 = #tag; }
  });

  // msg_type
  let mut messages: Vec<_> = fix.messages.values().collect();
  messages.sort_by(|a, b| a.name.cmp(&b.name));
  let msg_types = messages.iter().map(|m| {
    let name = ident(&m.name);
    let msg_type = &m.msg_type;
    quote! { pub const #name: MsgType = MsgType::new(#msg_type); }
  });

  // codesets
  let mut names = HashSet::new();
  let codesets = codesets_used.iter().map(|cs| {
    let codes = &fix.codesets[cs];
    let enum_name = cs.strip_suffix("CodeSet").unwrap_or(cs);
    assert!(names.insert(enum_name.to_owned()), "duplicate {enum_name}");
    assert!(names.insert(cs.clone()), "duplicate {cs}");
    let marker_doc = doc(format!(
      "The datatype of fields whose values are a [`{enum_name}`] (`{}`).",
      fix.codeset_types[cs]
    ));
    let cs = ident(cs);
    let enum_name = ident(enum_name);

    let variants = codes.iter().map(|code| {
      let doc = doc(format!("`{}`", code.value));
      let name = ident(&code.name);
      quote! { #doc #name, }
    });
    let from_bytes = codes.iter().map(|code| {
      let name = ident(&code.name);
      let value = Literal::byte_string(code.value.as_bytes());
      quote! { #value => Self::#name, }
    });
    let wire = codes.iter().map(|code| {
      let name = ident(&code.name);
      let value = Literal::byte_string(code.value.as_bytes());
      quote! { Self::#name => #value, }
    });
    let code_names = codes.iter().map(|code| {
      let name = ident(&code.name);
      let text = &code.name;
      quote! { Self::#name => #text, }
    });

    quote! {
      #marker_doc
      pub enum #cs {}
      impl FieldType for #cs {
        type Value<'a> = #enum_name<'a>;
        fn decode(raw: &[u8]) -> Result<#enum_name<'_>, ValueError> {
          Ok(#enum_name::from_bytes(raw))
        }
      }
      impl ToFix<#cs> for #enum_name<'_> {
        fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {
          w.put(self.wire());
          Ok(())
        }
      }
      #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
      #[non_exhaustive]
      pub enum #enum_name<'a> {
        #(#variants)*
        /// A value the codeset does not list.
        Unlisted(&'a [u8]),
      }
      impl<'a> #enum_name<'a> {
        /// Decode; never fails. Listed values always decode to their variant,
        /// never to `Unlisted`.
        pub fn from_bytes(raw: &'a [u8]) -> Self {
          match raw {
            #(#from_bytes)*
            other => Self::Unlisted(other),
          }
        }
        /// The value as it goes on the wire.
        pub fn wire(&self) -> &'a [u8] {
          match self {
            #(#wire)*
            Self::Unlisted(v) => v,
          }
        }
        /// The code's name in the FIX specification; empty for `Unlisted`.
        pub fn name(&self) -> &'static str {
          match self {
            #(#code_names)*
            Self::Unlisted(_) => "",
          }
        }
      }
    }
  });
  let codesets: TokenStream = codesets.collect();

  let scopes =
    scopes::generate_scopes(fix, module, &length_fields, &group_fields);

  quote! {
    #fields_doc
    pub mod fields {
      use babelfix_core::message::types::{Field, GroupField, datatypes as dt};
      use super::codesets as cs;
      #field_consts
    }

    /// Plain tag numbers, for `match` and for loops over several fields.
    pub mod tags {
      #(#tag_consts)*
    }

    /// `MsgType(35)` values, by message name.
    pub mod msg_type {
      use babelfix_core::message::types::MsgType;
      #(#msg_types)*
    }

    /// Codeset enums, and their datatype markers (`SideCodeSet` for `Side`).
    ///
    /// Decoding never fails: a value the codeset does not list — a newer code,
    /// or one agreed bilaterally, as `Reserved100Plus` codesets invite — is
    /// `Unlisted`, carrying its bytes, and can be written back as it came.
    pub mod codesets {
      use babelfix_core::message::ValueError;
      use babelfix_core::message::types::{FieldType, ToFix, ValueWriter};
      #codesets
    }

    #scopes
  }
}

fn marker(
  fix: &FixVersion,
  f: &Field,
  length_fields: &HashSet<u32>,
  group_fields: &HashSet<u32>,
) -> Marker {
  if length_fields.contains(&f.id) {
    return Marker::Datatype("DataLength");
  }
  let datatype = f.datatype(fix);
  if group_fields.contains(&f.id) || fix.datatype_is(datatype, "NumInGroup") {
    return Marker::Group;
  }
  if f.is_codeset(fix) {
    return match datatype {
      "int" | "char" | "String" => Marker::Codeset(f.field_type.clone()),
      "Boolean" => Marker::Datatype("Boolean"),
      // Multiple-value codesets: a space-separated list, kept as text for now.
      _ => Marker::Datatype("Str"),
    };
  }
  // The nearest datatype we have a marker for, walking up `baseType`.
  let known = [
    ("UTCTimestamp", "UtcTimestamp"),
    ("UTCDateOnly", "UtcDateOnly"),
    ("UTCTimeOnly", "UtcTimeOnly"),
    ("LocalMktDate", "LocalMktDate"),
    ("LocalMktTime", "LocalMktTime"),
    ("Qty", "Qty"),
    ("Price", "Price"),
    ("PriceOffset", "PriceOffset"),
    ("Amt", "Amt"),
    ("Percentage", "Percentage"),
    ("float", "Float"),
    ("SeqNum", "UInt"),
    ("Length", "UInt"),
    ("TagNum", "UInt"),
    ("DayOfMonth", "UInt"),
    ("int", "Int"),
    ("Boolean", "Boolean"),
    ("char", "Char"),
    ("data", "Data"),
    ("XMLData", "Data"),
  ];
  for (name, m) in known {
    if fix.datatype_is(datatype, name) {
      return Marker::Datatype(m);
    }
  }
  Marker::Datatype("Str")
}
