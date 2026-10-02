//! Generates the typed schema (`crate::schema::{fields, tags, msg_type,
//! codesets}`) from the FIX.Latest Orchestra data.
//!
//! One set of constants serves every FIX version: tag numbers, datatypes and
//! codes are shared, and FIX.Latest is the superset.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write;

use babelfix_repo::{Field, FixVersion};

fn main() {
  println!("cargo:rerun-if-changed=build.rs");
  let repo = babelfix_repo::orchestrate().expect("embedded Orchestra data");
  let fix = repo.get_version("FIX.Latest").expect("FIX.Latest");
  let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap())
    .join("schema.rs");
  std::fs::write(out, generate(&fix)).unwrap();
}

/// Where a field's value type comes from.
enum Marker {
  Datatype(&'static str),
  Codeset(String),
  Group,
}

fn generate(fix: &FixVersion) -> String {
  let mut fields: Vec<&Field> =
    fix.fields.values().map(|f| f.as_ref()).collect();
  fields.sort_by_key(|f| f.id);

  let length_fields: HashSet<u32> =
    fields.iter().filter_map(|f| f.length_id).collect();

  let mut src = String::new();
  let mut codesets_used = BTreeMap::new();

  // fields
  src.push_str(
    "/// Typed field constants: `Field<M>` for a field of datatype `M`, \
     `GroupField` for a NumInGroup.\npub mod fields {\n  \
     use crate::message::types::{Field, GroupField, datatypes as dt};\n  \
     use super::codesets as cs;\n",
  );
  for f in &fields {
    let doc = format!("  /// {} ({}): `{}`\n", f.name, f.id, f.field_type);
    src.push_str(&doc);
    match marker(fix, f, &length_fields) {
      Marker::Group => writeln!(
        src,
        "  pub const {}: GroupField = GroupField::new({});",
        f.name, f.id
      ),
      Marker::Datatype(m) => writeln!(
        src,
        "  pub const {}: Field<dt::{m}> = Field::new({});",
        f.name, f.id
      ),
      Marker::Codeset(cs) => {
        let marker = cs.clone();
        codesets_used.insert(cs, ());
        writeln!(
          src,
          "  pub const {}: Field<cs::{marker}> = Field::new({});",
          f.name, f.id
        )
      }
    }
    .unwrap();
  }
  src.push_str("}\n\n");

  // tags
  src.push_str(
    "/// Plain tag numbers, for `match` and for loops over several fields.\n\
     pub mod tags {\n",
  );
  for f in &fields {
    writeln!(src, "  pub const {}: u32 = {};", f.name, f.id).unwrap();
  }
  src.push_str("}\n\n");

  // msg_type
  src.push_str(
    "/// `MsgType(35)` values, by message name.\npub mod msg_type {\n  \
     use crate::message::types::MsgType;\n",
  );
  let mut messages: Vec<_> = fix.messages.values().collect();
  messages.sort_by(|a, b| a.name.cmp(&b.name));
  for m in messages {
    writeln!(
      src,
      "  pub const {}: MsgType = MsgType::new({:?});",
      m.name, m.msg_type
    )
    .unwrap();
  }
  src.push_str("}\n\n");

  // codesets
  src.push_str(
    "/// Codeset enums, and their datatype markers (`SideCodeSet` for `Side`).\n\
     ///\n\
     /// Decoding never fails: a value the codeset does not list — a newer code,\n\
     /// or one agreed bilaterally, as `Reserved100Plus` codesets invite — is\n\
     /// `Unlisted`, carrying its bytes, and can be written back as it came.\n\
     pub mod codesets {\n  \
     use crate::message::ValueError;\n  \
     use crate::message::types::{FieldType, ToFix, ValueWriter};\n",
  );
  let mut names = HashSet::new();
  for cs in codesets_used.keys() {
    let codes = &fix.codesets[cs];
    let enum_name = cs.strip_suffix("CodeSet").unwrap_or(cs);
    assert!(names.insert(enum_name.to_owned()), "duplicate {enum_name}");
    assert!(names.insert(cs.clone()), "duplicate {cs}");
    let base = &fix.codeset_types[cs];

    writeln!(src, "\n  /// The datatype of fields whose values are a [`{enum_name}`] (`{base}`).").unwrap();
    writeln!(src, "  pub enum {cs} {{}}").unwrap();
    writeln!(
      src,
      "  impl FieldType for {cs} {{\n    \
       type Value<'a> = {enum_name}<'a>;\n    \
       fn decode(raw: &[u8]) -> Result<{enum_name}<'_>, ValueError> {{\n      \
       Ok({enum_name}::from_bytes(raw))\n    }}\n  }}"
    )
    .unwrap();
    writeln!(
      src,
      "  impl ToFix<{cs}> for {enum_name}<'_> {{\n    \
       fn to_fix(&self, w: &mut ValueWriter<'_>) -> Result<(), ValueError> {{\n      \
       w.put(self.wire());\n      Ok(())\n    }}\n  }}"
    )
    .unwrap();

    writeln!(
      src,
      "  #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]\n  #[non_exhaustive]\n  \
       pub enum {enum_name}<'a> {{"
    )
    .unwrap();
    for code in codes.iter() {
      writeln!(src, "    /// `{}`", code.value).unwrap();
      writeln!(src, "    {},", code.name).unwrap();
    }
    writeln!(
      src,
      "    /// A value the codeset does not list.\n    Unlisted(&'a [u8]),\n  }}"
    )
    .unwrap();

    writeln!(src, "  impl<'a> {enum_name}<'a> {{").unwrap();
    writeln!(
      src,
      "    /// Decode; never fails. Listed values always decode to their variant,\n    \
       /// never to `Unlisted`.\n    \
       pub fn from_bytes(raw: &'a [u8]) -> Self {{\n      match raw {{"
    )
    .unwrap();
    for code in codes.iter() {
      writeln!(src, "        b{:?} => Self::{},", code.value, code.name)
        .unwrap();
    }
    writeln!(
      src,
      "        other => Self::Unlisted(other),\n      }}\n    }}"
    )
    .unwrap();

    writeln!(
      src,
      "    /// The value as it goes on the wire.\n    \
       pub fn wire(&self) -> &'a [u8] {{\n      match self {{"
    )
    .unwrap();
    for code in codes.iter() {
      writeln!(src, "        Self::{} => b{:?},", code.name, code.value)
        .unwrap();
    }
    writeln!(src, "        Self::Unlisted(v) => v,\n      }}\n    }}").unwrap();

    writeln!(
      src,
      "    /// The code's name in the FIX specification; empty for `Unlisted`.\n    \
       pub fn name(&self) -> &'static str {{\n      match self {{"
    )
    .unwrap();
    for code in codes.iter() {
      writeln!(src, "        Self::{0} => {0:?},", code.name).unwrap();
    }
    writeln!(
      src,
      "        Self::Unlisted(_) => \"\",\n      }}\n    }}\n  }}"
    )
    .unwrap();
  }
  src.push_str("}\n");
  src
}

fn marker(fix: &FixVersion, f: &Field, length_fields: &HashSet<u32>) -> Marker {
  if length_fields.contains(&f.id) {
    return Marker::Datatype("DataLength");
  }
  let datatype = f.datatype(fix);
  if fix.datatype_is(datatype, "NumInGroup") {
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
