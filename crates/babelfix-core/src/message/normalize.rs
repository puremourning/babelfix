//! Canonical field order, for comparing and displaying messages.

use std::collections::HashMap;

use super::tape::{Entry, Kind, Message, Region};
use crate::repository::{FieldBlock, FixVersion, MessageElement};

/// Field order within one block: tag → position in its definition.
type Order = HashMap<u32, usize>;

impl Message {
  /// Put every block's fields in the order the dictionary defines them: the
  /// header in `StandardHeader` order, the body in the message's order, each
  /// group instance in its group's order. Fields the dictionary does not place
  /// come after those it does, by tag number. Values and structure are
  /// unchanged; two messages with the same content normalise to the same tape.
  ///
  /// Only entries move — no message bytes are touched.
  pub fn normalize(&mut self) {
    let fix = self.dict.version().clone();
    let header = component_order(&fix, "StandardHeader");
    let trailer = component_order(&fix, "StandardTrailer");
    let body = fix
      .get_message(self.msg_type())
      .map(|m| block_order(&fix, m.get_elements()))
      .unwrap_or_default();

    let mut tape = Vec::with_capacity(self.tape.len());
    let (s, e) = self.region_range(Region::Header);
    self.reorder(s, e, &header, &mut tape);
    tape.push(Entry::gap(super::tape::HEADER_GAP));
    tape.resize(
      tape.len() + super::tape::HEADER_GAP as usize - 1,
      Entry::default(),
    );
    let body_start = tape.len() as u32;
    let (s, e) = self.region_range(Region::Body);
    self.reorder(s, e, &body, &mut tape);
    let trailer_start = tape.len() as u32;
    let (s, e) = self.region_range(Region::Trailer);
    self.reorder(s, e, &trailer, &mut tape);

    self.tape = tape;
    self.body_start = body_start;
    self.trailer_start = trailer_start;
    self.clean = false;
  }

  /// Append the children of `[start, end)` to `out`, sorted by `order`, with
  /// each group's instances normalised by the group's own order.
  fn reorder(&self, start: u32, end: u32, order: &Order, out: &mut Vec<Entry>) {
    // Each child is a run of entries that moves as one: a field, a Length
    // and its data field, or a whole group.
    let mut children = Vec::new();
    let mut i = start;
    while i < end {
      let e = &self.tape[i as usize];
      let len = match e.kind {
        Kind::Gap => {
          i += e.span();
          continue;
        }
        Kind::DataLen => 2,
        _ => e.span(),
      };
      children.push((i, len));
      i += len;
    }
    children.sort_by_key(|&(i, _)| {
      let tag = self.tape[i as usize].tag;
      (order.get(&tag).copied().unwrap_or(usize::MAX), tag)
    });

    for (i, len) in children {
      let e = self.tape[i as usize];
      if e.kind != Kind::Group {
        out.extend_from_slice(&self.tape[i as usize..(i + len) as usize]);
        continue;
      }
      out.push(e);
      let members: Order = self
        .group_def_at(i)
        .map(|g| {
          self
            .dict
            .group(g)
            .members
            .iter()
            .map(|(tag, pos)| (*tag, *pos as usize))
            .collect()
        })
        .unwrap_or_default();
      let mut q = i + 1;
      while q < i + len {
        let instance = self.tape[q as usize];
        out.push(instance);
        self.reorder(q + 1, q + instance.span(), &members, out);
        q += instance.span();
      }
    }
  }
}

fn component_order(fix: &FixVersion, name: &str) -> Order {
  fix
    .get_component_by_name(name)
    .map(|c| block_order(fix, &c.elements))
    .unwrap_or_default()
}

/// Positions of a block's fields in definition order, flattening components; a
/// group contributes its NumInGroup tag.
fn block_order(fix: &FixVersion, elements: &[MessageElement]) -> Order {
  fn walk(fix: &FixVersion, elements: &[MessageElement], out: &mut Order) {
    for element in elements {
      match element {
        MessageElement::Field(f) => {
          let n = out.len();
          out.entry(f.field_id).or_insert(n);
        }
        MessageElement::Component(c) => {
          if let Some(component) = fix.get_component(c.component_id) {
            walk(fix, &component.elements, out);
          }
        }
        MessageElement::Group(g) => {
          if let Some(group) = fix.get_group(g.group_id) {
            let n = out.len();
            out.entry(group.num_in_group_tag).or_insert(n);
          }
        }
      }
    }
  }
  let mut out = Order::new();
  walk(fix, elements, &mut out);
  out
}
