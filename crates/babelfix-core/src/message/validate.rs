//! Opt-in checks of the rules parsing does not enforce.

use super::error::{ParseError, reject_reason::*};
use super::tape::{Kind, Message, Region};

impl Message {
  /// Hold the message to the TagValue rules that parsing tolerates:
  ///
  /// * a tag appears at most once in the message, and at most once in each
  ///   group instance (TagValue §4.3.2) — reason 13, TagAppearsMoreThanOnce;
  /// * fields within a group instance are in the group's definition order
  ///   (TagValue §4.3.6.3) — reason 15, RepeatingGroupFieldsOutOfOrder.
  ///
  /// Both are seen broken in practice by counterparties that otherwise work,
  /// and checking them costs a pass over the message, so they are not part of
  /// parsing: call this when you want them. The error carries the reason and
  /// tag for a Reject.
  ///
  /// Header, body and trailer are one block for the duplicate rule: a tag may
  /// not appear in both the header and the body.
  pub fn validate_strict(&self) -> Result<(), ParseError> {
    let mut seen = Vec::new();
    for region in [Region::Header, Region::Body, Region::Trailer] {
      let (start, end) = self.region_range(region);
      self.collect(start, end, &mut seen)?;
    }
    check_unique(&mut seen)
  }

  /// Gather the direct children of `[start, end)` into `seen`, and check each
  /// group's instances as blocks of their own.
  fn collect(
    &self,
    start: u32,
    end: u32,
    seen: &mut Vec<u32>,
  ) -> Result<(), ParseError> {
    let mut i = start;
    while i < end {
      let e = &self.tape[i as usize];
      if e.is_tagged() {
        seen.push(e.tag);
      }
      if e.kind == Kind::Group {
        self.check_group(i)?;
      }
      i += e.span();
    }
    Ok(())
  }

  fn check_group(&self, g: u32) -> Result<(), ParseError> {
    let def = self.group_def_at(g).map(|d| self.dict.group(d));
    let end = self.next_sibling(g);
    let mut q = g + 1;
    while q < end {
      let instance = &self.tape[q as usize];
      let (start, stop) = (q + 1, q + instance.span());

      let mut seen = Vec::new();
      self.collect(start, stop, &mut seen)?;

      if let Some(def) = def {
        let mut last = None;
        for &tag in &seen {
          // Members only: an unknown tag here is the dictionary's gap, not
          // the counterparty's ordering.
          let Some(&pos) = def.members.get(&tag) else {
            continue;
          };
          if last.is_some_and(|l| pos < l) {
            return Err(ParseError::reject(
              REPEATING_GROUP_FIELDS_OUT_OF_ORDER,
              tag,
              format!(
                "tag {tag} is out of order in group {}",
                self.tape[g as usize].tag
              ),
            ));
          }
          last = Some(pos);
        }
      }

      check_unique(&mut seen)?;
      q = stop;
    }
    Ok(())
  }
}

fn check_unique(tags: &mut [u32]) -> Result<(), ParseError> {
  tags.sort_unstable();
  match tags.windows(2).find(|w| w[0] == w[1]) {
    Some(w) => Err(ParseError::reject(
      TAG_APPEARS_MORE_THAN_ONCE,
      w[0],
      format!("tag {} appears more than once", w[0]),
    )),
    None => Ok(()),
  }
}
