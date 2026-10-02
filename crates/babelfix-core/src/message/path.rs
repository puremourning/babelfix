//! Stable addresses of fields within a message.

use std::fmt;
use std::str::FromStr;

use super::tape::Region;

/// Which field (or group instance) you mean, as an owned address that
/// survives edits elsewhere in the message.
///
/// Tags are unique within a block (TagValue §4.3.2), so any entry is addressed
/// by its region, the (NumInGroup tag, instance index) steps down to its block,
/// and its own tag. Its text form is `body/453[1]/452`: the region, each
/// instance step, then the tag; leave the tag off to address the instance
/// itself (`body/453[1]`).
///
/// Resolve one with [`Message::cursor`](super::Message::cursor) or
/// [`Message::cursor_mut`](super::Message::cursor_mut); get one from
/// [`Cursor::path`](super::Cursor::path).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
  region: Region,
  instances: Vec<(u32, u32)>,
  tag: Option<u32>,
}

impl FieldPath {
  pub fn new(
    region: Region,
    instances: Vec<(u32, u32)>,
    tag: Option<u32>,
  ) -> Self {
    Self {
      region,
      instances,
      tag,
    }
  }

  /// A region of the message.
  pub fn region_root(region: Region) -> Self {
    Self::new(region, Vec::new(), None)
  }

  pub fn region(&self) -> Region {
    self.region
  }

  /// The (NumInGroup tag, instance index) steps, outermost first.
  pub fn instances(&self) -> &[(u32, u32)] {
    &self.instances
  }

  /// The addressed field's tag; `None` when the path addresses an instance or
  /// a region.
  pub fn tag(&self) -> Option<u32> {
    self.tag
  }

  /// The field `tag` in the block this path addresses.
  pub fn child(&self, tag: u32) -> FieldPath {
    let mut p = self.block();
    p.tag = Some(tag);
    p
  }

  /// Instance `index` of group `group` in the block this path addresses.
  pub fn instance(&self, group: u32, index: u32) -> FieldPath {
    let mut p = self.block();
    p.instances.push((group, index));
    p
  }

  /// One level up: a field's instance (or region), an instance's enclosing
  /// instance (or region). `None` for a region.
  pub fn parent(&self) -> Option<FieldPath> {
    if self.tag.is_some() {
      return Some(self.block());
    }
    let mut p = self.clone();
    p.instances.pop()?;
    Some(p)
  }

  /// The path of the block this path is in, or is.
  fn block(&self) -> FieldPath {
    Self::new(self.region, self.instances.clone(), None)
  }
}

impl fmt::Display for FieldPath {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self.region {
      Region::Header => "header",
      Region::Body => "body",
      Region::Trailer => "trailer",
    })?;
    for (group, index) in &self.instances {
      write!(f, "/{group}[{index}]")?;
    }
    if let Some(tag) = self.tag {
      write!(f, "/{tag}")?;
    }
    Ok(())
  }
}

impl fmt::Debug for FieldPath {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "FieldPath({self})")
  }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsePathError(String);

impl fmt::Display for ParsePathError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "invalid field path: {}", self.0)
  }
}

impl std::error::Error for ParsePathError {}

impl FromStr for FieldPath {
  type Err = ParsePathError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let err = || ParsePathError(s.to_owned());
    let mut parts = s.split('/');
    let region = match parts.next() {
      Some("header") => Region::Header,
      Some("body") => Region::Body,
      Some("trailer") => Region::Trailer,
      _ => return Err(err()),
    };
    let mut path = FieldPath::region_root(region);
    let parts: Vec<_> = parts.collect();
    for (i, part) in parts.iter().enumerate() {
      match part.strip_suffix(']').and_then(|p| p.split_once('[')) {
        Some((group, index)) => {
          let group = group.parse().map_err(|_| err())?;
          let index = index.parse().map_err(|_| err())?;
          path.instances.push((group, index));
        }
        None if i == parts.len() - 1 => {
          path.tag = Some(part.parse().map_err(|_| err())?);
        }
        None => return Err(err()),
      }
    }
    Ok(path)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn text_round_trip() {
    for s in [
      "body",
      "body/55",
      "body/453[1]",
      "body/453[1]/452",
      "header/627[0]/628",
      "body/453[0]/802[2]/523",
    ] {
      let p: FieldPath = s.parse().unwrap();
      assert_eq!(p.to_string(), s);
    }
    for bad in ["", "foo", "body/453[x]", "body/55/56", "body/453["] {
      assert!(bad.parse::<FieldPath>().is_err(), "{bad}");
    }
  }

  #[test]
  fn navigation() {
    let p: FieldPath = "body/453[1]/452".parse().unwrap();
    assert_eq!(p.parent().unwrap().to_string(), "body/453[1]");
    assert_eq!(p.parent().unwrap().parent().unwrap().to_string(), "body");
    assert_eq!(p.parent().unwrap().parent().unwrap().parent(), None);
    assert_eq!(p.child(448).to_string(), "body/453[1]/448");
    assert_eq!(p.instance(802, 0).to_string(), "body/453[1]/802[0]");
  }
}
