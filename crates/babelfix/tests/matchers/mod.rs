//! Test-only [googletest] matchers for babelfix messages and session events.
//!
//! * [`message`] — flat matchers over a whole [`Message`]: a tag anywhere in it
//!   (the first occurrence, at any depth), as text or bytes.
//! * [`block`] — structural matchers: a field in the header or body, an
//!   instance of a repeating group.
//! * [`value`] — matchers over a field's text, decoding it as needed
//!   (`value::float(eq(100.0))`).
//!
//! Compose them inside `matches_pattern!` on a `SessionEvent`, e.g.
//!
//! ```ignore
//! matches_pattern!(&SessionEvent::RawMessageReceived(
//!     ref all!(message::tag(35, eq("A")), message::tag(108, eq("30"))),
//!     ref anything(),
//! ))
//! ```
#![allow(dead_code)]

use babelfix as fix;
use fix::message::{Block, Message};

/// The first occurrence of `tag` anywhere in the message, as its value bytes.
fn find(msg: &Message, tag: u32) -> Option<&[u8]> {
  msg
    .walk()
    .filter(|c| c.tag() == tag)
    .find_map(|c| c.value())
}

/// `tag` among a block's own fields.
fn find_in<'a>(block: &Block<'a>, tag: u32) -> Option<&'a [u8]> {
  block.raw(tag)
}

fn text(bytes: &[u8]) -> Option<&str> {
  std::str::from_utf8(bytes).ok()
}

/// Matchers over a whole message.
pub mod message {
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  use super::{Message, find, text};

  #[derive(MatcherBase)]
  pub struct HasTagMatcher {
    tag: u32,
  }

  pub fn has_tag(tag: u32) -> HasTagMatcher {
    HasTagMatcher { tag }
  }

  impl Matcher<&Message> for HasTagMatcher {
    fn matches(&self, actual: &Message) -> MatcherResult {
      find(actual, self.tag).is_some().into()
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      match sense {
        MatcherResult::Match => {
          Description::new().text(format!("has tag {}", self.tag))
        }
        MatcherResult::NoMatch => {
          Description::new().text(format!("does not have tag {}", self.tag))
        }
      }
    }

    fn explain_match(&self, actual: &Message) -> Description {
      match find(actual, self.tag) {
        Some(v) => Description::new().text(format!(
          "whose tag {} is {:?}",
          self.tag,
          String::from_utf8_lossy(v)
        )),
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct TagMatcher<M> {
    tag: u32,
    inner: M,
  }

  /// Matches when tag `tag` is present with a text value `inner` accepts.
  pub fn tag<M>(tag: u32, inner: M) -> TagMatcher<M> {
    TagMatcher { tag, inner }
  }

  impl<M> Matcher<&Message> for TagMatcher<M>
  where
    M: for<'a> Matcher<&'a str>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      match find(actual, self.tag).and_then(text) {
        Some(s) => self.inner.matches(s),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has tag {} whose value", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      match find(actual, self.tag) {
        Some(v) => match text(v) {
          Some(s) => Description::new()
            .text(format!("whose tag {} is {s:?},", self.tag))
            .nested(self.inner.explain_match(s)),
          None => Description::new()
            .text(format!("whose tag {} is not text: {v:?}", self.tag)),
        },
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct DataMatcher<M> {
    tag: u32,
    inner: M,
  }

  /// Matches when tag `tag` is present and its bytes match `inner`.
  pub fn data<M>(tag: u32, inner: M) -> DataMatcher<M> {
    DataMatcher { tag, inner }
  }

  impl<M> Matcher<&Message> for DataMatcher<M>
  where
    M: for<'a> Matcher<&'a [u8]>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      match find(actual, self.tag) {
        Some(v) => self.inner.matches(v),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has tag {} whose bytes", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      match find(actual, self.tag) {
        Some(v) => Description::new()
          .text(format!("whose tag {} is {v:?},", self.tag))
          .nested(self.inner.explain_match(v)),
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }
}

/// Matchers over one field's text.
pub mod value {
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  #[derive(MatcherBase)]
  pub struct StringMatcher<M> {
    inner: M,
  }

  /// The text itself.
  pub fn string<M>(inner: M) -> StringMatcher<M> {
    StringMatcher { inner }
  }

  impl<M> Matcher<&str> for StringMatcher<M>
  where
    M: for<'a> Matcher<&'a str>,
  {
    fn matches(&self, actual: &str) -> MatcherResult {
      self.inner.matches(actual)
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      self.inner.describe(sense)
    }

    fn explain_match(&self, actual: &str) -> Description {
      self.inner.explain_match(actual)
    }
  }

  macro_rules! parsed_matcher {
    ($name:ident, $ctor:ident, $ty:ty, $label:literal) => {
      #[derive(MatcherBase)]
      pub struct $name<M> {
        inner: M,
      }

      #[doc = concat!("The text, parsed as ", $label, ".")]
      pub fn $ctor<M>(inner: M) -> $name<M> {
        $name { inner }
      }

      impl<M> Matcher<&str> for $name<M>
      where
        M: Matcher<$ty>,
      {
        fn matches(&self, actual: &str) -> MatcherResult {
          match actual.parse::<$ty>() {
            Ok(v) => self.inner.matches(v),
            Err(_) => MatcherResult::NoMatch,
          }
        }

        fn describe(&self, sense: MatcherResult) -> Description {
          Description::new()
            .text(concat!("is ", $label, " which"))
            .nested(self.inner.describe(sense))
        }

        fn explain_match(&self, actual: &str) -> Description {
          match actual.parse::<$ty>() {
            Ok(v) => Description::new()
              .text(format!(concat!("which is ", $label, " {:?},"), v))
              .nested(self.inner.explain_match(v)),
            Err(_) => Description::new()
              .text(format!(concat!("which is {:?}, not ", $label), actual)),
          }
        }
      }
    };
  }

  parsed_matcher!(IntMatcher, int, i64, "an integer");
  parsed_matcher!(FloatMatcher, float, f64, "a number");
}

/// Structural matchers: fields of a block, and group instances.
pub mod block {
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  use super::{Block, Message, find_in, text};

  #[derive(MatcherBase)]
  pub struct HasTagMatcher {
    tag: u32,
  }

  /// Matches a block that holds the field or group.
  pub fn has_tag(tag: u32) -> HasTagMatcher {
    HasTagMatcher { tag }
  }

  impl<'b> Matcher<&Block<'b>> for HasTagMatcher {
    fn matches(&self, actual: &Block<'b>) -> MatcherResult {
      actual.has(self.tag).into()
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      match sense {
        MatcherResult::Match => {
          Description::new().text(format!("has tag {}", self.tag))
        }
        MatcherResult::NoMatch => {
          Description::new().text(format!("does not have tag {}", self.tag))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct FieldMatcher<M> {
    tag: u32,
    inner: M,
  }

  /// Matches a block holding `tag` with a text value `inner` accepts.
  pub fn tag<M>(tag: u32, inner: M) -> FieldMatcher<M> {
    FieldMatcher { tag, inner }
  }

  impl<'b, M> Matcher<&Block<'b>> for FieldMatcher<M>
  where
    M: for<'a> Matcher<&'a str>,
  {
    fn matches(&self, actual: &Block<'b>) -> MatcherResult {
      match find_in(actual, self.tag).and_then(text) {
        Some(s) => self.inner.matches(s),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has tag {} whose value", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Block<'b>) -> Description {
      match find_in(actual, self.tag).and_then(text) {
        Some(s) => Description::new()
          .text(format!("whose tag {} is {s:?},", self.tag))
          .nested(self.inner.explain_match(s)),
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct GroupMatcher<M> {
    num_in_group: u32,
    index: usize,
    inner: M,
  }

  /// Matches a block whose group `num_in_group` has an instance `index` that
  /// `inner` (a block matcher) accepts.
  pub fn group<M>(
    num_in_group: u32,
    index: usize,
    inner: M,
  ) -> GroupMatcher<M> {
    GroupMatcher {
      num_in_group,
      index,
      inner,
    }
  }

  impl<'b, M> Matcher<&Block<'b>> for GroupMatcher<M>
  where
    M: for<'a, 'c> Matcher<&'a Block<'c>>,
  {
    fn matches(&self, actual: &Block<'b>) -> MatcherResult {
      match actual.group(self.num_in_group).get(self.index) {
        Some(instance) => self.inner.matches(&instance),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!(
          "has a group {} whose instance [{}]",
          self.num_in_group, self.index
        ))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Block<'b>) -> Description {
      let group = actual.group(self.num_in_group);
      match group.get(self.index) {
        Some(instance) => Description::new()
          .text(format!(
            "whose group {} instance [{}]",
            self.num_in_group, self.index
          ))
          .nested(self.inner.explain_match(&instance)),
        None => Description::new().text(format!(
          "whose group {} has {} instance(s)",
          self.num_in_group,
          group.len()
        )),
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct HeaderMatcher<M> {
    inner: M,
  }

  /// Applies `inner` (a block matcher) to a message's header.
  pub fn header<M>(inner: M) -> HeaderMatcher<M> {
    HeaderMatcher { inner }
  }

  impl<M> Matcher<&Message> for HeaderMatcher<M>
  where
    M: for<'a, 'c> Matcher<&'a Block<'c>>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      self.inner.matches(&actual.header())
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("has a header which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      Description::new()
        .text("whose header")
        .nested(self.inner.explain_match(&actual.header()))
    }
  }

  #[derive(MatcherBase)]
  pub struct BodyMatcher<M> {
    inner: M,
  }

  /// Applies `inner` (a block matcher) to a message's body.
  pub fn body<M>(inner: M) -> BodyMatcher<M> {
    BodyMatcher { inner }
  }

  impl<M> Matcher<&Message> for BodyMatcher<M>
  where
    M: for<'a, 'c> Matcher<&'a Block<'c>>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      self.inner.matches(&actual.body())
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("has a body which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      Description::new()
        .text("whose body")
        .nested(self.inner.explain_match(&actual.body()))
    }
  }
}

#[test]
fn matcher_smoke() {
  use fix::schema::fields::{HeartBtInt, SenderCompID};
  use fix::schema::tags;
  use googletest::prelude::*;

  let fix44 = crate::session::DICTS.get("FIX.4.4").unwrap().clone();

  let mut built = Message::new(&fix44, "A");
  built.header_mut().set(SenderCompID, "CLIENT");
  built.body_mut().set(HeartBtInt, 30u64);

  verify_that!(&built, block::header(not(block::has_tag(tags::HeartBtInt))))
    .unwrap();

  verify_that!(
    &built,
    all!(
      block::header(block::tag(
        tags::SenderCompID,
        value::string(eq("CLIENT"))
      )),
      block::body(block::tag(tags::HeartBtInt, value::int(ge(1)))),
    )
  )
  .unwrap();
  verify_that!(
    &built,
    block::body(block::tag(tags::HeartBtInt, value::float(eq(30.0))))
  )
  .unwrap();
  // Not a number.
  verify_that!(
    &built,
    block::header(block::tag(tags::SenderCompID, value::int(anything())))
  )
  .unwrap_err();

  verify_that!(
    &built,
    all!(
      message::tag(35, eq("A")),
      message::tag(tags::HeartBtInt, eq("30")),
      message::tag(tags::SenderCompID, eq("CLIENT")),
    )
  )
  .unwrap();

  // Negative (wrong value) and absence both fail.
  verify_that!(&built, message::tag(35, eq("D"))).unwrap_err();
  verify_that!(&built, message::tag(9999, anything())).unwrap_err();

  // Composition with matches_pattern! on a SessionEvent (the intended usage).
  let ev = fix::session::SessionEvent::RawMessageReceived(
    built.clone(),
    chrono::Utc::now(),
  );
  verify_that!(
    &ev,
    // NB: `ref` is required because the fields are not Copy, so
    // matches_pattern! must match them by reference.
    matches_pattern!(&fix::session::SessionEvent::RawMessageReceived(
      ref message::tag(35, eq("A")),
      ref anything(),
    ))
  )
  .unwrap();

  let ev = fix::session::SessionEvent::MessageReceived {
    seq_num: 1,
    msg: built,
  };
  verify_that!(
    &ev,
    matches_pattern!(&fix::session::SessionEvent::MessageReceived {
        seq_num: eq(1),
        msg: ref all!(
          block::header(not(block::has_tag(tags::TargetCompID))),
          block::header(block::tag(
            tags::SenderCompID,
            value::string(eq("CLIENT"))
          )),
          block::body(block::tag(tags::HeartBtInt, value::int(lt(100)))),
        ),
    })
  )
  .unwrap();
}
