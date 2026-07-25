//! Test-only [googletest] matchers for babelfix FIX messages and session events.
//!
//! Two families, one per message representation:
//!
//! * [`message`] — matchers over the wire form [`FixMessage`](babelfix::FixMessage).
//!   Values are string-or-bytes (the wire has no numeric/bool type), so
//!   [`message::tag`] takes a `Matcher<&str>` and [`message::data`] a
//!   `Matcher<&[u8]>`.
//! * [`builder`] / [`typedvalue`] — matchers over the typed
//!   [`builder::Message`](babelfix::message::builder::Message). Here values are
//!   [`TypedValue`](babelfix::message::builder::TypedValue)s, so the
//!   [`typedvalue`] matchers assert the *variant* as well as the value — which
//!   is the point of testing the typed form.
//!
//! Compose them inside `matches_pattern!` on a `SessionEvent`, e.g.
//!
//! ```ignore
//! matches_pattern!(&SessionEvent::RawMessageReceived(
//!     all!(message::tag(35, eq("A")), message::tag(108, eq("30"))),
//!     anything(),
//! ))
//! ```
#![allow(dead_code)]

use babelfix as fix;
use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

/// Matchers over the wire form, [`FixMessage`](babelfix::FixMessage).
pub mod message {
  use super::fix;
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  fn find(msg: &fix::FixMessage, tag: u32) -> Option<&fix::Value> {
    msg.tags.iter().find(|(t, _)| *t == tag).map(|(_, v)| v)
  }

  #[derive(MatcherBase)]
  pub struct HasTagMatcher {
    tag: u32,
  }

  pub fn has_tag(tag: u32) -> HasTagMatcher {
    HasTagMatcher { tag }
  }

  impl Matcher<&fix::FixMessage> for HasTagMatcher {
    fn matches(&self, actual: &fix::FixMessage) -> MatcherResult {
      if find(actual, self.tag).is_some() {
        MatcherResult::Match
      } else {
        MatcherResult::NoMatch
      }
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

    fn explain_match(&self, actual: &fix::FixMessage) -> Description {
      match find(actual, self.tag) {
        Some(v) => {
          Description::new().text(format!("whose tag {} is {v:?}", self.tag))
        }
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

  /// Matches when tag `tag` is present as a (non-data) string value that the
  /// `inner` matcher accepts.
  pub fn tag<M>(tag: u32, inner: M) -> TagMatcher<M> {
    TagMatcher { tag, inner }
  }

  impl<M> Matcher<&fix::FixMessage> for TagMatcher<M>
  where
    M: for<'a> Matcher<&'a str>,
  {
    fn matches(&self, actual: &fix::FixMessage) -> MatcherResult {
      match find(actual, self.tag) {
        Some(v) if !v.is_data() => self.inner.matches(v.as_str(&actual.data)),
        _ => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has string tag {} whose value", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &fix::FixMessage) -> Description {
      match find(actual, self.tag) {
        Some(v) if !v.is_data() => {
          let s = v.as_str(&actual.data);
          Description::new()
            .text(format!("whose tag {} is {s:?},", self.tag))
            .nested(self.inner.explain_match(s))
        }
        Some(_) => Description::new().text(format!(
          "whose tag {} is binary data, not a string",
          self.tag
        )),
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

  /// Matches when tag `tag` is present and its raw bytes match `inner`.
  pub fn data<M>(tag: u32, inner: M) -> DataMatcher<M> {
    DataMatcher { tag, inner }
  }

  impl<M> Matcher<&fix::FixMessage> for DataMatcher<M>
  where
    M: for<'a> Matcher<&'a [u8]>,
  {
    fn matches(&self, actual: &fix::FixMessage) -> MatcherResult {
      match find(actual, self.tag) {
        Some(v) => self.inner.matches(v.as_bytes(&actual.data)),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has data tag {} whose bytes", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &fix::FixMessage) -> Description {
      match find(actual, self.tag) {
        Some(v) => {
          let b = v.as_bytes(&actual.data);
          Description::new()
            .text(format!("whose tag {} is {b:?},", self.tag))
            .nested(self.inner.explain_match(b))
        }
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }
}

/// Matchers over a single [`TypedValue`](babelfix::message::builder::TypedValue).
///
/// Each asserts the value's *variant* and applies an inner matcher to the
/// contained value, so a numeric field stored as `String` will (correctly) not
/// match [`int`].
pub mod typedvalue {
  use babelfix::message::builder::TypedValue;
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  macro_rules! scalar_matcher {
    ($name:ident, $ctor:ident, $variant:ident, $ty:ty, $bound:tt, $label:literal) => {
      #[derive(MatcherBase)]
      pub struct $name<M> {
        inner: M,
      }

      #[doc = concat!("Matches a `TypedValue::", stringify!($variant), "` whose value the `inner` matcher accepts.")]
      pub fn $ctor<M>(inner: M) -> $name<M> {
        $name { inner }
      }

      impl<M> Matcher<&TypedValue> for $name<M>
      where
        M: $bound,
      {
        fn matches(&self, actual: &TypedValue) -> MatcherResult {
          match actual {
            TypedValue::$variant(v) => self.inner.matches(*v),
            _ => MatcherResult::NoMatch,
          }
        }

        fn describe(&self, sense: MatcherResult) -> Description {
          Description::new()
            .text(concat!("is ", $label, " which"))
            .nested(self.inner.describe(sense))
        }

        fn explain_match(&self, actual: &TypedValue) -> Description {
          match actual {
            TypedValue::$variant(v) => Description::new()
              .text(format!(concat!("which is ", $label, " {:?},"), v))
              .nested(self.inner.explain_match(*v)),
            other => Description::new()
              .text(format!(concat!("which is {:?}, not ", $label), other)),
          }
        }
      }
    };
  }

  // Copy scalars: the contained value is dereferenced and matched by value.
  scalar_matcher!(IntMatcher, int, Int, i64, (Matcher<i64>), "an integer");
  scalar_matcher!(FloatMatcher, float, Float, f64, (Matcher<f64>), "a float");
  scalar_matcher!(
    BoolMatcher,
    bool,
    Boolean,
    bool,
    (Matcher<bool>),
    "a boolean"
  );
  scalar_matcher!(CharMatcher, char, Char, char, (Matcher<char>), "a char");

  // Reference values (str / bytes) need a by-reference projection.
  #[derive(MatcherBase)]
  pub struct StringMatcher<M> {
    inner: M,
  }

  /// Matches a `TypedValue::String` whose value the `inner` matcher accepts.
  pub fn string<M>(inner: M) -> StringMatcher<M> {
    StringMatcher { inner }
  }

  impl<M> Matcher<&TypedValue> for StringMatcher<M>
  where
    M: for<'a> Matcher<&'a str>,
  {
    fn matches(&self, actual: &TypedValue) -> MatcherResult {
      match actual {
        TypedValue::String(s) => self.inner.matches(s.as_str()),
        _ => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("is a string which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &TypedValue) -> Description {
      match actual {
        TypedValue::String(s) => Description::new()
          .text(format!("which is the string {s:?},"))
          .nested(self.inner.explain_match(s.as_str())),
        other => {
          Description::new().text(format!("which is {other:?}, not a string"))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct DataMatcher<M> {
    inner: M,
  }

  /// Matches a `TypedValue::Data` whose bytes the `inner` matcher accepts.
  pub fn data<M>(inner: M) -> DataMatcher<M> {
    DataMatcher { inner }
  }

  impl<M> Matcher<&TypedValue> for DataMatcher<M>
  where
    M: for<'a> Matcher<&'a [u8]>,
  {
    fn matches(&self, actual: &TypedValue) -> MatcherResult {
      match actual {
        TypedValue::Data(d) => self.inner.matches(d.as_slice()),
        _ => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("is binary data which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &TypedValue) -> Description {
      match actual {
        TypedValue::Data(d) => Description::new()
          .text(format!("which is the data {d:?},"))
          .nested(self.inner.explain_match(d.as_slice())),
        other => {
          Description::new().text(format!("which is {other:?}, not data"))
        }
      }
    }
  }
}

/// Matchers over the typed [`builder::Message`](babelfix::message::builder::Message).
///
/// [`tag`] and [`group`] operate on a [`Block`](babelfix::message::builder::Block);
/// [`header`] and [`body`] project the respective block out of a `Message` so
/// you can address header vs. body fields.
pub mod builder {
  use babelfix::message::builder::{Block, Message, TypedValue};
  use googletest::description::Description;
  use googletest::matcher::{Matcher, MatcherBase, MatcherResult};

  #[derive(MatcherBase)]
  pub struct HasTagMatcher {
    tag: u32,
  }

  pub fn has_tag(tag: u32) -> HasTagMatcher {
    HasTagMatcher { tag }
  }

  impl Matcher<&Block> for HasTagMatcher {
    fn matches(&self, actual: &Block) -> MatcherResult {
      if actual.has_tag(self.tag) {
        MatcherResult::Match
      } else {
        MatcherResult::NoMatch
      }
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

    fn explain_match(&self, actual: &Block) -> Description {
      match actual.tag(self.tag) {
        Some(v) => {
          Description::new().text(format!("whose tag {} is {v:?}", self.tag))
        }
        None => {
          Description::new().text(format!("which has no tag {}", self.tag))
        }
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct BlockFieldMatcher<M> {
    tag: u32,
    inner: M,
  }

  /// Matches when the block contains tag `tag` whose [`TypedValue`] the `inner`
  /// matcher accepts. Operates on a [`Block`] (use [`header`]/[`body`] to reach
  /// a `Message`'s blocks).
  pub fn tag<M>(tag: u32, inner: M) -> BlockFieldMatcher<M> {
    BlockFieldMatcher { tag, inner }
  }

  impl<M> Matcher<&Block> for BlockFieldMatcher<M>
  where
    M: for<'a> Matcher<&'a TypedValue>,
  {
    fn matches(&self, actual: &Block) -> MatcherResult {
      match actual.tag(self.tag) {
        Some(v) => self.inner.matches(v),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!("has tag {} whose value", self.tag))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Block) -> Description {
      match actual.tag(self.tag) {
        Some(v) => Description::new()
          .text(format!("whose tag {} is {v:?},", self.tag))
          .nested(self.inner.explain_match(v)),
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

  /// Matches the `index`-th entry of the repeating group identified by its
  /// `num_in_group` tag; the entry is itself a [`Block`], matched by `inner`.
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

  impl<M> Matcher<&Block> for GroupMatcher<M>
  where
    M: for<'a> Matcher<&'a Block>,
  {
    fn matches(&self, actual: &Block) -> MatcherResult {
      match actual
        .group(self.num_in_group)
        .and_then(|g| g.get(self.index))
      {
        Some(entry) => self.inner.matches(entry),
        None => MatcherResult::NoMatch,
      }
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text(format!(
          "has a group {} whose entry [{}]",
          self.num_in_group, self.index
        ))
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Block) -> Description {
      match actual.group(self.num_in_group) {
        Some(g) => match g.get(self.index) {
          Some(entry) => Description::new()
            .text(format!(
              "whose group {} entry [{}]",
              self.num_in_group, self.index
            ))
            .nested(self.inner.explain_match(entry)),
          None => Description::new().text(format!(
            "whose group {} has only {} entries",
            self.num_in_group,
            g.len()
          )),
        },
        None => Description::new()
          .text(format!("which has no group {}", self.num_in_group)),
      }
    }
  }

  #[derive(MatcherBase)]
  pub struct HeaderMatcher<M> {
    inner: M,
  }

  /// Applies `inner` (a block matcher) to a message's header block.
  pub fn header<M>(inner: M) -> HeaderMatcher<M> {
    HeaderMatcher { inner }
  }

  impl<M> Matcher<&Message> for HeaderMatcher<M>
  where
    M: for<'a> Matcher<&'a Block>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      self.inner.matches(&actual.header)
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("has a header which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      Description::new()
        .text("whose header")
        .nested(self.inner.explain_match(&actual.header))
    }
  }

  #[derive(MatcherBase)]
  pub struct BodyMatcher<M> {
    inner: M,
  }

  /// Applies `inner` (a block matcher) to a message's body block.
  pub fn body<M>(inner: M) -> BodyMatcher<M> {
    BodyMatcher { inner }
  }

  impl<M> Matcher<&Message> for BodyMatcher<M>
  where
    M: for<'a> Matcher<&'a Block>,
  {
    fn matches(&self, actual: &Message) -> MatcherResult {
      self.inner.matches(&actual.body)
    }

    fn describe(&self, sense: MatcherResult) -> Description {
      Description::new()
        .text("has a body which")
        .nested(self.inner.describe(sense))
    }

    fn explain_match(&self, actual: &Message) -> Description {
      Description::new()
        .text("whose body")
        .nested(self.inner.explain_match(&actual.body))
    }
  }
}

#[test]
fn matcher_smoke() {
  use fix::schema::FIX_Latest::Fields;
  use googletest::prelude::*;

  let fix44 = crate::session::FIX_REPO.get_version("FIX.4.4").unwrap();

  // Construct a typed builder message (HeartBtInt as Int, comp id as String).
  let built = fix::message::builder::Message::new(fix44.clone(), "A")
    .unwrap()
    .with_header_field(Fields::SenderCompID, "CLIENT")
    .with_body_field(Fields::HeartBtInt, 30i64);

  verify_that!(
    &built,
    builder::header(not(builder::has_tag(Fields::HeartBtInt)))
  )
  .unwrap();

  // Typed assertions: variant + value, header vs body.
  verify_that!(
    &built,
    all!(
      builder::header(builder::tag(
        Fields::SenderCompID,
        typedvalue::string(eq("CLIENT"))
      )),
      builder::body(builder::tag(Fields::HeartBtInt, typedvalue::int(ge(1)))),
    )
  )
  .unwrap();

  // Type-strictness: HeartBtInt is Int, so a string matcher must NOT match.
  verify_that!(
    &built,
    builder::body(builder::tag(
      Fields::HeartBtInt,
      typedvalue::string(anything())
    ))
  )
  .unwrap_err();

  // Wire form: everything is a string.
  let wire = built.as_message().unwrap();
  verify_that!(
    &wire,
    all!(
      message::tag(35, eq("A")),
      message::tag(Fields::HeartBtInt, eq("30")),
      message::tag(Fields::SenderCompID, eq("CLIENT")),
    )
  )
  .unwrap();

  // Negative (wrong value) and absence both fail.
  verify_that!(&wire, message::tag(35, eq("D"))).unwrap_err();
  verify_that!(&wire, message::tag(9999, anything())).unwrap_err();

  // Composition with matches_pattern! on a SessionEvent (the intended usage).
  let ev = fix::session::SessionEvent::RawMessageReceived(
    wire.clone(),
    fix::session::Session::default(),
  );
  verify_that!(
    &ev,
    // NB: `ref` is required because the fields (FixMessage, Session) are not
    // Copy, so matches_pattern! must match them by reference.
    matches_pattern!(&fix::session::SessionEvent::RawMessageReceived(
      ref message::tag(35, eq("A")),
      ref anything(),
    ))
  )
  .unwrap();

  let ev = fix::session::SessionEvent::MessageReceived(built);
  verify_that!(
    &ev,
    matches_pattern!(&fix::session::SessionEvent::MessageReceived(
        ref all!(
          builder::header(not(builder::has_tag(Fields::TargetCompID))),
          builder::header(builder::tag(
            Fields::SenderCompID,
            typedvalue::string(eq("CLIENT"))
          )),
          builder::body(builder::tag(
            Fields::HeartBtInt,
            typedvalue::int(lt(100))
          )),
        )
    ))
  )
  .unwrap();
}
