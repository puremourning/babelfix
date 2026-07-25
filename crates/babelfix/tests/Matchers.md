  ```rust
  // Matches on FixMessage
  // e.g.
  // matches_pattern!(&SessionEvent::RawMessageReceived(
  //    all!(
  //       message::tag(35, eq("D")),
  //       message::tag(1001, eq("12")), // length tag
  //       message::data(1002, eq(b"Hello World!")),
  //       message::sequence(&[1003, 1004], &[tag(1003, eq("A")), tag(1004, eq("B"))])
  //   ),
  //   anything()
  // ))
  matchers::message::tag(<num>, Matcher<&str> )
  matchers::message::data(<num>, Matcher<&[u8]> )
  // Matches on a sequence of tags literally - such as a group applies
  // matchers[0] on each field until it matches, then requires matchers[1..] to
  // mach the remaining fields in sequence (by slicing the underlying vec?).
  // Defferd - until we actually need it
  // matchers::message::sequence(&[dyn Matcher<FixMessage>])

  //
  // Matches a builder::Message
  //
  // e.g.
  // matches_pattern!(&SessionEvent::MessageReceived(
  //   all!(
  //     builder::header(
  //       builder::tag(35, typedvalue::string(eq("D"))),
  //     ),
  //     builder::body(all!(
  //       builder::tag(1001, typedvalue::int(ge(100))),
  //       builder::tag(555, typedvalue::int(eq(2))),
  //       builder::group(555, 0, all!(
  //           builder::tag(600, typedvalue::string(eq("6B")))
  //           builder::group(777, 0, buidler::tag(700, typedvalue::string(eq("6C"))))
  //       )),
  //     ))
  //   ),
  //   anything()
  // )
  //
  // note tag() and group() both operate on Block.
  // Message is just a header Block and body Block, so body and header unpack
  // that (operating on builder::Message)
  matchers::builder::header(Matcher<&Block> )
  matchers::builder::body(Matcher<&Block> )

  matchers::builder::tag(<num>, Matcher<&TypedValue> )
  matchers::builder::group(<numingroup tag>, index, Matcher<&TypedValue> )
  matchers::typedvalue::string(Matcher<&str>)
  matchers::typedvalue::int(Matcher<i64>)
  matchers::typedvalue::float(Matcher<f64>)
  matchers::typedvalue::data(Matcher<&[u8]>)

```
