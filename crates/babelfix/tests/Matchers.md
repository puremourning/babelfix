  ```rust
  // Matches on a whole Message, wherever the tag is
  // e.g.
  // matches_pattern!(&SessionEvent::RawMessageReceived(
  //    ref all!(
  //       message::tag(35, eq("D")),
  //       message::data(1002, eq(b"Hello World!")),
  //   ),
  //   ref anything()
  // ))
  matchers::message::has_tag(<num>)
  matchers::message::tag(<num>, Matcher<&str> )
  matchers::message::data(<num>, Matcher<&[u8]> )
  // Matches on a sequence of tags literally - such as a group applies
  // matchers[0] on each field until it matches, then requires matchers[1..] to
  // mach the remaining fields in sequence.
  // Defferd - until we actually need it
  // matchers::message::sequence(&[dyn Matcher<&Message>])

  //
  // Matches the structure of a Message
  //
  // e.g.
  // matches_pattern!(&SessionEvent::MessageReceived(
  //   ref all!(
  //     block::header(
  //       block::tag(35, value::string(eq("D"))),
  //     ),
  //     block::body(all!(
  //       block::tag(1001, value::int(ge(100))),
  //       block::group(555, 0, all!(
  //           block::tag(600, value::string(eq("6B"))),
  //           block::group(777, 0, block::tag(700, value::string(eq("6C"))))
  //       )),
  //     ))
  //   ),
  // ))
  //
  // note tag() and group() both operate on Block.
  // header() and body() unpack a Message into its header and body Blocks.
  matchers::block::header(Matcher<&Block> )
  matchers::block::body(Matcher<&Block> )

  matchers::block::has_tag(<num>)
  matchers::block::tag(<num>, Matcher<&str> )
  matchers::block::group(<numingroup tag>, index, Matcher<&Block> )
  matchers::value::string(Matcher<&str>)
  matchers::value::int(Matcher<i64>)
  matchers::value::float(Matcher<f64>)

```
