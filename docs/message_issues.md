# Serious problems with message representation

babelfix core has 2 message representations:

- FixMessage - which is just the data of the message with the field offsets cached as offsets into it. This is quite fast to parse, but has no understanding of the structure (groups, in particular)
- builder::Message - basically a tree of TypedValue lists, understanding the
  structure, but paying a huge cost for it including allocating Strings and
  parsing prices/quantities into F64 values.

The problem is that the former is too dumb and the latter is slow and dumb in
different ways.

We _do_ need structure and we _do_ need performance. 

We also need to work with decimals in a non-stupid way (no F64 conversion) and
not copy every string.

We _do_ need to keep the raw message bytes for a reader so they can be
serialosed. We _do_ want to keep a consistend API between reader and builder,
but they don't _have_ to be the same type (c.f. capnp Reader<'_> and
Builder<'_>)

We _do_ want random access to fields and we _do_ want to be able to manipulate
groups, remove, insert, etc. though these _can_ incur costs on top of pushing
things on the end of the message.

We do _not_ need to maintain any API compatibilty - this is a new crate only
used by my projects, so we can break everything and start over (it's just a bit
tedious).

One suggestion is to have something like how BSON works:

 - a raw data buffer, which is actually used for readers as canonical, and for
   builders as an arena
 - a list of entries containing field number, offset/length, type, nesting
   depth, and skip count (for groups)
 - lazy type conversions instead of eager ones (i.e. don't force decimals to
   F64), have the user request conversion from string at the point of use. This
   also allows borrowing the string instead of copying it.

Some of the builder API was "kind of" designed with this in mind, but the actual
implementation was lazy and naive. If we had this sort of structure, we can
ditch the "2 representations" version and just have one with a reasonably rich
API. In practice, we pay the cost of the slow version anyway because that's what
the session API ends up providing and requiring from the user code.


An earlier discussion about this is in
$HOME/Downloads/fix-message-design-brief.md and some possible sample code in
$HOME/Downloads/fix_simd.rs - neither of which are truly "settled" - just a
discussion and some ideas. The discussion started around SIMD usage which is why
the agent has written all that as settled, but it's not necessarily. Having used
babelfix a bit now, i think the more general API and usabilty are more important
than the scanning perfomrance for now, and SIMD optimisation can be added after
that API is settled properly and usage code migrated. NOTE ALSO: the agent that
wrote this summary did not read the babelfix code, rather it was independent
discussion so use the *ideas* where useful, but do not take its instructions
literally.

The most relevant usage code we have for babelfix here is in ../fixation, which
is what babelfix was written for. And even it has a lot of kind of annoying code
to work with the message buffers.

But in another project i have started to use it as a proper FIX engine and i'm
not loving it in that context. Concrete example:

```rust

// order is some capnproto message builder
{
    // FIXME: why do ibneed to convert to F64 in order to get a decimix Dec19, that
    // completely defeats the purpose of a decimal type:

    if let Some(price) = msg.tag(fix::schema::FIX_latest::Fields::Price) {
       let mut price_value = order.init_price();
       let fix::message::TypedValue::Float(p) = price else {
              panic!("Price tag is not a float");
       };
       // Sigh
       price_value.get_priced().set_dec19(
        decimix::Dec19::from_f64_lossy(p, decimix::Dec19::SMALLEST_STEP, decimix::Round::HalfAwayFromZero));
    } else {
        order.init_price().set_unpriced();
    }

    let exec_report = fix::message::builder::Message::new(8 /* ...*/ );
    exec_report.set_tag(fix::schema::FIX_latest::Fields::ClOrdID,
        msg.tag(fix::schema::FIX_latest::Fields::ClOrdID).ok()?.as_string());
        // ugh piggy and ugly
}
```

reference decimix: ../rust/decimix
