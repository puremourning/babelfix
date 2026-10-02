# Proposal: one message representation for babelfix (rev 2)

Response to `message_issues.md`. The ideas are from the design brief (tape,
lazy typed decode, GAT field types), adapted to what babelfix actually has
today. SIMD and incremental checksums are deferred on purpose: get the API
right, migrate the callers, then tune.

Spec references are to the FIX TagValue Encoding v1.0 (June 2020, "TV"), the
FIX Session Layer (Nov 2020, "FSL"), and the Orchestra XML shipped in
`babelfix-repo` (datatype definitions).

**Rev 2 changes:**
- Data/Length pairing spelled out (§2.6, §4.2).
- Dates as thin views with chrono conversions (§2.5).
- SendingTime is a typestate, with no clock in core (§6).
- Cursor and FieldPath designed properly (§2.8).
- Lookups take a hint (§2.2).
- Gap entries make the regions real (§3).
- `val_off` is cached (§3).
- Group order: tolerant on parse, conformant on build (§4).
- No session options (§4).
- The Dictionary sits beside Orchestra, not instead of it (§5).
- Codesets move into the codegen phase (§5).
- Single-tag `copy` (§2.4).
- Qty sign settled (§2.5).

## 1. What's wrong today (concretely)

From reading `crates/babelfix-core/src/message.rs` and its callers:

- **Every application message is parsed twice.** The codec produces a
  `FixMessage` (cheap: offsets). Then `SessionState` calls
  `builder::Message::from_message` (`session/state.rs:206`), which allocates a
  `String` per field, a `HashMap` per block, a `Vec<Block>` per group, and
  parses every Price/Qty into `f64`. `Event::MessageReceived` only hands out
  the slow one, so every user pays for it.
- **Every outbound message is serialised twice.** `builder::Message::as_message`
  turns each `TypedValue` back into a `String`, and `FixMessage::write_to`
  then formats those.
- **`f64` is lossy and unwanted.** `12.30` goes in as `f64` and comes out as
  `12.3`.
- **Typing is guessed at runtime from strings.** `parse_typed_value` matches
  `field.field_type.as_str()`, and a bad value quietly becomes
  `TypedValue::String`. The caller can't tell "absent" from "malformed", and
  the tag constants are bare `u32`.
- **Removal is a tombstone.** FIX has no empty values (TV §4.2.5), so
  `remove_tag` marks the slot `Empty` and serialisation skips it. Every
  reader then has to filter dead slots (`Block::iter` does `filter(is_set)`),
  and `len()` counts them.
- **Groups.** `FixMessage` has none. `builder` has them, but each instance is
  a separate heap `Block` with its own `HashMap`.
- **Data fields.** `FixMessage` handles them in the parser. `builder` has
  `RawDataTag`, but fixation `todo!()`s it in three places, because nothing
  makes the Length/data pair transparent.
- **`FixMessage` mixes owned and borrowed values.** `Value` is
  `StringRef | String | DataRef | Data`, and `into_bytes` assumes the message
  is wholly one or the other.

## 2. Usage first

All of this is target API. None of it compiles today.

### 2.1 Your exec-report example

```rust
use babelfix::message::Message;
use babelfix::schema::{fields::*, msg_type, codesets::{ExecType, Side}, tags};
use decimix::Dec19;                       // babelfix's `decimix` feature provides the FromFix impls

fn on_order(order: &Message, mut out: order_capnp::Builder<'_>) -> Result<Message, FieldError> {
    let body = order.body();

    // `get_as` decodes straight from the value bytes into any `T: FromFix<Price>`.
    // Ok(None) = absent; Err = present but malformed (FieldError carries the tag).
    match body.get_as::<Dec19>(Price)? {
        Some(px) => out.init_price().get_priced().set_dec19(px),
        None     => out.init_price().set_unpriced(),
    }

    let mut er = Message::new(order.dict(), msg_type::ExecutionReport);
    let mut b = er.body_mut();
    b.set(ClOrdID, body.req(ClOrdID)?);   // FixStr borrowed from `order`; bytes copied into er's arena
    b.set(OrderQty, body.req(OrderQty)?); // Decimal<'_> passed through as text, never converted
    b.set(Side, body.req(Side)?);         // codeset enum; Side::Other(b"..") round-trips too
    b.set(ExecType, ExecType::New);
    for t in [tags::Symbol, tags::Account, tags::SecurityID, tags::SecurityIDSource] {
        b.copy(&body, t);                 // copy if present: bytes only, no decoding
    }
    Ok(er)
}
```

Nothing above allocates per field. The value type is inferred from the tag,
so there's no turbofish and no matching on variants.

### 2.2 Reading

```rust
let b = msg.body();

let px:  Option<Decimal> = b.get(Price)?;       // Result<Option<T>, FieldError>; borrowed, validated text
let px:  Option<Dec19>   = b.get_as(Price)?;    // decoded to any T: FromFix<Price>
let qty: UDec19          = b.req_as(OrderQty)?; // missing is an error too
let raw: Option<&[u8]>   = b.raw(tags::Text);   // untyped bytes; never fails
let n:   Option<u64>     = b.get(Field::<UInt>::new(5001))?;   // custom tag, typed at the call site
let id:  Option<FixStr>  = msg.find(ClOrdID)?;  // header, then body; top level only

// FieldError → SessionRejectReason (FSL §11.11), no lookup table needed by callers:
// FieldError { tag: 44, kind: Malformed } => 6 (IncorrectDataFormatForValue)
// FieldError { tag: 38, kind: Missing }   => 1 (RequiredTagMissing)
```

**Hinted lookup.** `get_from` / `find_from` take a starting position. The
search runs from the hint to the end of the block and then wraps round to
the start, so a wrong hint is only slower, never wrong:

```rust
let legs = body.group(NoLegs);
let px = body.get_from(Price, legs.end())?;    // "I know Price comes after the legs"
for f in body.fields() {                       // iterating: "the next thing I want is after here"
    if f.tag() == tags::NoPartyIDs {
        let side = body.get_from(Side, f.pos())?;
    }
}
// Pos is a Copy wrapper around a tape index. Cursor, Group, Entry and Block::start/end all produce one.
```

`header()`, `body()` and `trailer()` are `Block<'_>` views. The spec
requires header, then body, then trailer (TV §4.3.3), so a region is a
contiguous range of the tape (see §3), and `body()` is already the hint for
"at or after the body start".

### 2.3 Groups: reading

```rust
for party in msg.body().group(NoPartyIDs) {        // empty iterator if absent
    let id:   FixStr     = party.req(PartyID)?;
    let role: PartyRole  = party.req(PartyRole)?;
    for sub in party.group(NoPartySubIDs) { /* nested: same Block API */ }
}
let parties = msg.body().group(NoPartyIDs);
parties.len();                    // == declared NumInGroup (checked at parse: reason 16)
parties.get(1)?.raw(tags::PartyID);
```

### 2.4 Building and editing (one type, mutable views)

```rust
let mut m = Message::new(&dict, msg_type::NewOrderSingle);
{
    let mut b = m.body_mut();
    b.set(ClOrdID, "abc")
     .set(Side, Side::Buy)
     .set(OrderQty, udec!(100))      // UDec19 (decimix feature)
     .set(Price, px);                // Dec19; no f64 anywhere

    // NumInGroup is maintained for you and is never written by hand.
    let mut parties = b.group_mut(NoPartyIDs);
    parties.push().set(PartyRole, PartyRole::ClientID).set(PartyID, "CLIENT-A");
    // ^ set in any order: inside a group instance, set() places each field at its
    //   definition-order position, so we emit conformant groups (TV §4.3.6.3).
}

// Editing a parsed message (a router rewriting in flight):
let mut b = inbound.body_mut();
b.set(Account, "HOUSE");                 // replace: tape entry repointed at new arena bytes
b.remove(ExDestination);                 // really removed
b.group_mut(NoPartyIDs).insert(0).set(PartyID, "ROUTER").set(PartyRole, PartyRole::ExecutingFirm);
b.group_mut(NoPartyIDs).retain(|p| p.raw(tags::PartyRole) != Some(b"3"));
b.copy(&other.body(), tags::NoPartyIDs); // copying a group tag copies the whole subtree
```

`copy(&src, tag)` is the single-tag primitive. A list version is just a
`for` loop, as in §2.1.

`set(tag, "")` and any other empty value trip a `debug_assert!`. In release
builds they remove the field, because empty means absent (TV §4.3.2).

### 2.5 Values

| Orchestra datatype | `get` yields | `get_as` / `set` conversions |
|---|---|---|
| String, Currency, Exchange, Country, XID, MultipleStringValue, ... | `FixStr<'a>` | `&str` (UTF-8 → Latin-1 on set) |
| char | `u8` | |
| Boolean | `bool` | |
| int | `i64` | smaller ints |
| SeqNum, NumInGroup, DayOfMonth, TagNum | `u64` | smaller uints |
| Length (the Length tag of a data field) | `u64`, read-only (§2.6) | none: you can't `set` it |
| data, XMLData | `&'a [u8]` | (§2.6) |
| float, Price, PriceOffset, Amt, Percentage | `Decimal<'a>` | `Dec19`, finance newtypes |
| Qty | `Decimal<'a>` | `UDec19`, `Dec19`, `finance::Qty`, `finance::DeltaQty` |
| UTCTimestamp | `UtcTimestamp<'a>` | `chrono::DateTime<Utc>` |
| UTCDateOnly, LocalMktDate | `FixDate<'a>` | `chrono::NaiveDate` |
| UTCTimeOnly, LocalMktTime | `FixTime<'a>` | `chrono::NaiveTime` |
| MonthYear, TZTimestamp, TZTimeOnly, Tenor, ... | `FixStr<'a>` | none in v1 |
| `*CodeSet` | generated enum, with `Other(&'a [u8])` (§5) | the enum |

**Escape hatches on every block, for every tag:** `raw(tag)` and
`set_raw(tag, bytes)`. They matter for codesets, because many are open by
design: 158 fields in FIX Latest are `unionDataType="Reserved100Plus"`, 114
are `Reserved1000Plus` and 32 are `Reserved4000Plus`, with bilaterally
agreed values beyond the listed codes. `set_raw` still `debug_assert!`s
non-empty and SOH-free (outside data fields), and still refuses derived tags
(§2.6, §6).

`FromFix<M>` / `ToFix<M>` are the open traits underneath. They are keyed on
the datatype marker `M`, so the impls encode the type rules, and a user can
add impls for their own types (for example `rust_decimal`).

**Strings are Latin-1** (TV §4.1: "By default, the encoding is ISO/IEC
8859-1"; other single-byte sets by agreement; other scripts go in the
`Encoded*` data fields). ASCII is still the
common case and the fast path:

```rust
pub struct FixStr<'a>(&'a [u8]);              // Copy
impl<'a> FixStr<'a> {
    fn as_bytes(&self) -> &'a [u8];
    fn as_ascii(&self) -> Option<&'a str>;    // zero-copy; None if any byte >= 0x80
    fn to_str(&self) -> Cow<'a, str>;         // Borrowed if ASCII, else Latin-1 → UTF-8
}
impl PartialEq<str> for FixStr<'_>           // body.req(OrdStatus)? == "2"
impl Display for FixStr<'_>                  // Latin-1 decode, no allocation
```

**Decimals are not coupled to decimix.** Every FIX float type yields
`Decimal<'a>`: borrowed bytes checked against the `float` lexical space
(Orchestra `float`; TV datatype table): `-?[0-9]*(\.[0-9]*)?`, at least one
digit, no `+`, no exponent, leading zeros and a trailing `.` allowed. Passing
a `Decimal` between messages is a byte copy. The `decimix` and
`decimix-finance` cargo features only add impls (features must be additive,
so they can't change what `get` returns):

```rust
impl FromFix<Qty> for UDec19     // the natural Qty type: negative → Malformed
impl FromFix<Qty> for Dec19      // for the spec's "may be negative unless specified otherwise"
                                 // (LastQtyChanged is the documented case)
impl FromFix<Qty> for finance::DeltaQty
impl FromFix<Amt> for Dec19      // likewise Price, PriceOffset, Float, Percentage
impl ToFix<..> for ..            // the same in reverse
```

decimix's parser (`ascii.rs:91`) also accepts a leading `+`, so `Decimal`
validation (babelfix's own loop) rejects `+` before decimix sees it. After
that, conversion can only fail on range or precision (`from_ascii` is exact,
so more than 19 dp → `Malformed`).

**Dates and times: thin views with few assumptions.** `UtcTimestamp`,
`FixDate` and `FixTime` are newtypes over `FixStr`. `get` never inspects the
content, so it can't fail on a timestamp a counterparty formats oddly. Only
conversion validates:

```rust
let t: UtcTimestamp = b.req(TransactTime)?;     // just bytes
let t: DateTime<Utc> = b.req_as(TransactTime)?; // parse: YYYYMMDD-HH:MM:SS[.s{1,9}]
b.set(TransactTime, (now, TimePrecision::Micros));   // ToFix from (DateTime<Utc>, precision)
b.set(TradeDate, NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
b.set_raw(MaturityMonthYear, b"202612w2");      // MonthYear: raw only (YYYYMM, YYYYMMDD, YYYYMMwN)
```

The precision for timestamps reuses `time::TimePrecision`, which the
session already has.

### 2.6 Data fields

A data field is two wire fields that must stay together: `95=5|96=ab\x01cd|`.
The Length field "must immediately precede" the data field (TV §4.2.5,
datatype table). The data bytes may contain SOH, NUL, `=`, anything at all.
Which Length tag belongs to which data tag comes from Orchestra's `lengthId`
attribute (`<fixr:field name="RawData" id="96" lengthId="95">`). There are
84 such pairs in FIX Latest, including XmlData(213)/XmlDataLen(212),
SecureData(91)/SecureDataLen(90) in the header, and
Signature(89)/SignatureLength(93) in the trailer. babelfix-repo doesn't read
`lengthId` today; it will need to.

The pairing is fully transparent and driven by the dictionary:

```rust
let bytes: &[u8] = b.req(RawData)?;            // exactly the data, whatever it contains
let n: u64 = b.req(RawDataLength)?;            // readable; always equals bytes.len()
b.set(RawData, &payload[..]);                  // writes or updates BOTH entries
b.remove(RawData);                             // removes both (so does remove(RawDataLength))
// b.set(RawDataLength, 5);                    // does not compile: Field<DataLength> has no ToFix
b.set_raw(tags::RawDataLength, b"5");          // Err(FieldError::Derived) + debug_assert
for e in msg.walk() { /* yields DataLen(95, n) then Data(96, bytes): both visible, for display */ }
```

Parsing: when a Length tag that has a `lengthId` partner is seen, the very
next field must be its data tag. The parser takes exactly *n* bytes and then
requires SOH. Anything else makes the message malformed, and the session
rejects it with reason 6 or treats it as garbled. A Length/data pair that
isn't in the dictionary can't be parsed safely (the data could contain SOH),
so custom data fields must be declared in a custom Orchestra file, the same
as custom groups. For building an ad hoc pair, there's
`set_data_raw(len_tag, data_tag, bytes)`.

The bytes' interpretation (for example `Encoded*` text under
MessageEncoding(347)) is left to the application.

### 2.7 Session layer

```rust
Event::MessageReceived(&'a Message)                       // the codec's message, no conversion
Event::RawMessageReceived { msg: &'a Message, wire: &'a [u8], session: &'a Session }
Event::RawMessageSent     { msg: &'a Message, wire: &'a [u8], session: &'a Session }  // exact bytes, for the journal
Command::Send(Message)
Command::Replay(Message)                                  // usually Message::parse(&dict, stored_bytes)

// SessionState::transmit, today ~20 lines of set_tag + as_message():
msg.header_mut()
   .set(MsgSeqNum, seq)
   .set(SenderCompID, &*self.sender)
   .set(TargetCompID, &*self.target);   // all land in the header gap: no shifting (§3)

// Replay: header-only edits on the stored message; the body is never touched or copied.
let mut h = msg.header_mut();
h.copy_value(tags::SendingTime, tags::OrigSendingTime)?;   // within one message: no borrow dance
h.set(PossDupFlag, true);
```

SendingTime is covered in §6.

### 2.8 Cursors and paths (generic code, fixation's editor and diff)

There are two kinds of position, because they answer different questions:

- **`Cursor<'a>` / `CursorMut<'a>`**: *where am I right now?* This is a
  tape index plus a borrow of the message. It's cheap and positional, and
  only valid while the borrow lives. The borrow checker enforces that: you
  can't hold a `Cursor` across an edit.
- **`FieldPath`**: *which field do I mean?* An owned, semantic address. It
  survives edits elsewhere in the message, and it's what a UI keeps between
  frames.

```rust
impl<'a> Cursor<'a> {
    fn entry(&self) -> Entry<'a>;            // tag, kind, value bytes / count
    fn pos(&self) -> Pos;                    // for hinted lookups
    fn depth(&self) -> u8;
    fn next(&self) -> Option<Cursor<'a>>;    // next sibling in the same block (skips over groups)
    fn prev(&self) -> Option<Cursor<'a>>;
    fn down(&self) -> Option<Cursor<'a>>;    // group → first instance; instance → its first field
    fn up(&self) -> Option<Cursor<'a>>;      // field → its instance; instance → its group
    fn block(&self) -> Block<'a>;            // the block containing this position
    fn path(&self) -> FieldPath;
}
impl<'a> CursorMut<'a> {                     // same navigation, plus:
    fn set_raw(&mut self, v: &[u8]) -> Result<(), FieldError>;
    fn remove(self) -> Option<CursorMut<'a>>;           // returns the cursor at the next sibling
    fn insert_after(self, tag: u32, v: &[u8]) -> CursorMut<'a>;
    fn push_instance(self) -> CursorMut<'a>;            // on a group: append an instance, cursor inside it
}

msg.cursor(&path) -> Option<Cursor<'_>>      // resolve; None if it no longer exists
msg.cursor_mut(&path) -> Option<CursorMut<'_>>
msg.walk() -> impl Iterator<Item = Cursor<'_>>   // depth-first, wire order, gaps skipped
```

What is a `FieldPath` concretely? Tags are unique within a block (TV
§4.3.2), so any entry is uniquely addressed by its region, the chain of
(group tag, instance index) pairs leading down to its block, and optionally
its own tag. That's barely more than a `Vec<u32>`, so it gets a small fixed
API, not a `std::path` imitation:

```rust
pub struct FieldPath {
    region: Region,                           // Header | Body | Trailer
    instances: SmallVec<[(u32, u32); 3]>,     // (NumInGroup tag, instance index), outermost first
    tag: Option<u32>,                         // None = the instance (or region) itself
}
impl FieldPath {
    fn parent(&self) -> Option<FieldPath>;
    fn child(&self, tag: u32) -> FieldPath;
    fn instance(&self, group: u32, index: u32) -> FieldPath;
}
impl Display for FieldPath { /* body/453[1]/452 */ }
impl FromStr for FieldPath { .. }             // the same syntax, for tests and logs
```

fixation's editor (`ui.rs:732-904`) today rebuilds the entire message to
change one value. With these types it holds a `FieldPath` as the cursor
position, and an edit becomes `msg.cursor_mut(&path)?.set_raw(..)`. Adding a
repeat is `cursor_mut(&group_path)?.push_instance()`, which seeds the
delimiter field. `diff.rs` pairs up two `walk()`s.

### 2.9 Other tooling

```rust
format!("{msg}")      // 8=FIX.4.4|9=..|  pipe-delimited, for logs
format!("{msg:#?}")   // indented by depth, with field and enum names from the dictionary
let m = fix!(&dict, "35=D|11=abc|453=1|448=X|452=3|");   // test macro: fills in 8/9/10
Message::parse_delimited(&dict, input.as_bytes(), b'|')  // fixation's text input; no Bytes, no tuple
msg.normalize()       // canonical order: a stable sort of entry ranges per block
```

## 3. Representation

One owned type, `Message`, with borrowed views `Block`, `Group`, `Cursor`
and their `Mut` forms. Parsed and built messages are the same type.

```rust
pub struct Message {
    dict:    Arc<Dictionary>,  // §5
    wire:    Bytes,            // received bytes, zero-copy from the codec; empty if built
    arena:   Vec<u8>,          // bytes appended by building and editing
    tape:    Vec<Entry>,       // wire order: header | body | trailer
    regions: [u32; 2],         // tape index where the body and the trailer start
}

#[repr(C)]
struct Entry {                 // 16 bytes
    tag:     u32,
    kind:    u8,               // Field | DataLen | Data | Group | Instance | Gap
    depth:   u8,
    seg:     u8,               // Wire | Arena
    val_off: u8,               // value start relative to `off`
    off:     u32,              // Field/DataLen/Data: start of "tag=" in seg.  Group: instance count
    len:     u32,              // Field/DataLen/Data: value length.  Group/Instance/Gap: skip (entries covered)
}
```

Details:

- **`val_off` is cached in the spare byte.** It equals digits(tag) + 1, and a
  `u32` tag has at most 10 digits, so it's always ≤ 11 and a `u8` never
  overflows. No "0 = more than 255" sentinel is needed.
- **Two segments.** A received message keeps the codec's `Bytes`, so there's
  no copy. Edits append to `arena` and repoint the entry, and the old bytes
  become garbage until `clear()` (compaction is an open question in the
  brief, and probably never needed for messages this short-lived).
- **Gap entries make the regions cheap to edit**, which also makes them real
  rather than just an API idea. A `Gap` entry stands for `len` unused tape
  slots, skipped in O(1) like a group. Inserting at the start of a gap
  writes into that slot and shrinks the gap, with no memmove.
  - `Message::new` reserves a gap of 20 entries at the end of the header,
    and the parser does the same at the header/body boundary.
    - Why 20: Sender/Target/DeliverTo/OnBehalfOf × CompID/SubID/LocationID
      is already 12, before MsgSeqNum, SendingTime, PossDupFlag and
      OrigSendingTime.
    - Cost: 320 bytes of tape, written once (one `Gap` header entry plus
      zeroed slots), skipped in O(1), and reused by pooled messages.
    - If the gap is ever exhausted, the next header insert falls back to a
      splice and re-reserves the gap, so it's a performance cliff, not a
      correctness one.
  - So the session's header stamping (SenderCompID, TargetCompID, MsgSeqNum,
    SendingTime) and replay's edits (PossDupFlag, OrigSendingTime) never
    shift the body.
  - Body appends are pushes onto the end of the tape. The trailer is
    generated at serialise time, so a built message has no trailer entries.
  - Inserting anywhere else is a splice: a memmove of 16-byte entries, never
    of message bytes.
- **Span fixups by scanning.** After a splice at index *i*, walk `0..i` and
  extend every Group/Instance/Gap whose range covers *i*. It's O(tape), the
  same as the memmove, and it means `BlockMut` doesn't carry an ancestor
  stack, so nested `group_mut().push().group_mut()` borrows stay simple.
- **Groups carry their count, not their text.** The serialiser writes
  `tag=count|` from the entry. A Group's NumInGroup value is always derived.
- **Serialise by walking the tape.** It writes 8 and 9, then copies each
  field's `tag=value|` span, coalescing runs that are contiguous in one
  segment, then computes 10. BodyLength and CheckSum are computed during the
  copy (incremental maintenance comes later). An unedited parsed message
  doesn't need serialising at all, because `wire` already holds its bytes.
- **Random access** scans the block's tape range for `tag` at the block's
  depth, skipping groups by their span. That's a short linear scan over
  16-byte entries. Hints (§2.2) cover the cases where you know better, and
  SoA/SIMD scanning or a slot table can come later if profiling says so.

## 4. Parsing rules: correct, standard-compliant, tolerant, and no options

There are no per-session parsing options. Every rule below is either the
spec, or the spec relaxed in one direction, and the relaxation applies to
everyone.

| Rule | Behaviour | Source |
|---|---|---|
| 8, 9, 35 first in that order; 10 last | enforced (garbled / reject) | TV §4.3.3 |
| header → body → trailer | enforced: header tag after a body tag → reason 14 | TV §4.3.3 |
| empty value | reject, reason 4 | TV §4.2.5, §4.3.2 |
| tag at most once per message, or once per group instance | reject, reason 13 | TV §4.3.2 |
| NumInGroup matches the instances found | reject, reason 16 | TV §4.3.6.2 |
| data field immediately preceded by its Length | enforced (§2.6) | TV §4.2.5 |
| field order *within* a group instance | **not validated**: groups are assigned by membership, and instances start at the delimiter. Built messages are emitted in definition order (§2.4). | TV §4.3.6.3, relaxed |
| unknown tag | tolerated: kept in the tape, survives a round trip, available via `raw` | |
| unknown group | rejected in effect: the second instance's delimiter is a duplicate → reason 13. A single-instance unknown group can't be distinguished from unknown tags, so it's accepted as such. | |

Group delimiter: the first field of the group definition, which "may be a
component or nested repeating group" (TV §4.3.6.4). If it is, the delimiter
is the component's first field or the nested NumInGroup tag. The existing
`Group::get_marker_tag` already implements this; the Dictionary
precomputes it.

## 5. Dictionary and codegen

**The Orchestra model stays. The Dictionary is added beside it.**
`babelfix-repo`'s `FixVersion` keeps everything: components, groups,
messages, codesets and documentation. fixation keeps using it for display
and editing (`ui.rs:2253` recurses through `MessageElement` components, and
`is_member` filters fields when deriving messages). The `Dictionary` is a
flattened, read-only, optimised view compiled from a `FixVersion`, used by
the parser and the builders:

- per field (dense id): kind (Plain / NumInGroup / DataLen / Data / Derived)
  and datatype marker;
- the Length ↔ data pairs, from `lengthId`;
- per (msg type, NumInGroup tag): group id. The same NumInGroup tag can mean
  different groups in different messages (`NoLegs`);
- per group: delimiter tag, member bitset, and member definition-order index
  (for conformant building);
- header and trailer member sets, for the region boundaries;
- codeset names, for `{:#?}` and fixation's display.

`msg.dict().version()` returns the full `Arc<FixVersion>`. The Dictionary
is built at runtime, so merged and custom Orchestra files keep working.
`babelfix-repo` needs to start reading `lengthId` and `unionDataType`.

**Codegen (`babelfix-repogen`), codesets included:**

```rust
pub mod fields {          // typed tags
    pub const Price: Field<Price> = Field::new(44);
    pub const Side: Field<codesets::Side> = Field::new(54);
    pub const RawDataLength: Field<DataLength> = Field::new(95);   // readable, not settable
    pub const NoPartyIDs: GroupField = GroupField::new(453);
}
pub mod tags { pub const Price: u32 = 44; /* ... */ }   // plain u32, for `match` and loops
pub mod msg_type { pub const ExecutionReport: MsgType = MsgType::new("8"); }
pub mod codesets {
    #[non_exhaustive]
    pub enum Side<'a> { Buy, Sell, SellShort, /* ... */ Other(&'a [u8]) }
    impl Side<'_> {
        pub const fn wire(&self) -> &[u8];          // b"1"
        pub const fn name(&self) -> &'static str;   // "Buy" ("" for Other)
    }
}
```

- **Codeset decode never fails.** An unlisted value becomes `Other`, which
  covers `Reserved100Plus`-style ranges and the two `unionDataType="Qty"`
  fields. `Other` is settable, so bilateral values round-trip.
- **Base types.** Codesets come in int (511), char (85), String (51) and
  Boolean (31). `MultipleCharValue` / `MultipleStringValue` codesets decode
  to an iterator of the enum.
- **Matching on a tag.** A `const` of a non-primitive type can't be a
  `match` pattern against a `u32`, hence the separate `tags` module.
- **One set of constants.** They're generated from `FIX_Latest`, as today,
  and used for every version.

**Generated message views (later phase, sugar):**

```rust
let nos = NewOrderSingle::view(&msg)?;          // checks MsgType
nos.price()? -> Option<Decimal>;  nos.parties() -> Group<'_>;  nos.block() -> Block<'_>
```

These are thin newtypes over `Block`, so generic code, view code and ad hoc
code all share one representation.

## 6. SendingTime without a clock in core

The invariant (commit `a1d373e`) is that babelfix-core never reads a clock.
The driver supplies the time, once per message, as late as possible. Today
that invariant is kept with an empty placeholder that `stamp_sending_time`
finds by linear search and replaces with an allocated `String`. It errors if
the placeholder is missing, so a driver that forgets to stamp can't send
silently.

Keep the invariant, and enforce it with types instead:

```rust
pub trait SessionOutput {
    fn transmit(&mut self, msg: Unstamped<'_>, session: &Session) -> Result<()>;
    fn event(&mut self, event: Event<'_>) -> Result<()>;
}

/// An outbound message whose SendingTime slot is reserved but empty. The only way to
/// get at the message, and therefore to encode it, is to stamp it.
#[must_use]
pub struct Unstamped<'a> { msg: &'a mut Message, slot: Pos, precision: TimePrecision }
impl<'a> Unstamped<'a> {
    pub fn stamp(self, now: DateTime<Utc>) -> &'a Message;
}

// driver
fn transmit(&mut self, msg: Unstamped<'_>, _: &Session) -> Result<()> {
    let msg = msg.stamp(Utc::now());       // the clock read lives here, in the driver
    self.codec.encode(msg, &mut self.out)
}
```

The session reserves the slot in the header gap, sized for its
`TimePrecision` (timestamps of a given precision have a fixed width). So
`stamp` is an in-place write of 17–27 bytes: no allocation, no search, no
tape edit. Forgetting to stamp is now a compile-time impossibility (you
can't reach the message otherwise), plus a `#[must_use]` warning if the
`Unstamped` is dropped. `RawMessageSent` sees the stamped message, as
today.

## 7. Migration plan

Each step leaves the tree green.

0. **Usage first.** Commit §2 as `#[ignore]`d or `compile_fail` tests, plus
   ports of the worst fixation call sites and of `session/state.rs`'s message
   code. Iterate on that before writing internals.
1. **babelfix-repo**: read `lengthId` and `unionDataType`. **`Dictionary`**
   compiled from `FixVersion`, with tests: group membership and order, the
   per-message NoLegs case, and the data pairs.
2. **`Message` + parser + `Block`/`Cursor` reader + serialiser.**
   - Golden tests: for every message in the existing test and benchmark
     corpus, `parse → serialise` is byte-identical, and the structure
     matches today's `builder::Message::from_message`.
   - Proptest round trip: build random → serialise → parse → compare.
   - One test per row of the §4 table.
3. **`FieldType`/`FromFix`/`ToFix`; typed codegen including codesets;
   `decimix`/`decimix-finance` features; chrono conversions.**
4. **`BlockMut`/`GroupMut`/`CursorMut`**: set/remove/insert/retain/copy, gap
   handling, definition-order placement in groups, and a debug assertion
   that recomputes structure and spans after every edit.
5. **Switch the session layer**: `codec` (and `Unstamped`), `state.rs`,
   `handshake.rs`, `replay.rs`, `driver.rs`, the tokio crate, the tests,
   fix-to-kafka; port `normalize()`.
6. **Delete** `FixMessage`, `Value`, `builder::*`, `TypedValue`.
7. **Migrate fixation** (five files; the editor shrinks the most) and the
   other project.
8. **Generated message views.**
9. **Performance**: compare against today's `message_benchmarks.rs`, then
   add the SIMD scanner, incremental checksum/length, SoA keys and the slot
   table, as the profile dictates.

## 8. Other ideas

- **Hand the application the codec's message, untouched.** This is most of
  the win, and it falls out of step 5.
- **`Message::clear()` keeps capacity**, so messages can be pooled with
  zero steady-state allocation. Design it in now.
- **Events carry `wire: &[u8]`**, so journals store the exact bytes rather
  than re-serialised text (fixation persists `msg.to_string()` today).
- **The `fix!` macro** replaces hand-built bytes in `tests/session/raw.rs`
  and the benchmarks.

## 9. Decisions

1. **Empty means absent.** Empty on the wire → reason 4. `set` of an empty
   value → `debug_assert!`, and the field is removed in release builds.
2. **decimix is optional and decoupled.** Floats decode to `Decimal<'a>`; the
   features add impls only. Qty → `UDec19` naturally, and `Dec19` or
   `DeltaQty` on request.
3. **Strictness** is the §4 table. There are no session options. Group field
   order isn't validated, but built messages conform to it.
4. **Header/body/trailer** are enforced (TV §4.3.3), and implemented as tape
   ranges with gap entries.
5. **Codesets are in the typed codegen phase**, as non-exhaustive enums with
   `Other`.
6. **The `Decimal` grammar is FIX `float`, exactly.**
7. **Strings are `FixStr`**, Latin-1 aware, with an ASCII fast path.
8. **Data/Length pairing** is transparent and driven by `lengthId`, and the
   Length tag can't be set directly.
9. **The Orchestra model stays as is.** The Dictionary is an added,
   flattened view.
10. **SendingTime uses the `Unstamped` typestate.** Core still never reads a
    clock.

11. **Header gap: 20 entries** (§3).
12. **chrono stays a plain dependency of core**, as today: no feature flag.
    - Formatting legitimate timestamps is a core capability: the session
      stamps SendingTime, and users set TransactTime and similar fields.
      That means `time::fix_time` and the `(DateTime<Utc>, TimePrecision)`
      `ToFix` impl stay in core. Parsing goes through `get_as`.
    - Making chrono optional would mean a home-grown timestamp type in the
      `Unstamped::stamp` signature and in every conversion, which costs more
      than it saves. If you ever want core chrono-free, the change is
      contained: swap `stamp`'s argument for an epoch-nanos newtype with
      `From<DateTime<Utc>>` and `From<SystemTime>`.

## 10. Still open

Nothing blocking. Next step is §7 step 0: write the §2 usage as
`compile_fail` / `#[ignore]` tests and iterate on the names there.

## 11. Implementation notes (where the build differs from the above)

- **`Unlisted`, not `Other`.** 73 FIX codes are named `Other`, so the codeset
  catch-all is `Unlisted(&[u8])`. No code uses that name.
- **`Dictionaries`.** One compiled `Dictionary` per version, shared by every
  connection, replaces `Arc<FixRepository>` in the endpoint, connection and
  driver APIs. An acceptor needs it to latch the version from the first frame.
  `Dictionaries::standard()` builds them from the embedded Orchestra data.
- **Events carry `&Message`, not `wire: &[u8]`.** A received message's
  `wire()` is the exact bytes. A sent message re-encodes deterministically
  (`to_bytes()`). Passing the driver's buffer slice through the state machine
  wasn't worth the extra coupling.
- **`parse_fragment` moves header fields.** Text that people type puts header
  fields anywhere. Fragments place them in the header, and wire parsing
  still rejects them (reason 14). The encoder always writes 35 third.
- **Checksums of `|`-delimited text are the SOH checksum.** This is what logs
  show, so a logged message parses back.
- **Tags with leading zeros are garbled** (TagValue: TagNum "may not contain
  leading zeros"). This also bounds the cached `val_off` at 11.
- **The typed schema is generated by `babelfix-core`'s `build.rs`.**
  `babelfix-repogen` (the `FIX_x::Fields` u32 modules) is still re-exported
  but is now redundant with `schema::tags`. **Licensing:** the generated
  schema derives from the Apache-2.0 Orchestra data, which is why repogen is
  `MIT AND Apache-2.0`. `babelfix-core` is plain MIT, so this needs a decision
  before release.
- **Duplicates are opt-in.** Parsing keeps duplicate tags (reads see the
  first). `Message::validate_strict()` checks them (reason 13), and also
  group-instance field order (reason 15). That reverses decision §9.3 for
  duplicates; the unknown-group rule therefore also applies only under
  `validate_strict`.
