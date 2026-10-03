# Migrating to application-owned sequence numbers

The session layer no longer allocates outbound sequence numbers, and no longer
emits session state for you to persist. Every outbound message — admin
messages included — carries the `MsgSeqNum` the application gave it, and a
message is persisted *before* it is sent rather than after. The
[proposal](https://github.com/puremourning/babelfix/blob/main/docs/session_state_management_proposal.md)
explains why. In short: an application persisting `RawMessageSent` could crash
with the message on the wire and not in its store, and its next Logon would
then be too low.

Most applications use the tokio endpoint, where the change is small: supply a
`SessionStore`, and the session persists each message before sending it.

## At a glance

| Before | After |
|---|---|
| `session::Session { next_out_seq_num, next_in_seq_num, heartbeat_interval, dict, time_precision }` | `session::SessionConfig { heartbeat_interval, dict, time_precision }`, plus `sequencer::Resume { next_out_seq_num, next_in_seq_num }` |
| `endpoint::connect(.., session, config)` | `endpoint::connect(.., SessionSetup::new(config).resume(resume).store(store), config)` |
| `EndpointEvent::NewSession` answered with `Session` | answered with `SessionSetup` |
| `SessionEvent::SessionState(Session)` — persist it | gone: outbound messages go to your `SessionStore::persist_outbound`, the inbound watermark to `persist_watermark` |
| `SessionEvent::RawMessageSent(msg, session)` — persist it to replay later | `RawMessageSent(msg)`: audit and display only. Replay from what `persist_outbound` stored |
| `SessionEvent::RawMessageReceived(msg, session)` | `RawMessageReceived(msg, received_at)`: when its bytes were read from the socket. Order a journal of both directions by this, not by when you record it |
| `SessionEvent::MessageReceived(msg)` | `MessageReceived { seq_num, msg }` |
| — | `SessionEvent::LoggedOn`, once both Logons have been exchanged |
| Session-level Reject (35=3) swallowed | delivered as `MessageReceived` |
| `SessionCommand::GetSessionState` → `Session` | → `SessionStatus { next_out_seq_num, next_in_seq_num, watermark }` |
| — | `SessionCommand::SendRaw(msg)`: persist and send under the message's own `MsgSeqNum` |
| — | `SessionCommand::Handled(seq_num)`, under `InboundPolicy::Explicit` |
| `Send` overwrote `SendingTime` | a `SendingTime` you set is kept |
| `SessionConnection::initiate(.., session, sink)` / `PendingSession::accept(session, sink)` | take a `SessionSetup`; `send` returns the sequence number |

## The tokio endpoint

Implement `store::SessionStore` over your storage. Each method starts a write
and returns a `'static` future that completes once it is durable. Several may
be in flight at once.

```rust
use futures::FutureExt;

struct MyStore { db: Db }

impl babelfix::store::SessionStore for MyStore {
    fn persist_outbound(&self, seq_num: u64, wire: bytes::Bytes)
        -> futures::future::BoxFuture<'static, babelfix::Result<()>>
    {
        let db = self.db.clone();
        async move { db.put_message(seq_num, wire).await; Ok(()) }.boxed()
    }

    fn persist_watermark(&self, next_in_seq_num: u64)
        -> futures::future::BoxFuture<'static, babelfix::Result<()>>
    {
        let db = self.db.clone();
        async move { db.put_next_in(next_in_seq_num).await; Ok(()) }.boxed()
    }
}
```

Resume a session from what was stored: one past the highest `seq_num` given
to `persist_outbound`, and the last watermark.

```rust
let setup = SessionSetup::new(SessionConfig::new(fix44))
    .resume(Resume { next_out_seq_num, next_in_seq_num })
    .store(Arc::new(MyStore { db }));
```

`SessionSetup::new` alone uses `VolatileStore`, which persists nothing and
completes every write at once. That is the old behaviour, for tools and tests.

Answer a `ResendRequest` from the store, exactly as before. The stored message
carries its original `SendingTime`, which the session moves to
`OrigSendingTime`.

By default an inbound message counts as handled once it is delivered
(`InboundPolicy::OnDelivery`). To resume from the last message you actually
finished with, use `InboundPolicy::Explicit` and send
`SessionCommand::Handled(seq_num)`.

## Your own event loop

`SessionDriver` and the handshake drivers now need the application to number
what they send. Either:

- **Use a `sequencer::Sequencer`** (message-centric). Wrap your sink with
  `sequencer.sink(&mut app)`. Handle `SeqEvent::Persist` by starting the
  write, and call `sequencer.persisted(seq_num, now, &mut driver, &mut app)`
  when it completes. See the `sequencer` module docs.
- **Number messages yourself** (event-centric). Handle
  `Event::AdminSendRequired(msg)`: set `MsgSeqNum` (and `SendingTime`, if you
  want the event's timestamp), and send it back with `Command::Send`. Every
  `Command::Send` needs a `MsgSeqNum`.

`InitiatorDriver::start` and `AcceptorDriver::accept` take the inbound
sequence number to expect alongside a `SessionConfig`. Both ask for their
Logon with `AdminSendRequired` rather than sending one. `SessionDriver::send_logon`
is gone; a hand-rolled handshake uses `request_logon`.

---

# Migrating to the `Message` API

babelfix used to have two message types: `FixMessage`, a flat list of tags
over the received bytes, and `builder::Message`, a typed tree of
`TypedValue`s. The session converted every inbound message from the first to
the second, and every outbound one back. Both are gone. There is now one
type, `message::Message`, whether it was parsed or built, and the session
hands you the codec's message without converting it.

This guide maps the old API onto the new one. The `message` module docs
cover the API itself, and
[`docs/message_proposal.md`](https://github.com/puremourning/babelfix/blob/main/docs/message_proposal.md)
explains the design.

## At a glance

| Before                                                                                | After                                                                                      |
|---------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `repository::orchestrate()` → `Arc<FixRepository>` passed to endpoints                | `message::Dictionaries::standard()?` → `Arc<Dictionaries>`                                 |
| `Arc<FixVersion>` in `Session`, builders                                              | `Arc<Dictionary>`: `dicts.get("FIX.4.4")`, `dicts.for_begin_string(b"FIX.4.4")`            |
| `Session { fix_version, .. }`                                                         | `Session { dict, .. }`, or `Session::new(dict)`                                            |
| `FixMessage`, `builder::Message`                                                      | `Message`                                                                                  |
| `schema::FIX_Latest::Fields::X` (`u32`)                                               | `schema::tags::X` (`u32`), or `schema::fields::X` (typed)                                  |
| `msg.get_type()` / `msg.fix_message.msg_type`                                         | `msg.msg_type()` → `&str`                                                                  |
| `msg.is_admin_message()`                                                              | `msg.is_admin()`                                                                           |
| `builder::Message::new(fix, "D")?`                                                    | `Message::new(&dict, "D")` (or `msg_type::NewOrderSingle`)                                 |
| `msg.body.tag(t)` → `Option<&TypedValue>`                                             | `msg.body().get(field)?` → `Option<T>`, or `msg.body().raw(t)` → `Option<&[u8]>`           |
| `msg.body.set_tag(t, v)`                                                              | `msg.body_mut().set(field, v)`, or `.set_raw(t, bytes)`                                    |
| `msg.body.remove_tag(t)`                                                              | `msg.body_mut().remove(t)`                                                                 |
| `msg.body.group_mut(n).push(block)`                                                   | `msg.body_mut().group_mut(n).push().set(..)`                                               |
| `FixMessage::from_bytes(fix, bytes)` → `(msg, consumed)`                              | `Message::parse(&dict, bytes)?` (exactly one message), or `FixDecoder::decode` for a stream |
| `FixMessage::from_bytes_delimited(fix, bytes, b'\|')`                                 | `Message::parse_delimited(&dict, &bytes, b'\|')?`, or `parse_fragment` for hand-typed text |
| `builder::Message::from_message(&m)` / `as_message()` / `into_message()`              | (nothing: there is one type)                                                               |
| `msg.write_to(&mut buf, SOH)` / `into_bytes()`                                        | `msg.encode(&mut buf)` / `msg.to_bytes()`                                                  |
| `msg.to_string_delimited(b'\|')`                                                      | `msg.to_string()` (`Display` uses `\|`), or `encode_delimited(&mut buf, b'\|')`            |
| `builder.normalize()?` → new message                                                  | `msg.normalize()` (in place)                                                               |
| `FixDecoder::new(repo, d)` / `with_version(repo, d, fix)`                             | `FixDecoder::new(dicts, d)` / `with_dictionary(dicts, d, dict)`                            |
| `FixEncoder::new(d).with_precision(p)`, `encode_stamped`, `codec::stamp_sending_time` | `FixEncoder::new(d)`; stamping is `Unstamped::stamp` (below)                               |

## Setting up

```rust
use babelfix::message::{Dictionaries, Message};

// Compile once; share the Arc. One Dictionary per FIX version.
let dicts = Dictionaries::standard()?;
let fix44 = dicts.get("FIX.4.4").unwrap();           // &Arc<Dictionary>

let session = babelfix::session::Session::new(fix44.clone());
let endpoint = babelfix::endpoint::serve(addr, dicts.clone(), config).await?;
```

Dictionaries are named by Orchestra version: `FIX.4.2`, `FIX.4.4` and
`FIX.Latest`. FIX.Latest goes on the wire as `FIXT.1.1`, so
`dicts.get("FIXT.1.1")` finds nothing, but
`dicts.for_begin_string(b"FIXT.1.1")` finds FIX.Latest. For Orchestra files of
your own, `Dictionaries::new(&repository::load_orchestration(path)?)`.

An acceptor learns the version from the peer's first message, which is why
endpoints, connections and `DriverConfig` take the whole `Dictionaries`. Answer
`EndpointEvent::NewSession` with a `Session` for the peer's version:

```rust
EndpointEvent::NewSession { session_id, response } => {
    // Use the sequence numbers you persisted for this peer.
    let session = dicts
        .for_begin_string(session_id.begin_string.as_bytes())
        .map(|d| Session::new(d.clone()))
        .ok_or_else(|| Error::unspecified("unknown FIX version"));
    let _ = response.send(session);
}
```

The full Orchestra model (components, documentation, everything a UI shows)
is still `repository::FixVersion`, available from any dictionary as
`dict.version()`. A `Dictionary` adds lookups: `field_name(tag)`,
`codeset_name(tag, value)` and `message_name(msg_type)`.

## Field constants

`schema::FIX_Latest::Fields`, the other per-version modules, and the
`babelfix-repogen` crate behind them are gone. In their place is the
`babelfix-schema` crate, re-exported as `babelfix::schema` (if you depend on
`babelfix-core` alone, add `babelfix-schema` too). It has four modules, all
generated from FIX.Latest (tag numbers are shared across versions):

- **`schema::fields`**: typed constants. `Price: Field<datatypes::Price>`,
  `Side: Field<codesets::SideCodeSet>`, `NoPartyIDs: GroupField`. Use these
  with `get`/`set`.
- **`schema::tags`**: the same names as plain `u32`s, for `match`, loops over
  several tags, and anywhere that takes `impl Tag` (`raw`, `set_raw`, `has`,
  `remove`, `copy`).
- **`schema::msg_type`**: `msg_type::NewOrderSingle`, and so on.
- **`schema::codesets`**: an enum per codeset (`codesets::Side::Buy`), each
  with an `Unlisted(&[u8])` variant for values the spec doesn't list.

`use schema::fields::*;` and `use schema::codesets::*;` together are fine.
`Side` the constant and `Side` the enum live in different namespaces, so
`b.set(Side, Side::Buy)` compiles.

## Reading

```rust
let body = msg.body();                         // also msg.header(), msg.trailer()

body.get(ClOrdID)?                             // Result<Option<FixStr>, FieldError>
body.req(ClOrdID)?                             // missing is an error too
body.get_as::<Dec19>(Price)?                   // with the `decimix` feature (below)
body.raw(tags::Text)                           // Option<&[u8]>, never fails
msg.find(MsgSeqNum)?                           // header or body, top level
```

What `get` returns depends on the field's datatype:

| Datatype | `get` yields | Notes |
|---|---|---|
| String and its kin | `FixStr<'_>` | FIX strings are Latin-1 (TagValue §4.1), not UTF-8. Use `as_ascii()` → `Option<&str>`, `to_str()` → `Cow<str>`, `Display`, or `== "x"`. |
| int / SeqNum, NumInGroup, ... | `i64` / `u64` | |
| float, Price, Qty, Amt, ... | `Decimal<'_>` | Validated text, never `f64`. `get_as::<Dec19>` / `UDec19` / the decimix-finance types with the features; `to_f64_lossy()` for display. |
| Boolean / char | `bool` / `u8` | |
| UTCTimestamp, dates, times | `Timestamp` / `Date` / `Time` | Thin text views; `get_as::<DateTime<Utc>>`, `NaiveDate`, `NaiveTime` parse them. |
| data | `&[u8]` | May contain SOH. |
| codesets | the enum | Never fails: unknown values are `Unlisted(bytes)`. |

`get` returns `Ok(None)` when the field is absent and `Err(FieldError)` when it
is present but malformed. `FieldError` carries the tag, and
`reject_reason()` gives the `SessionRejectReason`.

`Decimal` compares as text: `"1.0"` and `"1.00"` are not equal. Convert with
`get_as` to compare numbers.

### Decimals

Decimal fields decode to `Decimal`, the validated text. To get numbers, enable
the `decimix` feature (on `babelfix` or `babelfix-core`) and convert with
`get_as`/`req_as`; `set` takes them directly:

```toml
babelfix = { version = "0.1", features = ["decimix"] }   # or "decimix-finance"
```

```rust
use decimix::{Dec19, UDec19};

let px: Option<Dec19> = order.body().get_as(Price)?;
let qty: UDec19 = order.body().req_as(OrderQty)?;      // Qty is unsigned
er.body_mut().set(OrderQty, qty);
```

`decimix-finance` adds the same conversions for its `Price`, `Qty`,
`DeltaQty`, `Amt` and `Percentage` types.

Converting `TypedValue`:
- `.as_string()` (which allocated) becomes `get(..)?.map(|s| s.to_string())`,
  or better, keep the `FixStr`.
- `TypedValue::Float(p)` → `Dec19::from_f64_lossy(p, ..)` becomes
  `get_as::<Dec19>(Price)?`.
- `if let Some(TypedValue::String(s)) = ...` becomes a plain `get`.

### Groups

```rust
for party in msg.body().group(NoPartyIDs) {     // empty if absent
    let id = party.req(PartyID)?;
    for sub in party.group(NoPartySubIDs) { /* ... */ }
}
let parties = msg.body().group(NoPartyIDs);
parties.len();
parties.get(1);                                 // Option<Block>
```

### Hints

If you know where a field is, say so; a wrong hint only costs time:

```rust
let legs = body.group(NoLegs);
body.get_from(Price, legs.end())?;
```

## Building and editing

```rust
let mut order = Message::new(&dict, msg_type::NewOrderSingle);
order
    .body_mut()
    .set(ClOrdID, "order-1")
    .set(Side, Side::Buy)
    .set(OrderQty, 100u64)                        // or a Decimal, or a UDec19
    .set_raw(tags::Price, b"42.50");
let mut parties = order.body_mut().group_mut(NoPartyIDs);
parties.push().set(PartyID, "CLIENT-A").set(PartyRole, PartyRole::ClientID);
```

- **`set` takes typed values.** Strings are `&str` (written as Latin-1),
  numbers are integers, decimals are `Decimal` or decimix types, codesets are
  the enum. For anything the dictionary types differently from what a
  counterparty sends, use `set_raw(tag, bytes)`.
- **Timestamps take a precision.** `set(TransactTime, (Utc::now(),
  TimePrecision::Micros))`; a bare `DateTime<Utc>` doesn't say how many
  fractional digits to write.
- **Text outside Latin-1 is an invalid value.** Like an empty value, it fails
  a `debug_assert!` in `set` and removes the field in release builds; use
  `try_set` for text you didn't write.
- **No empty values.** FIX has none. `set(.., "")` fails a `debug_assert!`
  and removes the field in release builds; `try_set` returns the error instead.
- **NumInGroup is maintained for you.** You never write it. An instance left
  empty is removed when you let go of it, and its group with it if that was
  the last one, so `len()` is always what is written.
- **Order within a group instance is handled for you.** Fields go in the
  group's definition order, whatever order you set them in.
- **Data fields are set as pairs.** `set(RawData, bytes)` writes `RawData`
  and `RawDataLength` together, and `remove` removes both. Setting a Length
  field directly is an error.
- **`remove` really removes.** No tombstones.
- **Session fields are not yours to set.** You don't set `MsgSeqNum`,
  `SenderCompID`, `TargetCompID` or `SendingTime` on a message you send; the
  session sets them, as before. BeginString, BodyLength, MsgType and CheckSum
  can't be set (`MsgType` comes from `Message::new`).
- **`clear(msg_type)` reuses a message's allocations.**

To edit a parsed message, use the same calls: `header_mut()`, `body_mut()`.
New bytes go into the message's arena, and the received bytes are never
copied.

Copying between messages copies bytes and decodes nothing:

```rust
b.set(ClOrdID, order.body().req(ClOrdID)?);   // one field, typed
b.copy(&order.body(), tags::Symbol);          // one field or a whole group, as is
```

Within one message, where you can't borrow and set at once, use
`h.copy_value(tags::SendingTime, tags::OrigSendingTime)?`.

## Generic code: walking, cursors, paths

The old `builder::Element` match (`Tag` / `Group` / `RawDataTag`) becomes a
`Cursor`:

```rust
for c in msg.walk() {                       // every entry, depth first, wire order
    match c.kind() {
        EntryKind::Field | EntryKind::DataLength | EntryKind::Data => { c.tag(); c.value(); }
        EntryKind::Group => { c.count(); }
        EntryKind::Instance => { c.instance_index(); }
    }
    c.depth();
}
block.fields()                              // one level of one block
```

A cursor borrows the message. For a position that survives edits, which is
what a UI holds between frames, take its `path()`:

```rust
let path: FieldPath = cursor.path();            // e.g. body/453[1]/452
"body/453[1]/452".parse::<FieldPath>()?;        // and back

// A path may no longer resolve, so cursor_mut is an Option.
if let Some(mut c) = msg.cursor_mut(&path) {
    c.set_raw(b"new")?;                         // edit in place
}
if let Some(mut c) = msg.cursor_mut(&group_path) {
    if let Some(mut repeat) = c.push_instance() { // add a repeat
        repeat.set_raw(tags::PartyID, b"?");    // (an empty one is removed)
    }
}
if let Some(c) = msg.cursor_mut(&path) {
    c.remove();
}
```

`values_mut` rewrites values throughout the message, for placeholders like
`{{now}}`. Return `Some(bytes)` to replace a value, `None` to leave it:

```rust
msg.values_mut(|_tag, value| (value == b"{{now}}").then(|| now.clone()));
```

It skips data fields, and BeginString, BodyLength, MsgType and CheckSum. An
empty replacement, or one containing SOH, is ignored (and fails a
`debug_assert!`). `msg.normalize()` puts every block in
definition order. `format!("{msg:#?}")` prints one field per line, with names
and codeset names from the dictionary.

## The session layer

- `SessionEvent::MessageReceived(Message)`, `RawMessageReceived(Message, Session)`,
  `RawMessageSent(Message, Session)`, and `ResendRequest { resend_request:
  Message, .. }`. In core, `Event` borrows the same types.
- `SessionCommand::Send(Message)` and `Replay(Message)`. For a replay, parse
  the stored bytes with `Message::parse(&dict, bytes)?`.
- To journal the exact bytes: a received message's `wire()` is the bytes as
  they arrived. A sent message re-encodes identically with `to_bytes()`.

If you implement `SessionOutput` yourself, `transmit` now receives an
`Unstamped` message (`babelfix::session::Unstamped`). Reading your clock and stamping is the only way to reach
it:

```rust
fn transmit(&mut self, msg: Unstamped<'_>, _: &Session) -> Result<()> {
    let msg = msg.stamp(Utc::now());
    self.encoder.encode(msg, &mut self.out)
}
```

The session's `time_precision` decides the width, so the encoder no longer
takes a precision.

## Parsing is stricter in some ways, looser in others

Parsing follows the TagValue Encoding spec:

- **Rejected:**
  - an empty value (reason 4);
  - a header field after the body has begun (reason 14);
  - a NumInGroup that doesn't match the instances present (reason 16);
  - an instance that doesn't start with the group's delimiter (reason 15).
- **Garbled:**
  - a data field that doesn't follow its Length, or isn't exactly that
    long;
  - a tag with leading zeros;
  - wrong BodyLength or CheckSum.
- **Kept, not rejected:**
  - unknown tags;
  - fields in any order within a group instance;
  - duplicate tags. Reads see the first occurrence.

`msg.validate_strict()` checks the two rules parsing tolerates: duplicate tags
(reason 13) and field order within group instances (reason 15). Call it where
you want them enforced.

**Fragments.** `Message::parse_fragment(&dict, text, b'|')` is for text a
person typed:
- 8, 9 and 10 are optional (missing 8 is supplied, 9 and 10 are recomputed);
- 35 is required;
- header fields anywhere are moved into the header, except header groups
  (NoHops), whose instances can't be told apart from body fields once the body
  has begun: those must come before the body.

**`|` checksums.** A `|`-delimited message's CheckSum is the checksum of its
SOH form, which is what logs show. So `parse_delimited` accepts logged
messages, and `Display` produces them.
