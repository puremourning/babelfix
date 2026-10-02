# babelfix

[![CI](https://github.com/puremourning/babelfix/actions/workflows/ci.yml/badge.svg)](https://github.com/puremourning/babelfix/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An asynchronous [FIX protocol](https://www.fixtrading.org/) engine for Rust,
driven by the FIX Orchestra metadata repository.

> babelfix is pre-1.0 — the API may change between minor versions.

Messages are one type, parsed or built: a flat index over the message's bytes,
with repeating groups, typed fields decoded on demand (prices as decimals, never
`f64`), and edits that never copy the bytes they leave alone. Upgrading from the
earlier `FixMessage`/`builder::Message` API? See [MIGRATION.md](MIGRATION.md).

The library does not provide any persistence or storage of messages, sequence
numbers or session state. It is the application's responsibility to persist them
for recovery and session reconnection/replay.

## Overview

babelfix parses, builds and exchanges [FIX](https://www.fixtrading.org/)
messages. It is built around the FIX Orchestra metadata (embedded for FIX 4.2,
4.4 and FIX.Latest), so message structure, field types and repeating groups come
from the specification. An application can load Orchestra files of its own
instead: see the `Dictionaries` docs.

It is a small stack of layers, each usable on its own:

| Layer | Crate | Responsibility |
|-------|-------|----------------|
| Schema | `babelfix-schema` | Typed field constants, message types and codeset enums, generated from FIX.Latest |
| Repository | `babelfix-repo` | Parsed Orchestra metadata: versions, messages, fields, components, groups |
| Message | `babelfix-core::message` | Parse, read, build, edit and serialise messages |
| Codec | `babelfix-core::codec` | Frame a byte stream into messages and back |
| Session | `babelfix-core::session` | Sequence numbers, heartbeats, test requests, resend/replay |
| Driver | `babelfix-core::driver` | The above assembled: feed bytes, drain bytes |
| Connection | `babelfix-tokio::connection` | A session driven inline, without channels |
| Endpoint | `babelfix-tokio::endpoint` | TCP acceptor/initiator that spawns sessions |

Dictionaries are named by version: `FIX.4.2`, `FIX.4.4` and `FIX.Latest`.
FIX.Latest's BeginString on the wire is `FIXT.1.1`, which
`Dictionaries::for_begin_string` understands.

Most applications depend only on the `babelfix` crate, which re-exports all of
the above.

## Which layer do I want?

The protocol lives in `babelfix-core`, which is *sans-io*: no sockets, no
timers, no tasks, and no async runtime anywhere in its dependency tree. It does
not even read a clock — timestamps are handed to it. Everything above that is a
way of feeding it.

| If you | Use | You give up |
|--------|-----|-------------|
| own your event loop — `epoll`, `io_uring`, a busy-polled socket | `babelfix-core::driver::SessionDriver` | nothing is done for you: you read, you write, you decide when |
| want async, but your loop is the hot loop | `babelfix-tokio::connection::SessionConnection` | heartbeats only advance while you are in the loop |
| want a FIX engine | `babelfix::endpoint` | two channel hops and a task per session |

Measured on one round trip — an order out, an execution report back — the layers
cost roughly:

| | µs |
|---|---|
| serialise and parse alone | 1.0 |
| + the session layer (`SessionDriver`) | 2.4 |
| + sockets, tasks and channels (`endpoint`) | 27.2 |

A loopback TCP round trip carrying the same bytes is 19.6µs of that, so most of
the difference is the transport rather than anything babelfix does. Re-run
`cargo bench -p babelfix` on the machine you care about before drawing
conclusions.

## Quickstart

```toml
[dependencies]
babelfix = "0.1"
```

Prices and quantities are never `f64`. They decode to their validated text,
and with the `decimix` feature (`features = ["decimix"]`, or `"decimix-finance"`)
convert exactly to `decimix` decimals with `get_as`.

Compile the embedded dictionaries, then build, serialise and read a message:

```rust
use babelfix::message::{Dictionaries, Message};
use babelfix::schema::{codesets, fields::*, msg_type};

// The FIX Orchestra data is embedded; nothing is read from disk. Compile the
// dictionaries once and share them.
let dicts = Dictionaries::standard().unwrap();
let fix44 = dicts.get("FIX.4.4").unwrap();

// `new` presets BeginString (8) and MsgType (35).
let mut order = Message::new(fix44, msg_type::NewOrderSingle);
order
    .body_mut()
    .set(ClOrdID, "order-1")
    .set(Symbol, "AAPL")
    .set(Side, codesets::Side::Buy)
    .set(OrderQty, 100u64);

// BodyLength (9) and CheckSum (10) are computed on serialisation. `Display`
// shows the wire form with `|` for SOH.
println!("{order}");
let wire = order.to_bytes();

// Parsing keeps the bytes and indexes them; fields decode when read, typed by
// the field: Symbol is a string, OrderQty a decimal.
let parsed = Message::parse(fix44, wire).unwrap();
assert_eq!(parsed.body().req(Symbol).unwrap(), "AAPL");
assert_eq!(parsed.body().req(OrderQty).unwrap().as_str(), "100");
```

Timestamps are set with a precision: `set(TransactTime, (Utc::now(),
TimePrecision::Micros))`. Strings are Latin-1, as FIX defines them; setting
text outside Latin-1 panics in debug builds, so use `try_set` for text you
didn't write.

Running a FIX session over TCP — accepting connections with `endpoint::serve`
or initiating them with `endpoint::connect`, then driving the resulting
`session::SessionHandle` — is covered in the `endpoint` and `session` module
documentation.

## Features

| Feature | Default | |
|---|---|---|
| `tokio` | yes | The TCP transport: `endpoint`, `connection` and the async session driver. |
| `decimix` | | Decimal fields as `decimix::Dec19`/`UDec19`, via `get_as` and `set`. |
| `decimix-finance` | | As `decimix`, plus the `decimix-finance` `Price`, `Qty`, `Amt`, ... types. |
| `serde` | | `Serialize`/`Deserialize` for `SessionIdentifier` and `TimePrecision`. |

For the sans-io core alone, use `default-features = false`, or depend on
`babelfix-core` directly.

## Documentation

Full API documentation is on [docs.rs/babelfix](https://docs.rs/babelfix). The
`message`, `session`, `driver`, `connection` and `endpoint` module docs include
worked examples for building messages and running the session/recovery
machinery.

[CONFORMANCE.md](CONFORMANCE.md) records the known deviations of the session
layer from the FIX Session Layer Technical Specification.

## Minimum supported Rust version

Rust 1.98 (edition 2024), required by the `decimix` crate behind the optional
`decimix` features.

## Licence

Licensed under the [MIT license](LICENSE).

The `babelfix-repo` and `babelfix-schema` crates additionally bundle or derive
from the [FIX Orchestra](https://www.fixtrading.org/standards/fix-orchestra/)
reference data, which is licensed under Apache-2.0: `babelfix-repo` embeds it,
and `babelfix-schema` is generated from it. Those crates are therefore
distributed under
`MIT AND Apache-2.0`; the upstream licence and notice are retained under
`crates/babelfix-repo/third-party/fix_orchestra/`.

[`endpoint::serve`]: https://docs.rs/babelfix/latest/babelfix/endpoint/fn.serve.html
[`endpoint::connect`]: https://docs.rs/babelfix/latest/babelfix/endpoint/fn.connect.html
[`session::SessionHandle`]: https://docs.rs/babelfix/latest/babelfix/session/struct.SessionHandle.html
