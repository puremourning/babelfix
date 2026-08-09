# babelfix

[![CI](https://github.com/puremourning/babelfix/actions/workflows/ci.yml/badge.svg)](https://github.com/puremourning/babelfix/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An asynchronous [FIX protocol](https://www.fixtrading.org/) engine for Rust,
driven by the FIX Orchestra metadata repository.

> babelfix is pre-1.0 — the API may change between minor versions.

The implementation is intended to be practical, rather than optimal. In
particualr, the `message::Builder` API is designed for ease of use, rather than
maximum performance.

The library does not provide any persistence or storage of messages, sequence
numbers or session state. It is the application's responsiblity to persist them
for recovery and session reconnection/replay.

## Overview

babelfix parses, builds and exchanges [FIX](https://www.fixtrading.org/)
messages. It is built around the FIX Orchestra metadata (embedded for FIX 4.2,
4.4 and FIX.Latest), so message structure, field types and repeating groups come
from the specification. Any FIX Orchestra files can be used by the appliction.

It is a small stack of layers, each usable on its own:

| Layer | Crate | Responsibility |
|-------|-------|----------------|
| Schema | `babelfix-repogen` | Compile-time FIX tag-number constants (`schema::FIX_Latest::Fields`) |
| Repository | `babelfix-repo` | Parsed Orchestra metadata: versions, messages, fields, components, groups |
| Message | `babelfix-core::message` | Parse, build and serialise individual messages |
| Codec | `babelfix-core::codec` | Frame a byte stream into messages and back |
| Session | `babelfix-core::session` | Sequence numbers, heartbeats, test requests, resend/replay |
| Driver | `babelfix-core::driver` | The above assembled: feed bytes, drain bytes |
| Connection | `babelfix-tokio::connection` | A session driven inline, without channels |
| Endpoint | `babelfix-tokio::endpoint` | TCP acceptor/initiator that spawns sessions |

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
| serialise and parse alone | 4.8 |
| + the session layer (`SessionDriver`) | 8.3 |
| + sockets, tasks and channels (`endpoint`) | 37.4 |

A loopback TCP round trip carrying the same bytes is 19.6µs of that, so most of
the difference is the transport rather than anything babelfix does. Re-run
`cargo bench -p babelfix` on the machine you care about before drawing
conclusions.

## Quickstart

```toml
[dependencies]
babelfix = "0.1"
```

Load the embedded repository, then build and serialise a message:

```rust
use babelfix::{repository, message::builder};
use babelfix::schema::FIX_Latest::Fields;

// The FIX Orchestra data is embedded; nothing is read from disk.
let repo = repository::orchestrate().unwrap();
let fix44 = repo.get_version("FIX.4.4").unwrap();

// NewOrderSingle (MsgType = "D"). `new` presets BeginString (8) and MsgType (35).
let mut order = builder::Message::new(fix44, "D").unwrap();
order.body.set_tag(Fields::ClOrdID, "order-1");
order.body.set_tag(Fields::Symbol, "AAPL");
order.body.set_tag(Fields::Side, "1"); // 1 = Buy
order.body.set_tag(Fields::OrderQty, 100i64);

// BodyLength (9) and CheckSum (10) are computed on serialisation.
let msg = order.into_message().unwrap();
// SOH (b'\x01') is the real field separator; b'|' is convenient for logging.
println!("{}", msg.to_string_delimited(b'|'));
```

Running a FIX session over TCP — accepting connections with
[`endpoint::serve`] or initiating them with [`endpoint::connect`], then driving
the resulting [`session::SessionHandle`] — is covered in the `endpoint` and
`session` module documentation.

## Documentation

Full API documentation is on [docs.rs/babelfix](https://docs.rs/babelfix). The
`message`, `session`, `driver`, `connection` and `endpoint` module docs include
worked examples for building messages and running the session/recovery
machinery.

[CONFORMANCE.md](CONFORMANCE.md) records the known deviations of the session
layer from the FIX Session Layer Technical Specification.

## Minimum supported Rust version

Rust 1.85 (edition 2024).

## Licence

Licensed under the [MIT license](LICENSE).

The `babelfix-repo` and `babelfix-repogen` crates additionally bundle and derive
from the [FIX Orchestra](https://www.fixtrading.org/standards/fix-orchestra/)
reference data, which is licensed under Apache-2.0. Those two crates are
therefore distributed under `MIT AND Apache-2.0`; the upstream licence and
notice are retained under `crates/babelfix-repo/third-party/fix_orchestra/`.

[`endpoint::serve`]: https://docs.rs/babelfix/latest/babelfix/endpoint/fn.serve.html
[`endpoint::connect`]: https://docs.rs/babelfix/latest/babelfix/endpoint/fn.connect.html
[`session::SessionHandle`]: https://docs.rs/babelfix/latest/babelfix/session/struct.SessionHandle.html
