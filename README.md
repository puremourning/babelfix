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
| Message | `babelfix::message` | Parse, build and serialise individual messages |
| Session | `babelfix::session` | Sequence numbers, heartbeats, test requests, resend/replay |
| Endpoint | `babelfix::endpoint` | TCP acceptor/initiator that frames the wire protocol and drives sessions |

Most applications depend only on the `babelfix` crate, which re-exports the other
two as `babelfix::repository` and `babelfix::schema`.

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
`message`, `session` and `endpoint` module docs include worked examples for
building messages, and running the session/recovery machinery.

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
