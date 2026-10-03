# Proposal: application-owned sequence numbers (rev 2)

Response to `session_state_management_issues.md`.

The FIX session layer stops allocating sequence numbers. The application
supplies the `MsgSeqNum` of every outbound message, including the admin
messages the session wants to send. The session layer stops claiming that
anything it emits is persisted state.

Two modes are built from that one mechanism:

- **Sequenced (message-centric).** This is the default, because it's what
  most FIX engines do. A sans-io `Sequencer` allocates numbers, asks the app
  to persist each message, and releases it to the wire only once the write
  has completed.
- **External (event-centric).** The app's event stream allocates the numbers
  implicitly, and the core is used directly. A round trip costs one persisted
  write in each direction.

Spec references are to the FIX Session Layer (Nov 2020, "FSL").

**Rev 2 changes:**
- **The header is the contract.** `MsgSeqNum`, `SendingTime` and
  `OrigSendingTime` live in the message, and `Command` carries no extra
  fields (§3.2).
- **Admin requests hand the message to the app.** There is no `AdminId` and
  no `SendAdmin`; the app owns persistence and correlation (§3.3).
- **Timeouts.** No general close timeout. A fatal Logout waits at most one
  heartbeat interval for the loopback (§3.3).
- **Replay stays the app's loop.** There's no store-level `replay(range)` (§4).
- **`send_raw`** is added to sequenced mode (§4).
- **`SequencedDriver`** is the main sequenced API, because your reactor
  builds on `SessionDriver` (§6).

## 1. What's wrong today

Recapped from the issues doc, with the code that causes each problem:

- **The core allocates outbound numbers.** `send()` strips any app-supplied
  `MsgSeqNum` (`session/state.rs:351`) and `transmit()` increments
  `next_out_seq_num` (`state.rs:323`). Heartbeats, TestRequests (`HELO-`
  after logon and after every replay), ResendRequests and Logouts all take
  numbers the app never chose.
- **"Persist before send" can't be achieved.** The app learns a message's
  seqno from `RawMessageSent`. In the tokio driver that event is only queued
  on a channel before the bytes are written (`babelfix-tokio/src/session.rs:342`).
  A crash after the write leaves the peer holding a number we've forgotten.
  The next Logon is then too low (fatal), and the trigger may be re-sent.
- **`SessionState` fires before `MessageReceived`** (`state.rs:587`), and for
  admin messages too. An app that persists it has claimed a message it never
  processed.
- **`Session` mixes stored state with configuration.** The seqnos sit next to
  the heartbeat, dictionary and precision.
- **The cost is visible.** fix-to-kafka and fixation both do a synchronous
  write per `SessionState`/`Raw*` event. That's roughly three writes per
  message, and it still isn't crash-safe.

## 2. Layering

```
               External mode                      Sequenced mode (default)
          ┌──────────────────────┐            ┌──────────────────────────┐
  app ───▶│ SessionState (core)  │    app ───▶│ Sequencer (core, sans-io)│
          │  - seqnos from app   │            │  - assigns seqnos        │
          │  - admin loopback    │◀── store   │  - persist requests      │◀── store
          └──────────┬───────────┘            │  - inbound watermark     │
                     │                        └────────────┬─────────────┘
                     ▼                                     ▼
          SessionDriver (core)                 SessionState / SessionDriver (core)
          codec + buffers + clock              unchanged, External underneath
                     │                                     │
          ┌──────────┴───────────┐            ┌────────────┴─────────────┐
          │ your reactor | tokio │            │ your reactor | tokio     │
          └──────────────────────┘            └──────────────────────────┘
```

**Rule:** every piece of babelfix-specific logic lives in `babelfix-core` as
sans-io. That covers seqno validation, admin request bookkeeping,
persistence ordering, watermark coalescing and replay. A driver (tokio, or
your own reactor) only does I/O:

- reads bytes;
- writes `pending_writes()`;
- arms a timer from `next_deadline()`;
- carries persist requests to a store, and feeds completions back.

Today the tokio driver reimplements part of `SessionDriver`: its own
`PendingOutput`, stamping and encoding. That gets folded back as part of
this work (§6).

The `Sequencer` goes in **`babelfix_core::sequencer`** rather than a new
crate:

- it needs only core types;
- `SequencedDriver` (§6) composes it with `SessionDriver`, which is also in
  core;
- a sixth crate would add release overhead and buy nothing.

## 3. Core changes (`babelfix_core::session`)

### 3.1 Configuration vs. state

```rust
/// Negotiated settings. Nothing here needs persisting.
#[derive(Clone)]
pub struct SessionConfig {
  pub heartbeat_interval: Duration,
  pub dict: Arc<Dictionary>,
  pub time_precision: TimePrecision,
}
```

The only state the core needs from the app at logon is `next_in_seq_num`.
It needs no `next_out` at all, because every outbound number arrives with
its message.

### 3.2 Outbound: the header is the contract

```rust
pub enum Command {
  /// Send `msg`. `MsgSeqNum(34)` is required.
  ///
  /// If `SendingTime(52)` is set, it is sent as-is. In External mode, set it
  /// to the time the message was committed (the event timestamp), so that
  /// recovery always knows it. If it is absent, it is stamped from the
  /// driver's clock at the wire, as today.
  ///
  /// The session always sets SenderCompID/TargetCompID, and strips
  /// PossDupFlag/OrigSendingTime.
  Send(Message),

  /// Retransmit `msg` in answer to a ResendRequest. `MsgSeqNum` is required.
  ///
  /// `OrigSendingTime(122)` is kept if present. Otherwise it is copied from
  /// `SendingTime`. If neither is present, that's an error: FSL §4.8 requires
  /// it. `PossDupFlag=Y` is set and a fresh `SendingTime` is stamped.
  /// Admin messages other than Reject and XMLnonFIX are gap-filled, as today.
  Replay(Message),

  ReplayComplete,
  Disconnect,
}
```

This is today's command set. The changes are that `Send` requires
`MsgSeqNum` and respects a supplied `SendingTime`.

Validation is deliberately thin:

- A `MsgSeqNum` at or below the highest already sent is rejected with an
  error, and the session is unaffected. Going backwards always gets you
  logged out by the peer, so it's caught here.
- **Forward jumps are allowed.** The peer will send a ResendRequest, and the
  app gap-fills or replays.
- Messages sent during a replay are held back until `ReplayComplete`, as
  they are today.

`Unstamped` stays, but only for messages without a `SendingTime`.

### 3.3 Admin requests

Whenever the session wants to send something, it hands the message to the
app:

```rust
Event::AdminSendRequired(Message)   // owned
```

This covers heartbeats, the TestRequest answer, the `HELO-` TestRequests,
the ResendRequest on a gap, Logout, and the Logon itself (§3.5).

The app owns everything from here:

- how the request is persisted (whole message, partly built message,
  placeholder);
- any changes it makes, such as Logon password encoding or extra tags, using
  its normal message pipeline;
- when it sends the message back with `Command::Send`, carrying a
  `MsgSeqNum`.

The core keeps no IDs and no copy of the message.

**Consequence.** `Event` is no longer purely borrowed. It's one owned
message per admin request, which is cheap next to the persisted write it
leads to. The tokio driver no longer clones it.

The core still needs two things, without IDs:

- **Not asking twice.** It keeps flags such as "heartbeat requested" and
  "answer to TestRequest X requested". These clear when the core sees a
  message of that type sent.
- **Recognising what the app sends back.** It reads the type of each
  outbound message:
  - an outbound Logon completes our side of the handshake;
  - an outbound Logout starts closing;
  - the `HELO-` TestRequest is matched by its `TestReqID`.

  **Contract: the app must not change `TestReqID`.**

**Contract: admin requests from a previous connection are never sent.** In
External mode an app that persists the whole message could restart and send
it. A stale Heartbeat is harmless, but:

- a stale Logout ends the session;
- a stale Logon mid-session violates the protocol;
- a stale ResendRequest re-pulls a range for nothing.

Such numbers are gap-filled when the peer asks for them, which replay does
for admin messages anyway. The core enforces part of this: a Logon outside
the handshake is rejected.

**Gap fills don't go through the app.** They reuse existing numbers, so they
go straight out.

**Fatal Logouts** (too-low MsgSeqNum, protocol errors) are requested like
any other admin message. The core moves to a closing state and ignores
further inbound traffic. It returns `Progress::Close` when the Logout is
sent, or after **one heartbeat interval** without it. Without that limit, a
dead store would leave the session stuck half-closed, because the core's own
missed-heartbeat Logout would wait on the same app round trip. Nothing else
gets a new timeout: as with Logon, heartbeat timeouts are enough.

### 3.4 Inbound

```rust
Event::MessageReceived { seq_num: u64, msg: &'a Message }   // the transaction id
Event::InboundAdvanced { next_in_seq_num: u64 }             // admin / gap fill consumed numbers
```

- `Event::SessionState` is removed. The core's `next_in` lives only in memory.
- `InboundAdvanced` is a hint, not a requirement. An external app can fold
  the latest value into its next write at no extra cost, so a quiet session
  doesn't face a large ResendRequest after a restart. The `Sequencer` uses it
  to mark admin traffic as handled.
- Inbound Reject(35=3) is delivered as `MessageReceived` (currently dropped;
  FIXME at `state.rs:612`).

### 3.5 Handshake

- **Initiator.** `InitiatorHandshake::start(id, config, next_in, …)` emits
  `ConnectionEstablished`, then `AdminSendRequired(Logon)`. The app's
  `Send(Logon)` puts it on the wire. The logon deadline covers the store
  write.
- **Acceptor.** `identify()` is unchanged. Then
  `accept(config, next_in, …)` emits `AdminSendRequired(Logon reply)`.
- **Validating the app's Logon.** The core checks that the HeartBtInt and
  BeginString the app sends back still agree with `SessionConfig`.
- A Logon whose number is ahead of what the peer expects is the normal case
  when events were committed while disconnected. The peer sends a
  ResendRequest, and the replay path handles it.

### 3.6 Replay

`ResendRequest { begin_seq_no, end_seq_no }` is unchanged. An open-ended
request still resolves to the highest number *sent*, which includes our
Logon.

- **Committed before the Logon, never sent.** These fall inside the range,
  so they are replayed with `PossDupFlag=Y`, or gap-filled.
- **Committed after the Logon.** These are above the range, so they go out
  as ordinary `Send`s once `ReplayComplete` has been sent.

### 3.7 Journal

The journal events are kept, but they are audit only:

- `RawMessageSent(&Message)` is the wire form, with `MsgSeqNum` in the
  header.
- `RawMessageReceived(&Message)`.

The docs will state that nothing in recovery depends on them.

## 4. The Sequencer (`babelfix_core::sequencer`)

The `Sequencer` is the message-centric policy, written as a sans-io state
machine on top of External mode. It stores no data itself. It asks for
writes and is told when they've completed.

```rust
pub struct Resume {
  /// Highest persisted outbound number + 1.
  pub next_out_seq_num: u64,
  /// The persisted inbound watermark.
  pub next_in_seq_num: u64,
}

pub enum InboundPolicy {
  /// The watermark advances when a message is delivered. At-most-once:
  /// a crash mid-processing loses the message. Fine for tools.
  OnDelivery,
  /// The watermark advances when the app calls `handled(seq_num)`.
  /// `Contiguous`: the lowest unhandled number (sequential apps, no
  /// idempotency needed). `Highest`: the highest handled (concurrent apps,
  /// which must be idempotent on the seqno).
  Explicit(WatermarkMode),
}

/// What the Sequencer asks the store to do.
pub enum Persist<'a> {
  /// The message exactly as it will go on the wire: MsgSeqNum and
  /// SendingTime are in the header.
  Outbound { seq_num: u64, msg: &'a Message },
  /// Coalesced: at most one in flight; the latest value wins.
  Watermark { next_in_seq_num: u64 },
}
```

How it behaves:

- **Assigning numbers.** `send(msg)` and every `AdminSendRequired` the
  Sequencer intercepts get the next `MsgSeqNum`.
  - `SendingTime` is kept if the app set it. Otherwise it's stamped from the
    driver's clock at assignment, before the write. The stored message is
    the message that's sent, so replay always knows OrigSendingTime.
  - The cost is that SendingTime lags the wire by the store's latency.
  - The Sequencer then emits `Persist::Outbound`.
- **Admin messages in sequenced mode.** The Sequencer intercepts
  `AdminSendRequired`, so admin traffic is numbered, persisted and sent
  automatically. An app that wants to customise admin messages (a Logon
  password, say) uses External mode. A sequenced-mode hook can be added
  later if it's ever needed.
- **`send_raw(msg)`** is for test tools such as fixation. The app supplies
  `MsgSeqNum`, and may supply PossDupFlag, OrigSendingTime, or a
  hand-built SequenceReset. The message is still persisted, then released
  in order. If the app jumps forward, the Sequencer's next number continues
  after the jump. Going backwards is rejected, as in the core.
- **Releasing to the wire.** `persisted(seq_num)` releases the *contiguous
  prefix* of completed writes, in order, as `Command::Send` to the session.
  A completion that arrives out of order waits.
- **Admin messages are persisted too.** They must be, because the next
  `next_out` is "highest persisted + 1". The app can store a placeholder
  instead of the body, since admin messages are always gap-filled on replay.
- **Inbound.** `InboundAdvanced` and the inbound policy move the in-memory
  watermark. When it moves and no watermark write is in flight, the
  Sequencer emits `Persist::Watermark`.
- **Store failure.** `persist_failed(seq_num)` ends the session: the
  transport is closed with no Logout, because a Logout would also need a
  write.
- **Bounded.** `max_in_flight` limits outstanding outbound writes. Beyond
  it, `send` returns `WouldBlock`, which the tokio driver turns into channel
  backpressure.
- **Replay** stays the app's loop, exactly as fixation does it today:
  ResendRequest, then read the store, then `Replay` for each, then
  `ReplayComplete`. Stored messages carry their SendingTime, so OrigSendingTime
  is automatic.

## 5. Usage

### 5.1 External mode, own reactor (core `SessionDriver`)

This is the 2-write order round trip. The reactor owns the event store,
which reports commits in stream order. A FIX event's seqno is its ordinal
position after the gateway's `SESSION_START_EVENT_TIMESTAMP`. Non-FIX events
take no number.

The sink can't call back into the driver while the driver is borrowed. So
the sink records what it wants, and the reactor applies it once the call
returns. That's how a reactor has to work today anyway.

```rust
use babelfix_core::driver::SessionDriver;
use babelfix_core::session::{Command, Event};
use babelfix_core::schema::fields::{MsgSeqNum, SendingTime};

struct Gateway {
  id: GatewayId,
  driver: Box<SessionDriver>,
  /// Admin messages requested on *this* connection and appended to the
  /// stream, waiting to commit. The app's own correlation; babelfix knows
  /// nothing of it. Lost on restart, which is the point: §3.3.
  pending_admin: HashMap<EventId, Message>,
}

impl Gateway {
  /// Write #1: order state + OrderCreateRequested, one transaction.
  fn on_send_order(&mut self, req: SendOrder, store: &mut EventStore) {
    store.append_txn(self.id, [order_state(&req), Ev::OrderCreateRequested(req)]);
  }

  /// The store says an event is durable. If it's a FIX event, it now has
  /// an implicit seqno: send it.
  fn on_committed(&mut self, ev: &Committed, now: Instant, sink: &mut Sink) -> Result<()> {
    let Some(seq_num) = ev.fix_seq_num else { return Ok(()) };
    let mut msg = match &ev.kind {
      Ev::FixAdminMsg(_) => match self.pending_admin.remove(&ev.id) {
        Some(msg) => msg,
        None => return Ok(()), // from a previous connection: never sent, gap-filled on replay
      },
      kind => to_fix(kind)?,
    };
    msg.header_mut().set(MsgSeqNum, seq_num).set(SendingTime, ev.timestamp);
    self.driver.on_command(now, Command::Send(msg), sink)?;
    Ok(())
  }

  fn on_session_event(&mut self, ev: Event<'_>, store: &mut EventStore, todo: &mut Vec<Command>) {
    match ev {
      Event::AdminSendRequired(mut msg) => {
        customise_admin(&mut msg); // e.g. Logon password encoding
        let eid = store.append(self.id, Ev::FixAdminMsg(msg.to_bytes()));
        self.pending_admin.insert(eid, msg);
      }
      // Write #2: OrderAcked + the inbound transaction id, one transaction.
      // On reconnect, next_in = the latest (or earliest-gap) committed in_seq + 1.
      Event::MessageReceived { seq_num, msg } => {
        store.append_txn_with_inbound(self.id, apply_exec_report(msg), seq_num);
      }
      Event::ResendRequest { begin_seq_no, end_seq_no, .. } => {
        for ev in store.fix_events(self.id, begin_seq_no..=end_seq_no) {
          if ev.is_admin() || is_stale(&ev) { continue } // skipped numbers are gap-filled
          let mut msg = to_fix(&ev.kind).unwrap();
          msg.header_mut().set(MsgSeqNum, ev.fix_seq_num).set(SendingTime, ev.timestamp);
          todo.push(Command::Replay(msg)); // SendingTime -> OrigSendingTime
        }
        todo.push(Command::ReplayComplete);
      }
      Event::RawMessageSent(_) | Event::RawMessageReceived(_) => journal_async(ev),
      _ => {}
    }
  }
}

// Connecting: no next_out anywhere. The Logon is just the next FIX_ADMIN_MSG.
let init = InitiatorDriver::start(session_id, config, next_in, driver_cfg, now, &mut sink)?;
```

Writes per round trip: #1 before the NewOrderSingle leaves, #2 before
"order accepted" is published.

### 5.2 Sequenced mode, own reactor (core `SequencedDriver`)

```rust
use babelfix_core::driver::SequencedDriver;
use babelfix_core::sequencer::{InboundPolicy, Persist, Resume, SeqEvent, WatermarkMode};

let mut init = SequencedDriver::initiate(
  session_id, config,
  Resume { next_out_seq_num: store.max_out()? + 1, next_in_seq_num: store.next_in()? },
  InboundPolicy::Explicit(WatermarkMode::Contiguous),
  driver_cfg, now, &mut sink,
)?;

// Sink:
match ev {
  SeqEvent::Persist(Persist::Outbound { seq_num, msg }) => {
    let bytes = msg.to_bytes();
    store.put_async(seq_num, bytes, move || reactor.post(Done::Out(seq_num)));
  }
  SeqEvent::Persist(Persist::Watermark { next_in_seq_num: n }) => {
    store.put_next_in_async(n, move || reactor.post(Done::Watermark(n)));
  }
  SeqEvent::Session(Event::MessageReceived { seq_num, msg }) => {
    process(msg); // then, once it's safe:
    todo.push(Todo::Handled(seq_num));
  }
  SeqEvent::Session(Event::ResendRequest { begin_seq_no, end_seq_no, .. }) => {
    todo.push(Todo::ReplayFrom(begin_seq_no, end_seq_no)); // read the store, Command::Replay each
  }
  SeqEvent::Session(_) => {}
}

// Reactor:
let seq = driver.send(now, msg, &mut sink)?;              // -> Persist::Outbound
Done::Out(seq)       => driver.persisted(now, seq, &mut sink)?,   // bytes appear in pending_writes()
Done::Watermark(n)   => driver.watermark_persisted(n, &mut sink)?,
Todo::Handled(seq)   => driver.handled(seq, &mut sink)?,
```

### 5.3 Sequenced mode, tokio (the default)

The tokio glue holds no ordering logic. It runs each persist future, in
order, with up to `max_in_flight` outstanding, and feeds each completion to
`SequencedDriver`.

```rust
use babelfix::session::{SessionStore, Sequencing, Resume, InboundPolicy};

struct Store { db: sturdb::Environment, session: Uuid }

impl SessionStore for Store {
  fn persist_outbound(&self, seq_num: u64, wire: Bytes)
    -> impl Future<Output = babelfix::Result<()>> + Send { async move { /* insert row */ Ok(()) } }
  fn persist_watermark(&self, next_in_seq_num: u64)
    -> impl Future<Output = babelfix::Result<()>> + Send { async move { /* update session */ Ok(()) } }
}

let initiator = endpoint::connect(
  addrs, dicts, session_id, config,
  Sequencing::sequenced(store, Resume { next_out_seq_num, next_in_seq_num }, InboundPolicy::OnDelivery),
  EndpointConfig::default(),
)?;

handle.tx.send(SessionCommand::Send(msg)).await?;     // numbered, persisted, then sent
handle.tx.send(SessionCommand::SendRaw(msg)).await?;  // app-chosen MsgSeqNum, still persisted
while let Some(ev) = handle.events.next().await {
  match ev {
    SessionEvent::MessageReceived { seq_num, msg } => { /* … */ }
    SessionEvent::ResendRequest { begin_seq_no, end_seq_no, .. } => { /* replay from store */ }
    _ => {}
  }
}
```

For the acceptor, `EndpointEvent::NewSession` is answered with
`(SessionConfig, Sequencing)`.

### 5.4 External mode, tokio

```rust
let initiator = endpoint::connect(addrs, dicts, session_id, config,
  Sequencing::External { next_in_seq_num }, EndpointConfig::default())?;

SessionEvent::AdminSendRequired(msg) => { /* append FIX_ADMIN_MSG; on commit, set MsgSeqNum and: */
  handle.tx.send(SessionCommand::Send(msg)).await? }
```

## 6. Driver work

- **`SessionDriver`** (core) takes External-mode commands and events.
  `InitiatorDriver`/`AcceptorDriver` go through the Logon admin request.
- **New `SequencedDriver`** (core) is the main sequenced API, because that's
  what your reactor builds on. It holds a `SessionDriver` plus a
  `Sequencer`. It:
  - intercepts `AdminSendRequired` and `InboundAdvanced`;
  - presents `SeqEvent` to the app;
  - offers `send`, `send_raw`, `persisted`, `persist_failed`, `handled` and
    `watermark_persisted`.

  The bare `Sequencer` pieces (`assign`, `intercept(event)`,
  `persisted -> impl Iterator<Item = Command>`) stay public for drivers
  built on `SessionState`.
- **The tokio session runner** is rebuilt on these two driver types, instead
  of `SessionState` plus its own `PendingOutput` encoding and stamping. What
  stays tokio-specific: the socket, `sleep_until(next_deadline)`, the
  channels, and the `SessionStore` futures. If something can't be written
  that way, that's a sign the logic belongs in core.

## 7. Crash walkthroughs

| Crash point | External | Sequenced |
|---|---|---|
| Number assigned, write not durable | No event exists, so nothing was sent. The number is free. | Not persisted, so nothing was sent. `next_out` = highest persisted + 1 reuses it safely. |
| Durable, not yet sent | Logon is ahead of the peer. The peer sends a ResendRequest; replay sends it with PossDup (OrigSendingTime = event ts), or gap-fills a stale one. | Same, replayed from the store. |
| Sent, nothing else recorded | The peer has it; our Logon is ≥ next. Correct by construction. | Same. |
| Admin message durable, not sent | Never sent after restart (§3.3); gap-filled on request. | Same; the Sequencer never re-sends stored admin messages. |
| Inbound received, effects not committed | `next_in` comes from the last committed `in_seq`, so the peer resends with PossDup, and the message is processed once. | `Explicit`: the watermark wasn't advanced, so it's re-requested (`Highest` needs idempotency). `OnDelivery`: lost (by choice). |
| Inbound handled, watermark write not durable | n/a: written in the same transaction as the effects. | Re-requested. Sequential apps are already past it by `Contiguous`; concurrent apps dedup on seqno. |

A deterministic harness in core will cover every row for both modes. It
uses a simulated store with crash injection between each write and send,
and asserts:

- no too-low Logon;
- no duplicated trigger;
- no message lost (except under `OnDelivery`).

## 8. Validation against fixation

Fixation is a deliberately unsophisticated app: a GUI tester using the tokio
initiator and an async `sturdb` store. It maps onto Sequenced mode with
`OnDelivery`:

| Fixation today | Under this proposal |
|---|---|
| `Messages` row per `RawMessageSent`, written *after* the send, plus a `Session` next_in/out update per event | `persist_outbound(seq, wire)` inserts the same row, *before* the send, and sets `Session.next_out_seq_num = seq + 1` in the same transaction. The crash bug goes away; the latency is invisible in a GUI. |
| `Messages` row per `RawMessageReceived`, plus next_in | Journal row from `RawMessageReceived` (async). `persist_watermark(n)` updates `Session.next_in_seq_num` (coalesced, so fewer writes than today). |
| `SessionState` handler (persist + repaint) | Removed. Repaint on journal events. Live values via `GetSessionState`, which now reports the Sequencer's next_out/watermark. |
| `handle_replay`: range query on `BySessionSeqNum` → `Replay`/`ReplayComplete` | Unchanged. Stored messages carry their SendingTime, so OrigSendingTime is now exact. |
| Reset: new `Session` record (uuid v7 + `reset_timestamp`), 1/1 | Unchanged, and still entirely the app's job. `reset_timestamp` already acts like `SESSION_START_EVENT_TIMESTAMP`. |
| UI override of in/out seqnos | Unchanged: they become the `Resume` values. |
| Admin messages shown in the message list | They arrive as `Persist::Outbound` like everything else, so they're in `Messages` before they're sent. |
| Composing a message in the UI | `Send` as normal. A future "send with this seqno / PossDup" option uses `SendRaw`. |

## 9. Steps

1. **Core, outbound.** Add `SessionConfig`; `Send` requires `MsgSeqNum` and
   respects `SendingTime`; `Replay` respects `OrigSendingTime`; add
   `AdminSendRequired(Message)` with "requested" flags, the backwards-seqno
   check, outbound type recognition, and the fatal-Logout limit of one
   heartbeat interval.
2. **Core, inbound.** Remove `SessionState`; add
   `MessageReceived { seq_num }`, `InboundAdvanced`, and deliver Reject(3).
3. **Core, handshake.** The Logon goes through the admin request on both
   sides; check the app's Logon against `SessionConfig`.
4. **Core, `Sequencer` + `SequencedDriver`.** Includes `send_raw`, inbound
   policies, watermark coalescing, `max_in_flight` and failure handling. Existing session tests run through
   `SequencedDriver` with an immediate in-memory store, so they stay
   readable.
5. **Crash harness** (§7), for both modes.
6. **Tokio.** Rebuild the runner on the core drivers; add `Sequencing`,
   `SessionStore`, and the new `SessionCommand`/`SessionEvent` variants.
7. **Examples.** Move fix-to-kafka to Sequenced mode. Add a new
   `examples/event-centric`: the order round trip on an in-memory event log
   with simulated store latency, asserting exactly 2 writes.
8. **Port fixation** and record the result in `MIGRATION.md`.

## 10. Settled

- **Customising admin messages.** Only External mode supports it for now.
  A sequenced-mode hook would be YAGNI until something needs it.
- **Owned admin messages in `Event`.** Accepted: `AdminSendRequired` is the
  reverse of `Send`. The "every payload is borrowed" comment on `Event`
  (`session/mod.rs`) is narrative, not a design rule, and is updated in
  step 1.

## 11. Implementation notes

Where the implementation (branch `session-app-owned-seqnums`) departs from the
above:

- **No `SequencedDriver` type.** The `Sequencer` works with every stage of a
  session through a `CommandTarget` trait, implemented by `InitiatorDriver`,
  `EstablishedDriver` and `SessionDriver`. A separate driver type would have
  doubled the handshake types for nothing.
  - `sequencer.sink(&mut app)` is the event sink to hand any driver.
  - `sequencer.persisted(seq_num, now, &mut driver, &mut app)` releases
    completed writes, in order.
- **`Sequencer::new` takes the session identity and config.** It stamps the
  CompIDs as well as `MsgSeqNum` and `SendingTime`, so the stored message is
  byte-for-byte what is sent.
- **`Sequencer::send` strips `PossDupFlag`/`OrigSendingTime`.** A new message
  under a new number cannot be a possible duplicate. `send_raw` keeps
  everything; the core's `Command::Send` keeps everything too.
- **Messages persisted before logon.** These go out with the synchronisation
  TestRequest that logging on always asks for, ahead of it.
- **New `Event::LoggedOn`.** It marks both Logons having been exchanged, and
  is what releases application messages in sequenced mode.
- **Heartbeat timer.** It is still deferred only by application messages, as
  before. An admin message the application sends back (a Heartbeat answering
  a TestRequest, say) does not reset it.
- **Tokio exposes a `SessionSetup`, not a `Sequencing` enum.**
  - It holds `SessionConfig`, `Resume`, `InboundPolicy` and
    `Arc<dyn SessionStore>`.
  - `SessionStore` returns boxed `'static` futures, so it is object-safe and
    several writes can be in flight without borrowing it.
  - `VolatileStore` persists nothing and is the default.
  - External (event-centric) mode is not exposed through tokio yet: use the
    core directly.
- **Writes and ordering.** Writes a store completes at once are fed back in
  the same pass, so `VolatileStore` sends with no extra loop iteration.
  Admin answers to a batch of inbound frames go out after the whole batch,
  not between its frames.

Done: steps 1–4 and 6, with fix-to-kafka ported to a kv-backed
`SessionStore` (the rest of step 7). Not yet done:

- the crash-injection harness (step 5);
- the event-centric example (step 7);
- porting fixation (step 8);
- External mode in tokio.
