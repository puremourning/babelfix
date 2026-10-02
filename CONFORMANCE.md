# FIX session layer conformance

Known deviations of the babelfix session layer from the *FIX Session Layer
Technical Specification* (Errata November 2020), cross-referenced against the
*FIX Session Layer Test Cases* (June 2020).

References are written `S§4.5.2` for a specification section and `TC 10a` for a
test case scenario. **M** marks a requirement the specification states as
*must*/*shall* or the test cases document marks Mandatory.

Behaviour that *is* implemented and conformant is covered by the integration
tests in `crates/babelfix/tests/session_*_test.rs` and the sans-io tests in
`crates/babelfix-core/tests/`; each test's doc comment states the rule it pins
down.

Source references name a file rather than a line: the protocol lives in
`babelfix-core` and the transport in `babelfix-tokio`, and line numbers in a
document rot on the next edit.

## Scope

The following are deliberately not implemented and are not listed as
deviations:

| Feature | Reference |
|---|---|
| `NextExpectedMsgSeqNum(789)` synchronisation | S§4.4.1, S§4.7.1 |
| `ResetSeqNumFlag(141)` session reset | S§4.4.2, S§4.4.3 |
| LFIXT session profile | S§5.4 |
| Application layer encryption | S§4.3.3, TC 17 |
| Third party addressing | S§6.2, TC 18 |
| `XMLnonFIX(35=n)` | S§9.8 |
| `TestMessageIndicator(464)` validation | S§4.3.2 |
| `MaxMessageSize(383)` negotiation | S§4.3.6 |

## Deviations

### 1. SequenceReset-Reset is unsupported — **M**

*S§4.8.6, S§4.8.8 Table 3, TC 11.* `SessionState::gap_fill_new_seq_num`
(`babelfix-core/src/session/state.rs`) requires `GapFillFlag(123)` to be present
and equal to `Y`. An absent flag and `123=N` both produce
`ProtocolViolation("Sequence reset message is garbage and not supported")`,
which terminates the session.

`GapFillFlag` is an optional field whose default is Reset, and Reset is
mandatory to support: accept the message without regard to its `MsgSeqNum`;
`NewSeqNo > NextNumIn` sets `NextNumIn`; `NewSeqNo = NextNumIn` is accepted with
a warning; `NewSeqNo < NextNumIn` is rejected with `SessionRejectReason(373)` of
5 and leaves `NextNumIn` unchanged.

### 2. Garbled messages terminate the connection — **M**

*S§4.5.2, TC 2d, 2m, 3b, 3c, 3e.* `FixDecoder::decode`
(`babelfix-core/src/codec.rs`) returns `Err` for an incorrect checksum or an
unparseable frame (`babelfix-core/src/message/parse.rs`), and the driver
(`babelfix-core/src/driver.rs`) treats a decoder error as fatal.

A garbled message is presumed to be a transmission error rather than a peer
defect. The specification requires disregarding it, **not** incrementing
`NextNumIn`, and continuing to accept messages; the next valid message then
appears as a sequence gap and is recovered normally. This covers an incorrect
`CheckSum(10)`, an incorrect `BodyLength(9)`, and `BeginString(8)`,
`BodyLength(9)` or `MsgType(35)` not appearing as the first three fields.

Note that recovering in place also requires the framing to resynchronise on the
next message boundary, which the current codec does not attempt.

### 3. `Reject(35=3)` is never generated — **M**

*S§4.5.4 and many test cases.* No `Reject` is constructed anywhere in the
crate. The parser does classify its errors — a `ParseError` carries the
`SessionRejectReason(373)` and tag a Reject would need, and
`Message::validate_strict` checks reasons 13 and 15 on request — but the
session never sends one. Every mandatory session level rejection is therefore
missing, including:

| Condition | `SessionRejectReason(373)` | Reference |
|---|---|---|
| Required tag missing | 1 | TC 14b |
| `PossDupFlag=Y` without `OrigSendingTime(122)` | 1 | TC 2g |
| Value out of range | 5 | TC 14e |
| Gap fill lowering the sequence number | 5 | TC 10e |
| SequenceReset-Reset lowering the sequence number | 5 | TC 11c |
| CompID problem | 9 | TC 2k |
| `SendingTime(52)` accuracy | 10 | TC 2o, 2f |
| Invalid `MsgType(35)` | 11 | TC 2q |
| Tag appears more than once | 13 | TC 14h |
| Tag out of required order | 14 | TC 14g |
| Repeating group problems | 15, 16 | TC 14i, 14j |

Where the specification calls for a `Reject`, the current implementation either
ignores the condition or terminates the session: a parse error (reasons 14 and
16, for example) is fatal, as in item 2, and the duplicate and group order
rules (13, 15) are not checked at all unless the application calls
`validate_strict`. Terminating is the more damaging of the two: a `Reject`
leaves the session usable.

### 4. `PossDupFlag(43)` is not consulted on an inbound message — **M**

*S§4.8.7, TC 2e.* `handle_session_message`
(`babelfix-core/src/session/state.rs`) terminates the connection with a Logout
whenever `MsgSeqNum` is below `NextNumIn`, without checking `PossDupFlag`. A
legitimately retransmitted message that has already been processed should be
ignored, and the session should continue.

More generally there is no inbound possible-duplicate handling at all: no
duplicate suppression, and no validation that `OrigSendingTime(122)` is present
and no later than `SendingTime(52)` (TC 2f, 2g).

### 5. Inbound `Reject` is discarded — **M**

*S§4.5.4, TC 7.* `dispatch_message`'s catch-all admin arm
(`babelfix-core/src/session/state.rs`) ignores a `Reject(35=3)`. Sequence
number handling is correct — the message is counted and the session continues
— but the application is only told through `Event::RawMessageReceived`, which
is meant for auditing, not through an event of its own.

`BusinessMessageReject(35=j)` is an application message
(`Message::is_admin`, `babelfix-core/src/message/view.rs`) and is delivered
as `Event::MessageReceived`.

### 6. Simultaneous resend requests terminate the session — **M**

*TC 20.* Receiving a `ResendRequest` while one is already being serviced
produces `ProtocolViolation("ResendRequest while a resend is already in
progress")` (`babelfix-core/src/session/state.rs`). The specification requires
performing the requested resend and then re-requesting anything still missing.

Note this is distinct from a `ResendRequest` arriving inside a sequence gap,
which is handled.

### 7. A missing `MsgSeqNum` terminates the session abruptly

*S§4.5.3.* `handle_session_message`
(`babelfix-core/src/session/state.rs`) returns an error ("tag 34 is
missing"), dropping the transport layer connection. The specification asks for
a Logout naming the missing field, since this indicates a defect that will only
be resolved by changing software.

### 8. Message-level identity and timestamp validation is absent — **M**

*S§4.2.2, S§4.2.3, S§4.5.2, TC 2i, 2k, 2n, 2o.* Marked by the `TODO` at
`babelfix-core/src/session/state.rs`. Specifically:

* `SenderCompID(49)` and `TargetCompID(56)` are not checked against the session
  on a per-message basis. A discrepancy should produce a `Reject` with
  `SessionRejectReason(373)` of 9 followed by a Logout.
* `SendingTime(52)` is not validated. There is no SendingTimeThreshold concept,
  so a message from a peer with a badly skewed clock is accepted.
* `BeginString(8)` is validated per message, but a mismatch is handled as a
  garbled message — fatal, as in item 2 — rather than with the Logout the
  specification asks for. The decoder (`babelfix-core/src/codec.rs`) latches
  the version of the first message and `Message::parse` rejects a later one
  whose `BeginString` differs.

### 9. The Logout initiator does not wait for the acknowledgement

*S§4.6, S§4.6.1, TC 12.* The messages are correct — a Logout is sent before the
transport layer is closed, and an inbound Logout is acknowledged exactly once —
but there is no Logout Pending state and no LogoutAckThreshold timer. The
initiator closes the connection immediately after transmitting, so a peer that
wanted to gap fill outstanding messages before agreeing to the logout has no
opportunity to do so.

The heartbeat timeout Logout (`babelfix-core/src/session/state.rs`) closes
immediately for the same reason.

A related edge: `Command::Disconnect` routes its Logout through
`SessionState::send`, which queues outbound messages while a resend is in
progress. Disconnecting mid-resend therefore queues the Logout and then breaks
out of the loop, so nothing is transmitted.

### 10. `HeartBtInt(108)` is not negotiated — **M**

*S§4.3.4, S§4.3.5.* Each peer uses its own configured interval. The value in
the inbound Logon is never read, and the acceptor echoes its own value rather
than the initiator's (`logon_message`, `babelfix-core/src/session/handshake.rs`). The specification
requires both peers to use the same value within a connection.

`HeartBtInt=0`, meaning heartbeats are disabled, is not handled.

Related, from *S§4.5.5*: the TestRequestThreshold is fixed at two heartbeat
intervals and dead peer detection at three
(`babelfix-core/src/session/state.rs`), with no configuration. The
specification recommends a threshold between 1.2 and 2.0 intervals, agreed
out of band.

### 11. Smaller items

* `DefaultApplVerID(1137)` is hardcoded to `"10"` on outbound Logons
  (`logon_message`, `babelfix-core/src/session/handshake.rs`), and
  `EncryptMethod(98)` on an inbound Logon is not validated.
* Gap fills do not carry `PossDupFlag=Y`. *S§4.8.4* requires it on any message
  sent in response to a `ResendRequest`. Receivers do not generally depend on
  it, but most engines set it.
