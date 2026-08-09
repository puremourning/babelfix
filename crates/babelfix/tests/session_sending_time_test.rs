//! `SendingTime` precision and per-message stamping.
//!
//! Two properties are load-bearing and easy to regress silently:
//!
//! * The field carries the precision the session was configured with. A
//!   counterparty that expects nanoseconds and receives milliseconds is not
//!   getting the timestamps it is paying for.
//! * Each message gets its own clock read. A single pass over the session can
//!   emit several messages — a gap fill and the retransmit after it, or a
//!   replay queue being drained — and stamping them all from one timestamp
//!   would make the field useless for ordering.
//!
//! These are asserted against what actually reaches the wire, via [`RawPeer`],
//! rather than against anything the session reports about itself.

// The shared `session` harness defines the `expect_event(s)` macros; these
// tests assert on raw wire bytes instead, so they go unused here.
#![allow(dead_code, unused_imports, unused_macros, unused_variables)]
use babelfix as fix;
use std::sync::Arc;

use googletest::prelude::*;

mod matchers;
mod session;

use fix::schema::FIX_Latest::Fields;
use fix::time::TimePrecision;
use matchers::*;
use session::raw::{RawMessage, RawPeer};
use session::{FIX_REPO, SessionOptions};

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

fn order(cl_ord_id: &str) -> anyhow::Result<fix::message::builder::Message> {
  let mut msg = fix::message::builder::Message::new(fix44(), "D")?;
  msg.body.set_tag(Fields::ClOrdID, cl_ord_id);
  msg.body.set_tag(Fields::Symbol, "AAPL");
  msg.body.set_tag(Fields::Side, "1");
  msg.body.set_tag(Fields::OrderQty, 100i64);
  Ok(msg)
}

/// Pull `SendingTime` off a message as it appeared on the wire.
fn sending_time(msg: &fix::FixMessage) -> String {
  msg
    .get_tag(Fields::SendingTime)
    .expect("every outbound message carries SendingTime")
    .to_string(&msg.data)
}

/// `YYYYMMDD-HH:MM:SS.` is 18 characters; the rest is the fraction.
fn fractional_digits(stamp: &str) -> usize {
  stamp.len() - 18
}

/// The default is nanoseconds, and it reaches the wire.
#[test_log::test(tokio::test)]
async fn sending_time_defaults_to_nanosecond_precision() -> anyhow::Result<()> {
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer =
    RawPeer::connect_and_logon(port, fix44(), "CLIENT", "SERVER").await?;
  session::wait_for_session(&server, &server_session_id).await?;

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(order("order-1")?))
    .await?;

  let received = peer.recv().await?;
  let stamp = sending_time(&received);

  assert_eq!(
    fractional_digits(&stamp),
    TimePrecision::Nanos.digits(),
    "expected nanosecond SendingTime, got {stamp:?}"
  );
  assert_eq!(stamp.len(), TimePrecision::Nanos.width());

  Ok(())
}

/// A session configured for milliseconds emits milliseconds — the knob is real,
/// not decorative. Counterparties do reject over-precise timestamps.
#[test_log::test(tokio::test)]
async fn sending_time_precision_is_configurable() -> anyhow::Result<()> {
  let options = SessionOptions {
    time_precision: TimePrecision::Millis,
    ..SessionOptions::default()
  };
  let (server_session_id, server, port) =
    session::serve("SERVER", options, "CLIENT", fix44()).await?;

  let mut peer =
    RawPeer::connect_and_logon(port, fix44(), "CLIENT", "SERVER").await?;
  session::wait_for_session(&server, &server_session_id).await?;

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(order("order-1")?))
    .await?;

  let received = peer.recv().await?;
  let stamp = sending_time(&received);

  assert_eq!(
    fractional_digits(&stamp),
    TimePrecision::Millis.digits(),
    "expected millisecond SendingTime, got {stamp:?}"
  );

  Ok(())
}

/// Every message gets its own clock read, including messages the session emits
/// back to back without returning to its caller.
///
/// This is the property that stops the timestamp being useful only per-batch,
/// and it is the reason the clock is read inside the send path rather than
/// handed in once per pass over the state machine.
///
/// The Logon acknowledgement and the synchronisation TestRequest that follows
/// it are the cleanest pair to assert on: the session emits both in immediate
/// succession, with no application involvement in between, so a per-batch clock
/// read would give them identical timestamps. The logon is driven by hand here
/// because `connect_and_logon` consumes both messages.
#[test_log::test(tokio::test)]
async fn each_message_is_stamped_individually() -> anyhow::Result<()> {
  let (_server_session_id, _server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER").await?;
  peer.logon(std::time::Duration::from_secs(30)).await?;

  let logon_ack = peer.recv().await?;
  anyhow::ensure!(logon_ack.get_type() == "A", "expected a Logon ack");
  let test_request = peer.recv().await?;
  anyhow::ensure!(test_request.get_type() == "1", "expected a TestRequest");

  let first = sending_time(&logon_ack);
  let second = sending_time(&test_request);

  assert_ne!(
    first, second,
    "two messages emitted in one pass shared a SendingTime; the clock is \
     being read per batch rather than per message"
  );
  // Monotonic as well as distinct — a later message must not be stamped
  // earlier than one already on the wire.
  assert!(
    second >= first,
    "SendingTime went backwards: {first:?} then {second:?}"
  );

  Ok(())
}
