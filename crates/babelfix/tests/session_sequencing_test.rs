//! Sequence number handling and application message relay.

#![allow(dead_code, unused_imports, unused_variables)]
use babelfix as fix;
use std::sync::Arc;
use std::time::Duration;

use googletest::prelude::*;

mod matchers;
mod session;

use fix::schema::tags as Fields;
use matchers::*;
use session::raw::{RawMessage, RawPeer};
use session::{DICTS, SessionOptions, expect_event, expect_events};

fn fix44() -> Arc<fix::message::Dictionary> {
  DICTS.get("FIX.4.4").unwrap().clone()
}

fn order(cl_ord_id: &str) -> anyhow::Result<fix::message::Message> {
  use fix::schema::{codesets, fields::*};
  let mut msg = fix::message::Message::new(&fix44(), "D");
  msg
    .body_mut()
    .set(ClOrdID, cl_ord_id)
    .set(Symbol, "AAPL")
    .set(Side, codesets::Side::Buy)
    .set(OrderQty, 100u64);
  Ok(msg)
}

/// An application message crosses the session unchanged in both directions,
/// repeating groups included, and is delivered only to the application-facing
/// event.
#[test_log::test(tokio::test)]
async fn application_messages_round_trip() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      SessionOptions::default(),
      "SERVER",
      SessionOptions::default(),
      fix44(),
    )
    .await?;

  client.session.settle().await?;
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .settle()
    .await?;

  let mut msg = order("order-1")?;
  for account in ["ACCT-A", "ACCT-B"] {
    msg
      .body_mut()
      .group_mut(Fields::NoAllocs)
      .push()
      .set(fix::schema::fields::AllocAccount, account);
  }
  client
    .session
    .command(fix::session::SessionCommand::Send(msg))
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived { msg: block::body(all!(
            block::tag(Fields::ClOrdID, value::string(eq("order-1"))),
            block::tag(Fields::OrderQty, value::float(eq(100.0))),
            block::group(Fields::NoAllocs, 0, block::tag(
              Fields::AllocAccount, value::string(eq("ACCT-A")))),
            block::group(Fields::NoAllocs, 1, block::tag(
              Fields::AllocAccount, value::string(eq("ACCT-B")))),
          )), .. } };
  };

  // And back the other way.
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(order("order-2")?))
    .await?;

  expect_events! {
    { client awaiting
      << fix::session::SessionEvent::MessageReceived { msg: block::body(block::tag(
            Fields::ClOrdID, value::string(eq("order-2")))), .. } };
  };

  Ok(())
}

/// The session owns the header fields an application cannot set correctly.
/// Whatever the application supplies for the sequence number or CompIDs is
/// replaced, and a new message cannot be a possible duplicate, so a stale or
/// hand-rolled message cannot corrupt the session. A `SendingTime` the
/// application supplies is its to choose, and is kept.
#[test_log::test(tokio::test)]
async fn session_owns_the_sequence_and_identity_header_fields()
-> anyhow::Result<()> {
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
    .settle()
    .await?;

  let mut msg = order("order-1")?;
  msg
    .header_mut()
    .set_raw(Fields::MsgSeqNum, b"999")
    .set_raw(Fields::PossDupFlag, b"Y")
    .set_raw(Fields::SenderCompID, b"BOGUS")
    .set_raw(Fields::TargetCompID, b"BOGUS")
    .set_raw(Fields::SendingTime, b"19700101-00:00:00.000");

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(msg))
    .await?;

  let received = peer.recv().await?;
  verify_that!(
    &received,
    all!(
      // 1 was the Logon acknowledgement, 2 the synchronisation TestRequest.
      message::tag(Fields::MsgSeqNum, eq("3")),
      message::tag(Fields::SenderCompID, eq("SERVER")),
      message::tag(Fields::TargetCompID, eq("CLIENT")),
      message::tag(Fields::SendingTime, eq("19700101-00:00:00.000")),
      not(message::has_tag(Fields::PossDupFlag)),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A gap in the inbound sequence produces one open-ended ResendRequest.
/// Messages arriving beyond the gap while recovery is outstanding are
/// discarded rather than triggering further requests — they fall inside the
/// range already asked for, and the peer will retransmit them.
#[test_log::test(tokio::test)]
async fn a_sequence_gap_produces_exactly_one_resend_request()
-> anyhow::Result<()> {
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
    .settle()
    .await?;

  // The acceptor expects 3 next; skip ahead to 6.
  peer
    .send(
      RawMessage::new("D")
        .seq(6)
        .body(Fields::ClOrdID, "skipped-1"),
    )
    .await?;

  let resend_request = peer.recv().await?;
  verify_that!(
    &resend_request,
    all!(
      message::tag(Fields::MsgType, eq("2")),
      message::tag(Fields::BeginSeqNo, eq("3")),
      message::tag(Fields::EndSeqNo, eq("0")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  // Further messages beyond the gap do not provoke a second request.
  peer
    .send(
      RawMessage::new("D")
        .seq(7)
        .body(Fields::ClOrdID, "skipped-2"),
    )
    .await?;
  peer.expect_silence(Duration::from_millis(200)).await?;

  // Closing the gap lets the session resume, and the message that closes it is
  // delivered.
  peer
    .send(
      RawMessage::new("4")
        .seq(3)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "8"),
    )
    .await?;
  peer
    .send(
      RawMessage::new("D")
        .seq(8)
        .body(Fields::ClOrdID, "after-gap"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived { msg: block::body(block::tag(
            Fields::ClOrdID, value::string(eq("after-gap")))), .. } };
  };

  Ok(())
}

/// Recovery is not a one-shot: once a gap has been closed, a later gap
/// produces a fresh ResendRequest.
#[test_log::test(tokio::test)]
async fn a_later_gap_produces_another_resend_request() -> anyhow::Result<()> {
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
    .settle()
    .await?;

  peer
    .send(
      RawMessage::new("D")
        .seq(6)
        .body(Fields::ClOrdID, "first-gap"),
    )
    .await?;
  let first = peer.recv().await?;
  verify_that!(&first, message::tag(Fields::BeginSeqNo, eq("3")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  peer
    .send(
      RawMessage::new("4")
        .seq(3)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "7"),
    )
    .await?;

  // Caught up at 7; skip ahead again.
  peer
    .send(
      RawMessage::new("D")
        .seq(11)
        .body(Fields::ClOrdID, "second-gap"),
    )
    .await?;
  let second = peer.recv().await?;
  verify_that!(
    &second,
    all!(
      message::tag(Fields::MsgType, eq("2")),
      message::tag(Fields::BeginSeqNo, eq("7")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A session-level Reject is about a message we sent, so unlike other session
/// messages it reaches the application — in sequence, like any other.
#[test_log::test(tokio::test)]
async fn a_session_level_reject_is_delivered_to_the_application()
-> anyhow::Result<()> {
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
    .settle()
    .await?;

  peer
    .send(
      RawMessage::new("3")
        .body(Fields::RefSeqNum, "1")
        .body(Fields::SessionRejectReason, "5")
        .body(Fields::Text, "something was wrong"),
    )
    .await?;
  peer
    .send(RawMessage::new("D").body(Fields::ClOrdID, "after-reject"))
    .await?;

  // The Reject is about something we sent, so it is passed up, in sequence;
  // the next application message is delivered as normal.
  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived { msg: block::body(block::tag(
            Fields::ClOrdID, value::string(eq("after-reject")))), .. } };
  };

  let events = {
    let mut guard = server.lock().await;
    guard.session(&server_session_id).unwrap().events.clone()
  };
  let delivered = events
    .iter()
    .filter(|e| matches!(e, fix::session::SessionEvent::MessageReceived { .. }))
    .count();
  anyhow::ensure!(
    delivered == 2,
    "Expected the Reject and the application message to be delivered, got \
     {delivered}"
  );

  Ok(())
}

/// PossResend is an application layer concern — the session must pass it
/// through so the application can decide whether it has seen the message
/// before.
#[test_log::test(tokio::test)]
async fn poss_resend_is_passed_through_to_the_application() -> anyhow::Result<()>
{
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
    .settle()
    .await?;

  peer
    .send(
      RawMessage::new("D")
        .header(Fields::PossResend, "Y")
        .body(Fields::ClOrdID, "maybe-seen-before"),
    )
    .await?;

  let event = server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .next_event_matching(&matches_pattern!(
      &fix::session::SessionEvent::MessageReceived { msg: ref anything(), .. }
    ))
    .await?;
  let fix::session::SessionEvent::MessageReceived { msg, .. } = event else {
    anyhow::bail!("expected an application message");
  };

  verify_that!(
    &msg,
    all!(
      // Boolean-valued fields whose Orchestra type is a code set rather than
      // the primitive Boolean arrive as strings.
      block::header(block::tag(Fields::PossResend, value::string(eq("Y")))),
      block::body(block::tag(
        Fields::ClOrdID,
        value::string(eq("maybe-seen-before"))
      )),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// The event stream an application observes must be usable without knowing the
/// session protocol: a connection is announced once, every message on the wire
/// is reported exactly once in the direction it travelled, the sequence numbers
/// carried alongside never go backwards, application messages only arrive after
/// recovery has completed, and the stream ends with a single disconnection.
#[test_log::test(tokio::test)]
async fn event_stream_is_well_formed() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      SessionOptions::default(),
      "SERVER",
      SessionOptions::default(),
      fix44(),
    )
    .await?;

  client.session.settle().await?;
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .settle()
    .await?;

  client
    .session
    .command(fix::session::SessionCommand::Send(order("order-1")?))
    .await?;
  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived { msg: anything(), .. } };
  };

  client
    .session
    .command(fix::session::SessionCommand::Disconnect)
    .await?;
  expect_events! {
    { client awaiting << fix::session::SessionEvent::Disconnected };
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  let client_events: Vec<_> = client.session.events.iter().cloned().collect();
  let server_events: Vec<_> = {
    let mut guard = server.lock().await;
    guard
      .session(&server_session_id)
      .unwrap()
      .events
      .iter()
      .cloned()
      .collect()
  };

  for (side, events) in [("client", &client_events), ("server", &server_events)]
  {
    check_event_stream(side, events)?;
  }

  Ok(())
}

fn check_event_stream(
  side: &str,
  events: &[fix::session::SessionEvent],
) -> anyhow::Result<()> {
  use fix::session::SessionEvent as E;

  anyhow::ensure!(
    matches!(events.first(), Some(E::ConnectionEstablished)),
    "{side}: stream must open with ConnectionEstablished, got {:?}",
    events.first()
  );
  anyhow::ensure!(
    matches!(events.last(), Some(E::Disconnected)),
    "{side}: stream must close with Disconnected, got {:?}",
    events.last()
  );
  anyhow::ensure!(
    events
      .iter()
      .filter(|e| matches!(e, E::ConnectionEstablished))
      .count()
      == 1,
    "{side}: ConnectionEstablished must be reported exactly once"
  );
  anyhow::ensure!(
    events
      .iter()
      .filter(|e| matches!(e, E::Disconnected))
      .count()
      == 1,
    "{side}: Disconnected must be reported exactly once"
  );

  let recovered_at = events
    .iter()
    .position(|e| matches!(e, E::RecoveryCompleted))
    .ok_or_else(|| anyhow::anyhow!("{side}: recovery never completed"))?;
  if let Some(first_message) = events
    .iter()
    .position(|e| matches!(e, E::MessageReceived { .. }))
  {
    anyhow::ensure!(
      first_message > recovered_at,
      "{side}: an application message was delivered before recovery completed"
    );
  }

  // Inbound sequence numbers only ever move forwards.
  let mut last_in = 0;
  for event in events {
    let E::MessageReceived { seq_num, .. } = event else {
      continue;
    };
    anyhow::ensure!(
      *seq_num > last_in,
      "{side}: inbound sequence numbers went backwards: {last_in} then \
       {seq_num}"
    );
    last_in = *seq_num;
  }

  // Every message this side sent is reported once, in order, with no gaps.
  let sent: Vec<u32> = events
    .iter()
    .filter_map(|e| match e {
      E::RawMessageSent(msg) => session::raw::tag_value(msg, Fields::MsgSeqNum)
        .and_then(|v| v.parse().ok()),
      _ => None,
    })
    .collect();
  anyhow::ensure!(
    sent == (1..=sent.len() as u32).collect::<Vec<_>>(),
    "{side}: outbound sequence numbers were not a gapless run from 1: {sent:?}"
  );

  Ok(())
}
