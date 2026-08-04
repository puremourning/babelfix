//! Logon and FIX connection establishment.
//!
//! Covers the initiator and acceptor sides of the logon exchange, the initial
//! sequence number comparison, and the cases where a connection must be
//! refused.

#![allow(dead_code, unused_imports, unused_variables)]
use babelfix as fix;
use std::sync::Arc;
use std::time::Duration;

use googletest::prelude::*;

mod matchers;
mod session;

use fix::schema::FIX_Latest::Fields;
use matchers::*;
use session::raw::{RawMessage, RawPeer};
use session::{FIX_REPO, SessionOptions, expect_event, expect_events};

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

/// A clean logon on both sides, followed by the TestRequest/Heartbeat exchange
/// each peer performs to confirm the session is synchronised.
///
/// The ordering asserted here is deterministic even though it interleaves
/// locally generated and peer-driven events: each side transmits its
/// synchronisation TestRequest before entering its select loop, so the sent
/// TestRequest always precedes anything received from the peer.
#[test_log::test(tokio::test)]
async fn simple_logon() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      SessionOptions::default(),
      "SERVER",
      SessionOptions::default(),
      fix44(),
    )
    .await?;

  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("1")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::EncryptMethod, eq("0")),
          message::tag(Fields::SenderCompID, eq("CLIENT")),
          message::tag(Fields::TargetCompID, eq("SERVER")),
        ),
        anything()
      )
    };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("1")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::EncryptMethod, eq("0")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      ),
    };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("2")),
            message::tag(Fields::TestReqID, starts_with("HELO-")),
          ), anything()) };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RecoveryCompleted };

    { client <<
      fix::session::SessionEvent::ConnectionEstablished };
    { client <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("1")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::EncryptMethod, eq("0")),
          message::tag(Fields::SenderCompID, eq("CLIENT")),
          message::tag(Fields::TargetCompID, eq("SERVER")),
        ),
        anything()
      )
    };
    { client <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("1")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      )
    };
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("2")),
            message::tag(Fields::TestReqID, starts_with("HELO-")),
          ), anything()) };
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { client ignoring_state
      << fix::session::SessionEvent::RecoveryCompleted };
  };

  Ok(())
}

/// The acceptor identifies the session from the inbound Logon by swapping the
/// CompIDs: the initiator's `TargetCompID` becomes the acceptor's
/// `SenderCompID` and vice versa.
#[test_log::test(tokio::test)]
async fn acceptor_identifies_session_by_swapping_comp_ids() -> anyhow::Result<()>
{
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER").await?;
  peer.logon(Duration::from_secs(30)).await?;

  // The acceptor only produces a session — and therefore the harness only sees
  // one under this identifier — if it derived the identity as expected.
  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        message::tag(Fields::MsgType, eq("A")), anything()) };
  };

  let ack = peer.recv().await?;
  verify_that!(
    &ack,
    all!(
      message::tag(Fields::MsgType, eq("A")),
      message::tag(Fields::SenderCompID, eq("SERVER")),
      message::tag(Fields::TargetCompID, eq("CLIENT")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A FIX connection must begin with a Logon. Anything else is refused by
/// terminating the transport layer connection, without a Logout — a Logout
/// would consume a sequence number and leak session details to an
/// unauthenticated peer.
#[test_log::test(tokio::test)]
async fn first_message_that_is_not_a_logon_is_refused() -> anyhow::Result<()> {
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER").await?;
  peer
    .send(RawMessage::new("0").body(Fields::TestReqID, "not-a-logon"))
    .await?;

  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        message::tag(Fields::MsgType, eq("0")), anything()) };
    { server(server_session_id) <<
      fix::session::SessionEvent::Disconnected };
  };

  peer.expect_closed().await?;

  Ok(())
}

/// A Logon naming a session the acceptor is not configured for is refused
/// silently, for the same reason.
#[test_log::test(tokio::test)]
async fn logon_for_an_unknown_session_is_refused_silently() -> anyhow::Result<()>
{
  let (_server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer = RawPeer::connect(port, fix44(), "STRANGER", "SERVER").await?;
  peer.logon(Duration::from_secs(30)).await?;

  peer.expect_closed().await?;

  Ok(())
}

/// A peer that establishes the transport layer and then disappears without
/// sending a Logon is reported to the application rather than silently
/// forgotten.
#[test_log::test(tokio::test)]
async fn peer_that_sends_nothing_is_reported_as_invalid() -> anyhow::Result<()>
{
  let (_server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER").await?;
  peer.disconnect().await?;

  let deadline = std::time::Instant::now() + Duration::from_secs(5);
  loop {
    if !server.lock().await.invalid_sessions().is_empty() {
      break;
    }
    anyhow::ensure!(
      std::time::Instant::now() < deadline,
      "Peer that sent nothing was never reported as invalid"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
  }

  Ok(())
}

/// A Logon whose `MsgSeqNum` is below the acceptor's next expected inbound
/// number means one side has lost session state: the connection is terminated
/// with a Logout naming the discrepancy, and no recovery is attempted.
#[test_log::test(tokio::test)]
async fn logon_with_sequence_number_too_low_is_logged_out() -> anyhow::Result<()>
{
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      SessionOptions::default(),
      "SERVER",
      SessionOptions::at(1, 5),
      fix44(),
    )
    .await?;

  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("1")),
        ), anything()) };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("A")), anything()) };
    { server(server_session_id) ignoring_state <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("5")),
          message::tag(Fields::Text, contains_substring("too low")),
          message::tag(Fields::Text, contains_substring("Expected 5")),
          message::tag(Fields::Text, contains_substring("got 1")),
        ), anything()) };
    { server(server_session_id) ignoring_state <<
      fix::session::SessionEvent::Disconnected };
  };

  // The initiator sees the Logout and the connection go away. No sequence
  // recovery is attempted by either side.
  client
    .session
    .next_event_matching(&matches_pattern!(
      &fix::session::SessionEvent::RawMessageReceived(
        ref message::tag(Fields::MsgType, eq("5")),
        ref anything()
      )
    ))
    .await?;
  client
    .session
    .next_event_matching(&matches_pattern!(
      &fix::session::SessionEvent::Disconnected
    ))
    .await?;

  Ok(())
}
