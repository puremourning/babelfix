//! FIX connection termination.

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

/// A Logout received from the peer is acknowledged with a Logout and then ends
/// the session. Continuing to exchange messages after acknowledging a Logout
/// would leave both peers echoing Logouts at each other indefinitely, each one
/// consuming a sequence number.
#[test_log::test(tokio::test)]
async fn unsolicited_logout_is_acknowledged_once_and_ends_the_session()
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
    .send(RawMessage::new("5").body(Fields::Text, "done for the day"))
    .await?;

  let logout_ack = peer.recv().await?;
  verify_that!(&logout_ack, message::tag(Fields::MsgType, eq("5")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  // Exactly one acknowledgement, and then the transport layer goes away.
  peer.expect_closed().await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}

/// A Logout that arrives while we are recovering must not be answered
/// immediately: the messages still missing precede it, and acknowledging first
/// would abandon them. Recovery runs to completion and only then is the Logout
/// acknowledged.
///
/// It also cannot be discarded and waited for again — a Logout is a session
/// layer message, so the peer gap fills over it rather than retransmitting it,
/// and the request would be lost.
#[test_log::test(tokio::test)]
async fn logout_inside_a_gap_is_acknowledged_once_recovery_completes()
-> anyhow::Result<()> {
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  // Logging on at 5 leaves the acceptor expecting 1, so it is recovering.
  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER")
    .await?
    .starting_at(5);
  peer.logon(Duration::from_secs(30)).await?;

  let ack = peer.recv().await?;
  anyhow::ensure!(ack.get_type() == "A");
  let resend_request = peer.recv().await?;
  verify_that!(&resend_request, message::tag(Fields::MsgType, eq("2")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;
  let test_request = peer.recv().await?;
  anyhow::ensure!(test_request.get_type() == "1");

  // The Logout falls inside the gap.
  peer
    .send(RawMessage::new("5").body(Fields::Text, "shutting down"))
    .await?;

  // No acknowledgement yet: the acceptor is still missing messages 1 to 5.
  peer.expect_silence(Duration::from_millis(200)).await?;

  // Gap fill over 1 to 6, which is everything including the Logout itself.
  peer
    .send(
      RawMessage::new("4")
        .seq(1)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "7"),
    )
    .await?;

  let logout_ack = peer.recv().await?;
  verify_that!(&logout_ack, message::tag(Fields::MsgType, eq("5")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  peer.expect_closed().await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}

/// Terminating a session sends a Logout before closing the transport layer, so
/// the peer can distinguish an orderly shutdown from a network failure.
#[test_log::test(tokio::test)]
async fn disconnect_sends_a_logout_before_closing() -> anyhow::Result<()> {
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

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Disconnect)
    .await?;

  let logout = peer.recv().await?;
  verify_that!(
    &logout,
    all!(
      message::tag(Fields::MsgType, eq("5")),
      // The Logout consumes the next outbound sequence number like any other
      // message.
      message::tag(Fields::MsgSeqNum, eq("3")),
      message::tag(Fields::SenderCompID, eq("SERVER")),
      message::tag(Fields::TargetCompID, eq("CLIENT")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  peer.expect_closed().await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}

/// Losing the transport layer without a Logout is reported to the application
/// as a disconnection rather than hanging or panicking.
#[test_log::test(tokio::test)]
async fn abrupt_loss_of_the_transport_layer_is_reported() -> anyhow::Result<()>
{
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let peer =
    RawPeer::connect_and_logon(port, fix44(), "CLIENT", "SERVER").await?;
  session::wait_for_session(&server, &server_session_id).await?;
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .settle()
    .await?;

  peer.disconnect().await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}

/// Between two babelfix peers, one side terminating the session brings the
/// other down with it: the Logout is delivered, acknowledged, and both
/// applications observe a disconnection.
#[test_log::test(tokio::test)]
async fn logout_terminates_the_session_on_both_sides() -> anyhow::Result<()> {
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
    .command(fix::session::SessionCommand::Disconnect)
    .await?;

  expect_events! {
    { client awaiting
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("5")), anything()) };
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("5")), anything()) };
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
    { client awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}
