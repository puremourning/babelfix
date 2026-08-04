//! FIX connection keep-alive: heartbeats, test requests and dead peer
//! detection.
//!
//! These tests run with a heartbeat interval in the low hundreds of
//! milliseconds so the timers fire within a test, rather than the 30 seconds a
//! real session would use.

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

/// Short enough that three intervals elapse well inside a test, long enough
/// that scheduling jitter on a loaded machine does not reorder events.
const HEARTBEAT: Duration = Duration::from_millis(200);

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

async fn settled_peer(
  heartbeat: Duration,
) -> anyhow::Result<(
  fix::session::SessionIdentifier,
  Arc<tokio::sync::Mutex<session::Server>>,
  RawPeer,
)> {
  let (server_session_id, server, port) = session::serve(
    "SERVER",
    SessionOptions::default().heartbeat(heartbeat),
    "CLIENT",
    fix44(),
  )
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

  Ok((server_session_id, server, peer))
}

/// During a period of inactivity the session emits a Heartbeat, so the peer
/// can tell the connection is still alive. It is a plain Heartbeat: TestReqID
/// is only present when responding to a TestRequest.
#[test_log::test(tokio::test)]
async fn idle_session_emits_a_heartbeat() -> anyhow::Result<()> {
  let (_server_session_id, _server, mut peer) = settled_peer(HEARTBEAT).await?;

  let started = std::time::Instant::now();
  let heartbeat = peer.recv().await?;
  let elapsed = started.elapsed();

  verify_that!(&heartbeat, message::tag(Fields::MsgType, eq("0")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;
  verify_that!(&heartbeat, not(message::has_tag(Fields::TestReqID)))
    .map_err(|e| anyhow::anyhow!("{e}"))?;
  anyhow::ensure!(
    elapsed >= HEARTBEAT / 2,
    "Heartbeat arrived after {elapsed:?}, far sooner than the {HEARTBEAT:?} \
     interval"
  );

  Ok(())
}

/// A TestRequest must be answered with a Heartbeat echoing its TestReqID; that
/// is what makes it evidence the peer is actually processing messages rather
/// than merely emitting heartbeats on a timer.
#[test_log::test(tokio::test)]
async fn test_request_is_answered_with_a_matching_heartbeat()
-> anyhow::Result<()> {
  let (_server_session_id, _server, mut peer) =
    settled_peer(Duration::from_secs(30)).await?;

  peer
    .send(RawMessage::new("1").body(Fields::TestReqID, "PING-1"))
    .await?;

  let heartbeat = peer.recv().await?;
  verify_that!(
    &heartbeat,
    all!(
      message::tag(Fields::MsgType, eq("0")),
      message::tag(Fields::TestReqID, eq("PING-1")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A Heartbeat that does not carry the TestReqID the session is waiting for
/// does not complete recovery: the whole point of the synchronisation exchange
/// is that only the matching response proves the peer is caught up.
#[test_log::test(tokio::test)]
async fn heartbeat_with_the_wrong_test_req_id_does_not_complete_recovery()
-> anyhow::Result<()> {
  let (server_session_id, server, port) = session::serve(
    "SERVER",
    SessionOptions::default().heartbeat(Duration::from_secs(30)),
    "CLIENT",
    fix44(),
  )
  .await?;

  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER").await?;
  peer.logon(Duration::from_secs(30)).await?;

  let ack = peer.recv().await?;
  anyhow::ensure!(ack.get_type() == "A");
  let test_request = peer.recv().await?;
  anyhow::ensure!(test_request.get_type() == "1");

  peer
    .send(RawMessage::new("0").body(Fields::TestReqID, "not-the-one"))
    .await?;

  session::wait_for_session(&server, &server_session_id).await?;
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .expect_no_event(
      Duration::from_millis(300),
      &matches_pattern!(&fix::session::SessionEvent::RecoveryCompleted),
    )
    .await?;

  Ok(())
}

/// A peer that stops sending is probed with a TestRequest and, if it still
/// does not respond, the connection is terminated with a Logout explaining
/// why.
#[test_log::test(tokio::test)]
async fn silent_peer_is_probed_and_then_logged_out() -> anyhow::Result<()> {
  let (server_session_id, server, mut peer) = settled_peer(HEARTBEAT).await?;

  let test_request = peer.recv_skipping_heartbeats("1").await?;
  verify_that!(
    &test_request,
    message::tag(Fields::TestReqID, not(starts_with("HELO-")))
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  let logout = peer.recv_skipping_heartbeats("5").await?;
  verify_that!(
    &logout,
    message::tag(Fields::Text, contains_substring("Heartbeat timeout"))
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  peer.expect_closed().await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };

  Ok(())
}

/// Sending application traffic keeps the connection alive on its own, so no
/// Heartbeat is generated while messages are flowing.
#[test_log::test(tokio::test)]
async fn outbound_traffic_suppresses_heartbeats() -> anyhow::Result<()> {
  let interval = Duration::from_millis(300);
  let (server_session_id, server, mut peer) = settled_peer(interval).await?;

  let sender = tokio::spawn({
    let server = Arc::clone(&server);
    let server_session_id = server_session_id.clone();
    async move {
      for i in 0..5 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut msg =
          fix::message::builder::Message::new(fix44(), "D").unwrap();
        msg.body.set_tag(Fields::ClOrdID, format!("order-{i}"));
        msg.body.set_tag(Fields::Symbol, "AAPL");
        msg.body.set_tag(Fields::Side, "1");
        let _ = server
          .lock()
          .await
          .session(&server_session_id)
          .unwrap()
          .command(fix::session::SessionCommand::Send(msg))
          .await;
      }
    }
  });

  // Five orders at 100ms intervals span 500ms — well past the 300ms heartbeat
  // interval, which should be reset by each one.
  let deadline = std::time::Instant::now() + Duration::from_millis(550);
  while std::time::Instant::now() < deadline {
    let msg = peer.recv().await?;
    anyhow::ensure!(
      msg.get_type() != "0",
      "Heartbeat emitted while application traffic was flowing"
    );
  }

  sender.await?;
  Ok(())
}

/// Querying the session state is not traffic: it puts nothing on the wire, so
/// it must not postpone the heartbeat the peer is relying on.
#[test_log::test(tokio::test)]
async fn polling_the_session_state_does_not_suppress_heartbeats()
-> anyhow::Result<()> {
  let interval = Duration::from_millis(300);
  let (server_session_id, server, mut peer) = settled_peer(interval).await?;

  let poller = tokio::spawn({
    let server = Arc::clone(&server);
    let server_session_id = server_session_id.clone();
    async move {
      for _ in 0..12 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (tx, rx) = futures::channel::oneshot::channel();
        let sent = server
          .lock()
          .await
          .session(&server_session_id)
          .unwrap()
          .command(fix::session::SessionCommand::GetSessionState(tx))
          .await;
        if sent.is_ok() {
          let _ = rx.await;
        }
      }
    }
  });

  let heartbeat = peer.recv().await?;
  verify_that!(&heartbeat, message::tag(Fields::MsgType, eq("0")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  poller.abort();
  Ok(())
}
