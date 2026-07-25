#![allow(dead_code, unused_imports, unused_variables)]
use babelfix as fix;
use std::sync::Arc;

use googletest::prelude::*;
use tracing::info;

mod matchers;
mod session;

use matchers::*;
use session::FIX_REPO;

#[test_log::test(tokio::test)]
async fn simple_logon() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      None,
      "SERVER",
      None,
      FIX_REPO.get_version("FIX.4.4").unwrap(),
    )
    .await?;

  use fix::schema::FIX_Latest::Fields;

  server
    .lock()
    .await
    .session(&server_session_id)
    .ok_or_else(|| anyhow::anyhow!("Session not found"))?
    .expect_event(matches_pattern!(
      &fix::session::SessionEvent::ConnectionEstablished,
    ))
    .await?;

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .expect_event(matches_pattern!(
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("CLIENT")),
          message::tag(Fields::TargetCompID, eq("SERVER")),
        ),
        anything()
      ),
    ))
    .await?;

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .expect_event(matches_pattern!(
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      ),
    ))
    .await?;

  client
    .session
    .expect_event(matches_pattern!(
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("CLIENT")),
          message::tag(Fields::TargetCompID, eq("SERVER")),
        ),
        anything()
      ),
    ))
    .await?;

  client
    .session
    .expect_event(matches_pattern!(
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      ),
    ))
    .await?;

  client
    .session
    .expect_event(matches_pattern!(
      fix::session::SessionEvent::ConnectionEstablished,
    ))
    .await?;

  Ok(())
}
