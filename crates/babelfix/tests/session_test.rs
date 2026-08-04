#![allow(dead_code, unused_imports, unused_variables)]
use babelfix as fix;
use futures::SinkExt;
use std::sync::Arc;

use googletest::prelude::*;
use tracing::info;

mod matchers;
mod session;

use matchers::*;
use session::FIX_REPO;

macro_rules! expect_event {
  ( $server:ident ( $session_id:expr ) << $($event:tt)+ ) => {
    let event = $server
      .lock()
      .await
      .session(&$session_id)
      .ok_or_else(|| anyhow::anyhow!("Session not found"))?
      .next_event(&[])
      .await?;
     verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
       anyhow::anyhow!("Expected server event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $server:ident ( $session_id:expr ) skipping [ $($($skip:tt)+),* ] << $($event:tt)+ ) => {
    let event = $server
      .lock()
      .await
      .session(&$session_id)
      .ok_or_else(|| anyhow::anyhow!("Session not found"))?
      .next_event(&[$(&matches_pattern!($($skip)+)),*])
      .await?;
     verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
       anyhow::anyhow!("Expected server event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $client:ident << $($event:tt)+ ) => {
    let event = $client
      .session
      .next_event(&[])
      .await?;
    verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
      anyhow::anyhow!("Expected client event '{}' not received\n{e}", stringify!($($event)+)))?;
  };

  ( $client:ident skipping [ $($($skip:tt)+),* ] << $($event:tt)+ ) => {
    let event = $client
      .session
      .next_event(&[$(&matches_pattern!($($skip)+)),*])
      .await?;
    verify_that!(&event, matches_pattern!($($event)+)).map_err(|e|
      anyhow::anyhow!("Expected client event '{}' not received\n{e}", stringify!($($event)+)))?;
  };
}

macro_rules! expect_events {
  ( $( { $($item:tt)+ } ; )* ) => {
    $( expect_event!($($item)+); )*
  };
}

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

  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
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
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      ),
    };
    // TODO: this section should be "in any order ?" because the sequence is not
    // really deterministic
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RecoveryCompleted };

    { client <<
      fix::session::SessionEvent::ConnectionEstablished };
    { client <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::HeartBtInt, eq("30")),
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
          message::tag(Fields::HeartBtInt, eq("30")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      )
    };
    // TODO: this section should be "in any order ?" because the sequence is not
    // really deterministic
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("1")), anything()) };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("0")), anything()) };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RecoveryCompleted };
  };

  Ok(())
}

#[test_log::test(tokio::test)]
async fn server_recovers_client_messages() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      Some((5, 1)),
      "SERVER",
      Some((1, 1)),
      FIX_REPO.get_version("FIX.4.4").unwrap(),
    )
    .await?;

  use fix::schema::FIX_Latest::Fields;

  expect_events! {
    { server(server_session_id) <<
      fix::session::SessionEvent::ConnectionEstablished };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("5")),
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
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
        ),
        anything()
      ),
    };
    { server(server_session_id) <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("2")),
          message::tag(Fields::MsgSeqNum, eq("2")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
          message::tag(Fields::BeginSeqNo, eq("1")),
          message::tag(Fields::EndSeqNo, eq("0"))
        ),
        anything()
      ),
    };

    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("3")),
          ),
          anything()
        )
    };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("6"))
          ),
          anything()
        )
    };

    // TestRequest is queued for delivery in SERVER due to seqno 6, awaiting
    // client to replay 1-4

    { client <<
      fix::session::SessionEvent::ConnectionEstablished };
    { client <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("A")),
          message::tag(Fields::MsgSeqNum, eq("5")),
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
        ),
        anything()
      )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("6")),
          ),
          anything()
        )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("2")),
          message::tag(Fields::MsgSeqNum, eq("2")),
          message::tag(Fields::SenderCompID, eq("SERVER")),
          message::tag(Fields::TargetCompID, eq("CLIENT")),
          message::tag(Fields::BeginSeqNo, eq("1")),
          message::tag(Fields::EndSeqNo, eq("0"))
        ),
        anything()
      ),
    };
    {
      client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::ResendRequest{
        resend_request: anything(),
        begin_seq_no: eq(&1),
        end_seq_no: eq(&6),
      }
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      << fix::session::SessionEvent::RawMessageReceived(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("3")),
          ),
          anything()
        )
    };
  };
  // Send logon which should be queued for gap fill
  let mut msg = fix::message::builder::Message::new(
    FIX_REPO.get_version("FIX.4.4").unwrap(),
    "A",
  )
  .unwrap();
  msg.header.set_tag(Fields::MsgSeqNum, 1);
  client
    .session
    .handle
    .tx
    .send(fix::session::SessionCommand::Replay(msg))
    .await?;
  // Gap fill the remaining messages
  client
    .session
    .handle
    .tx
    .send(fix::session::SessionCommand::ReplayComplete)
    .await?;

  expect_events! {
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("4")),
          message::tag(Fields::MsgSeqNum, eq("1")),
          message::tag(Fields::NewSeqNo, eq("7")),
        ),
        anything()
      )
    };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("0")),
          message::tag(Fields::MsgSeqNum, eq("7")),
          message::tag(Fields::TestReqID, starts_with("HELO-")),
        ),
        anything()
      )
    };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RecoveryCompleted
    };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageReceived(
        all!(
          message::tag(Fields::MsgType, eq("1")),
          message::tag(Fields::MsgSeqNum, eq("8")),
          message::tag(Fields::TestReqID, starts_with("HELO-")),
        ),
        anything()
      )
    };
    { server(server_session_id)
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageSent(
        all!(
          message::tag(Fields::MsgType, eq("0")),
          message::tag(Fields::TestReqID, starts_with("HELO-")),
        ),
        anything()
      )
    };
  }

  expect_events! {
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("4")), anything()
      )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("0")), anything()
      )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("1")), anything()
      )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RawMessageReceived(
        message::tag(Fields::MsgType, eq("0")), anything()
      )
    };
    { client
      skipping [ fix::session::SessionEvent::SessionState(anything()) ]
      <<
      fix::session::SessionEvent::RecoveryCompleted
    };
  }

  Ok(())
}
