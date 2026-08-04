//! Message recovery: resend requests, replay and gap fill.

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

/// The initiator resumes a session at outbound sequence number 5 while the
/// acceptor still expects 1, so the acceptor must recover the four messages it
/// never saw. The application replays a single (admin) message and then
/// declares the replay complete, so the whole range collapses into one gap
/// fill.
#[test_log::test(tokio::test)]
async fn server_recovers_client_messages() -> anyhow::Result<()> {
  let ((client_session_id, mut client), (server_session_id, server)) =
    session::establish(
      "CLIENT",
      SessionOptions::at(5, 1),
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
    // The Logon acknowledgement precedes the ResendRequest: the connection is
    // accepted first, then recovery is requested.
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

    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("3")),
          ),
          anything()
        )
    };
    { server(server_session_id) ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("6"))
          ),
          anything()
        )
    };

    // The acceptor drops that TestRequest: its sequence number is beyond the
    // gap it is still recovering, so it will arrive again after the replay.

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
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageSent(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("6")),
          ),
          anything()
        )
    };
    { client ignoring_state
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
    // The open-ended request is resolved to a concrete end sequence number
    // before it reaches the application.
    {
      client ignoring_state
      << fix::session::SessionEvent::ResendRequest{
        resend_request: anything(),
        begin_seq_no: eq(&1),
        end_seq_no: eq(&6),
      }
    };
    { client ignoring_state
      << fix::session::SessionEvent::RawMessageReceived(
          all!(
            message::tag(Fields::MsgType, eq("1")),
            message::tag(Fields::MsgSeqNum, eq("3")),
          ),
          anything()
        )
    };
  };

  // Replaying an admin message gap fills over it: session layer messages are
  // never retransmitted.
  let mut msg = fix::message::builder::Message::new(fix44(), "A")?;
  msg.header.set_tag(Fields::MsgSeqNum, 1);
  client
    .session
    .command(fix::session::SessionCommand::Replay(msg))
    .await?;
  client
    .session
    .command(fix::session::SessionCommand::ReplayComplete)
    .await?;

  expect_events! {
    { server(server_session_id) ignoring_state
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
    { server(server_session_id) ignoring_state
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
    { server(server_session_id) ignoring_state
      <<
      fix::session::SessionEvent::RecoveryCompleted
    };
    { server(server_session_id) ignoring_state
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
    { server(server_session_id) ignoring_state
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
    { client ignoring_state
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("4")), anything()
      )
    };
    { client ignoring_state
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("0")), anything()
      )
    };
    { client ignoring_state
      <<
      fix::session::SessionEvent::RawMessageSent(
        message::tag(Fields::MsgType, eq("1")), anything()
      )
    };
    { client ignoring_state
      <<
      fix::session::SessionEvent::RawMessageReceived(
        message::tag(Fields::MsgType, eq("0")), anything()
      )
    };
    { client ignoring_state
      <<
      fix::session::SessionEvent::RecoveryCompleted
    };
  }

  Ok(())
}
