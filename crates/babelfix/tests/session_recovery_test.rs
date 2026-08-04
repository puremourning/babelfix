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

/// An application message the session under test can relay.
fn order(cl_ord_id: &str) -> anyhow::Result<fix::message::builder::Message> {
  let mut msg = fix::message::builder::Message::new(fix44(), "D")?;
  msg.body.set_tag(Fields::ClOrdID, cl_ord_id);
  msg.body.set_tag(Fields::Symbol, "AAPL");
  msg.body.set_tag(Fields::Side, "1");
  msg.body.set_tag(Fields::OrderQty, 100i64);
  Ok(msg)
}

/// Reconstruct a previously sent message for replay, as an application that
/// persisted its outbound stream would.
fn replayed(
  msg_type: &str,
  seq_num: u32,
) -> anyhow::Result<fix::message::builder::Message> {
  let mut msg = if msg_type == "D" {
    order(&format!("order-{seq_num}"))?
  } else {
    fix::message::builder::Message::new(fix44(), msg_type)?
  };
  msg.header.set_tag(Fields::MsgSeqNum, seq_num);
  // The original SendingTime, which the session moves to OrigSendingTime.
  msg
    .header
    .set_tag(Fields::SendingTime, "20200101-00:00:00.000");
  Ok(msg)
}

/// A resend spanning both session layer and application messages must gap fill
/// over exactly the messages that are not retransmitted, and no further.
///
/// The acceptor has sent four messages: a Logon acknowledgement (1), a
/// TestRequest (2) and two orders (3, 4). Session layer messages are never
/// retransmitted, so 1 and 2 collapse into a single SequenceReset-GapFill whose
/// NewSeqNo must be 3 — the sequence number of the message sent immediately
/// after it. A NewSeqNo of 4 would tell the peer to expect 4 next, and the
/// order that follows with MsgSeqNum 3 would then look like a stale duplicate.
#[test_log::test(tokio::test)]
async fn gap_fill_stops_short_of_the_next_retransmitted_message()
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

  for id in ["order-3", "order-4"] {
    server
      .lock()
      .await
      .session(&server_session_id)
      .unwrap()
      .command(fix::session::SessionCommand::Send(order(id)?))
      .await?;
    let sent = peer.recv().await?;
    verify_that!(&sent, message::tag(Fields::MsgType, eq("D")))
      .map_err(|e| anyhow::anyhow!("{e}"))?;
  }

  // Ask for everything from the beginning of the session.
  peer
    .send(
      RawMessage::new("2")
        .body(Fields::BeginSeqNo, "1")
        .body(Fields::EndSeqNo, "0"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::ResendRequest {
        resend_request: anything(),
        begin_seq_no: eq(&1),
        end_seq_no: eq(&4),
      }
    };
  };

  for (msg_type, seq_num) in [("A", 1), ("1", 2), ("D", 3), ("D", 4)] {
    server
      .lock()
      .await
      .session(&server_session_id)
      .unwrap()
      .command(fix::session::SessionCommand::Replay(replayed(
        msg_type, seq_num,
      )?))
      .await?;
  }
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::ReplayComplete)
    .await?;

  let gap_fill = peer.recv().await?;
  verify_that!(
    &gap_fill,
    all!(
      message::tag(Fields::MsgType, eq("4")),
      message::tag(Fields::GapFillFlag, eq("Y")),
      message::tag(Fields::MsgSeqNum, eq("1")),
      message::tag(Fields::NewSeqNo, eq("3")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  for seq_num in ["3", "4"] {
    let order = peer.recv().await?;
    verify_that!(
      &order,
      all!(
        message::tag(Fields::MsgType, eq("D")),
        message::tag(Fields::MsgSeqNum, eq(seq_num)),
        message::tag(Fields::PossDupFlag, eq("Y")),
        message::tag(Fields::OrigSendingTime, eq("20200101-00:00:00.000")),
        message::tag(Fields::ClOrdID, eq(format!("order-{seq_num}").as_str())),
      )
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
  }

  Ok(())
}

/// An application that declines to retransmit anything still has to eliminate
/// the gap: the whole requested range collapses into one SequenceReset-GapFill
/// whose NewSeqNo is one past the end of the range.
#[test_log::test(tokio::test)]
async fn declining_to_replay_gap_fills_the_whole_range() -> anyhow::Result<()> {
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
      RawMessage::new("2")
        .body(Fields::BeginSeqNo, "1")
        .body(Fields::EndSeqNo, "2"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::ResendRequest {
        resend_request: anything(),
        begin_seq_no: eq(&1),
        end_seq_no: eq(&2),
      }
    };
  };

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::ReplayComplete)
    .await?;

  let gap_fill = peer.recv().await?;
  verify_that!(
    &gap_fill,
    all!(
      message::tag(Fields::MsgType, eq("4")),
      message::tag(Fields::GapFillFlag, eq("Y")),
      message::tag(Fields::MsgSeqNum, eq("1")),
      message::tag(Fields::NewSeqNo, eq("3")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// New messages produced while a resend is in progress must not overtake the
/// retransmission: they are held back and sent afterwards, with fresh sequence
/// numbers beyond the requested range.
#[test_log::test(tokio::test)]
async fn messages_sent_during_a_replay_are_queued_until_it_completes()
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
      RawMessage::new("2")
        .body(Fields::BeginSeqNo, "1")
        .body(Fields::EndSeqNo, "2"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::ResendRequest {
        resend_request: anything(),
        begin_seq_no: anything(),
        end_seq_no: anything(),
      }
    };
  };

  // Queued behind the in-flight resend.
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(order("new-order")?))
    .await?;
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::ReplayComplete)
    .await?;

  let gap_fill = peer.recv().await?;
  verify_that!(&gap_fill, message::tag(Fields::MsgType, eq("4")))
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  let new_order = peer.recv().await?;
  verify_that!(
    &new_order,
    all!(
      message::tag(Fields::MsgType, eq("D")),
      message::tag(Fields::ClOrdID, eq("new-order")),
      message::tag(Fields::MsgSeqNum, eq("3")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A SequenceReset-GapFill received in sequence advances the next expected
/// inbound sequence number to NewSeqNo, closing the gap without the skipped
/// messages ever arriving.
#[test_log::test(tokio::test)]
async fn inbound_gap_fill_advances_the_expected_sequence_number()
-> anyhow::Result<()> {
  // The acceptor expects inbound 1; the peer logs on at 5, so four messages
  // are missing and the acceptor asks for them.
  let (server_session_id, server, port) =
    session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44())
      .await?;

  let mut peer = RawPeer::connect(port, fix44(), "CLIENT", "SERVER")
    .await?
    .starting_at(5);
  peer.logon(Duration::from_secs(30)).await?;

  let ack = peer.recv().await?;
  anyhow::ensure!(ack.get_type() == "A");
  let resend_request = peer.recv().await?;
  verify_that!(
    &resend_request,
    all!(
      message::tag(Fields::MsgType, eq("2")),
      message::tag(Fields::BeginSeqNo, eq("1")),
      message::tag(Fields::EndSeqNo, eq("0")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  // Gap fill over 1-5 inclusive, so the next message the acceptor expects is 6.
  peer
    .send(
      RawMessage::new("4")
        .seq(1)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "6"),
    )
    .await?;

  session::wait_for_session(&server, &server_session_id).await?;
  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::RawMessageReceived(
          message::tag(Fields::MsgType, eq("4")), anything()) };
  };

  // With the gap closed, a message at 6 is accepted and delivered.
  peer
    .send(
      RawMessage::new("D")
        .seq(6)
        .body(Fields::ClOrdID, "after-gap")
        .body(Fields::Symbol, "AAPL")
        .body(Fields::Side, "1"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived(
          builder::body(builder::tag(
            Fields::ClOrdID, typedvalue::string(eq("after-gap"))))) };
  };

  Ok(())
}

/// An application that replays past the end of the requested range must not
/// have those messages transmitted: their sequence numbers have not been
/// consumed yet, and reusing them would put the same MsgSeqNum on the wire
/// twice.
#[test_log::test(tokio::test)]
async fn replay_beyond_the_requested_range_is_not_transmitted()
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
      RawMessage::new("2")
        .body(Fields::BeginSeqNo, "1")
        .body(Fields::EndSeqNo, "2"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::ResendRequest {
        resend_request: anything(),
        begin_seq_no: eq(&1),
        end_seq_no: eq(&2),
      }
    };
  };

  // Only 1 and 2 were asked for; 3 has not been sent yet.
  for seq_num in [1, 2, 3] {
    server
      .lock()
      .await
      .session(&server_session_id)
      .unwrap()
      .command(fix::session::SessionCommand::Replay(replayed(
        "D", seq_num,
      )?))
      .await?;
  }
  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::ReplayComplete)
    .await?;

  for seq_num in ["1", "2"] {
    let replayed = peer.recv().await?;
    verify_that!(
      &replayed,
      all!(
        message::tag(Fields::MsgType, eq("D")),
        message::tag(Fields::MsgSeqNum, eq(seq_num)),
        message::tag(Fields::PossDupFlag, eq("Y")),
      )
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
  }

  // Completing a resend re-confirms synchronisation, which consumes sequence
  // number 3 — the one the out-of-range replay would have reused.
  let test_request = peer.recv().await?;
  verify_that!(
    &test_request,
    all!(
      message::tag(Fields::MsgType, eq("1")),
      message::tag(Fields::MsgSeqNum, eq("3")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  server
    .lock()
    .await
    .session(&server_session_id)
    .unwrap()
    .command(fix::session::SessionCommand::Send(order("brand-new")?))
    .await?;

  let new_order = peer.recv().await?;
  verify_that!(
    &new_order,
    all!(
      message::tag(Fields::MsgSeqNum, eq("4")),
      message::tag(Fields::ClOrdID, eq("brand-new")),
      not(message::has_tag(Fields::PossDupFlag)),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// A gap fill occupies a slot in the message stream like any other message, so
/// it has to arrive in sequence. One that arrives too high must not be applied:
/// doing so would skip over the messages between the expected sequence number
/// and the gap fill's own, which were never accounted for by anybody.
#[test_log::test(tokio::test)]
async fn out_of_sequence_gap_fill_does_not_skip_messages() -> anyhow::Result<()>
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

  // The acceptor expects 3. A gap fill claiming to start at 9 leaves 3-8
  // unaccounted for, so it must be recovered rather than jumped over.
  peer
    .send(
      RawMessage::new("4")
        .seq(9)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "20"),
    )
    .await?;

  let resend_request = peer.recv().await?;
  verify_that!(
    &resend_request,
    all!(
      message::tag(Fields::MsgType, eq("2")),
      message::tag(Fields::BeginSeqNo, eq("3")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  // The expected inbound sequence number is still 3, so an in-sequence gap
  // fill closes the gap and the message after it is accepted.
  peer
    .send(
      RawMessage::new("4")
        .seq(3)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "10"),
    )
    .await?;
  peer
    .send(
      RawMessage::new("D")
        .seq(10)
        .body(Fields::ClOrdID, "after-gap"),
    )
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::MessageReceived(
          builder::body(builder::tag(
            Fields::ClOrdID, typedvalue::string(eq("after-gap"))))) };
  };

  Ok(())
}

/// A gap fill below the expected inbound sequence number is a sequence number
/// error like any other, and terminates the connection with a Logout.
#[test_log::test(tokio::test)]
async fn gap_fill_below_the_expected_sequence_number_is_logged_out()
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

  // The acceptor expects 3.
  peer
    .send(
      RawMessage::new("4")
        .seq(1)
        .body(Fields::GapFillFlag, "Y")
        .body(Fields::NewSeqNo, "2"),
    )
    .await?;

  let logout = peer.recv().await?;
  verify_that!(
    &logout,
    all!(
      message::tag(Fields::MsgType, eq("5")),
      message::tag(Fields::Text, contains_substring("too low")),
    )
  )
  .map_err(|e| anyhow::anyhow!("{e}"))?;

  Ok(())
}

/// Replay is only meaningful in response to a ResendRequest; outside one it is
/// a programming error in the application and terminates the session rather
/// than silently corrupting the outbound sequence.
#[test_log::test(tokio::test)]
async fn replay_without_a_resend_request_is_rejected() -> anyhow::Result<()> {
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
    .command(fix::session::SessionCommand::Replay(replayed("D", 1)?))
    .await?;

  expect_events! {
    { server(server_session_id) awaiting
      << fix::session::SessionEvent::Disconnected };
  };
  peer.expect_closed().await?;

  Ok(())
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
