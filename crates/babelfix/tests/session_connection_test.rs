//! The channel-free async session, over an in-memory duplex.
//!
//! These are the tier between `babelfix-core` (own your event loop) and
//! `endpoint::serve` (own nothing): real async, real codec, real framing, but
//! no socket, no port allocation, no spawned task and no channels. That
//! combination was not testable at all before the split — the session was
//! hard-bound to `tokio::net::tcp::ReadHalf`.

#![allow(dead_code, unused_imports, unused_variables)]
use std::sync::{Arc, LazyLock};

use babelfix as fix;
use fix::connection::SessionConnection;
use fix::message::builder;
use fix::schema::FIX_Latest::Fields;
use fix::session::{Command, Event, Progress, Session, SessionIdentifier};

static FIX_REPO: LazyLock<Arc<fix::repository::FixRepository>> =
  LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

fn order(cl_ord_id: &str) -> builder::Message {
  let mut msg = builder::Message::new(fix44(), "D").unwrap();
  msg.body.set_tag(Fields::ClOrdID, cl_ord_id);
  msg.body.set_tag(Fields::Symbol, "AAPL");
  msg.body.set_tag(Fields::Side, "1");
  msg.body.set_tag(Fields::OrderQty, 100i64);
  msg
}

fn session_id(us: &str, them: &str) -> SessionIdentifier {
  SessionIdentifier {
    begin_string: "FIX.4.4".into(),
    sender_comp_id: us.into(),
    target_comp_id: them.into(),
  }
}

/// Records the application-visible messages, so a test can assert on them
/// without the events ever being cloned into owned form.
#[derive(Default)]
struct Seen {
  orders: Vec<String>,
}

impl Seen {
  fn sink(&mut self) -> impl FnMut(Event<'_>) -> fix::Result<()> + '_ {
    move |event: Event<'_>| {
      if let Event::MessageReceived(msg) = event {
        self.orders.push(
          msg
            .body
            .tag(Fields::ClOrdID)
            .map(|v| v.as_string())
            .unwrap_or_default(),
        );
      }
      Ok(())
    }
  }
}

/// An application message crosses a duplex pair with no channels involved.
#[test_log::test(tokio::test)]
async fn a_message_crosses_a_duplex_pair() -> anyhow::Result<()> {
  let (client_io, server_io) = tokio::io::duplex(64 * 1024);

  // The acceptor waits for the peer's Logon before it knows who it is talking
  // to, which is why accepting is two steps.
  let acceptor = tokio::spawn(async move {
    let mut seen = Seen::default();
    let pending =
      SessionConnection::accept(server_io, FIX_REPO.clone(), None).await?;
    assert_eq!(pending.session_id().sender_comp_id, "SERVER");
    assert_eq!(pending.session_id().target_comp_id, "CLIENT");

    let mut conn = {
      let mut sink = seen.sink();
      pending.accept(Session::new(fix44()), &mut sink).await?
    };

    // Step until the order arrives. The sink borrows `seen`, so it has to be
    // dropped before the loop can look at what was collected.
    for _ in 0..8 {
      let closed = {
        let mut sink = seen.sink();
        conn.step(&mut sink).await?.is_close()
      };
      if closed || !seen.orders.is_empty() {
        break;
      }
    }
    Ok::<_, anyhow::Error>(seen.orders)
  });

  let mut seen = Seen::default();
  let mut sink = seen.sink();
  let mut client = SessionConnection::initiate(
    client_io,
    FIX_REPO.clone(),
    None,
    session_id("CLIENT", "SERVER"),
    Session::new(fix44()),
    &mut sink,
  )
  .await?;

  let progress = client.send(order("order-1"), &mut sink).await?;
  assert_eq!(progress, Progress::Continue);
  drop(sink);

  let orders =
    tokio::time::timeout(std::time::Duration::from_secs(5), acceptor)
      .await???;

  assert_eq!(orders, vec!["order-1"]);
  Ok(())
}

/// The acceptor learns the peer's identity from the Logon and can refuse, or
/// supply persisted sequence numbers, before the session starts.
#[test_log::test(tokio::test)]
async fn the_acceptor_sees_the_identity_before_committing() -> anyhow::Result<()>
{
  let (client_io, server_io) = tokio::io::duplex(64 * 1024);

  let acceptor = tokio::spawn(async move {
    let pending =
      SessionConnection::accept(server_io, FIX_REPO.clone(), None).await?;
    let id = pending.session_id().clone();
    // The Logon itself is available too, for applications that authenticate.
    assert_eq!(pending.logon().fix_message.msg_type, "A");
    Ok::<_, anyhow::Error>(id)
  });

  let mut seen = Seen::default();
  let mut sink = seen.sink();
  // The initiator's own logon send completes even though the acceptor never
  // answers, because the duplex buffer is large enough.
  let _client = tokio::time::timeout(
    std::time::Duration::from_secs(5),
    SessionConnection::initiate(
      client_io,
      FIX_REPO.clone(),
      None,
      session_id("CLIENT", "SERVER"),
      Session::new(fix44()),
      &mut sink,
    ),
  )
  .await;

  let id = tokio::time::timeout(std::time::Duration::from_secs(5), acceptor)
    .await???;

  assert_eq!(id.sender_comp_id, "SERVER");
  assert_eq!(id.target_comp_id, "CLIENT");
  assert_eq!(id.begin_string, "FIX.4.4");
  Ok(())
}
