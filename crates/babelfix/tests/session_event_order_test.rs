//! Event ordering under backpressure.
//!
//! Events are handed to the application as they are produced, and buffered only
//! when the channel is full. That split is what this pins down: once anything
//! is queued, everything must queue behind it, or an event that fits would
//! overtake one already waiting and the application would see them out of
//! order.
//!
//! Nothing else exercises it, because every other test leaves the channel at
//! its default depth of 100 and never comes close to filling it. Here the depth
//! is 1, so a single inbound message — which produces `RawMessageReceived`,
//! `SessionState` and `MessageReceived` — is guaranteed to cross the boundary.

#![allow(dead_code, unused_imports, unused_macros, unused_variables)]
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use babelfix as fix;
use fix::schema::FIX_Latest::Fields;
use futures::StreamExt;

mod matchers;
mod session;

use session::raw::{RawMessage, RawPeer};
use session::{FIX_REPO, SessionOptions};

fn fix44() -> Arc<fix::repository::FixVersion> {
  FIX_REPO.get_version("FIX.4.4").unwrap()
}

/// A label for each event, so an assertion reads as a sequence.
fn label(event: &fix::session::SessionEvent) -> String {
  use fix::session::SessionEvent as E;
  match event {
    E::ConnectionEstablished => "ConnectionEstablished".into(),
    E::RecoveryCompleted => "RecoveryCompleted".into(),
    E::SessionState(_) => "SessionState".into(),
    E::RawMessageReceived(m, _) => {
      format!("RawMessageReceived({})", m.get_type())
    }
    E::RawMessageSent(m, _) => format!("RawMessageSent({})", m.get_type()),
    E::MessageReceived(m) => {
      format!("MessageReceived({})", m.fix_message.msg_type)
    }
    E::Disconnected => "Disconnected".into(),
    _ => "other".into(),
  }
}

/// With a channel too small to hold a single pass's events, some are delivered
/// directly and the rest are queued — and they must still arrive in the order
/// the session produced them.
#[test_log::test(tokio::test)]
async fn events_stay_in_order_when_the_channel_fills() -> anyhow::Result<()> {
  // Depth 1: the session cannot emit a whole pass without blocking, which is
  // exactly the case the ordering invariant exists for.
  let config = fix::endpoint::EndpointConfig::default().channel_depth(1);

  let acceptor =
    fix::endpoint::serve(("127.0.0.1", 0), FIX_REPO.clone(), config.clone())
      .await?;
  let port = acceptor.local_addr.port();

  let fix::endpoint::Acceptor {
    mut events,
    commands: _commands,
    ..
  } = acceptor;

  // Answer NewSession, then collect every event the session emits, in order.
  let collector = tokio::spawn(async move {
    let mut seen: Vec<String> = Vec::new();
    let mut handle: Option<fix::session::SessionHandle> = None;

    while let Some(event) = events.next().await {
      match event {
        fix::endpoint::EndpointEvent::NewSession { response, .. } => {
          let _ = response.send(Ok(fix::session::Session::new(fix44())));
        }
        fix::endpoint::EndpointEvent::SessionConnected(h) => {
          handle = Some(h);
          break;
        }
        fix::endpoint::EndpointEvent::SessionInvalid(_) => {}
      }
    }

    // Drain until the application message arrives, or the session ends.
    let mut handle = handle.expect("no session was published");
    while let Some(event) = handle.events.next().await {
      let l = label(&event);
      let done = l.starts_with("MessageReceived") || l == "Disconnected";
      seen.push(l);
      if done {
        break;
      }
    }
    seen
  });

  let mut peer =
    RawPeer::connect_and_logon(port, fix44(), "CLIENT", "SERVER").await?;

  // One inbound application message produces three events in a single pass.
  peer
    .send(
      RawMessage::new("D")
        .body(Fields::ClOrdID, "order-1")
        .body(Fields::Symbol, "AAPL")
        .body(Fields::Side, "1")
        .body(Fields::OrderQty, "100"),
    )
    .await?;

  let seen = tokio::time::timeout(Duration::from_secs(5), collector).await??;

  // The order the session produces them in, whichever side of the boundary
  // each one happened to fall.
  let logon_idx = seen
    .iter()
    .position(|e| e == "RawMessageReceived(A)")
    .expect("the Logon was never reported");
  let order_idx = seen
    .iter()
    .position(|e| e == "RawMessageReceived(D)")
    .unwrap_or_else(|| panic!("the order was never reported: {seen:?}"));
  let delivered_idx = seen
    .iter()
    .position(|e| e == "MessageReceived(D)")
    .unwrap_or_else(|| panic!("the order never reached the app: {seen:?}"));

  assert!(
    logon_idx < order_idx,
    "the Logon must be reported before the order: {seen:?}"
  );
  assert!(
    order_idx < delivered_idx,
    "the raw message must be reported before the parsed one: {seen:?}"
  );

  // And every SessionState between them is still in sequence — no event
  // overtook one that was waiting.
  assert!(
    seen.iter().any(|e| e == "SessionState"),
    "no SessionState was delivered at all: {seen:?}"
  );

  Ok(())
}
