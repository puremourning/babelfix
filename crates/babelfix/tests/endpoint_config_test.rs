//! [`EndpointConfig`] settings are wired through, not merely accepted.
//!
//! These were hardcoded constants until recently. A configuration field that
//! silently keeps using the old default is worse than no field at all, so each
//! knob here is asserted against observable behaviour.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use babelfix as fix;
use fix::endpoint::{self, EndpointConfig};
use tokio::io::AsyncReadExt;

static FIX_REPO: LazyLock<Arc<fix::repository::FixRepository>> =
  LazyLock::new(|| Arc::new(fix::repository::orchestrate().unwrap()));

/// Short enough to assert on quickly, long enough not to race a loaded machine.
const SHORT_LOGON_TIMEOUT: Duration = Duration::from_millis(300);

/// A peer that connects and then says nothing must be dropped once the logon
/// timeout expires — and at the *configured* timeout, not the 30s default.
#[test_log::test(tokio::test)]
async fn the_logon_timeout_is_honoured() -> anyhow::Result<()> {
  let endpoint = endpoint::serve(
    ("127.0.0.1", 0),
    FIX_REPO.clone(),
    EndpointConfig::default().logon_timeout(SHORT_LOGON_TIMEOUT),
  )
  .await?;
  let addr = endpoint.local_addr;

  let started = Instant::now();
  let mut peer = tokio::net::TcpStream::connect(addr).await?;

  // Say nothing. The server should give up and close the connection.
  let mut buf = [0u8; 64];
  let read = tokio::time::timeout(Duration::from_secs(5), peer.read(&mut buf))
    .await
    .map_err(|_| {
      anyhow::anyhow!(
        "server held the connection open well past its {SHORT_LOGON_TIMEOUT:?} \
         logon timeout"
      )
    })??;

  assert_eq!(
    read, 0,
    "expected the server to close, not to send something"
  );
  let elapsed = started.elapsed();
  assert!(
    elapsed >= SHORT_LOGON_TIMEOUT,
    "closed after {elapsed:?}, before the configured {SHORT_LOGON_TIMEOUT:?}"
  );
  assert!(
    elapsed < Duration::from_secs(5),
    "closed after {elapsed:?}; the configured timeout was ignored"
  );

  Ok(())
}

/// Closing an initiator's command channel stops the reconnect loop rather than
/// leaking a task that retries forever.
#[test_log::test(tokio::test)]
async fn shutting_down_an_initiator_stops_it_reconnecting() -> anyhow::Result<()>
{
  // Port 1 on loopback refuses instantly, so the initiator spends its life in
  // the backoff ladder — which is exactly where shutdown has to be observed.
  let initiator = endpoint::connect(
    vec![("127.0.0.1".to_string(), 1)],
    FIX_REPO.clone(),
    fix::session::SessionIdentifier {
      begin_string: "FIX.4.4".into(),
      sender_comp_id: "CLIENT".into(),
      target_comp_id: "SERVER".into(),
    },
    fix::session::Session::new(FIX_REPO.get_version("FIX.4.4").unwrap()),
    EndpointConfig::default()
      .connect_timeout(Duration::from_millis(50))
      .backoff([Duration::from_millis(10)]),
  )?;

  let endpoint::Initiator {
    session,
    commands,
    join_handle,
  } = initiator;
  // The handle exists before anything has connected, which is the whole point:
  // a synchronous caller can take it and wire up the async half afterwards.
  assert_eq!(session.session_id.sender_comp_id, "CLIENT");

  // Let it fail a few times, then shut it down.
  tokio::time::sleep(Duration::from_millis(100)).await;
  drop(commands);

  let result = tokio::time::timeout(Duration::from_secs(5), join_handle)
    .await
    .map_err(|_| {
      anyhow::anyhow!("initiator kept reconnecting after its channel closed")
    })?;
  assert!(
    result.is_ok(),
    "clean shutdown should not be an error: {result:?}"
  );

  Ok(())
}

/// An empty endpoint list is rejected up front rather than spinning a task that
/// can never succeed.
#[test_log::test(tokio::test)]
async fn connecting_to_nothing_is_an_error() {
  let result = endpoint::connect(
    vec![],
    FIX_REPO.clone(),
    fix::session::SessionIdentifier {
      begin_string: "FIX.4.4".into(),
      sender_comp_id: "CLIENT".into(),
      target_comp_id: "SERVER".into(),
    },
    fix::session::Session::new(FIX_REPO.get_version("FIX.4.4").unwrap()),
    EndpointConfig::default(),
  );
  assert!(result.is_err());
}
