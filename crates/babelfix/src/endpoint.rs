//! TCP endpoint layer: accept or initiate FIX connections.
//!
//! The endpoint owns the wire framing — an internal `tokio_util` codec that
//! splits the byte stream into [`FixMessage`]s and verifies
//! `BodyLength`/`CheckSum` — and spawns a [`session`] task per connection.
//!
//! * [`serve`] binds a listener and returns an [`Endpoint`]. Iterate its `events`
//!   receiver: reply to [`EndpointEvent::NewSession`] with a
//!   [`Session`](crate::session::Session) for the negotiated version, then take
//!   the [`SessionHandle`](crate::session::SessionHandle) from
//!   [`EndpointEvent::SessionConnected`].
//! * [`connect`] initiates an outbound connection (with reconnect and backoff),
//!   emitting the same [`EndpointEvent::SessionConnected`] once logged on.
//!
//! Both take an `Arc<`[`FixRepository`](crate::repository::FixRepository)`>` and
//! an optional field delimiter (`None` uses SOH, `b'\x01'`).
//!
//! # Server
//!
//! ```ignore
//! use std::sync::Arc;
//! use babelfix::{endpoint, session, repository};
//! use futures::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let repo = Arc::new(repository::orchestrate()?);
//!     let mut endpoint = endpoint::serve(9878, None, repo.clone(), None).await?;
//!
//!     while let Some(event) = endpoint.events.next().await {
//!         match event {
//!             endpoint::EndpointEvent::NewSession { session_id, response } => {
//!                 let fix = repo
//!                     .get_version(&session_id.begin_string)
//!                     .ok_or_else(|| anyhow::anyhow!("unknown FIX version"))?;
//!                 let _ = response.send(session::Session::new(fix));
//!             }
//!             endpoint::EndpointEvent::SessionConnected(handle) => {
//!                 // Drive `handle` — see the `session` module docs.
//!                 tokio::spawn(async move { let _ = handle; });
//!             }
//!             endpoint::EndpointEvent::SessionInvalid(_peer) => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```

#![allow(unused_variables)]
use std::sync::Arc;

use futures::SinkExt;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use tracing::{Instrument, debug, error, info, trace};

use super::*;
use crate::repository::FieldBlock;

pub enum EndpointEvent {
  NewSession {
    session_id: session::SessionIdentifier,
    response: oneshot::Sender<Result<session::Session>>,
  },
  SessionInvalid(String), // Partner address{
  SessionConnected(session::SessionHandle),
}

pub enum EndpointCommand {
  Shutdown,
}

pub struct Endpoint<T: Future<Output = Result<()>> + Send + 'static> {
  pub events: mpsc::Receiver<EndpointEvent>,
  pub commands: mpsc::Sender<EndpointCommand>,
  pub local_addr: std::net::SocketAddr,
  pub join_handle: T,
}

#[derive(Default)]
struct FixDecoder {
  repo: Arc<crate::repository::FixRepository>,
  delimiter: Option<u8>,
  fix_version: Option<Arc<crate::repository::FixVersion>>,
}

impl tokio_util::codec::Decoder for FixDecoder {
  type Item = crate::message::FixMessage;
  type Error = Error;

  fn decode(
    &mut self,
    data: &mut bytes::BytesMut,
  ) -> std::result::Result<Option<Self::Item>, Self::Error> {
    // FIXME: PIGGY PIGGY PIGGY PORKER SO MANY UNNECESSARY PARSES IN THE CASE OF
    // FRAGMENT
    //  We can store the begin_string_len and body_len after parsing and skip
    //  it if we already parsed it once, rather than re-parsing the fragment
    //  every time
    let begin_string_len;
    let body_len;
    if self.fix_version.is_none() {
      (self.fix_version, body_len, begin_string_len) =
        crate::message::peek_infer_version_and_length(
          self.repo.as_ref(),
          data,
          self.delimiter.unwrap_or(b'\x01'),
        )?;
    } else {
      (_, body_len, begin_string_len) =
        crate::message::peek_infer_version_and_length(
          self.repo.as_ref(),
          data,
          self.delimiter.unwrap_or(b'\x01'),
        )?;
    }

    let (Some(fix_version), Some(msg_len)) =
      (self.fix_version.as_ref(), body_len)
    else {
      return Ok(None);
    };

    if data.len() <= begin_string_len + msg_len {
      // Either part message or no additional checksum
      return Ok(None);
    }

    let (Some(checksum), checksum_len) = crate::message::peek_checksum(
      self.repo.as_ref(),
      &data[begin_string_len + msg_len..],
      self.delimiter.unwrap_or(b'\x01'),
    )?
    else {
      return Ok(None);
    };

    let buf = data.split_to(begin_string_len + msg_len + checksum_len);
    let mut calc_checksum: u8 = 0;
    for ch in buf[..begin_string_len + msg_len].iter() {
      calc_checksum = calc_checksum.wrapping_add(*ch);
    }

    if checksum != calc_checksum {
      return Err(Error::invalid_message(format!(
        "Invalid checksum; expected {calc_checksum} but got {checksum}",
      )));
    }

    // Convert BytesMut to Bytes (zero-copy)
    let bytes = buf.freeze();
    let (fix_msg, consumed) = FixMessage::from_bytes_delimited(
      fix_version.clone(),
      bytes,
      self.delimiter.unwrap_or(b'\x01'),
    )?;

    if consumed != begin_string_len + msg_len + checksum_len {
      return Err(Error::invalid_message(
        "Consumed length does not match expected length",
      ));
    }

    debug!("Decoded FIX message: {:?}", fix_msg);
    Ok(Some(fix_msg))
  }
}

struct FixEncoder {
  delimiter: Option<u8>,
}

impl tokio_util::codec::Encoder<crate::message::FixMessage> for FixEncoder {
  type Error = Error;

  fn encode(
    &mut self,
    item: crate::message::FixMessage,
    dst: &mut bytes::BytesMut,
  ) -> std::result::Result<(), Self::Error> {
    item.write_to(dst, self.delimiter.unwrap_or(b'\x01'))?;

    debug!("Encoded FIX message: {:?}", dst);
    Ok(())
  }
}

pub async fn connect(
  endpoints: Vec<(String, u16)>,
  event_sender: mpsc::Sender<EndpointEvent>,
  repo: Arc<crate::repository::FixRepository>,
  delimiter: Option<u8>,
  session_id: session::SessionIdentifier,
  session: session::Session,
  mut cancellation_token: futures::channel::oneshot::Receiver<()>,
) -> Result<()> {
  for (host, port) in endpoints.iter().cycle() {
    for backoff in [10, 100, 1000, 5000, 5000] {
      info!("Connecting to {}:{}", host, port);
      let connect_result = tokio::select! {
        _ = &mut cancellation_token => return Ok(()),
        result = tokio::time::timeout(
          std::time::Duration::from_secs(10),
          tokio::net::TcpStream::connect((host.as_str(), *port)),
        ) => result,
      };

      match connect_result {
        Ok(Ok(stream)) => {
          trace!("Connected to {host}:{port}; logging in");
          stream.set_nodelay(true)?;
          // TODO: we should maybe handle logon errors differently and retry
          // etc
          return initiate_connection(
            stream,
            event_sender,
            repo,
            delimiter,
            session_id,
            session,
          )
          .await;
        }
        Ok(Err(e)) => trace!("Failed to connect to {host}:{port}: {e}"),
        Err(_) => trace!("Failed to connect to {host}:{port}: timeout"),
      }

      tokio::select! {
        _ = &mut cancellation_token => return Ok(()),
        _ = tokio::time::sleep(std::time::Duration::from_millis(backoff)) => {}
      }
    }
  }

  Err(Error::connection_failed(format!(
    "Failed to connect to any endpoint: {:?}",
    endpoints
  )))
}

fn make_logon_message(
  fix_version: &Arc<crate::repository::FixVersion>,
  session: &session::Session,
) -> Result<crate::message::builder::Message> {
  let mut logon_msg =
    crate::message::builder::Message::new(fix_version.clone(), "A")?;

  logon_msg.body.set_tag(
    crate::schema::FIX_Latest::Fields::HeartBtInt,
    session.heartbeat_interval.as_secs().to_string(),
  );
  if logon_msg.fix_message.is_member(
    fix_version.as_ref(),
    crate::schema::FIX_Latest::Fields::DefaultApplVerID,
  ) {
    logon_msg.body.set_tag(
      crate::schema::FIX_Latest::Fields::DefaultApplVerID,
      "10", // TODO: Default application version: FIXLatest
    );
  }
  logon_msg.body.set_tag(
    crate::schema::FIX_Latest::Fields::EncryptMethod,
    "0", // No encryption
  );
  Ok(logon_msg)
}

async fn initiate_connection(
  mut stream: tokio::net::TcpStream,
  mut event_sender: mpsc::Sender<EndpointEvent>,
  repo: Arc<crate::repository::FixRepository>,
  delimiter: Option<u8>,
  session_id: session::SessionIdentifier,
  mut session: session::Session,
) -> Result<()> {
  let peer_addr = stream
    .peer_addr()
    .map_or("Unknown".to_string(), |addr| addr.to_string());
  let (rx, tx) = stream.split();
  let mut rx = tokio_util::codec::FramedRead::new(
    rx,
    FixDecoder {
      repo: repo.clone(),
      delimiter,
      fix_version: Some(session.fix_version.clone()),
    },
  );
  let mut tx =
    tokio_util::codec::FramedWrite::new(tx, FixEncoder { delimiter });

  let span = tracing::info_span!(
    "ClientSession",
    local_comp_id = session_id.sender_comp_id,
    remote_comp_id = session_id.target_comp_id,
  );

  async {
    let (session_send, session_recv) =
      mpsc::channel::<session::SessionCommand>(100);
    let (mut session_event_sender, session_event_recv) =
      mpsc::channel::<session::SessionEvent>(100);

    session_event_sender
      .send(session::SessionEvent::ConnectionEstablished)
      .await.map_err(crate::chan_closed)?;

    session
      .send(
        make_logon_message(&session.fix_version, &session)?,
        &session_id,
        &mut tx,
        &mut session_event_sender,
      )
      .await?;

    let mut connect_timer = tokio::time::interval_at(
      tokio::time::Instant::now() + std::time::Duration::from_secs(30),
      std::time::Duration::from_secs(30),
    );

    // First message has to be a logon
    let logon_fix_msg = tokio::select! {
      _ = connect_timer.tick() => {
        return Err(Error::connection_failed("Connection timed out after 30 seconds"));
      },
      msg_event = rx.next() => {
        match msg_event {
          Some(Ok(msg)) => msg,
          Some(Err(e)) => {
            return Err(Error::connection_failed(format!("Failed to read first message: {e}")));
          }
          None => {
            event_sender.send(EndpointEvent::SessionInvalid(
              peer_addr
            )).await.map_err(crate::chan_closed)?;
            return Err(Error::connection_failed("Connection closed before first message"));
          }
        }
      },
      else => {
        return Err(Error::protocol_violation("First mesasge was not a logon, or invalid data received"));
      }
    };

    let logon_msg =
      crate::message::builder::Message::from_message(&logon_fix_msg)?;

    session_event_sender
      .send(session::SessionEvent::RawMessageReceived(
        logon_fix_msg,
        session.clone(),
      ))
      .await.map_err(crate::chan_closed)?;

    let session_handle = session::SessionHandle {
      session_id: session_id.clone(),
      tx: session_send,
      events: session_event_recv,
    };

    event_sender
      .send(EndpointEvent::SessionConnected(session_handle))
      .await.map_err(crate::chan_closed)?;

    // Create a disconnecter to ensure we send a disconnect event when the
    // SessionManager is dropped
    let disconnector = Disconnector {
      session_event_sender: session_event_sender.clone(),
    };

    if logon_msg.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon_msg
      )));
    }

    let mut session_manager = session::SessionManager::new(
      rx,
      tx,
      session_id,
      session,
      session_event_sender,
      session_recv,
    );

    session_manager.run(logon_msg).await
  }
  .instrument(span)
  .await
}

struct Disconnector {
  session_event_sender: mpsc::Sender<session::SessionEvent>,
}

impl Drop for Disconnector {
  fn drop(&mut self) {
    // Send a last-ditch disconnect request and bail.
    self
      .session_event_sender
      .try_send(session::SessionEvent::Disconnected)
      .ok();
  }
}

async fn accept_connection(
  mut stream: tokio::net::TcpStream,
  mut event_sender: mpsc::Sender<EndpointEvent>,
  repo: Arc<crate::repository::FixRepository>,
  delimiter: Option<u8>,
) -> Result<()> {
  info!("Handling new client connection");
  let partner = stream
    .peer_addr()
    .map_or("Unknown".to_string(), |addr| addr.to_string());
  let (rx, tx) = stream.split();
  let mut rx = tokio_util::codec::FramedRead::new(
    rx,
    FixDecoder {
      repo: repo.clone(),
      delimiter,
      ..Default::default()
    },
  );
  let mut tx =
    tokio_util::codec::FramedWrite::new(tx, FixEncoder { delimiter });

  let (session_send, session_recv) =
    mpsc::channel::<session::SessionCommand>(100);
  let (mut session_event_sender, session_event_recv) =
    mpsc::channel::<session::SessionEvent>(100);

  let mut connect_timer = tokio::time::interval_at(
    tokio::time::Instant::now() + std::time::Duration::from_secs(30),
    std::time::Duration::from_secs(30),
  );

  // First message has to be a logon
  let logon_fix_msg = tokio::select! {
    _ = connect_timer.tick() => {
      return Err(Error::connection_failed("Connection timed out after 30 seconds"));
    },
    msg_event = rx.next() => {
      match msg_event {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => {
          return Err(Error::connection_failed(format!("Failed to read first message: {e}")));
        }
        None => {
          event_sender.send(EndpointEvent::SessionInvalid(partner)).await.map_err(crate::chan_closed)?;
          return Err(Error::connection_failed("Connection closed before first message"));
        }
      }
    },
    else => {
      return Err(Error::protocol_violation("First mesasge was not a logon, or invalid data received"));
    }
  };

  session_event_sender
    .send(session::SessionEvent::ConnectionEstablished)
    .await
    .map_err(crate::chan_closed)?;

  // Create a disconnecter to ensure we send a disconnect event when the
  // SessionManager is dropped
  let disconnector = Disconnector {
    session_event_sender: session_event_sender.clone(),
  };

  let logon_msg =
    crate::message::builder::Message::from_message(&logon_fix_msg)?;

  let session_id = session::SessionIdentifier {
    begin_string: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::BeginString)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing BeginString")
      })?
      .as_string(),
    sender_comp_id: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::TargetCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing TargetCompID")
      })?
      .as_string(),
    target_comp_id: logon_msg
      .header
      .tag(crate::schema::FIX_Latest::Fields::SenderCompID)
      .ok_or_else(|| {
        Error::protocol_violation("Logon message missing SenderCompID")
      })?
      .as_string(),
  };

  let (set_session, get_session) =
    oneshot::channel::<Result<session::Session>>();
  event_sender
    .send(EndpointEvent::NewSession {
      session_id: session_id.clone(),
      response: set_session,
    })
    .await
    .map_err(crate::chan_closed)?;
  let mut session = get_session.await.map_err(crate::chan_closed)??;

  session_event_sender
    .send(session::SessionEvent::RawMessageReceived(
      logon_fix_msg,
      session.clone(),
    ))
    .await
    .map_err(crate::chan_closed)?;

  let span = tracing::info_span!(
    "ServerSession",
    local_comp_id = session_id.sender_comp_id,
    remote_comp_id = session_id.target_comp_id,
  );

  async {
    let session_handle = session::SessionHandle {
      session_id: session_id.clone(),
      tx: session_send,
      events: session_event_recv,
    };

    event_sender
      .send(EndpointEvent::SessionConnected(session_handle))
      .await
      .map_err(crate::chan_closed)?;

    if logon_msg.fix_message.msg_type.as_str() != "A" {
      return Err(Error::protocol_violation(format!(
        "First message was not a logon, got: {:?}",
        logon_msg
      )));
    }

    session
      .send(
        make_logon_message(&session.fix_version, &session)?,
        &session_id,
        &mut tx,
        &mut session_event_sender,
      )
      .await?;

    let mut session_manager = session::SessionManager::new(
      rx,
      tx,
      session_id,
      session,
      session_event_sender,
      session_recv,
    );

    session_manager.run(logon_msg).await
  }
  .instrument(span)
  .await
}

// TODO: Buidler pattern, or config obejct?
pub async fn serve(
  port: u16,
  host: Option<String>,
  repo: Arc<crate::repository::FixRepository>,
  delimieter: Option<u8>,
) -> Result<Endpoint<impl Future<Output = Result<()>> + Send + 'static>> {
  let (event_sender, event_receiver) = mpsc::channel::<EndpointEvent>(100);
  let (command_sender, mut command_receiver) =
    mpsc::channel::<EndpointCommand>(100);

  let listener =
    tokio::net::TcpListener::bind((host.unwrap_or("0.0.0.0".into()), port))
      .await?;

  let local_addr = listener.local_addr()?;

  let join_handle = tokio::spawn(async move {
    loop {
      tokio::select! {
        maybe_command = command_receiver.next() => {
          match maybe_command {
            Some(EndpointCommand::Shutdown) => {
              info!("Shutting down server");
              break;
            },
            None => {
              info!("Command channel closed, shutting down server");
              break;
            },
          }
        }
        maybe_socket = listener.accept() => {
          match maybe_socket {
            Ok((stream, sockaddr)) => {
              stream.set_nodelay(true)?;
              let s = tracing::info_span!("ClientConnection", %sockaddr);
              let event_sender = event_sender.clone();
              let repo = repo.clone();
              tokio::spawn(crate::util::wrap_and_report(async move {
                accept_connection(
                  stream,
                  event_sender,
                  repo,
                  delimieter).instrument(s).await
              }));
            },
            Err(e) => {
              error!("Failed to accept TCP connection: {e}");
              continue;
            }
          }
        }
      }
    }
    Ok(())
  });

  let resolve_join_handle = async move {
    match join_handle.await {
      Ok(Ok(())) => Ok(()),
      Ok(Err(e)) => Err(e),
      Err(e) => Err(Error::unspecified(format!("Server task panicked: {e:?}"))),
    }
  };

  Ok(Endpoint {
    commands: command_sender,
    events: event_receiver,
    local_addr,
    join_handle: resolve_join_handle,
  })
}
