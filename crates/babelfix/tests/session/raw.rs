//! A byte-level FIX counterparty for tests.
//!
//! [`RawPeer`] speaks the wire protocol directly instead of running a babelfix
//! session, which lets a test do things a compliant engine never would — send a
//! message out of sequence, gap fill over the wrong range, fall silent, or drop
//! the socket mid-exchange — and observe exactly what the session under test
//! puts on the wire in response.
//!
//! ```ignore
//! let (session_id, server, port) =
//!   session::serve("SERVER", SessionOptions::default(), "CLIENT", fix44).await?;
//! let mut peer = RawPeer::connect(port, fix44, "CLIENT", "SERVER").await?;
//! peer.logon(Duration::from_secs(30)).await?;
//! let logon_ack = peer.recv().await?;
//! ```

use super::DICTS;
use super::fix;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use fix::message::Message;
use fix::schema::tags as Fields;

/// How long [`RawPeer::recv`] and friends wait for the session under test to
/// respond before failing the test.
const RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const SOH: u8 = b'\x01';

/// Read a tag's value out of a wire message, or `None` if it is absent.
///
/// Returns the last occurrence, which is what the session layer's own header
/// handling effectively sees for the non-repeating tags tests care about.
pub fn tag_value(msg: &Message, tag: u32) -> Option<&str> {
  msg
    .walk()
    .filter(|c| c.tag() == tag)
    .filter_map(|c| c.value())
    .last()
    .and_then(|v| std::str::from_utf8(v).ok())
}

/// A message to put on the wire, described in terms of raw tags.
///
/// `SenderCompID`, `TargetCompID` and `SendingTime` are filled in by
/// [`RawPeer::send`] unless the test overrides them via [`RawMessage::header`];
/// `MsgSeqNum` comes from the peer's own counter unless pinned with
/// [`RawMessage::seq`].
#[derive(Debug, Clone)]
pub struct RawMessage {
  msg_type: String,
  seq_num: Option<u32>,
  header: Vec<(u32, String)>,
  body: Vec<(u32, String)>,
}

impl RawMessage {
  pub fn new(msg_type: &str) -> Self {
    Self {
      msg_type: msg_type.to_string(),
      seq_num: None,
      header: Vec::new(),
      body: Vec::new(),
    }
  }

  /// Send with this exact `MsgSeqNum`, leaving the peer's counter untouched.
  /// Without it the peer consumes and increments its own next outbound number.
  pub fn seq(mut self, seq_num: u32) -> Self {
    self.seq_num = Some(seq_num);
    self
  }

  pub fn header(mut self, tag: u32, value: impl Into<String>) -> Self {
    self.header.push((tag, value.into()));
    self
  }

  pub fn body(mut self, tag: u32, value: impl Into<String>) -> Self {
    self.body.push((tag, value.into()));
    self
  }
}

pub struct RawPeer {
  stream: tokio::net::TcpStream,
  buf: bytes::BytesMut,
  fix_version: Arc<fix::message::Dictionary>,
  sender_comp_id: String,
  target_comp_id: String,
  /// The `MsgSeqNum` the next unpinned [`RawPeer::send`] will use.
  pub next_out_seq_num: u32,
}

impl RawPeer {
  /// Connect to an acceptor on `127.0.0.1:port` without sending anything.
  pub async fn connect(
    port: u16,
    fix_version: Arc<fix::message::Dictionary>,
    sender_comp_id: impl Into<String>,
    target_comp_id: impl Into<String>,
  ) -> anyhow::Result<Self> {
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    stream.set_nodelay(true)?;
    Ok(Self {
      stream,
      buf: bytes::BytesMut::with_capacity(4096),
      fix_version,
      sender_comp_id: sender_comp_id.into(),
      target_comp_id: target_comp_id.into(),
      next_out_seq_num: 1,
    })
  }

  /// Start this peer's outbound sequence somewhere other than 1, as if it were
  /// resuming a session.
  pub fn starting_at(mut self, next_out_seq_num: u32) -> Self {
    self.next_out_seq_num = next_out_seq_num;
    self
  }

  /// Send a Logon(35=A) with the given heartbeat interval.
  pub async fn logon(
    &mut self,
    heartbeat_interval: std::time::Duration,
  ) -> anyhow::Result<u32> {
    self
      .send(
        RawMessage::new("A")
          .body(Fields::EncryptMethod, "0")
          .body(Fields::HeartBtInt, heartbeat_interval.as_secs().to_string()),
      )
      .await
  }

  /// Connect, log on, and answer the acceptor's synchronisation TestRequest.
  ///
  /// On return both sides are synchronised and both next outbound sequence
  /// numbers are 3: the peer has sent a Logon (1) and a Heartbeat (2), the
  /// acceptor a Logon acknowledgement (1) and a TestRequest (2).
  pub async fn connect_and_logon(
    port: u16,
    fix_version: Arc<fix::message::Dictionary>,
    sender_comp_id: impl Into<String>,
    target_comp_id: impl Into<String>,
  ) -> anyhow::Result<Self> {
    let mut peer =
      Self::connect(port, fix_version, sender_comp_id, target_comp_id).await?;
    peer.logon(std::time::Duration::from_secs(30)).await?;

    let ack = peer.recv().await?;
    anyhow::ensure!(
      ack.msg_type() == "A",
      "expected a Logon acknowledgement, got {}",
      ack
    );

    let test_request = peer.recv().await?;
    anyhow::ensure!(
      test_request.msg_type() == "1",
      "expected a synchronisation TestRequest, got {}",
      test_request
    );
    let test_req_id = tag_value(&test_request, Fields::TestReqID)
      .ok_or_else(|| anyhow::anyhow!("TestRequest without a TestReqID"))?
      .to_string();

    peer
      .send(RawMessage::new("0").body(Fields::TestReqID, test_req_id))
      .await?;

    Ok(peer)
  }

  /// Encode and transmit `msg`, returning the `MsgSeqNum` used.
  pub async fn send(&mut self, msg: RawMessage) -> anyhow::Result<u32> {
    let seq_num = msg.seq_num.unwrap_or(self.next_out_seq_num);
    if msg.seq_num.is_none() {
      self.next_out_seq_num += 1;
    }

    let mut m = Message::new(&self.fix_version, &msg.msg_type);
    m.header_mut()
      .set_raw(Fields::MsgSeqNum, seq_num.to_string().as_bytes())
      .set_raw(Fields::SenderCompID, self.sender_comp_id.as_bytes())
      .set_raw(Fields::TargetCompID, self.target_comp_id.as_bytes())
      .set_raw(Fields::SendingTime, fix::util::time_now_fix().as_bytes());
    for (tag, value) in &msg.header {
      m.header_mut().set_raw(*tag, value.as_bytes());
    }
    for (tag, value) in &msg.body {
      m.body_mut().set_raw(*tag, value.as_bytes());
    }

    let mut out = bytes::BytesMut::new();
    m.encode(&mut out);
    tracing::info!(
      "RawPeer sending: {}",
      String::from_utf8_lossy(&out).replace(SOH as char, "|")
    );
    self.stream.write_all(&out).await?;
    self.stream.flush().await?;
    Ok(seq_num)
  }

  /// Transmit `bytes` verbatim, with no framing, length or checksum fixups.
  pub async fn send_raw(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
    self.stream.write_all(bytes).await?;
    self.stream.flush().await?;
    Ok(())
  }

  /// Wait for the next message from the peer.
  pub async fn recv(&mut self) -> anyhow::Result<Message> {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
      if let Some(msg) = self.take_framed()? {
        tracing::info!("RawPeer received: {}", msg);
        return Ok(msg);
      }
      let read =
        tokio::time::timeout_at(deadline, self.stream.read_buf(&mut self.buf))
          .await
          .map_err(|_| {
            anyhow::anyhow!("Timed out waiting for a message from the peer")
          })??;
      if read == 0 {
        anyhow::bail!("Peer closed the connection while awaiting a message");
      }
    }
  }

  /// Wait for the next message whose `MsgType` is `msg_type`, discarding
  /// Heartbeat(35=0) messages along the way.
  ///
  /// Useful when a test runs with a short heartbeat interval and only cares
  /// about one particular response.
  pub async fn recv_skipping_heartbeats(
    &mut self,
    msg_type: &str,
  ) -> anyhow::Result<Message> {
    loop {
      let msg = self.recv().await?;
      if msg.msg_type() == msg_type {
        return Ok(msg);
      }
      if msg.msg_type() != "0" {
        anyhow::bail!(
          "Expected MsgType {msg_type} but got {}: {}",
          msg.msg_type(),
          msg
        );
      }
    }
  }

  /// Assert the peer closes the connection without sending anything further.
  pub async fn expect_closed(&mut self) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    loop {
      if let Some(msg) = self.take_framed()? {
        anyhow::bail!("Expected the connection to close, but received {}", msg);
      }
      let read =
        tokio::time::timeout_at(deadline, self.stream.read_buf(&mut self.buf))
          .await
          .map_err(|_| {
            anyhow::anyhow!("Timed out waiting for the connection to close")
          })??;
      if read == 0 {
        return Ok(());
      }
    }
  }

  /// Assert that nothing arrives from the peer for `window`.
  pub async fn expect_silence(
    &mut self,
    window: std::time::Duration,
  ) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + window;
    loop {
      if let Some(msg) = self.take_framed()? {
        anyhow::bail!("Expected silence, but received {}", msg);
      }
      match tokio::time::timeout_at(
        deadline,
        self.stream.read_buf(&mut self.buf),
      )
      .await
      {
        // The window elapsed with nothing (or nothing complete) received.
        Err(_) => return Ok(()),
        Ok(read) => {
          if read? == 0 {
            anyhow::bail!(
              "Peer closed the connection during the silent window"
            );
          }
        }
      }
    }
  }

  /// Close the socket, simulating an abrupt loss of the transport layer.
  pub async fn disconnect(mut self) -> anyhow::Result<()> {
    self.stream.shutdown().await?;
    Ok(())
  }

  /// Split one complete message off the front of the read buffer, if there is
  /// one. Mirrors the framing the endpoint codec performs.
  fn take_framed(&mut self) -> anyhow::Result<Option<Message>> {
    let mut decoder = fix::codec::FixDecoder::with_dictionary(
      DICTS.clone(),
      None,
      self.fix_version.clone(),
    );
    Ok(decoder.decode(&mut self.buf)?)
  }
}
