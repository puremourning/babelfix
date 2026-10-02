//! Parsing: one pass over the bytes, building the tape.
//!
//! The rules (TagValue Encoding v1.0, "TV"):
//!
//! | Rule | Behaviour |
//! |---|---|
//! | 8, 9, 35 first, in that order; 10 last (TV §4.3.3) | enforced |
//! | header, then body, then trailer (TV §4.3.3) | a header tag after the body has begun: reason 14 |
//! | no empty values (TV §4.2.5) | reason 4 |
//! | NumInGroup matches the instances present | reason 16 |
//! | a data field immediately follows its Length (TV §4.2.5) | enforced; its value is exactly Length bytes, SOH and all |
//! | field order *within* a group instance (TV §4.3.6.3) | not checked: fields join an instance by membership, and an instance starts at its delimiter |
//! | unknown tags | kept |
//! | a tag at most once per message, or per group instance (TV §4.3.2) | not checked; see below |
//!
//! Duplicate tags are seen in the wild, whether to tolerate them is a matter of
//! counterparty agreement, and checking costs a scan per field — so parsing
//! keeps them, and reads see the first occurrence. Call
//! [`Message::validate_strict`] to hold a message to the rule (reason 13), and
//! to field order within group instances (reason 15).
//!
//! An unknown group is caught by the same rule: its NumInGroup looks like a
//! plain field, so its second instance's first field is a duplicate. A
//! one-instance unknown group cannot be told apart from a set of unknown tags.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::SOH;
use super::dict::{Dictionary, FieldKind, GroupIdx};
use super::error::{ParseError, reject_reason::*};
use super::tape::{Entry, Kind, Message, Region, Seg};
use super::types::parse_uint_value;

/// How much framing to insist on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Framing {
  /// A complete message: 8, 9, 35 first; 10 last; BodyLength and CheckSum
  /// correct.
  Strict,
  /// A fragment typed or pasted by a person: 8, 9 and 10 are optional and not
  /// checked, but 35 is required. Missing 8 is supplied. A header field after
  /// the body has begun is moved into the header rather than rejected — the
  /// ordering rule is for the wire, not for what a person types.
  Fragment,
}

/// An open repeating group.
struct Frame {
  group: GroupIdx,
  /// Tape index of the Group entry.
  entry: usize,
  /// Tape index of the current Instance entry, once one has started.
  instance: Option<usize>,
  declared: u64,
  count: u64,
  /// Depth of the Group entry; its instances are one deeper.
  depth: u8,
}

struct Parser<'d> {
  dict: &'d Dictionary,
  framing: Framing,
  msg: Message,
  stack: Vec<Frame>,
  region: Region,
  /// Fields seen so far, for the positional rules on 8, 9 and 35.
  ordinal: usize,
  /// The checksum's field start and value, once seen.
  checksum: Option<(usize, u8)>,
  /// BodyLength's value, and where the body it measures starts.
  body_length: Option<(u64, usize)>,
  /// Where each field's terminating delimiter is, when asked to record them.
  terminators: Option<Vec<usize>>,
}

impl Message {
  /// Parse a complete, SOH-delimited message: `8=...|9=...|35=...|...|10=...|`.
  ///
  /// The bytes are kept, not copied: the message's fields point into them.
  /// BodyLength and CheckSum are verified.
  ///
  /// The bytes must be exactly one message: anything after the CheckSum is an
  /// error. To take messages off a stream, use
  /// [`FixDecoder::decode`](crate::codec::FixDecoder::decode).
  pub fn parse(
    dict: &Arc<Dictionary>,
    wire: impl Into<Bytes>,
  ) -> Result<Message, ParseError> {
    let wire = wire.into();
    let (msg, _) = Parser::new(dict, Framing::Strict).run(wire, SOH)?;
    Ok(msg)
  }

  /// Parse a complete message delimited by `delimiter` instead of SOH — `b'|'`
  /// for logs and tests. The bytes are copied, with the delimiters replaced by
  /// SOH.
  ///
  /// A data field's contents are sized by its Length and taken as they are, so
  /// a `|` inside one stays a `|`. A log that turned SOH *inside* a data field
  /// into `|` as well has lost that byte: the data reads back with `|`, and
  /// the CheckSum will not match.
  pub fn parse_delimited(
    dict: &Arc<Dictionary>,
    bytes: &[u8],
    delimiter: u8,
  ) -> Result<Message, ParseError> {
    Self::parse_copy(dict, bytes, delimiter, Framing::Strict)
  }

  /// Parse a fragment of a message as a person would type it: `35=D|55=X|...`,
  /// delimited by `delimiter`. `BeginString`, `BodyLength` and `CheckSum` are
  /// optional and not checked (they are recomputed when the message is
  /// encoded); `MsgType` is required. Header fields may appear anywhere, and
  /// are moved into the header — except header groups (NoHops), which must
  /// come before the body, since their instances would be indistinguishable
  /// from body fields. Every other rule applies.
  pub fn parse_fragment(
    dict: &Arc<Dictionary>,
    bytes: &[u8],
    delimiter: u8,
  ) -> Result<Message, ParseError> {
    Self::parse_copy(dict, bytes, delimiter, Framing::Fragment)
  }

  fn parse_copy(
    dict: &Arc<Dictionary>,
    bytes: &[u8],
    delimiter: u8,
    framing: Framing,
  ) -> Result<Message, ParseError> {
    let mut buf = BytesMut::from(bytes);
    // A fragment may stop without a final delimiter.
    if framing == Framing::Fragment && buf.last() != Some(&delimiter) {
      buf.extend_from_slice(&[delimiter]);
    }
    let mut parser = Parser::new(dict, framing);
    parser.terminators = Some(Vec::new());
    let (mut msg, terminators) = parser.run(buf.clone().freeze(), delimiter)?;
    if delimiter != SOH {
      // Replace each field's terminating delimiter, leaving any
      // delimiter-valued bytes inside data fields alone.
      for at in terminators.unwrap_or_default() {
        buf[at] = SOH;
      }
      msg.wire = buf.freeze();
      msg.clean = framing == Framing::Strict;
    }
    if framing == Framing::Fragment {
      msg.clean = false;
    }
    Ok(msg)
  }
}

impl<'d> Parser<'d> {
  fn new(dict: &'d Arc<Dictionary>, framing: Framing) -> Self {
    Self {
      dict,
      framing,
      msg: Message {
        dict: dict.clone(),
        msg_def: None,
        wire: Bytes::new(),
        arena: Vec::new(),
        tape: Vec::with_capacity(64),
        body_start: 0,
        trailer_start: 0,
        clean: false,
      },
      stack: Vec::new(),
      region: Region::Header,
      ordinal: 0,
      checksum: None,
      body_length: None,
      terminators: None,
    }
  }

  #[allow(clippy::type_complexity)]
  fn run(
    mut self,
    wire: Bytes,
    delim: u8,
  ) -> Result<(Message, Option<Vec<usize>>), ParseError> {
    let buf: &[u8] = &wire;
    let mut pos = 0;
    // The data tag the previous field announced, and its length.
    let mut pending_data: Option<(u32, usize)> = None;

    while pos < buf.len() {
      let field_start = pos;
      let (tag, val_start) = parse_tag(buf, pos, delim)?;

      let (kind, val_end) = match pending_data.take() {
        Some((data_tag, n)) => {
          if tag != data_tag {
            return Err(ParseError::reject(
              INCORRECT_DATA_FORMAT_FOR_VALUE,
              data_tag,
              format!("data field {data_tag} must follow its length field"),
            ));
          }
          // `n` comes off the wire: never trust it to stay in bounds.
          let Some(end) = val_start.checked_add(n).filter(|&e| e < buf.len())
          else {
            return Err(ParseError::garbled(format!(
              "data field {tag} is not {n} bytes long"
            )));
          };
          if buf[end] != delim {
            return Err(ParseError::garbled(format!(
              "data field {tag} is not {n} bytes long"
            )));
          }
          (Kind::Data, end)
        }
        None => {
          let Some(len) = buf[val_start..].iter().position(|&c| c == delim)
          else {
            return Err(ParseError::garbled(format!(
              "field {tag} has no terminating delimiter"
            )));
          };
          let kind = match self.dict.kind(tag) {
            FieldKind::DataLength { .. } => Kind::DataLen,
            _ => Kind::Field,
          };
          (kind, val_start + len)
        }
      };
      if val_end == val_start {
        return Err(ParseError::reject(
          TAG_SPECIFIED_WITHOUT_A_VALUE,
          tag,
          format!("tag {tag} has no value"),
        ));
      }
      let value = &buf[val_start..val_end];
      if kind == Kind::DataLen
        && let FieldKind::DataLength { data } = self.dict.kind(tag)
      {
        let n = parse_uint_value(value).map_err(|_| {
          ParseError::reject(
            INCORRECT_DATA_FORMAT_FOR_VALUE,
            tag,
            format!("length field {tag} is not a number"),
          )
        })?;
        pending_data = Some((data, n as usize));
      }

      let is_last = self.framing_rules(tag, field_start, value)?;

      let entry = Entry {
        tag,
        kind,
        depth: 0,
        seg: Seg::Wire,
        val_off: (val_start - field_start) as u8,
        off: field_start as u32,
        len: (val_end - val_start) as u32,
      };
      self.push(entry, value)?;

      if let Some(t) = &mut self.terminators {
        t.push(val_end);
      }
      pos = val_end + 1;
      if is_last {
        break;
      }
    }

    if pending_data.is_some() {
      return Err(ParseError::garbled("message ends after a length field"));
    }
    if pos != buf.len() {
      return Err(ParseError::garbled("bytes after CheckSum(10)"));
    }
    while !self.stack.is_empty() {
      self.close_group()?;
    }
    self.finish_regions();

    if self.framing == Framing::Strict {
      let Some((checksum_at, checksum)) = self.checksum else {
        return Err(ParseError::garbled("no CheckSum(10)"));
      };
      if let Some((declared, body_start)) = self.body_length
        && declared != (checksum_at - body_start) as u64
      {
        return Err(ParseError::garbled(format!(
          "BodyLength(9) is {declared} but the body is {} bytes",
          checksum_at - body_start
        )));
      }
      // Each field before 10 ends in one delimiter; a delimited rendering
      // carries the checksum of the SOH original.
      let fields = self.ordinal - 1;
      let actual =
        super::encode::soh_checksum(&buf[..checksum_at], fields, delim);
      if actual != checksum {
        return Err(ParseError::garbled(format!(
          "CheckSum(10) is {checksum:03} but the message sums to {actual:03}"
        )));
      }
    } else if !self.msg.tape.iter().any(|e| e.tag == 8 && e.is_field()) {
      // Supply BeginString so the fragment is a whole message.
      let mut e = self
        .msg
        .append_field(8, self.dict.begin_string().as_bytes());
      e.depth = 0;
      self.msg.tape.insert(0, e);
      self.msg.body_start += 1;
      self.msg.trailer_start += 1;
    }

    if self.msg.msg_def.is_none() && !self.has_msg_type() {
      return Err(ParseError::reject(
        REQUIRED_TAG_MISSING,
        35,
        "no MsgType(35)",
      ));
    }

    self.msg.wire = wire;
    self.msg.clean = self.framing == Framing::Strict && delim == SOH;
    Ok((self.msg, self.terminators))
  }

  fn has_msg_type(&self) -> bool {
    self.msg.tape[..self.msg.body_start as usize]
      .iter()
      .any(|e| e.tag == 35 && e.is_field())
  }

  /// The positional rules on 8, 9, 35 and 10. Returns whether this field ends
  /// the message.
  fn framing_rules(
    &mut self,
    tag: u32,
    field_start: usize,
    value: &[u8],
  ) -> Result<bool, ParseError> {
    let ordinal = self.ordinal;
    self.ordinal += 1;
    let strict = self.framing == Framing::Strict;

    let expected = match ordinal {
      0 => Some(8),
      1 => Some(9),
      2 => Some(35),
      _ => None,
    };
    if strict {
      if let Some(expected) = expected {
        if tag != expected {
          return Err(ParseError::garbled(format!(
            "field {} must be {expected}, not {tag}",
            ordinal + 1
          )));
        }
      } else if matches!(tag, 8 | 9 | 35) {
        return Err(ParseError::reject(
          TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER,
          tag,
          format!("tag {tag} must be among the first three fields"),
        ));
      }
    }

    match tag {
      8 if value != self.dict.begin_string().as_bytes() => {
        Err(ParseError::garbled(format!(
          "BeginString(8) is {}, expected {}",
          String::from_utf8_lossy(value),
          self.dict.begin_string()
        )))
      }
      9 => {
        let declared = parse_uint_value(value)
          .map_err(|_| ParseError::garbled("BodyLength(9) is not a number"))?;
        let body_start = field_start + 2 + value.len() + 1;
        self.body_length = Some((declared, body_start));
        Ok(false)
      }
      35 => {
        if !value.iter().all(|c| c.is_ascii_alphanumeric()) {
          return Err(ParseError::reject(
            INVALID_MSG_TYPE,
            35,
            "MsgType(35) is not alphanumeric",
          ));
        }
        self.msg.msg_def = self.dict.message(value);
        Ok(false)
      }
      10 => {
        let checksum = match value {
          [a, b, c] if value.iter().all(u8::is_ascii_digit) => {
            let v = (a - b'0') as u32 * 100
              + (b - b'0') as u32 * 10
              + (c - b'0') as u32;
            u8::try_from(v)
              .map_err(|_| ParseError::garbled("CheckSum(10) exceeds 255"))?
          }
          _ => {
            return Err(ParseError::garbled(
              "CheckSum(10) is not three digits",
            ));
          }
        };
        self.checksum = Some((field_start, checksum));
        Ok(true)
      }
      _ => Ok(false),
    }
  }

  /// Place a parsed field on the tape: close groups it does not belong to,
  /// start an instance if it is a delimiter, move between regions, and open a
  /// group if it is a NumInGroup.
  fn push(&mut self, mut entry: Entry, value: &[u8]) -> Result<(), ParseError> {
    let tag = entry.tag;

    let dict = self.dict;
    // Which open group, if any, does this field belong to?
    while let Some(frame) = self.stack.last() {
      let def = dict.group(frame.group);
      if tag == def.delimiter {
        self.start_instance(tag)?;
        break;
      }
      if def.members.contains_key(&tag) {
        if frame.instance.is_none() {
          return Err(ParseError::reject(
            REPEATING_GROUP_FIELDS_OUT_OF_ORDER,
            tag,
            format!(
              "group {} must start with {}, not {tag}",
              def.num_in_group, def.delimiter
            ),
          ));
        }
        break;
      }
      self.close_group()?;
    }

    let depth = match self.stack.last() {
      Some(frame) => frame.depth + 1,
      None => {
        if self.framing == Framing::Fragment
          && self.region != Region::Header
          && self.dict.is_header(tag)
        {
          // A late header group's instances would follow it into the body,
          // where they cannot be told apart from body fields.
          if entry.kind == Kind::Field
            && self
              .dict
              .top_level_group(self.msg.msg_def, true, tag)
              .is_some()
          {
            return Err(ParseError::reject(
              TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER,
              tag,
              format!(
                "header group {tag} must come before the body, even in a fragment"
              ),
            ));
          }
          // A field, or a data field's Length and then the data field itself:
          // each moves to the header in turn, so the pair stays together.
          return self.push_late_header(entry);
        }
        self.enter_region(tag)?;
        0
      }
    };
    entry.depth = depth;

    let group = if entry.kind == Kind::Field {
      match self.stack.last() {
        Some(frame) => self.dict.nested_group(frame.group, tag),
        None => self.dict.top_level_group(
          self.msg.msg_def,
          self.region == Region::Header,
          tag,
        ),
      }
    } else {
      None
    };

    match group {
      Some(group) => {
        let declared = parse_uint_value(value).map_err(|_| {
          ParseError::reject(
            INCORRECT_NUM_IN_GROUP_COUNT,
            tag,
            format!("NumInGroup {tag} is not a number"),
          )
        })?;
        let idx = self.msg.tape.len();
        self.msg.tape.push(Entry {
          tag,
          kind: Kind::Group,
          depth,
          off: 0,
          len: 1,
          ..Default::default()
        });
        if declared > 0 {
          self.stack.push(Frame {
            group,
            entry: idx,
            instance: None,
            declared,
            count: 0,
            depth,
          });
        }
      }
      None => self.msg.tape.push(entry),
    }
    Ok(())
  }

  /// A fragment's header field that arrived after the body began: put it at the
  /// end of the header. No group is open (the field closed them all), so
  /// nothing holds a tape index that the insert could invalidate.
  fn push_late_header(&mut self, mut entry: Entry) -> Result<(), ParseError> {
    entry.depth = 0;
    match self.msg.header_gap() {
      Some(gap) => {
        let room = self.msg.tape[gap as usize].span();
        self.msg.tape[gap as usize] = entry;
        if room > 1 {
          self.msg.tape[gap as usize + 1] = Entry::gap(room - 1);
        }
      }
      None => {
        let at = self.msg.body_start as usize;
        self.msg.tape.insert(at, entry);
        self.msg.body_start += 1;
        if self.region == Region::Trailer {
          self.msg.trailer_start += 1;
        }
      }
    }
    Ok(())
  }

  /// The field is the open group's delimiter: end the current instance and
  /// start another.
  fn start_instance(&mut self, tag: u32) -> Result<(), ParseError> {
    self.close_instance();
    let tape_len = self.msg.tape.len();
    let frame = self.stack.last_mut().expect("an open group");
    if frame.count == frame.declared {
      return Err(ParseError::reject(
        INCORRECT_NUM_IN_GROUP_COUNT,
        self.msg.tape[frame.entry].tag,
        format!(
          "group {} declares {} instances but has more ({tag} repeats)",
          self.msg.tape[frame.entry].tag, frame.declared
        ),
      ));
    }
    frame.count += 1;
    frame.instance = Some(tape_len);
    let depth = frame.depth + 1;
    self.msg.tape.push(Entry {
      kind: Kind::Instance,
      depth,
      len: 1,
      ..Default::default()
    });
    Ok(())
  }

  fn close_instance(&mut self) {
    let len = self.msg.tape.len();
    if let Some(frame) = self.stack.last()
      && let Some(i) = frame.instance
    {
      self.msg.tape[i].len = (len - i) as u32;
    }
  }

  fn close_group(&mut self) -> Result<(), ParseError> {
    self.close_instance();
    let frame = self.stack.pop().expect("an open group");
    let len = self.msg.tape.len();
    let entry = &mut self.msg.tape[frame.entry];
    entry.len = (len - frame.entry) as u32;
    entry.off = frame.count as u32;
    if frame.count != frame.declared {
      return Err(ParseError::reject(
        INCORRECT_NUM_IN_GROUP_COUNT,
        entry.tag,
        format!(
          "group {} declares {} instances but has {}",
          entry.tag, frame.declared, frame.count
        ),
      ));
    }
    Ok(())
  }

  /// Track the header → body → trailer progression for a top-level field.
  fn enter_region(&mut self, tag: u32) -> Result<(), ParseError> {
    let header = self.dict.is_header(tag);
    let trailer = self.dict.is_trailer(tag);
    match self.region {
      Region::Header if header => {}
      Region::Header => {
        self.msg.push_gap();
        self.msg.body_start = self.msg.tape.len() as u32;
        self.region = Region::Body;
        if trailer {
          self.msg.trailer_start = self.msg.body_start;
          self.region = Region::Trailer;
        }
      }
      Region::Body if header => {
        return Err(ParseError::reject(
          TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER,
          tag,
          format!("header tag {tag} after the body has begun"),
        ));
      }
      Region::Body if trailer => {
        self.msg.trailer_start = self.msg.tape.len() as u32;
        self.region = Region::Trailer;
      }
      Region::Body => {}
      Region::Trailer if trailer => {}
      Region::Trailer => {
        return Err(ParseError::reject(
          TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER,
          tag,
          format!("tag {tag} after the trailer has begun"),
        ));
      }
    }
    Ok(())
  }

  fn finish_regions(&mut self) {
    let len = self.msg.tape.len() as u32;
    match self.region {
      Region::Header => {
        self.msg.push_gap();
        self.msg.body_start = self.msg.tape.len() as u32;
        self.msg.trailer_start = self.msg.body_start;
      }
      Region::Body => self.msg.trailer_start = len,
      Region::Trailer => {}
    }
  }
}

/// Parse `tag=` at `pos`; returns the tag and where its value starts.
fn parse_tag(
  buf: &[u8],
  pos: usize,
  delim: u8,
) -> Result<(u32, usize), ParseError> {
  let mut tag: u32 = 0;
  let mut i = pos;
  while i < buf.len() {
    match buf[i] {
      b'=' if i > pos => {
        if tag == 0 {
          return Err(ParseError::garbled("tag 0"));
        }
        return Ok((tag, i + 1));
      }
      // TagNum "may not contain leading zeros" (TagValue datatypes).
      b'0' if i == pos => {
        return Err(ParseError::garbled(format!(
          "tag at offset {pos} has a leading zero"
        )));
      }
      c @ b'0'..=b'9' => {
        tag = tag
          .checked_mul(10)
          .and_then(|t| t.checked_add((c - b'0') as u32))
          .ok_or_else(|| ParseError::garbled("tag number too large"))?;
      }
      c if c == delim => {
        return Err(ParseError::garbled(format!(
          "field at offset {pos} has no '='"
        )));
      }
      _ => {
        return Err(ParseError::garbled(format!(
          "invalid tag at offset {pos}"
        )));
      }
    }
    i += 1;
  }
  Err(ParseError::garbled("message ends inside a tag"))
}

pub(crate) fn checksum_of(bytes: &[u8]) -> u8 {
  // Sum in a wide accumulator, which the compiler vectorises; reduce once.
  let sum: u64 = bytes.iter().map(|&b| b as u64).sum();
  (sum % 256) as u8
}
