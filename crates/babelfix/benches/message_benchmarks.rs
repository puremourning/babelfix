use std::sync::{Arc, OnceLock};

use babelfix::message::builder::{self, TypedValue};
use babelfix::message::FixMessage;
use babelfix::repository;
use babelfix::schema::FIX_4_4 as FIX44;
use bytes::Bytes;
use criterion::{
  black_box, criterion_group, criterion_main, Criterion, Throughput,
};

static REPO: OnceLock<Arc<repository::FixRepository>> = OnceLock::new();

fn load_repo() -> Arc<repository::FixRepository> {
  REPO
    .get_or_init(|| Arc::new(repository::orchestrate().unwrap()))
    .clone()
}

fn fix44() -> Arc<repository::FixVersion> {
  load_repo().get_version("FIX.4.4").unwrap()
}

// ---------------------------------------------------------------------------
// Test message construction (built once via the builder API)
// ---------------------------------------------------------------------------

struct TestMessages {
  simple: Vec<u8>,
  with_groups: Vec<u8>,
  out_of_order: Vec<u8>,
  exec_small: Vec<u8>,
  exec_medium: Vec<u8>,
  exec_large: Vec<u8>,
  exec_super: Vec<u8>,
}

static TEST_MSGS: OnceLock<TestMessages> = OnceLock::new();

// Tag constants for groups not in the generated schema
const NO_PARTY_IDS: u32 = 453;
const PARTY_ID: u32 = 448;
const PARTY_ID_SOURCE: u32 = 447;
const PARTY_ROLE: u32 = 452;
const NO_CONTRA_BROKERS: u32 = 382;
const CONTRA_BROKER: u32 = 375;
const CONTRA_TRADER: u32 = 337;
const CONTRA_TRADE_QTY: u32 = 437;
const CONTRA_TRADE_TIME: u32 = 438;

/// Build an ExecutionReport (35=8) at approximately `target_bytes`.
/// Grows the message using realistic repeating groups: Parties (with sub-IDs),
/// ContraBrokers, and Legs — the kind of structure seen on real execution reports.
fn build_exec_report(
  fix: &Arc<repository::FixVersion>,
  target_bytes: usize,
) -> Vec<u8> {
  let mut msg = builder::Message::new(fix.clone(), "8").unwrap();

  // Header
  msg
    .header
    .push_tag(FIX44::Fields::SenderCompID, "EXCHANGESIM")
    .unwrap();
  msg
    .header
    .push_tag(FIX44::Fields::TargetCompID, "CLIENTFIRM")
    .unwrap();
  msg
    .header
    .push_tag(FIX44::Fields::MsgSeqNum, 42358)
    .unwrap();
  msg
    .header
    .push_tag(FIX44::Fields::SendingTime, "20231215-14:30:22.123")
    .unwrap();

  // Core exec report fields
  msg
    .body
    .push_tag(FIX44::Fields::OrderID, "ORD-20231215-000847")
    .unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::ClOrdID, "CLIENT-20231215-003921")
    .unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::ExecID, "EXEC-20231215-019283")
    .unwrap();
  msg.body.push_tag(FIX44::Fields::ExecType, "F").unwrap();
  msg.body.push_tag(FIX44::Fields::OrdStatus, "2").unwrap();
  msg.body.push_tag(FIX44::Fields::Symbol, "ESH4").unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::SecurityExchange, "CME")
    .unwrap();
  msg.body.push_tag(FIX44::Fields::Side, "1").unwrap();
  msg.body.push_tag(FIX44::Fields::OrderQty, 500).unwrap();
  msg.body.push_tag(FIX44::Fields::OrdType, "2").unwrap();
  msg.body.push_tag(FIX44::Fields::Price, 4782.25).unwrap();
  msg.body.push_tag(FIX44::Fields::LastPx, 4782.25).unwrap();
  msg.body.push_tag(FIX44::Fields::LastQty, 500).unwrap();
  msg.body.push_tag(FIX44::Fields::LeavesQty, 0).unwrap();
  msg.body.push_tag(FIX44::Fields::CumQty, 500).unwrap();
  msg.body.push_tag(FIX44::Fields::AvgPx, 4782.25).unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::TransactTime, "20231215-14:30:22.119")
    .unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::TradeDate, "20231215")
    .unwrap();
  msg
    .body
    .push_tag(FIX44::Fields::Account, "ACCT-98374")
    .unwrap();
  msg.body.push_tag(FIX44::Fields::Currency, "USD").unwrap();
  msg.body.push_tag(FIX44::Fields::HandlInst, "1").unwrap();
  msg.body.push_tag(FIX44::Fields::TimeInForce, "0").unwrap();

  fn serialized_len(msg: &builder::Message) -> usize {
    msg
      .clone()
      .into_message()
      .unwrap()
      .to_string_delimited(b'|')
      .len()
  }

  if serialized_len(&msg) >= target_bytes {
    return msg
      .into_message()
      .unwrap()
      .to_string_delimited(b'|')
      .into_bytes();
  }

  // Phase 1: Add Party groups (~60-80 bytes each with sub-IDs)
  let party_roles = [
    ("FIRM-001", "D", 1),
    ("TRADER-JSmith", "D", 12),
    ("CLEARING-99", "D", 4),
    ("BROKER-CME-7", "D", 17),
    ("CLIENT-EXT-42", "D", 3),
    ("EXCHANGE-CME", "M", 22),
    ("CUSTODIAN-BNY", "D", 28),
    ("GIVE-UP-FIRM", "D", 6),
    ("ALGO-STRAT-V2", "D", 24),
    ("REGULATOR-SEC", "D", 61),
    ("SETTLE-AGENT", "D", 10),
    ("RISK-MGR-01", "D", 62),
    ("PRIME-BRKR", "D", 63),
    ("EXEC-VENUE-A", "M", 30),
    ("SPONSOR-FIRM", "D", 64),
    ("ALLOC-ACCT-1", "D", 65),
    ("ALLOC-ACCT-2", "D", 65),
    ("ALLOC-ACCT-3", "D", 65),
    ("ORDER-ORIG", "D", 11),
    ("COMPLIANCE-1", "D", 66),
    ("CLEARING-ALT", "D", 4),
    ("BROKER-ICE-3", "D", 17),
    ("CLIENT-INT-77", "D", 3),
    ("EXCHANGE-ICE", "M", 22),
    ("CUSTODIAN-SSB", "D", 28),
    ("GIVE-UP-ALT", "D", 6),
    ("ALGO-TWAP-V1", "D", 24),
    ("SETTLE-FED", "D", 10),
    ("RISK-MGR-02", "D", 62),
    ("PRIME-BRKR-2", "D", 63),
    ("EXEC-VENUE-B", "M", 30),
    ("SPONSOR-ALT", "D", 64),
    ("ALLOC-ACCT-4", "D", 65),
    ("ALLOC-ACCT-5", "D", 65),
    ("ALLOC-ACCT-6", "D", 65),
    ("ORDER-ORIG-2", "D", 11),
    ("COMPLIANCE-2", "D", 66),
    ("CLEARING-3RD", "D", 4),
    ("BROKER-BATS", "D", 17),
    ("CLIENT-HF-99", "D", 3),
    ("TRADER-BWONG", "D", 12),
    ("CLEARING-ALT2", "D", 4),
    ("BROKER-BATS-2", "D", 17),
    ("CUSTDN-STATE", "D", 28),
    ("ALGO-IS-V3", "D", 24),
    ("RISK-MGR-03", "D", 62),
    ("PRIME-BRKR-3", "D", 63),
    ("EXEC-VENUE-C", "M", 30),
    ("ALLOC-ACCT-7", "D", 65),
  ];

  for (party_id, source, role) in &party_roles {
    if serialized_len(&msg) >= target_bytes {
      break;
    }
    let mut party = builder::Block::new();
    party.push_tag(PARTY_ID, *party_id).unwrap();
    party.push_tag(PARTY_ID_SOURCE, *source).unwrap();
    party.push_tag(PARTY_ROLE, *role as i64).unwrap();
    msg.body.push_group(NO_PARTY_IDS, party);
  }

  // Phase 2: Add ContraBroker groups (~80 bytes each)
  let contras = [
    ("CITI-FI", "JDOE-C", 250, "20231215-14:30:22.100"),
    ("GS-EQ", "ASMITH-G", 150, "20231215-14:30:22.105"),
    ("JPM-DRV", "BWONG-J", 100, "20231215-14:30:22.110"),
    ("MS-PRIME", "CLEE-M", 200, "20231215-14:30:22.112"),
    ("BARCLAYS-FX", "DPATEL-B", 175, "20231215-14:30:22.115"),
    ("UBS-FLOW", "EWANG-U", 125, "20231215-14:30:22.117"),
    ("DB-STRUC", "FCHEN-D", 300, "20231215-14:30:22.118"),
    ("HSBC-RATES", "GKUMAR-H", 225, "20231215-14:30:22.119"),
    ("NOMURA-EQ", "HTANAKA-N", 180, "20231215-14:30:22.120"),
    ("BNP-DERIV", "IDURAND-B", 140, "20231215-14:30:22.121"),
    ("SOCGEN-FI", "JMARTIN-S", 320, "20231215-14:30:22.122"),
    ("MACQ-COMM", "KBROWN-M", 275, "20231215-14:30:22.123"),
    ("CREDIT-SUI", "LMEYER-C", 190, "20231215-14:30:22.124"),
    ("BOFA-RATES", "MJONES-B", 210, "20231215-14:30:22.125"),
    ("WELLS-EQ", "NPARK-W", 165, "20231215-14:30:22.126"),
    ("JEFFERIES", "OGREEN-J", 340, "20231215-14:30:22.127"),
    ("CANTOR-FI", "PWHITE-C", 155, "20231215-14:30:22.128"),
    ("STIFEL-MU", "QADAMS-S", 280, "20231215-14:30:22.129"),
  ];

  for (broker, trader, qty, time) in &contras {
    if serialized_len(&msg) >= target_bytes {
      break;
    }
    let mut contra = builder::Block::new();
    contra.push_tag(CONTRA_BROKER, *broker).unwrap();
    contra.push_tag(CONTRA_TRADER, *trader).unwrap();
    contra.push_tag(CONTRA_TRADE_QTY, *qty as i64).unwrap();
    contra.push_tag(CONTRA_TRADE_TIME, *time).unwrap();
    msg.body.push_group(NO_CONTRA_BROKERS, contra);
  }

  // Phase 3: Add Leg groups (~90 bytes each)
  let legs = [
    ("ESH4", "FXXXXX", "20240315", "1", "4782.25"),
    ("ESM4", "FXXXXX", "20240621", "2", "4795.50"),
    ("ESU4", "FXXXXX", "20240920", "1", "4810.75"),
    ("ESZ4", "FXXXXX", "20241220", "2", "4825.00"),
    ("ESH5", "FXXXXX", "20250321", "1", "4840.25"),
    ("ESM5", "FXXXXX", "20250620", "2", "4855.50"),
    ("ESU5", "FXXXXX", "20250919", "1", "4870.75"),
    ("ESZ5", "FXXXXX", "20251219", "2", "4886.00"),
    ("NQH4", "FXXXXX", "20240315", "1", "16850.00"),
    ("NQM4", "FXXXXX", "20240621", "2", "16920.50"),
    ("YMH4", "FXXXXX", "20240315", "1", "37250.00"),
    ("YMM4", "FXXXXX", "20240621", "2", "37480.75"),
    ("RTH4", "FXXXXX", "20240315", "1", "2025.50"),
    ("RTM4", "FXXXXX", "20240621", "2", "2038.25"),
    ("CLF4", "FXXXXX", "20240119", "1", "72.35"),
    ("CLG4", "FXXXXX", "20240220", "2", "73.10"),
    ("GCG4", "FXXXXX", "20240227", "1", "2048.50"),
    ("GCJ4", "FXXXXX", "20240426", "2", "2065.75"),
    ("SIH4", "FXXXXX", "20240326", "1", "23.85"),
    ("SIK4", "FXXXXX", "20240528", "2", "24.10"),
    ("ZNH4", "FXXXXX", "20240319", "1", "110.25"),
    ("ZNM4", "FXXXXX", "20240618", "2", "110.50"),
  ];

  for (sym, cfi, maturity, side, px) in &legs {
    if serialized_len(&msg) >= target_bytes {
      break;
    }
    let mut leg = builder::Block::new();
    leg.push_tag(FIX44::Fields::LegSymbol, *sym).unwrap();
    leg.push_tag(FIX44::Fields::LegCFICode, *cfi).unwrap();
    leg
      .push_tag(FIX44::Fields::LegMaturityDate, *maturity)
      .unwrap();
    leg.push_tag(FIX44::Fields::LegSide, *side).unwrap();
    leg.push_tag(FIX44::Fields::LegPrice, *px).unwrap();
    msg.body.push_group(FIX44::Fields::NoLegs, leg);
  }

  msg
    .into_message()
    .unwrap()
    .to_string_delimited(b'|')
    .into_bytes()
}

fn test_messages() -> &'static TestMessages {
  TEST_MSGS.get_or_init(|| {
    let fix = fix44();

    // Simple NewOrderSingle
    let mut msg = builder::Message::new(fix.clone(), "D").unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::SenderCompID, "Sender")
      .unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::TargetCompID, "Target")
      .unwrap();
    msg.header.push_tag(FIX44::Fields::MsgSeqNum, 1).unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::SendingTime, "20231010-12:00:00.000")
      .unwrap();
    msg.body.push_tag(FIX44::Fields::ClOrdID, "123456").unwrap();
    msg.body.push_tag(FIX44::Fields::Side, "1").unwrap();
    msg
      .body
      .push_tag(FIX44::Fields::TransactTime, "20231010-12:00:00.000")
      .unwrap();
    msg.body.push_tag(FIX44::Fields::OrderQty, 1000).unwrap();
    msg.body.push_tag(FIX44::Fields::OrdType, "2").unwrap();
    msg.body.push_tag(FIX44::Fields::Symbol, "AAPL").unwrap();
    let simple = msg
      .into_message()
      .unwrap()
      .to_string_delimited(b'|')
      .into_bytes();

    // Message with repeating groups (NewOrderMultileg)
    let mut msg = builder::Message::new(fix.clone(), "AB").unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::SenderCompID, "SenderCompID")
      .unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::TargetCompID, "TargetCompID")
      .unwrap();
    msg.header.push_tag(FIX44::Fields::MsgSeqNum, 1).unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::SendingTime, "20231010-12:00:00.000")
      .unwrap();
    msg.body.push_tag(FIX44::Fields::ClOrdID, "123456").unwrap();
    msg.body.push_tag(FIX44::Fields::Side, "1").unwrap();
    msg
      .body
      .push_tag(FIX44::Fields::TransactTime, "20231010-12:00:00.000")
      .unwrap();
    msg.body.push_tag(FIX44::Fields::OrderQty, 1000).unwrap();
    msg.body.push_tag(FIX44::Fields::OrdType, "2").unwrap();

    let mut leg1 = builder::Block::new();
    leg1.push_tag(FIX44::Fields::LegSymbol, "6B").unwrap();
    leg1.push_tag(FIX44::Fields::LegCFICode, "F").unwrap();
    leg1
      .push_tag(FIX44::Fields::LegMaturityDate, "202509")
      .unwrap();
    msg.body.push_group(FIX44::Fields::NoLegs, leg1);

    let mut leg2 = builder::Block::new();
    leg2.push_tag(FIX44::Fields::LegSymbol, "6B").unwrap();
    leg2.push_tag(FIX44::Fields::LegCFICode, "F").unwrap();
    leg2
      .push_tag(FIX44::Fields::LegMaturityDate, "202505")
      .unwrap();
    msg.body.push_group(FIX44::Fields::NoLegs, leg2);

    let with_groups = msg
      .into_message()
      .unwrap()
      .to_string_delimited(b'|')
      .into_bytes();

    // Out-of-order message (body fields before header fields)
    // Build it correctly first, then manually reorder the serialized form
    let mut msg = builder::Message::new(fix.clone(), "D").unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::SenderCompID, "Sender")
      .unwrap();
    msg
      .header
      .push_tag(FIX44::Fields::TargetCompID, "Target")
      .unwrap();
    msg.header.push_tag(FIX44::Fields::MsgSeqNum, 1).unwrap();
    msg
      .body
      .push_tag(FIX44::Fields::ClOrdID, "ClOrdID")
      .unwrap();
    msg.body.push_tag(FIX44::Fields::Symbol, "Symbol").unwrap();
    // Serialize to get valid checksum/bodylength, then swap field order
    let fix_msg = msg.into_message().unwrap();
    let serialized = fix_msg.to_string_delimited(b'|');
    // Reorder: put Symbol (55) before SenderCompID (49)
    let out_of_order = serialized
      .replace(
        "49=Sender|56=Target|34=1|11=ClOrdID|55=Symbol",
        "55=Symbol|49=Sender|11=ClOrdID|56=Target|34=1",
      )
      .into_bytes();

    // Execution reports at increasing sizes
    let exec_small = build_exec_report(&fix, 500);
    let exec_medium = build_exec_report(&fix, 1024);
    let exec_large = build_exec_report(&fix, 2048);
    let exec_super = build_exec_report(&fix, 4096);

    eprintln!(
      "Exec report sizes: small={}B medium={}B large={}B super={}B",
      exec_small.len(),
      exec_medium.len(),
      exec_large.len(),
      exec_super.len(),
    );

    TestMessages {
      simple,
      with_groups,
      out_of_order,
      exec_small,
      exec_medium,
      exec_large,
      exec_super,
    }
  })
}

// ---------------------------------------------------------------------------
// 1. Low-level FixMessage parsing
// ---------------------------------------------------------------------------

fn bench_fix_message_parsing(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();
  let mut group = c.benchmark_group("fix_message_parsing");

  group.throughput(Throughput::Bytes(msgs.simple.len() as u64));
  group.bench_function("parse_simple", |b| {
    let data = Bytes::from(msgs.simple.clone());
    b.iter(|| {
      FixMessage::from_bytes_delimited(
        fix.clone(),
        black_box(data.clone()),
        b'|',
      )
      .unwrap()
    })
  });

  group.throughput(Throughput::Bytes(msgs.with_groups.len() as u64));
  group.bench_function("parse_with_groups", |b| {
    let data = Bytes::from(msgs.with_groups.clone());
    b.iter(|| {
      FixMessage::from_bytes_delimited(
        fix.clone(),
        black_box(data.clone()),
        b'|',
      )
      .unwrap()
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 2. Builder parsing (bytes -> hierarchical Message)
// ---------------------------------------------------------------------------

fn bench_builder_parsing(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();
  let mut group = c.benchmark_group("builder_parsing");

  // Pre-parse a FixMessage for the from_message benchmark
  let (fix_msg, _) = FixMessage::from_bytes_delimited(
    fix.clone(),
    Bytes::from(msgs.with_groups.clone()),
    b'|',
  )
  .unwrap();

  group.bench_function("from_fix_message", |b| {
    b.iter(|| builder::Message::from_message(black_box(&fix_msg)).unwrap())
  });

  group.throughput(Throughput::Bytes(msgs.with_groups.len() as u64));
  group.bench_function("from_bytes", |b| {
    b.iter(|| {
      builder::Message::from_bytes_delimited(
        fix.clone(),
        black_box(&msgs.with_groups),
        b'|',
      )
      .unwrap()
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 3. Message construction via builder API
// ---------------------------------------------------------------------------

fn bench_message_construction(c: &mut Criterion) {
  let fix = fix44();
  let mut group = c.benchmark_group("message_construction");

  group.bench_function("build_simple", |b| {
    b.iter(|| {
      let mut msg = builder::Message::new(fix.clone(), "D").unwrap();
      msg
        .header
        .push_tag(FIX44::Fields::SenderCompID, "Sender")
        .unwrap();
      msg
        .header
        .push_tag(FIX44::Fields::TargetCompID, "Target")
        .unwrap();
      msg.header.push_tag(FIX44::Fields::MsgSeqNum, 1).unwrap();
      msg.body.push_tag(FIX44::Fields::ClOrdID, "123456").unwrap();
      msg.body.push_tag(FIX44::Fields::Side, "1").unwrap();
      msg.body.push_tag(FIX44::Fields::OrderQty, 1000).unwrap();
      msg.body.push_tag(FIX44::Fields::OrdType, "2").unwrap();
      msg.body.push_tag(FIX44::Fields::Symbol, "AAPL").unwrap();
      black_box(&msg);
    })
  });

  group.bench_function("build_with_groups", |b| {
    b.iter(|| {
      let mut msg = builder::Message::new(fix.clone(), "AB").unwrap();
      msg
        .header
        .push_tag(FIX44::Fields::SenderCompID, "Sender")
        .unwrap();
      msg
        .header
        .push_tag(FIX44::Fields::TargetCompID, "Target")
        .unwrap();
      msg.header.push_tag(FIX44::Fields::MsgSeqNum, 1).unwrap();

      let mut leg1 = builder::Block::new();
      leg1.push_tag(FIX44::Fields::LegSymbol, "FDAX").unwrap();
      msg.body.push_group(FIX44::Fields::NoLegs, leg1);

      let mut leg2 = builder::Block::new();
      leg2.push_tag(FIX44::Fields::LegSymbol, "ODAX").unwrap();
      msg.body.push_group(FIX44::Fields::NoLegs, leg2);

      black_box(&msg);
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 4. Serialization (builder -> wire format)
// ---------------------------------------------------------------------------

fn bench_message_serialization(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();
  let mut group = c.benchmark_group("message_serialization");

  // Pre-build a message for serialization benchmarks
  let (fix_msg, _) = FixMessage::from_bytes_delimited(
    fix.clone(),
    Bytes::from(msgs.with_groups.clone()),
    b'|',
  )
  .unwrap();
  let builder_msg = builder::Message::from_message(&fix_msg).unwrap();

  group.bench_function("into_message", |b| {
    b.iter(|| {
      let msg = black_box(builder_msg.clone());
      msg.into_message().unwrap()
    })
  });

  group.bench_function("to_string_delimited", |b| {
    b.iter(|| black_box(&fix_msg).to_string_delimited(b'|'))
  });

  group.throughput(Throughput::Bytes(msgs.with_groups.len() as u64));
  group.bench_function("roundtrip", |b| {
    let data = Bytes::from(msgs.with_groups.clone());
    b.iter(|| {
      let (fix_msg, _) = FixMessage::from_bytes_delimited(
        fix.clone(),
        black_box(data.clone()),
        b'|',
      )
      .unwrap();
      let builder_msg = builder::Message::from_message(&fix_msg).unwrap();
      let fix_msg_out = builder_msg.into_message().unwrap();
      black_box(fix_msg_out.to_string_delimited(b'|'))
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 5. Message normalization
// ---------------------------------------------------------------------------

fn bench_message_normalization(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();
  let mut group = c.benchmark_group("message_normalization");

  let builder_msg =
    builder::Message::from_bytes_delimited(fix, &msgs.out_of_order, b'|')
      .unwrap();

  group.bench_function("normalize", |b| {
    b.iter(|| black_box(&builder_msg).normalize().unwrap())
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 6. Field access
// ---------------------------------------------------------------------------

fn bench_field_access(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();
  let mut group = c.benchmark_group("field_access");

  let (fix_msg, _) = FixMessage::from_bytes_delimited(
    fix.clone(),
    Bytes::from(msgs.with_groups.clone()),
    b'|',
  )
  .unwrap();
  let builder_msg = builder::Message::from_message(&fix_msg).unwrap();

  group.bench_function("tag_lookup_hit", |b| {
    b.iter(|| {
      black_box(builder_msg.body.tag(black_box(FIX44::Fields::ClOrdID)))
    })
  });

  group.bench_function("tag_lookup_miss", |b| {
    b.iter(|| {
      black_box(builder_msg.body.tag(black_box(FIX44::Fields::Account)))
    })
  });

  group.bench_function("set_tag", |b| {
    let mut msg = builder_msg.clone();
    b.iter(|| {
      msg.body.set_tag(
        black_box(FIX44::Fields::ClOrdID),
        TypedValue::String("bench".into()),
      );
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 7. Version detection
// ---------------------------------------------------------------------------

fn bench_version_detection(c: &mut Criterion) {
  let repo = load_repo();
  let msgs = test_messages();
  let mut group = c.benchmark_group("version_detection");

  group.throughput(Throughput::Bytes(msgs.simple.len() as u64));
  group.bench_function("peek_infer_version_and_length", |b| {
    b.iter(|| {
      babelfix::message::peek_infer_version_and_length(
        &repo,
        black_box(&msgs.simple),
        b'|',
      )
      .unwrap()
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// 8. Message size scaling (execution reports at ~500B, ~1K, ~2K, ~4K)
// ---------------------------------------------------------------------------

fn bench_exec_report_scaling(c: &mut Criterion) {
  let fix = fix44();
  let msgs = test_messages();

  let tiers: &[(&str, &[u8])] = &[
    ("small", &msgs.exec_small),
    ("medium", &msgs.exec_medium),
    ("large", &msgs.exec_large),
    ("super", &msgs.exec_super),
  ];

  // Low-level parsing scaling
  {
    let mut group = c.benchmark_group("exec_report_parse");
    for (name, data) in tiers {
      group.throughput(Throughput::Bytes(data.len() as u64));
      let bytes = Bytes::from(data.to_vec());
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        b.iter(|| {
          FixMessage::from_bytes_delimited(
            fix.clone(),
            black_box(bytes.clone()),
            b'|',
          )
          .unwrap()
        })
      });
    }
    group.finish();
  }

  // Builder (hierarchical) parsing scaling
  {
    let mut group = c.benchmark_group("exec_report_builder_parse");
    for (name, data) in tiers {
      group.throughput(Throughput::Bytes(data.len() as u64));
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        b.iter(|| {
          builder::Message::from_bytes_delimited(
            fix.clone(),
            black_box(*data),
            b'|',
          )
          .unwrap()
        })
      });
    }
    group.finish();
  }

  // Serialization scaling (builder -> wire)
  {
    let mut group = c.benchmark_group("exec_report_serialize");
    for (name, data) in tiers {
      let builder_msg =
        builder::Message::from_bytes_delimited(fix.clone(), data, b'|')
          .unwrap();
      group.throughput(Throughput::Bytes(data.len() as u64));
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        b.iter(|| black_box(builder_msg.clone()).into_message().unwrap())
      });
    }
    group.finish();
  }

  // Full roundtrip scaling
  {
    let mut group = c.benchmark_group("exec_report_roundtrip");
    for (name, data) in tiers {
      group.throughput(Throughput::Bytes(data.len() as u64));
      let bytes = Bytes::from(data.to_vec());
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        b.iter(|| {
          let (fix_msg, _) = FixMessage::from_bytes_delimited(
            fix.clone(),
            black_box(bytes.clone()),
            b'|',
          )
          .unwrap();
          let builder_msg = builder::Message::from_message(&fix_msg).unwrap();
          let fix_msg_out = builder_msg.into_message().unwrap();
          black_box(fix_msg_out.to_string_delimited(b'|'))
        })
      });
    }
    group.finish();
  }
}

// ---------------------------------------------------------------------------

criterion_group!(
  benches,
  bench_fix_message_parsing,
  bench_builder_parsing,
  bench_message_construction,
  bench_message_serialization,
  bench_message_normalization,
  bench_field_access,
  bench_version_detection,
  bench_exec_report_scaling,
);

criterion_main!(benches);
