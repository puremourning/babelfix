use std::hint::black_box;
use std::sync::{Arc, OnceLock};

use babelfix::message::{Dictionaries, Dictionary, Message};
use babelfix::schema::fields::{ClOrdID, MsgSeqNum, Price, SendingTime};
use babelfix::schema::tags;
use bytes::{Bytes, BytesMut};
use criterion::{
  BatchSize, Criterion, Throughput, criterion_group, criterion_main,
};

fn fix44() -> Arc<Dictionary> {
  static DICT: OnceLock<Arc<Dictionary>> = OnceLock::new();
  DICT
    .get_or_init(|| {
      Dictionaries::standard()
        .unwrap()
        .get("FIX.4.4")
        .unwrap()
        .clone()
    })
    .clone()
}

// ---------------------------------------------------------------------------
// Test messages: the same content as the benchmarks of the previous
// representation measured, so the numbers compare. All SOH-delimited.
// ---------------------------------------------------------------------------

struct TestMessages {
  simple: Bytes,
  with_groups: Bytes,
  out_of_order: Message,
  exec_small: Bytes,
  exec_medium: Bytes,
  exec_large: Bytes,
  exec_super: Bytes,
}

static TEST_MSGS: OnceLock<TestMessages> = OnceLock::new();

/// Set `fields` (tag, value) in order in a block.
fn put(b: &mut babelfix::message::BlockMut<'_>, fields: &[(u32, &str)]) {
  for (tag, value) in fields {
    b.set_raw(*tag, value.as_bytes());
  }
}

/// Build an ExecutionReport (35=8) at approximately `target_bytes`, growing it
/// with Parties, ContraBrokers and Legs as real execution reports do.
fn build_exec_report(dict: &Arc<Dictionary>, target_bytes: usize) -> Bytes {
  let mut msg = Message::new(dict, "8");
  put(
    &mut msg.header_mut(),
    &[
      (tags::SenderCompID, "EXCHANGESIM"),
      (tags::TargetCompID, "CLIENTFIRM"),
      (tags::MsgSeqNum, "42358"),
      (tags::SendingTime, "20231215-14:30:22.123"),
    ],
  );
  put(
    &mut msg.body_mut(),
    &[
      (tags::OrderID, "ORD-20231215-000847"),
      (tags::ClOrdID, "CLIENT-20231215-003921"),
      (tags::ExecID, "EXEC-20231215-019283"),
      (tags::ExecType, "F"),
      (tags::OrdStatus, "2"),
      (tags::Symbol, "ESH4"),
      (tags::SecurityExchange, "CME"),
      (tags::Side, "1"),
      (tags::OrderQty, "500"),
      (tags::OrdType, "2"),
      (tags::Price, "4782.25"),
      (tags::LastPx, "4782.25"),
      (tags::LastQty, "500"),
      (tags::LeavesQty, "0"),
      (tags::CumQty, "500"),
      (tags::AvgPx, "4782.25"),
      (tags::TransactTime, "20231215-14:30:22.119"),
      (tags::TradeDate, "20231215"),
      (tags::Account, "ACCT-98374"),
      (tags::Currency, "USD"),
      (tags::HandlInst, "1"),
      (tags::TimeInForce, "0"),
    ],
  );

  let full = |m: &Message| m.to_bytes().len() >= target_bytes;

  let parties = [
    ("FIRM-001", "D", "1"),
    ("TRADER-JSmith", "D", "12"),
    ("CLEARING-99", "D", "4"),
    ("BROKER-CME-7", "D", "17"),
    ("CLIENT-EXT-42", "D", "3"),
    ("EXCHANGE-CME", "M", "22"),
    ("CUSTODIAN-BNY", "D", "28"),
    ("GIVE-UP-FIRM", "D", "6"),
    ("ALGO-STRAT-V2", "D", "24"),
    ("REGULATOR-SEC", "D", "61"),
    ("SETTLE-AGENT", "D", "10"),
    ("RISK-MGR-01", "D", "62"),
    ("PRIME-BRKR", "D", "63"),
    ("EXEC-VENUE-A", "M", "30"),
    ("SPONSOR-FIRM", "D", "64"),
    ("ALLOC-ACCT-1", "D", "65"),
    ("ALLOC-ACCT-2", "D", "65"),
    ("ALLOC-ACCT-3", "D", "65"),
    ("ORDER-ORIG", "D", "11"),
    ("COMPLIANCE-1", "D", "66"),
    ("CLEARING-ALT", "D", "4"),
    ("BROKER-ICE-3", "D", "17"),
    ("CLIENT-INT-77", "D", "3"),
    ("EXCHANGE-ICE", "M", "22"),
    ("CUSTODIAN-SSB", "D", "28"),
    ("GIVE-UP-ALT", "D", "6"),
    ("ALGO-TWAP-V1", "D", "24"),
    ("SETTLE-FED", "D", "10"),
    ("RISK-MGR-02", "D", "62"),
    ("PRIME-BRKR-2", "D", "63"),
    ("EXEC-VENUE-B", "M", "30"),
    ("SPONSOR-ALT", "D", "64"),
    ("ALLOC-ACCT-4", "D", "65"),
    ("ALLOC-ACCT-5", "D", "65"),
    ("ALLOC-ACCT-6", "D", "65"),
    ("ORDER-ORIG-2", "D", "11"),
    ("COMPLIANCE-2", "D", "66"),
    ("CLEARING-3RD", "D", "4"),
    ("BROKER-BATS", "D", "17"),
    ("CLIENT-HF-99", "D", "3"),
    ("TRADER-BWONG", "D", "12"),
    ("CLEARING-ALT2", "D", "4"),
    ("BROKER-BATS-2", "D", "17"),
    ("CUSTDN-STATE", "D", "28"),
    ("ALGO-IS-V3", "D", "24"),
    ("RISK-MGR-03", "D", "62"),
    ("PRIME-BRKR-3", "D", "63"),
    ("EXEC-VENUE-C", "M", "30"),
    ("ALLOC-ACCT-7", "D", "65"),
  ];
  for (id, source, role) in parties {
    if full(&msg) {
      break;
    }
    put(
      &mut msg.body_mut().group_mut(tags::NoPartyIDs).push(),
      &[
        (tags::PartyID, id),
        (tags::PartyIDSource, source),
        (tags::PartyRole, role),
      ],
    );
  }

  let contras = [
    ("CITI-FI", "JDOE-C", "250", "20231215-14:30:22.100"),
    ("GS-EQ", "ASMITH-G", "150", "20231215-14:30:22.105"),
    ("JPM-DRV", "BWONG-J", "100", "20231215-14:30:22.110"),
    ("MS-PRIME", "CLEE-M", "200", "20231215-14:30:22.112"),
    ("BARCLAYS-FX", "DPATEL-B", "175", "20231215-14:30:22.115"),
    ("UBS-FLOW", "EWANG-U", "125", "20231215-14:30:22.117"),
    ("DB-STRUC", "FCHEN-D", "300", "20231215-14:30:22.118"),
    ("HSBC-RATES", "GKUMAR-H", "225", "20231215-14:30:22.119"),
    ("NOMURA-EQ", "HTANAKA-N", "180", "20231215-14:30:22.120"),
    ("BNP-DERIV", "IDURAND-B", "140", "20231215-14:30:22.121"),
    ("SOCGEN-FI", "JMARTIN-S", "320", "20231215-14:30:22.122"),
    ("MACQ-COMM", "KBROWN-M", "275", "20231215-14:30:22.123"),
    ("CREDIT-SUI", "LMEYER-C", "190", "20231215-14:30:22.124"),
    ("BOFA-RATES", "MJONES-B", "210", "20231215-14:30:22.125"),
    ("WELLS-EQ", "NPARK-W", "165", "20231215-14:30:22.126"),
    ("JEFFERIES", "OGREEN-J", "340", "20231215-14:30:22.127"),
    ("CANTOR-FI", "PWHITE-C", "155", "20231215-14:30:22.128"),
    ("STIFEL-MU", "QADAMS-S", "280", "20231215-14:30:22.129"),
  ];
  for (broker, trader, qty, time) in contras {
    if full(&msg) {
      break;
    }
    put(
      &mut msg.body_mut().group_mut(tags::NoContraBrokers).push(),
      &[
        (tags::ContraBroker, broker),
        (tags::ContraTrader, trader),
        (tags::ContraTradeQty, qty),
        (tags::ContraTradeTime, time),
      ],
    );
  }

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
  for (sym, cfi, maturity, side, px) in legs {
    if full(&msg) {
      break;
    }
    put(
      &mut msg.body_mut().group_mut(tags::NoLegs).push(),
      &[
        (tags::LegSymbol, sym),
        (tags::LegCFICode, cfi),
        (tags::LegMaturityDate, maturity),
        (tags::LegSide, side),
        (tags::LegPrice, px),
      ],
    );
  }

  msg.to_bytes()
}

fn test_messages() -> &'static TestMessages {
  TEST_MSGS.get_or_init(|| {
    let dict = fix44();

    let mut msg = Message::new(&dict, "D");
    put(
      &mut msg.header_mut(),
      &[
        (tags::SenderCompID, "Sender"),
        (tags::TargetCompID, "Target"),
        (tags::MsgSeqNum, "1"),
        (tags::SendingTime, "20231010-12:00:00.000"),
      ],
    );
    put(
      &mut msg.body_mut(),
      &[
        (tags::ClOrdID, "123456"),
        (tags::Side, "1"),
        (tags::TransactTime, "20231010-12:00:00.000"),
        (tags::OrderQty, "1000"),
        (tags::OrdType, "2"),
        (tags::Symbol, "AAPL"),
      ],
    );
    let simple = msg.to_bytes();

    let mut msg = Message::new(&dict, "AB");
    put(
      &mut msg.header_mut(),
      &[
        (tags::SenderCompID, "SenderCompID"),
        (tags::TargetCompID, "TargetCompID"),
        (tags::MsgSeqNum, "1"),
        (tags::SendingTime, "20231010-12:00:00.000"),
      ],
    );
    put(
      &mut msg.body_mut(),
      &[
        (tags::ClOrdID, "123456"),
        (tags::Side, "1"),
        (tags::TransactTime, "20231010-12:00:00.000"),
        (tags::OrderQty, "1000"),
        (tags::OrdType, "2"),
      ],
    );
    for maturity in ["202509", "202505"] {
      put(
        &mut msg.body_mut().group_mut(tags::NoLegs).push(),
        &[
          (tags::LegSymbol, "6B"),
          (tags::LegCFICode, "F"),
          (tags::LegMaturityDate, maturity),
        ],
      );
    }
    let with_groups = msg.to_bytes();

    // Out of canonical order, for normalising.
    let out_of_order = Message::parse_fragment(
      &dict,
      b"35=D|55=Symbol|49=Sender|11=ClOrdID|56=Target|34=1",
      b'|',
    )
    .unwrap();

    let exec_small = build_exec_report(&dict, 500);
    let exec_medium = build_exec_report(&dict, 1024);
    let exec_large = build_exec_report(&dict, 2048);
    let exec_super = build_exec_report(&dict, 4096);

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
// Parsing: one pass to a structured, typed-on-demand message.
// ---------------------------------------------------------------------------

fn bench_parsing(c: &mut Criterion) {
  let msgs = test_messages();
  let dict = fix44();
  let mut group = c.benchmark_group("message_parsing");

  group.throughput(Throughput::Bytes(msgs.simple.len() as u64));
  group.bench_function("parse_simple", |b| {
    b.iter(|| Message::parse(&dict, black_box(msgs.simple.clone())).unwrap())
  });

  group.throughput(Throughput::Bytes(msgs.with_groups.len() as u64));
  group.bench_function("parse_with_groups", |b| {
    b.iter(|| {
      Message::parse(&dict, black_box(msgs.with_groups.clone())).unwrap()
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

fn bench_construction(c: &mut Criterion) {
  let dict = fix44();
  let mut group = c.benchmark_group("message_construction");

  group.bench_function("build_simple", |b| {
    b.iter(|| {
      let mut msg = Message::new(&dict, "D");
      put(
        &mut msg.header_mut(),
        &[
          (tags::SenderCompID, "Sender"),
          (tags::TargetCompID, "Target"),
          (tags::MsgSeqNum, "1"),
        ],
      );
      put(
        &mut msg.body_mut(),
        &[
          (tags::ClOrdID, "123456"),
          (tags::Side, "1"),
          (tags::OrderQty, "1000"),
          (tags::OrdType, "2"),
          (tags::Symbol, "AAPL"),
        ],
      );
      black_box(msg)
    })
  });

  group.bench_function("build_with_groups", |b| {
    b.iter(|| {
      let mut msg = Message::new(&dict, "AB");
      put(
        &mut msg.header_mut(),
        &[
          (tags::SenderCompID, "Sender"),
          (tags::TargetCompID, "Target"),
          (tags::MsgSeqNum, "1"),
        ],
      );
      for sym in ["FDAX", "ODAX"] {
        msg
          .body_mut()
          .group_mut(tags::NoLegs)
          .push()
          .set_raw(tags::LegSymbol, sym.as_bytes());
      }
      black_box(msg)
    })
  });

  // A large group: building it should cost in proportion to its size.
  for n in [100usize, 1000] {
    group.bench_function(format!("build_group_{n}"), |b| {
      b.iter(|| {
        let mut msg = Message::new(&dict, "D");
        let mut body = msg.body_mut();
        let mut parties = body.group_mut(tags::NoPartyIDs);
        for i in 0..n {
          parties
            .push()
            .set_raw(tags::PartyID, format!("P{i}").as_bytes())
            .set_raw(tags::PartyIDSource, b"D")
            .set_raw(tags::PartyRole, b"3");
        }
        drop(parties);
        black_box(msg)
      })
    });
  }

  // The steady state of a pooled message: no allocation.
  group.bench_function("build_simple_reused", |b| {
    let mut msg = Message::new(&dict, "D");
    b.iter(|| {
      msg.clear("D");
      put(
        &mut msg.header_mut(),
        &[
          (tags::SenderCompID, "Sender"),
          (tags::TargetCompID, "Target"),
          (tags::MsgSeqNum, "1"),
        ],
      );
      put(
        &mut msg.body_mut(),
        &[
          (tags::ClOrdID, "123456"),
          (tags::Side, "1"),
          (tags::OrderQty, "1000"),
          (tags::OrdType, "2"),
          (tags::Symbol, "AAPL"),
        ],
      );
      black_box(&msg);
    })
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

fn bench_serialization(c: &mut Criterion) {
  let msgs = test_messages();
  let dict = fix44();
  let mut group = c.benchmark_group("message_serialization");

  let parsed = Message::parse(&dict, msgs.with_groups.clone()).unwrap();
  let mut edited = parsed.clone();
  edited.header_mut().set(MsgSeqNum, 2u64);

  // Unedited: the received bytes, copied.
  group.bench_function("encode_clean", |b| {
    let mut buf = BytesMut::with_capacity(1024);
    b.iter(|| {
      buf.clear();
      black_box(&parsed).encode(&mut buf);
    })
  });

  // Edited: written from the tape.
  group.bench_function("encode", |b| {
    let mut buf = BytesMut::with_capacity(1024);
    b.iter(|| {
      buf.clear();
      black_box(&edited).encode(&mut buf);
    })
  });

  group
    .bench_function("to_string", |b| b.iter(|| black_box(&edited).to_string()));

  group.bench_function("roundtrip", |b| {
    let mut buf = BytesMut::with_capacity(1024);
    b.iter(|| {
      let mut msg =
        Message::parse(&dict, black_box(msgs.with_groups.clone())).unwrap();
      msg.header_mut().set(MsgSeqNum, 2u64);
      buf.clear();
      msg.encode(&mut buf);
    })
  });

  group.finish();
}

fn bench_normalization(c: &mut Criterion) {
  let msgs = test_messages();
  let mut group = c.benchmark_group("message_normalization");
  group.bench_function("normalize", |b| {
    b.iter_batched(
      || msgs.out_of_order.clone(),
      |mut m| {
        m.normalize();
        m
      },
      BatchSize::SmallInput,
    )
  });
  group.finish();
}

// ---------------------------------------------------------------------------
// Field access
// ---------------------------------------------------------------------------

fn bench_field_access(c: &mut Criterion) {
  let msgs = test_messages();
  let dict = fix44();
  let mut group = c.benchmark_group("field_access");

  let msg = Message::parse(&dict, msgs.exec_medium.clone()).unwrap();

  group.bench_function("tag_lookup_hit", |b| {
    b.iter(|| black_box(msg.body().raw(black_box(tags::ClOrdID))))
  });
  group.bench_function("tag_lookup_miss", |b| {
    b.iter(|| black_box(msg.body().raw(black_box(9999u32))))
  });
  group.bench_function("typed_get_str", |b| {
    b.iter(|| black_box(msg.body().get(black_box(ClOrdID)).unwrap()))
  });
  group.bench_function("typed_get_decimal", |b| {
    b.iter(|| black_box(msg.body().get(black_box(Price)).unwrap()))
  });
  group.bench_function("group_iterate", |b| {
    b.iter(|| {
      let mut n = 0;
      for party in msg.body().group(tags::NoPartyIDs) {
        n += party.raw(tags::PartyID).map_or(0, <[u8]>::len);
      }
      black_box(n)
    })
  });
  group.bench_function("set_header_field", |b| {
    b.iter_batched(
      || msg.clone(),
      |mut m| {
        m.header_mut().set(
          SendingTime,
          black_box(msg.header().req(SendingTime).unwrap()),
        );
        m
      },
      BatchSize::SmallInput,
    )
  });

  group.finish();
}

// ---------------------------------------------------------------------------
// Execution report scaling
// ---------------------------------------------------------------------------

fn bench_exec_report_scaling(c: &mut Criterion) {
  let msgs = test_messages();
  let dict = fix44();
  let sizes: [(&str, &Bytes); 4] = [
    ("small", &msgs.exec_small),
    ("medium", &msgs.exec_medium),
    ("large", &msgs.exec_large),
    ("super", &msgs.exec_super),
  ];

  {
    let mut group = c.benchmark_group("exec_report_parse");
    for (name, data) in &sizes {
      group.throughput(Throughput::Bytes(data.len() as u64));
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        b.iter(|| Message::parse(&dict, black_box((*data).clone())).unwrap())
      });
    }
    group.finish();
  }

  {
    let mut group = c.benchmark_group("exec_report_serialize");
    for (name, data) in &sizes {
      let mut msg = Message::parse(&dict, (*data).clone()).unwrap();
      // Edited, so the encode walks the tape rather than copying the input.
      msg.header_mut().set(MsgSeqNum, 1u64);
      group.throughput(Throughput::Bytes(data.len() as u64));
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        let mut buf = BytesMut::with_capacity(8192);
        b.iter(|| {
          buf.clear();
          black_box(&msg).encode(&mut buf);
        })
      });
    }
    group.finish();
  }

  {
    let mut group = c.benchmark_group("exec_report_roundtrip");
    for (name, data) in &sizes {
      group.throughput(Throughput::Bytes(data.len() as u64));
      group.bench_function(format!("{name}_{}B", data.len()), |b| {
        let mut buf = BytesMut::with_capacity(8192);
        b.iter(|| {
          let mut msg =
            Message::parse(&dict, black_box((*data).clone())).unwrap();
          msg.header_mut().set(MsgSeqNum, 1u64);
          buf.clear();
          msg.encode(&mut buf);
        })
      });
    }
    group.finish();
  }
}

criterion_group!(
  benches,
  bench_parsing,
  bench_construction,
  bench_serialization,
  bench_normalization,
  bench_field_access,
  bench_exec_report_scaling,
);
criterion_main!(benches);
