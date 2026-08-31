//! `CodSpeed` microbenchmarks for the broker's produce hot path.
//!
//! The verbatim pass-through is the design's central produce claim: a v2 batch
//! that satisfies the predicate reaches the log as the producer's own bytes,
//! with no decode, re-encode or re-CRC. The owned fallback behind it is a
//! complete second implementation, and any change that quietly widens the
//! predicate's misses moves every batch onto it. This suite puts a number on
//! that difference so the move is visible.
//!
//! Each shape runs through [`krabka_broker::produce_hot_path`], which is
//! `prepare_batch`, the writer's `build_produce_data`, and the log append
//! behind them — the same three functions the produce pipeline calls, without
//! the writer actor's hand-off.
//!
//! Three shapes, three paths:
//!   * `verbatim` — a native v2 batch on a producer-pass-through topic;
//!   * `owned` — the same bytes forced down the owned fallback;
//!   * `legacy_v1` — a v0/v1 `MessageSet`, which the fallback up-converts.
//!
//! `bench_ratio` prints the owned-over-verbatim and legacy-over-verbatim
//! ratios per shape, so a run answers "what does the fallback cost" without
//! reading the criterion report.

use std::{
    cell::RefCell,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use krabka_broker::{
    metrics::BrokerMetrics,
    produce_hot_path::{HotPathSettings, PathChoice, ProducePath, append_one_batch},
};
use krabka_compression::RecordDecompressionPolicy;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_records_legacy::Magic;
use tempfile::TempDir;

/// The record shapes: one large record, a mid-sized batch, and a wide batch of
/// small records. They bracket what a producer's `batch.size` and
/// `linger.ms` actually produce.
const SHAPES: [(&str, i32, usize); 3] = [
    ("1rec_100KiB", 1, 100 * 1024),
    ("100rec_1KiB", 100, 1024),
    ("1000rec_100B", 1000, 100),
];

/// Bytes a single benchmark's log may take before it starts over.
///
/// Criterion runs a fast append hundreds of thousands of times, and every one
/// of them lands on disk. The reset happens outside the timed region, so it
/// costs the measurement nothing.
const LOG_BUDGET: usize = 256 * 1024 * 1024;

/// The leader epoch the verbatim writer stamps into the batch header.
const LEADER_EPOCH: i32 = 3;

/// Untimed appends run before the measured ones, against the same log.
///
/// The first append into a fresh log pays for the temp directory, the segment
/// file and its first page faults. Criterion amortizes that over a large
/// `iters`, but [`bench_ratio`]'s short run does not, and it lands entirely on
/// whichever path a shape measures first — the verbatim one, which is the
/// denominator of both ratios. A handful of untimed appends move it out of the
/// measurement.
const WARMUP: u64 = 8;

fn settings(metrics: &BrokerMetrics) -> HotPathSettings<'_> {
    HotPathSettings {
        topic_name: "bench-topic",
        // Producer pass-through: the topic asks for no recompression, which is
        // the configuration the verbatim path needs.
        topic_compression: None,
        decompression_policy: RecordDecompressionPolicy::default(),
        metrics,
        leader_epoch: LEADER_EPOCH,
    }
}

fn make_batch(records: i32, payload: usize) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: records - 1,
        ..RecordBatch::default()
    };
    for i in 0..records {
        batch.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i:08}"))),
            // A repeating non-uniform pattern, so a compressed variant of this
            // suite would not measure an unrealistically compressible payload.
            value: Some(Bytes::from(
                (0..payload)
                    .map(|b| u8::try_from(b % 251).expect("b % 251 fits in a byte"))
                    .collect::<Vec<u8>>(),
            )),
            ..Record::default()
        });
    }
    batch
}

/// The wire bytes of one partition's records field, as a producer sends it.
fn v2_records(records: i32, payload: usize) -> Bytes {
    let batch = make_batch(records, payload);
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).expect("encode v2 batch");
    buf.freeze()
}

/// The same records as a v1 `MessageSet`, which an older-message-format client
/// still sends over a v≥3 produce.
fn legacy_v1_records(records: i32, payload: usize) -> Bytes {
    krabka_records_legacy::v2_to_legacy(&make_batch(records, payload), Magic::V1)
        .expect("down-convert to a v1 message set")
}

/// A log that starts over once it has taken [`LOG_BUDGET`] bytes.
///
/// Every reset opens a fresh temp dir, so a long criterion run never grows the
/// log past the budget and never measures a log whose segment count keeps
/// climbing.
struct BoundedLog {
    _dir: TempDir,
    log: Log,
    written: usize,
}

impl BoundedLog {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        Self {
            _dir: dir,
            log,
            written: 0,
        }
    }

    /// Replace the log when it is full. Call this outside the timed region.
    fn rotate_if_full(&mut self, incoming: usize) {
        if self.written + incoming > LOG_BUDGET {
            *self = Self::new();
        }
        self.written += incoming;
    }
}

/// The measured region: one records field through prepare, writer-data build
/// and append.
///
/// The path assertion is what keeps a ratio meaningful. Were a change to the
/// verbatim predicate to send the `verbatim` case down the fallback, the two
/// columns would converge and the table would report the fallback as free.
/// This fails the run instead. It compares two enum discriminants against an
/// append that costs tens of microseconds, so it stays inside the measured
/// region rather than buying back a nanosecond by checking only sometimes.
fn append(
    store: &mut BoundedLog,
    payload: Bytes,
    choice: PathChoice,
    expected: ProducePath,
    settings: &HotPathSettings<'_>,
) {
    let path = append_one_batch(payload, choice, settings, &mut store.log)
        .expect("the bench shapes are all valid produce payloads");
    assert!(path == expected, "{choice:?} took the {path:?} path");
}

/// Append `records` once, returning only the time the append took.
///
/// The log rotation and the `Bytes` clone sit outside the timer. The clone is
/// a refcount bump, and the produce pipeline hands the hot path an equivalent
/// zero-copy view of the request frame.
///
/// This is [`bench_ratio`]'s own timer. The criterion group leaves the timing
/// to criterion, which is what lets `CodSpeed` instrument it.
fn append_once(
    store: &mut BoundedLog,
    records: &Bytes,
    choice: PathChoice,
    expected: ProducePath,
    settings: &HotPathSettings<'_>,
) -> Duration {
    store.rotate_if_full(records.len());
    let payload = records.clone();
    let start = Instant::now();
    append(store, payload, choice, expected, settings);
    start.elapsed()
}

/// Append `records` `iters` times, returning only the time the appends took.
fn timed_appends(
    records: &Bytes,
    choice: PathChoice,
    expected: ProducePath,
    iters: u64,
) -> Duration {
    let metrics = BrokerMetrics::new();
    let settings = settings(&metrics);
    let mut store = BoundedLog::new();
    for _ in 0..WARMUP {
        append_once(&mut store, records, choice, expected, &settings);
    }
    let mut elapsed = Duration::ZERO;
    for _ in 0..iters {
        elapsed += append_once(&mut store, records, choice, expected, &settings);
    }
    elapsed
}

/// The three paths a shape is measured on, in report order, each paired with
/// the path through `prepare_batch` that its case has to actually take.
fn paths(records: i32, payload: usize) -> [(&'static str, Bytes, PathChoice, ProducePath); 3] {
    let v2 = v2_records(records, payload);
    [
        (
            "verbatim",
            v2.clone(),
            PathChoice::Dispatch,
            ProducePath::Verbatim,
        ),
        ("owned", v2, PathChoice::ForceOwned, ProducePath::Owned),
        (
            "legacy_v1",
            legacy_v1_records(records, payload),
            PathChoice::Dispatch,
            ProducePath::Owned,
        ),
    ]
}

/// Time one append per iteration, with everything around it left untimed.
///
/// `iter_batched` rather than `iter_custom`, on purpose. `iter_custom` hands
/// the suite its own timer, which is the one API the `CodSpeed` compat shim
/// cannot instrument: its `Bencher::iter_custom` prints a skip line saying
/// custom iterations are unsupported and never calls the closure, so under
/// instrumentation every case here would be skipped and the regression this
/// suite exists to catch would go unmeasured. `iter_batched` is instrumented,
/// and it keeps the same split — the log rotation and the `Bytes` clone run in
/// the setup closure, outside the measurement, and only the append is timed.
fn bench_produce_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/produce");

    for (shape, records, payload) in SHAPES {
        for (path, wire, choice, expected) in paths(records, payload) {
            group.throughput(Throughput::Bytes(wire.len() as u64));
            group.bench_function(format!("{shape}/{path}"), |b| {
                let metrics = BrokerMetrics::new();
                let settings = settings(&metrics);
                // Both closures reach the log: the setup rotates it, the
                // routine appends into it.
                let store = RefCell::new(BoundedLog::new());
                b.iter_batched(
                    || {
                        store.borrow_mut().rotate_if_full(wire.len());
                        wire.clone()
                    },
                    |payload| {
                        append(
                            &mut store.borrow_mut(),
                            payload,
                            choice,
                            expected,
                            &settings,
                        );
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }

    group.finish();
}

/// Print the owned-over-verbatim and legacy-over-verbatim ratios per shape.
///
/// Criterion compares a benchmark against its own previous run, not against a
/// sibling, so the one number this suite exists for — what the fallback costs
/// against the pass-through — has to be computed here.
fn bench_ratio(_c: &mut Criterion) {
    /// Appends per measurement run.
    const SAMPLES: u32 = 100;
    /// Measurement runs per case, of which the fastest is the one reported.
    ///
    /// A single run is noise-dominated on a machine that is doing anything
    /// else — enough, observed, to invert a ratio and report the fallback as
    /// cheaper than the pass-through. Interference only ever adds time, so the
    /// minimum over a few runs is the estimator it cannot pull the wrong way.
    const RUNS: u32 = 5;

    println!(
        "\nbroker/produce — nanoseconds per batch (fastest of {RUNS} runs), and the cost of the fallback"
    );
    println!(
        "{:<14} {:>12} {:>12} {:>12} {:>16} {:>16}",
        "shape", "verbatim", "owned", "legacy_v1", "owned/verbatim", "legacy/verbatim"
    );
    for (shape, records, payload) in SHAPES {
        let means: Vec<f64> = paths(records, payload)
            .into_iter()
            .map(|(_, wire, choice, expected)| {
                (0..RUNS)
                    .map(|_| {
                        timed_appends(&wire, choice, expected, u64::from(SAMPLES)).as_secs_f64()
                            * 1e9
                            / f64::from(SAMPLES)
                    })
                    .fold(f64::INFINITY, f64::min)
            })
            .collect();
        let (verbatim, owned, legacy) = (means[0], means[1], means[2]);
        println!(
            "{shape:<14} {verbatim:>12.0} {owned:>12.0} {legacy:>12.0} {:>15.2}x {:>15.2}x",
            owned / verbatim,
            legacy / verbatim
        );
    }
    println!();
}

criterion_group!(benches, bench_produce_hot_path, bench_ratio);
criterion_main!(benches);
