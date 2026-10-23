//! `CodSpeed` microbenchmarks for `krabka-log`.
//!
//! These benchmarks cover the public `Log` surface: single and bulk appends,
//! reads at various offsets, tail reads across many retained segments, open
//! and recover with many segments, truncate, and time-based rolls. They deliberately exercise both the active-segment
//! hot path and the sealed-segment scan path.

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::{IoSlice, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use {
    criterion::{Criterion, black_box, criterion_group, criterion_main},
    krabka_ids::{LeaderEpoch, Offset, ProducerId},
    krabka_log::{AbortedTxn, Log, LogConfig, SegmentAllocation, TxnIndex, VerbatimBatch},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSize, bytes, gibibytes, kibibytes, mebibytes},
    std::{
        sync::{Arc, Mutex},
        time::Instant,
    },
    tempfile::tempdir,
};

/// A read budget larger than the whole benchmark log, so the "unbounded"
/// case never clips.
const UNBOUNDED: ByteSize = gibibytes(4);

krabka_macros::record_batch_fixture!(make_batch);

fn make_verbatim_batch(n: i32, payload_size: usize) -> VerbatimBatch {
    make_verbatim_from_batch(&make_batch(n, payload_size))
}

fn make_verbatim_from_batch(batch: &RecordBatch) -> VerbatimBatch {
    let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).unwrap();
    let is_txn = batch.attributes.is_transactional();
    let epoch = LeaderEpoch(batch.partition_leader_epoch);
    let pid = ProducerId(batch.producer_id);
    VerbatimBatch {
        bytes: buf.freeze(),
        last_offset_delta: batch.last_offset_delta,
        max_timestamp: batch.max_timestamp,
        leader_epoch: epoch,
        producer_id: pid,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        is_transactional: is_txn,
    }
}

// ---------------------------------------------------------------------------
// Append benchmarks
// ---------------------------------------------------------------------------

/// An open log together with the temporary directory that holds it.
struct TempLog {
    dir: tempfile::TempDir,
    log: Log,
}

fn open_log(config: LogConfig) -> TempLog {
    let scratch = tempdir().expect("tempdir");
    let target_path = scratch.path();
    let log = Log::open(target_path, config).expect("open log");
    TempLog { dir: scratch, log }
}

fn populated_log(config: LogConfig, appends: usize, records: i32, payload: usize) -> TempLog {
    let TempLog { dir, mut log } = open_log(config);
    for _ in 0..appends {
        log.append(&mut make_batch(records, payload)).unwrap();
    }
    TempLog { dir, log }
}

/// A shared log in a temporary directory, with a verbatim batch to append to it.
struct VerbatimFixture {
    dir: tempfile::TempDir,
    log: Arc<Mutex<Log>>,
    batch: VerbatimBatch,
}

fn verbatim_fixture(payload: usize) -> VerbatimFixture {
    let TempLog { dir, log } = open_log(LogConfig::default());
    let batch = make_verbatim_batch(1, payload);
    VerbatimFixture {
        dir,
        log: Arc::new(Mutex::new(log)),
        batch,
    }
}

/// An empty file in a temporary directory.
#[cfg(unix)]
struct FileFixture {
    dir: tempfile::TempDir,
    file: std::fs::File,
}

#[cfg(unix)]
fn file_fixture() -> FileFixture {
    let dir = tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("bench.log"))
        .unwrap();
    FileFixture { dir, file }
}

/// A [`VerbatimFixture`] with the tokio runtime that drives it.
struct AsyncVerbatimFixture {
    dir: tempfile::TempDir,
    log: Arc<Mutex<Log>>,
    batch: VerbatimBatch,
    runtime: tokio::runtime::Runtime,
}

fn async_verbatim_fixture() -> AsyncVerbatimFixture {
    let VerbatimFixture { dir, log, batch } = verbatim_fixture(100 * 1024);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    AsyncVerbatimFixture {
        dir,
        log,
        batch,
        runtime,
    }
}

fn bench_append_record_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append");

    for &(records, payload) in &[
        (1i32, 64usize),
        (10, 64),
        (100, 256),
        (100, 1024),
        (500, 256),
    ] {
        let TempLog { dir: _dir, mut log } = open_log(LogConfig::default());
        group.bench_function(format!("{records}rec_{payload}B"), |b| {
            b.iter(|| {
                let mut batch = make_batch(records, payload);
                log.append(&mut batch).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_append_large_message_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append_large_message");

    group.bench_function("owned_1rec_100KiB", |b| {
        let TempLog { dir: _dir, mut log } = open_log(LogConfig::default());
        b.iter(|| {
            let mut batch = make_batch(1, 100 * 1024);
            log.append(&mut batch).unwrap();
        });
    });

    for (label, payload) in [
        ("verbatim_1rec_100KiB", 100 * 1024),
        ("verbatim_1rec_512KiB", 512 * 1024),
    ] {
        group.bench_function(label, |b| {
            let TempLog { dir: _dir, mut log } = open_log(LogConfig::default());
            let batch = make_verbatim_batch(1, payload);
            b.iter(|| {
                log.append_verbatim(&batch).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_append_direct_groups(c: &mut Criterion) {
    let Some(path) = std::env::var_os("KRABKA_DIRECT_BENCH_DIR") else {
        return;
    };
    let batch = make_verbatim_batch(1, 64);
    let mut group = c.benchmark_group("log/direct_groups");
    for count in [8, 32, 64] {
        group.throughput(criterion::Throughput::Elements(count));
        let batches = vec![&batch; usize::try_from(count).unwrap()];
        for coalesce in [false, true] {
            let dir = tempfile::tempdir_in(&path).unwrap();
            let mut log = Log::open(dir.path(), LogConfig {
                segment_allocation: SegmentAllocation::Preallocate,
                ..LogConfig::default()
            })
            .unwrap();
            log.append_verbatim(&batch).unwrap();
            assert2::assert!(
                log.try_append_verbatim_group(&batches).is_some(),
                "benchmark directory must support Direct I/O"
            );
            let mode = if coalesce { "coalesced" } else { "individual" };
            group.bench_function(format!("{mode}_{count}"), |b| {
                b.iter(|| {
                    if coalesce && let Some(result) = log.try_append_verbatim_group(&batches) {
                        result.unwrap();
                    } else {
                        for &batch in &batches {
                            log.append_verbatim(batch).unwrap();
                        }
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_append_handoff(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append_handoff");

    group.bench_function("direct_mutex_verbatim_1rec_100KiB", |b| {
        let VerbatimFixture {
            dir: _dir,
            log,
            batch,
        } = verbatim_fixture(100 * 1024);
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                log.lock().unwrap().append_verbatim(&batch).unwrap();
            }
            start.elapsed()
        });
    });

    group.bench_function("spawn_blocking_mutex_verbatim_1rec_100KiB", |b| {
        let AsyncVerbatimFixture {
            dir: _dir,
            log,
            batch,
            runtime: rt,
        } = async_verbatim_fixture();
        b.iter_custom(|iters| {
            rt.block_on(async {
                let t0 = Instant::now();
                for _ in 0..iters {
                    let log = Arc::clone(&log);
                    let batch = batch.clone();
                    tokio::task::spawn_blocking(move || {
                        log.lock().unwrap().append_verbatim(&batch).unwrap();
                    })
                    .await
                    .unwrap();
                }
                t0.elapsed()
            })
        });
    });

    group.bench_function("block_in_place_mutex_verbatim_1rec_100KiB", |b| {
        let AsyncVerbatimFixture {
            dir: _dir,
            log,
            batch,
            runtime: rt,
        } = async_verbatim_fixture();
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    tokio::task::block_in_place(|| {
                        log.lock().unwrap().append_verbatim(&batch).unwrap();
                    });
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

#[cfg(unix)]
fn bench_file_write_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/file_write_shapes");
    let header = [0xABu8; 61];
    let body = vec![0xCDu8; (100 * 1024) - header.len()];

    group.bench_function("seek_end_writev_100KiB", |b| {
        let FileFixture { dir: _dir, file } = file_fixture();
        b.iter(|| {
            (&file).seek(SeekFrom::End(0)).unwrap();
            write_file_payload_vectored(&file, &header, &body);
        });
    });

    group.bench_function("writev_at_current_cursor_100KiB", |b| {
        let FileFixture { dir: _dir, file } = file_fixture();
        b.iter(|| {
            write_file_payload_vectored(&file, &header, &body);
        });
    });

    group.bench_function("write_all_at_twice_100KiB", |b| {
        let FileFixture { dir: _dir, file } = file_fixture();
        let mut position = 0u64;
        b.iter(|| {
            file.write_all_at(&header, position).unwrap();
            file.write_all_at(&body, position + header.len() as u64)
                .unwrap();
            position += (header.len() + body.len()) as u64;
        });
    });

    group.finish();
}

#[cfg(unix)]
fn write_file_payload_vectored(file: &std::fs::File, header: &[u8], body: &[u8]) {
    let bufs = [IoSlice::new(header), IoSlice::new(body)];
    let mut writer = file;
    let bytes_written = writer.write_vectored(&bufs).unwrap();
    assert2::assert!(bytes_written == header.len() + body.len());
}

#[cfg(not(unix))]
fn bench_file_write_shapes(_c: &mut Criterion) {}

// ---------------------------------------------------------------------------
// Read benchmarks at varied offsets / sizes
// ---------------------------------------------------------------------------

fn bench_read(c: &mut Criterion) {
    let TempLog { dir: _dir, log } = populated_log(LogConfig::default(), 200, 100, 256);
    let end = log.log_end_offset();

    let mut group = c.benchmark_group("log/read");

    for (label, offset, budget) in [
        ("from_start_1MiB", Offset(0), mebibytes(1)),
        ("from_start_unbounded", Offset(0), UNBOUNDED),
        ("from_middle_1MiB", Offset(end.0 / 2), mebibytes(1)),
        (
            "from_end_minus_100_1MiB",
            (end - 100).max(Offset(0)),
            mebibytes(1),
        ),
        ("past_end_returns_empty", end, mebibytes(1)),
    ] {
        group.bench_function(label, |b| {
            b.iter(|| {
                let out = log.read(black_box(offset), budget).unwrap();
                black_box(out);
            });
        });
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    let (_descriptor_dir, descriptor_log, descriptor_end) = {
        let TempLog {
            dir,
            log: mut descriptor_log,
        } = open_log(LogConfig::default());
        for _ in 0..1024 {
            let mut batch = make_batch(1, 64);
            descriptor_log.append(&mut batch).unwrap();
        }
        let descriptor_end = descriptor_log.log_end_offset();
        (dir, descriptor_log, descriptor_end)
    };

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    group.bench_function("descriptor_small_batches", |b| {
        b.iter(|| {
            let out = descriptor_log
                .read_raw_desc(black_box(Offset(0)), descriptor_end, UNBOUNDED)
                .unwrap();
            black_box(out);
        });
    });

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    let (_large_batch_dir, large_batch_log, large_batch_end) = {
        let TempLog {
            dir,
            log: mut large_batch_log,
        } = open_log(LogConfig::default());
        let mut batch = make_batch(1, 1024 * 1024);
        large_batch_log.append(&mut batch).unwrap();
        let end = large_batch_log.log_end_offset();
        (dir, large_batch_log, end)
    };

    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    group.bench_function("descriptor_large_batch", |b| {
        b.iter(|| {
            let out = large_batch_log
                .read_raw_desc(black_box(Offset(0)), large_batch_end, UNBOUNDED)
                .unwrap();
            black_box(out);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Tail reads — varied retained segment counts
// ---------------------------------------------------------------------------

/// The retained segment counts the tail-read benchmark compares.
const TAIL_SEGMENT_COUNTS: [usize; 4] = [1, 100, 1_000, 10_000];

/// The payload of every batch in the tail-read logs, and so of the tail the
/// benchmark reads.
const TAIL_PAYLOAD: usize = 1024;

/// A log of `segments` segments that each hold one one-record batch of
/// [`TAIL_PAYLOAD`] bytes.
///
/// A one-byte `segment_size` rolls the active segment on every append after
/// the first, so the log ends with `segments - 1` sealed segments and an
/// active one that holds the last batch.
fn tail_log(segments: usize) -> TempLog {
    populated_log(
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
        segments,
        1,
        TAIL_PAYLOAD,
    )
}

/// The open files a log of `segments` segments holds, with room to spare: each
/// segment keeps about three open, and appending briefly holds a fourth.
fn files_for(segments: usize) -> u64 {
    u64::try_from(segments)
        .unwrap_or(u64::MAX)
        .saturating_mul(4)
        .saturating_add(256)
}

/// Whether this process may open the files a log of `segments` segments holds.
///
/// GitHub's hosted runners allow 65,536, enough for every count here. A
/// container with a lower hard limit cannot hold the largest log, and the
/// benchmark skips that count rather than fail on `EMFILE` halfway through it.
#[cfg(target_os = "linux")]
fn open_files_allow(segments: usize) -> bool {
    rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .is_none_or(|limit| limit >= files_for(segments))
}

#[cfg(not(target_os = "linux"))]
fn open_files_allow(_segments: usize) -> bool { true }

/// A fetch of the last batch of a log, which is what a consumer or follower
/// at the tail asks for, as the number of retained segments grows.
///
/// The read finds its first segment by a binary search over the sealed
/// segments, so the time should not grow with the count. The second read
/// starts in the last sealed segment and crosses into the active one.
fn bench_read_tail(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/read_tail");

    for segments in TAIL_SEGMENT_COUNTS {
        if !open_files_allow(segments) {
            eprintln!(
                "log/read_tail: skipping {segments} segments, which need about {} open files",
                files_for(segments)
            );
            continue;
        }
        let TempLog { dir: _dir, log } = tail_log(segments);
        let end = log.log_end_offset();
        let last = end - 1;
        let crossing = (end - 2).max(Offset(0));

        group.bench_function(format!("decoded_last_batch_{segments}_segments"), |b| {
            b.iter(|| black_box(log.read(black_box(last), mebibytes(1)).unwrap()));
        });
        group.bench_function(format!("raw_last_batch_{segments}_segments"), |b| {
            b.iter(|| black_box(log.read_raw(black_box(last), end, mebibytes(1)).unwrap()));
        });
        group.bench_function(format!("raw_last_two_batches_{segments}_segments"), |b| {
            b.iter(|| {
                black_box(
                    log.read_raw(black_box(crossing), end, mebibytes(1))
                        .unwrap(),
                )
            });
        });
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        group.bench_function(format!("descriptor_last_batch_{segments}_segments"), |b| {
            b.iter(|| {
                black_box(
                    log.read_raw_desc(black_box(last), end, mebibytes(1))
                        .unwrap(),
                )
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Open / recovery — varied segment counts
// ---------------------------------------------------------------------------

fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/open");

    for &num_appends in &[50usize, 200, 500] {
        let TempLog { dir, log } = populated_log(
            LogConfig {
                segment_size: kibibytes(1),
                ..LogConfig::default()
            },
            num_appends,
            5,
            64,
        );
        drop(log);
        group.bench_function(format!("{num_appends}_appends_validate_on_open"), |b| {
            b.iter(|| {
                let log = Log::open(dir.path(), LogConfig::default()).unwrap();
                black_box(log);
            });
        });
        group.bench_function(format!("{num_appends}_appends_no_validate"), |b| {
            let cfg = LogConfig {
                validate_on_open: false,
                ..LogConfig::default()
            };
            b.iter(|| {
                let log = Log::open(dir.path(), cfg.clone()).unwrap();
                black_box(log);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Truncate
// ---------------------------------------------------------------------------

fn bench_truncate(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/truncate");

    group.bench_function("truncate_recent_offset", |b| {
        b.iter_with_setup(
            || {
                let TempLog { dir, log } = populated_log(LogConfig::default(), 50, 10, 64);
                let end = log.log_end_offset();
                (dir, log, end)
            },
            |(_dir, mut log, end)| {
                log.truncate_to(end - 50).unwrap();
                black_box(log);
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// log_end_offset / log_start_offset accessors — should be O(1)
// ---------------------------------------------------------------------------

fn bench_accessors(c: &mut Criterion) {
    let TempLog { dir: _dir, log } = populated_log(LogConfig::default(), 100, 10, 64);

    let mut group = c.benchmark_group("log/accessors");
    group.bench_function("log_end_offset", |b| {
        b.iter(|| black_box(&log).log_end_offset());
    });
    group.bench_function("log_start_offset", |b| {
        b.iter(|| black_box(&log).log_start_offset());
    });
    group.bench_function("lso", |b| {
        b.iter(|| black_box(&log).lso());
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Transaction-index benchmarks
// ---------------------------------------------------------------------------

fn txn_entry(n: i64) -> AbortedTxn {
    AbortedTxn {
        producer_id: ProducerId(n),
        start_offset: Offset(n * 2),
        last_offset: Offset(n * 2 + 1),
        last_stable_offset: Offset(n * 2 + 2),
    }
}

fn txn_bytes(entries: impl IntoIterator<Item = AbortedTxn>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for entry in entries {
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&entry.producer_id.0.to_be_bytes());
        bytes.extend_from_slice(&entry.start_offset.0.to_be_bytes());
        bytes.extend_from_slice(&entry.last_offset.0.to_be_bytes());
        bytes.extend_from_slice(&entry.last_stable_offset.0.to_be_bytes());
    }
    bytes
}

/// Measure decode/dedup independently from append I/O. Duplicate rates are
/// exact retries distributed through the file rather than adjacent records.
fn bench_txn_index_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("txn_index/open");
    for distinct in [1_000_i64, 10_000, 100_000] {
        for duplicate_percent in [0_i64, 50, 90] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("00000000000000000000.txnindex");
            let total = distinct * 100 / (100 - duplicate_percent);
            let entries = (0..total).map(|n| txn_entry(n % distinct));
            std::fs::write(&path, txn_bytes(entries)).unwrap();
            group.bench_function(
                format!("{distinct}_distinct_{duplicate_percent}pct_duplicates"),
                |b| b.iter(|| black_box(TxnIndex::open(path.clone()).unwrap())),
            );
        }
    }
    group.finish();
}

/// Exact-duplicate append retries do no file I/O, isolating membership lookup
/// as the number of distinct transactions in the active segment grows.
fn bench_txn_index_append_retry(c: &mut Criterion) {
    let mut group = c.benchmark_group("txn_index/append_retry");
    for distinct in [1_000_i64, 10_000, 100_000] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.txnindex");
        std::fs::write(&path, txn_bytes((0..distinct).map(txn_entry))).unwrap();
        let mut index = TxnIndex::open(path).unwrap();
        let mut candidate = 0;
        group.bench_function(format!("{distinct}_distinct"), |b| {
            b.iter(|| {
                candidate = (candidate + 7919) % distinct;
                index.append(black_box(txn_entry(candidate))).unwrap();
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_append_record_sizes,
    bench_append_large_message_paths,
    bench_append_direct_groups,
    bench_append_handoff,
    bench_file_write_shapes,
    bench_read,
    bench_read_tail,
    bench_open,
    bench_truncate,
    bench_accessors,
    bench_txn_index_open,
    bench_txn_index_append_retry,
);
criterion_main!(benches);
