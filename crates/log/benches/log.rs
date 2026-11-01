//! `CodSpeed` microbenchmarks for `krabka-log`.
//!
//! These benchmarks cover the public `Log` surface: single and bulk appends,
//! reads at various offsets, tail reads across many retained segments, open
//! and recover with many segments, truncate, and time-based rolls. They deliberately exercise both the active-segment
//! hot path and the sealed-segment scan path.

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::{self, IoSlice, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use {
    criterion::{BenchmarkGroup, Criterion, black_box, measurement::WallTime},
    krabka_ids::{LeaderEpoch, Offset, ProducerId},
    krabka_log::{AbortedTxn, Log, LogConfig, SegmentAllocation, TxnIndex, VerbatimBatch},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSize, bytes, gibibytes, kibibytes, mebibytes},
    rustix::process::{Resource, getrlimit},
    std::{
        cell::RefCell,
        env,
        fs::{self, File},
        sync::{Arc, Mutex, PoisonError},
        time::Instant,
    },
    tempfile::tempdir,
    tokio::{runtime::Runtime, task},
};

/// A read budget larger than the whole benchmark log, so the "unbounded"
/// case never clips.
const UNBOUNDED: ByteSize = gibibytes(4);

krabka_macros::record_batch_fixture!(make_batch);

krabka_macros::bench_failures!();

fn make_verbatim_batch(n: i32, payload_size: usize) -> BenchResult<VerbatimBatch> {
    make_verbatim_from_batch(&make_batch(n, payload_size))
}

fn make_verbatim_from_batch(batch: &RecordBatch) -> BenchResult<VerbatimBatch> {
    let mut buf = bytes::BytesMut::with_capacity(batch.encoded_batch_len());
    batch.encode_batch(&mut buf)?;
    let is_txn = batch.attributes.is_transactional();
    let epoch = LeaderEpoch(batch.partition_leader_epoch);
    let pid = ProducerId(batch.producer_id);
    Ok(VerbatimBatch {
        bytes: buf.freeze(),
        last_offset_delta: batch.last_offset_delta,
        max_timestamp: batch.max_timestamp,
        leader_epoch: epoch,
        producer_id: pid,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        is_transactional: is_txn,
    })
}

// ---------------------------------------------------------------------------
// Append benchmarks
// ---------------------------------------------------------------------------

/// An open log together with the temporary directory that holds it.
struct TempLog {
    dir: tempfile::TempDir,
    log: Log,
}

fn open_log(config: LogConfig) -> BenchResult<TempLog> {
    let scratch = tempdir()?;
    let target_path = scratch.path();
    let log = Log::open(target_path, config)?;
    Ok(TempLog { dir: scratch, log })
}

fn populated_log(
    config: LogConfig,
    appends: usize,
    records: i32,
    payload: usize,
) -> BenchResult<TempLog> {
    let TempLog { dir, mut log } = open_log(config)?;
    for _ in 0..appends {
        log.append(&mut make_batch(records, payload))?;
    }
    Ok(TempLog { dir, log })
}

/// A shared log in a temporary directory, with a verbatim batch to append to it.
struct VerbatimFixture {
    dir: tempfile::TempDir,
    log: Arc<Mutex<Log>>,
    batch: VerbatimBatch,
}

fn verbatim_fixture(payload: usize) -> BenchResult<VerbatimFixture> {
    let TempLog { dir, log } = open_log(LogConfig::default())?;
    let batch = make_verbatim_batch(1, payload)?;
    Ok(VerbatimFixture {
        dir,
        log: Arc::new(Mutex::new(log)),
        batch,
    })
}

/// An empty file in a temporary directory.
#[cfg(unix)]
struct FileFixture {
    dir: tempfile::TempDir,
    file: File,
}

#[cfg(unix)]
fn file_fixture() -> BenchResult<FileFixture> {
    let dir = tempdir()?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(dir.path().join("bench.log"))?;
    Ok(FileFixture { dir, file })
}

/// A [`VerbatimFixture`] with the tokio runtime that drives it.
struct AsyncVerbatimFixture {
    dir: tempfile::TempDir,
    log: Arc<Mutex<Log>>,
    batch: VerbatimBatch,
    runtime: Runtime,
}

fn async_verbatim_fixture() -> BenchResult<AsyncVerbatimFixture> {
    let VerbatimFixture { dir, log, batch } = verbatim_fixture(100 * 1024)?;
    let runtime = Runtime::new()?;
    Ok(AsyncVerbatimFixture {
        dir,
        log,
        batch,
        runtime,
    })
}

fn bench_append_record_sizes(c: &mut Criterion) { run(c, "log/append", append_record_sizes); }

fn append_record_sizes(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();

    for &(records, payload) in &[
        (1i32, 64usize),
        (10, 64),
        (100, 256),
        (100, 1024),
        (500, 256),
    ] {
        let TempLog { dir: _dir, mut log } = open_log(LogConfig::default())?;
        group.bench_function(format!("{records}rec_{payload}B"), |b| {
            b.iter(|| {
                let mut batch = make_batch(records, payload);
                failed.record(log.append(&mut batch));
            });
        });
    }

    failed.into_result()
}

fn bench_append_large_message_paths(c: &mut Criterion) {
    run(c, "log/append_large_message", append_large_message_paths);
}

fn append_large_message_paths(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();

    group.bench_function("owned_1rec_100KiB", |b| {
        let Some(TempLog { dir: _dir, mut log }) = failed.record(open_log(LogConfig::default()))
        else {
            return;
        };
        b.iter(|| {
            let mut batch = make_batch(1, 100 * 1024);
            failed.record(log.append(&mut batch));
        });
    });

    for (label, payload) in [
        ("verbatim_1rec_100KiB", 100 * 1024),
        ("verbatim_1rec_512KiB", 512 * 1024),
    ] {
        group.bench_function(label, |b| {
            let Some((TempLog { dir: _dir, mut log }, batch)) = failed.record(
                open_log(LogConfig::default())
                    .and_then(|log| Ok((log, make_verbatim_batch(1, payload)?))),
            ) else {
                return;
            };
            b.iter(|| {
                failed.record(log.append_verbatim(&batch));
            });
        });
    }

    failed.into_result()
}

fn bench_append_direct_groups(c: &mut Criterion) {
    run(c, "log/direct_groups", append_direct_groups);
}

fn append_direct_groups(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let Some(path) = env::var_os("KRABKA_DIRECT_BENCH_DIR") else {
        return Ok(());
    };
    let batch = make_verbatim_batch(1, 64)?;
    let mut failed = FirstError::default();
    for count in [8_usize, 32, 64] {
        group.throughput(criterion::Throughput::Elements(count as u64));
        let batches = vec![&batch; count];
        for coalesce in [false, true] {
            let dir = tempfile::tempdir_in(&path)?;
            let mut log = Log::open(dir.path(), LogConfig {
                segment_allocation: SegmentAllocation::Preallocate,
                ..LogConfig::default()
            })?;
            log.append_verbatim(&batch)?;
            if log.try_append_verbatim_group(&batches).is_none() {
                return Err("benchmark directory must support Direct I/O".into());
            }
            let mode = if coalesce { "coalesced" } else { "individual" };
            group.bench_function(format!("{mode}_{count}"), |b| {
                b.iter(|| {
                    if coalesce && let Some(result) = log.try_append_verbatim_group(&batches) {
                        failed.record(result);
                    } else {
                        for &batch in &batches {
                            failed.record(log.append_verbatim(batch));
                        }
                    }
                });
            });
        }
    }
    failed.into_result()
}

fn bench_append_handoff(c: &mut Criterion) { run(c, "log/append_handoff", append_handoff); }

fn append_handoff(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();

    group.bench_function("direct_mutex_verbatim_1rec_100KiB", |b| {
        let Some(VerbatimFixture {
            dir: _dir,
            log,
            batch,
        }) = failed.record(verbatim_fixture(100 * 1024))
        else {
            return;
        };
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                failed.record(
                    log.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .append_verbatim(&batch),
                );
            }
            start.elapsed()
        });
    });

    group.bench_function("spawn_blocking_mutex_verbatim_1rec_100KiB", |b| {
        let Some(AsyncVerbatimFixture {
            dir: _dir,
            log,
            batch,
            runtime: rt,
        }) = failed.record(async_verbatim_fixture())
        else {
            return;
        };
        b.iter_custom(|iters| {
            rt.block_on(async {
                let t0 = Instant::now();
                for _ in 0..iters {
                    let log = Arc::clone(&log);
                    let batch = batch.clone();
                    let appended = task::spawn_blocking(move || {
                        log.lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .append_verbatim(&batch)
                    })
                    .await;
                    if let Some(appended) = failed.record(appended) {
                        failed.record(appended);
                    }
                }
                t0.elapsed()
            })
        });
    });

    group.bench_function("block_in_place_mutex_verbatim_1rec_100KiB", |b| {
        let Some(fixture) = failed.record(async_verbatim_fixture()) else {
            return;
        };
        b.iter_custom(|iters| {
            fixture.runtime.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    task::block_in_place(|| {
                        failed.record(
                            fixture
                                .log
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .append_verbatim(&fixture.batch),
                        );
                    });
                }
                start.elapsed()
            })
        });
    });

    failed.into_result()
}

#[cfg(unix)]
fn bench_file_write_shapes(c: &mut Criterion) {
    run(c, "log/file_write_shapes", file_write_shapes);
}

#[cfg(unix)]
fn file_write_shapes(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();
    let header = [0xABu8; 61];
    let body = vec![0xCDu8; (100 * 1024) - header.len()];

    group.bench_function("seek_end_writev_100KiB", |b| {
        let Some(FileFixture { dir: _dir, file }) = failed.record(file_fixture()) else {
            return;
        };
        b.iter(|| {
            failed.record(
                (&file)
                    .seek(SeekFrom::End(0))
                    .and_then(|_| write_file_payload_vectored(&file, &header, &body)),
            );
        });
    });

    group.bench_function("writev_at_current_cursor_100KiB", |b| {
        let Some(FileFixture { dir: _dir, file }) = failed.record(file_fixture()) else {
            return;
        };
        b.iter(|| {
            failed.record(write_file_payload_vectored(&file, &header, &body));
        });
    });

    group.bench_function("write_all_at_twice_100KiB", |b| {
        let Some(FileFixture { dir: _dir, file }) = failed.record(file_fixture()) else {
            return;
        };
        let mut position = 0u64;
        b.iter(|| {
            failed.record(
                file.write_all_at(&header, position)
                    .and_then(|()| file.write_all_at(&body, position + header.len() as u64)),
            );
            position += (header.len() + body.len()) as u64;
        });
    });

    failed.into_result()
}

#[cfg(unix)]
fn write_file_payload_vectored(file: &File, header: &[u8], body: &[u8]) -> io::Result<()> {
    let bufs = [IoSlice::new(header), IoSlice::new(body)];
    let mut writer = file;
    let bytes_written = writer.write_vectored(&bufs)?;
    if bytes_written == header.len() + body.len() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "a vectored write took {bytes_written} of {} bytes",
            header.len() + body.len()
        )))
    }
}

#[cfg(not(unix))]
fn bench_file_write_shapes(_c: &mut Criterion) {}

// ---------------------------------------------------------------------------
// Read benchmarks at varied offsets / sizes
// ---------------------------------------------------------------------------

fn bench_read(c: &mut Criterion) { run(c, "log/read", read); }

fn read(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let TempLog { dir: _dir, log } = populated_log(LogConfig::default(), 200, 100, 256)?;
    let end = log.log_end_offset();

    let mut failed = FirstError::default();

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
                let out = failed.record(log.read(black_box(offset), budget));
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
        } = open_log(LogConfig::default())?;
        for _ in 0..1024 {
            let mut batch = make_batch(1, 64);
            descriptor_log.append(&mut batch)?;
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
            let out = failed.record(descriptor_log.read_raw_desc(
                black_box(Offset(0)),
                descriptor_end,
                UNBOUNDED,
            ));
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
        } = open_log(LogConfig::default())?;
        let mut batch = make_batch(1, 1024 * 1024);
        large_batch_log.append(&mut batch)?;
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
            let out = failed.record(large_batch_log.read_raw_desc(
                black_box(Offset(0)),
                large_batch_end,
                UNBOUNDED,
            ));
            black_box(out);
        });
    });

    failed.into_result()
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
fn tail_log(segments: usize) -> BenchResult<TempLog> {
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
    getrlimit(Resource::Nofile)
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
fn bench_read_tail(c: &mut Criterion) { run(c, "log/read_tail", read_tail); }

fn read_tail(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();

    for segments in TAIL_SEGMENT_COUNTS {
        if !open_files_allow(segments) {
            eprintln!(
                "log/read_tail: skipping {segments} segments, which need about {} open files",
                files_for(segments)
            );
            continue;
        }
        let TempLog { dir: _dir, log } = tail_log(segments)?;
        let end = log.log_end_offset();
        let last = end - 1;
        let crossing = (end - 2).max(Offset(0));

        group.bench_function(format!("decoded_last_batch_{segments}_segments"), |b| {
            b.iter(|| black_box(failed.record(log.read(black_box(last), mebibytes(1)))));
        });
        group.bench_function(format!("raw_last_batch_{segments}_segments"), |b| {
            b.iter(|| black_box(failed.record(log.read_raw(black_box(last), end, mebibytes(1)))));
        });
        group.bench_function(format!("raw_last_two_batches_{segments}_segments"), |b| {
            b.iter(|| {
                black_box(failed.record(log.read_raw(black_box(crossing), end, mebibytes(1))))
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
                black_box(failed.record(log.read_raw_desc(black_box(last), end, mebibytes(1))))
            });
        });
    }

    failed.into_result()
}

// ---------------------------------------------------------------------------
// Open / recovery — varied segment counts
// ---------------------------------------------------------------------------

fn bench_open(c: &mut Criterion) { run(c, "log/open", open); }

fn open(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();

    for &num_appends in &[50usize, 200, 500] {
        let TempLog { dir, log } = populated_log(
            LogConfig {
                segment_size: kibibytes(1),
                ..LogConfig::default()
            },
            num_appends,
            5,
            64,
        )?;
        drop(log);
        group.bench_function(format!("{num_appends}_appends_validate_on_open"), |b| {
            b.iter(|| {
                let log = failed.record(Log::open(dir.path(), LogConfig::default()));
                black_box(log);
            });
        });
        group.bench_function(format!("{num_appends}_appends_no_validate"), |b| {
            let cfg = LogConfig {
                validate_on_open: false,
                ..LogConfig::default()
            };
            b.iter(|| {
                let log = failed.record(Log::open(dir.path(), cfg.clone()));
                black_box(log);
            });
        });
    }

    failed.into_result()
}

// ---------------------------------------------------------------------------
// Truncate
// ---------------------------------------------------------------------------

fn bench_truncate(c: &mut Criterion) { run(c, "log/truncate", truncate); }

fn truncate(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let failed = RefCell::new(FirstError::default());

    group.bench_function("truncate_recent_offset", |b| {
        b.iter_with_setup(
            || {
                failed
                    .borrow_mut()
                    .record(populated_log(LogConfig::default(), 50, 10, 64))
                    .map(|TempLog { dir, log }| {
                        let end = log.log_end_offset();
                        (dir, log, end)
                    })
            },
            |setup| {
                if let Some((_dir, mut log, end)) = setup {
                    failed.borrow_mut().record(log.truncate_to(end - 50));
                    black_box(log);
                }
            },
        );
    });

    failed.into_inner().into_result()
}

// ---------------------------------------------------------------------------
// log_end_offset / log_start_offset accessors — should be O(1)
// ---------------------------------------------------------------------------

fn bench_accessors(c: &mut Criterion) { run(c, "log/accessors", accessors); }

fn accessors(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let TempLog { dir: _dir, log } = populated_log(LogConfig::default(), 100, 10, 64)?;

    group.bench_function("log_end_offset", |b| {
        b.iter(|| black_box(&log).log_end_offset());
    });
    group.bench_function("log_start_offset", |b| {
        b.iter(|| black_box(&log).log_start_offset());
    });
    group.bench_function("lso", |b| {
        b.iter(|| black_box(&log).lso());
    });
    Ok(())
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
fn bench_txn_index_open(c: &mut Criterion) { run(c, "txn_index/open", txn_index_open); }

fn txn_index_open(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();
    for distinct in [1_000_i64, 10_000, 100_000] {
        for duplicate_percent in [0_i64, 50, 90] {
            let dir = tempdir()?;
            let path = dir.path().join("00000000000000000000.txnindex");
            let total = distinct * 100 / (100 - duplicate_percent);
            let entries = (0..total).map(|n| txn_entry(n % distinct));
            fs::write(&path, txn_bytes(entries))?;
            group.bench_function(
                format!("{distinct}_distinct_{duplicate_percent}pct_duplicates"),
                |b| {
                    b.iter(|| black_box(failed.record(TxnIndex::open(path.clone()))));
                },
            );
        }
    }
    failed.into_result()
}

/// Exact-duplicate append retries do no file I/O, isolating membership lookup
/// as the number of distinct transactions in the active segment grows.
fn bench_txn_index_append_retry(c: &mut Criterion) {
    run(c, "txn_index/append_retry", txn_index_append_retry);
}

fn txn_index_append_retry(group: &mut BenchmarkGroup<'_, WallTime>) -> BenchResult {
    let mut failed = FirstError::default();
    for distinct in [1_000_i64, 10_000, 100_000] {
        let dir = tempdir()?;
        let path = dir.path().join("00000000000000000000.txnindex");
        fs::write(&path, txn_bytes((0..distinct).map(txn_entry)))?;
        let mut index = TxnIndex::open(path)?;
        let mut candidate = 0;
        group.bench_function(format!("{distinct}_distinct"), |b| {
            b.iter(|| {
                candidate = (candidate + 7919) % distinct;
                failed.record(index.append(black_box(txn_entry(candidate))));
            });
        });
    }
    failed.into_result()
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

krabka_macros::bench_main!(
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
    report_failures
);
