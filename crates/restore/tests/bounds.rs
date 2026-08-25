//! End-to-end proof that the restore bound changes what a full restore
//! actually WRITES, through the whole `discover -> verify -> bound ->
//! materialize` pipeline behind [`crabka_restore::restore`] -- not just what
//! `Predicates::decide_batch`/`decide_record` decide in isolation, and not
//! just what `materialize::write_segment` does when handed a hand-built
//! `VerifiedSegment` directly.
//!
//! Every scenario archives real batches through a real `crabka_log::Log`
//! and a real `LocalTieredStorage`, the same pattern
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` uses to build a
//! KIP-405 archive, then drives `restore()` and reads the restored
//! partition back with a fresh `crabka_log::Log`.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use assert2::{assert, check};
use bytes::Bytes;
use clap::Parser as _;
use crabka_ids::{LeaderEpoch, Offset};
use crabka_log::{Log, LogConfig, name};
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};
use crabka_remote_storage::{
    LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};
use crabka_restore::{Cli, RestoreArgs, restore};
use tempfile::TempDir;
use uuid::Uuid;

/// Base timestamp every fixture batch's `base_timestamp` starts from, so a
/// record's absolute timestamp is `BASE_TIMESTAMP + timestamp_delta`.
const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

/// A `.leader_epoch_checkpoint`'s bytes: one entry, epoch 0 at offset 0.
/// `verify_segment` checks this file's own internal framing only, never
/// against the segment's actual offset range (see
/// `crates/restore/src/verify.rs`'s `parse_leader_epoch_checkpoint`), so the
/// same bytes are valid for every fixture segment below regardless of what
/// it archives.
const LEADER_EPOCH_CHECKPOINT: &[u8] = b"0\n1\n0 0\n";

// ---------------------------------------------------------------------
// Fixture construction
// ---------------------------------------------------------------------

/// A `LogConfig` that rolls the active segment on every append past the
/// first, so [`build_archive`] can seal each fixture batch into its own
/// segment.
///
/// `crabka-restore` does not depend on `crabka-units`, so this scales the
/// log crate's own default `segment_size` -- Kafka's documented 1 GiB
/// `segment.bytes` -- down by its own byte count rather than naming
/// `crabka_units::ByteSize` directly. The result is far smaller than any
/// batch this file builds (every one is at least several dozen bytes) even
/// if `crabka_log`'s default ever changes, because the division only ever
/// shrinks it further.
fn roll_after_every_batch() -> LogConfig {
    LogConfig {
        segment_size: LogConfig::default().segment_size / 1_073_741_824.0,
        ..LogConfig::default()
    }
}

/// One throwaway batch, appended after every real fixture batch so the log
/// rolls and seals the batch before it. Its own bytes never reach the
/// archive: [`build_archive`] only copies `Log::tierable_segments`'s sealed
/// segments, and this trigger always ends up in the still-active one.
fn roll_trigger() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"roll-trigger")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// Append every batch in `batches` to a fresh local log, in order, each
/// sealed into its own segment via [`roll_after_every_batch`], then archive
/// every sealed segment into a fresh KIP-405 local tiered-storage tree for
/// `topic`-`partition`.
///
/// Mutates each batch in place with the `base_offset` `Log::append` assigns
/// it, so the caller's own copies become the exact values a restore's Keep
/// path round-trips verbatim -- ready to reuse as expected values.
///
/// Returns the archive root, to pass as `--archive-local`.
fn build_archive(topic: &str, partition: i32, batches: &mut [RecordBatch]) -> TempDir {
    assert!(!batches.is_empty(), "a fixture needs at least one batch");
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), roll_after_every_batch()).expect("open local log");
    for batch in batches.iter_mut() {
        log.append(batch).expect("append fixture batch");
    }
    // Seals the last real batch above; `tierable_segments` below excludes
    // whatever segment this trigger itself lands in.
    log.append(&mut roll_trigger())
        .expect("append roll trigger");

    let sealed = log.tierable_segments();
    assert!(
        sealed.len() == batches.len(),
        "expected one sealed segment per fixture batch, got {} for {}",
        sealed.len(),
        batches.len(),
    );

    let topic_id = Uuid::new_v4();
    let archive_root = tempfile::tempdir().expect("archive root tempdir");
    let storage = LocalTieredStorage::new(archive_root.path());
    for export in sealed {
        let metadata = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(topic_id, topic, partition),
                Uuid::new_v4(),
            ),
            export.base_offset.0,
            export.last_offset.0,
            export.max_timestamp,
            1,
            0,
            RemoteLogSegmentDetails::new(
                i32::try_from(
                    std::fs::metadata(&export.log_path)
                        .expect("fixture segment metadata")
                        .len(),
                )
                .expect("test segment fits i32"),
                RemoteLogSegmentState::CopySegmentFinished,
                BTreeMap::from([(LeaderEpoch(0), export.base_offset.0)]),
            ),
        )
        .expect("valid remote metadata");
        storage
            .copy_log_segment_data(
                &metadata,
                &LogSegmentData {
                    log_segment: export.log_path.clone(),
                    offset_index: export.offset_index_path.clone(),
                    time_index: export.time_index_path.clone(),
                    transaction_index: export.transaction_index_path.clone(),
                    producer_snapshot_index: Some(export.producer_snapshot_path.clone()),
                    leader_epoch_index: Bytes::from_static(LEADER_EPOCH_CHECKPOINT),
                },
            )
            .expect("archive the segment");
    }
    archive_root
}

// ---------------------------------------------------------------------
// Record and batch builders
// ---------------------------------------------------------------------

fn value_record(offset_delta: i32, value: &str) -> Record {
    Record {
        offset_delta,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        ..Record::default()
    }
}

fn keyed_record(offset_delta: i32, key: &str, value: &str) -> Record {
    Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        ..value_record(offset_delta, value)
    }
}

fn timestamped_record(offset_delta: i32, timestamp_delta: i64, value: &str) -> Record {
    Record {
        timestamp_delta,
        ..value_record(offset_delta, value)
    }
}

fn headered_record(
    offset_delta: i32,
    value: &str,
    header_name: &str,
    header_value: &str,
) -> Record {
    Record {
        headers: vec![RecordHeader {
            key: header_name.to_owned(),
            value: Some(Bytes::copy_from_slice(header_value.as_bytes())),
        }],
        ..value_record(offset_delta, value)
    }
}

/// A batch from `producer_id`, with `base_offset` overwritten by
/// `Log::append`. `last_offset_delta` and `max_timestamp` are derived from
/// `records`, matching the convention `bound.rs`'s and `materialize.rs`'s
/// own unit tests already use.
fn producer_batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    let last_offset_delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
    let max_delta = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
    RecordBatch {
        last_offset_delta,
        base_timestamp: BASE_TIMESTAMP,
        max_timestamp: BASE_TIMESTAMP + max_delta,
        producer_id,
        records,
        ..RecordBatch::default()
    }
}

/// A batch with no producer id (the ordinary, non-idempotent case).
fn plain_batch(records: Vec<Record>) -> RecordBatch {
    producer_batch(-1, records)
}

/// A transactional (non-control) batch for `producer_id`.
fn transactional_batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..producer_batch(producer_id, records)
    }
}

/// A 4-byte control-marker key: `(version=0: i16, marker_type: i16)` BE.
fn control_key(marker_type: i16) -> Bytes {
    let mut buf = [0u8; 4];
    buf[0..2].copy_from_slice(&0i16.to_be_bytes());
    buf[2..4].copy_from_slice(&marker_type.to_be_bytes());
    Bytes::from(buf.to_vec())
}

/// A control-marker value: `(version=0: i16, coordinator epoch: i32)` BE.
fn control_value(coordinator_epoch: i32) -> Bytes {
    let mut buf = [0u8; 6];
    buf[0..2].copy_from_slice(&0i16.to_be_bytes());
    buf[2..6].copy_from_slice(&coordinator_epoch.to_be_bytes());
    Bytes::from(buf.to_vec())
}

/// A COMMIT control batch (`marker_type=1`) for `producer_id`, matching the
/// shape `crates/log/src/log.rs`'s own `commit_marker` test helper builds.
fn commit_marker(producer_id: i64) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            key: Some(control_key(1 /* COMMIT */)),
            value: Some(control_value(0)),
            ..Record::default()
        }],
        ..producer_batch(producer_id, vec![])
    }
}

// ---------------------------------------------------------------------
// Restore driving and read-back
// ---------------------------------------------------------------------

/// Build the `RestoreArgs` every scenario shares -- a local archive, a
/// fresh target directory, and a standalone node 1, matching the shape
/// `materialize.rs`'s own tests use to satisfy `format_target`'s
/// `--node-id` requirement -- via `Cli::try_parse_from`, the same path the
/// binary and the crate's own tests use. `extra` carries the bound flags
/// under test.
fn restore_args(archive_dir: &Path, target_dir: &Path, extra: &[&str]) -> RestoreArgs {
    let mut argv: Vec<String> = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_dir.display().to_string(),
        "--log-dir".to_owned(),
        target_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// Run a restore of `archive_dir` with `extra` bound flags, into a fresh
/// empty target directory. Returns the target's `TempDir` (keep it alive
/// for as long as the returned path is read) and the target log directory
/// itself.
async fn run_restore(archive_dir: &Path, extra: &[&str]) -> (TempDir, PathBuf) {
    let target = tempfile::tempdir().expect("target tempdir");
    let target_dir = target.path().join("restored");
    let args = restore_args(archive_dir, &target_dir, extra);
    restore(&args).await.expect("restore");
    (target, target_dir)
}

/// Reopen the partition `restore()` wrote, the way an operator would after
/// the tool exits.
fn reopen(target_dir: &Path, topic: &str, partition: i32) -> Log {
    let dir = name::partition_dir(target_dir, topic, partition);
    Log::open(&dir, LogConfig::default()).expect("reopen restored partition")
}

// ---------------------------------------------------------------------
// Scenario 1: --to-offset is boundary-inclusive and truncates the tail.
// ---------------------------------------------------------------------

#[tokio::test]
async fn to_offset_bound_is_inclusive_and_truncates_the_tail() {
    let mut fixture: Vec<RecordBatch> = (0..10)
        .map(|i: i32| plain_batch(vec![value_record(0, &format!("v{i}"))]))
        .collect();
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--to-offset", "orders:0=5"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(6));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    check!(read.batches == fixture[..=5].to_vec());
}

// ---------------------------------------------------------------------
// Scenario 2: --to-timestamp keeps only records strictly before the bound.
// ---------------------------------------------------------------------

#[tokio::test]
async fn to_timestamp_keeps_only_records_strictly_before_the_bound() {
    let mut fixture = vec![plain_batch(vec![
        timestamped_record(0, 0, "t0"),
        timestamped_record(1, 10, "t1"),
        timestamped_record(2, 20, "t2"),
        timestamped_record(3, 30, "t3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);
    let bound = (BASE_TIMESTAMP + 25).to_string();

    let (_target, target_dir) = run_restore(archive.path(), &["--to-timestamp", &bound]).await;

    let log = reopen(&target_dir, "orders", 0);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![RecordBatch {
        records: vec![
            timestamped_record(0, 0, "t0"),
            timestamped_record(1, 10, "t1"),
            timestamped_record(2, 20, "t2"),
        ],
        ..fixture[0].clone()
    }];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 3: --exclude-key drops a MIDDLE record without shifting
// anything else.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_drops_a_middle_record_without_shifting_later_offsets() {
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "keep-1", "v0"),
            keyed_record(1, "drop-2", "v1"),
            keyed_record(2, "keep-3", "v2"),
        ]),
        plain_batch(vec![value_record(0, "keep-4")]),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--exclude-key", "^drop"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(4));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![
        RecordBatch {
            records: vec![
                keyed_record(0, "keep-1", "v0"),
                keyed_record(2, "keep-3", "v2"),
            ],
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 4: --exclude-key emptying a WHOLE batch still claims its
// offsets, and an adjacent batch is untouched.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_matching_every_record_of_one_batch_still_claims_its_offsets() {
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "drop-a", "va"),
            keyed_record(1, "drop-b", "vb"),
        ]),
        plain_batch(vec![value_record(0, "keep-c")]),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--exclude-key", "^drop"]).await;

    let log = reopen(&target_dir, "orders", 0);
    // The emptied batch's offset range (0..=1) is still claimed, so the
    // untouched batch after it lands at its own original offset (2).
    check!(log.log_end_offset() == Offset(3));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![
        RecordBatch {
            records: Vec::new(),
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 5: dropping a batch's TRAILING record does not strand the
// batch archived after it, end to end.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_dropping_a_batchs_trailing_record_survives_the_full_pipeline() {
    // `materialize.rs`'s own
    // `a_filtered_batch_that_drops_its_trailing_record_does_not_strand_the_next_batch`
    // covers this at the `write_segment` level directly. This drives the
    // same shape through `discover -> verify -> bound -> materialize`.
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "keep-x", "v0"),
            keyed_record(1, "drop-y", "v1"),
        ]),
        plain_batch(vec![value_record(0, "keep-z")]),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--exclude-key", "^drop"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(3));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![
        RecordBatch {
            records: vec![keyed_record(0, "keep-x", "v0")],
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 6: --exclude-header matches on name AND value, never on name
// alone or on an absent header.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_header_matches_on_name_and_value_not_name_alone() {
    let mut fixture = vec![plain_batch(vec![
        headered_record(0, "v0", "trace", "bad-1"),
        headered_record(1, "v1", "trace", "good-1"),
        value_record(2, "v2"), // no headers at all
    ])];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) =
        run_restore(archive.path(), &["--exclude-header", "trace=^bad"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(3));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![RecordBatch {
        records: vec![
            headered_record(1, "v1", "trace", "good-1"),
            value_record(2, "v2"),
        ],
        ..fixture[0].clone()
    }];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 7: --exclude-producer-id drops one producer's whole batch and
// leaves the other producer's batch untouched.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_producer_id_drops_only_that_producers_batch() {
    let mut fixture = vec![
        producer_batch(
            101,
            vec![value_record(0, "p101-1"), value_record(1, "p101-2")],
        ),
        producer_batch(202, vec![value_record(0, "p202-1")]),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) =
        run_restore(archive.path(), &["--exclude-producer-id", "101"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(3));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![
        RecordBatch {
            records: Vec::new(),
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 8: --exclude-offset's half-open and inclusive spellings
// exclude the same offsets, over the same archived data.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_offset_half_open_and_inclusive_spellings_agree() {
    let mut fixture = vec![plain_batch(vec![
        value_record(0, "keep-0"),
        value_record(1, "drop-1"),
        value_record(2, "drop-2"),
        value_record(3, "keep-3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);
    let expected = vec![RecordBatch {
        records: vec![value_record(0, "keep-0"), value_record(3, "keep-3")],
        ..fixture[0].clone()
    }];

    for spelling in ["orders:0=1..3", "orders:0=1..=2"] {
        let (_target, target_dir) =
            run_restore(archive.path(), &["--exclude-offset", spelling]).await;

        let log = reopen(&target_dir, "orders", 0);
        check!(log.log_end_offset() == Offset(4), "{spelling}");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == expected, "{spelling}");
    }
}

// ---------------------------------------------------------------------
// Scenario 9: a control (transaction marker) batch must never be excluded.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_producer_id_never_drops_a_control_batch() {
    let mut fixture = vec![
        transactional_batch(77, vec![value_record(0, "tx-a"), value_record(1, "tx-b")]),
        commit_marker(77),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--exclude-producer-id", "77"]).await;

    let log = reopen(&target_dir, "orders", 0);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    // Desired: the producer's ordinary data is excluded (a bare header),
    // but its commit marker survives intact at its own original offset.
    let expected = vec![
        RecordBatch {
            records: Vec::new(),
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    check!(read.batches == expected);
}
