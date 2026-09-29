//! End-to-end compaction over a log that holds transactions: what
//! [`Log::compact`] keeps of a compacted topic that an exactly-once producer
//! wrote to, and what its `.txnindex` says afterwards.

use bytes::Bytes;
use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, RecordBatch};
use krabka_units::prelude::{bytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{abort_marker, commit_marker, compaction_ctx, keyed_batch},
    txn_index::AbortedTxn,
};

/// A transactional batch of one keyed record; `Log::append` assigns the offset.
fn transactional_keyed(pid: i64, sequence: i32, key: &[u8], value: &[u8]) -> RecordBatch {
    let mut batch = keyed_batch(0, &[(0, key, value)]);
    batch.producer_id = pid;
    batch.producer_epoch = 0;
    batch.base_sequence = sequence;
    batch.attributes = Attributes::default().with_transactional(true);
    batch
}

/// A compacted log with one batch per sealed segment, and a final append that
/// stays in the active segment.
fn log_with_a_batch_per_segment(dir: &std::path::Path, batches: Vec<RecordBatch>) -> Log {
    let cfg = LogConfig {
        cleanup_policy: crate::CleanupPolicy::Compact,
        segment_size: bytes(1),
        ..Default::default()
    };
    let mut log = Log::open(dir, cfg).unwrap();
    for mut batch in batches {
        log.append(&mut batch).unwrap();
    }
    let mut tail = keyed_batch(0, &[(0, b"tail", b"t")]);
    log.append(&mut tail).unwrap();
    // The pass groups by `segment.bytes`; widen it so this pass merges them.
    let mut roomier = log.config_snapshot();
    roomier.segment_size = mebibytes(1);
    log.set_config(roomier);
    log
}

fn values_of(log: &Log, key: &[u8]) -> Vec<Bytes> {
    log.read(Offset(0), mebibytes(1))
        .unwrap()
        .batches
        .iter()
        .filter(|batch| !batch.attributes.is_control_batch())
        .flat_map(|batch| batch.records.iter())
        .filter(|record| record.key.as_deref() == Some(key))
        .filter_map(|record| record.value.clone())
        .collect()
}

fn marker_offsets(log: &Log) -> Vec<i64> {
    log.read(Offset(0), mebibytes(1))
        .unwrap()
        .batches
        .iter()
        .filter(|batch| batch.attributes.is_control_batch())
        .map(|batch| batch.base_offset)
        .collect()
}

fn aborted_txns(log: &Log) -> Vec<AbortedTxn> {
    log.aborted_in_range(Offset(0), Offset(10))
}

/// The one aborted transaction the fixtures write: producer 2000's data at
/// offset 2 and its abort marker at offset 3.
fn index_of_the_fixture(log: &Log) -> Vec<AbortedTxn> {
    let index = aborted_txns(log);
    assert2::assert!(index.len() == 1);
    assert2::assert!(index[0].producer_id == ProducerId(2000));
    assert2::assert!(index[0].start_offset == Offset(2));
    assert2::assert!(index[0].last_offset == Offset(3));
    index
}

/// A read-committed consumer of a compacted, transactional topic must keep the
/// last committed value of a key whose newest write was aborted. Key `k` is
/// committed at offset 0 and written again inside a transaction that aborts.
/// Kafka's cleaner drops the aborted record and never lets it shadow the
/// committed one; the abort marker and its `.txnindex` entry stay, because the
/// pass still met the aborted batch.
#[test]
fn a_committed_value_survives_a_newer_aborted_write() {
    let dir = tempdir().unwrap();
    let mut log = log_with_a_batch_per_segment(
        dir.path(),
        vec![
            transactional_keyed(1000, 0, b"k", b"committed"), // 0
            commit_marker(1000, 0),                           // 1
            transactional_keyed(2000, 0, b"k", b"aborted"),   // 2
            abort_marker(2000, 0),                            // 3
            keyed_batch(0, &[(0, b"after", b"a")]),           // 4
        ],
    );
    let index_before = index_of_the_fixture(&log);

    log.compact(&compaction_ctx()).unwrap();

    assert2::assert!(values_of(&log, b"k") == vec![Bytes::from_static(b"committed")]);
    assert2::assert!(marker_offsets(&log) == vec![1, 3]);
    assert2::assert!(aborted_txns(&log) == index_before);
}

/// The pass after the one that dropped the aborted data finds no batch of the
/// transaction left, so it stamps the abort marker and drops the transaction's
/// `.txnindex` entry, and the pass after the delete horizon removes the marker.
#[test]
fn an_abort_marker_and_its_index_entry_age_out_once_the_aborted_data_is_gone() {
    let dir = tempdir().unwrap();
    let mut log = log_with_a_batch_per_segment(
        dir.path(),
        vec![
            transactional_keyed(1000, 0, b"k", b"committed"),
            commit_marker(1000, 0),
            transactional_keyed(2000, 0, b"k", b"aborted"),
            abort_marker(2000, 0),
            keyed_batch(0, &[(0, b"after", b"a")]),
        ],
    );
    let index_before = index_of_the_fixture(&log);
    log.compact(&compaction_ctx()).unwrap();
    assert2::assert!(marker_offsets(&log) == vec![1, 3]);
    assert2::assert!(aborted_txns(&log) == index_before);

    // The second pass reads the first pass's output, which holds no batch of
    // producer 2000's transaction. The marker is stamped and the entry, which
    // describes nothing the log still holds, is dropped.
    log.compact(&compaction_ctx()).unwrap();
    assert2::assert!(marker_offsets(&log) == vec![1, 3]);
    assert2::assert!(aborted_txns(&log).is_empty());

    // Once the delete horizon has passed, the marker goes.
    let delete_retention_ms = LogConfig::default().delete_retention.millis_i64_trunc();
    let horizon_passed = CompactionContext {
        now: std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis(u64::try_from(delete_retention_ms).unwrap() + 1_000),
        ..compaction_ctx()
    };
    log.compact(&horizon_passed).unwrap();
    assert2::assert!(marker_offsets(&log) == vec![1]);
    assert2::assert!(values_of(&log, b"k") == vec![Bytes::from_static(b"committed")]);
}

/// The pass can stop before a transaction's abort marker, when the last stable
/// offset or the min compaction lag ends its range there. The transaction's
/// `.txnindex` entry then sits in a segment the pass does not consume, and the
/// pass must still find it: it drops the aborted batch inside the range and
/// keeps the committed value.
#[test]
fn an_aborted_batch_is_dropped_when_its_marker_lies_beyond_the_consumed_range() {
    let dir = tempdir().unwrap();
    let mut log = log_with_a_batch_per_segment(
        dir.path(),
        vec![
            transactional_keyed(1000, 0, b"k", b"committed"), // 0
            commit_marker(1000, 0),                           // 1
            transactional_keyed(2000, 0, b"k", b"aborted"),   // 2
            keyed_batch(0, &[(0, b"other", b"o")]),           // 3
            abort_marker(2000, 0),                            // 4
        ],
    );
    // The pass consumes the sealed segments below offset 4 only.
    let ctx = CompactionContext {
        last_stable_offset: Offset(4),
        ..compaction_ctx()
    };

    log.compact(&ctx).unwrap();

    assert2::assert!(values_of(&log, b"k") == vec![Bytes::from_static(b"committed")]);
    assert2::assert!(values_of(&log, b"other") == vec![Bytes::from_static(b"o")]);
    assert2::assert!(marker_offsets(&log) == vec![1, 4]);
}
