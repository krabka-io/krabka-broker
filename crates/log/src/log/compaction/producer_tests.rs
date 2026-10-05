//! What a compaction pass keeps for the producers of the log.
//!
//! Kafka's `Cleaner.cleanSegments` reads the last record of each active
//! producer from the producer state of the log
//! (`UnifiedLog.lastRecordsOfActiveProducers`), and `Cleaner.cleanInto` keeps
//! that record, as an empty batch when compaction removes all of its records.
//! Every append path updates the producer state, so a leader, a follower and a
//! follower that appends verbatim keep the same batches.

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{Time, bytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    CleanupPolicy,
    config::LogConfig,
    log::test_support::{commit_marker, compaction_ctx, verbatim_from},
};

/// The path a batch takes into the log.
#[derive(Debug, Clone, Copy)]
enum Path {
    /// `Log::append`, as a leader appends a client batch.
    Leader,
    /// `Log::append_at`, as a follower appends a decoded replicated batch.
    Follower,
    /// `Log::append_verbatim_at` for a data batch and `Log::append_at` for a
    /// control batch, as a follower appends a passthrough fetch.
    Verbatim,
}

/// One batch of the log after compaction. The compared fields are the ones
/// that carry the state of a producer, and the records.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Kept {
    base_offset: i64,
    last_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    control: bool,
    records: Vec<(Option<Bytes>, Option<Bytes>)>,
}

impl Kept {
    fn of(batch: &RecordBatch) -> Self {
        Self {
            base_offset: batch.base_offset,
            last_offset: batch.base_offset + i64::from(batch.last_offset_delta),
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            control: batch.attributes.is_control_batch(),
            records: batch
                .records
                .iter()
                .map(|record| (record.key.clone(), record.value.clone()))
                .collect(),
        }
    }

    /// The one-record `batch` at `offset` with its record.
    fn whole(offset: i64, batch: &RecordBatch) -> Self {
        Self {
            base_offset: offset,
            last_offset: offset,
            ..Self::of(batch)
        }
    }

    /// The one-record `batch` at `offset` with no records: the bare header
    /// that Kafka's `RETAIN_EMPTY` writes.
    fn header(offset: i64, batch: &RecordBatch) -> Self {
        Self {
            records: Vec::new(),
            ..Self::whole(offset, batch)
        }
    }
}

/// A one-record data batch of `key` and `value`. `producer` is `(id, epoch,
/// base_sequence)`, or `None` for a client with no idempotence.
fn record(
    producer: Option<(i64, i16, i32)>,
    transactional: bool,
    (key, value): (&str, &str),
) -> RecordBatch {
    let (producer_id, producer_epoch, base_sequence) = producer.unwrap_or((-1, -1, -1));
    RecordBatch {
        attributes: Attributes::default().with_transactional(transactional),
        producer_id,
        producer_epoch,
        base_sequence,
        records: vec![Record {
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

fn append(log: &mut Log, path: Path, mut batch: RecordBatch) {
    let log_end = log.log_end_offset();
    match path {
        Path::Leader => {
            log.append(&mut batch).unwrap();
        }
        Path::Verbatim if !batch.attributes.is_control_batch() => {
            batch.base_offset = log_end.0;
            let (_wire, verbatim) = verbatim_from(&batch, LeaderEpoch(0));
            log.append_verbatim_at(&verbatim, log_end).unwrap();
        }
        Path::Follower | Path::Verbatim => log.append_at(&mut batch, log_end).unwrap(),
    }
}

/// Kafka's cleaner keeps the last record of each producer in the producer
/// state of the log. That record is the last data batch of the producer at
/// its epoch, or a marker at its epoch when a marker at a new epoch has
/// cleared its batches. Kafka keeps no batch of a producer that
/// `removeExpiredProducers` removed. Each case ends with a batch that stays in
/// the active segment, which no pass rewrites, and the three passes take a
/// commit marker past its delete horizon.
#[test]
fn compaction_keeps_the_last_record_of_each_producer_in_the_log() {
    let idempotent = |epoch, sequence| Some((7, epoch, sequence));
    let superseded = vec![
        record(idempotent(0, 0), false, ("a", "p-a")),
        record(idempotent(0, 1), false, ("b", "p-b")),
        record(None, false, ("a", "a-2")),
        record(None, false, ("b", "b-3")),
        record(None, false, ("c", "c-4")),
    ];
    let new_epoch = vec![
        record(idempotent(0, 0), false, ("a", "p-a")),
        record(idempotent(1, 0), false, ("b", "p-b")),
        record(None, false, ("a", "a-2")),
        record(None, false, ("b", "b-3")),
        record(None, false, ("c", "c-4")),
    ];
    // Transaction version 1 commits at the epoch of the transaction, and
    // transaction version 2 at the next epoch.
    let same_epoch_commit = vec![
        record(Some((9, 2, 0)), true, ("a", "t-a")),
        commit_marker(9, 2),
        record(None, false, ("a", "a-2")),
        record(None, false, ("c", "c-3")),
    ];
    let next_epoch_commit = vec![
        record(Some((10, 2, 0)), true, ("a", "t-a")),
        commit_marker(10, 3),
        record(None, false, ("a", "a-2")),
        record(None, false, ("c", "c-3")),
    ];
    // (case, batches, whether the producers expire first, the kept batches)
    let cases: Vec<(&str, &Vec<RecordBatch>, bool, Vec<Kept>)> = vec![
        (
            "the last batch of an idempotent producer",
            &superseded,
            false,
            vec![
                Kept::header(1, &superseded[1]),
                Kept::whole(2, &superseded[2]),
                Kept::whole(3, &superseded[3]),
                Kept::whole(4, &superseded[4]),
            ],
        ),
        (
            "an expired idempotent producer",
            &superseded,
            true,
            vec![
                Kept::whole(2, &superseded[2]),
                Kept::whole(3, &superseded[3]),
                Kept::whole(4, &superseded[4]),
            ],
        ),
        (
            "the last batch at the new epoch of a producer",
            &new_epoch,
            false,
            vec![
                Kept::header(1, &new_epoch[1]),
                Kept::whole(2, &new_epoch[2]),
                Kept::whole(3, &new_epoch[3]),
                Kept::whole(4, &new_epoch[4]),
            ],
        ),
        // The data batch is the last record of the producer, so every pass
        // meets a batch of the transaction and the marker stays.
        (
            "a commit marker at the epoch of the transaction",
            &same_epoch_commit,
            false,
            vec![
                Kept::header(0, &same_epoch_commit[0]),
                Kept::whole(1, &same_epoch_commit[1]),
                Kept::whole(2, &same_epoch_commit[2]),
                Kept::whole(3, &same_epoch_commit[3]),
            ],
        ),
        // The marker is the last record of the producer: it holds the epoch
        // that fences the producer.
        (
            "a commit marker at the next epoch",
            &next_epoch_commit,
            false,
            vec![
                Kept::header(1, &next_epoch_commit[1]),
                Kept::whole(2, &next_epoch_commit[2]),
                Kept::whole(3, &next_epoch_commit[3]),
            ],
        ),
        (
            "a commit marker of an expired producer",
            &next_epoch_commit,
            true,
            vec![
                Kept::whole(2, &next_epoch_commit[2]),
                Kept::whole(3, &next_epoch_commit[3]),
            ],
        ),
    ];
    for (name, batches, expired, want) in cases {
        for path in [Path::Leader, Path::Follower, Path::Verbatim] {
            let dir = tempdir().unwrap();
            // One batch for each segment, and no delete retention, so that the
            // third pass finds the delete horizon of a marker passed.
            let mut log = Log::open(
                dir.path(),
                LogConfig {
                    cleanup_policy: CleanupPolicy::Compact,
                    segment_size: bytes(1),
                    delete_retention: Time::ZERO,
                    ..LogConfig::default()
                },
            )
            .unwrap();
            for batch in batches.iter().cloned() {
                append(&mut log, path, batch);
            }
            // The pass groups by `segment.bytes`; widen it so that a pass
            // merges the sealed segments.
            let mut roomier = log.config_snapshot();
            roomier.segment_size = mebibytes(1);
            log.set_config(roomier);
            if expired {
                log.remove_expired_producers(i64::MAX, 1);
            }

            for _ in 0..3 {
                log.compact(&compaction_ctx()).unwrap();
            }

            let kept: Vec<Kept> = log
                .read(Offset(0), mebibytes(1))
                .unwrap()
                .batches
                .iter()
                .map(Kept::of)
                .collect();
            assert2::check!(kept == want, "{name}, {path:?}");
        }
    }
}
