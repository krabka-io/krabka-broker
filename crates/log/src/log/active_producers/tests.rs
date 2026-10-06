use assert2::assert;
use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use super::*;
use crate::{
    TransactionalBatch,
    config::LogConfig,
    log::test_support::{control_key, control_value, test_log, verbatim_from},
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

/// A data batch of `producer` (`(id, epoch)`) with `records` records from
/// `base_sequence`, whose max timestamp is `max_timestamp`.
fn data(
    (producer_id, producer_epoch): (i64, i16),
    base_sequence: i32,
    records: i32,
    max_timestamp: i64,
    transactional: bool,
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(transactional),
        last_offset_delta: records - 1,
        base_timestamp: max_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records: (0..records)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(Bytes::from_static(b"v")),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

/// A commit (`commit = true`) or abort marker of `producer` that
/// `coordinator_epoch` wrote at `timestamp`.
fn marker(
    (producer_id, producer_epoch): (i64, i16),
    commit: bool,
    coordinator_epoch: i32,
    timestamp: i64,
) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        producer_id,
        producer_epoch,
        records: vec![Record {
            key: Some(control_key(i16::from(commit))),
            value: Some(control_value(coordinator_epoch)),
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

fn active(
    producer_id: i64,
    producer_epoch: i16,
    last_sequence: i32,
    last_timestamp: i64,
    coordinator_epoch: i32,
    current_txn_start_offset: Option<i64>,
) -> ActiveProducer {
    ActiveProducer {
        producer_id: ProducerId(producer_id),
        producer_epoch,
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_txn_start_offset: current_txn_start_offset.map(Offset),
    }
}

/// Kafka's `UnifiedLog.activeProducers` reads the `ProducerStateEntry` of
/// each producer: the last sequence of its last batch at its epoch, the max
/// timestamp of that batch or of the marker after it, the coordinator epoch
/// of its last marker and the first offset of its open transaction. A leader
/// append, a follower append and a verbatim follower append give the same
/// answer, and so do a reopen that replays the log and a reopen that loads
/// the producer-state snapshot.
#[test]
fn active_producers_report_the_producer_state_of_every_append_path() {
    let cases: Vec<(&str, Vec<RecordBatch>, Vec<ActiveProducer>)> = vec![
        (
            "a batch without a producer id",
            vec![data((-1, -1), -1, 2, 1_000, false)],
            vec![],
        ),
        (
            "idempotent batches",
            vec![
                data((7, 2), 0, 3, 1_000, false),
                data((7, 2), 3, 2, 2_000, false),
            ],
            vec![active(7, 2, 4, 2_000, -1, None)],
        ),
        (
            "an open transaction",
            vec![
                data((8, 0), 0, 2, 1_000, true),
                data((8, 0), 2, 1, 1_500, true),
            ],
            vec![active(8, 0, 2, 1_500, -1, Some(0))],
        ),
        (
            "a commit at the same epoch (transaction version 1)",
            vec![
                data((9, 1), 0, 2, 1_000, true),
                marker((9, 1), true, 5, 3_000),
            ],
            vec![active(9, 1, 1, 3_000, 5, None)],
        ),
        (
            "an abort at a bumped epoch (transaction version 2)",
            vec![
                data((10, 1), 0, 2, 1_000, true),
                marker((10, 2), false, 6, 4_000),
            ],
            vec![active(10, 2, -1, 4_000, 6, None)],
        ),
        (
            "a marker without a data batch",
            vec![marker((11, 3), true, 7, 5_000)],
            vec![active(11, 3, -1, 5_000, 7, None)],
        ),
        (
            "a transaction after a commit",
            vec![
                data((12, 0), 0, 1, 1_000, true),
                marker((12, 0), true, 2, 2_000),
                data((12, 0), 1, 1, 3_000, true),
            ],
            vec![active(12, 0, 1, 3_000, 2, Some(2))],
        ),
        (
            "several producers, in producer id order",
            vec![
                data((30, 0), 0, 1, 1_000, false),
                data((20, 0), 0, 1, 2_000, false),
                data((25, 4), 0, 1, 3_000, true),
            ],
            vec![
                active(20, 0, 0, 2_000, -1, None),
                active(25, 4, 0, 3_000, -1, Some(2)),
                active(30, 0, 0, 1_000, -1, None),
            ],
        ),
    ];
    for (name, batches, want) in cases {
        for path in [Path::Leader, Path::Follower, Path::Verbatim] {
            let (dir, mut log) = test_log();
            for batch in batches.clone() {
                append(&mut log, path, batch);
            }
            assert!(log.active_producers() == want, "{name}, {path:?}");

            drop(log);
            let replayed = Log::open(dir.path(), LogConfig::default()).unwrap();
            assert!(
                replayed.active_producers() == want,
                "{name}, {path:?}, replayed"
            );

            replayed.close();
            let loaded = Log::open(dir.path(), LogConfig::default()).unwrap();
            assert!(
                loaded.active_producers() == want,
                "{name}, {path:?}, from the snapshot"
            );
        }
    }
}

/// Kafka's `UnifiedLog.lastRecordsOfActiveProducers`, which the cleaner
/// reads: the last offset of the last data batch of each producer, or none
/// when a marker at a new epoch has cleared its batches, and the epoch of the
/// producer. A leader append, a follower append and a verbatim follower append
/// give the same answer, and so do the two kinds of reopen.
#[test]
fn last_records_of_active_producers_follow_every_append_path() {
    let last = |producer_id, last_data_offset: Option<i64>, producer_epoch| {
        (
            ProducerId(producer_id),
            ProducerLastRecord {
                last_data_offset: last_data_offset.map(Offset),
                producer_epoch,
            },
        )
    };
    let cases: Vec<(
        &str,
        Vec<RecordBatch>,
        HashMap<ProducerId, ProducerLastRecord>,
    )> = vec![
        (
            "a batch without a producer id",
            vec![data((-1, -1), -1, 2, 1_000, false)],
            HashMap::new(),
        ),
        (
            "idempotent batches",
            vec![
                data((7, 2), 0, 3, 1_000, false),
                data((7, 2), 3, 2, 2_000, false),
            ],
            HashMap::from([last(7, Some(4), 2)]),
        ),
        (
            "a commit at the same epoch (transaction version 1)",
            vec![
                data((9, 1), 0, 2, 1_000, true),
                marker((9, 1), true, 5, 3_000),
            ],
            HashMap::from([last(9, Some(1), 1)]),
        ),
        (
            "an abort at a bumped epoch (transaction version 2)",
            vec![
                data((10, 1), 0, 2, 1_000, true),
                marker((10, 2), false, 6, 4_000),
            ],
            HashMap::from([last(10, None, 2)]),
        ),
        (
            "a marker without a data batch",
            vec![marker((11, 3), true, 7, 5_000)],
            HashMap::from([last(11, None, 3)]),
        ),
        (
            "several producers",
            vec![
                data((30, 0), 0, 1, 1_000, false),
                data((20, 0), 0, 1, 2_000, false),
                data((25, 4), 0, 1, 3_000, true),
            ],
            HashMap::from([
                last(20, Some(1), 0),
                last(25, Some(2), 4),
                last(30, Some(0), 0),
            ]),
        ),
    ];
    for (name, batches, want) in cases {
        for path in [Path::Leader, Path::Follower, Path::Verbatim] {
            let (dir, mut log) = test_log();
            for batch in batches.clone() {
                append(&mut log, path, batch);
            }
            assert!(
                log.last_records_of_active_producers() == want,
                "{name}, {path:?}"
            );

            drop(log);
            let replayed = Log::open(dir.path(), LogConfig::default()).unwrap();
            assert!(
                replayed.last_records_of_active_producers() == want,
                "{name}, {path:?}, replayed"
            );

            replayed.close();
            let loaded = Log::open(dir.path(), LogConfig::default()).unwrap();
            assert!(
                loaded.last_records_of_active_producers() == want,
                "{name}, {path:?}, from the snapshot"
            );
        }
    }
}

/// Kafka's `ProducerStateManager.isProducerExpired`: a producer expires when
/// it has no open transaction and `producer.id.expiration.ms` or more has
/// passed since its last timestamp. A producer with an open transaction never
/// expires.
#[test]
fn remove_expired_producers_keeps_open_transactions_and_recent_producers() {
    const EXPIRATION_MS: i64 = 1_000;
    // (case, now_ms, the producers that stay)
    let cases = [
        ("before any producer expires", 1_999, vec![1, 2, 3]),
        (
            "the idle producer at the exact expiration",
            2_000,
            vec![2, 3],
        ),
        ("both idle producers", 4_000, vec![3]),
        ("long after every write", i64::MAX, vec![3]),
    ];
    for (name, now_ms, staying) in cases {
        let (_dir, mut log) = test_log();
        log.append(&mut data((1, 0), 0, 1, 1_000, false)).unwrap();
        log.append(&mut data((2, 0), 0, 1, 1_500, true)).unwrap();
        log.append(&mut marker((2, 0), true, 4, 3_000)).unwrap();
        log.append(&mut data((3, 0), 0, 1, 1_000, true)).unwrap();
        let before = log.active_producers();

        log.remove_expired_producers(now_ms, EXPIRATION_MS);

        let want: Vec<ActiveProducer> = before
            .into_iter()
            .filter(|producer| staying.contains(&producer.producer_id.get()))
            .collect();
        assert!(log.active_producers() == want, "{name}");
        // An expired producer leaves no state behind: its coordinator epoch
        // does not fence a later marker.
        for producer_id in [1, 2, 3] {
            let kept = staying.contains(&producer_id);
            let (_, coordinator_epoch, _) = log.transaction_marker_state(ProducerId(producer_id));
            assert!(
                (coordinator_epoch == 4) == (kept && producer_id == 2),
                "{name}, producer {producer_id}"
            );
        }
    }
}

/// An expired producer starts again from no state: its retained batches are
/// gone too, so the next batch at the same epoch is its only batch.
#[test]
fn an_expired_producer_starts_again_without_its_retained_batches() {
    let (_dir, mut log) = test_log();
    log.append(&mut data((5, 0), 0, 1, 1_000, false)).unwrap();
    log.append(&mut data((5, 0), 1, 1, 1_000, false)).unwrap();

    log.remove_expired_producers(10_000, 1_000);
    log.append(&mut data((5, 0), 2, 1, 20_000, false)).unwrap();

    let recovered = log.recovered_producers();
    assert!(recovered.len() == 1);
    assert!(recovered[0].earlier.is_empty());
    assert!(log.active_producers() == vec![active(5, 0, 2, 20_000, -1, None)]);
}

/// Kafka's `removeExpiredProducers` also drops verification state that is
/// `producer.id.expiration.ms` or more old, so an append must start a new
/// verification.
#[test]
fn remove_expired_producers_drops_old_verification_state() {
    let batch = TransactionalBatch {
        producer_id: ProducerId(6),
        producer_epoch: 0,
        base_sequence: 0,
        is_transactional: true,
        is_control: false,
    };
    // (case, now_ms, whether the guard still verifies the append)
    let cases = [
        ("a fresh verification", 1_999, true),
        ("a verification at the expiration", 2_000, false),
    ];
    for (name, now_ms, verifies) in cases {
        let (_dir, mut log) = test_log();
        let guard = log
            .maybe_start_transaction_verification(batch, false, (1_000, 86_400_000))
            .unwrap();

        log.remove_expired_producers(now_ms, 1_000);

        assert!(
            log.check_transactional_append(batch, guard).is_ok() == verifies,
            "{name}"
        );
    }
}
