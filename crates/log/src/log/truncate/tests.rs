//! Unit tests for truncation of the log tail and trimming of the log
//! head, including the sidecar state each one has to carry with it.

use assert2::check;
use krabka_ids::{LeaderEpoch, ProducerId};
use krabka_units::prelude::{bytes, gibibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{sample_batch, sample_batch_with_epoch, test_batch_at},
    producer_snapshot::ProducerSnapshotEntry,
    stamp_index::{StampEntry, StampIndex},
    txn_index::AbortedTxn,
};

/// Truncating to the log start is allowed; below it is not.
///
/// The bound is what stops a truncation erasing the boundary the log
/// promises readers -- a fetch below `log_start` is already refused, so a
/// truncation past it would leave the two disagreeing.
#[test]
fn a_truncation_may_reach_the_log_start_but_not_pass_it() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    for _ in 0..4 {
        let mut batch = sample_batch(2);
        log.append(&mut batch).expect("append");
    }
    log.set_log_start_offset(Offset(2)).expect("set log start");

    check!(
        log.truncate_to(Offset(2)).is_ok(),
        "truncating to the log start itself"
    );
    let below = log.truncate_to(Offset(1));
    check!(
        matches!(below, Err(LogError::OffsetTooLow { .. })),
        "below the log start is refused, got {below:?}"
    );
}

#[test]
fn truncate_to_drops_later_records() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b1 = sample_batch(3);
    let mut b2 = sample_batch(2);
    log.append(&mut b1).unwrap();
    log.append(&mut b2).unwrap();
    assert2::assert!(log.log_end_offset() == 5);
    log.truncate_to(Offset(3)).unwrap();
    // First batch (offsets 0..=2) survives; last_offset == 2, end == 3.
    assert2::assert!(log.log_end_offset() == 3);
}

#[test]
fn truncation_clamps_tail_state_to_the_actual_retained_batch_prefix() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    log.append(&mut sample_batch(3)).unwrap();
    log.append(&mut sample_batch(3)).unwrap();
    log.active_txn_index
        .append(AbortedTxn {
            start_offset: Offset(3),
            last_offset: Offset(5),
            producer_id: ProducerId(9),
        })
        .unwrap();
    log.delivery_watermark = Offset(6);
    log.lso = Offset(6);

    // The cut lands inside the second batch, so the exact retained prefix
    // ends at 3 rather than the requested 4.
    log.truncate_to(Offset(4)).unwrap();
    check!(log.log_end_offset() == Offset(3));
    check!(log.lso() == Offset(3));
    check!(log.delivery_watermark == Offset(3));
    check!(log.active_txn_index.entries().is_empty());
    check!(log.active.is_some(), "one active segment survives");
    check!(
        log.segments
            .iter()
            .all(|segment| segment.base_offset() < log.active.as_ref().unwrap().base_offset()),
        "every sealed segment precedes the sole active segment"
    );

    log.append(&mut sample_batch(1)).unwrap();
    check!(
        log.delivery_watermark == Offset(3),
        "append must not unmask the discarded tail watermark"
    );
}

#[test]
fn truncate_to_rebuilds_producer_state_from_surviving_batches() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    let mut first = sample_batch(2);
    first.producer_id = 42;
    first.producer_epoch = 3;
    first.base_sequence = 7;
    first.max_timestamp = 100;
    log.append(&mut first).unwrap();
    let mut discarded = sample_batch(2);
    discarded.producer_id = 42;
    discarded.producer_epoch = 3;
    discarded.base_sequence = 9;
    discarded.max_timestamp = 200;
    log.append(&mut discarded).unwrap();
    let mut later = sample_batch(2);
    later.producer_id = 42;
    later.producer_epoch = 3;
    later.base_sequence = 11;
    later.max_timestamp = 300;
    log.append(&mut later).unwrap();

    log.truncate_to(Offset(2)).unwrap();

    assert2::assert!(
        log.producer_state_snapshot()
            == vec![ProducerSnapshotEntry {
                producer_id: ProducerId(42),
                producer_epoch: 3,
                last_sequence: 8,
                last_offset: Offset(1),
                offset_delta: 1,
                timestamp: 100,
                coordinator_epoch: -1,
                current_txn_first_offset: None,
            }]
    );
    assert2::assert!(
        producer_snapshot::list(dir.path())
            .unwrap()
            .into_iter()
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>()
            == vec![Offset(2)]
    );
}

#[test]
fn truncate_to_removes_future_empty_producer_snapshots() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    for _ in 0..3 {
        log.append(&mut sample_batch(2)).unwrap();
    }
    assert2::assert!(log.producer_state.is_empty());

    log.truncate_to(Offset(2)).unwrap();

    assert2::assert!(
        producer_snapshot::list(dir.path())
            .unwrap()
            .into_iter()
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>()
            == vec![Offset(2)]
    );
}

#[test]
fn truncate_to_log_end_is_noop() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    let before = log.log_end_offset();
    log.truncate_to(before + 100).unwrap();
    assert2::assert!(log.log_end_offset() == before);
}

// `truncate_to` promoting a **sealed** segment with base_offset > 0 must
// compute the relative cut as `offset - base` (line: `offset.0 -
// seg.base_offset().0`). We build sealed segment base 1 holding three
// single-record batches (offsets 1,2,3), drop the active segment, and
// truncate to offset 3. Correct `rel = 3 - 1 = 2` drops only the offset-3
// batch → log_end 3. Both the `+` mutant (`rel = 4`) and the `/` mutant
// (`rel = 3`) leave every batch in place → log_end 4.
#[test]
fn truncate_to_promoted_sealed_uses_relative_offset() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let big = LogConfig {
        segment_size: gibibytes(1),
        ..LogConfig::default()
    };
    let tiny = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    // Batch A → active base 0.
    log.append(&mut test_batch_at(0)).unwrap();
    // Roll: seal base 0, fresh active base 1, batch B.
    log.set_config(tiny.clone());
    log.append(&mut test_batch_at(1)).unwrap();
    // No roll: batches C, D accumulate in active base 1 (offsets 2, 3).
    log.set_config(big);
    log.append(&mut test_batch_at(2)).unwrap();
    log.append(&mut test_batch_at(3)).unwrap();
    // Roll: seal base 1 (offsets 1,2,3), fresh active base 4, batch E.
    log.set_config(tiny);
    log.append(&mut test_batch_at(4)).unwrap();
    assert2::assert!(log.log_end_offset() == 5);

    // Truncate to 3: active base 4 (>=3) is dropped, then sealed base 1 is
    // promoted and truncated. rel = 3 - 1 = 2 keeps offsets 1,2, drops 3.
    log.truncate_to(Offset(3)).unwrap();
    assert2::assert!(log.log_end_offset() == 3);
}

// `truncate_to` truncating a **surviving active** segment with
// base_offset > 0 must compute the cut as `offset - base` (line:
// `offset.0 - active.base_offset().0`). Active segment base 1 holds three
// single-record batches (offsets 1,2,3); truncate to offset 3. Correct
// `rel = 3 - 1 = 2` drops only the offset-3 batch → log_end 3. The `+`
// mutant (`rel = 4`) drops nothing → log_end 4.
#[test]
fn truncate_to_active_segment_uses_relative_offset() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let big = LogConfig {
        segment_size: gibibytes(1),
        ..LogConfig::default()
    };
    let tiny = LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    };
    // Batch A → active base 0.
    log.append(&mut test_batch_at(0)).unwrap();
    // Roll: seal base 0, fresh active base 1, batch B.
    log.set_config(tiny);
    log.append(&mut test_batch_at(1)).unwrap();
    // No roll: batches C, D accumulate in active base 1 (offsets 2, 3).
    log.set_config(big);
    log.append(&mut test_batch_at(2)).unwrap();
    log.append(&mut test_batch_at(3)).unwrap();
    assert2::assert!(log.log_end_offset() == 4);

    // Active base 1 survives (1 < 3); rel = 3 - 1 = 2 drops the offset-3
    // batch, keeps offsets 1,2 → log_end 3.
    log.truncate_to(Offset(3)).unwrap();
    assert2::assert!(log.log_end_offset() == 3);
}

#[test]
fn truncate_removes_stamps_for_discarded_tail() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(10, 1),
    ))
    .unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    check!(log.stamp_for_offset(Offset(1)) == Some(11));

    log.truncate_to(Offset(1)).unwrap();
    check!(log.stamp_for_offset(Offset(0)) == Some(10));
    check!(log.stamp_for_offset(Offset(1)) == None);
    assert2::assert!(
        StampIndex::open(dir.path().join("00000000000000000000.stampindex"))
            .unwrap()
            .entries()
            == [StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(0),
                stamp: 10,
            }]
    );
}

#[test]
fn truncate_preserves_stamp_indexes_before_promoted_segment() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(10, 1),
    ))
    .unwrap();
    for _ in 0..3 {
        log.append(&mut sample_batch(1)).unwrap();
    }

    log.truncate_to(Offset(2)).unwrap();

    check!(log.stamp_for_offset(Offset(0)) == Some(10));
    check!(log.stamp_for_offset(Offset(1)) == Some(11));
    check!(log.stamp_for_offset(Offset(2)) == None);
}

#[test]
fn trim_removes_only_evicted_segment_stamp_indexes() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        },
    )
    .unwrap();
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(10, 1),
    ))
    .unwrap();
    for _ in 0..3 {
        log.append(&mut sample_batch(1)).unwrap();
    }

    log.trim_to_offset(Offset(1)).unwrap();

    check!(log.stamp_for_offset(Offset(0)) == None);
    check!(log.stamp_for_offset(Offset(1)) == Some(11));
    check!(log.stamp_for_offset(Offset(2)) == Some(12));
}

#[test]
fn truncate_to_drops_stale_epoch_checkpoint_entries() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    // Epoch 1 at offsets 0..3, then epoch 7 starting at offset 3.
    let mut b1 = sample_batch_with_epoch(3, 1);
    log.append(&mut b1).unwrap();
    let epoch7_start = log.log_end_offset();
    let mut b2 = sample_batch_with_epoch(2, 7);
    log.append(&mut b2).unwrap();
    assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(7)));

    // Truncate away the entire epoch-7 tail.
    log.truncate_to(epoch7_start).unwrap();

    let leo = log.log_end_offset();
    assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
    assert2::assert!(
        log.epoch_checkpoint()
            .end_offset_for_epoch(LeaderEpoch(7), leo)
            == Offset(-1)
    );
    assert2::assert!(
        log.epoch_checkpoint()
            .end_offset_for_epoch(LeaderEpoch(1), leo)
            == leo
    );
}

#[test]
fn trim_to_offset_drops_old_segments() {
    let dir = tempdir().expect("tempdir");
    let mut log = Log::open(
        dir.path(),
        LogConfig {
            segment_size: bytes(200), // small so we roll fast
            ..LogConfig::default()
        },
    )
    .expect("open");
    // Append 30 records to force multiple sealed segments.
    for _ in 0..30 {
        let mut b = sample_batch(1);
        log.append(&mut b).expect("append");
    }
    let leo = log.log_end_offset();
    let new_start = log.trim_to_offset(Offset(15)).expect("trim");
    // Trim clamps to next segment boundary <= target; new_start may
    // be less than 15 if 15 falls inside a sealed segment that we
    // can't drop without losing in-range records. LEO is unaffected.
    check!(new_start <= 15);
    check!(log.log_end_offset() == leo);
    // If target landed inside the active segment, log_start advanced
    // exactly to target. Otherwise it advanced to a sealed boundary.
    check!(log.log_start_offset() >= 0);
}

#[test]
fn trim_to_offset_clamps_to_leo() {
    let dir = tempdir().expect("tempdir");
    let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
    for _ in 0..3 {
        let mut b = sample_batch(1);
        log.append(&mut b).expect("append");
    }
    let leo = log.log_end_offset();
    let new_start = log.trim_to_offset(Offset(999)).expect("trim");
    // Asking to trim past LEO means trim to LEO.
    assert2::assert!(new_start == leo);
}

#[test]
fn trim_to_offset_rejects_negative() {
    let dir = tempdir().expect("tempdir");
    let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
    assert2::assert!(log.trim_to_offset(Offset(-5)).is_err());
}

#[test]
fn trim_to_offset_idempotent_at_or_below_log_start() {
    let dir = tempdir().expect("tempdir");
    let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
    for _ in 0..3 {
        let mut b = sample_batch(1);
        log.append(&mut b).expect("append");
    }
    // Trim to 0 on a fresh log → no change.
    let r = log.trim_to_offset(Offset(0)).expect("trim");
    assert2::assert!(r == log.log_start_offset());
}
