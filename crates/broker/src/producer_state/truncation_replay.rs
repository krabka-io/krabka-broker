use assert2::assert;
use krabka_ids::PartitionIndex;
use krabka_protocol::records::{Record, RecordBatch};

use super::{Decision, ProducerState, RetainedBatch, snapshot_tail::check};

#[tokio::test]
async fn rebuilt_producer_retries_never_name_a_truncated_batch() {
    for cut in 0..=5 {
        let dir = tempfile::tempdir().unwrap();
        let config = krabka_log::LogConfig::default();
        let mut log = krabka_log::Log::open(dir.path(), config.clone()).unwrap();
        for sequence in [0, 2] {
            log.append(&mut RecordBatch {
                producer_id: 42,
                producer_epoch: 7,
                base_sequence: sequence,
                last_offset_delta: 1,
                records: vec![
                    Record::default(),
                    Record {
                        offset_delta: 1,
                        ..Record::default()
                    },
                ],
                ..RecordBatch::default()
            })
            .unwrap();
        }
        log.append(&mut RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        })
        .unwrap();
        log.truncate_to(krabka_log::Offset(cut)).unwrap();
        let end = log.log_end_offset().0;
        let expected_end = match cut {
            0 | 1 => 0,
            2 | 3 => 2,
            4 => 4,
            _ => 5,
        };
        assert!(end == expected_end);
        let state = ProducerState::new();
        state
            .rebuild_from_log("t", PartitionIndex(0), &log)
            .await
            .unwrap();
        for sequence in [0, 2] {
            let checked = check(&state, (42, 7), (sequence, 1)).await;
            if i64::from(sequence) + 2 <= end {
                assert!(
                    checked.decision
                        == Decision::Duplicate {
                            base_offset: i64::from(sequence)
                        }
                );
                assert!(
                    checked.duplicate
                        == Some(RetainedBatch {
                            base_sequence: sequence,
                            last_sequence: sequence + 1,
                            base_offset: i64::from(sequence),
                            last_offset: i64::from(sequence) + 1,
                            timestamp: 0,
                        })
                );
            } else {
                assert!(!matches!(checked.decision, Decision::Duplicate { .. }));
                assert!(checked.duplicate == None);
            }
        }
        drop(log);
        let reopened = krabka_log::Log::open(dir.path(), config).unwrap();
        let state = ProducerState::new();
        state
            .rebuild_from_log("t", PartitionIndex(0), &reopened)
            .await
            .unwrap();
        // A snapshot-only reopen retains just the latest batch metadata.
        // Qualify that actual recovered window without assuming earlier slots.
        let recovered = reopened.recovered_producers();
        for producer in &recovered {
            let row = producer.entry;
            let checked = check(
                &state,
                (row.producer_id.0, row.producer_epoch),
                (
                    krabka_verified::decrement_sequence(row.last_sequence, row.offset_delta),
                    row.offset_delta,
                ),
            )
            .await;
            let witness = checked.duplicate.unwrap();
            assert!(witness.base_offset == row.last_offset.0 - i64::from(row.offset_delta));
            assert!(witness.last_offset < end);
        }
        assert!(recovered.is_empty() == (end == 0));
    }
}

#[tokio::test]
async fn deleting_the_tail_reintroduces_an_older_sequence_alias() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    for (sequence, delta) in [(0, 0), (1, i32::MAX - 1), (0, 0), (1, 0), (2, 0), (3, 0)] {
        log.append(&mut RecordBatch {
            producer_id: 42,
            producer_epoch: 7,
            base_sequence: sequence,
            last_offset_delta: delta,
            records: vec![Record {
                offset_delta: delta,
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .unwrap();
    }
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), &log)
        .await
        .unwrap();
    let checked = check(&state, (42, 7), (0, 0)).await;
    assert!(
        checked.decision
            == Decision::Duplicate {
                base_offset: i64::from(i32::MAX) + 1
            }
    );
    // The middle alias survives, but replay of the shorter log brings back
    // the oldest alias that the previous five-slot window had evicted.
    let cut = log.log_end_offset().0 - 1;
    log.truncate_to(krabka_log::Offset(cut)).unwrap();
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), &log)
        .await
        .unwrap();
    let checked = check(&state, (42, 7), (0, 0)).await;
    assert!(checked.decision == Decision::Duplicate { base_offset: 0 });
    assert!(
        checked.duplicate
            == Some(RetainedBatch {
                base_sequence: 0,
                last_sequence: 0,
                base_offset: 0,
                last_offset: 0,
                timestamp: 0,
            })
    );
}

#[tokio::test]
async fn deleting_a_newer_epoch_restores_the_surviving_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    for epoch in [6, 7] {
        log.append(&mut RecordBatch {
            producer_id: 42,
            producer_epoch: epoch,
            base_sequence: 0,
            last_offset_delta: 1,
            records: vec![
                Record::default(),
                Record {
                    offset_delta: 1,
                    ..Record::default()
                },
            ],
            ..RecordBatch::default()
        })
        .unwrap();
    }
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), &log)
        .await
        .unwrap();
    let checked = check(&state, (42, 6), (0, 1)).await;
    assert!(checked.decision == Decision::Fenced);
    log.truncate_to(krabka_log::Offset(3)).unwrap();
    assert!(log.log_end_offset().0 == 2);
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), &log)
        .await
        .unwrap();
    let checked = check(&state, (42, 6), (0, 1)).await;
    assert!(checked.decision == Decision::Duplicate { base_offset: 0 });
    assert!(
        checked.duplicate
            == Some(RetainedBatch {
                base_sequence: 0,
                last_sequence: 1,
                base_offset: 0,
                last_offset: 1,
                timestamp: 0,
            })
    );
}
