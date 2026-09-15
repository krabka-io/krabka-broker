//! Unit tests for the topic and partition map plumbing and for the
//! produce-path dedup, commit, truncate, and snapshot behaviour of
//! `ProducerState`.

use assert2::{assert, check};

use super::*;

#[tokio::test]
async fn first_batch_appends() {
    let s = ProducerState::new();
    let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
    assert!(d == Decision::Append);
}

#[tokio::test]
async fn next_sequence_appends() {
    let s = ProducerState::new();
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        0,
        0,
        4,
        /* base_offset */ 0,
        /* ts */ 1,
    )
    .await;
    let d = s.check("t", PartitionIndex(0), 1000, 0, 5, 2).await;
    assert!(d == Decision::Append);
}

#[tokio::test]
async fn duplicate_returns_cached_offset() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
    let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
    assert!(d == Decision::Duplicate { base_offset: 0 });
}

/// A deferred `acks=all` commit for a pre-marker batch must not undo a
/// marker's epoch bump.
///
/// `AppendCommit::record` waits on the high-watermark gate for its own
/// append, so it can resolve long after a later transaction marker mirrored
/// a bumped epoch into the tracker. `commit` must skip a write that would
/// regress the tracked epoch, or the tracker would fence the wrong epoch, or
/// accept a first sequence at the new epoch that is not 0, until the next
/// restart.
#[tokio::test]
async fn a_late_commit_at_an_older_epoch_does_not_undo_a_marker() {
    let s = ProducerState::new();
    // The marker's mirror lands first, well before the older batch's deferred
    // acks=all commit resolves.
    s.mirror_log_entries(
        "t",
        PartitionIndex(0),
        vec![krabka_log::ProducerSnapshotEntry {
            producer_id: krabka_log::ProducerId(1000),
            producer_epoch: 4,
            last_sequence: -1,
            last_offset: krabka_log::Offset(-1),
            offset_delta: 0,
            timestamp: -1,
            coordinator_epoch: 0,
            current_txn_first_offset: None,
        }],
    )
    .await;

    // The stale, pre-marker commit for epoch 3 resolves after the marker.
    commit!(s, "t", PartitionIndex(0), 1000, 3, 0, 2, 10, 1).await;

    assert!(
        s.check("t", PartitionIndex(0), 1000, 4, 0, 0).await == Decision::Append,
        "the marker's epoch must still be the tracked one"
    );
    assert!(
        s.check("t", PartitionIndex(0), 1000, 3, 0, 2).await == Decision::Fenced,
        "the stale commit must not have reopened the pre-marker epoch"
    );
}

#[tokio::test]
async fn only_an_exact_retry_of_the_last_batch_is_duplicate() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 0, 3, 1, 10, 1).await;

    check!(
        s.check("t", PartitionIndex(0), 1000, 0, 3, 1).await
            == Decision::Duplicate { base_offset: 10 }
    );
    check!(s.check("t", PartitionIndex(0), 1000, 0, 3, 0).await == Decision::OutOfOrder);
    check!(s.check("t", PartitionIndex(0), 1000, 0, 2, 1).await == Decision::OutOfOrder);
}

#[tokio::test]
async fn sequence_rollover_appends_and_commits_without_overflow() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 1, 10, 1,).await;

    check!(s.check("t", PartitionIndex(0), 1000, 0, 0, 0).await == Decision::Append);
    check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::OutOfOrder);

    commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 2, 12, 2).await;
    let entry = s.snapshot("t", PartitionIndex(0)).await[0].1;
    check!(entry.last_sequence == 2);
    check!(entry.last_offset == 14);
}

#[tokio::test]
async fn batch_can_cross_sequence_rollover() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 2, 20, 1,).await;

    check!(
        s.check("t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 2)
            .await
            == Decision::Duplicate { base_offset: 20 }
    );
    check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::Append);
}

#[tokio::test]
async fn truncate_drops_dedup_entry_above_offset_so_retry_reappends() {
    // The failover-stall regression: a batch was appended at base_offset
    // 1471686 (last_offset 1471699), then the divergent tail was truncated
    // back to 1471686 on rejoin. A retry must NOT be deduplicated against
    // the now-truncated offset — otherwise the acks=all HW gate
    // (await_hw_at_least(1471700)) waits forever for a high watermark the
    // log can never reach, stalling the producer.
    let s = ProducerState::new();
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        0,
        /*base_seq*/ 0,
        /*delta*/ 13,
        1_471_686,
        1,
    )
    .await;
    assert!(
        s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await
            == Decision::Duplicate {
                base_offset: 1_471_686
            }
    );
    s.truncate("t", PartitionIndex(0), 1_471_686).await;
    assert!(
        s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await == Decision::Append,
        "after truncation the retried batch must re-append, not dedup against the truncated offset"
    );
}

#[tokio::test]
async fn truncate_keeps_dedup_entry_below_offset() {
    // A batch whose records survive the truncation (last_offset < offset)
    // must stay deduplicated.
    let s = ProducerState::new();
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        0,
        0,
        4,
        /*base_offset*/ 100,
        1,
    )
    .await; // last_offset 104
    s.truncate("t", PartitionIndex(0), 200).await;
    assert!(
        s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await
            == Decision::Duplicate { base_offset: 100 }
    );
}

#[tokio::test]
async fn truncate_drops_dedup_entry_at_exact_offset_boundary() {
    // Truncating at an entry's last_offset removes that entry: the last
    // accepted record is no longer below the log end being retained.
    let s = ProducerState::new();
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        0,
        0,
        4,
        /*base_offset*/ 100,
        1,
    )
    .await; // last_offset 104
    s.truncate("t", PartitionIndex(0), 104).await;
    assert!(s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await == Decision::Append);
}

#[tokio::test]
async fn truncate_unknown_partition_is_noop() {
    let s = ProducerState::new();
    s.truncate("never-seen", PartitionIndex(7), 0).await; // must not panic or create state
    assert!(s.snapshot("never-seen", PartitionIndex(7)).await.is_empty());
}

/// A marker-only entry (a transaction-version-2 marker cleared the retained
/// batch, so `last_offset` is the Kafka `-1` sentinel) carries no offset of
/// its own, so it cannot be placed relative to a truncation cut. `truncate`
/// must drop it regardless of `offset`, or a KIP-320 divergent-tail
/// truncation that removes the marker itself would leave the tracker fencing
/// or accepting sequences at an epoch the log no longer has. See
/// `ProducerState::truncate`'s doc comment.
#[tokio::test]
async fn truncate_drops_every_marker_only_entry_regardless_of_offset() {
    for offset in [0, 1, i64::MAX] {
        let s = ProducerState::new();
        s.mirror_log_entries(
            "t",
            PartitionIndex(0),
            vec![krabka_log::ProducerSnapshotEntry {
                producer_id: krabka_log::ProducerId(1000),
                producer_epoch: 4,
                last_sequence: -1,
                last_offset: krabka_log::Offset(-1),
                offset_delta: 0,
                timestamp: -1,
                coordinator_epoch: 0,
                current_txn_first_offset: None,
            }],
        )
        .await;
        s.truncate("t", PartitionIndex(0), offset).await;
        assert!(
            s.snapshot("t", PartitionIndex(0)).await.is_empty(),
            "offset={offset}"
        );
    }
}

#[tokio::test]
async fn out_of_order_when_gap() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
    // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
    let d = s.check("t", PartitionIndex(0), 1000, 0, 10, 2).await;
    assert!(d == Decision::OutOfOrder);
}

#[tokio::test]
async fn lower_epoch_is_fenced() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 5, 0, 4, 0, 1).await;
    let d = s.check("t", PartitionIndex(0), 1000, 4, 5, 2).await;
    assert!(d == Decision::Fenced);
}

/// A bumped producer epoch (same `producer_id`, higher epoch) establishes a
/// FRESH sequence baseline: `base_sequence == 0` at the new epoch must be a
/// fresh `Append`, NOT a `Duplicate` against the prior epoch's high-water.
/// This is the EOS-restart path. The client resets its sequence to 0.
///
/// This is the regression test for the cross-restart EOS data-loss bug.
/// Before the fix, the broker silently deduped a restarted EOS producer's
/// first record on each partition and echoed the old `base_offset`. The
/// txn's offset commit still landed. The source offset advanced, but the
/// output record vanished.
#[tokio::test]
async fn higher_epoch_at_seq_zero_appends() {
    let s = ProducerState::new();
    // Epoch 5 committed sequences 0..=2 (last_sequence = 2).
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        5,
        0,
        2,
        /* base_offset */ 0,
        1,
    )
    .await;
    // Same pid, epoch 6, base_sequence 0 — a fresh write, NOT a duplicate.
    let d = s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await;
    assert!(d == Decision::Append);
}

/// A bumped epoch that continues the sequence (`base_sequence > 0`) is out
/// of order. Kafka's `ProducerAppendInfo.checkSequence` requires sequence 0
/// for the first batch at a new epoch. That includes the KIP-890
/// (transaction version 2) epoch bump on every commit or abort, after which
/// the Java client calls `resetSequenceNumbers()`. The rejected batch changes
/// nothing, so the batch at sequence 0 still appends, and same-epoch dedup
/// resumes after it commits.
#[tokio::test]
async fn higher_epoch_continuing_sequence_is_out_of_order() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(0), 1000, 5, 0, 2, 0, 1).await;
    check!(s.check("t", PartitionIndex(0), 1000, 6, 3, 0).await == Decision::OutOfOrder);
    check!(s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await == Decision::Append);
    commit!(
        s,
        "t",
        PartitionIndex(0),
        1000,
        6,
        0,
        0,
        /* base_offset */ 10,
        2,
    )
    .await;
    let dup = s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await;
    assert!(dup == Decision::Duplicate { base_offset: 10 });
}

#[tokio::test]
async fn snapshot_reports_committed_entries() {
    let s = ProducerState::new();
    commit!(s, "t", PartitionIndex(3), 1000, 0, 0, 4, 7, 1).await;
    let snap = s.snapshot("t", PartitionIndex(3)).await;
    // `last_activity_ms` is wall-clock; copy it from the actual entry so
    // the comparison stays deterministic.
    let expected = vec![(
        1000,
        ProducerEntry {
            epoch: 0,
            last_sequence: 4,
            last_offset: 11,
            base_offset: 7,
            last_timestamp: 1,
            last_activity_ms: snap[0].1.last_activity_ms,
            earlier: crate::producer_state::NO_EARLIER_BATCHES,
        },
    )];
    assert!(snap == expected);
    // Untouched partition / topic report empty without panicking.
    for (topic, partition) in [("t", PartitionIndex(0)), ("other", PartitionIndex(3))] {
        assert!(
            s.snapshot(topic, partition).await == vec![],
            "case: {topic}/{partition}"
        );
    }
}

/// The log keeps one batch per producer, so the mirror of a marker entry
/// keeps the tracker's earlier batches only when the marker leaves the epoch
/// and the last batch as they are (transaction version 1). A marker at a new
/// epoch clears them, as Kafka's `ProducerStateEntry.maybeUpdateProducerEpoch`
/// does.
#[tokio::test]
async fn a_marker_mirror_keeps_the_earlier_batches_only_at_the_same_epoch() {
    let marker_entry =
        |epoch: i16, last_sequence: i32, last_offset: i64| krabka_log::ProducerSnapshotEntry {
            producer_id: krabka_log::ProducerId(1000),
            producer_epoch: epoch,
            last_sequence,
            last_offset: krabka_log::Offset(last_offset),
            offset_delta: 0,
            timestamp: 2,
            coordinator_epoch: 0,
            current_txn_first_offset: None,
        };
    let cases = [
        (
            "transaction version 1 marker",
            marker_entry(0, 1, 1),
            Decision::Duplicate { base_offset: 0 },
        ),
        (
            "transaction version 2 marker",
            marker_entry(1, -1, -1),
            Decision::Fenced,
        ),
    ];
    for (name, entry, want) in cases {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 0, 0, 1).await;
        commit!(s, "t", PartitionIndex(0), 1000, 0, 1, 0, 1, 2).await;
        s.mirror_log_entries("t", PartitionIndex(0), vec![entry])
            .await;
        check!(
            s.check("t", PartitionIndex(0), 1000, 0, 0, 0).await == want,
            "{name}"
        );
    }
}
