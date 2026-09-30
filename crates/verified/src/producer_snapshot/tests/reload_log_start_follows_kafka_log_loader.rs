use super::*;

#[test]
fn reload_log_start_follows_kafka_log_loader() {
    for (remote, established, local, expected) in [
        // Remote storage: the checkpoint, below the local segments.
        (true, Some(3), 8, 3),
        // Remote storage and no checkpoint: Kafka reads 0.
        (true, None, 8, 0),
        // Local only: max(checkpoint, first segment base).
        (false, Some(3), 8, 8),
        (false, None, 8, 8),
    ] {
        assert2::check!(producer_snapshot_reload_log_start(remote, established, local) == expected);
    }
}

#[test]
fn reload_keeps_exactly_the_half_open_range_above_the_log_start() {
    for (offset, kept) in [
        (4, false),
        // A snapshot at the log start is deleted.
        (5, false),
        (6, true),
        // A snapshot at the log end is kept.
        (10, true),
        (11, false),
        (-1, false),
    ] {
        assert2::check!(
            producer_snapshot_reload_keeps(offset, RANGE) == kept,
            "{offset}"
        );
    }
}

#[test]
fn stray_snapshots_follow_kafka_remove_stray_snapshots() {
    for (snapshots, bases, removed) in [
        // Every snapshot sits on a segment base.
        (
            &[0_i64, 4, 8][..],
            &[0_i64, 4, 8][..],
            &[false, false, false][..],
        ),
        // A stray below the newest base goes; so does one between bases.
        (&[2, 4, 6], &[0, 4, 8], &[true, false, true]),
        // The newest snapshot above every base survives: a clean-shutdown
        // snapshot at the log end.
        (&[4, 8, 11], &[0, 4, 8], &[false, false, false]),
        // Two strays above every base: only the newer survives.
        (&[9, 11], &[0, 4, 8], &[true, false]),
        // No segments: the newest snapshot survives and older ones go.
        (&[3, 7], &[], &[true, false]),
        // Order is irrelevant.
        (&[11, 2, 4], &[8, 0, 4], &[false, true, false]),
    ] {
        for (index, expected) in removed.iter().enumerate() {
            assert2::check!(
                producer_snapshot_stray(snapshots, index, bases) == *expected,
                "{snapshots:?} {bases:?} {index}"
            );
        }
    }
}

#[test]
fn latest_snapshot_is_the_newest_one_the_reload_keeps() {
    for (offsets, expected) in [
        (&[][..], None),
        // At or below the log start, or above the log end: none loads.
        (&[3, 5, 11, -1][..], None),
        (&[6, 10, 7, 12][..], Some(1)),
        (&[5, 6][..], Some(1)),
        (&[10, 10][..], Some(0)),
    ] {
        assert2::check!(producer_snapshot_latest_index(offsets, RANGE) == expected);
    }
    let top = ProducerReloadRange {
        log_start: i64::MAX - 1,
        local_start: i64::MAX - 1,
        log_end: i64::MAX,
    };
    assert2::check!(producer_snapshot_latest_index(&[i64::MAX], top) == Some(0));
}

/// One row per validity rule over a snapshot at offset 10, each a change
/// to one valid entry: producer 7 at epoch 2, whose last batch is
/// sequences ..=4 at offsets 6..=9 (delta 3), with a transaction open
/// since offset 5 under coordinator epoch 0.
#[test]
fn entry_validation_is_exact_and_snapshot_bounded() {
    const VALID: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
        producer_id: 7,
        producer_epoch: 2,
        last_sequence: 4,
        last_offset: 9,
        offset_delta: 3,
        coordinator_epoch: 0,
        current_txn_first_offset: 5,
    };
    const MARKER_ONLY: ProducerSnapshotEntryFacts = ProducerSnapshotEntryFacts {
        producer_id: 0,
        producer_epoch: 0,
        last_sequence: -1,
        last_offset: -1,
        offset_delta: 0,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    };
    let rows: [(&str, i64, ProducerSnapshotEntryFacts, bool); 13] = [
        ("a real producer before the boundary", 10, VALID, true),
        ("a marker-only producer", 10, MARKER_ONLY, true),
        (
            "the full integer ranges",
            i64::MAX,
            ProducerSnapshotEntryFacts {
                producer_id: i64::MAX,
                producer_epoch: i16::MAX,
                last_sequence: i32::MAX,
                last_offset: i64::MAX - 1,
                offset_delta: i32::MAX,
                coordinator_epoch: i32::MAX,
                current_txn_first_offset: i64::MAX - 2,
            },
            true,
        ),
        ("a negative snapshot offset", -1, MARKER_ONLY, false),
        (
            "a negative producer id",
            10,
            ProducerSnapshotEntryFacts {
                producer_id: -1,
                ..VALID
            },
            false,
        ),
        (
            "a negative producer epoch",
            10,
            ProducerSnapshotEntryFacts {
                producer_epoch: -1,
                ..VALID
            },
            false,
        ),
        (
            "a coordinator epoch below the sentinel",
            10,
            ProducerSnapshotEntryFacts {
                coordinator_epoch: -2,
                ..VALID
            },
            false,
        ),
        (
            "a last record at the boundary",
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 10,
                ..VALID
            },
            false,
        ),
        (
            "a delta reaching below offset zero",
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 0,
                offset_delta: 1,
                current_txn_first_offset: -1,
                ..VALID
            },
            false,
        ),
        (
            "a sentinel with a delta",
            10,
            ProducerSnapshotEntryFacts {
                offset_delta: 1,
                ..MARKER_ONLY
            },
            false,
        ),
        (
            "a transaction opened at the boundary",
            10,
            ProducerSnapshotEntryFacts {
                current_txn_first_offset: 10,
                ..VALID
            },
            false,
        ),
        (
            "a transaction opened after the last record",
            10,
            ProducerSnapshotEntryFacts {
                last_offset: 4,
                offset_delta: 0,
                ..VALID
            },
            false,
        ),
        (
            "a transaction start below the sentinel",
            10,
            ProducerSnapshotEntryFacts {
                current_txn_first_offset: -2,
                ..VALID
            },
            false,
        ),
    ];
    for (name, snapshot_offset, entry, expected) in rows {
        assert2::check!(
            producer_snapshot_entry_valid(snapshot_offset, entry) == expected,
            "{name}"
        );
    }
}
