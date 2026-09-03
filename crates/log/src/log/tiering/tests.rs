//! Unit tests for the tiered-storage surface: the sealed-segment export,
//! the leader-epoch ranges it carries, and the local deletion that
//! follows a successful offload.

use assert2::check;
use krabka_units::prelude::{bytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{rolled_log, sample_batch, sample_batch_with_epoch},
};

/// An epoch covers `[start_offset, next.start_offset)`, so one ending
/// exactly where the requested range begins does not overlap it.
///
/// The half-open end is what keeps a fetch from being told the epoch of a
/// record it did not ask for -- the divergence check a follower runs on
/// the answer would then compare against the wrong epoch.
#[test]
fn an_epoch_ending_where_the_range_begins_does_not_overlap_it() {
    use crate::leader_epoch_checkpoint::EpochEntry;

    let sorted = vec![
        EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(0),
        },
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(10),
        },
    ];

    // Epoch 1 covers [0, 10) and epoch 2 covers [10, MAX).
    let from_ten = epochs_for_range(&sorted, Offset(10), Offset(20));
    check!(
        from_ten == vec![(LeaderEpoch(2), Offset(10))],
        "a range starting at 10 is epoch 2 alone, got {from_ten:?}"
    );

    // One offset earlier and both epochs are in range.
    let from_nine = epochs_for_range(&sorted, Offset(9), Offset(20));
    check!(
        from_nine == vec![(LeaderEpoch(1), Offset(9)), (LeaderEpoch(2), Offset(10))],
        "got {from_nine:?}"
    );

    // A range wholly inside the first epoch sees only it.
    let early = epochs_for_range(&sorted, Offset(0), Offset(5));
    check!(early == vec![(LeaderEpoch(1), Offset(0))], "got {early:?}");
}

#[test]
fn tiered_local_delete_removes_only_deleted_segment_stamp_indexes() {
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

    check!(log.delete_local_segments_through(Offset(1)).unwrap() == 1);

    check!(log.stamp_for_offset(Offset(0)) == None);
    check!(log.stamp_for_offset(Offset(1)) == Some(11));
    check!(log.stamp_for_offset(Offset(2)) == Some(12));
}

#[test]
fn tierable_segments_excludes_active_and_reports_paths() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(200), // small so we roll fast
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for _ in 0..10 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    let sealed_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
        .count()
        - 1; // minus the active segment's .log

    let exports = log.tierable_segments();
    assert2::assert!(exports.len() == sealed_count);

    let active_base = log.log_end_offset(); // not literally, but exports must be below it
    let mut prev_last = Offset(-1);
    for ex in &exports {
        check!(ex.log_path.exists(), "log file present: {:?}", ex.log_path);
        check!(ex.offset_index_path.exists());
        check!(ex.time_index_path.exists());
        check!(ex.last_offset >= ex.base_offset);
        check!(ex.base_offset > prev_last, "segments are offset-ordered");
        prev_last = ex.last_offset;
        assert2::assert!(ex.last_offset < active_base);
    }
}

#[test]
fn tierable_segments_empty_for_single_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch(3);
    log.append(&mut b).unwrap();
    // No roll happened: the only segment is active and never tierable.
    assert2::assert!(log.tierable_segments().is_empty());
}

#[test]
fn tierable_segments_last_offset_matches_next_base() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(200),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for _ in 0..8 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    let exports = log.tierable_segments();
    // Each sealed segment's last_offset is exactly one below the next
    // segment's base — contiguous coverage with no gaps.
    for pair in exports.windows(2) {
        assert2::assert!(pair[0].last_offset + 1 == pair[1].base_offset);
    }
}

#[test]
fn tierable_segments_carry_leader_epochs() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        segment_size: bytes(200),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    // epoch 0 for the first few, then epoch 1.
    for _ in 0..4 {
        let mut b = sample_batch_with_epoch(2, 0);
        log.append(&mut b).unwrap();
    }
    for _ in 0..4 {
        let mut b = sample_batch_with_epoch(2, 1);
        log.append(&mut b).unwrap();
    }
    let exports = log.tierable_segments();
    assert2::assert!(!exports.is_empty());
    // Every export carries at least one epoch, and each recorded start
    // offset is clamped to >= the segment base.
    for ex in &exports {
        assert2::assert!(!ex.leader_epochs.is_empty());
        for (_epoch, start) in &ex.leader_epochs {
            assert2::assert!(*start >= ex.base_offset);
            assert2::assert!(*start <= ex.last_offset);
        }
    }
}

#[test]
fn epochs_for_range_clamps_and_filters() {
    use crate::leader_epoch_checkpoint::EpochEntry;
    let entries = vec![
        EpochEntry {
            epoch: LeaderEpoch(0),
            start_offset: Offset(0),
        },
        EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(50),
        },
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(100),
        },
    ];
    for (name, start, end, want) in [
        // Segment [60, 90] sits entirely in epoch 1.
        (
            "within one epoch",
            Offset(60),
            Offset(90),
            vec![(LeaderEpoch(1), Offset(60))],
        ),
        // Segment [40, 60] straddles epoch 0 (->clamped to 40) and epoch 1.
        (
            "straddles epochs",
            Offset(40),
            Offset(60),
            vec![(LeaderEpoch(0), Offset(40)), (LeaderEpoch(1), Offset(50))],
        ),
        // Segment [0, 200] covers all three.
        (
            "covers all epochs",
            Offset(0),
            Offset(200),
            vec![
                (LeaderEpoch(0), Offset(0)),
                (LeaderEpoch(1), Offset(50)),
                (LeaderEpoch(2), Offset(100)),
            ],
        ),
    ] {
        check!(
            epochs_for_range(&entries, start, end) == want,
            "case {name}: range [{}, {}]",
            start.0,
            end.0
        );
    }
    // No entries -> empty.
    assert2::assert!(epochs_for_range(&[], Offset(0), Offset(100)).is_empty());
}

#[test]
fn local_log_start_offset_matches_log_start_offset() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    for _ in 0..3 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    assert2::assert!(log.local_log_start_offset() == log.log_start_offset());
}

#[test]
fn delete_local_segments_through_drops_sealed_below_target() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    assert2::assert!(exports.len() >= 3);

    // Pick a target strictly between two sealed-segment boundaries:
    // one past the second sealed segment's last_offset. Every sealed
    // segment whose last_offset < target should be deleted.
    let target = exports[1].last_offset + 1;
    let expected_deleted: Vec<Offset> = exports
        .iter()
        .filter(|e| e.last_offset < target)
        .map(|e| e.base_offset)
        .collect();
    let active_base_before = log.log_end_offset();

    let removed = log.delete_local_segments_through(target).unwrap();
    assert2::assert!(removed == expected_deleted.len());

    // (a) sealed segments below target are gone from the in-memory list.
    let remaining_bases: Vec<Offset> = log
        .tierable_segments()
        .iter()
        .map(|e| e.base_offset)
        .collect();
    for base in &expected_deleted {
        assert2::assert!(!remaining_bases.contains(base));
    }

    // (b) on-disk files for deleted segments are gone.
    for base in &expected_deleted {
        check!(!name::log_path(dir.path(), base.0).exists());
        check!(!name::index_path(dir.path(), base.0).exists());
        check!(!name::timeindex_path(dir.path(), base.0).exists());
    }

    // (c) the active segment is untouched.
    assert2::assert!(log.log_end_offset() == active_base_before);
}

#[test]
fn delete_local_segments_through_keeps_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let leo_before = log.log_end_offset();
    let active_log = dir.path().join(format!(
        "{:020}.log",
        log.tierable_segments().last().unwrap().last_offset + 1
    ));
    // The active segment's .log file should exist before and after.
    assert2::assert!(active_log.exists());

    // First: target far beyond every sealed segment but well past
    // active.base_offset. The active segment must not be removed.
    let huge_target = leo_before + 1_000_000;
    let _ = log.delete_local_segments_through(huge_target).unwrap();
    check!(active_log.exists(), "active segment must survive");
    check!(
        log.log_end_offset() == leo_before,
        "active segment untouched (LEO unchanged)"
    );
    // Sealed-segment pointer should have advanced past everything.
    check!(log.tierable_segments().is_empty());
}

/// KIP-405 gives a tiered partition two floors, and a local eviction moves
/// only one of them: the records are still in the remote tier, so the global
/// `log_start_offset` a fetch is measured against must stay where it was.
/// When the two moved together, an offset whose copy the archive still held
/// answered `OFFSET_OUT_OF_RANGE` from the local floor and `ListOffsets`
/// reported an earliest offset that skipped everything tiered.
#[test]
fn delete_local_segments_through_moves_only_the_local_start_pointer() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    let start_before = log.log_start_offset();
    let target = exports[1].last_offset + 1;

    log.delete_local_segments_through(target).unwrap();

    assert2::assert!(log.local_log_start_offset() == target);
    assert2::assert!(log.log_start_offset() == start_before);
}

/// The band between the two floors is the remote tier's to serve, so a local
/// read of it fails rather than answering with the first batch a surviving
/// segment happens to begin with. Below the global floor there is nothing
/// anywhere, and the error says so.
#[test]
fn a_read_between_the_two_floors_is_the_remote_tier_s_to_serve() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    let target = exports[1].last_offset + 1;
    log.delete_local_segments_through(target).unwrap();
    log.set_log_start_offset(exports[0].base_offset + 1)
        .unwrap();
    let global = log.log_start_offset();

    check!(
        matches!(
            log.read(global - 1, mebibytes(1)),
            Err(LogError::OffsetTooLow { .. })
        ),
        "below the global floor no tier answers"
    );
    for offset in [global, target - 1] {
        check!(
            matches!(
                log.read(offset, mebibytes(1)),
                Err(LogError::OffsetBelowLocalStart { .. })
            ),
            "offset {offset} is tiered"
        );
        check!(
            matches!(
                log.read_raw(offset, log.log_end_offset(), mebibytes(1)),
                Err(LogError::OffsetBelowLocalStart { .. })
            ),
            "offset {offset} is tiered, raw read"
        );
    }
    check!(
        log.read(target, mebibytes(1)).is_ok(),
        "the local floor itself still reads"
    );
}

#[test]
fn delete_local_segments_through_is_noop_at_or_below_current_start() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let start_before = log.log_start_offset();
    let sealed_before = log.tierable_segments().len();

    let removed = log.delete_local_segments_through(start_before).unwrap();
    assert2::assert!(removed == 0);
    let removed_below = log
        .delete_local_segments_through((start_before - 1).max(Offset(0)))
        .unwrap();
    assert2::assert!(removed_below == 0);
    assert2::assert!(log.log_start_offset() == start_before);
    assert2::assert!(log.tierable_segments().len() == sealed_before);
}

#[test]
fn delete_local_segments_through_rejects_negative_target() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let err = log.delete_local_segments_through(Offset(-1)).unwrap_err();
    assert2::assert!(matches!(err, LogError::InvalidArgument(_)));
}
