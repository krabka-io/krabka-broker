//! Producer-snapshot selection, validation, replay, and deletion decisions.
//!
//! The rules are Kafka's, from three places:
//!
//! - `LogLoader.load` picks the log start the reload runs against, and first
//!   calls `ProducerStateManager.removeStraySnapshots` with the base offset
//!   of every local segment.
//! - `ProducerStateManager.truncateAndReload(logStartOffset, logEndOffset, _)`
//!   deletes every snapshot outside `(logStartOffset, logEndOffset]`, then
//!   `loadFromSnapshot` loads the newest snapshot that is left.
//! - `UnifiedLog.rebuildProducerState` replays each local segment from
//!   `max(segment.baseOffset, mapEndOffset, logStartOffset)`.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// The offsets one producer-state reload runs against.
///
/// These are three `i64` values with three different meanings, so they
/// travel as one struct and a transposed call site does not compile.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerReloadRange {
    /// The `logStartOffset` Kafka hands `truncateAndReload`, as
    /// [`producer_snapshot_reload_log_start`] picks it.
    pub log_start: i64,
    /// The first offset a local segment can serve: the greater of the
    /// oldest local segment's base and the log start.
    pub local_start: i64,
    /// The log end offset the state is rebuilt up to.
    pub log_end: i64,
}

/// Kafka's `truncateAndReload` keeps a snapshot exactly when
/// `logStartOffset < offset <= logEndOffset`. A snapshot at the log start
/// describes no record that is still in the log, so Kafka deletes it too.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn kafka_reload_keeps(offset: Int, log_start: Int, log_end: Int) -> bool {
    pearlite! { log_start < offset && offset <= log_end }
}

/// Some local segment starts at `offset`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn is_segment_base(bases: Seq<i64>, offset: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < bases.len() && bases[j]@ == offset }
}

/// Kafka's `removeStraySnapshots(segmentBaseOffsets)` deletes a snapshot
/// exactly when no segment starts at its offset, unless it is the newest
/// snapshot and lies above every segment base. That one survivor is the
/// snapshot a clean shutdown writes at the log end.
///
/// The method walks the snapshots in offset order, deleting each stray one
/// as soon as a later stray one turns up, then deletes the last stray one if
/// it lies below the greatest segment base. What is left is the rule above.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn kafka_stray_removed(snapshots: Seq<i64>, index: Int, bases: Seq<i64>) -> bool {
    pearlite! {
        !is_segment_base(bases, snapshots[index]@)
            && !((forall<j: Int> 0 <= j && j < snapshots.len()
                    ==> snapshots[j]@ <= snapshots[index]@)
                && (forall<j: Int> 0 <= j && j < bases.len()
                    ==> bases[j]@ < snapshots[index]@))
    }
}

/// Kafka's replay cursor: `max(segment.baseOffset, mapEndOffset,
/// logStartOffset)` for the first local segment the replay reads, where
/// `mapEndOffset` is the loaded snapshot's offset, or the log start when no
/// snapshot loads. A loaded snapshot always lies above the log start.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn kafka_replay_start(range: ProducerReloadRange, snapshot: Option<i64>) -> Int {
    pearlite! {
        match snapshot {
            Some(offset) => if offset@ >= range.local_start@ { offset@ } else { range.local_start@ },
            None => if range.log_start@ >= range.local_start@ {
                range.log_start@
            } else {
                range.local_start@
            },
        }
    }
}

/// Pick the log start a producer-state reload runs against, as Kafka's
/// `LogLoader.load` does.
///
/// With remote storage enabled (KIP-405), it is the checkpointed log start,
/// which may sit below every local segment. Kafka reads a missing checkpoint
/// as 0, so `established` is `None` answers 0. Without remote storage, it is
/// the greater of the checkpoint and the oldest local segment's base, which
/// is `local_log_start`.
#[ensures(result == if remote_storage_enable {
    match established {
        Some(start) => start,
        None => 0i64,
    }
} else {
    local_log_start
})]
#[must_use]
pub const fn producer_snapshot_reload_log_start(
    remote_storage_enable: bool,
    established: Option<i64>,
    local_log_start: i64,
) -> i64 {
    if remote_storage_enable {
        match established {
            Some(start) => start,
            None => 0,
        }
    } else {
        local_log_start
    }
}

/// Keep a snapshot through a reload exactly when Kafka's `truncateAndReload`
/// keeps it: when it lies in `(log_start, log_end]`.
#[ensures(result == kafka_reload_keeps(offset@, range.log_start@, range.log_end@))]
#[must_use]
pub const fn producer_snapshot_reload_keeps(offset: i64, range: ProducerReloadRange) -> bool {
    range.log_start < offset && offset <= range.log_end
}

/// Delete the snapshot at `snapshots[index]` exactly when Kafka's
/// `removeStraySnapshots` deletes it, given every local segment's base
/// offset. The snapshot offsets must be distinct; their order is irrelevant.
#[requires(index@ < snapshots@.len())]
#[ensures(result == kafka_stray_removed(snapshots@, index@, segment_bases@))]
#[must_use]
pub fn producer_snapshot_stray(snapshots: &[i64], index: usize, segment_bases: &[i64]) -> bool {
    let offset = snapshots[index];
    let mut is_base = false;
    let mut above_every_base = true;
    let mut i = 0usize;
    #[invariant(i@ <= segment_bases@.len())]
    #[invariant(is_base == exists<j: Int> 0 <= j && j < i@ && segment_bases@[j]@ == offset@)]
    #[invariant(above_every_base == forall<j: Int> 0 <= j && j < i@
        ==> segment_bases@[j]@ < offset@)]
    #[variant(segment_bases@.len() - i@)]
    while i < segment_bases.len() {
        if segment_bases[i] == offset {
            is_base = true;
        }
        if segment_bases[i] >= offset {
            above_every_base = false;
        }
        i += 1;
    }
    let mut newest = true;
    let mut k = 0usize;
    #[invariant(k@ <= snapshots@.len())]
    #[invariant(newest == forall<j: Int> 0 <= j && j < k@ ==> snapshots@[j]@ <= offset@)]
    #[variant(snapshots@.len() - k@)]
    while k < snapshots.len() {
        if snapshots[k] > offset {
            newest = false;
        }
        k += 1;
    }
    !(is_base || (newest && above_every_base))
}

/// Return the index of the newest snapshot a reload keeps, as Kafka's
/// `loadFromSnapshot` loads `latestSnapshotFile()` after `truncateAndReload`
/// deleted every snapshot outside `(log_start, log_end]`. Input order is
/// irrelevant.
#[ensures(result == None ==>
    forall<i: Int> 0 <= i && i < offsets@.len()
        ==> !kafka_reload_keeps(offsets@[i]@, range.log_start@, range.log_end@))]
#[ensures(forall<selected: usize> result == Some(selected) ==>
    selected@ < offsets@.len()
        && kafka_reload_keeps(offsets@[selected@]@, range.log_start@, range.log_end@)
        && (forall<i: Int> 0 <= i && i < offsets@.len()
            && kafka_reload_keeps(offsets@[i]@, range.log_start@, range.log_end@)
            ==> offsets@[i]@ <= offsets@[selected@]@))]
#[must_use]
pub fn producer_snapshot_latest_index(
    offsets: &[i64],
    range: ProducerReloadRange,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(match best {
        None => forall<j: Int> 0 <= j && j < i@
            ==> !kafka_reload_keeps(offsets@[j]@, range.log_start@, range.log_end@),
        Some(selected) => selected@ < i@
            && kafka_reload_keeps(offsets@[selected@]@, range.log_start@, range.log_end@)
            && (forall<j: Int> 0 <= j && j < i@
                && kafka_reload_keeps(offsets@[j]@, range.log_start@, range.log_end@)
                ==> offsets@[j]@ <= offsets@[selected@]@),
    })]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if producer_snapshot_reload_keeps(offsets[i], range) {
            match best {
                None => best = Some(i),
                Some(selected) if offsets[i] > offsets[selected] => best = Some(i),
                Some(_) => {}
            }
        }
        i += 1;
    }
    best
}

/// One decoded producer-state snapshot entry, field for field as Kafka's
/// `ProducerStateManager` snapshot schema (version 1) lays it out, minus the
/// timestamp, which no validity rule reads.
///
/// Seven integers of four widths with seven meanings travel as one struct, so
/// a transposed call site does not compile.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerSnapshotEntryFacts {
    /// `ProducerId`.
    pub producer_id: i64,
    /// `ProducerEpoch`.
    pub producer_epoch: i16,
    /// `LastSequence`, or `-1` for a producer whose only record is a marker.
    pub last_sequence: i32,
    /// `LastOffset`, or `-1` with the same meaning.
    pub last_offset: i64,
    /// `OffsetDelta`: the last batch's offset count less one.
    pub offset_delta: i32,
    /// `CoordinatorEpoch`, or `-1` before any marker.
    pub coordinator_epoch: i32,
    /// `CurrentTxnFirstOffset`, or `-1` with no open transaction.
    pub current_txn_first_offset: i64,
}

/// The entry's last-record fields are either the no-record sentinel -- a
/// producer that has only written transaction markers, which Kafka writes as
/// `(-1, -1, 0)` -- or a real batch that ends before the snapshot's offset,
/// with room below its last offset for the offsets its delta spans.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn snapshot_last_record_valid(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        (entry.last_offset@ == -1 && entry.last_sequence@ == -1 && entry.offset_delta@ == 0)
            || (entry.last_offset@ >= 0
                && entry.last_sequence@ >= 0
                && entry.offset_delta@ >= 0
                && entry.last_offset@ >= entry.offset_delta@
                && entry.last_offset@ < snapshot_offset)
    }
}

/// The entry's open transaction is either absent (`-1`), or starts before
/// the snapshot's offset and no later than the producer's last record.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn snapshot_transaction_valid(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        entry.current_txn_first_offset@ == -1
            || (entry.current_txn_first_offset@ >= 0
                && entry.current_txn_first_offset@ < snapshot_offset
                && entry.current_txn_first_offset@ <= entry.last_offset@)
    }
}

/// What a snapshot at `snapshot_offset` can truthfully say about one
/// producer: a real producer identity, a coordinator epoch that is real or
/// Kafka's `-1`, and last-record and transaction fields that describe only
/// records before the snapshot's offset.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn snapshot_entry_valid_model(snapshot_offset: Int, entry: ProducerSnapshotEntryFacts) -> bool {
    pearlite! {
        snapshot_offset >= 0
            && entry.producer_id@ >= 0
            && entry.producer_epoch@ >= 0
            && entry.coordinator_epoch@ >= -1
            && snapshot_last_record_valid(snapshot_offset, entry)
            && snapshot_transaction_valid(snapshot_offset, entry)
    }
}

/// Validate one decoded producer entry against its snapshot's exclusive
/// offset boundary: the result is `snapshot_entry_valid_model`.
#[ensures(result == snapshot_entry_valid_model(snapshot_offset@, entry))]
#[must_use]
pub fn producer_snapshot_entry_valid(
    snapshot_offset: i64,
    entry: ProducerSnapshotEntryFacts,
) -> bool {
    let last_record_valid =
        (entry.last_offset == -1 && entry.last_sequence == -1 && entry.offset_delta == 0)
            || (entry.last_offset >= 0
                && entry.last_sequence >= 0
                && entry.offset_delta >= 0
                && entry.last_offset >= i64::from(entry.offset_delta)
                && entry.last_offset < snapshot_offset);
    let transaction_valid = entry.current_txn_first_offset == -1
        || (entry.current_txn_first_offset >= 0
            && entry.current_txn_first_offset < snapshot_offset
            && entry.current_txn_first_offset <= entry.last_offset);
    snapshot_offset >= 0
        && entry.producer_id >= 0
        && entry.producer_epoch >= 0
        && entry.coordinator_epoch >= -1
        && last_record_valid
        && transaction_valid
}

/// Select the replay cursor after a reload loaded `snapshot`, or none.
///
/// Kafka's `UnifiedLog.rebuildProducerState` replays each local segment from
/// `max(segment.baseOffset, mapEndOffset, logStartOffset)` (see
/// `kafka_replay_start`). The range must be nonnegative and ordered, with
/// the local start at or below the log end, and a loaded snapshot must be one
/// the reload keeps; anything else is rejected as corrupt.
///
/// The cursor can land inside a batch when a trim did. Kafka's
/// `LogSegment.read(startOffset, ..)` then starts from the batch that holds
/// the cursor, and replaying that whole batch is the host's job.
#[ensures((result != None) == (range.log_start@ >= 0
    && range.log_start@ <= range.log_end@
    && range.local_start@ >= 0
    && range.local_start@ <= range.log_end@
    && match snapshot {
        Some(offset) => kafka_reload_keeps(offset@, range.log_start@, range.log_end@),
        None => true,
    }))]
#[ensures(forall<start: i64> result == Some(start) ==> start@ == kafka_replay_start(range, snapshot))]
#[must_use]
pub fn producer_snapshot_replay_start(
    range: ProducerReloadRange,
    snapshot: Option<i64>,
) -> Option<i64> {
    if range.log_start < 0
        || range.log_end < range.log_start
        || range.local_start < 0
        || range.log_end < range.local_start
    {
        return None;
    }
    let map_end = match snapshot {
        Some(offset) if producer_snapshot_reload_keeps(offset, range) => offset,
        Some(_) => return None,
        None => range.log_start,
    };
    Some(map_end.max(range.local_start))
}

#[cfg(test)]
mod tests {
    use super::{
        ProducerReloadRange, ProducerSnapshotEntryFacts, producer_snapshot_entry_valid,
        producer_snapshot_latest_index, producer_snapshot_reload_keeps,
        producer_snapshot_reload_log_start, producer_snapshot_replay_start,
        producer_snapshot_stray,
    };

    const RANGE: ProducerReloadRange = ProducerReloadRange {
        log_start: 5,
        local_start: 5,
        log_end: 10,
    };

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
            assert2::check!(
                producer_snapshot_reload_log_start(remote, established, local) == expected
            );
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

    #[test]
    fn replay_starts_at_the_loaded_snapshot_or_the_log_start() {
        let tiered = ProducerReloadRange {
            log_start: 0,
            local_start: 5,
            log_end: 10,
        };
        let trimmed_below_local = ProducerReloadRange {
            log_start: 7,
            local_start: 5,
            log_end: 10,
        };
        for (range, snapshot, expected) in [
            (
                ProducerReloadRange {
                    log_start: 0,
                    local_start: 0,
                    log_end: 0,
                },
                None,
                Some(0),
            ),
            (RANGE, None, Some(5)),
            (RANGE, Some(7), Some(7)),
            (RANGE, Some(10), Some(10)),
            // Snapshots the reload deletes cannot be loaded.
            (RANGE, Some(5), None),
            (RANGE, Some(3), None),
            (RANGE, Some(11), None),
            (RANGE, Some(-1), None),
            // Remote storage: no replay below the first local segment.
            (tiered, None, Some(5)),
            (tiered, Some(3), Some(5)),
            (tiered, Some(8), Some(8)),
            (trimmed_below_local, None, Some(7)),
            // Malformed ranges.
            (
                ProducerReloadRange {
                    log_start: 5,
                    local_start: 5,
                    log_end: 4,
                },
                None,
                None,
            ),
            (
                ProducerReloadRange {
                    log_start: -1,
                    local_start: 0,
                    log_end: 10,
                },
                None,
                None,
            ),
            (
                ProducerReloadRange {
                    log_start: 0,
                    local_start: 11,
                    log_end: 10,
                },
                None,
                None,
            ),
            (
                ProducerReloadRange {
                    log_start: 0,
                    local_start: -1,
                    log_end: 10,
                },
                None,
                None,
            ),
        ] {
            assert2::check!(
                producer_snapshot_replay_start(range, snapshot) == expected,
                "{range:?} {snapshot:?}"
            );
        }
    }
}
