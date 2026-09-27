//! Restore-side validation of archived sparse indexes and state sidecars.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, invariant};

/// Compute the largest relative offset an archived sidecar may reference.
///
/// Kafka's sparse indexes store offsets relative to the segment base as a
/// 32-bit value (`AbstractIndex.relativeOffset`), so a segment whose span does
/// not fit `u32` has no valid index. The frontier is returned as `i64` so the
/// entry validators compare widened `u32` offsets against it and no narrowing
/// conversion is needed.
#[ensures(match result {
    Some(frontier) => segment_base@ >= 0
        && segment_end@ >= segment_base@
        && frontier@ == segment_end@ - segment_base@
        && frontier@ <= u32::MAX@,
    None => segment_base@ < 0
        || segment_end@ < segment_base@
        || segment_end@ - segment_base@ > u32::MAX@,
})]
#[must_use]
pub fn restore_index_frontier(segment_base: i64, segment_end: i64) -> Option<i64> {
    if segment_base < 0 || segment_end < segment_base {
        return None;
    }
    let relative = segment_end - segment_base;
    if relative > i64::from(u32::MAX) {
        None
    } else {
        Some(relative)
    }
}

/// Validate one decoded offset-index entry against its predecessor and the
/// verified log extent.
#[ensures(result == (relative_offset@ <= max_relative@
    && position@ < log_bytes@
    && match previous {
        Some((previous_relative, previous_position)) =>
            previous_relative@ < relative_offset@ && previous_position@ < position@,
        None => true,
    }))]
#[must_use]
pub fn restore_offset_index_entry_valid(
    previous: Option<(u32, u32)>,
    relative_offset: u32,
    position: u32,
    max_relative: i64,
    log_bytes: u64,
) -> bool {
    i64::from(relative_offset) <= max_relative
        && u64::from(position) < log_bytes
        && match previous {
            Some((previous_relative, previous_position)) => {
                previous_relative < relative_offset && previous_position < position
            }
            None => true,
        }
}

/// Validate one decoded time-index entry. Relative offsets strictly advance;
/// timestamps may repeat but may not decrease.
///
/// Kafka's `TimeIndex.maybeAppend` only appends a strictly greater timestamp,
/// but krabka's segment writer appends the segment's running maximum
/// timestamp at every sparse-index point, so an archive krabka wrote repeats a
/// timestamp whenever a batch does not raise the maximum. Non-decreasing is
/// the rule both writers satisfy, and the one a binary search over the index
/// (`TimeIndex.lookup`) needs.
#[ensures(result == (relative_offset@ <= max_relative@
    && match previous {
        Some((previous_timestamp, previous_relative)) =>
            previous_timestamp@ <= timestamp@ && previous_relative@ < relative_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_time_index_entry_valid(
    previous: Option<(i64, u32)>,
    timestamp: i64,
    relative_offset: u32,
    max_relative: i64,
) -> bool {
    i64::from(relative_offset) <= max_relative
        && match previous {
            Some((previous_timestamp, previous_relative)) => {
                previous_timestamp <= timestamp && previous_relative < relative_offset
            }
            None => true,
        }
}

/// One decoded aborted-transaction index entry (`AbortedTxn`).
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreAbortedTxn {
    pub producer_id: i64,
    /// The transaction's first offset. It may precede the segment base when
    /// the transaction began before a segment roll.
    pub start_offset: i64,
    /// The abort marker's offset.
    pub last_offset: i64,
}

/// The inclusive offset extent of the segment that owns an index.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RestoreSegmentExtent {
    pub base_offset: i64,
    pub last_offset: i64,
}

/// Validate one decoded aborted-transaction index entry against the previous
/// entry's `last_offset`.
///
/// An aborted transaction is indexed in the segment that holds its abort
/// marker, in marker order, so `last_offset` (the marker) lies inside the
/// segment and strictly increases across entries (Kafka's
/// `TransactionIndex.append` rejects a non-increasing `lastOffset`, and
/// `TransactionIndex.sanityCheck` requires `lastOffset` at or past the index's
/// start offset). `start_offset` is neither bounded below by the segment base
/// (a transaction may span a segment roll) nor ordered across entries
/// (transactions interleave: one opened later can abort first).
#[ensures(result == (entry.producer_id@ >= 0
    && segment.base_offset@ >= 0
    && entry.start_offset@ >= 0
    && entry.start_offset@ <= entry.last_offset@
    && segment.base_offset@ <= entry.last_offset@
    && entry.last_offset@ <= segment.last_offset@
    && match previous_last {
        Some(previous) => previous@ < entry.last_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_txn_index_entry_valid(
    previous_last: Option<i64>,
    entry: RestoreAbortedTxn,
    segment: RestoreSegmentExtent,
) -> bool {
    entry.producer_id >= 0
        && segment.base_offset >= 0
        && entry.start_offset >= 0
        && entry.start_offset <= entry.last_offset
        && segment.base_offset <= entry.last_offset
        && entry.last_offset <= segment.last_offset
        && match previous_last {
            Some(previous) => previous < entry.last_offset,
            None => true,
        }
}

/// Validate one segment-scoped leader-epoch checkpoint row.
#[ensures(result == (epoch@ >= 0
    && segment_base@ >= 0
    && segment_base@ <= start_offset@
    && start_offset@ <= segment_end@
    && match previous {
        Some((previous_epoch, previous_start)) =>
            previous_epoch@ < epoch@ && previous_start@ < start_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_leader_epoch_entry_valid(
    previous: Option<(i32, i64)>,
    epoch: i32,
    start_offset: i64,
    segment_base: i64,
    segment_end: i64,
) -> bool {
    epoch >= 0
        && segment_base >= 0
        && segment_base <= start_offset
        && start_offset <= segment_end
        && match previous {
            Some((previous_epoch, previous_start)) => {
                previous_epoch < epoch && previous_start < start_offset
            }
            None => true,
        }
}

/// Accept exactly the canonical strictly increasing producer-ID order. Strict
/// order also proves every ID is unique while keeping validation linear.
#[ensures(result == (forall<i: Int> 1 <= i && i < producer_ids@.len()
    ==> producer_ids@[i - 1]@ < producer_ids@[i]@))]
#[must_use]
pub fn restore_producer_ids_strict(producer_ids: &[i64]) -> bool {
    let mut index = 0usize;
    #[invariant(index@ <= producer_ids@.len())]
    #[invariant(forall<i: Int> 1 <= i && i < index@
        ==> producer_ids@[i - 1]@ < producer_ids@[i]@)]
    #[variant(producer_ids@.len() - index@)]
    while index < producer_ids.len() {
        if index > 0 && producer_ids[index - 1] >= producer_ids[index] {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        RestoreAbortedTxn, RestoreSegmentExtent, restore_index_frontier,
        restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
        restore_producer_ids_strict, restore_time_index_entry_valid, restore_txn_index_entry_valid,
    };

    #[test]
    fn index_frontier_is_the_u32_relative_span() {
        for (base, end, expected) in [
            (100, 104, Some(4)),
            (0, 0, Some(0)),
            (0, i64::from(u32::MAX), Some(i64::from(u32::MAX))),
            (0, i64::from(u32::MAX) + 1, None),
            (0, i64::MAX, None),
            (-1, 104, None),
            (104, 100, None),
        ] {
            check!(restore_index_frontier(base, end) == expected);
        }
    }

    #[test]
    fn offset_and_time_index_entries_are_strict_and_bounded() {
        check!(restore_offset_index_entry_valid(None, 0, 0, 4, 100));
        check!(restore_offset_index_entry_valid(
            Some((0, 0)),
            3,
            50,
            4,
            100
        ));
        check!(!restore_offset_index_entry_valid(
            Some((3, 50)),
            3,
            60,
            4,
            100
        ));
        check!(!restore_offset_index_entry_valid(
            Some((0, 50)),
            3,
            50,
            4,
            100
        ));
        check!(!restore_offset_index_entry_valid(None, 5, 0, 4, 100));
        check!(!restore_offset_index_entry_valid(None, 0, 100, 4, 100));
        check!(restore_offset_index_entry_valid(
            None,
            u32::MAX,
            0,
            i64::from(u32::MAX),
            1
        ));

        check!(restore_time_index_entry_valid(None, 10, 0, 4));
        // krabka's writer repeats the running maximum timestamp.
        check!(restore_time_index_entry_valid(Some((10, 0)), 10, 3, 4));
        check!(!restore_time_index_entry_valid(Some((10, 3)), 11, 3, 4));
        check!(!restore_time_index_entry_valid(Some((10, 0)), 9, 3, 4));
        check!(!restore_time_index_entry_valid(None, 10, 5, 4));
    }

    const SEGMENT: RestoreSegmentExtent = RestoreSegmentExtent {
        base_offset: 100,
        last_offset: 110,
    };

    const fn txn(producer_id: i64, start_offset: i64, last_offset: i64) -> RestoreAbortedTxn {
        RestoreAbortedTxn {
            producer_id,
            start_offset,
            last_offset,
        }
    }

    /// Walk a whole index the way the host does, threading `last_offset`.
    fn txn_index_valid(entries: &[RestoreAbortedTxn], segment: RestoreSegmentExtent) -> bool {
        let mut previous_last = None;
        for &entry in entries {
            if !restore_txn_index_entry_valid(previous_last, entry, segment) {
                return false;
            }
            previous_last = Some(entry.last_offset);
        }
        true
    }

    #[test]
    fn transaction_index_follows_kafka_marker_order() {
        for (name, entries, segment, expected) in [
            ("one abort", vec![txn(7, 100, 102)], SEGMENT, true),
            (
                "sequential aborts",
                vec![txn(7, 100, 102), txn(8, 103, 104)],
                SEGMENT,
                true,
            ),
            (
                // Producer B opens at 105 and aborts at 108; producer A
                // opened earlier at 100 and aborts later at 110. Entries are
                // in marker order, so starts decrease.
                "interleaved aborts in marker order",
                vec![txn(2, 105, 108), txn(1, 100, 110)],
                SEGMENT,
                true,
            ),
            (
                // The transaction's data 0..=2 sits in the segment based at
                // 0; the abort marker at 3 lands in the segment rolled at 3.
                "transaction spanning a segment roll",
                vec![txn(1_000, 0, 3)],
                RestoreSegmentExtent {
                    base_offset: 3,
                    last_offset: 4,
                },
                true,
            ),
            (
                "marker order regressing",
                vec![txn(1, 100, 110), txn(2, 105, 108)],
                SEGMENT,
                false,
            ),
            (
                "repeated marker",
                vec![txn(1, 100, 104), txn(2, 101, 104)],
                SEGMENT,
                false,
            ),
            (
                "marker before the segment",
                vec![txn(7, 90, 99)],
                SEGMENT,
                false,
            ),
            (
                "marker past the segment",
                vec![txn(7, 100, 111)],
                SEGMENT,
                false,
            ),
            ("start after marker", vec![txn(7, 105, 104)], SEGMENT, false),
            ("negative start", vec![txn(7, -1, 104)], SEGMENT, false),
            ("negative producer", vec![txn(-1, 100, 102)], SEGMENT, false),
        ] {
            check!(txn_index_valid(&entries, segment) == expected, "{name}");
        }
    }

    #[test]
    fn leader_epoch_entries_are_strict_and_bounded() {
        check!(restore_leader_epoch_entry_valid(None, 0, 100, 100, 104));
        check!(restore_leader_epoch_entry_valid(
            Some((0, 100)),
            1,
            103,
            100,
            104
        ));
        check!(!restore_leader_epoch_entry_valid(
            Some((1, 100)),
            1,
            103,
            100,
            104
        ));
        check!(!restore_leader_epoch_entry_valid(
            Some((0, 103)),
            1,
            103,
            100,
            104
        ));
        check!(!restore_leader_epoch_entry_valid(None, -1, 100, 100, 104));
        check!(!restore_leader_epoch_entry_valid(None, 0, 105, 100, 104));
    }

    #[test]
    fn producer_ids_are_canonical_and_unique() {
        check!(restore_producer_ids_strict(&[]));
        check!(restore_producer_ids_strict(&[7]));
        check!(restore_producer_ids_strict(&[7, 8, 9]));
        check!(!restore_producer_ids_strict(&[7, 9, 8]));
        check!(!restore_producer_ids_strict(&[7, 9, 9]));
    }
}
