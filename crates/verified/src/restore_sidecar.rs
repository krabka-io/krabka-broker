//! Restore-side validation of archived sparse indexes and state sidecars.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};

/// Compute the largest relative offset an archived sidecar may reference.
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
pub fn restore_index_frontier(segment_base: i64, segment_end: i64) -> Option<u32> {
    if segment_base < 0 || segment_end < segment_base {
        return None;
    }
    let relative = segment_end.checked_sub(segment_base)?;
    if relative > i64::from(u32::MAX) {
        None
    } else {
        #[cfg(creusot)]
        {
            Some(relative as u32)
        }
        #[cfg(not(creusot))]
        u32::try_from(relative).ok()
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
    max_relative: u32,
    log_bytes: u64,
) -> bool {
    relative_offset <= max_relative
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
    max_relative: u32,
) -> bool {
    relative_offset <= max_relative
        && match previous {
            Some((previous_timestamp, previous_relative)) => {
                previous_timestamp <= timestamp && previous_relative < relative_offset
            }
            None => true,
        }
}

/// Validate one decoded aborted-transaction index entry.
#[ensures(result == (producer_id@ >= 0
    && segment_base@ >= 0
    && segment_base@ <= start_offset@
    && start_offset@ <= last_offset@
    && last_offset@ <= segment_end@
    && match previous_start {
        Some(previous) => previous@ < start_offset@,
        None => true,
    }))]
#[must_use]
pub fn restore_txn_index_entry_valid(
    previous_start: Option<i64>,
    start_offset: i64,
    last_offset: i64,
    producer_id: i64,
    segment_base: i64,
    segment_end: i64,
) -> bool {
    producer_id >= 0
        && segment_base >= 0
        && segment_base <= start_offset
        && start_offset <= last_offset
        && last_offset <= segment_end
        && match previous_start {
            Some(previous) => previous < start_offset,
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
    use super::{
        restore_index_frontier, restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
        restore_producer_ids_strict, restore_time_index_entry_valid, restore_txn_index_entry_valid,
    };

    #[test]
    fn index_frontier_and_entries_cover_boundaries() {
        assert2::check!(restore_index_frontier(100, 104) == Some(4));
        assert2::check!(restore_index_frontier(-1, 104) == None);
        assert2::check!(restore_index_frontier(104, 100) == None);
        assert2::check!(restore_index_frontier(0, i64::MAX) == None);

        assert2::check!(restore_offset_index_entry_valid(None, 0, 0, 4, 100));
        assert2::check!(restore_offset_index_entry_valid(
            Some((0, 0)),
            3,
            50,
            4,
            100
        ));
        assert2::check!(!restore_offset_index_entry_valid(
            Some((3, 50)),
            3,
            60,
            4,
            100
        ));
        assert2::check!(!restore_offset_index_entry_valid(
            Some((0, 50)),
            3,
            50,
            4,
            100
        ));
        assert2::check!(!restore_offset_index_entry_valid(None, 5, 0, 4, 100));
        assert2::check!(!restore_offset_index_entry_valid(None, 0, 100, 4, 100));

        assert2::check!(restore_time_index_entry_valid(None, 10, 0, 4));
        assert2::check!(restore_time_index_entry_valid(Some((10, 0)), 10, 3, 4));
        assert2::check!(!restore_time_index_entry_valid(Some((10, 3)), 11, 3, 4));
        assert2::check!(!restore_time_index_entry_valid(Some((10, 0)), 9, 3, 4));
    }

    #[test]
    fn transaction_and_epoch_entries_are_strict_and_bounded() {
        assert2::check!(restore_txn_index_entry_valid(None, 100, 102, 7, 100, 104));
        assert2::check!(restore_txn_index_entry_valid(
            Some(100),
            103,
            104,
            8,
            100,
            104
        ));
        assert2::check!(!restore_txn_index_entry_valid(
            Some(100),
            100,
            104,
            8,
            100,
            104
        ));
        assert2::check!(!restore_txn_index_entry_valid(None, 99, 102, 7, 100, 104));
        assert2::check!(!restore_txn_index_entry_valid(None, 100, 105, 7, 100, 104));
        assert2::check!(!restore_txn_index_entry_valid(None, 100, 102, -1, 100, 104));

        assert2::check!(restore_leader_epoch_entry_valid(None, 0, 100, 100, 104));
        assert2::check!(restore_leader_epoch_entry_valid(
            Some((0, 100)),
            1,
            103,
            100,
            104
        ));
        assert2::check!(!restore_leader_epoch_entry_valid(
            Some((1, 100)),
            1,
            103,
            100,
            104
        ));
        assert2::check!(!restore_leader_epoch_entry_valid(
            Some((0, 103)),
            1,
            103,
            100,
            104
        ));
        assert2::check!(!restore_leader_epoch_entry_valid(None, -1, 100, 100, 104));
        assert2::check!(!restore_leader_epoch_entry_valid(None, 0, 105, 100, 104));
    }

    #[test]
    fn producer_ids_are_canonical_and_unique() {
        assert2::check!(restore_producer_ids_strict(&[]));
        assert2::check!(restore_producer_ids_strict(&[7]));
        assert2::check!(restore_producer_ids_strict(&[7, 8, 9]));
        assert2::check!(!restore_producer_ids_strict(&[7, 9, 8]));
        assert2::check!(!restore_producer_ids_strict(&[7, 9, 9]));
    }
}
