//! Pure `KRaft` offset-frontier and half-open-window kernels.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::*;

/// The only response-derived mutation an admitted `KRaft` Fetch may perform.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum FetchResponseMutation {
    Reject,
    Snapshot,
    Truncate,
    Append,
    HighWatermark,
}

/// Fence a Fetch response against the live role, leader, and epoch, then
/// select exactly one mutation path.
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Reject) ==
    (role_leader != Some(from)
        || current_leader != Some(from)
        || response_leader != from
        || response_epoch != current_epoch)))]
#[cfg_attr(creusot, ensures(match result {
    FetchResponseMutation::Snapshot => role_leader == Some(from)
        && current_leader == Some(from)
        && response_leader == from
        && response_epoch == current_epoch
        && has_snapshot,
    FetchResponseMutation::Truncate => role_leader == Some(from)
        && current_leader == Some(from)
        && response_leader == from
        && response_epoch == current_epoch
        && !has_snapshot
        && has_divergence,
    FetchResponseMutation::Append => role_leader == Some(from)
        && current_leader == Some(from)
        && response_leader == from
        && response_epoch == current_epoch
        && !has_snapshot
        && !has_divergence
        && has_records,
    FetchResponseMutation::HighWatermark => role_leader == Some(from)
        && current_leader == Some(from)
        && response_leader == from
        && response_epoch == current_epoch
        && !has_snapshot
        && !has_divergence
        && !has_records,
    FetchResponseMutation::Reject => true,
}))]
#[must_use]
#[allow(
    clippy::too_many_arguments,
    reason = "the safety boundary takes the exact role, leader, epoch, and mutation fields"
)]
pub fn fetch_response_mutation(
    role_leader: Option<u64>,
    current_leader: Option<u64>,
    current_epoch: u32,
    from: u64,
    response_leader: u64,
    response_epoch: u32,
    has_snapshot: bool,
    has_divergence: bool,
    has_records: bool,
) -> FetchResponseMutation {
    if role_leader != Some(from)
        || current_leader != Some(from)
        || response_leader != from
        || response_epoch != current_epoch
    {
        FetchResponseMutation::Reject
    } else if has_snapshot {
        FetchResponseMutation::Snapshot
    } else if has_divergence {
        FetchResponseMutation::Truncate
    } else if has_records {
        FetchResponseMutation::Append
    } else {
        FetchResponseMutation::HighWatermark
    }
}

/// Advance a high watermark monotonically without passing the log end.
#[must_use]
#[cfg_attr(creusot, ensures(result@ >= previous@))]
#[cfg_attr(creusot, ensures(previous@ <= log_end@ ==> result@ <= log_end@))]
#[cfg_attr(creusot, ensures(result@ ==
    if requested@ <= log_end@ {
        if previous@ >= requested@ { previous@ } else { requested@ }
    } else if previous@ >= log_end@ { previous@ } else { log_end@ }))]
pub const fn advance_high_watermark(previous: i64, requested: i64, log_end: i64) -> i64 {
    let clamped = if requested <= log_end {
        requested
    } else {
        log_end
    };
    if previous >= clamped {
        previous
    } else {
        clamped
    }
}

/// Whether `value` lies in `[start, end)`.
#[must_use]
#[cfg_attr(creusot, ensures(result == (start@ <= value@ && value@ < end@)))]
pub const fn in_half_open_window(value: i64, start: i64, end: i64) -> bool {
    start <= value && value < end
}

/// Whether a monotonic frontier has reached a target offset.
#[must_use]
#[cfg_attr(creusot, ensures(result == (frontier@ >= target@)))]
pub const fn frontier_reaches(frontier: i64, target: i64) -> bool {
    frontier >= target
}

/// Return the length of the strictly ordered control-record prefix below a
/// half-open frontier.
#[cfg_attr(creusot, requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < offsets@.len() ==> offsets@[i]@ < offsets@[j]@))]
#[cfg_attr(creusot, ensures(result@ <= offsets@.len()))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    0 <= i && i < result@ ==> offsets@[i]@ < frontier@))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    result@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
#[must_use]
pub fn control_history_frontier(offsets: &[i64], frontier: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = offsets.len();
    #[cfg_attr(creusot, invariant(lo@ <= hi@ && hi@ <= offsets@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        0 <= i && i < lo@ ==> offsets@[i]@ < frontier@))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        hi@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
    #[cfg_attr(creusot, variant(hi - lo))]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if offsets[mid] < frontier {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Compute one record's contiguous offset delta and its batch's final delta.
///
/// Empty batches, out-of-bounds indexes, and record counts that cannot be
/// represented by Kafka's signed offset-delta field fail closed.
#[must_use]
#[cfg_attr(creusot, ensures(match result {
    Some((offset_delta, last_offset_delta)) => record_count@ > 0
        && record_index@ < record_count@
        && record_count@ <= i32::MAX@ + 1
        && offset_delta@ == record_index@
        && last_offset_delta@ == record_count@ - 1,
    None => record_count@ == 0
        || record_index@ >= record_count@
        || record_count@ > i32::MAX@ + 1,
}))]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "the kernel rejects values outside i32 before conversion"
)]
pub fn metadata_record_coordinates(record_count: usize, record_index: usize) -> Option<(i32, i32)> {
    let max_records = i32::MAX as usize + 1;
    if record_count == 0 || record_index >= record_count || record_count > max_records {
        None
    } else {
        Some((record_index as i32, (record_count - 1) as i32))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn high_watermark_is_monotonic_and_clamped() {
        for (previous, requested, log_end, expected) in
            [(2, 5, 4, 4), (2, 1, 4, 2), (2, 3, 4, 3), (5, 1, 4, 5)]
        {
            assert!(advance_high_watermark(previous, requested, log_end) == expected);
        }
    }

    #[test]
    fn windows_and_frontiers_use_exact_boundaries() {
        assert!(in_half_open_window(5, 5, 8));
        assert!(in_half_open_window(7, 5, 8));
        assert!(!in_half_open_window(4, 5, 8));
        assert!(!in_half_open_window(8, 5, 8));
        assert!(!frontier_reaches(4, 5));
        assert!(frontier_reaches(5, 5));
    }

    #[test]
    fn control_history_frontier_is_strictly_half_open() {
        let offsets = [-1, 2, 5, 9];
        for (frontier, expected) in [(-1, 0), (0, 1), (2, 1), (5, 2), (9, 3), (10, 4)] {
            assert!(control_history_frontier(&offsets, frontier) == expected);
        }
        assert!(control_history_frontier(&[], 5) == 0);
    }

    #[test]
    fn metadata_coordinates_are_contiguous_and_fail_closed() {
        assert!(metadata_record_coordinates(1, 0) == Some((0, 0)));
        assert!(metadata_record_coordinates(3, 0) == Some((0, 2)));
        assert!(metadata_record_coordinates(3, 1) == Some((1, 2)));
        assert!(metadata_record_coordinates(3, 2) == Some((2, 2)));
        assert!(metadata_record_coordinates(0, 0) == None);
        assert!(metadata_record_coordinates(3, 3) == None);
        assert!(metadata_record_coordinates(i32::MAX as usize + 2, 0) == None);
    }

    #[test]
    fn fetch_response_is_fenced_before_one_exclusive_mutation() {
        use FetchResponseMutation::{Append, HighWatermark, Reject, Snapshot, Truncate};

        let decide = |role_leader, current_leader, current_epoch, from, leader, epoch, s, d, r| {
            fetch_response_mutation(
                role_leader,
                current_leader,
                current_epoch,
                from,
                leader,
                epoch,
                s,
                d,
                r,
            )
        };
        assert!(decide(Some(2), Some(2), 3, 2, 2, 3, true, true, true) == Snapshot);
        assert!(decide(Some(2), Some(2), 3, 2, 2, 3, false, true, true) == Truncate);
        assert!(decide(Some(2), Some(2), 3, 2, 2, 3, false, false, true) == Append);
        assert!(decide(Some(2), Some(2), 3, 2, 2, 3, false, false, false) == HighWatermark);
        assert!(decide(None, Some(2), 3, 2, 2, 3, false, false, true) == Reject);
        assert!(decide(Some(2), Some(3), 3, 2, 2, 3, false, false, true) == Reject);
        assert!(decide(Some(2), Some(2), 3, 3, 2, 3, false, false, true) == Reject);
        assert!(decide(Some(2), Some(2), 3, 2, 3, 3, false, false, true) == Reject);
        assert!(decide(Some(2), Some(2), 3, 2, 2, 4, false, false, true) == Reject);
    }
}
