//! Storage-transition admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Segment-prefix and active-segment selection for a tail truncation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct LocalTruncationPlan {
    pub retained_sealed: usize,
    pub keep_active: bool,
}

/// Admit one batch only at the expected logical frontier and compute its
/// inclusive last offset and exclusive successor without signed overflow.
#[ensures(match result {
    Some((last, next)) => expected_base@ >= 0
        && supplied_base@ == expected_base@
        && last_offset_delta@ >= 0
        && last@ == supplied_base@ + last_offset_delta@
        && next@ == last@ + 1
        && supplied_base@ <= last@
        && last@ < next@,
    None => expected_base@ < 0
        || supplied_base@ != expected_base@
        || last_offset_delta@ < 0
        || supplied_base@ + last_offset_delta@ > i64::MAX@
        || supplied_base@ + last_offset_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn local_append_coordinates(
    expected_base: i64,
    supplied_base: i64,
    last_offset_delta: i32,
) -> Option<(i64, i64)> {
    if expected_base < 0 || supplied_base != expected_base || last_offset_delta < 0 {
        return None;
    }
    let last = supplied_base.checked_add(i64::from(last_offset_delta))?;
    let next = last.checked_add(1)?;
    Some((last, next))
}

/// Keep exactly the sealed-segment prefix whose bases precede the cut and keep
/// the current active segment exactly when its base also precedes the cut.
#[requires(forall<i: Int, j: Int>
    0 <= i && i < j && j < sealed_bases@.len() ==> sealed_bases@[i] < sealed_bases@[j])]
#[ensures(result.retained_sealed@ <= sealed_bases@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.retained_sealed@ ==>
    sealed_bases@[i]@ < cut@)]
#[ensures(forall<i: Int> result.retained_sealed@ <= i && i < sealed_bases@.len() ==>
    sealed_bases@[i]@ >= cut@)]
#[ensures(result.keep_active == match active_base {
    Some(base) => base@ < cut@,
    None => false,
})]
#[must_use]
pub fn local_truncation_plan(
    sealed_bases: &[i64],
    active_base: Option<i64>,
    cut: i64,
) -> LocalTruncationPlan {
    let mut retained_sealed = 0usize;
    #[invariant(retained_sealed@ <= sealed_bases@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < retained_sealed@ ==>
        sealed_bases@[i]@ < cut@)]
    #[variant(sealed_bases@.len() - retained_sealed@)]
    while retained_sealed < sealed_bases.len() && sealed_bases[retained_sealed] < cut {
        retained_sealed += 1;
    }
    LocalTruncationPlan {
        retained_sealed,
        keep_active: match active_base {
            Some(base) => base < cut,
            None => false,
        },
    }
}

/// Convert an absolute cut to a segment-relative offset without signed or
/// `u32` overflow. Segment bases and log cuts are nonnegative Kafka offsets.
#[ensures(match result {
    Some(relative) => segment_base@ >= 0
        && cut@ >= segment_base@
        && relative@ == cut@ - segment_base@
        && relative@ <= u32::MAX@,
    None => segment_base@ < 0
        || cut@ < segment_base@
        || cut@ - segment_base@ > u32::MAX@,
})]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[must_use]
pub fn truncation_relative_offset(segment_base: i64, cut: i64) -> Option<u32> {
    if segment_base < 0 || cut < segment_base {
        return None;
    }
    let relative = cut - segment_base;
    if relative > i64::from(u32::MAX) {
        None
    } else {
        Some(relative as u32)
    }
}

/// One decoded batch belongs to the exact retained prefix iff its inclusive
/// last offset is below the exclusive cut.
#[ensures(result == (batch_last@ < cut@))]
#[must_use]
pub const fn truncation_batch_retained(batch_last: i64, cut: i64) -> bool {
    batch_last < cut
}

/// Clamp a dependent frontier to the new log end.
#[ensures(result@ <= frontier@)]
#[ensures(result@ <= new_end@)]
#[ensures(result@ == frontier@ || result@ == new_end@)]
#[must_use]
pub const fn truncation_frontier(frontier: i64, new_end: i64) -> i64 {
    if frontier < new_end {
        frontier
    } else {
        new_end
    }
}

/// A future log may replace the current log only at the exact same frontier.
#[ensures(result == (current_leo@ == future_leo@))]
#[must_use]
pub const fn future_log_swap_admission(current_leo: i64, future_leo: i64) -> bool {
    current_leo == future_leo
}

/// Admit exactly the legal remote-segment lifecycle edges.
///
/// The host maps `CopySegmentStarted`, `CopySegmentFinished`,
/// `DeleteSegmentStarted`, and `DeleteSegmentFinished` to `0..=3`.
#[ensures(result == (
    (from@ == 0 && (to@ == 1 || to@ == 2))
        || (from@ == 1 && to@ == 2)
        || (from@ == 2 && to@ == 3)
))]
#[must_use]
pub const fn remote_segment_transition(from: u8, to: u8) -> bool {
    (from == 0 && (to == 1 || to == 2)) || (from == 1 && to == 2) || (from == 2 && to == 3)
}

/// Admit exactly the legal remote-partition deletion lifecycle edges.
///
/// The host maps no prior state to `0`, then `DeletePartitionMarked`,
/// `DeletePartitionStarted`, and `DeletePartitionFinished` to `1..=3`.
#[ensures(result == (
    (from@ == 0 && to@ == 1)
        || (from@ == 1 && to@ == 2)
        || (from@ == 2 && to@ == 3)
))]
#[must_use]
pub const fn remote_partition_delete_transition(from: u8, to: u8) -> bool {
    (from == 0 && to == 1) || (from == 1 && to == 2) || (from == 2 && to == 3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_truncation_kernels_cover_boundaries_and_invalid_offsets() {
        assert2::check!(
            local_truncation_plan(&[0, 10, 20], Some(30), 20)
                == LocalTruncationPlan {
                    retained_sealed: 2,
                    keep_active: false,
                }
        );
        assert2::check!(
            local_truncation_plan(&[0, 10], Some(15), 20)
                == LocalTruncationPlan {
                    retained_sealed: 2,
                    keep_active: true,
                }
        );

        assert2::check!(truncation_relative_offset(10, 15) == Some(5));
        assert2::check!(truncation_relative_offset(-1, 0).is_none());
        assert2::check!(truncation_relative_offset(10, 9).is_none());
        assert2::check!(truncation_relative_offset(0, i64::from(u32::MAX) + 1).is_none());

        assert2::check!(truncation_batch_retained(19, 20));
        assert2::check!(!truncation_batch_retained(20, 20));
        assert2::check!(truncation_frontier(12, 20) == 12);
        assert2::check!(truncation_frontier(21, 20) == 20);
    }

    #[test]
    fn local_append_coordinates_are_exact_and_fail_closed() {
        assert2::check!(local_append_coordinates(10, 10, 2) == Some((12, 13)));
        assert2::check!(local_append_coordinates(10, 9, 0).is_none());
        assert2::check!(local_append_coordinates(-1, -1, 0).is_none());
        assert2::check!(local_append_coordinates(10, 10, -1).is_none());
        assert2::check!(local_append_coordinates(i64::MAX, i64::MAX, 0).is_none());
        assert2::check!(
            local_append_coordinates(i64::MAX - 1, i64::MAX - 1, 0)
                == Some((i64::MAX - 1, i64::MAX))
        );
    }

    #[test]
    fn future_log_swap_requires_equal_frontiers() {
        assert2::assert!(future_log_swap_admission(7, 7));
        assert2::assert!(!future_log_swap_admission(7, 6));
        assert2::assert!(!future_log_swap_admission(7, 8));
    }

    #[test]
    fn remote_segment_transition_matrix_is_exhaustive() {
        let expected = [
            [false, true, true, false],
            [false, false, true, false],
            [false, false, false, true],
            [false, false, false, false],
        ];
        for (from, row) in [0_u8, 1, 2, 3].into_iter().zip(expected) {
            for (to, want) in [0_u8, 1, 2, 3].into_iter().zip(row) {
                assert2::check!(remote_segment_transition(from, to) == want);
            }
        }
    }

    #[test]
    fn remote_partition_delete_transition_matrix_is_exhaustive() {
        let expected = [
            [true, false, false],
            [false, true, false],
            [false, false, true],
            [false, false, false],
        ];
        for (from, row) in [0_u8, 1, 2, 3].into_iter().zip(expected) {
            for (to, want) in [1_u8, 2, 3].into_iter().zip(row) {
                assert2::check!(remote_partition_delete_transition(from, to) == want);
            }
        }
    }
}
