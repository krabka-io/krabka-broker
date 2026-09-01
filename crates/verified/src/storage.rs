//! Storage-transition admission decisions.

use creusot_std::prelude::*;

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
