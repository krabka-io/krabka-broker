//! Storage-transition admission decisions.

use creusot_std::prelude::*;

/// A future log may replace the current log only at the exact same frontier.
#[ensures(result == (current_leo@ == future_leo@))]
#[must_use]
pub const fn future_log_swap_admission(current_leo: i64, future_leo: i64) -> bool {
    current_leo == future_leo
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
}
