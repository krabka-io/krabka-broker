//! Pure `KRaft` offset-frontier and half-open-window kernels.

#[cfg(creusot)]
use creusot_std::prelude::*;

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
}
