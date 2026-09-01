//! Pure local-trim decision for the diskless WAL flusher.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Whether and where to advance one diskless partition's local log start.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct DisklessTrimDecision {
    pub should_trim: bool,
    pub target: i64,
}

#[cfg(creusot)]
#[logic]
fn effective_trim_lag(safety_lag: i64) -> Int {
    pearlite! { if safety_lag@ < 0 { 0 } else { safety_lag@ } }
}

#[cfg(creusot)]
#[logic]
fn trim_target(frontier: i64, high_watermark: i64, safety_lag: i64) -> Int {
    pearlite! {
        let high_watermark_floor = high_watermark@ - effective_trim_lag(safety_lag);
        if frontier@ < high_watermark_floor { frontier@ } else { high_watermark_floor }
    }
}

/// Plan a local trim behind both the committed object-store frontier and the
/// high watermark's configured safety lag.
///
/// Negative offsets and a lag larger than the high watermark fail closed. A
/// negative lag retains the caller's previous behavior and is treated as zero.
#[ensures(result.should_trim == (
    frontier@ >= 0
        && high_watermark@ >= 0
        && current_start@ >= 0
        && effective_trim_lag(safety_lag) <= high_watermark@
        && current_start@ < trim_target(frontier, high_watermark, safety_lag)
))]
#[ensures(result.target@ == if result.should_trim {
    trim_target(frontier, high_watermark, safety_lag)
} else {
    current_start@
})]
#[ensures(result.target@ >= current_start@)]
#[ensures(result.should_trim ==> result.target@ <= frontier@)]
#[ensures(result.should_trim ==>
    result.target@ + effective_trim_lag(safety_lag) <= high_watermark@)]
#[must_use]
pub fn diskless_trim_decision(
    frontier: i64,
    high_watermark: i64,
    safety_lag: i64,
    current_start: i64,
) -> DisklessTrimDecision {
    if frontier < 0 || high_watermark < 0 || current_start < 0 {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let safety_lag = safety_lag.max(0);
    if safety_lag > high_watermark {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let target = frontier.min(high_watermark - safety_lag);
    if target <= current_start {
        DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        }
    } else {
        DisklessTrimDecision {
            should_trim: true,
            target,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn trim_is_bounded_non_regressing_and_overflow_safe() {
        for (frontier, high_watermark, lag, current, expected) in [
            (90, 100, 10, 50, (true, 90)),
            (100, 90, 10, 50, (true, 80)),
            (80, 90, 10, 80, (false, 80)),
            (70, 90, 10, 80, (false, 80)),
            (i64::MAX, i64::MAX, 0, i64::MAX - 1, (true, i64::MAX)),
            (i64::MAX, 0, i64::MAX, 0, (false, 0)),
            (10, 10, -1, 0, (true, 10)),
            (-1, 10, 0, 0, (false, 0)),
            (10, -1, 0, 0, (false, 0)),
        ] {
            let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
            check!(decision.should_trim == expected.0);
            check!(decision.target == expected.1);
            check!(decision.target >= current);
            if decision.should_trim {
                check!(decision.target <= frontier);
                check!(decision.target <= high_watermark - lag.max(0));
            }
        }
    }
}
