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

/// One decoded-batch step in a diskless cold-read run.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum DisklessBatchStep {
    Invalid,
    Skip(usize),
    Start(usize),
    Continue(usize),
    Stop,
}

/// Select the covering logical range, or the first successor after a gap.
#[requires(forall<i: Int> 0 <= i && i < entries@.len()
    ==> entries@[i].0@ <= entries@[i].1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].1@ < entries@[j].0@)]
#[ensures(match result {
    Some(index) => index@ < entries@.len()
        && ((entries@[index@].0@ <= requested@ && requested@ <= entries@[index@].1@)
            || (requested@ < entries@[index@].0@
                && index@ > 0
                && entries@[index@ - 1].1@ < requested@)),
    None => entries@.len() == 0
        || requested@ < entries@[0].0@
        || entries@[entries@.len() - 1].1@ < requested@,
})]
#[must_use]
pub fn diskless_logical_range(entries: &[(i64, i64)], requested: i64) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = entries.len();
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ <= requested@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len()
        ==> requested@ < entries@[i].0@)]
    #[variant(hi@ - lo@)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= requested {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        return None;
    }
    let floor = lo - 1;
    if requested <= entries[floor].1 {
        Some(floor)
    } else if lo < entries.len() {
        Some(lo)
    } else {
        None
    }
}

/// Extend an object byte span only across a contiguous whole indexed range.
#[ensures(match result {
    Some(total) => same_object
        && current_start@ + current_len@ == next_start@
        && total@ == current_len@ + next_len@
        && total@ <= max_bytes@,
    None => true,
})]
#[must_use]
pub fn diskless_span_extension(
    current_start: u64,
    current_len: u64,
    next_start: u64,
    next_len: u64,
    same_object: bool,
    max_bytes: u64,
) -> Option<u64> {
    if !same_object || current_start.checked_add(current_len) != Some(next_start) {
        return None;
    }
    let total = current_len.checked_add(next_len)?;
    (total <= max_bytes).then_some(total)
}

/// Classify one decoded batch without splitting it or overflowing coordinates.
#[ensures(match result {
    DisklessBatchStep::Skip(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && base_offset@ + last_offset_delta@ < floor@,
    DisklessBatchStep::Start(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && floor@ <= base_offset@ + last_offset_delta@,
    DisklessBatchStep::Continue(next) => match selected_start {
        Some(start) => start@ <= batch_start@
            && next@ == batch_start@ + encoded_len@
            && next@ - start@ <= max_bytes@,
        None => false,
    },
    DisklessBatchStep::Stop => match selected_start {
        Some(start) => start@ <= batch_start@
            && batch_start@ + encoded_len@ - start@ > max_bytes@,
        None => false,
    },
    DisklessBatchStep::Invalid => true,
})]
#[must_use]
pub fn diskless_batch_step(
    selected_start: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base_offset: i64,
    last_offset_delta: i32,
    floor: i64,
    max_bytes: usize,
) -> DisklessBatchStep {
    if encoded_len == 0 || last_offset_delta < 0 {
        return DisklessBatchStep::Invalid;
    }
    let Some(next) = batch_start.checked_add(encoded_len) else {
        return DisklessBatchStep::Invalid;
    };
    let Some(last_offset) = base_offset.checked_add(i64::from(last_offset_delta)) else {
        return DisklessBatchStep::Invalid;
    };
    if let Some(start) = selected_start {
        if start > batch_start {
            return DisklessBatchStep::Invalid;
        }
        if next - start > max_bytes {
            DisklessBatchStep::Stop
        } else {
            DisklessBatchStep::Continue(next)
        }
    } else if last_offset < floor {
        DisklessBatchStep::Skip(next)
    } else {
        DisklessBatchStep::Start(next)
    }
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

    #[test]
    fn cold_read_decisions_cover_boundaries_gaps_caps_and_limits() {
        let entries = [(0, 4), (7, 9), (12, 15)];
        assert2::assert!(diskless_logical_range(&entries, -1).is_none());
        assert2::assert!(diskless_logical_range(&entries, 0) == Some(0));
        assert2::assert!(diskless_logical_range(&entries, 5) == Some(1));
        assert2::assert!(diskless_logical_range(&entries, 15) == Some(2));
        assert2::assert!(diskless_logical_range(&entries, 16).is_none());

        assert2::assert!(diskless_span_extension(10, 5, 15, 7, true, 12) == Some(12));
        assert2::assert!(diskless_span_extension(10, 5, 16, 7, true, 12).is_none());
        assert2::assert!(diskless_span_extension(10, 5, 15, 7, false, 12).is_none());
        assert2::assert!(diskless_span_extension(u64::MAX, 1, 0, 1, true, 2).is_none());

        assert2::assert!(
            diskless_batch_step(None, 0, 10, 0, 0, 1, 5) == DisklessBatchStep::Skip(10)
        );
        assert2::assert!(
            diskless_batch_step(None, 10, 10, 1, 0, 1, 5) == DisklessBatchStep::Start(20)
        );
        assert2::assert!(
            diskless_batch_step(Some(10), 20, 10, 2, 0, 1, 20) == DisklessBatchStep::Continue(30)
        );
        assert2::assert!(
            diskless_batch_step(Some(10), 20, 10, 2, 0, 1, 19) == DisklessBatchStep::Stop
        );
        assert2::assert!(
            diskless_batch_step(None, usize::MAX, 1, 0, 0, 0, usize::MAX)
                == DisklessBatchStep::Invalid
        );
        assert2::assert!(
            diskless_batch_step(None, 0, 1, i64::MAX, 1, 0, usize::MAX)
                == DisklessBatchStep::Invalid
        );
    }
}
