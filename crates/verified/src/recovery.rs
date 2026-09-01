//! Controller recovery replay-bound decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReplayRecordDecision {
    Apply(i64),
    Skip,
}

#[ensures(match result {
    ReplayRecordDecision::Apply(offset) => record_delta@ >= 0
        && batch_base@ < end@
        && control_batch == replay_control
        && batch_base@ <= i64::MAX@ - record_delta@
        && offset@ == batch_base@ + record_delta@
        && from@ <= offset@
        && offset@ < end@,
    ReplayRecordDecision::Skip => !(record_delta@ >= 0
        && batch_base@ < end@
        && control_batch == replay_control
        && batch_base@ <= i64::MAX@ - record_delta@
        && from@ <= batch_base@ + record_delta@
        && batch_base@ + record_delta@ < end@),
})]
#[must_use]
pub fn replay_record_decision(
    batch_base: i64,
    record_delta: i32,
    from: i64,
    end: i64,
    control_batch: bool,
    replay_control: bool,
) -> ReplayRecordDecision {
    if record_delta < 0 || batch_base >= end || control_batch != replay_control {
        return ReplayRecordDecision::Skip;
    }
    let delta = i64::from(record_delta);
    if batch_base > i64::MAX - delta {
        return ReplayRecordDecision::Skip;
    }
    let offset = batch_base + delta;
    if offset < from || offset >= end {
        ReplayRecordDecision::Skip
    } else {
        ReplayRecordDecision::Apply(offset)
    }
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum ReplayCursorDecision {
    Advance(i64),
    Stop,
}

#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => next == Some(next_offset)
        && next_offset@ > cursor@,
    ReplayCursorDecision::Stop => match next {
        None => true,
        Some(next_offset) => next_offset@ <= cursor@,
    },
})]
#[must_use]
pub fn replay_cursor_decision(cursor: i64, next: Option<i64>) -> ReplayCursorDecision {
    match next {
        Some(next_offset) if next_offset > cursor => ReplayCursorDecision::Advance(next_offset),
        Some(_) | None => ReplayCursorDecision::Stop,
    }
}

/// Validate one decoded replay batch and compute its exclusive next cursor.
///
/// Compacted logs may contain forward gaps. Overlap, malformed spans, batches
/// outside the captured replay bound, and an unrepresentable successor stop
/// replay without advancing.
#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => match batch {
        Some((base, last_delta)) => cursor@ < end@
            && last_delta@ >= 0
            && base@ >= cursor@
            && next_offset@ == base@ + last_delta@ + 1
            && next_offset@ > cursor@
            && next_offset@ <= end@
            && next_offset@ <= i64::MAX@,
        None => false,
    },
    ReplayCursorDecision::Stop => match batch {
        None => true,
        Some((base, last_delta)) => cursor@ >= end@
            || last_delta@ < 0
            || base@ < cursor@
            || base@ + last_delta@ + 1 > i64::MAX@
            || base@ + last_delta@ + 1 > end@,
    },
})]
#[must_use]
pub fn replay_batch_cursor_decision(
    cursor: i64,
    end: i64,
    batch: Option<(i64, i32)>,
) -> ReplayCursorDecision {
    if cursor >= end {
        return ReplayCursorDecision::Stop;
    }
    let Some((base, last_delta)) = batch else {
        return ReplayCursorDecision::Stop;
    };
    let Some(next) = crate::restore::restore_batch_step(cursor, base, last_delta) else {
        return ReplayCursorDecision::Stop;
    };
    if next > end {
        ReplayCursorDecision::Stop
    } else {
        ReplayCursorDecision::Advance(next)
    }
}

#[ensures(result == (!pending_exists && is_downgrade))]
#[must_use]
pub const fn should_capture_first_downgrade(pending_exists: bool, is_downgrade: bool) -> bool {
    !pending_exists && is_downgrade
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_replay_is_bounded_and_type_separated() {
        use ReplayRecordDecision::{Apply, Skip};

        assert2::assert!(replay_record_decision(10, 1, 10, 12, false, false) == Apply(11));
        assert2::assert!(replay_record_decision(10, 2, 10, 12, false, false) == Skip);
        assert2::assert!(replay_record_decision(10, -1, 0, 12, false, false) == Skip);
        assert2::assert!(replay_record_decision(i64::MAX, 1, 0, i64::MAX, false, false) == Skip);
        assert2::assert!(replay_record_decision(10, 0, 10, 12, true, false) == Skip);
        assert2::assert!(replay_record_decision(10, 0, 10, 12, true, true) == Apply(10));
    }

    #[test]
    fn cursor_and_downgrade_decisions_are_fail_closed() {
        use ReplayCursorDecision::{Advance, Stop};

        assert2::assert!(replay_cursor_decision(10, Some(11)) == Advance(11));
        assert2::assert!(replay_cursor_decision(10, Some(10)) == Stop);
        assert2::assert!(replay_cursor_decision(10, Some(9)) == Stop);
        assert2::assert!(replay_cursor_decision(10, None) == Stop);
        assert2::assert!(should_capture_first_downgrade(false, true));
        assert2::assert!(!should_capture_first_downgrade(true, true));
    }

    #[test]
    fn batch_cursor_is_bounded_progress_or_stop() {
        use ReplayCursorDecision::{Advance, Stop};

        for (cursor, end, batch, expected) in [
            (10, 20, None, Stop),
            (10, 20, Some((10, 2)), Advance(13)),
            (10, 20, Some((15, 1)), Advance(17)),
            (10, 20, Some((9, 2)), Stop),
            (10, 20, Some((10, -1)), Stop),
            (10, 12, Some((10, 2)), Stop),
            (20, 20, Some((20, 0)), Stop),
            (
                i64::MAX - 1,
                i64::MAX,
                Some((i64::MAX - 1, 0)),
                Advance(i64::MAX),
            ),
            (i64::MAX - 1, i64::MAX, Some((i64::MAX - 1, 1)), Stop),
        ] {
            assert2::assert!(replay_batch_cursor_decision(cursor, end, batch) == expected);
        }
    }
}
