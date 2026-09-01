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
}
