//! Batch-offset continuity for offline restore verification.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

pub const RESTORE_SNAPSHOT_MISSING: u8 = 0;
pub const RESTORE_SNAPSHOT_LIVE: u8 = 1;
pub const RESTORE_SNAPSHOT_DELETE_STARTED: u8 = 2;
pub const RESTORE_SNAPSHOT_DELETE_FINISHED: u8 = 3;

/// Batch fate after folding the exact per-record selection results.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RestoreFilterDecision {
    Keep,
    Empty,
    Filter,
}

/// Keep one record exactly when it is inside both bounds and no exclusion
/// predicate matches. Offset bounds are inclusive; timestamp bounds are
/// exclusive.
#[ensures(result == (
    (!offset_bound_applies || offset@ <= offset_bound@)
        && (!timestamp_bound_applies || timestamp@ < timestamp_bound@)
        && !producer_excluded
        && !offset_excluded
        && !key_excluded
        && !header_excluded
))]
#[allow(
    clippy::fn_params_excessive_bools,
    clippy::too_many_arguments,
    reason = "each boolean is one independently computed restore predicate"
)]
#[must_use]
pub fn restore_record_selected(
    offset: i64,
    offset_bound_applies: bool,
    offset_bound: i64,
    timestamp: i64,
    timestamp_bound_applies: bool,
    timestamp_bound: i64,
    producer_excluded: bool,
    offset_excluded: bool,
    key_excluded: bool,
    header_excluded: bool,
) -> bool {
    (!offset_bound_applies || offset <= offset_bound)
        && (!timestamp_bound_applies || timestamp < timestamp_bound)
        && !producer_excluded
        && !offset_excluded
        && !key_excluded
        && !header_excluded
}

/// Fold whether the record walk saw at least one keep and one drop.
#[ensures(match result {
    RestoreFilterDecision::Keep => !saw_drop,
    RestoreFilterDecision::Empty => saw_drop && !saw_keep,
    RestoreFilterDecision::Filter => saw_keep && saw_drop,
})]
#[must_use]
pub fn restore_batch_filter_decision(saw_keep: bool, saw_drop: bool) -> RestoreFilterDecision {
    if saw_keep && saw_drop {
        RestoreFilterDecision::Filter
    } else if saw_drop {
        RestoreFilterDecision::Empty
    } else {
        RestoreFilterDecision::Keep
    }
}

/// A sorted batch stream may stop exactly when the next batch base is past an
/// applicable inclusive offset bound.
#[ensures(result == (offset_bound_applies && batch_base@ > offset_bound@))]
#[must_use]
pub fn restore_batch_past_offset_bound(
    batch_base: i64,
    offset_bound_applies: bool,
    offset_bound: i64,
) -> bool {
    offset_bound_applies && batch_base > offset_bound
}

/// Reconcile one archive-scan observation with one snapshot lifecycle state.
/// `Some(true)` keeps scanned bytes, `Some(false)` excludes absent or deleting
/// bytes, and `None` reports an explicit source disagreement.
#[ensures((result == Some(true)) ==
    (scanned && snapshot_state@ == RESTORE_SNAPSHOT_LIVE@))]
#[ensures((result == Some(false)) ==
    ((!scanned && snapshot_state@ <= RESTORE_SNAPSHOT_DELETE_FINISHED@
        && snapshot_state@ != RESTORE_SNAPSHOT_LIVE@)
        || (scanned && snapshot_state@ == RESTORE_SNAPSHOT_DELETE_STARTED@)))]
#[ensures((result == None) ==
    (snapshot_state@ > RESTORE_SNAPSHOT_DELETE_FINISHED@
        || (!scanned && snapshot_state@ == RESTORE_SNAPSHOT_LIVE@)
        || (scanned && (snapshot_state@ == RESTORE_SNAPSHOT_MISSING@
            || snapshot_state@ == RESTORE_SNAPSHOT_DELETE_FINISHED@))))]
#[must_use]
pub fn restore_archive_reconcile(scanned: bool, snapshot_state: u8) -> Option<bool> {
    if snapshot_state > RESTORE_SNAPSHOT_DELETE_FINISHED {
        return None;
    }
    if snapshot_state == RESTORE_SNAPSHOT_LIVE {
        return if scanned { Some(true) } else { None };
    }
    if scanned {
        if snapshot_state == RESTORE_SNAPSHOT_DELETE_STARTED {
            Some(false)
        } else {
            None
        }
    } else {
        Some(false)
    }
}

/// Validate one archived batch and return its exclusive next offset.
///
/// The caller supplies the minimum base offset the next batch may carry. Kafka
/// compaction preserves absolute offsets and may leave gaps, so a later base is
/// valid; overlap/regression, a negative span, and an exclusive end outside
/// `i64` fail closed.
#[ensures(match result {
    Some(next) => last_delta@ >= 0
        && base@ >= minimum_base@
        && next@ == base@ + last_delta@ + 1
        && next@ > base@
        && next@ <= i64::MAX@,
    None => last_delta@ < 0
        || base@ < minimum_base@
        || base@ + last_delta@ + 1 > i64::MAX@,
})]
#[must_use]
pub fn restore_batch_step(minimum_base: i64, base: i64, last_delta: i32) -> Option<i64> {
    if last_delta < 0 || base < minimum_base {
        return None;
    }

    let delta = i64::from(last_delta);
    if base > i64::MAX - delta - 1 {
        None
    } else {
        Some(base + delta + 1)
    }
}

/// Compute one record's absolute offset and timestamp within its batch.
///
/// A record must carry a non-negative offset delta no greater than the batch's
/// declared last offset delta. Both absolute coordinates must also fit in
/// `i64`; malformed ranges and either kind of overflow fail closed.
#[ensures(match result {
    Some((offset, timestamp)) => last_offset_delta@ >= 0
        && offset_delta@ >= 0
        && offset_delta@ <= last_offset_delta@
        && offset@ == base_offset@ + offset_delta@
        && offset@ >= i64::MIN@
        && offset@ <= i64::MAX@
        && timestamp@ == base_timestamp@ + timestamp_delta@
        && timestamp@ >= i64::MIN@
        && timestamp@ <= i64::MAX@,
    None => last_offset_delta@ < 0
        || offset_delta@ < 0
        || offset_delta@ > last_offset_delta@
        || base_offset@ + offset_delta@ > i64::MAX@
        || base_timestamp@ + timestamp_delta@ < i64::MIN@
        || base_timestamp@ + timestamp_delta@ > i64::MAX@,
})]
#[must_use]
pub fn restore_record_coordinates(
    base_offset: i64,
    last_offset_delta: i32,
    base_timestamp: i64,
    offset_delta: i32,
    timestamp_delta: i64,
) -> Option<(i64, i64)> {
    if last_offset_delta < 0 || offset_delta < 0 || offset_delta > last_offset_delta {
        return None;
    }

    let offset_delta = i64::from(offset_delta);
    if base_offset > i64::MAX - offset_delta {
        return None;
    }
    let offset = base_offset + offset_delta;

    let timestamp = if timestamp_delta >= 0 {
        if base_timestamp > i64::MAX - timestamp_delta {
            return None;
        }
        base_timestamp + timestamp_delta
    } else {
        if base_timestamp < i64::MIN - timestamp_delta {
            return None;
        }
        base_timestamp + timestamp_delta
    };

    Some((offset, timestamp))
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn archive_reconciliation_is_pointwise_and_fail_closed() {
        assert2::assert!(restore_archive_reconcile(true, RESTORE_SNAPSHOT_LIVE) == Some(true));
        assert2::assert!(
            restore_archive_reconcile(true, RESTORE_SNAPSHOT_DELETE_STARTED) == Some(false)
        );
        assert2::assert!(restore_archive_reconcile(true, RESTORE_SNAPSHOT_MISSING).is_none());
        assert2::assert!(
            restore_archive_reconcile(true, RESTORE_SNAPSHOT_DELETE_FINISHED).is_none()
        );
        assert2::assert!(restore_archive_reconcile(false, RESTORE_SNAPSHOT_LIVE).is_none());
        assert2::assert!(
            restore_archive_reconcile(false, RESTORE_SNAPSHOT_DELETE_STARTED) == Some(false)
        );
        assert2::assert!(
            restore_archive_reconcile(false, RESTORE_SNAPSHOT_DELETE_FINISHED) == Some(false)
        );
        assert2::assert!(restore_archive_reconcile(false, u8::MAX).is_none());
    }

    #[test]
    fn batch_step_is_contiguous_and_overflow_safe() {
        check!(restore_batch_step(10, 10, 2) == Some(13));
        check!(restore_batch_step(10, 11, 0) == Some(12));
        check!(restore_batch_step(10, 9, 0) == None);
        check!(restore_batch_step(10, 10, -1) == None);
        check!(restore_batch_step(i64::MAX, i64::MAX, 0) == None);
        check!(restore_batch_step(i64::MAX - 1, i64::MAX - 1, 0) == Some(i64::MAX));
    }

    #[test]
    fn record_coordinates_reject_invalid_ranges_and_overflow() {
        check!(restore_record_coordinates(10, 2, 100, 1, -5) == Some((11, 95)));
        check!(restore_record_coordinates(10, -1, 100, 0, 0) == None);
        check!(restore_record_coordinates(10, 2, 100, -1, 0) == None);
        check!(restore_record_coordinates(10, 2, 100, 3, 0) == None);
        check!(restore_record_coordinates(i64::MAX, 1, 100, 1, 0) == None);
        check!(restore_record_coordinates(10, 0, i64::MAX, 0, 1) == None);
        check!(restore_record_coordinates(10, 0, i64::MIN, 0, -1) == None);
        check!(
            restore_record_coordinates(i64::MAX, 0, i64::MAX, 0, 0) == Some((i64::MAX, i64::MAX))
        );
        check!(
            restore_record_coordinates(i64::MIN, 0, i64::MIN, 0, 0) == Some((i64::MIN, i64::MIN))
        );
    }

    #[test]
    fn record_selection_uses_inclusive_offset_and_exclusive_time_bounds() {
        let selected = |offset, timestamp| {
            restore_record_selected(
                offset, true, 10, timestamp, true, 100, false, false, false, false,
            )
        };
        check!(selected(10, 99));
        check!(!selected(11, 99));
        check!(!selected(10, 100));
        check!(!restore_record_selected(
            10, true, 10, 99, true, 100, true, false, false, false
        ));
        check!(restore_record_selected(
            i64::MIN,
            false,
            i64::MIN,
            i64::MAX,
            false,
            i64::MAX,
            false,
            false,
            false,
            false,
        ));
    }

    #[test]
    fn batch_fold_and_skip_are_exact_and_repeatable() {
        check!(restore_batch_filter_decision(false, false) == RestoreFilterDecision::Keep);
        check!(restore_batch_filter_decision(true, false) == RestoreFilterDecision::Keep);
        check!(restore_batch_filter_decision(false, true) == RestoreFilterDecision::Empty);
        check!(restore_batch_filter_decision(true, true) == RestoreFilterDecision::Filter);
        check!(!restore_batch_past_offset_bound(10, true, 10));
        check!(restore_batch_past_offset_bound(11, true, 10));
        check!(!restore_batch_past_offset_bound(i64::MAX, false, i64::MIN));
        check!(restore_batch_past_offset_bound(11, true, 10));
    }
}
