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

/// Admit a batch header synthesized or re-encoded by restore and return its
/// exclusive offset frontier.
///
/// A data batch uses either Kafka's exact non-idempotent `-1` identity
/// sentinels or a nonnegative producer identity. A transaction control marker
/// has a nonnegative producer and the control-record `base_sequence` sentinel;
/// a non-transactional control batch has the exact barrier sentinels. The
/// retained record count is nonnegative and the archived offset span remains
/// representable.
#[ensures(match result {
    Some(frontier) => records_count@ >= 0
        && base_offset@ >= 0
        && last_offset_delta@ >= 0
        && (!control_batch || (last_offset_delta@ == 0 && records_count@ <= 1))
        && frontier@ == base_offset@ + last_offset_delta@ + 1
        && frontier@ > base_offset@
        && frontier@ <= i64::MAX@
        && (if control_batch {
            (transactional
                    && producer_id@ >= 0
                    && producer_epoch@ >= 0
                    && base_sequence@ == -1)
                || (!transactional
                    && producer_id@ == -1
                    && producer_epoch@ == -1
                    && base_sequence@ == -1)
        } else {
            (!transactional
                    && producer_id@ == -1
                    && producer_epoch@ == -1
                    && base_sequence@ == -1)
                || (producer_id@ >= 0 && producer_epoch@ >= 0 && base_sequence@ >= 0)
        }),
    None => records_count@ < 0
        || base_offset@ < 0
        || last_offset_delta@ < 0
        || (control_batch && (last_offset_delta@ != 0 || records_count@ > 1))
        || base_offset@ + last_offset_delta@ + 1 > i64::MAX@
        || !(if control_batch {
            (transactional
                    && producer_id@ >= 0
                    && producer_epoch@ >= 0
                    && base_sequence@ == -1)
                || (!transactional
                    && producer_id@ == -1
                    && producer_epoch@ == -1
                    && base_sequence@ == -1)
        } else {
            (!transactional
                    && producer_id@ == -1
                    && producer_epoch@ == -1
                    && base_sequence@ == -1)
                || (producer_id@ >= 0 && producer_epoch@ >= 0 && base_sequence@ >= 0)
        }),
})]
#[allow(
    clippy::fn_params_excessive_bools,
    clippy::too_many_arguments,
    reason = "the proof classifies the independent CRC-covered rewrite header fields"
)]
#[must_use]
pub fn restore_rewritten_batch_header(
    base_offset: i64,
    last_offset_delta: i32,
    records_count: i32,
    control_batch: bool,
    transactional: bool,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Option<i64> {
    let producer_valid = if control_batch {
        (transactional && producer_id >= 0 && producer_epoch >= 0 && base_sequence == -1)
            || (!transactional && producer_id == -1 && producer_epoch == -1 && base_sequence == -1)
    } else {
        (!transactional && producer_id == -1 && producer_epoch == -1 && base_sequence == -1)
            || (producer_id >= 0 && producer_epoch >= 0 && base_sequence >= 0)
    };
    if records_count < 0
        || base_offset < 0
        || last_offset_delta < 0
        || (control_batch && (last_offset_delta != 0 || records_count > 1))
        || !producer_valid
    {
        return None;
    }

    base_offset
        .checked_add(i64::from(last_offset_delta))?
        .checked_add(1)
}

/// Validate one retained record against the synthesized header.
///
/// Record deltas must be strictly increasing, stay inside the archived span,
/// and reconstruct an absolute timestamp without overflow. The preserved
/// archived `max_timestamp` remains an upper bound; it may be greater than the
/// retained maximum when the record that established it was filtered out.
#[ensures(match result {
    Some((offset, timestamp)) => last_offset_delta@ >= 0
        && offset_delta@ >= 0
        && offset_delta@ <= last_offset_delta@
        && match previous_offset_delta {
            Some(previous) => previous@ < offset_delta@,
            None => true,
        }
        && offset@ == base_offset@ + offset_delta@
        && offset@ >= i64::MIN@
        && offset@ <= i64::MAX@
        && timestamp@ == base_timestamp@ + timestamp_delta@
        && timestamp@ >= i64::MIN@
        && timestamp@ <= max_timestamp@,
    None => last_offset_delta@ < 0
        || offset_delta@ < 0
        || offset_delta@ > last_offset_delta@
        || match previous_offset_delta {
            Some(previous) => previous@ >= offset_delta@,
            None => false,
        }
        || base_offset@ + offset_delta@ > i64::MAX@
        || base_timestamp@ + timestamp_delta@ < i64::MIN@
        || base_timestamp@ + timestamp_delta@ > i64::MAX@
        || base_timestamp@ + timestamp_delta@ > max_timestamp@,
})]
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn restore_rewritten_record(
    previous_offset_delta: Option<i32>,
    base_offset: i64,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    offset_delta: i32,
    timestamp_delta: i64,
) -> Option<(i64, i64)> {
    if previous_offset_delta.is_some_and(|previous| previous >= offset_delta) {
        return None;
    }
    let coordinates = restore_record_coordinates(
        base_offset,
        last_offset_delta,
        base_timestamp,
        offset_delta,
        timestamp_delta,
    )?;
    if coordinates.1 > max_timestamp {
        None
    } else {
        Some(coordinates)
    }
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
    fn rewritten_headers_are_checked_and_have_legal_producer_semantics() {
        check!(restore_rewritten_batch_header(10, 2, 2, false, false, -1, -1, -1) == Some(13));
        check!(restore_rewritten_batch_header(10, 2, 0, false, true, 7, 2, 4) == Some(13));
        check!(restore_rewritten_batch_header(10, 0, 1, true, true, 7, 2, -1) == Some(11));
        check!(restore_rewritten_batch_header(10, 0, 1, true, false, -1, -1, -1) == Some(11));
        check!(restore_rewritten_batch_header(10, 0, 1, true, true, 7, 2, 4) == None);
        check!(restore_rewritten_batch_header(10, 0, 1, true, true, -1, -1, -1) == None);
        check!(restore_rewritten_batch_header(10, 2, -1, false, false, -1, -1, -1) == None);
        check!(restore_rewritten_batch_header(10, 2, 2, false, true, -1, -1, -1) == None);
        check!(restore_rewritten_batch_header(10, 2, 2, false, false, -1, 0, -1) == None);
        check!(restore_rewritten_batch_header(10, 2, 2, false, false, 7, -1, 0) == None);
        check!(restore_rewritten_batch_header(i64::MAX, 0, 0, false, false, -1, -1, -1) == None);
    }

    #[test]
    fn rewritten_records_are_strict_bounded_and_timestamp_checked() {
        check!(restore_rewritten_record(None, 10, 4, 100, 110, 1, 5) == Some((11, 105)));
        check!(restore_rewritten_record(Some(1), 10, 4, 100, 110, 4, 10) == Some((14, 110)));
        check!(restore_rewritten_record(Some(1), 10, 4, 100, 110, 1, 5) == None);
        check!(restore_rewritten_record(Some(3), 10, 4, 100, 110, 2, 5) == None);
        check!(restore_rewritten_record(None, 10, 4, 100, 110, 5, 5) == None);
        check!(restore_rewritten_record(None, 10, 4, 100, 104, 1, 5) == None);
        check!(restore_rewritten_record(None, 10, 4, i64::MAX, i64::MAX, 1, 1) == None);
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
