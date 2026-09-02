//! Timestamp-scan selection and progress kernels.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};

/// Select the first timestamp at or after `target`.
#[ensures(match result {
    Some(index) => index@ < timestamps@.len()
        && timestamps@[index@]@ >= target@
        && forall<i: Int> 0 <= i && i < index@ ==> timestamps@[i]@ < target@,
    None => forall<i: Int> 0 <= i && i < timestamps@.len() ==> timestamps@[i]@ < target@,
})]
#[must_use]
pub fn first_timestamp_index(timestamps: &[i64], target: i64) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(index@ <= timestamps@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> timestamps@[i]@ < target@)]
    #[variant(timestamps@.len() - index@)]
    while index < timestamps.len() {
        if timestamps[index] >= target {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Select the earliest occurrence of the maximum timestamp.
#[ensures(match result {
    Some(index) => index@ < timestamps@.len()
        && forall<i: Int> 0 <= i && i < timestamps@.len() ==>
            timestamps@[i]@ <= timestamps@[index@]@
        && forall<i: Int> 0 <= i && i < index@ ==>
            timestamps@[i]@ < timestamps@[index@]@,
    None => timestamps@.len() == 0,
})]
#[must_use]
pub fn earliest_max_timestamp_index(timestamps: &[i64]) -> Option<usize> {
    if matches!(timestamps.len(), 0) {
        return None;
    }
    let mut best = 0usize;
    let mut index = 1usize;
    #[invariant(best@ < index@)]
    #[invariant(index@ <= timestamps@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==>
        timestamps@[i]@ <= timestamps@[best@]@)]
    #[invariant(forall<i: Int> 0 <= i && i < best@ ==>
        timestamps@[i]@ < timestamps@[best@]@)]
    #[variant(timestamps@.len() - index@)]
    while index < timestamps.len() {
        if timestamps[index] > timestamps[best] {
            best = index;
        }
        index += 1;
    }
    Some(best)
}

/// Compute one record's absolute offset and timestamp without signed overflow.
#[ensures(match result {
    Some((offset, timestamp)) => offset@ == batch_base@ + offset_delta@
        && timestamp@ == batch_timestamp@ + timestamp_delta@,
    None => batch_base@ + offset_delta@ < i64::MIN@
        || batch_base@ + offset_delta@ > i64::MAX@
        || batch_timestamp@ + timestamp_delta@ < i64::MIN@
        || batch_timestamp@ + timestamp_delta@ > i64::MAX@,
})]
#[must_use]
pub fn timestamp_record_coordinates(
    batch_base: i64,
    offset_delta: i32,
    batch_timestamp: i64,
    timestamp_delta: i64,
) -> Option<(i64, i64)> {
    Some((
        batch_base.checked_add(i64::from(offset_delta))?,
        batch_timestamp.checked_add(timestamp_delta)?,
    ))
}

/// Advance strictly past a decoded batch, rejecting malformed or stale bounds.
#[ensures(match result {
    Some(next) => next@ > cursor@
        && next@ == batch_base@ + last_offset_delta@ + 1,
    None => last_offset_delta@ < 0
        || batch_base@ + last_offset_delta@ + 1 > i64::MAX@
        || batch_base@ + last_offset_delta@ < cursor@,
})]
#[must_use]
pub fn timestamp_scan_next(cursor: i64, batch_base: i64, last_offset_delta: i32) -> Option<i64> {
    if last_offset_delta < 0 {
        return None;
    }
    let last = batch_base.checked_add(i64::from(last_offset_delta))?;
    if last < cursor {
        return None;
    }
    last.checked_add(1)
}

/// Double a nonzero scan window with checked arithmetic.
#[ensures(match result {
    Some(next) => next@ == window@ * 2 && next@ > window@,
    None => window@ == 0 || window@ * 2 > u32::MAX@,
})]
#[must_use]
pub fn timestamp_scan_window(window: u32) -> Option<u32> {
    if window == 0 {
        return None;
    }
    window.checked_mul(2)
}

#[cfg(test)]
mod tests {
    use super::{
        earliest_max_timestamp_index, first_timestamp_index, timestamp_record_coordinates,
        timestamp_scan_next, timestamp_scan_window,
    };

    #[test]
    fn selection_progress_and_overflow_are_fail_closed() {
        let timestamps = [5, 9, 9, 3];
        assert2::assert!(first_timestamp_index(&timestamps, 9) == Some(1));
        assert2::assert!(first_timestamp_index(&timestamps, 10).is_none());
        assert2::assert!(earliest_max_timestamp_index(&timestamps) == Some(1));
        assert2::assert!(earliest_max_timestamp_index(&[]).is_none());
        assert2::assert!(timestamp_record_coordinates(10, 2, 100, 3) == Some((12, 103)));
        assert2::assert!(timestamp_record_coordinates(i64::MAX, 1, 0, 0).is_none());
        assert2::assert!(timestamp_record_coordinates(0, 0, i64::MAX, 1).is_none());
        assert2::assert!(timestamp_scan_next(10, 8, 2) == Some(11));
        assert2::assert!(timestamp_scan_next(10, 8, 1).is_none());
        assert2::assert!(timestamp_scan_next(i64::MAX, i64::MAX, 0).is_none());
        assert2::assert!(timestamp_scan_window(1) == Some(2));
        assert2::assert!(timestamp_scan_window(u32::MAX).is_none());
    }
}
