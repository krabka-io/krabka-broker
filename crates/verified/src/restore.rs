//! Batch-offset continuity for offline restore verification.

use creusot_std::prelude::*;

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
}
