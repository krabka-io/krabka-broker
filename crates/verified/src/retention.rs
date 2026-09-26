//! Contiguous retention-prefix selection.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Decide whether one held barrier cut falls outside the retained epoch
/// window.
///
/// A nonpositive retention count and an unrepresentable cutoff both fail
/// closed by expiring nothing.
#[ensures(result == (retained_cuts@ > 0
    && published_epoch@ - retained_cuts@ >= i64::MIN@
    && held_epoch@ <= published_epoch@ - retained_cuts@))]
#[must_use]
pub fn barrier_cut_expired(published_epoch: i64, retained_cuts: i32, held_epoch: i64) -> bool {
    if retained_cuts <= 0 {
        return false;
    }
    let retained_cuts = i64::from(retained_cuts);
    if published_epoch < i64::MIN + retained_cuts {
        return false;
    }
    held_epoch <= published_epoch - retained_cuts
}

#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct RetentionPrefix {
    pub len: usize,
    pub remaining_size_debt: u64,
}

/// Select the oldest contiguous local-log prefix allowed by age or size
/// retention while preserving scheduled data and at least one segment.
///
/// A segment is size-evictable only while the remaining excess covers the
/// whole segment (`remaining_size_debt >= sizes[i]`), matching Kafka's
/// `deleteRetentionSizeBreachedSegments` predicate `diff - segment.size() >=
/// 0`. A segment larger than the remaining excess stops the size pass, even
/// though it is heavier than the log is allowed to be over budget by.
#[requires(time_expired@.len() == scheduled@.len())]
#[requires(time_expired@.len() == sizes@.len())]
#[ensures(result.len@ <= time_expired@.len())]
#[ensures(!has_active && time_expired@.len() > 0 ==>
    result.len@ < time_expired@.len())]
#[ensures(result.remaining_size_debt@ <= initial_size_debt@)]
#[ensures(forall<i: Int> 0 <= i && i < result.len@ ==> !scheduled@[i])]
#[ensures(initial_size_debt@ == 0 ==>
    forall<i: Int> 0 <= i && i < result.len@ ==> time_expired@[i])]
#[ensures(time_expired@.len() > 0
    && (has_active || time_expired@.len() > 1)
    && !scheduled@[0]
    && (time_expired@[0] || initial_size_debt@ >= sizes@[0]@)
    ==> result.len@ > 0)]
#[must_use]
pub fn local_retention_prefix(
    time_expired: &[bool],
    scheduled: &[bool],
    sizes: &[u64],
    initial_size_debt: u64,
    has_active: bool,
) -> RetentionPrefix {
    if matches!(time_expired.len(), 0) {
        return RetentionPrefix {
            len: 0,
            remaining_size_debt: initial_size_debt,
        };
    }
    let max_delete = if has_active {
        time_expired.len()
    } else {
        time_expired.len() - 1
    };
    // Segment 0 is peeled out of the loop below rather than folded into its
    // first iteration. The `ensures` above ties `result.len@ > 0` to concrete
    // facts about index 0 (`time_expired@[0]`, `sizes@[0]@`), and a generic
    // loop invariant -- which must hold uniformly at every iteration,
    // including before any of them run -- cannot also assert "the first
    // iteration always makes progress" without this explicit case split.
    // Kafka's size predicate (see the doc comment above) applies identically
    // here: segment 0 is size-evictable only while
    // `initial_size_debt >= sizes[0]`.
    if max_delete == 0 || scheduled[0] || (!time_expired[0] && initial_size_debt < sizes[0]) {
        return RetentionPrefix {
            len: 0,
            remaining_size_debt: initial_size_debt,
        };
    }
    let mut len = 1usize;
    let mut remaining_size_debt = initial_size_debt;
    if remaining_size_debt > 0 {
        remaining_size_debt = remaining_size_debt.saturating_sub(sizes[0]);
    }
    #[invariant(0 < len@ && len@ <= max_delete@)]
    #[invariant(max_delete@ <= time_expired@.len())]
    #[invariant(remaining_size_debt@ <= initial_size_debt@)]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==> !scheduled@[i])]
    #[invariant(initial_size_debt@ == 0 ==>
        forall<i: Int> 0 <= i && i < len@ ==> time_expired@[i])]
    #[variant(max_delete@ - len@)]
    while len < max_delete {
        if scheduled[len] {
            break;
        }
        // Kafka's size predicate: this segment is size-evictable only while
        // the remaining excess is at least its whole size. Falling one byte
        // short of a segment's size stops the size pass at that segment,
        // the same as running out of excess entirely.
        let size_evict = remaining_size_debt >= sizes[len];
        if !time_expired[len] && !size_evict {
            break;
        }
        if remaining_size_debt > 0 {
            remaining_size_debt = remaining_size_debt.saturating_sub(sizes[len]);
        }
        len += 1;
    }
    RetentionPrefix {
        len,
        remaining_size_debt,
    }
}

#[requires(finished@.len() == time_expired@.len())]
#[requires(finished@.len() == sizes@.len())]
#[ensures(result.len@ <= finished@.len())]
#[ensures(result.remaining_size_debt@ <= initial_size_debt@)]
#[ensures(!mutable ==> result.len@ == 0
    && result.remaining_size_debt@ == initial_size_debt@)]
#[ensures(forall<i: Int> 0 <= i && i < result.len@ ==> finished@[i])]
#[ensures(initial_size_debt@ == 0 ==>
    forall<i: Int> 0 <= i && i < result.len@ ==> time_expired@[i])]
#[ensures(mutable && result.len@ < finished@.len() ==>
    !finished@[result.len@]
        || (!time_expired@[result.len@] && result.remaining_size_debt@ == 0))]
#[must_use]
pub fn retention_prefix(
    mutable: bool,
    finished: &[bool],
    time_expired: &[bool],
    sizes: &[u64],
    initial_size_debt: u64,
) -> RetentionPrefix {
    if !mutable {
        return RetentionPrefix {
            len: 0,
            remaining_size_debt: initial_size_debt,
        };
    }
    let mut len = 0usize;
    let mut remaining_size_debt = initial_size_debt;
    #[invariant(len@ <= finished@.len())]
    #[invariant(remaining_size_debt@ <= initial_size_debt@)]
    #[invariant(forall<i: Int> 0 <= i && i < len@ ==> finished@[i])]
    #[invariant(initial_size_debt@ == 0 ==>
        forall<i: Int> 0 <= i && i < len@ ==> time_expired@[i])]
    #[variant(finished@.len() - len@)]
    while len < finished.len() {
        if !finished[len] || (!time_expired[len] && remaining_size_debt == 0) {
            break;
        }
        if remaining_size_debt > 0 {
            remaining_size_debt = remaining_size_debt.saturating_sub(sizes[len]);
        }
        len += 1;
    }
    RetentionPrefix {
        len,
        remaining_size_debt,
    }
}

#[ensures(last_offset == None ==> result == None)]
#[ensures(match last_offset {
    None => true,
    Some(last) => last@ == i64::MAX@ ==> result == None,
})]
#[ensures(match last_offset {
    None => true,
    Some(last) => last@ < i64::MAX@ ==> result != None,
})]
#[ensures(match (last_offset, result) {
    (Some(last), Some(target)) => target@ == last@ + 1,
    _ => true,
})]
#[must_use]
pub fn retention_delete_target(last_offset: Option<i64>) -> Option<i64> {
    match last_offset {
        Some(last) if last < i64::MAX => Some(last + 1),
        Some(_) | None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_cut_expiry_is_exact_and_fails_closed_at_extremes() {
        for (published, retained, held, expected) in [
            (10, 3, 6, true),
            (10, 3, 7, true),
            (10, 3, 8, false),
            (10, 0, i64::MIN, false),
            (10, -1, i64::MIN, false),
            (i64::MIN, 1, i64::MIN, false),
            (i64::MIN + 1, 1, i64::MIN, true),
            (i64::MAX, i32::MAX, i64::MAX, false),
        ] {
            assert2::check!(barrier_cut_expired(published, retained, held) == expected);
        }
    }

    #[test]
    fn retention_selection_is_a_safe_contiguous_prefix() {
        assert2::assert!(
            retention_prefix(
                true,
                &[true, true, true],
                &[true, false, true],
                &[10, 10, 10],
                0
            ) == RetentionPrefix {
                len: 1,
                remaining_size_debt: 0,
            }
        );
        assert2::assert!(
            retention_prefix(true, &[true, true, true], &[false; 3], &[10, 10, 10], 15)
                == RetentionPrefix {
                    len: 2,
                    remaining_size_debt: 0,
                }
        );
        assert2::assert!(retention_prefix(false, &[true], &[true], &[1], 1).len == 0);
        assert2::assert!(
            retention_prefix(true, &[true, false, true], &[true; 3], &[1; 3], 0).len == 1
        );
        assert2::assert!(
            retention_prefix(true, &[true, true], &[true, true], &[10, 10], 0).len == 2
        );
    }

    #[test]
    fn local_selection_preserves_scheduled_data_and_the_final_segment() {
        assert2::assert!(
            local_retention_prefix(&[true, true], &[false, false], &[10, 10], 0, true,).len == 2
        );
        // Segment 0 is time-expired and evicted outright, which knocks its
        // size off the remaining excess (15 - 10 = 5). Segment 1 is not
        // time-expired, and 5 does not cover its whole 10-byte size, so the
        // size pass stops there rather than deleting past the budget.
        assert2::assert!(
            local_retention_prefix(
                &[true, false, true, true],
                &[false, false, true, false],
                &[10, 10, 10, 10],
                15,
                true,
            ) == RetentionPrefix {
                len: 1,
                remaining_size_debt: 5,
            }
        );
        assert2::assert!(local_retention_prefix(&[true], &[false], &[1], 1, false).len == 0);
        assert2::assert!(
            local_retention_prefix(
                &[false, false],
                &[false, false],
                &[u64::MAX, 1],
                u64::MAX,
                true
            ) == RetentionPrefix {
                len: 1,
                remaining_size_debt: 0,
            }
        );
        assert2::assert!(
            local_retention_prefix(&[true, true], &[true, false], &[1, 1], 2, true).len == 0
        );
    }

    /// Kafka's `deleteRetentionSizeBreachedSegments` predicate is
    /// `diff - segment.size() >= 0`: a segment is deleted only while the
    /// remaining excess covers it whole. Three 60-byte segments over a
    /// budget that leaves an excess of exactly one segment, one byte short,
    /// and one byte over.
    #[test]
    fn size_retention_deletes_only_what_the_remaining_excess_covers() {
        let sizes = [60u64, 60, 60];
        let scheduled = [false, false, false];
        let not_time_expired = [false, false, false];

        // (excess, expected evicted count).
        let cases: [(u64, usize); 3] = [
            (60, 1), // excess is exactly one segment: delete it, keep 120.
            (59, 0), // one byte short: the first segment does not fit.
            (61, 1), // one byte over: still only the first segment fits.
        ];
        for (excess, expected) in cases {
            let prefix =
                local_retention_prefix(&not_time_expired, &scheduled, &sizes, excess, true);
            assert2::check!(prefix.len == expected, "excess={excess}: got {prefix:?}");
        }
    }

    #[test]
    fn delete_target_rejects_offset_exhaustion() {
        assert2::assert!(retention_delete_target(None) == None);
        assert2::assert!(retention_delete_target(Some(9)) == Some(10));
        assert2::assert!(retention_delete_target(Some(i64::MAX)) == None);
    }
}
