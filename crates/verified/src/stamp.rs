//! Commit-stamp range-index decisions.

#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};
use creusot_std::prelude::{ensures, requires};

/// Validate sorted inclusive ranges as individually ordered and pairwise
/// nonoverlapping.
#[ensures(result == (bases@.len() == lasts@.len()
    && (forall<i: Int> 0 <= i && i < bases@.len() ==> bases@[i]@ <= lasts@[i]@)
    && (forall<i: Int> 1 <= i && i < bases@.len() ==>
        lasts@[i - 1]@ < bases@[i]@)))]
#[must_use]
pub fn stamp_ranges_valid(bases: &[i64], lasts: &[i64]) -> bool {
    if bases.len() != lasts.len() {
        return false;
    }
    let mut index = 0usize;
    #[invariant(index@ <= bases@.len())]
    #[invariant(bases@.len() == lasts@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> bases@[i]@ <= lasts@[i]@)]
    #[invariant(forall<i: Int> 1 <= i && i < index@ ==> lasts@[i - 1]@ < bases@[i]@)]
    #[variant(bases@.len() - index@)]
    while index < bases.len() {
        if bases[index] > lasts[index] || (index > 0 && lasts[index - 1] >= bases[index]) {
            return false;
        }
        index += 1;
    }
    true
}

/// Return the sorted insertion position for one nonoverlapping range.
#[requires(bases@.len() == lasts@.len())]
#[requires(forall<i: Int> 0 <= i && i < bases@.len() ==> bases@[i]@ <= lasts@[i]@)]
#[requires(forall<i: Int> 1 <= i && i < bases@.len() ==>
    lasts@[i - 1]@ < bases@[i]@)]
#[ensures(match result {
    Some(index) => index@ <= bases@.len()
        && new_base@ <= new_last@
        && (forall<i: Int> 0 <= i && i < index@ ==> lasts@[i]@ < new_base@)
        && (index@ == bases@.len() || new_last@ < bases@[index@]@),
    None => new_base@ > new_last@
        || (exists<i: Int> 0 <= i && i < bases@.len()
            && bases@[i]@ <= new_last@ && new_base@ <= lasts@[i]@),
})]
#[must_use]
pub fn stamp_range_insertion_index(
    bases: &[i64],
    lasts: &[i64],
    new_base: i64,
    new_last: i64,
) -> Option<usize> {
    if new_base > new_last {
        return None;
    }
    let mut index = 0usize;
    #[invariant(index@ <= bases@.len())]
    #[invariant(bases@.len() == lasts@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> lasts@[i]@ < new_base@)]
    #[variant(bases@.len() - index@)]
    while index < bases.len() {
        if new_last < bases[index] {
            return Some(index);
        }
        if new_base <= lasts[index] {
            return None;
        }
        index += 1;
    }
    Some(index)
}

/// Find the first range with both exact inclusive boundaries.
#[requires(bases@.len() == lasts@.len())]
#[ensures(match result {
    Some(index) => index@ < bases@.len()
        && bases@[index@]@ == target_base@
        && lasts@[index@]@ == target_last@
        && (forall<i: Int> 0 <= i && i < index@ ==>
            bases@[i]@ != target_base@ || lasts@[i]@ != target_last@),
    None => forall<i: Int> 0 <= i && i < bases@.len() ==>
        bases@[i]@ != target_base@ || lasts@[i]@ != target_last@,
})]
#[must_use]
pub fn exact_stamp_range_index(
    bases: &[i64],
    lasts: &[i64],
    target_base: i64,
    target_last: i64,
) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(index@ <= bases@.len())]
    #[invariant(bases@.len() == lasts@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==>
        bases@[i]@ != target_base@ || lasts@[i]@ != target_last@)]
    #[variant(bases@.len() - index@)]
    while index < bases.len() {
        if bases[index] == target_base && lasts[index] == target_last {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Find the first inclusive range that covers `offset`.
#[requires(bases@.len() == lasts@.len())]
#[ensures(match result {
    Some(index) => index@ < bases@.len()
        && bases@[index@]@ <= offset@ && offset@ <= lasts@[index@]@
        && (forall<i: Int> 0 <= i && i < index@ ==>
            offset@ < bases@[i]@ || lasts@[i]@ < offset@),
    None => forall<i: Int> 0 <= i && i < bases@.len() ==>
        offset@ < bases@[i]@ || lasts@[i]@ < offset@,
})]
#[must_use]
pub fn covering_stamp_range_index(bases: &[i64], lasts: &[i64], offset: i64) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(index@ <= bases@.len())]
    #[invariant(bases@.len() == lasts@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==>
        offset@ < bases@[i]@ || lasts@[i]@ < offset@)]
    #[variant(bases@.len() - index@)]
    while index < bases.len() {
        if bases[index] <= offset && offset <= lasts[index] {
            return Some(index);
        }
        index += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        covering_stamp_range_index, exact_stamp_range_index, stamp_range_insertion_index,
        stamp_ranges_valid,
    };

    #[test]
    fn range_decisions_cover_boundaries_retries_and_malformed_inputs() {
        let bases = [0, 5, 10];
        let lasts = [2, 7, 12];
        assert2::assert!(stamp_ranges_valid(&bases, &lasts));
        assert2::assert!(!stamp_ranges_valid(&[0, 5], &[2]));
        assert2::assert!(!stamp_ranges_valid(&[3], &[2]));
        assert2::assert!(!stamp_ranges_valid(&[0, 2], &[2, 4]));
        assert2::assert!(stamp_range_insertion_index(&bases, &lasts, 3, 4) == Some(1));
        assert2::assert!(stamp_range_insertion_index(&bases, &lasts, 8, 9) == Some(2));
        assert2::assert!(stamp_range_insertion_index(&bases, &lasts, 2, 3).is_none());
        assert2::assert!(stamp_range_insertion_index(&bases, &lasts, 4, 3).is_none());
        assert2::assert!(exact_stamp_range_index(&bases, &lasts, 5, 7) == Some(1));
        assert2::assert!(exact_stamp_range_index(&bases, &lasts, 5, 8).is_none());
        assert2::assert!(covering_stamp_range_index(&bases, &lasts, 7) == Some(1));
        assert2::assert!(covering_stamp_range_index(&bases, &lasts, 8).is_none());
    }
}
