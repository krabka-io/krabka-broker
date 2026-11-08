//! Commit-stamp range-index decisions.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, Seq, invariant, logic};
use creusot_std::prelude::{ensures, requires};

/// One inclusive offset range `[base, last]` of the commit-stamp index.
#[krabka_macros::model_type]
pub struct StampRange {
    /// The first offset the range covers.
    pub base: i64,
    /// The last offset the range covers.
    pub last: i64,
}

/// Inclusive ranges, each ordered and each ending before the next begins.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn sorted_disjoint(ranges: Seq<StampRange>) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < ranges.len() ==> ranges[i].base@ <= ranges[i].last@)
            && (forall<i: Int> 1 <= i && i < ranges.len()
                ==> ranges[i - 1].last@ < ranges[i].base@)
    }
}

/// Validate sorted inclusive ranges as individually ordered and pairwise
/// nonoverlapping.
#[ensures(result == sorted_disjoint(ranges@))]
#[must_use]
pub fn stamp_ranges_valid(ranges: &[StampRange]) -> bool {
    let mut index = 0usize;
    #[invariant(index@ <= ranges@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> ranges@[i].base@ <= ranges@[i].last@)]
    #[invariant(forall<i: Int> 1 <= i && i < index@
        ==> ranges@[i - 1].last@ < ranges@[i].base@)]
    #[variant(ranges@.len() - index@)]
    while index < ranges.len() {
        let Some(&range) = ranges.get(index) else {
            return false;
        };
        if range.base > range.last {
            return false;
        }
        if index > 0 {
            let Some(&previous) = ranges.get(index - 1) else {
                return false;
            };
            if previous.last >= range.base {
                return false;
            }
        }
        index += 1;
    }
    true
}

/// Return the sorted insertion position for one nonoverlapping range.
///
/// `ranges` must be sorted, individually ordered and pairwise nonoverlapping.
/// Use [`stamp_ranges_valid`] to check this precondition before calling.
/// Proof annotations do not validate inputs in ordinary Rust builds.
#[requires(sorted_disjoint(ranges@))]
#[ensures(match result {
    Some(index) => index@ <= ranges@.len()
        && new.base@ <= new.last@
        && (forall<i: Int> 0 <= i && i < index@ ==> ranges@[i].last@ < new.base@)
        && (index@ == ranges@.len() || new.last@ < ranges@[index@].base@),
    None => new.base@ > new.last@
        || (exists<i: Int> 0 <= i && i < ranges@.len()
            && ranges@[i].base@ <= new.last@ && new.base@ <= ranges@[i].last@),
})]
#[must_use]
pub fn stamp_range_insertion_index(ranges: &[StampRange], new: StampRange) -> Option<usize> {
    if new.base > new.last {
        return None;
    }
    let mut index = 0usize;
    #[invariant(index@ <= ranges@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> ranges@[i].last@ < new.base@)]
    #[variant(ranges@.len() - index@)]
    while index < ranges.len() {
        let &range = ranges.get(index)?;
        if new.last < range.base {
            return Some(index);
        }
        if new.base <= range.last {
            return None;
        }
        index += 1;
    }
    Some(index)
}

/// Find the first range with both of `target`'s exact inclusive boundaries.
#[ensures(stamp_lookup_result(result, ranges@, StampLookupQuery::Exact(target)))]
#[must_use]
pub fn exact_stamp_range_index(ranges: &[StampRange], target: StampRange) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(stamp_lookup_cursor_valid(ranges@, index@, StampLookupQuery::Exact(target)))]
    #[variant(ranges@.len() - index@)]
    while index < ranges.len() {
        let &range = ranges.get(index)?;
        if range.base == target.base && range.last == target.last {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Find the first inclusive range that covers `offset`.
#[ensures(stamp_lookup_result(result, ranges@, StampLookupQuery::Covering { offset }))]
#[must_use]
pub fn covering_stamp_range_index(ranges: &[StampRange], offset: i64) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(stamp_lookup_cursor_valid(ranges@, index@, StampLookupQuery::Covering { offset }))]
    #[variant(ranges@.len() - index@)]
    while index < ranges.len() {
        let &range = ranges.get(index)?;
        if range.base <= offset && offset <= range.last {
            return Some(index);
        }
        index += 1;
    }
    None
}

#[cfg(creusot)]
#[derive(Clone, Copy)]
pub(crate) enum StampLookupQuery {
    Exact(StampRange),
    Covering { offset: i64 },
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(crate) fn stamp_lookup_matches(query: StampLookupQuery, range: StampRange) -> bool {
    pearlite! { match query {
        StampLookupQuery::Exact(target) =>
            range.base@ == target.base@ && range.last@ == target.last@,
        StampLookupQuery::Covering { offset } => range.base@ <= offset@ && offset@ <= range.last@,
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(crate) fn no_stamp_lookup_before(
    ranges: Seq<StampRange>,
    count: Int,
    query: StampLookupQuery,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count ==> !stamp_lookup_matches(query, ranges[i]) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(crate) fn stamp_lookup_cursor_valid(
    ranges: Seq<StampRange>,
    index: Int,
    query: StampLookupQuery,
) -> bool {
    pearlite! { index <= ranges.len() && no_stamp_lookup_before(ranges, index, query) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(crate) fn stamp_lookup_result(
    result: Option<usize>,
    ranges: Seq<StampRange>,
    query: StampLookupQuery,
) -> bool {
    pearlite! { match result {
        Some(index) => index@ < ranges.len()
            && stamp_lookup_matches(query, ranges[index@])
            && no_stamp_lookup_before(ranges, index@, query),
        None => no_stamp_lookup_before(ranges, ranges.len(), query),
    } }
}

#[cfg(test)]
mod tests {
    use super::{
        StampRange, covering_stamp_range_index, exact_stamp_range_index,
        stamp_range_insertion_index, stamp_ranges_valid,
    };

    const fn range(base: i64, last: i64) -> StampRange { StampRange { base, last } }

    #[test]
    fn range_decisions_cover_boundaries_retries_and_malformed_inputs() {
        let ranges = [range(0, 2), range(5, 7), range(10, 12)];
        assert2::assert!(stamp_ranges_valid(&ranges));
        assert2::assert!(!stamp_ranges_valid(&[range(3, 2)]));
        assert2::assert!(!stamp_ranges_valid(&[range(0, 2), range(2, 4)]));
        assert2::assert!(stamp_range_insertion_index(&ranges, range(3, 4)) == Some(1));
        assert2::assert!(stamp_range_insertion_index(&ranges, range(8, 9)) == Some(2));
        assert2::assert!(stamp_range_insertion_index(&ranges, range(2, 3)).is_none());
        assert2::assert!(stamp_range_insertion_index(&ranges, range(4, 3)).is_none());
        assert2::assert!(exact_stamp_range_index(&ranges, range(5, 7)) == Some(1));
        assert2::assert!(exact_stamp_range_index(&ranges, range(5, 8)).is_none());
        assert2::assert!(covering_stamp_range_index(&ranges, 7) == Some(1));
        assert2::assert!(covering_stamp_range_index(&ranges, 8).is_none());
        assert2::assert!(stamp_ranges_valid(&[range(1, 1)]));
        assert2::assert!(stamp_range_insertion_index(&[range(5, 7)], range(1, 1)) == Some(0));
        assert2::assert!(stamp_range_insertion_index(&[range(5, 7)], range(10, 10)) == Some(1));
        assert2::assert!(stamp_range_insertion_index(&[range(5, 7)], range(5, 5)).is_none());
        assert2::assert!(stamp_range_insertion_index(&[range(5, 7)], range(4, 5)).is_none());
    }
}
