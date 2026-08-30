//! Sparse-index lookup kernels.
//!
//! These kernels come out of the `OffsetIndex` and `TimeIndex` of `krabka-log`,
//! so that Creusot can verify them. They use hand-rolled binary searches, the
//! canonical Creusot loop, and not `binary_search_by_key`. The proofs thus do
//! not depend on a model of the std search.

use creusot_std::prelude::*;

/// The byte position to start reading at for `target`.
///
/// This is the position field of the largest entry with
/// `relative_offset <= target`, or 0 if no such entry exists. `entries` must be
/// strictly sorted by relative offset, which the construction of `OffsetIndex`
/// guarantees.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ <= target@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ <= target@
        && result@ == entries@[i].1@
        && (forall<j: Int> i < j && j < entries@.len() ==> entries@[j].0@ > target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ > target@)
    ==> result@ == 0)]
#[must_use]
pub fn offset_index_lookup(entries: &[(u32, u32)], target: u32) -> u32 {
    let mut lo = 0usize; // entries[..lo] all have rel <= target
    let mut hi = entries.len(); // entries[hi..] all have rel > target
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ <= target@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len() ==> entries@[i].0@ > target@)]
    #[variant(hi - lo)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 { 0 } else { entries[lo - 1].1 }
}

/// The relative offset to start reading at for `target_timestamp`.
///
/// This is the offset field of the last entry with
/// `timestamp <= target_timestamp`, or 0 if no such entry exists. `entries`
/// must be sorted by timestamp; equal timestamps are allowed.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ <= entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ <= target_timestamp@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ <= target_timestamp@
        && result@ == entries@[i].1@
        && (forall<j: Int> i < j && j < entries@.len()
            ==> entries@[j].0@ > target_timestamp@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len()
        ==> entries@[i].0@ > target_timestamp@)
    ==> result@ == 0)]
#[must_use]
pub fn time_index_lookup(entries: &[(i64, u32)], target_timestamp: i64) -> u32 {
    let mut lo = 0usize; // entries[..lo] all have timestamp <= target
    let mut hi = entries.len(); // entries[hi..] all have timestamp > target
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@
        ==> entries@[i].0@ <= target_timestamp@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len()
        ==> entries@[i].0@ > target_timestamp@)]
    #[variant(hi - lo)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= target_timestamp {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 { 0 } else { entries[lo - 1].1 }
}

/// The byte position of the first entry at or after `target`.
///
/// Returns `None` when every entry is below `target`. `entries` must be
/// strictly sorted by relative offset.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && target@ <= entries@[i].0@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && target@ <= entries@[i].0@
        && result == Some(entries@[i].1)
        && (forall<j: Int> 0 <= j && j < i ==> entries@[j].0@ < target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ < target@)
    ==> result == None)]
#[must_use]
pub fn offset_index_position_at_or_after(entries: &[(u32, u32)], target: u32) -> Option<u32> {
    let mut lo = 0usize; // entries[..lo] all have rel < target
    let mut hi = entries.len(); // entries[hi..] all have rel >= target
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ < target@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len() ==> target@ <= entries@[i].0@)]
    #[variant(hi - lo)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == entries.len() {
        None
    } else {
        Some(entries[lo].1)
    }
}

#[cfg(test)]
mod tests {

    use proptest::prelude::*;

    use super::*;

    fn offset_floor_oracle(entries: &[(u32, u32)], target: u32) -> u32 {
        match entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => entries[i].1,
            Err(0) => 0,
            Err(i) => entries[i - 1].1,
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_binary_search_oracle(
            rels in proptest::collection::btree_set(0u32..10_000, 0..64),
            target in 0u32..10_000,
        ) {
            // btree_set gives strictly-sorted unique keys, matching the
            // OffsetIndex construction invariant.
            let entries: Vec<(u32, u32)> = rels
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    (
                        *r,
                        u32::try_from(i).expect("btree set length is bounded to 64") * 17,
                    )
                })
                .collect();
            prop_assert_eq!(offset_index_lookup(&entries, target), offset_floor_oracle(&entries, target));
        }

        #[test]
        fn time_lookup_matches_linear_oracle(
            timestamps in proptest::collection::vec(-10_000i64..10_000, 0..64),
            target in -10_000i64..10_000,
        ) {
            let mut timestamps = timestamps;
            timestamps.sort_unstable();
            let entries: Vec<(i64, u32)> = timestamps
                .iter()
                .enumerate()
                .map(|(i, timestamp)| {
                    (
                        *timestamp,
                        u32::try_from(i).expect("timestamp list length is bounded to 64") * 17,
                    )
                })
                .collect();
            let want = entries
                .iter()
                .rfind(|&&(timestamp, _)| timestamp <= target)
                .map_or(0, |&(_, offset)| offset);
            prop_assert_eq!(time_index_lookup(&entries, target), want);
        }

        #[test]
        fn position_at_or_after_matches_linear_oracle(
            rels in proptest::collection::btree_set(0u32..10_000, 0..64),
            target in 0u32..10_000,
        ) {
            let entries: Vec<(u32, u32)> = rels
                .iter()
                .enumerate()
                .map(|(i, rel)| {
                    (
                        *rel,
                        u32::try_from(i).expect("btree set length is bounded to 64") * 17,
                    )
                })
                .collect();
            let want = entries
                .iter()
                .find(|&&(rel, _)| rel >= target)
                .map(|&(_, position)| position);
            prop_assert_eq!(offset_index_position_at_or_after(&entries, target), want);
        }
    }

    #[test]
    fn empty_indexes_return_fallbacks() {
        assert2::assert!(offset_index_lookup(&[], 42) == 0);
        assert2::assert!(time_index_lookup(&[], 42) == 0);
        assert2::assert!(offset_index_position_at_or_after(&[], 42) == None);
    }
}
