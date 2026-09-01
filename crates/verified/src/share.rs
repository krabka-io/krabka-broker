//! Share-group state pruning decisions.

use creusot_std::prelude::*;

/// Return the minimum latest-snapshot offset across every live share key.
#[ensures(match result {
    Some(frontier) => offsets@.len() > 0
        && (forall<i: Int> 0 <= i && i < offsets@.len()
            ==> frontier@ <= offsets@[i]@)
        && (exists<i: Int> 0 <= i && i < offsets@.len()
            && frontier@ == offsets@[i]@),
    None => offsets@.len() == 0,
})]
#[must_use]
#[allow(
    clippy::len_zero,
    reason = "the explicit length comparison is supported by Creusot v0.13"
)]
pub fn share_prune_frontier(offsets: &[i64]) -> Option<i64> {
    if offsets.len() == 0 {
        return None;
    }
    let mut frontier = offsets[0];
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= offsets@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==> frontier@ <= offsets@[k]@)]
    #[invariant(exists<k: Int> 0 <= k && k < i@ && frontier@ == offsets@[k]@)]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if offsets[i] < frontier {
            frontier = offsets[i];
        }
        i += 1;
    }
    Some(frontier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_frontier_covers_empty_single_ties_and_missing_snapshots() {
        for (offsets, expected) in [
            (&[][..], None),
            (&[42][..], Some(42)),
            (&[100, 30, 75, 30, 200][..], Some(30)),
            (&[0, 5, 9][..], Some(0)),
            (&[i64::MAX, i64::MIN, 0][..], Some(i64::MIN)),
        ] {
            assert2::check!(share_prune_frontier(offsets) == expected);
        }
    }
}
