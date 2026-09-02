//! Uniform group-assignor candidate selection.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, invariant};

/// Select the first least-loaded candidate, preferring rack matches.
///
/// The host supplies candidates in ascending member-ID order. Every candidate
/// is already known to subscribe to the topic. The tuple contains the current
/// topic load and whether the member's rack hosts the partition.
#[ensures((result == None) == (candidates@.len() == 0))]
#[ensures(match result {
    None => true,
    Some(index) => index@ < candidates@.len()
        && (forall<j: Int> 0 <= j && j < candidates@.len()
            ==> !candidates@[j].1 || candidates@[index@].1)
        && (forall<j: Int> 0 <= j && j < candidates@.len()
            && candidates@[j].1 == candidates@[index@].1
            ==> candidates@[index@].0@ <= candidates@[j].0@)
        && (forall<j: Int> 0 <= j && j < index@
            && candidates@[j].1 == candidates@[index@].1
            ==> candidates@[index@].0@ < candidates@[j].0@),
})]
#[must_use]
pub fn select_uniform_member(candidates: &[(usize, bool)]) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= candidates@.len())]
    #[invariant(match best {
        None => i@ == 0,
        Some(index) => index@ < i@
            && (forall<j: Int> 0 <= j && j < i@
                ==> !candidates@[j].1 || candidates@[index@].1)
            && (forall<j: Int> 0 <= j && j < i@
                && candidates@[j].1 == candidates@[index@].1
                ==> candidates@[index@].0@ <= candidates@[j].0@)
            && (forall<j: Int> 0 <= j && j < index@
                && candidates@[j].1 == candidates@[index@].1
                ==> candidates@[index@].0@ < candidates@[j].0@),
    })]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        match best {
            None => best = Some(i),
            Some(current) => {
                let candidate = candidates[i];
                let current = candidates[current];
                if (candidate.1 && !current.1)
                    || (candidate.1 == current.1 && candidate.0 < current.0)
                {
                    best = Some(i);
                }
            }
        }
        i += 1;
    }
    best
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::select_uniform_member;

    #[test]
    fn selection_prefers_rack_then_load_then_first_member() {
        assert!(select_uniform_member(&[]) == None);
        assert!(select_uniform_member(&[(4, false), (1, false), (1, false)]) == Some(1));
        assert!(select_uniform_member(&[(0, false), (5, true), (2, true)]) == Some(2));
        assert!(select_uniform_member(&[(usize::MAX, true), (usize::MAX, true)]) == Some(0));
    }
}
