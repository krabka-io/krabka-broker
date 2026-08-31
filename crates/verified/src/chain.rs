//! Sequence and link continuity shared by append-only integrity chains.

use creusot_std::prelude::*;

/// Result of checking one chain link against the running position.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
pub enum ChainStep {
    /// The record did not carry the next expected sequence number.
    SequenceMismatch,
    /// The record did not link to the running chain head.
    HeadMismatch,
    /// The record is valid, but no later `u64` sequence number exists.
    Exhausted,
    /// The record is valid and the chain continues at this sequence number.
    Continue(u64),
}

/// Selects the eligible chain receipt with the greatest `(offset, sequence)`
/// rank, preserving the first receipt when ranks tie.
///
/// The host keeps opaque chain heads and decoded metadata outside Creusot. It
/// supplies only each receipt's ordering keys and whether that receipt is
/// eligible to continue the chain.
#[ensures(result == None ==>
    forall<i: Int> 0 <= i && i < candidates@.len() ==> !candidates@[i].2)]
#[ensures(match result {
    None => true,
    Some(index) => index@ < candidates@.len()
        && candidates@[index@].2
        && (forall<i: Int> 0 <= i && i < candidates@.len() && candidates@[i].2 ==>
            candidates@[i].0@ < candidates@[index@].0@
            || (candidates@[i].0@ == candidates@[index@].0@
                && candidates@[i].1@ <= candidates@[index@].1@))
        && (forall<i: Int> 0 <= i && i < index@ && candidates@[i].2 ==>
            candidates@[i].0@ < candidates@[index@].0@
            || (candidates@[i].0@ == candidates@[index@].0@
                && candidates@[i].1@ < candidates@[index@].1@)),
})]
#[must_use]
pub fn select_chain_tip(candidates: &[(i64, u64, bool)]) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= candidates@.len())]
    #[invariant(match best {
        None => forall<j: Int> 0 <= j && j < i@ ==> !candidates@[j].2,
        Some(index) => index@ < i@
            && candidates@[index@].2
            && (forall<j: Int> 0 <= j && j < i@ && candidates@[j].2 ==>
                candidates@[j].0@ < candidates@[index@].0@
                || (candidates@[j].0@ == candidates@[index@].0@
                    && candidates@[j].1@ <= candidates@[index@].1@))
            && (forall<j: Int> 0 <= j && j < index@ && candidates@[j].2 ==>
                candidates@[j].0@ < candidates@[index@].0@
                || (candidates@[j].0@ == candidates@[index@].0@
                    && candidates@[j].1@ < candidates@[index@].1@)),
    })]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        if candidates[i].2 {
            match best {
                None => best = Some(i),
                Some(current) => {
                    let newer = candidates[i].0 > candidates[current].0
                        || (candidates[i].0 == candidates[current].0
                            && candidates[i].1 > candidates[current].1);
                    if newer {
                        best = Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    best
}

/// Checks sequence and opaque-head continuity for one append-only chain link.
///
/// Hashing and signature verification stay in the host crates. They pass only
/// whether the decoded previous head equals the independently recomputed head.
#[ensures(actual_seq@ != expected_seq@ ==> result == ChainStep::SequenceMismatch)]
#[ensures(actual_seq@ == expected_seq@ && !head_matches
    ==> result == ChainStep::HeadMismatch)]
#[ensures(actual_seq@ == expected_seq@ && head_matches && expected_seq@ == u64::MAX@
    ==> result == ChainStep::Exhausted)]
#[ensures(actual_seq@ == expected_seq@ && head_matches && expected_seq@ < u64::MAX@
    ==> exists<next: u64> result == ChainStep::Continue(next)
        && next@ == expected_seq@ + 1)]
#[ensures(forall<next: u64> result == ChainStep::Continue(next)
    ==> actual_seq@ == expected_seq@ && head_matches && next@ == expected_seq@ + 1)]
#[must_use]
pub fn chain_step(expected_seq: u64, actual_seq: u64, head_matches: bool) -> ChainStep {
    if actual_seq != expected_seq {
        ChainStep::SequenceMismatch
    } else if !head_matches {
        ChainStep::HeadMismatch
    } else if expected_seq == u64::MAX {
        ChainStep::Exhausted
    } else {
        ChainStep::Continue(expected_seq + 1)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn chain_tip_uses_offset_then_sequence_and_ignores_ineligible_receipts() {
        let candidates = [
            (100, 7, true),
            (200, 1, false),
            (100, 9, true),
            (90, u64::MAX, true),
        ];
        assert!(select_chain_tip(&candidates) == Some(2));
        assert!(select_chain_tip(&[(0, 0, false)]) == None);
        assert!(select_chain_tip(&[(5, 3, true), (5, 3, true)]) == Some(0));
    }

    #[test]
    fn chain_step_is_exact_and_never_wraps() {
        assert!(chain_step(4, 5, true) == ChainStep::SequenceMismatch);
        assert!(chain_step(4, 4, false) == ChainStep::HeadMismatch);
        assert!(chain_step(4, 4, true) == ChainStep::Continue(5));
        assert!(chain_step(u64::MAX, u64::MAX, true) == ChainStep::Exhausted);
    }
}
