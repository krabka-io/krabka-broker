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
    fn chain_step_is_exact_and_never_wraps() {
        assert!(chain_step(4, 5, true) == ChainStep::SequenceMismatch);
        assert!(chain_step(4, 4, false) == ChainStep::HeadMismatch);
        assert!(chain_step(4, 4, true) == ChainStep::Continue(5));
        assert!(chain_step(u64::MAX, u64::MAX, true) == ChainStep::Exhausted);
    }
}
