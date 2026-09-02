//! Inclusive interval integrity for remote aborted-transaction indexes.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Whether an aborted-transaction entry intersects a requested offset range.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum RemoteTxnOverlapDecision {
    /// At least one interval is inverted.
    Invalid,
    /// Both intervals are valid and do not intersect.
    Disjoint,
    /// Both intervals are valid and intersect inclusively.
    Overlap,
}

/// Classify the inclusive entry interval `[entry_start, entry_last]` against
/// the inclusive query interval `[query_from, query_to]`.
#[ensures(result == RemoteTxnOverlapDecision::Invalid
    == (entry_start@ > entry_last@ || query_from@ > query_to@))]
#[ensures(result == RemoteTxnOverlapDecision::Overlap
    == (entry_start@ <= entry_last@
        && query_from@ <= query_to@
        && entry_start@ <= query_to@
        && entry_last@ >= query_from@))]
#[ensures(result == RemoteTxnOverlapDecision::Disjoint
    == (entry_start@ <= entry_last@
        && query_from@ <= query_to@
        && (entry_start@ > query_to@ || entry_last@ < query_from@)))]
#[must_use]
pub fn remote_txn_overlap_decision(
    entry_start: i64,
    entry_last: i64,
    query_from: i64,
    query_to: i64,
) -> RemoteTxnOverlapDecision {
    if entry_start > entry_last || query_from > query_to {
        RemoteTxnOverlapDecision::Invalid
    } else if entry_start <= query_to && entry_last >= query_from {
        RemoteTxnOverlapDecision::Overlap
    } else {
        RemoteTxnOverlapDecision::Disjoint
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn overlap_is_inclusive_and_invalid_intervals_fail_closed() {
        use RemoteTxnOverlapDecision::{Disjoint, Invalid, Overlap};

        for (entry_start, entry_last, query_from, query_to, expected) in [
            (10, 14, 0, 9, Disjoint),
            (10, 14, 0, 10, Overlap),
            (10, 14, 14, 100, Overlap),
            (10, 14, 15, 100, Disjoint),
            (14, 10, 0, 100, Invalid),
            (10, 14, 100, 0, Invalid),
        ] {
            check!(
                remote_txn_overlap_decision(entry_start, entry_last, query_from, query_to)
                    == expected
            );
        }
    }
}
