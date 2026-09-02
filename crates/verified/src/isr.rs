//! Classic Kafka ISR high-watermark computation.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Ordered outcome of one controller-side ISR proposal.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum IsrAdmission {
    FencedLeaderEpoch,
    InvalidProposal,
    IneligibleReplica,
    Admit,
}

/// Apply Kafka's fail-closed ISR validation precedence.
#[ensures((result == IsrAdmission::FencedLeaderEpoch) == !leader_epoch_matches)]
#[ensures((result == IsrAdmission::InvalidProposal) ==
    (leader_epoch_matches && (!proposed_nonempty || !proposed_subset)))]
#[ensures((result == IsrAdmission::IneligibleReplica) ==
    (leader_epoch_matches && proposed_nonempty && proposed_subset && !replicas_eligible))]
#[ensures((result == IsrAdmission::Admit) ==
    (leader_epoch_matches && proposed_nonempty && proposed_subset && replicas_eligible))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies four independent ISR validation facts"
)]
#[must_use]
pub fn isr_admission(
    leader_epoch_matches: bool,
    proposed_nonempty: bool,
    proposed_subset: bool,
    replicas_eligible: bool,
) -> IsrAdmission {
    if !leader_epoch_matches {
        IsrAdmission::FencedLeaderEpoch
    } else if !proposed_nonempty || !proposed_subset {
        IsrAdmission::InvalidProposal
    } else if !replicas_eligible {
        IsrAdmission::IneligibleReplica
    } else {
        IsrAdmission::Admit
    }
}

/// Decide whether one unique assigned replica belongs in an ISR proposal.
#[ensures(result == (facts.0 && (
    facts.1
        || (facts.2 && facts.3)
        || (!facts.2 && facts.3 && facts.4)
)))]
#[must_use]
pub fn isr_maintenance_selected(facts: (bool, bool, bool, bool, bool)) -> bool {
    let (assigned, is_leader, in_isr, fetch_recent, caught_up_recent) = facts;
    assigned && (is_leader || fetch_recent && (in_isr || caught_up_recent))
}

/// Report a proposal only when at least one unique member is added or removed.
#[ensures(result == (removed@ > 0 || added@ > 0))]
#[must_use]
pub fn isr_proposal_changed(removed: usize, added: usize) -> bool {
    removed > 0 || added > 0
}

/// Return the minimum represented log-end offset across the leader and ISR.
#[ensures(result@ <= leader_leo@)]
#[ensures(forall<i: Int> 0 <= i && i < follower_leos@.len()
    ==> result@ <= follower_leos@[i]@)]
#[ensures(result@ == leader_leo@
    || exists<i: Int> 0 <= i && i < follower_leos@.len()
        && result@ == follower_leos@[i]@)]
#[ensures(follower_leos@.len() == 0 ==> result@ == leader_leo@)]
#[must_use]
pub fn isr_high_watermark(leader_leo: i64, follower_leos: &[i64]) -> i64 {
    let mut result = leader_leo;
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= follower_leos@.len()))]
    #[cfg_attr(creusot, invariant(result@ <= leader_leo@))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@
        ==> result@ <= follower_leos@[k]@))]
    #[cfg_attr(creusot, invariant(result@ == leader_leo@
        || exists<k: Int> 0 <= k && k < i@ && result@ == follower_leos@[k]@))]
    #[cfg_attr(creusot, variant(follower_leos@.len() - i@))]
    while i < follower_leos.len() {
        if follower_leos[i] < result {
            result = follower_leos[i];
        }
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isr_high_watermark_matches_iterator_oracle_at_boundaries() {
        let cases: &[(i64, &[i64])] = &[
            (42, &[]),
            (42, &[50]),
            (42, &[42, 7, 90]),
            (i64::MAX, &[i64::MIN, 0, i64::MAX]),
        ];
        for &(leader, followers) in cases {
            let expected = followers.iter().copied().fold(leader, i64::min);
            assert2::assert!((isr_high_watermark(leader, followers)) == (expected));
        }
    }

    #[test]
    fn isr_admission_is_ordered_and_fail_closed() {
        use IsrAdmission::{Admit, FencedLeaderEpoch, IneligibleReplica, InvalidProposal};

        for (epoch_matches, nonempty, subset, eligible, expected) in [
            (false, false, false, false, FencedLeaderEpoch),
            (true, false, true, true, InvalidProposal),
            (true, true, false, true, InvalidProposal),
            (true, true, true, false, IneligibleReplica),
            (true, true, true, true, Admit),
        ] {
            assert2::check!(isr_admission(epoch_matches, nonempty, subset, eligible) == expected);
        }
    }

    #[test]
    fn isr_maintenance_truth_table_retains_only_eligible_members() {
        for assigned in [false, true] {
            for leader in [false, true] {
                for current in [false, true] {
                    for fetch_recent in [false, true] {
                        for caught_up_recent in [false, true] {
                            let expected = assigned
                                && (leader || fetch_recent && (current || caught_up_recent));
                            assert2::check!(
                                isr_maintenance_selected((
                                    assigned,
                                    leader,
                                    current,
                                    fetch_recent,
                                    caught_up_recent,
                                )) == expected
                            );
                        }
                    }
                }
            }
        }
        assert2::check!(!isr_proposal_changed(0, 0));
        assert2::check!(isr_proposal_changed(1, 0));
        assert2::check!(isr_proposal_changed(0, 1));
        assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
    }
}
