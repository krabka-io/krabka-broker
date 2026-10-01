use creusot_std::prelude::*;

use super::constructed_voter_reconfiguration;
#[cfg(creusot)]
use super::spec::{
    adjacent_majorities_exceed_union, expected_member, grant_count, has_node, membership_coherent,
    valid_old,
};
#[cfg(creusot)]
use crate::reconfiguration::voter_reconfiguration_rejection;
use crate::{
    consensus::election_has_quorum,
    reconfiguration::{
        CurrentVoterSet, ReconfigurationLeadership, TargetVoter, VoterChangeKind,
        VoterChangeRequest, VoterReconfigurationPlan,
    },
};

type QuorumOverlap = Option<(
    VoterReconfigurationPlan,
    Vec<u64>,
    (usize, usize),
    (bool, bool),
    Option<u64>,
)>;

/// Count unique grants over the old IDs plus one candidate slot. Removal
/// cannot count the removed voter for the next quorum; the candidate slot
/// counts only for an admitted add. If both majorities exist, return an actual
/// common voter. This is logical single-step overlap, not a durable Raft proof.
#[requires(context.voter_count@ == old@.len())]
#[requires(votes@.len() == old@.len() + 1)]
#[ensures((match result { None => false, Some(_) => true }) == (valid_old(old@)
    && membership_coherent(old@, node, target.membership, request.kind)
    && match voter_reconfiguration_rejection(leadership, context, request, target) {
        None => true, Some(_) => false,
    }))]
#[ensures(match result { None => true, Some((plan, next, counts, quorums, common)) =>
    next@.len() == plan.next_voter_count@ && next@.len() > 0
    && counts.0@ <= old@.len() && counts.1@ <= next@.len()
    && counts.0@ == grant_count(old@, next@, votes@, node, false, votes@.len())
    && counts.1@ == grant_count(old@, next@, votes@, node, true, votes@.len())
    && quorums.0 == (counts.0@ >= old@.len() / 2 + 1)
    && quorums.1 == (counts.1@ >= next@.len() / 2 + 1)
    && (quorums.0 && quorums.1 ==> match common { None => false, Some(_) => true })
    && (common == None) == !(exists<i: Int> 0 <= i && i < old@.len()
        && votes@[i].0 && votes@[i].1 && has_node(next@, next@.len(), old@[i]))
    && match common { None => true, Some(id) => exists<i: Int> 0 <= i && i < old@.len()
        && old@[i] == id && votes@[i].0 && votes@[i].1
        && exists<j: Int> 0 <= j && j < next@.len() && next@[j] == id },
})]
#[ensures(match result { None => true, Some((_, next, _, _, _)) =>
    old@.len() - 1 <= next@.len() && next@.len() <= old@.len() + 1
    && (forall<id: u64> has_node(next@, next@.len(), id)
        == expected_member(old@, old@.len(), request.kind, node, id))
    && (forall<i: Int, j: Int> 0 <= i && i < j && j < next@.len() ==> next@[i] != next@[j]),
})]
pub(crate) fn reconfigured_majorities_overlap(
    old: &[u64],
    leadership: ReconfigurationLeadership,
    context: CurrentVoterSet,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
    votes: &[(bool, bool)],
) -> QuorumOverlap {
    let (plan, next) =
        constructed_voter_reconfiguration(old, leadership, context, request, node, target)?;
    let add = matches!(request.kind, VoterChangeKind::Add);
    let remove = matches!(request.kind, VoterChangeKind::Remove);
    let mut counts = (0usize, 0usize);
    let mut common = None;
    let mut i = 0usize;
    #[invariant(i@ <= votes@.len())]
    #[invariant(counts.0@ <= i@.min(old@.len()))]
    #[invariant(counts.1@ <= i@.min(old@.len())
        - if remove && has_node(old@, i@.min(old@.len()), node) { 1 } else { 0 }
        + if add && i@ > old@.len() { 1 } else { 0 })]
    #[invariant(counts.0@ == grant_count(old@, next@, votes@, node, false, i@)
        && counts.1@ == grant_count(old@, next@, votes@, node, true, i@))]
    #[invariant(common == None ==> counts.0@ + counts.1@
        <= i@.min(old@.len()) + if add && i@ > old@.len() { 1 } else { 0 })]
    #[invariant(match common { None => true, Some(id) => exists<j: Int> 0 <= j && j < i@
        && j < old@.len() && old@[j] == id && votes@[j].0 && votes@[j].1
        && (!remove || id != node) })]
    #[invariant((common == None) == !(exists<j: Int> 0 <= j && j < i@ && j < old@.len()
        && votes@[j].0 && votes@[j].1 && has_node(next@, next@.len(), old@[j])))]
    #[variant(votes@.len() - i@)]
    while i < votes.len() {
        let old_member = i < old.len();
        let new_member = if old_member {
            !remove || old[i] != node
        } else {
            add
        };
        let old_grant = old_member && votes[i].0;
        let new_grant = new_member && votes[i].1;
        if old_grant {
            counts.0 += 1;
        }
        if new_grant {
            counts.1 += 1;
        }
        if old_grant && new_grant && common.is_none() {
            common = Some(old[i]);
        }
        i += 1;
    }
    proof_assert!({
        adjacent_majorities_exceed_union(old@.len(), next@.len());
        true
    });
    let quorums = (
        election_has_quorum(old.len(), counts.0),
        election_has_quorum(next.len(), counts.1),
    );
    Some((plan, next, counts, quorums, common))
}
