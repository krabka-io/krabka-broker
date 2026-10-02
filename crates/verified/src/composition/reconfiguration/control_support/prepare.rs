use creusot_std::prelude::*;

use super::super::{constructed_voter_reconfiguration, reconfigured_majorities_overlap};
#[cfg(creusot)]
use super::{
    super::spec::{expected_member, has_node, membership_coherent, valid_old},
    spec::{control_record_count, next_size, prefix_count, prefix_grants_agree},
};
#[cfg(creusot)]
use crate::reconfiguration::{admitted_plan, voter_reconfiguration_rejection};
use crate::{
    raft::metadata_record_offset_deltas,
    reconfiguration::{
        CurrentVoterSet, ReconfigurationLeadership, TargetVoter, VoterChangeRequest,
        VoterReconfigurationPlan,
    },
    storage::local_append_coordinates,
};

type SupportedControl = Option<(
    VoterReconfigurationPlan,
    Vec<u64>,
    Vec<i32>,
    Option<(i64, (usize, usize), u64)>,
)>;

/// Construct real control-record deltas from the admitted plan and derive
/// old/new support from each node's reported prefix, never a supplied grant
/// count. Preflight writes nothing and needs no append support. Reported
/// prefixes, common log identity, directory/epoch facts and actual control
/// encoding remain host obligations. `KRaft` Fetch positions do not prove fsync.
#[requires(state.1.voter_count@ == old@.len())]
#[requires(reports@.len() == old@.len() + 1)]
#[ensures((match result { None => false, Some(_) => true }) == (valid_old(old@)
    && membership_coherent(old@, node, target.membership, request.kind)
    && match voter_reconfiguration_rejection(state.0, state.1, request, target) {
        Some(_) => false, None => true,
    }
    && (control_record_count(state.1.kraft_version, request.kind) == 0
        || (base@ >= 0 && base@ + control_record_count(state.1.kraft_version, request.kind) <= i64::MAX@
            && prefix_count(old@, reports@, request.kind, node,
                base@ + control_record_count(state.1.kraft_version, request.kind), false, reports@.len())
                >= old@.len() / 2 + 1
            && prefix_count(old@, reports@, request.kind, node,
                base@ + control_record_count(state.1.kraft_version, request.kind), true, reports@.len())
                >= next_size(old@.len(), request.kind) / 2 + 1))))]
#[ensures(match result { None => true, Some((plan, next, deltas, support)) =>
    admitted_plan(state.1, request.kind, plan)
    && next@.len() == plan.next_voter_count@ && next@.len() > 0
    && (forall<id: u64> has_node(next@, next@.len(), id)
        == expected_member(old@, old@.len(), request.kind, node, id))
    && (forall<i: Int, j: Int> 0 <= i && i < j && j < next@.len() ==> next@[i] != next@[j])
    && deltas@.len() == control_record_count(state.1.kraft_version, request.kind)
    && (forall<i: Int> 0 <= i && i < deltas@.len() ==> deltas@[i]@ == i)
    && (support == None) == (deltas@.len() == 0)
    && match support { None => plan.preflight_only,
        Some((end, counts, common)) => !plan.preflight_only && base@ >= 0
            && end@ == base@ + deltas@.len() && base@ < end@
            && counts.0@ <= old@.len() && counts.1@ <= next@.len()
            && counts.0@ == prefix_count(old@, reports@, request.kind, node, end@, false, reports@.len())
            && counts.1@ == prefix_count(old@, reports@, request.kind, node, end@, true, reports@.len())
            && counts.0@ >= old@.len() / 2 + 1 && counts.1@ >= next@.len() / 2 + 1
            && exists<i: Int> 0 <= i && i < old@.len() && old@[i] == common
                && reports@[i].0@ >= end@ && reports@[i].1@ >= end@
                && has_node(next@, next@.len(), common)
                && forall<j: Int> 0 <= j && j < deltas@.len()
                    ==> base@ + deltas@[j]@ < end@,
    },
})]
pub(crate) fn reconfiguration_control_prefix_support(
    old: &[u64],
    state: (ReconfigurationLeadership, CurrentVoterSet),
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) -> SupportedControl {
    let (plan, next) =
        constructed_voter_reconfiguration(old, state.0, state.1, request, node, target)?;
    if plan.preflight_only {
        return Some((plan, next, Vec::new(), None));
    }
    let mut records: Vec<()> = Vec::new();
    if plan.write_kraft_version {
        records.push(());
    }
    if plan.write_voters {
        records.push(());
    }
    let deltas = metadata_record_offset_deltas(records.len())?;
    let &last = deltas.last()?;
    let (_, end) = local_append_coordinates(base, base, last)?;
    let mut votes: Vec<(bool, bool)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= reports@.len() && votes@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        votes@[j].0 == (reports@[j].0@ >= end@) && votes@[j].1 == (reports@[j].1@ >= end@))]
    #[variant(reports@.len() - i@)]
    while i < reports.len() {
        votes.push((reports[i].0 >= end, reports[i].1 >= end));
        i += 1;
    }
    let (plan, next, counts, quorums, common) =
        reconfigured_majorities_overlap(old, state.0, state.1, request, node, target, &votes)?;
    proof_assert!({
        prefix_grants_agree(old@, next@, votes@, reports@, request.kind, node, end@, false, votes@.len());
        prefix_grants_agree(old@, next@, votes@, reports@, request.kind, node, end@, true, votes@.len());
        true
    });
    if !quorums.0 || !quorums.1 {
        return None;
    }
    let common = common?;
    Some((plan, next, deltas, Some((end, counts, common))))
}
