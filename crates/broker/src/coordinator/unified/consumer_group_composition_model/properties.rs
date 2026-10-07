//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function, the search boundary, and the properties
//! the checker proves.
//!
//! The transitions themselves live in the sibling modules. This file only
//! dispatches to them and states what must hold.

use std::collections::BTreeMap;

use stateright::{Model, Property};

use super::{
    MAX_OFFSET,
    commit::do_commit,
    config::CgcModel,
    projection::{assert_epoch_monotonic, project, rebuild_group},
    state::{CgcAction, CgcState, EpochKind, committed_map, committed_of, member},
};
use crate::coordinator::unified::actor::reconciliation_model_support::{
    MemberHeartbeat, apply_client_move, apply_member_heartbeat, client_moves, exclusive_ownership,
    overlaps_others, owned_map,
};

impl Model for CgcModel {
    type State = CgcState;
    type Action = CgcAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CgcState {
            group_epoch: 0,
            dirty: false,
            target_epoch: 0,
            members: vec![],
            client_owned: vec![],
            advertised: vec![],
            committed: vec![],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = state.group_epoch < self.max_epoch;
        if under_cap {
            for &id in &self.pool {
                if member(state, id).is_none() {
                    actions.push(CgcAction::Join(id.to_string()));
                }
            }
        }
        for m in &state.members {
            if under_cap {
                actions.push(CgcAction::Leave(m.id.clone()));
                actions.push(CgcAction::Heartbeat(m.id.clone()));
                actions.push(CgcAction::Keepalive(m.id.clone()));
            }
            for (add, partition) in client_moves(&state.advertised, &state.client_owned, &m.id) {
                actions.push(if add {
                    CgcAction::ClientAdd(m.id.clone(), partition)
                } else {
                    CgcAction::ClientRevoke(m.id.clone(), partition)
                });
            }
            // Offset commit: offered with EACH epoch kind (current / stale /
            // forward) so the real epoch fence — not a precondition — is what's
            // exercised; the committed counter is bounded by MAX_OFFSET.
            for part in 0..self.partitions {
                if committed_of(state, part) < MAX_OFFSET {
                    for kind in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                        actions.push(CgcAction::Commit(m.id.clone(), part, kind));
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(&last.client_owned);
        let mut adv: BTreeMap<_, _> = last.advertised.iter().cloned().collect();
        let committed = committed_map(last);
        let add = matches!(action, CgcAction::ClientAdd(..));
        let event = match action {
            CgcAction::ClientAdd(id, partition) | CgcAction::ClientRevoke(id, partition) => {
                let mut next = last.clone();
                next.client_owned =
                    apply_client_move(&last.advertised, &mut owned, id, partition, add)?;
                return Some(next);
            }
            CgcAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                MemberHeartbeat::Join(id)
            }
            CgcAction::Leave(id) => {
                member(last, &id)?;
                MemberHeartbeat::Leave(id)
            }
            CgcAction::Heartbeat(id) => {
                let epoch = member(last, &id)?.member_epoch;
                MemberHeartbeat::Heartbeat(id, epoch)
            }
            CgcAction::Keepalive(id) => {
                let epoch = member(last, &id)?.member_epoch;
                MemberHeartbeat::Keepalive(id, epoch)
            }
            CgcAction::Commit(id, part, kind) => return do_commit(last, &id, part, kind),
        };
        let mut group = rebuild_group(last);
        apply_member_heartbeat(&mut group, &self.metadata(), event, &mut owned, &mut adv);
        assert_epoch_monotonic(last, &group);
        Some(project(&group, &owned, &adv, &committed))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition
            // (the real reconciliation's withholding — re-verified in the composed
            // context, with offset traffic interleaved).
            Property::always("exclusive_ownership", |_, s: &CgcState| {
                exclusive_ownership(&s.client_owned)
            }),
            // A member is never advertised a partition another member currently
            // owns — the coordinator-side withholding invariant.
            Property::always(
                "advertised_disjoint_from_others_owned",
                |_, s: &CgcState| {
                    !overlaps_others(
                        s.advertised.iter().map(|(id, parts)| (id, parts)),
                        &s.client_owned,
                    )
                },
            ),
            // The real OffsetCommit epoch fence agrees with the independent oracle
            // is enforced as a per-transition equality assertion in the Commit arm (a
            // divergence is a real `validate_offset_commit` regression). The
            // value here is the COMPOSITION: the epochs that fence drives are set
            // by the real reconciliation, so a zombie from before a rebalance is
            // rejected — see the `member_epoch_advanced` witness.

            // ----- non-vacuity witnesses -----
            // A current-epoch commit was accepted (the fence's accept path fires).
            Property::sometimes("offset_advanced", |_, s: &CgcState| {
                s.committed.iter().any(|&(_, o)| o > 0)
            }),
            // The reconciliation actually advanced a member's epoch past its first
            // generation — so `Stale`/`Forward` commits are genuinely distinct from
            // `Current` and the fence is exercised over non-trivial epochs (a real
            // zombie scenario: a stale commit after a rebalance bumped the epoch).
            Property::sometimes("member_epoch_advanced", |_, s: &CgcState| {
                s.members.iter().any(|m| m.member_epoch >= 2)
            }),
            // KIP-1251: a member holds a partition from an epoch older than its
            // current one, so a `Stale` commit of that partition is accepted
            // and the per-partition half of the fence is exercised.
            Property::sometimes("stale_epoch_partition_witness", |_, s: &CgcState| {
                s.members.iter().any(|m| {
                    m.assignment_epochs
                        .iter()
                        .any(|&(_, assigned_at)| assigned_at < m.member_epoch)
                })
            }),
            // A handoff state: a partition is in one member's target while another
            // member currently owns it (the baton is mid-pass).
            Property::sometimes("handoff_witness", |_, s: &CgcState| {
                overlaps_others(
                    s.members.iter().map(|m| (&m.id, &m.target)),
                    &s.client_owned,
                )
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch && state.committed.iter().all(|&(_, o)| o <= MAX_OFFSET)
    }
}
