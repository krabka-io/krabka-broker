//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function that drives the real `step_heartbeat`, the
//! search boundary, and the properties the checker proves.
//!
//! The helpers the transitions call live in the sibling modules. This file only
//! sequences them and states what must hold.

use std::collections::BTreeMap;

use stateright::{Model, Property};

use super::{
    config::ReconModel,
    projection::{assert_epoch_monotonic, project, rebuild_group},
    state::{ReconAction, ReconState, member},
};
use crate::coordinator::unified::{
    actor::reconciliation_model_support::{
        MemberHeartbeat, apply_client_move, apply_member_heartbeat, client_moves,
        exclusive_ownership, overlaps_others, owned_map,
    },
    persistence_next_gen::MemberAssignmentState,
};

impl Model for ReconModel {
    type State = ReconState;
    type Action = ReconAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReconState {
            group_epoch: 0,
            dirty: false,
            target_epoch: 0,
            members: vec![],
            client_owned: vec![],
            advertised: vec![],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = state.group_epoch < self.max_epoch;
        // Join: any pool id not currently a member (epoch-advancing → gated).
        if under_cap {
            for &id in &self.pool {
                if member(state, id).is_none() {
                    actions.push(ReconAction::Join(id.to_string()));
                }
            }
        }
        for m in &state.members {
            // Leave + Heartbeat are epoch-advancing → gated by the cap.
            if under_cap {
                actions.push(ReconAction::Leave(m.id.clone()));
                actions.push(ReconAction::Heartbeat(m.id.clone()));
                actions.push(ReconAction::Keepalive(m.id.clone()));
            }
            // Faithful-client moves gate on the ADVERTISED assignment (what the
            // member was last told), not the raw target. No cross-member check.
            for (add, partition) in client_moves(&state.advertised, &state.client_owned, &m.id) {
                actions.push(if add {
                    ReconAction::ClientAdd(m.id.clone(), partition)
                } else {
                    ReconAction::ClientRevoke(m.id.clone(), partition)
                });
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(&last.client_owned);
        let mut adv: BTreeMap<_, _> = last.advertised.iter().cloned().collect();
        let add = matches!(action, ReconAction::ClientAdd(..));
        let event = match action {
            ReconAction::ClientAdd(id, partition) | ReconAction::ClientRevoke(id, partition) => {
                let mut next = last.clone();
                next.client_owned =
                    apply_client_move(&last.advertised, &mut owned, id, partition, add)?;
                return Some(next);
            }
            ReconAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                MemberHeartbeat::Join(id)
            }
            ReconAction::Leave(id) => {
                member(last, &id)?;
                MemberHeartbeat::Leave(id)
            }
            ReconAction::Heartbeat(id) => {
                let epoch = member(last, &id)?.member_epoch;
                MemberHeartbeat::Heartbeat(id, epoch)
            }
            ReconAction::Keepalive(id) => {
                let epoch = member(last, &id)?.member_epoch;
                MemberHeartbeat::Keepalive(id, epoch)
            }
        };
        let mut group = rebuild_group(last);
        apply_member_heartbeat(&mut group, &self.metadata(), event, &mut owned, &mut adv);
        assert_epoch_monotonic(last, &group);
        Some(project(&group, &owned, &adv))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition.
            Property::always("no_double_ownership", |_, s: &ReconState| {
                exclusive_ownership(&s.client_owned)
            }),
            // A member is never advertised a partition another member currently
            // owns — the coordinator-side withholding invariant.
            Property::always(
                "advertised_disjoint_from_others_owned",
                |_, s: &ReconState| {
                    !overlaps_others(
                        s.advertised.iter().map(|(id, parts)| (id, parts)),
                        &s.client_owned,
                    )
                },
            ),
            // Non-vacuity: a handoff state is reachable (a partition is in one
            // member's target while another member currently owns it).
            Property::sometimes("handoff_witness", |_, s: &ReconState| {
                overlaps_others(
                    s.members.iter().map(|m| (&m.id, &m.target)),
                    &s.client_owned,
                )
            }),
            // Non-vacuity: a fully-converged state is reachable (every member
            // owns exactly its target and is Stable).
            Property::sometimes("converged_witness", |_, s: &ReconState| {
                !s.members.is_empty()
                    && s.members.iter().all(|m| {
                        let owned: Vec<i32> = s
                            .client_owned
                            .iter()
                            .find(|(k, _)| k == &m.id)
                            .map(|(_, v)| v.clone())
                            .unwrap_or_default();
                        m.assignment_state == MemberAssignmentState::Stable && owned == m.target
                    })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch
    }
}
