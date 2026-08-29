//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function that drives the real `step_heartbeat`, the
//! search boundary, and the properties the checker proves.
//!
//! The helpers the transitions call live in the sibling modules. This file only
//! sequences them and states what must hold.

use std::{
    collections::{BTreeSet, HashSet},
    time::Instant,
};

use stateright::{Model, Property};

use super::{
    config::{ReconModel, config},
    heartbeat::{advertised_of, hb_request},
    projection::{assert_epoch_monotonic, project, rebuild_group},
    state::{
        ReconAction, ReconState, advertised_for, advertised_map, member, owned_map, owned_to_vec,
    },
};
use crate::coordinator::unified::{
    ClientIdentity, actor::step_heartbeat, persistence_next_gen::MemberAssignmentState,
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
            }
            // Faithful-client moves gate on the ADVERTISED assignment (what the
            // member was last told), not the raw target. No cross-member check.
            let advertised = advertised_for(state, &m.id);
            let owned: BTreeSet<i32> = state
                .client_owned
                .iter()
                .find(|(k, _)| k == &m.id)
                .map(|(_, v)| v.iter().copied().collect())
                .unwrap_or_default();
            for &tp in &advertised {
                if !owned.contains(&tp) {
                    actions.push(ReconAction::ClientAdd(m.id.clone(), tp));
                }
            }
            for &tp in &owned {
                if !advertised.contains(&tp) {
                    actions.push(ReconAction::ClientRevoke(m.id.clone(), tp));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(last);
        let mut adv = advertised_map(last);
        match action {
            ReconAction::ClientAdd(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if !advertised_has || entry.contains(&tp) {
                    return None;
                }
                entry.insert(tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            ReconAction::ClientRevoke(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if advertised_has || !entry.contains(&tp) {
                    return None;
                }
                entry.remove(&tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            ReconAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                let mut g = rebuild_group(last);
                let req = hb_request(&id, 0, &BTreeSet::new());
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.entry(id.clone()).or_default(); // new member owns nothing yet
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv))
            }
            ReconAction::Leave(id) => {
                member(last, &id)?;
                let mut g = rebuild_group(last);
                let req = hb_request(&id, -1, &BTreeSet::new());
                let _ = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.remove(&id);
                adv.remove(&id);
                Some(project(&g, &owned, &adv))
            }
            ReconAction::Heartbeat(id) => {
                let epoch = member(last, &id)?.member_epoch;
                let cur_owned: BTreeSet<i32> = owned.get(&id).cloned().unwrap_or_default();
                let mut g = rebuild_group(last);
                let req = hb_request(&id, epoch, &cur_owned);
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    ClientIdentity { id: "", host: "" },
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv))
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition.
            Property::always("no_double_ownership", |_, s: &ReconState| {
                let mut seen: HashSet<i32> = HashSet::new();
                for (_, parts) in &s.client_owned {
                    for &p in parts {
                        if !seen.insert(p) {
                            return false;
                        }
                    }
                }
                true
            }),
            // A member is never advertised a partition another member currently
            // owns — the coordinator-side withholding invariant.
            Property::always(
                "advertised_disjoint_from_others_owned",
                |_, s: &ReconState| {
                    for (mid, adv) in &s.advertised {
                        for &p in adv {
                            let owned_by_other = s
                                .client_owned
                                .iter()
                                .any(|(k, v)| k != mid && v.contains(&p));
                            if owned_by_other {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Non-vacuity: a handoff state is reachable (a partition is in one
            // member's target while another member currently owns it).
            Property::sometimes("handoff_witness", |_, s: &ReconState| {
                for m in &s.members {
                    for &tp in &m.target {
                        let owned_by_other = s
                            .client_owned
                            .iter()
                            .any(|(k, v)| k != &m.id && v.contains(&tp));
                        if owned_by_other {
                            return true;
                        }
                    }
                }
                false
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
