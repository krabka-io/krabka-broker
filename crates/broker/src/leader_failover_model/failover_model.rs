//! The stateright [`Model`] implementation for the failover scan: the initial
//! state, the enabled actions, the transition that calls the real
//! `failover_one`, the search boundary, and the properties the checker proves.
//!
//! A `Model` implementation is one indivisible unit, because the action
//! generator, the transition and the properties only make sense against each
//! other, so it stays whole in this file. The state it moves and the decision
//! invariants it asserts live in the sibling modules.

use std::collections::HashSet;

use krabka_raft::NodeId;
use stateright::{Model, Property};

use super::{
    decision::assert_decision,
    failover_state::{FailoverAction, FailoverModel, FailoverState, pr_of},
};
use crate::leader_election::{FailoverDecision, failover_one};

impl Model for FailoverModel {
    type State = FailoverState;
    type Action = FailoverAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            replicas: self.replicas.clone(),
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Die: any alive broker, keeping >= 1 alive.
        if state.alive.len() > 1 {
            for &r in &self.replicas {
                if state.alive.contains(&r) {
                    actions.push(FailoverAction::Die(r));
                }
            }
        }
        // Revive: any dead broker.
        for &r in &self.replicas {
            if !state.alive.contains(&r) {
                actions.push(FailoverAction::Revive(r));
            }
        }
        // Failover: any dead broker (the real scan's filter is replicas-or-isr;
        // all model brokers are replicas), under the epoch cap.
        if state.leader_epoch < self.max_epoch {
            for &r in &self.replicas {
                if !state.alive.contains(&r) {
                    actions.push(FailoverAction::Failover(r));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            FailoverAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            FailoverAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            FailoverAction::Failover(dead) => {
                if state.alive.contains(&dead) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                let decision = failover_one(
                    &pr,
                    dead,
                    &alive,
                    &self.witnesses,
                    // This model carries no ELR state; the KIP-966 rung it
                    // would unlock is checked in `leader_election::policy`.
                    &[],
                    self.strategy,
                    self.unclean_enabled,
                );
                assert_decision(
                    &state,
                    dead,
                    &decision,
                    self.unclean_enabled,
                    &self.witnesses,
                );
                match decision {
                    FailoverDecision::Elect { leader, isr, .. } => {
                        state.leader = leader;
                        state.isr = isr;
                        state.leader_epoch += 1;
                    }
                    FailoverDecision::ShrinkIsr { isr } => {
                        state.isr = isr;
                    }
                    FailoverDecision::Recover(_)
                    | FailoverDecision::Unavailable
                    | FailoverDecision::NoChange => return None,
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("isr_subset_replicas", |_, s: &FailoverState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &FailoverState| {
                s.replicas.contains(&s.leader)
            }),
            // The witness invariant: no reachable state has a witness leader.
            Property::always(
                "leader_never_witness",
                |model: &FailoverModel, s: &FailoverState| !model.witnesses.contains(&s.leader),
            ),
            Property::sometimes("can_elect", |_, s: &FailoverState| s.leader_epoch > 0),
            Property::sometimes("can_singleton_isr", |_, s: &FailoverState| s.isr.len() == 1),
            Property::sometimes("can_lose_isr_member", |_, s: &FailoverState| {
                s.isr.iter().any(|n| !s.alive.contains(n))
            }),
        ];
        if !self.witnesses.is_empty() {
            // The witness must survive an election that skipped it, because it
            // is what keeps `acks=all` writable after a site loss.
            properties.push(Property::sometimes(
                "witness_stays_in_isr_after_election",
                |model: &FailoverModel, s: &FailoverState| {
                    s.leader_epoch > 0 && s.isr.iter().any(|n| model.witnesses.contains(n))
                },
            ));
        }
        properties
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}
