//! The stateright [`Model`] implementation for KIP-966 winner selection: the
//! fan-out over the bounded response domain and the two properties that state
//! what `select_best_replica` and `has_newer_leader` must compute.
//!
//! A `Model` implementation is one indivisible unit, because the action
//! generator, the transition and the properties only make sense against each
//! other, so it stays whole in this file. The state it gathers lives in the
//! sibling `recovery_state` module.

use std::collections::BTreeMap;

use stateright::{Model, Property};

use super::recovery_state::{RecoveryAction, RecoveryModel, RecoveryState, ReplicaLog, infos_of};
use crate::unclean_recovery::{has_newer_leader, select_best_replica};

impl Model for RecoveryModel {
    type State = RecoveryState;
    type Action = RecoveryAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![RecoveryState {
            responses: BTreeMap::new(),
            known_leader_epoch: self.known_leader_epoch,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Each replica reports at most one log state; fan out over the bounded
        // (epoch, leo, current_epoch) domain. current_epoch ranges one past the
        // known epoch so has_newer_leader is reachable both ways.
        for &node in &self.replicas {
            if state.responses.contains_key(&node) {
                continue;
            }
            for last_written_epoch in 0..=self.max_epoch {
                for leo in 0..=self.max_leo {
                    for current_epoch in self.known_leader_epoch..=(self.known_leader_epoch + 1) {
                        actions.push(RecoveryAction::AddResponse {
                            node,
                            last_written_epoch,
                            leo,
                            current_epoch,
                        });
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            RecoveryAction::AddResponse {
                node,
                last_written_epoch,
                leo,
                current_epoch,
            } => {
                if state.responses.contains_key(&node) {
                    return None;
                }
                state.responses.insert(
                    node,
                    ReplicaLog {
                        last_written_leader_epoch: last_written_epoch,
                        log_end_offset: leo,
                        current_leader_epoch: current_epoch,
                    },
                );
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // The real select_best_replica returns the true maximum by
            // (last_written_leader_epoch, log_end_offset, then lowest broker_id).
            Property::always("select_best_is_max", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                match select_best_replica(&infos) {
                    None => infos.is_empty(),
                    Some(w) => {
                        let win = infos
                            .iter()
                            .find(|i| i.broker_id == w)
                            .expect("winner is among the inputs");
                        infos.iter().all(|i| {
                            (win.last_written_leader_epoch, win.log_end_offset)
                                .cmp(&(i.last_written_leader_epoch, i.log_end_offset))
                                .then(i.broker_id.cmp(&win.broker_id)) // lower id wins
                                != std::cmp::Ordering::Less
                        })
                    }
                }
            }),
            // The real has_newer_leader matches its specification.
            Property::always("has_newer_leader_matches", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                has_newer_leader(&infos, s.known_leader_epoch)
                    == infos
                        .iter()
                        .any(|i| i.current_leader_epoch > s.known_leader_epoch)
            }),
            Property::sometimes("can_pick_winner", |_, s: &RecoveryState| {
                !s.responses.is_empty()
            }),
            Property::sometimes("can_detect_newer", |_, s: &RecoveryState| {
                s.responses
                    .values()
                    .any(|l| l.current_leader_epoch > s.known_leader_epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.responses.len() <= self.replicas.len()
            && state.responses.values().all(|l| {
                l.last_written_leader_epoch <= self.max_epoch && l.log_end_offset <= self.max_leo
            })
    }
}
