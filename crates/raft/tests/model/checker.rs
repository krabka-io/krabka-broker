//! The checker interface delegates to enabled actions, transitions, and
//! safety and witness properties in child modules.

use std::collections::{BTreeMap, BTreeSet};

use krabka_raft::kraft::{
    QuorumStateMachine,
    action::TimerKind,
    event::Event,
    role::Role,
    types::{Epoch, LogView, NodeId, QuorumState},
};
use stateright::{
    Model, Property,
    semantics::{ConsistencyTester, LinearizabilityTester},
};

use super::{
    commit::settle_committed,
    config::ConsensusModel,
    log::ModelLog,
    spec::{APPENDER_COUNT, AppenderId, ClientId, KraftLogSpec, LogOp},
    state::{
        CommitPoint, ModelAction, ModelState, NodeModel, StepWitness, is_leader, live_authority,
        node_high_watermark,
    },
};

impl Model for ConsensusModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        self.model_init_states()
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        self.model_actions(state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        self.model_next_state(last, action)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        Self::model_properties()
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound the space HARD: stateright BFS/DFS keeps every visited unique
        // state in memory, so loose bounds OOM the machine. Cap in-flight
        // messages and the maximum leader epoch per the model's config.
        state.network.len() <= self.max_inflight
            && state
                .nodes
                .values()
                .all(|n| n.machine.quorum_state().leader_epoch <= self.max_epoch)
    }
}

#[path = "checker/actions.rs"]
mod actions;

#[path = "checker/transitions.rs"]
mod transitions;

#[path = "checker/invariants.rs"]
mod invariants;
