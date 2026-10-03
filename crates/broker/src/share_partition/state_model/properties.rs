//! The checker interface delegates to enabled actions, production state
//! transitions and safety properties in child modules.

use krabka_log::Offset;
use stateright::{Model, Property};

use super::{
    config::{LOCK, ShareModel},
    invariants::{
        assert_transition, delivery_complete_count_is_terminal_in_window, lock_consistency,
        mutual_exclusion, window_integrity,
    },
    observe::{acquired_runs, deferred_offsets, offset_state},
    state::{ShareAction, ShareState},
};
use crate::share_partition::state::{AckType, AcquisitionState, RecordState};

impl Model for ShareModel {
    type State = ShareState;
    type Action = ShareAction;

    fn init_states(&self) -> Vec<Self::State> {
        Self::model_init_states()
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        self.model_actions(state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        self.model_next_state(last, action)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        self.model_properties()
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound ONLY the design-unbounded dimensions (so the space is finite);
        // do NOT bound delivery_count — its <= max_attempts boundedness is a
        // property we verify, so pruning it would mask a violation. The 12-batch
        // cap is a loose structural safety net (real max over a <=3 window is 3).
        state.clock <= self.max_tick
            && state.hwm <= self.max_offset
            && state.sm.end_offset <= self.max_offset
            && state.log_start <= self.max_offset
            && state.sm.batches.len() <= 12
    }
}

#[path = "properties/actions.rs"]
mod actions;

#[path = "properties/transitions.rs"]
mod transitions;

#[path = "properties/invariants.rs"]
mod invariants;
