//! The checker bounds and the one entry point that both model tests call.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use stateright::{Checker, Model};

use super::config::ClassicModel;

// Exhaustiveness is bounded on UNIQUE states (memory-proportional); the BFS's
// generated count runs several times the unique count here (high branching:
// every idle member has join/leave/heartbeat actions). `TARGET_STATE_COUNT` is
// the truncation ceiling set high so the BFS runs to completion (the 3 GB host
// watchdog is the other runaway guard — `[[feedback_bound_model_checkers]]`);
// `state_count() < TARGET` then certifies the run was exhaustive.
const TARGET_STATE_COUNT: usize = 8_000_000;
const MAX_UNIQUE_STATES: usize = 500_000; // wide ~362k unique; margin for determinism
const MAX_DEPTH: usize = 80;

pub(super) fn run(model: ClassicModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert2::assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated at the state-count target — not exhaustive"
    );
    assert2::assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}
