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

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_BASIC: usize = 2_306;
pub(super) const PINNED_UNIQUE_STATES_WIDE: usize = 191_006;

pub(super) fn run(model: ClassicModel, label: &str, pinned_unique_states: usize) {
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
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
