//! The checker bounds and the one entry point every model test calls.
//!
//! State explosion is the central risk of this composition, so the bounds sit
//! next to the assertions that prove a run was exhaustive: a search that hit
//! the depth cap, the generated-state cap or the unique-state bound proves
//! nothing, and the two must be tuned together.

use stateright::{Checker, Model};

use super::model::DpModel;

const TARGET_STATE_COUNT: usize = 60_000_000;
const MAX_UNIQUE_STATES: usize = 8_000_000;
const MAX_DEPTH: usize = 70;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_CLEAN: usize = 521_626;
pub(super) const PINNED_UNIQUE_STATES_UNCLEAN: usize = 1_255_681;
pub(super) const PINNED_UNIQUE_STATES_ELR: usize = 3_271_184;
pub(super) const PINNED_UNIQUE_STATES_DISKLESS: usize = 120;

pub(super) fn run(model: DpModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert2::assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated"
    );
    assert2::assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique bound exceeded ({})",
        checker.unique_state_count()
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
