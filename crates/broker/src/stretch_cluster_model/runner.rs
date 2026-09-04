//! The checker bounds and the one entry point that every model test calls.
//!
//! The bounds are here, next to the assertions that prove a run was
//! exhaustive, because a truncated search proves nothing and the two must move
//! together.

use assert2::assert;
use stateright::{Checker, Model};

use super::config::StretchModel;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_THREE_SITES: usize = 2_226;
pub(super) const PINNED_UNIQUE_STATES_RED_LEGACY_ELECT: usize = 5_724;
pub(super) const PINNED_UNIQUE_STATES_RED_MIN_INSYNC_ONE: usize = 3_710;

pub fn run(model: StretchModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
