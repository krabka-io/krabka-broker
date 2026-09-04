//! The checker bounds and the two entry points the model tests call.
//!
//! stateright's BFS keeps every visited unique state resident, so the depth and
//! state-count fences live next to the assertions that prove a run was
//! exhaustive: a search that hit either cap proves nothing, and the two must
//! be tuned together.

use assert2::assert;
use stateright::{Checker, Model};

use super::{failover_state::FailoverModel, recovery_state::RecoveryModel};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_SAFE: usize = 105;
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_UNCLEAN: usize = 280;
pub(super) const PINNED_UNIQUE_STATES_FAILOVER_RECOVER: usize = 105;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_SAFE: usize = 77;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_UNCLEAN: usize = 140;
pub(super) const PINNED_UNIQUE_STATES_WITNESS_RECOVER: usize = 77;
pub(super) const PINNED_UNIQUE_STATES_OFFSET_RECOVERY: usize = 6_859;

pub(super) fn run_failover(model: FailoverModel, label: &str, pinned_unique_states: usize) {
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

pub(super) fn run_recovery(model: RecoveryModel, label: &str, pinned_unique_states: usize) {
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
