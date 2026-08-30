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

pub(super) fn run_failover(model: FailoverModel, label: &str) {
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
    checker.assert_properties();
}

pub(super) fn run_recovery(model: RecoveryModel, label: &str) {
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
    checker.assert_properties();
}
