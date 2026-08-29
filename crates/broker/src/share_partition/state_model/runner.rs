//! The checker bounds and the one entry point that every model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use std::time::Duration;

use assert2::assert;
use stateright::{Checker, Model};

use super::config::ShareModel;

/// Hard backstop on generated states. It bounds host memory even if
/// `within_boundary` is looser than intended. Set it well above each config's
/// true bounded count, so a real exhaustive run never truncates.
const MAX_STATES: usize = 200_000;
/// Depth backstop. It must exceed each config's reachable-graph diameter.
/// Otherwise the search is depth-truncated and incomplete, and the `run`
/// harness fails.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Run one bounded config to completion. Assert that the run was exhaustive,
/// that is, that no cap truncated it, and that all properties hold.
pub(super) fn run(model: ShareModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
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
        "[{label}] hit depth cap {MAX_DEPTH}: search is depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: search is truncated, not exhaustive"
    );
    checker.assert_properties();
}
