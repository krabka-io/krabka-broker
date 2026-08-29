//! The checker bounds and the one entry point every model test calls.
//!
//! State explosion is the central risk of this composition, so the bounds sit
//! next to the assertions that prove a run was exhaustive: a search that hit
//! the depth cap, the generated-state cap or the unique-state bound proves
//! nothing, and the two must be tuned together.

use std::time::Duration;

use stateright::{Checker, Model};

use super::model::DpModel;

const TARGET_STATE_COUNT: usize = 60_000_000;
const MAX_UNIQUE_STATES: usize = 8_000_000;
const MAX_DEPTH: usize = 70;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

pub(super) fn run(model: DpModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}
