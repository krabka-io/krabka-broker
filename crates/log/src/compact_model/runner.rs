//! The checker bounds and the one entry point every model test calls, together
//! with the two exhaustive runs.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use krabka_units::prelude::{Time, TimeExt as _, minutes};
use stateright::{Checker, Model};

use super::state::CompactModel;

// The `Compact` action converges many append/tick paths onto shared logs, so the
// BFS's *generated* count (`state_count()`) runs ~2-2.5x the *unique* count. We
// therefore bound exhaustiveness on the two metrics that actually matter:
//
//   * `TARGET_STATE_COUNT` — the stateright truncation target. Set high so the
//     BFS runs to *completion* on the configs below; `state_count() < TARGET`
//     after `.join()` then certifies the run was exhaustive (it stopped because
//     the frontier emptied, not because it hit the target). The real runaway
//     guards are the 2-minute `CHECK_TIMEOUT` and the 3 GB host memory watchdog
//     (see `[[feedback_bound_model_checkers]]`) — never run this unguarded.
//   * `MAX_UNIQUE_STATES` — the memory-proportional bound (resident memory ∝
//     distinct states). At the bounds below the unique space is ~67k (basic) /
//     ~460k (wide), generated ~191k / ~1.34M, and resident memory ~0.07 GB.
const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 600_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Time = minutes(2);

fn run(model: CompactModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT.to_std())
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    // Exhaustiveness: the BFS stopped because the frontier emptied, not because
    // it hit the truncation target.
    assert2::assert!(checker.state_count() < TARGET_STATE_COUNT);
    // Memory-proportional bound (resident memory ∝ distinct states).
    assert2::assert!(checker.unique_state_count() < MAX_UNIQUE_STATES);
    checker.assert_properties();
}

#[test]
fn compaction_basic() {
    run(
        CompactModel {
            max_len: 4,
            max_clock: 4,
        },
        "compaction_basic",
    );
}

#[test]
fn compaction_wide() {
    run(
        CompactModel {
            max_len: 5,
            max_clock: 4,
        },
        "compaction_wide",
    );
}
