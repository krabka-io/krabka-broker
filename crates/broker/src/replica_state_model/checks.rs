use super::*;

/// Runs one bounded config to completion. Asserts that the run was exhaustive,
/// that the cap or the depth did not truncate it, and that all properties hold.
pub(super) fn run(model: IsrModel, label: &str, pinned_unique_states: usize) {
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
    assert2::assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
