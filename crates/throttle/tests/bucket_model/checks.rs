use super::*;

pub(super) fn run(model: BucketModel) -> impl Checker<BucketModel> {
    model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .spawn_bfs()
        .join()
}

pub(super) fn green_run(model: BucketModel, label: &str, pinned_unique_states: usize) {
    let checker = run(model);
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < TARGET_STATE_COUNT);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

/// The smallest search in which the seqlock lets a straddled reset raise
/// `available` past the new burst: one consumer, one shrinking reset.
pub(super) fn straddle_config(algorithm: Algorithm) -> BucketModel {
    BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 1,
        configs: vec![Config { rate: 1, burst: 2 }, Config { rate: 1, burst: 1 }],
        max_resets: 1,
        max_time: 1,
        max_req: 1,
    }
}

/// The smallest search in which the seqlock drops a claimed refill: two
/// consumers and no reset at all.
pub(super) fn contention_config(algorithm: Algorithm) -> BucketModel {
    BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 2,
        configs: vec![Config { rate: 1, burst: 3 }],
        max_resets: 0,
        max_time: 1,
        max_req: 1,
    }
}
