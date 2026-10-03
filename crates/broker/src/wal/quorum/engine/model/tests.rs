use super::*;

#[test]
fn quorum_wal_append_ack_and_recovery_model() {
    let checker = WalModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[quorum_wal_model] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
    assert2::assert!(checker.state_count() < MAX_STATES, "state cap hit");
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
