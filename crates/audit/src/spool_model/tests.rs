use super::*;

#[test]
fn audit_spool_crash_and_replay_interleavings() {
    let checker = check(SpoolModel::SETTLE);
    eprintln!(
        "[audit-spool] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

/// RED witness: the checker rejects the settlement `PendingLosses` applied
/// before `settle_loss_batch`. A loss that `AuditHandle::emit` adds while a
/// marker is in flight stays in the marker's generation. Reconciliation after
/// a crash then zeroes it along with the marker's own losses, or a second
/// marker names the same generation.
#[test]
fn superseded_settlement_loses_and_rereports_losses() {
    let checker = check(SpoolModel {
        commit: superseded_commit,
        reconcile: superseded_reconcile,
    });
    for property in [
        "losses_accounted",
        "marker_generations_unique",
        "markers_settled_once",
    ] {
        assert2::assert!(checker.discovery(property).is_some(), "{property}");
    }
}

#[test]
fn loss_accounting_saturates_without_wrapping() {
    assert2::check!(add_loss_state(u64::MAX, 0, 1) == (u64::MAX, 1));
    assert2::check!(add_loss_state(7, u64::MAX, 1) == (7, u64::MAX));
}

#[test]
fn malformed_or_stale_replay_poison_requires_recovery() {
    assert2::check!(replay_recovery(false, Some(0), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, None, 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(1), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(0), 1) == ReplayRecovery::ClearPoison);
}
