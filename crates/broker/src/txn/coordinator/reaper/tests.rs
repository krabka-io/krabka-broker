use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use tokio::sync::Mutex;

use super::*;
use crate::txn::coordinator::test_support::{entry, test_coordinator};

#[tokio::test]
async fn reaper_retries_an_existing_prepare_abort() {
    let coordinator = test_coordinator();
    let mut prepared = TxnEntry::new_empty("tid-retry".to_string(), ProducerId(1000), 2, 60_000, 0);
    prepared.state = TxnState::PrepareAbort;
    coordinator.state.insert(
        prepared.transactional_id.clone(),
        Arc::new(Mutex::new(prepared.clone())),
    );

    let retried = ReaperBackend::prepare_abort(
        &coordinator,
        &prepared.transactional_id,
        1,
        TxnVersion::Verified,
    )
    .await
    .expect("prepared abort should be retried");

    check!(retried.transactional_id == prepared.transactional_id);
    check!(retried.producer_id == prepared.producer_id);
    check!(retried.producer_epoch == prepared.producer_epoch);
    check!(retried.state == TxnState::PrepareAbort);
}

// ── Pure transition / guard helpers ───────────────────────────────────

#[test]
fn apply_prepare_abort_flips_state_and_stamps_time() {
    let mut e = entry(1000, -1);
    e.state = TxnState::Ongoing;
    e.last_update_ms = 1;
    apply_prepare_abort(&mut e, 999);
    check!(e.state == TxnState::PrepareAbort);
    check!(e.last_update_ms == 999);
}

#[test]
fn apply_complete_abort_records_prev_only_on_a_pid_roll() {
    // No roll: same pid, epoch bumped → prev untouched.
    let mut e = entry(1000, -1);
    e.state = TxnState::PrepareAbort;
    e.producer_epoch = 4;
    e.partitions.insert(crate::txn::state::TopicPartition {
        topic: "orders".into(),
        partition: PartitionIndex(2),
    });
    apply_complete_abort(&mut e, ProducerId(1000), 5, 42);
    check!(e.state == TxnState::CompleteAbort);
    check!(e.producer_id == 1000);
    check!(e.producer_epoch == 5);
    check!(e.prev_producer_id == -1, "no roll must not set prev");
    check!(e.partitions.is_empty());
    check!(e.last_update_ms == 42);

    // Roll: fresh pid at epoch 0 → prior pid recorded as prev.
    let mut rolled = entry(1000, -1);
    rolled.state = TxnState::PrepareAbort;
    apply_complete_abort(&mut rolled, ProducerId(2000), 0, 43);
    check!(rolled.producer_id == 2000);
    check!(rolled.producer_epoch == 0);
    check!(
        rolled.prev_producer_id == 1000,
        "roll must record prior pid"
    );
}

#[test]
fn complete_abort_guard_rejects_identity_or_state_drift() {
    let mut prepared = entry(1000, -1);
    prepared.producer_epoch = 7;
    prepared.state = TxnState::PrepareAbort;

    // Exact match → ok.
    let mut current = prepared.clone();
    assert!(complete_abort_guard_ok(&current, &prepared));

    // pid changed → reject.
    current = prepared.clone();
    current.producer_id = ProducerId(9999);
    assert!(!complete_abort_guard_ok(&current, &prepared));

    // epoch changed → reject.
    current = prepared.clone();
    current.producer_epoch = 8;
    assert!(!complete_abort_guard_ok(&current, &prepared));

    // state advanced past PrepareAbort → reject.
    current = prepared.clone();
    current.state = TxnState::CompleteAbort;
    assert!(!complete_abort_guard_ok(&current, &prepared));
}

// ── Orchestration loop, driven against a mock backend ─────────────────

fn prepared_entry(tid: &str, pid: i64, epoch: i16) -> TxnEntry {
    let mut e = TxnEntry::new_empty(tid.to_owned(), ProducerId(pid), epoch, 60_000, 0);
    e.state = TxnState::PrepareAbort;
    e
}

#[tokio::test]
async fn sweep_runs_full_three_phase_abort_for_an_expired_tid() {
    let mut backend = MockReaperBackend::new();
    backend
        .expect_is_coordinator_for()
        .withf(|t| t == "tid-a")
        .returning(|_| true);
    backend
        .expect_prepare_abort()
        .times(1)
        .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
    backend
        .expect_dispatch_abort_markers()
        .times(1)
        .withf(|e| e.transactional_id == "tid-a" && e.state == TxnState::PrepareAbort)
        .returning(|_| true);
    backend
        .expect_complete_abort()
        .times(1)
        .withf(|e, _, _| e.transactional_id == "tid-a")
        .returning(|e, _, _| Some(e.clone()));

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;
    check!(out == vec!["tid-a".to_owned()]);
}

#[tokio::test]
async fn sweep_skips_tids_this_broker_does_not_coordinate() {
    let mut backend = MockReaperBackend::new();
    backend.expect_is_coordinator_for().returning(|_| false);
    // No prepare / dispatch / complete must be reached.
    backend.expect_prepare_abort().never();
    backend.expect_dispatch_abort_markers().never();
    backend.expect_complete_abort().never();

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn sweep_skips_tid_when_prepare_declines_and_does_not_dispatch() {
    let mut backend = MockReaperBackend::new();
    backend.expect_is_coordinator_for().returning(|_| true);
    // Not idle / persistence failed → None.
    backend
        .expect_prepare_abort()
        .times(1)
        .returning(|_, _, _| None);
    backend.expect_dispatch_abort_markers().never();
    backend.expect_complete_abort().never();

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn sweep_does_not_report_tid_when_complete_loses_the_race() {
    let mut backend = MockReaperBackend::new();
    backend.expect_is_coordinator_for().returning(|_| true);
    backend
        .expect_prepare_abort()
        .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
    // Markers still fan out (Phase 2 ran)...
    backend
        .expect_dispatch_abort_markers()
        .times(1)
        .returning(|_| true);
    // ...but Phase 3 lost the race → not finalized, not reported.
    backend
        .expect_complete_abort()
        .times(1)
        .returning(|_, _, _| None);

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;
    assert!(out.is_empty());
}

#[tokio::test]
async fn sweep_aborts_each_expired_tid_independently() {
    let mut backend = MockReaperBackend::new();
    // tid-a coordinated + expired; tid-b not coordinated.
    backend
        .expect_is_coordinator_for()
        .returning(|t| t == "tid-a");
    backend
        .expect_prepare_abort()
        .withf(|t, _, _| t == "tid-a")
        .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
    backend.expect_dispatch_abort_markers().returning(|_| true);
    backend
        .expect_complete_abort()
        .returning(|e, _, _| Some(e.clone()));

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned(), "tid-b".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;
    check!(out == vec!["tid-a".to_owned()]);
}

#[tokio::test]
async fn sweep_does_not_complete_when_marker_fanout_fails() {
    let mut backend = MockReaperBackend::new();
    backend.expect_is_coordinator_for().returning(|_| true);
    backend
        .expect_prepare_abort()
        .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
    backend.expect_dispatch_abort_markers().returning(|_| false);
    backend.expect_complete_abort().never();

    let out = sweep_with_backend(
        &backend,
        vec!["tid-a".to_owned()],
        1_000,
        TxnVersion::Verified,
    )
    .await;

    assert!(out.is_empty());
}
