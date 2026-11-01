//! Unit tests for the idle-transaction reaper: the pure transition helper,
//! the live `PrepareAbort` retry path, the sweep driven against a mock
//! `ReaperBackend`, and the sweep against real logs.

use {
    super::*,
    crate::{
        codes,
        txn::{
            coordinator::{
                completion::CompletionAttempt,
                fanout_gate::MarkerFanoutMode,
                test_support::{DATA_TOPIC, entry, live_coordinator, test_coordinator},
            },
            state::TopicPartition,
        },
    },
    assert2::{assert, check},
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    std::{sync::Arc, time::Duration},
    tokio::{sync::Mutex, time},
};

#[tokio::test]
async fn reaper_retries_an_existing_prepare_abort() {
    let coordinator = test_coordinator();
    let prepared = prepared_entry("tid-retry", 1000, 2);
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

#[tokio::test]
async fn reaper_never_retries_a_prepared_two_phase_transaction() {
    let coordinator = test_coordinator();
    let prepared = prepared_entry_with_timeout("tid-2pc", 1000, 2, NO_TIMEOUT_MS);
    coordinator.state.insert(
        prepared.transactional_id.clone(),
        Arc::new(Mutex::new(prepared.clone())),
    );

    assert!(
        ReaperBackend::prepare_abort(
            &coordinator,
            &prepared.transactional_id,
            i64::MAX,
            TxnVersion::Verified,
        )
        .await
        .is_none()
    );
    check!(
        *coordinator
            .get(&prepared.transactional_id)
            .expect("2PC entry")
            .lock()
            .await
            == prepared
    );
}

// ── Pure transition / guard helpers ───────────────────────────────────

#[test]
fn apply_prepare_abort_flips_state_and_stamps_time() {
    let mut e = entry().call();
    e.state = TxnState::Ongoing;
    e.last_update_ms = 1;
    apply_prepare_abort(&mut e, 999);
    check!(e.state == TxnState::PrepareAbort);
    check!(e.last_update_ms == 999);
}

#[tokio::test]
async fn failed_prepare_persistence_leaves_the_live_entry_ongoing() {
    let coordinator = test_coordinator();
    let mut ongoing = entry().call();
    ongoing.state = TxnState::Ongoing;
    ongoing.txn_timeout_ms = 1;
    ongoing.start_ms = 0;
    let tid = ongoing.transactional_id.clone();
    coordinator
        .state
        .insert(tid.clone(), Arc::new(Mutex::new(ongoing.clone())));

    assert!(
        ReaperBackend::prepare_abort(&coordinator, &tid, 2, TxnVersion::Classic)
            .await
            .is_none()
    );
    check!(*coordinator.get(&tid).expect("ongoing entry").lock().await == ongoing);
}

#[tokio::test]
async fn replaced_entry_handle_is_rejected_as_stale() {
    let coordinator = test_coordinator();
    let current = entry().call();
    let tid = current.transactional_id.clone();
    let stale = Arc::new(Mutex::new(current.clone()));
    coordinator.state.insert(tid.clone(), Arc::clone(&stale));
    assert!(handle_is_current(&coordinator, &tid, &stale));

    let replacement = Arc::new(Mutex::new(current));
    coordinator
        .state
        .insert(tid.clone(), Arc::clone(&replacement));
    assert!(!handle_is_current(&coordinator, &tid, &stale));
    assert!(handle_is_current(&coordinator, &tid, &replacement));
}

// ── The sweep, driven against a mock backend ──────────────────────────

fn prepared_entry(tid: &str, pid: i64, epoch: i16) -> TxnEntry {
    prepared_entry_with_timeout(tid, pid, epoch, 60_000)
}

fn prepared_entry_with_timeout(tid: &str, pid: i64, epoch: i16, timeout_ms: i32) -> TxnEntry {
    let mut e = TxnEntry::new_empty(tid.to_owned(), ProducerId(pid), epoch, timeout_ms, 0);
    e.state = TxnState::PrepareAbort;
    e
}

async fn sweep_candidates(backend: &MockReaperBackend, candidates: &[&str]) -> Vec<String> {
    sweep_with_backend(
        backend,
        candidates.iter().map(|tid| (*tid).to_owned()).collect(),
        1_000,
        TxnVersion::Verified,
    )
    .await
}

#[yare::parameterized(
    not_coordinator = { false },
    prepare_declines = { true },
)]
#[test_macro(tokio::test)]
async fn sweep_queues_nothing_it_did_not_prepare(is_coordinator: bool) {
    let mut backend = MockReaperBackend::new();
    backend
        .expect_is_coordinator_for()
        .returning(move |_| is_coordinator);
    if is_coordinator {
        // Not idle / persistence failed → None.
        backend
            .expect_prepare_abort()
            .times(1)
            .returning(|_, _, _| None);
    } else {
        // A non-coordinator must not start the abort protocol.
        backend.expect_prepare_abort().never();
    }
    backend.expect_queue_completion().never();

    let out = sweep_candidates(&backend, &["tid-a"]).await;
    assert2::assert!(out.is_empty());
}

/// The sweep prepares and queues the abort of each coordinated, expired tid,
/// once, and skips the others. Only `tid-a` is coordinated and expired.
#[yare::parameterized(
    alone = { &["tid-a"] },
    beside_one_it_does_not_coordinate = { &["tid-a", "tid-b"] },
)]
#[test_macro(tokio::test)]
async fn sweep_hands_each_prepared_abort_to_the_completion_task(candidates: &[&str]) {
    let mut backend = MockReaperBackend::new();
    backend
        .expect_is_coordinator_for()
        .returning(|t| t == "tid-a");
    backend
        .expect_prepare_abort()
        .times(1)
        .withf(|t, _, _| t == "tid-a")
        .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
    backend
        .expect_queue_completion()
        .times(1)
        .withf(|t| t == "tid-a")
        .return_const(());

    let out = sweep_candidates(&backend, candidates).await;
    check!(out == vec!["tid-a".to_owned()]);
}

// ── The abort of a timed-out transaction, against real logs ───────────

/// Kafka's `abortTimedOutTransactions` runs `prepareFenceProducerEpoch` and
/// then `endTransaction(isFromClient = false)` at the cluster's transaction
/// version. Every version aborts at `epoch + 1`, so the timed-out producer is
/// fenced at its partitions and on its next coordinator call. Below `TV_2` the
/// fence raises the epoch and keeps no last epoch. At `TV_2` the completion bump
/// raises it once and keeps the epoch the producer held as the last epoch. The
/// record and the markers carry `TV_2` on a `TV_2` cluster, and `TV_0` below it:
/// `endTransactionWithTV1` stamps `TV_0` on a `TV_1` cluster too.
#[tokio::test]
async fn a_timed_out_transaction_aborts_at_the_fenced_epoch_at_every_transaction_version() {
    const TID: &str = "tid-timed-out";
    const PID: ProducerId = ProducerId(1000);
    const HELD_EPOCH: i16 = 3;

    // (cluster level, the last epoch the abort records, the version it stamps)
    let cases = [
        (TxnVersion::Classic, -1, 0),
        (TxnVersion::Flexible, -1, 0),
        (TxnVersion::Verified, HELD_EPOCH, 2),
    ];
    for (txnv, last_epoch, stamp) in cases {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, data) = live_coordinator(dir.path()).await;
        let orders = TopicPartition {
            topic: DATA_TOPIC.to_owned(),
            partition: PartitionIndex(0),
        };
        let mut ongoing = TxnEntry::new_empty(TID.to_owned(), PID, HELD_EPOCH, 1, 0);
        ongoing.state = TxnState::Ongoing;
        ongoing.start_ms = 0;
        ongoing.partitions.insert(orders.clone());
        coordinator
            .put(ongoing, txnv)
            .await
            .expect("seed the ongoing transaction");

        check!(
            coordinator.sweep_expired(10_000, txnv).await == vec![TID.to_owned()],
            "{txnv:?}: the transaction times out"
        );
        // The sweep queues the durable PrepareAbort; the completion task
        // writes the markers and the CompleteAbort.
        check!(
            coordinator.take_completion_requests() == vec![TID.to_owned()],
            "{txnv:?}: the abort is queued for completion"
        );
        check!(
            coordinator.complete_prepared_transaction(TID, txnv).await
                == CompletionAttempt::Completed,
            "{txnv:?}: the completion task finishes the abort"
        );

        let aborted = coordinator
            .get(TID)
            .expect("the transaction is still tracked")
            .lock()
            .await
            .clone();
        let expected = TxnEntry {
            state: TxnState::CompleteAbort,
            producer_epoch: HELD_EPOCH + 1,
            last_producer_epoch: last_epoch,
            client_transaction_version: stamp,
            start_ms: 0,
            last_update_ms: aborted.last_update_ms,
            ..TxnEntry::new_empty(TID.to_owned(), PID, HELD_EPOCH + 1, 1, 0)
        };
        check!(aborted == expected, "{txnv:?}: the entry after the abort");
        let marker = data
            .read_log(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read the data partition")
            .batches
            .first()
            .map(|batch| batch.producer_epoch);
        check!(
            marker == Some(HELD_EPOCH + 1),
            "{txnv:?}: the abort marker fences the epoch the producer held"
        );

        // The zombie that resumes at the epoch it held is fenced instead of
        // starting a new transaction.
        let zombie = coordinator
            .register_partitions(TID, PID, HELD_EPOCH, vec![orders], txnv, 4)
            .await;
        check!(
            zombie == codes::PRODUCER_FENCED,
            "{txnv:?}: the timed-out producer is fenced"
        );
    }
}

/// A sweep never waits for a marker. Kafka's `abortTimedOutTransactions`
/// appends `PrepareAbort` and hands the markers to its marker channel, so a
/// partition that cannot commit a marker delays no other transaction's
/// timeout. Here one transaction's abort is already prepared and every
/// marker fan-out is held, as one is while its partition cannot commit, and
/// the sweep still prepares the abort of a second timed-out transaction and
/// queues both for completion.
#[tokio::test]
async fn a_held_marker_fan_out_does_not_stop_the_sweep() {
    let txnv = TxnVersion::Verified;
    let dir = tempfile::tempdir().expect("tempdir");
    let (coordinator, _data) = live_coordinator(dir.path()).await;
    let orders = TopicPartition {
        topic: DATA_TOPIC.to_owned(),
        partition: PartitionIndex(0),
    };
    let mut stuck = prepared_entry_with_timeout("tid-stuck", 1000, 4, 1);
    stuck.partitions.insert(orders.clone());
    let mut timed_out = TxnEntry::new_empty("tid-timed-out".to_owned(), ProducerId(2000), 3, 1, 0);
    timed_out.state = TxnState::Ongoing;
    timed_out.start_ms = 0;
    timed_out.partitions.insert(orders);
    for seeded in [stuck, timed_out] {
        coordinator
            .put(seeded, txnv)
            .await
            .expect("seed a transaction");
    }
    coordinator
        .marker_fanout_gate
        .set_mode(MarkerFanoutMode::Hold);

    let swept = time::timeout(
        Duration::from_secs(10),
        coordinator.sweep_expired(10_000, txnv),
    )
    .await
    .expect("the sweep does not wait for a held marker fan-out");

    let mut swept = swept;
    swept.sort_unstable();
    let mut queued = coordinator.take_completion_requests();
    queued.sort_unstable();
    let state = coordinator
        .get("tid-timed-out")
        .expect("the timed-out transaction is tracked")
        .lock()
        .await
        .state;
    let both = vec!["tid-stuck".to_owned(), "tid-timed-out".to_owned()];
    check!((swept, queued, state) == (both.clone(), both, TxnState::PrepareAbort));
    coordinator
        .marker_fanout_gate
        .set_mode(MarkerFanoutMode::Open);
}
