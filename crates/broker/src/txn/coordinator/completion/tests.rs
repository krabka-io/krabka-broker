//! Tests for the completion of prepared transactions: the pure state and
//! decision helpers, and one completion attempt against a coordinator whose
//! `__transaction_state-0` partition and data partition are real logs.

use std::{path::Path, sync::Arc};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, ProducerId};
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    partition::Partition,
    partition_registry::PartitionRegistry,
    txn::{bootstrap, state::TopicPartition},
};

const TID: &str = "tid-completion";
const DATA_TOPIC: &str = "orders";

#[test]
fn only_a_prepare_state_has_a_completion() {
    let cases = [
        (
            TxnState::PrepareCommit,
            Some((MarkerType::Commit, TxnState::CompleteCommit)),
        ),
        (
            TxnState::PrepareAbort,
            Some((MarkerType::Abort, TxnState::CompleteAbort)),
        ),
        (TxnState::Empty, None),
        (TxnState::Ongoing, None),
        (TxnState::CompleteCommit, None),
        (TxnState::CompleteAbort, None),
        (TxnState::Dead, None),
    ];
    for (state, expected) in cases {
        check!(completion_for(state) == expected, "{state:?}");
    }
}

fn prepared_entry(state: TxnState) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 4, 60_000, 0);
    entry.state = state;
    // Every append stamps the client's transaction version on the record.
    entry.client_transaction_version = 2;
    entry.partitions.insert(TopicPartition {
        topic: DATA_TOPIC.to_owned(),
        partition: PartitionIndex(0),
    });
    entry
}

#[test]
fn completion_adopts_the_staged_identity_and_clears_the_transaction() {
    // (label, completion identity, expected prior producer ID)
    let cases = [
        ("same producer ID", (ProducerId(1000), 5), ProducerId(-1)),
        (
            "rotated producer ID",
            (ProducerId(2000), 0),
            ProducerId(1000),
        ),
    ];
    for (label, identity, prev_producer_id) in cases {
        let mut entry = prepared_entry(TxnState::PrepareCommit);
        apply_completion(&mut entry, TxnState::CompleteCommit, identity, 77);
        let expected = TxnEntry {
            producer_id: identity.0,
            producer_epoch: identity.1,
            state: TxnState::CompleteCommit,
            prev_producer_id,
            last_update_ms: 77,
            // The completion keeps the transaction version of the record it
            // completes.
            client_transaction_version: 2,
            ..TxnEntry::new_empty(TID.to_owned(), identity.0, identity.1, 60_000, 0)
        };
        check!(entry == expected, "{label}");
    }
}

#[test]
fn completion_decision_accepts_only_the_exact_prepared_snapshot() {
    let prepared = prepared_entry(TxnState::PrepareCommit);
    let pair = (TxnState::PrepareCommit, TxnState::CompleteCommit);

    check!(completion_decision(&prepared, &prepared, pair) == CompletionDecision::Proceed);

    let mut grown = prepared.clone();
    grown.partitions.insert(TopicPartition {
        topic: "payments".to_owned(),
        partition: PartitionIndex(1),
    });
    check!(
        completion_decision(&grown, &prepared, pair)
            == CompletionDecision::RejectChangedPreparedState
    );

    let mut completed = prepared.clone();
    completed.state = TxnState::CompleteCommit;
    check!(completion_decision(&completed, &prepared, pair) == CompletionDecision::AlreadyComplete);

    let mut other_result = prepared.clone();
    other_result.state = TxnState::CompleteAbort;
    check!(
        completion_decision(&other_result, &prepared, pair) != CompletionDecision::AlreadyComplete
    );
}

fn image(leader: NodeId, leader_epoch: i32) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_owned(),
        topic_id: Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_owned(),
        partition: 0,
        leader,
        replicas: vec![leader],
        isr: vec![leader],
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        ..Default::default()
    }));
    image
}

fn open_partition(dir: &Path, topic: &str) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(dir, topic, 0);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    crate::broker::spawn_partition(
        topic.to_owned(),
        PartitionIndex(0),
        dir.to_path_buf(),
        Log::open(&part_dir, LogConfig::default()).expect("open log"),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    )
}

/// A coordinator that persisted `entry` as the leader of
/// `__transaction_state-0`, after which `leader` was elected at a higher
/// leader epoch. `with_data_partition` hosts the data partition locally, so a
/// local marker fan-out can succeed.
async fn coordinator(
    entry: TxnEntry,
    leader: NodeId,
    with_data_partition: bool,
) -> (Arc<TxnCoordinator>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        open_partition(dir.path(), bootstrap::TOPIC),
    );
    if with_data_partition {
        partitions.insert(
            DATA_TOPIC.into(),
            PartitionIndex(0),
            open_partition(dir.path(), DATA_TOPIC),
        );
    }
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ));
    coordinator
        .refresh_leader_partitions(&image(NodeId(1), 0))
        .await
        .finished()
        .await;
    coordinator
        .put(entry, TxnVersion::Verified)
        .await
        .expect("seed __transaction_state");
    // The election of `leader` comes at a higher leader epoch. This broker
    // loads the partition again, or unloads it.
    coordinator
        .refresh_leader_partitions(&image(leader, 1))
        .await
        .finished()
        .await;
    (coordinator, dir)
}

async fn current(coordinator: &TxnCoordinator) -> Option<TxnEntry> {
    match coordinator.get(TID) {
        Some(entry) => Some(entry.lock().await.clone()),
        None => None,
    }
}

#[tokio::test]
async fn one_attempt_completes_retries_or_leaves_the_entry_alone() {
    struct Case {
        name: &'static str,
        entry: TxnEntry,
        leader: NodeId,
        with_data_partition: bool,
        attempt: CompletionAttempt,
        /// The state after the attempt, or `None` when this broker unloaded
        /// the transaction.
        state: Option<TxnState>,
    }
    let cases = [
        Case {
            name: "prepared commit completes",
            entry: prepared_entry(TxnState::PrepareCommit),
            leader: NodeId(1),
            with_data_partition: true,
            attempt: CompletionAttempt::Completed,
            state: Some(TxnState::CompleteCommit),
        },
        Case {
            name: "prepared abort completes",
            entry: prepared_entry(TxnState::PrepareAbort),
            leader: NodeId(1),
            with_data_partition: true,
            attempt: CompletionAttempt::Completed,
            state: Some(TxnState::CompleteAbort),
        },
        Case {
            name: "a failed marker fan-out retries",
            entry: prepared_entry(TxnState::PrepareCommit),
            leader: NodeId(1),
            with_data_partition: false,
            attempt: CompletionAttempt::Retry,
            state: Some(TxnState::PrepareCommit),
        },
        Case {
            name: "another coordinator owns it, and this broker unloaded it",
            entry: prepared_entry(TxnState::PrepareCommit),
            leader: NodeId(2),
            with_data_partition: true,
            attempt: CompletionAttempt::NothingToComplete,
            state: None,
        },
        Case {
            name: "an ongoing transaction is not prepared",
            entry: prepared_entry(TxnState::Ongoing),
            leader: NodeId(1),
            with_data_partition: true,
            attempt: CompletionAttempt::NothingToComplete,
            state: Some(TxnState::Ongoing),
        },
    ];
    for case in cases {
        let (coordinator, _dir) =
            coordinator(case.entry.clone(), case.leader, case.with_data_partition).await;
        let attempt = coordinator.complete_prepared_transaction(TID).await;
        check!(attempt == case.attempt, "{}", case.name);
        let after = current(&coordinator).await;
        check!(
            after.as_ref().map(|entry| entry.state) == case.state,
            "{}",
            case.name
        );
        if case.state == Some(case.entry.state) {
            check!(after == Some(case.entry), "{}: entry unchanged", case.name);
        }
    }
}

#[tokio::test]
async fn a_client_transaction_version_zero_completion_stays_classic() {
    // #892: completion honors the transaction version the record was
    // prepared under (`TransactionLogValue.ClientTransactionVersion`), not
    // the cluster's live level. A version-0 client never staged a recovery
    // identity, so completion leaves the epoch untouched and persists the
    // completed entry under `TxnVersion::Classic`, writing no v1 tags.
    //
    // The shared `coordinator()` fixture always seeds through
    // `TxnVersion::Verified`, which would stamp `client_transaction_version`
    // back to `2` before this test could observe the classic case, so this
    // seeds directly with `TxnVersion::Classic` instead.
    let mut entry = prepared_entry(TxnState::PrepareCommit);
    entry.client_transaction_version = 0;
    let epoch_before = entry.producer_epoch;

    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        open_partition(dir.path(), bootstrap::TOPIC),
    );
    partitions.insert(
        DATA_TOPIC.into(),
        PartitionIndex(0),
        open_partition(dir.path(), DATA_TOPIC),
    );
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ));
    coordinator
        .refresh_leader_partitions(&image(NodeId(1), 0))
        .await
        .finished()
        .await;
    coordinator
        .put(entry, TxnVersion::Classic)
        .await
        .expect("seed __transaction_state under TxnVersion::Classic");

    let attempt = coordinator.complete_prepared_transaction(TID).await;
    check!(attempt == CompletionAttempt::Completed);

    let after = current(&coordinator).await.expect("entry still tracked");
    check!(after.state == TxnState::CompleteCommit);
    check!(after.producer_epoch == epoch_before, "epoch not bumped");
    check!(after.client_transaction_version == 0, "stays classic");
}

#[tokio::test]
async fn recovery_queues_every_prepared_transaction_for_completion() {
    let (coordinator, _dir) =
        coordinator(prepared_entry(TxnState::PrepareCommit), NodeId(1), true).await;
    // The second load of the fixture queued the prepared transaction already.
    check!(coordinator.take_completion_requests() == vec![TID.to_owned()]);
    let mut ongoing = TxnEntry::new_empty("tid-ongoing".to_owned(), ProducerId(3000), 0, 60_000, 0);
    ongoing.state = TxnState::Ongoing;
    coordinator
        .put(ongoing, TxnVersion::Verified)
        .await
        .expect("persist an ongoing transaction");
    assert!(coordinator.take_completion_requests().is_empty());

    // A new election loads the partition again.
    coordinator
        .recover(&image(NodeId(1), 2))
        .await
        .expect("replay __transaction_state");

    check!(coordinator.take_completion_requests() == vec![TID.to_owned()]);
    check!(coordinator.take_completion_requests().is_empty());
}
