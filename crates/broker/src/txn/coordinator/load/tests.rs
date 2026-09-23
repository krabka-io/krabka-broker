//! A leadership change moves `__transaction_state-0` between two coordinators
//! that share one partition log, as replication would.

use std::{path::Path, sync::Arc};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, ProducerId};
use krabka_metadata::{
    LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord,
};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    partition_registry::PartitionRegistry,
    txn::{
        state::{TopicPartition, TxnEntry, TxnState},
        version::TxnVersion,
    },
};

const TID: &str = "tid-moved";
const P0: PartitionIndex = PartitionIndex(0);

fn image(leader: NodeId, leader_epoch: i32) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_owned(),
        topic_id: Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_owned(),
        partition: 0,
        leader,
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1), NodeId(2)],
        leader_epoch: LeaderEpoch(leader_epoch),
        ..Default::default()
    }));
    image
}

fn open_state_partition(dir: &Path) -> Arc<crate::partition::Partition> {
    let part_dir = crate::log_dir::partition_dir(dir, bootstrap::TOPIC, 0);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    crate::broker::spawn_partition(
        bootstrap::TOPIC.to_owned(),
        P0,
        dir.to_path_buf(),
        Log::open(&part_dir, LogConfig::default()).expect("open log"),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    )
}

fn coordinator(node: NodeId, partitions: &Arc<PartitionRegistry>) -> Arc<TxnCoordinator> {
    Arc::new(TxnCoordinator::new(
        node,
        Arc::clone(partitions),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ))
}

fn prepared_entry() -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.to_owned(), ProducerId(1000), 4, 60_000, 0);
    entry.state = TxnState::PrepareCommit;
    // Every append stamps the client's transaction version on the record.
    entry.client_transaction_version = 2;
    entry.partitions.insert(TopicPartition {
        topic: "orders".to_owned(),
        partition: P0,
    });
    entry
}

/// What one coordinator shows for `TID` after a step.
#[derive(Debug, PartialEq, Eq)]
struct View {
    coordinator_error: Option<i16>,
    entry: Option<TxnEntry>,
    producer_id_maps_to: Option<String>,
}

async fn view(coordinator: &TxnCoordinator) -> View {
    let entry = match coordinator.get(TID) {
        Some(handle) => Some(handle.lock().await.clone()),
        None => None,
    };
    View {
        coordinator_error: coordinator.coordinator_error(TID).await,
        entry,
        producer_id_maps_to: coordinator.tid_for_pid(ProducerId(1000)),
    }
}

/// Kafka `TransactionCoordinator.onElection` loads the partition and hands
/// every `Prepare*` transaction to the marker channel. `onResignation` drops
/// the partition, and `getAndMaybeAddTransactionState` answers
/// `COORDINATOR_LOAD_IN_PROGRESS` while the load runs and `NOT_COORDINATOR`
/// after it.
#[tokio::test]
async fn a_leadership_change_loads_the_new_leader_and_unloads_the_old_one() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        P0,
        open_state_partition(dir.path()),
    );
    let first = coordinator(NodeId(1), &partitions);
    let second = coordinator(NodeId(2), &partitions);

    // Broker 1 leads at epoch 0 and writes a PrepareCommit.
    first
        .refresh_leader_partitions(&image(NodeId(1), 0))
        .await
        .finished()
        .await;
    drop(second.refresh_leader_partitions(&image(NodeId(1), 0)).await);
    first
        .put(prepared_entry(), TxnVersion::Verified)
        .await
        .expect("persist the prepared transaction");
    check!(first.take_completion_requests().is_empty());
    check!(
        view(&second).await
            == View {
                coordinator_error: Some(crate::codes::NOT_COORDINATOR),
                entry: None,
                producer_id_maps_to: None,
            }
    );

    // Broker 2 is elected at epoch 1. Its load waits while an append holds
    // the state-partition lock, and it answers COORDINATOR_LOAD_IN_PROGRESS.
    let held_append = second.state_partition_writes[0].lock().await;
    let loads = second.refresh_leader_partitions(&image(NodeId(2), 1)).await;
    check!(
        view(&second).await
            == View {
                coordinator_error: Some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS),
                entry: None,
                producer_id_maps_to: None,
            }
    );
    drop(held_append);
    loads.finished().await;
    check!(
        view(&second).await
            == View {
                coordinator_error: None,
                entry: Some(prepared_entry()),
                producer_id_maps_to: Some(TID.to_owned()),
            }
    );
    check!(second.take_completion_requests() == vec![TID.to_owned()]);

    // Broker 1 applies the same election. It resigns and drops the
    // transaction and its producer id.
    drop(first.refresh_leader_partitions(&image(NodeId(2), 1)).await);
    check!(
        view(&first).await
            == View {
                coordinator_error: Some(crate::codes::NOT_COORDINATOR),
                entry: None,
                producer_id_maps_to: None,
            }
    );
    let refused = first.put(prepared_entry(), TxnVersion::Verified).await;
    assert!(refused.is_err(), "a resigned coordinator must not append");
}

/// An append checks the generation it started in before it publishes, as
/// Kafka's `appendTransactionToLog` callback refuses a changed coordinator
/// epoch. A new term refuses the generation of the old one.
#[tokio::test]
async fn a_new_term_refuses_the_generation_of_the_old_term() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        P0,
        open_state_partition(dir.path()),
    );
    let coordinator = coordinator(NodeId(1), &partitions);
    coordinator
        .refresh_leader_partitions(&image(NodeId(1), 0))
        .await
        .finished()
        .await;

    let leaders = coordinator.leader_partitions.read().await;
    let generation = TxnCoordinator::require_loaded(&leaders, P0).expect("loaded");
    drop(leaders);
    let loads = coordinator
        .refresh_leader_partitions(&image(NodeId(1), 1))
        .await;
    loads.finished().await;
    let leaders = coordinator.leader_partitions.read().await;
    check!(TxnCoordinator::require_generation(&leaders, P0, generation).is_err());
    let newer = TxnCoordinator::require_loaded(&leaders, P0).expect("loaded again");
    check!(TxnCoordinator::require_generation(&leaders, P0, newer).is_ok());
}

/// #892: `last_producer_epoch` (`TransactionLogValue`'s `LastProducerEpoch`,
/// tag 4) is persisted, so it survives the entry moving to a new coordinator
/// through the log, and a retried `InitProducerId` naming it is still
/// admitted after the reload.
#[tokio::test]
async fn a_reload_preserves_last_producer_epoch_and_admits_its_retry() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        P0,
        open_state_partition(dir.path()),
    );
    let first = coordinator(NodeId(1), &partitions);
    let second = coordinator(NodeId(2), &partitions);

    first
        .refresh_leader_partitions(&image(NodeId(1), 0))
        .await
        .finished()
        .await;
    let mut entry = prepared_entry();
    // The entry's live epoch has moved past the one a failed epoch fence
    // recorded, per `prepareIncrementProducerEpoch`/`prepareProducerIdRotation`,
    // so a retry naming the recorded epoch is distinct from a fresh bump.
    entry.producer_epoch = 5;
    entry.last_producer_epoch = 4;
    entry.has_failed_epoch_fence = true;
    first
        .put(entry, TxnVersion::Verified)
        .await
        .expect("persist the entry with a recorded last epoch");

    second
        .refresh_leader_partitions(&image(NodeId(2), 1))
        .await
        .finished()
        .await;

    let reloaded = view(&second).await.entry.expect("reloaded from disk");
    check!(reloaded.last_producer_epoch == 4);

    let decision = krabka_verified::transaction::init_producer_id_identity_decision(
        reloaded.producer_id.get(),
        reloaded.producer_epoch,
        reloaded.last_producer_epoch,
        reloaded.prev_producer_id.get(),
        reloaded.producer_id.get(),
        reloaded.last_producer_epoch,
    );
    check!(
        decision == krabka_verified::transaction::InitProducerIdIdentityDecision::Retry,
        "a retry naming the persisted last epoch is admitted, not fenced"
    );
}

/// A replay that fails leaves the partition unloaded until the next election.
#[tokio::test]
async fn a_failed_load_answers_not_coordinator() {
    let dir = TempDir::new().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    let part = open_state_partition(dir.path());
    partitions.insert(bootstrap::TOPIC.into(), P0, Arc::clone(&part));
    // A value record under a transactional id that maps to another state
    // partition of a two-partition topic is misplaced, and the replay refuses
    // it.
    let misplaced = (0..1_000)
        .map(|n| format!("tid-{n}"))
        .find(|tid| crate::txn::partitioner::partition_for_tid(tid, 2) == 1)
        .expect("a transactional id in partition 1");
    let entry = TxnEntry::new_empty(misplaced.clone(), ProducerId(7), 0, 60_000, 0);
    let mut batch = krabka_protocol::records::RecordBatch::default();
    batch.records.push(krabka_protocol::records::Record {
        key: Some(crate::txn::log_record::encode_key(&misplaced).into()),
        value: Some(crate::txn::log_record::encode_value(&entry, TxnVersion::Verified).into()),
        ..Default::default()
    });
    part.produce_batch(batch).await.expect("append");
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        Arc::clone(&partitions),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        2,
        krabka_units::mebibytes(1),
    ));

    let recovered = coordinator.recover(&image(NodeId(1), 0)).await;

    check!(recovered.is_err());
    check!(coordinator.load_status(P0).await == Some(LoadStatus::Failed));
    let in_partition_0 = (0..1_000)
        .map(|n| format!("tid-{n}"))
        .find(|tid| coordinator.partition_for(tid) == P0)
        .expect("a transactional id in partition 0");
    check!(
        coordinator.coordinator_error(&in_partition_0).await == Some(crate::codes::NOT_COORDINATOR)
    );
    check!(coordinator.get(&misplaced).is_none());
}
