//! Shared fixtures for the transaction-coordinator unit tests.
//!
//! The builders make a `TxnCoordinator` with no live partitions and a plain
//! `TxnEntry`, so the tests in more than one submodule build the same fixture
//! without repeating it.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, ProducerId};
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};

use super::TxnCoordinator;
use crate::{
    partition::Partition,
    partition_registry::PartitionRegistry,
    txn::{bootstrap, state::TxnEntry},
};

/// The topic of the data partition [`live_coordinator`] hosts.
pub(super) const DATA_TOPIC: &str = "orders";

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

/// A coordinator that leads its one `__transaction_state` partition and hosts
/// the data partition `orders-0`, both as real logs under `dir`, so a marker
/// fan-out and an append succeed. It returns the data partition too, for a test
/// that reads the markers.
pub(super) async fn live_coordinator(dir: &Path) -> (Arc<TxnCoordinator>, Arc<Partition>) {
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        open_partition(dir, bootstrap::TOPIC),
    );
    let data = open_partition(dir, DATA_TOPIC);
    // The metadata reconcile installs this broker, node 1, as the leader, so
    // the partition takes markers.
    data.install_leader_change(1, 0).await;
    partitions.insert(DATA_TOPIC.into(), PartitionIndex(0), Arc::clone(&data));
    let coordinator = Arc::new(TxnCoordinator::new(
        NodeId(1),
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ));
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_owned(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_owned(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1)],
        isr: vec![NodeId(1)],
        ..Default::default()
    }));
    coordinator
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    (coordinator, data)
}

pub(super) fn test_coordinator() -> TxnCoordinator {
    test_coordinator_with_partitions(50)
}

pub(super) fn test_coordinator_with_partitions(num_partitions: i32) -> TxnCoordinator {
    TxnCoordinator::new(
        krabka_metadata::NodeId(1),
        Arc::new(PartitionRegistry::new()),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        num_partitions,
        krabka_units::mebibytes(1),
    )
}

pub(super) fn entry(pid: i64, prev: i64) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid-a".into(), ProducerId(pid), 0, 60_000, 0);
    e.prev_producer_id = ProducerId(prev);
    e
}
