//! Shared fixtures for the transaction-coordinator unit tests.
//!
//! The builders make a `TxnCoordinator` with no live partitions and a plain
//! `TxnEntry`, so the tests in more than one submodule build the same fixture
//! without repeating it.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};

use super::TxnCoordinator;
use crate::{
    partition::Partition,
    partition_registry::PartitionRegistry,
    txn::{bootstrap, state::TxnEntry},
};

/// The topic of the data partition [`live_coordinator`] hosts.
pub(super) const DATA_TOPIC: &str = "orders";

/// Metadata for the single transaction-state partition, with explicit replicas.
pub(super) fn state_image(leader: NodeId, leader_epoch: i32, replicas: &[NodeId]) -> MetadataImage {
    state_image_with_id(1, leader, leader_epoch, replicas)
}

/// The same single-partition metadata with an explicit topic identity.
pub(super) fn state_image_with_id(
    topic_id: u128,
    leader: NodeId,
    leader_epoch: i32,
    replicas: &[NodeId],
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_owned(),
        topic_id: uuid::Uuid::from_u128(topic_id),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).expect("test replication factor"),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_owned(),
        partition: 0,
        leader,
        replicas: replicas.to_vec(),
        isr: replicas.to_vec(),
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        ..Default::default()
    }));
    image
}

pub(super) fn state_registry(dir: &Path) -> Arc<PartitionRegistry> {
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        crate::test_support::open_partition(dir, bootstrap::TOPIC, 0),
    );
    partitions
}

pub(super) fn coordinator_with_registry(
    node: NodeId,
    partitions: Arc<PartitionRegistry>,
    num_partitions: i32,
) -> TxnCoordinator {
    TxnCoordinator::new(
        node,
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        num_partitions,
        krabka_units::mebibytes(1),
    )
}

/// A coordinator that leads its one `__transaction_state` partition and hosts
/// the data partition `orders-0`, both as real logs under `dir`, so a marker
/// fan-out and an append succeed. It returns the data partition too, for a test
/// that reads the markers.
pub(super) async fn live_coordinator(dir: &Path) -> (Arc<TxnCoordinator>, Arc<Partition>) {
    let partitions = state_registry(dir);
    let data = crate::test_support::open_partition(dir, DATA_TOPIC, 0);
    // The metadata reconcile installs this broker, node 1, as the leader, so
    // the partition takes markers.
    data.install_leader_change(1, 0).await;
    partitions.insert(DATA_TOPIC.into(), PartitionIndex(0), Arc::clone(&data));
    let coordinator = Arc::new(coordinator_with_registry(NodeId(1), partitions, 1));
    let image = state_image(NodeId(1), 0, &[NodeId(1)]);
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
    coordinator_with_registry(
        NodeId(1),
        Arc::new(PartitionRegistry::new()),
        num_partitions,
    )
}

pub(super) fn entry(pid: i64, prev: i64) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid-a".into(), ProducerId(pid), 0, 60_000, 0);
    e.prev_producer_id = ProducerId(prev);
    e
}
