//! Shared fixtures for the `ShareCoordinator` unit tests: a `StateBatch`
//! builder, a real `__share_group_state` partition with a live writer, a
//! coordinator that owns all of them, and a leadership seed.
//!
//! Every submodule of `coordinator` needs the same live partition logs, so the
//! builders live in one place instead of once per test module.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};

use super::ShareCoordinator;
use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{bootstrap, config::ShareCoordinatorConfig, persistence::StateBatch},
};

pub(super) fn batch(first: i64, last: i64) -> StateBatch {
    StateBatch {
        first_offset: Offset(first),
        last_offset: Offset(last),
        delivery_state: 0,
        delivery_count: 1,
    }
}

/// Builds a real `__share_group_state`-`p` partition and registers it.
///
/// The partition has a live writer. This function mirrors
/// `fixture_partition` in `partition_registry`.
pub(super) fn open_state_partition(reg: &PartitionRegistry, log_dir: &Path, p: i32) {
    let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, p);
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = Log::open(&part_dir, LogConfig::default()).unwrap();
    let part = crate::broker::spawn_partition(
        bootstrap::TOPIC.to_string(),
        PartitionIndex(p),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    reg.insert(bootstrap::TOPIC.into(), PartitionIndex(p), part);
}

/// A coordinator that leads every state partition it touches.
///
/// All 50 `__share_group_state` partitions are open locally.
pub(super) fn coordinator(dir: &Path) -> (ShareCoordinator, Arc<PartitionRegistry>) {
    let reg = Arc::new(PartitionRegistry::new());
    for p in 0..ShareCoordinatorConfig::default().state_topic_num_partitions {
        open_state_partition(&reg, dir, p);
    }
    let coord = ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    );
    (coord, reg)
}

pub(super) async fn lead_all(coord: &ShareCoordinator) {
    coord.lead_all_partitions_for_test().await;
}

/// A metadata image that holds one data topic, `t`, with id `topic_id` and
/// `partitions` partitions.
pub(crate) fn image_with_topic(
    topic_id: uuid::Uuid,
    partitions: i32,
) -> krabka_metadata::MetadataImage {
    let mut records = vec![krabka_metadata::MetadataRecord::V1Topic(
        krabka_metadata::TopicRecord {
            name: "t".to_owned(),
            topic_id,
            partitions,
            replication_factor: 1,
        },
    )];
    for partition in 0..partitions {
        records.push(krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "t".to_owned(),
                partition,
                leader: krabka_metadata::NodeId(1),
                replicas: vec![krabka_metadata::NodeId(1)],
                isr: vec![krabka_metadata::NodeId(1)],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            },
        ));
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

/// A `WriteShareGroupState` partition.
pub(crate) fn share_write(
    epochs: (i32, i32),
    progress: (i64, i32),
    batches: Vec<StateBatch>,
) -> super::ShareWrite {
    super::ShareWrite {
        state_epoch: epochs.0,
        leader_epoch: epochs.1,
        start_offset: Offset(progress.0),
        delivery_complete_count: progress.1,
        batches,
    }
}
