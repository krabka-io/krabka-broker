//! Shared fixtures for the `ShareCoordinator` unit tests: a `StateBatch`
//! builder, a real `__share_group_state` partition with a live writer, a
//! coordinator that owns all of them, and a leadership seed.
//!
//! Every submodule of `coordinator` needs the same live partition logs, so the
//! builders live in one place instead of once per test module.

use std::{collections::HashSet, path::Path, sync::Arc};

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
    reg.insert(bootstrap::TOPIC.to_string(), PartitionIndex(p), part);
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
    let mut set = HashSet::new();
    for p in 0..coord.config.state_topic_num_partitions {
        set.insert(PartitionIndex(p));
    }
    *coord.leader_partitions.write().await = set;
}
