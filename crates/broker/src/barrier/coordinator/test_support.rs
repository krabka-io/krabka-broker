//! The fixture that the coordinator unit tests share.
//!
//! The module opens the state partitions and the data partitions of one
//! cluster, and it builds a coordinator over them. Every test module under
//! this coordinator needs that setup, so it lives in one file.

use std::sync::Arc;

use krabka_metadata::{MetadataRecord, NodeId};
use krabka_units::{Time, millis};
use tempfile::{TempDir, tempdir};

use super::BarrierCoordinator;
use crate::{
    barrier::{
        STATE_TOPIC,
        config::BarrierConfig,
        metrics::NoBarrierMetrics,
        state::GroupSpec,
        test_support::{metadata_source, open_partition, topic_records},
    },
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    test_support::FakeMetadataSource,
};

pub(super) const GROUP: &str = "orders-cut";

fn config() -> BarrierConfig {
    BarrierConfig {
        state_topic_num_partitions: 4,
        injection_timeout: millis(30),
        retry_backoff: millis(1),
        retry_backoff_max: millis(2),
        ..BarrierConfig::default()
    }
}

pub(super) fn spec(topics: &[&str], interval: Option<Time>, retained_cuts: i32) -> GroupSpec {
    GroupSpec {
        topics: topics.iter().map(|t| (*t).to_owned()).collect(),
        interval,
        retained_cuts,
    }
}

fn cluster_records() -> Vec<MetadataRecord> {
    [
        topic_records(STATE_TOPIC, 4, NodeId(1)),
        topic_records("orders", 2, NodeId(1)),
        topic_records("payments", 1, NodeId(1)),
    ]
    .concat()
}

// A broker that leads every state partition and every data partition.
pub(super) struct Fixture {
    _dir: TempDir,
    pub(super) registry: Arc<PartitionRegistry>,
    pub(super) source: Arc<FakeMetadataSource>,
    config: BarrierConfig,
}

impl Fixture {
    // Every partition of the cluster is open here, and this broker leads
    // all of them.
    pub(super) fn new() -> Self {
        Self::with_data_partitions(&[("orders", 2), ("payments", 1)])
    }

    // Open the state partitions, and only the named data partitions.
    pub(super) fn with_data_partitions(data: &[(&str, i32)]) -> Self {
        let dir = tempdir().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        for p in 0..4 {
            open_partition(&registry, dir.path(), STATE_TOPIC, p);
        }
        for (topic, count) in data {
            for p in 0..*count {
                open_partition(&registry, dir.path(), topic, p);
            }
        }
        Self {
            _dir: dir,
            registry,
            source: Arc::new(metadata_source(&cluster_records())),
            config: config(),
        }
    }

    pub(super) async fn coordinator(&self) -> BarrierCoordinator {
        for (topic, count) in [("orders", 2), ("payments", 1)] {
            for partition in 0..count {
                if let Some(handle) = self
                    .registry
                    .get(topic, krabka_ids::PartitionIndex(partition))
                {
                    handle.install_leader_change(NodeId(1).get(), 3).await;
                }
            }
        }
        let controller: Arc<dyn MetadataSource> = Arc::clone(&self.source) as _;
        let coordinator = BarrierCoordinator::new(
            NodeId(1),
            Arc::clone(&self.registry),
            controller,
            self.config.clone(),
            Arc::new(NoBarrierMetrics),
        );
        coordinator
            .refresh_leader_partitions(&self.source.current_image())
            .await;
        coordinator
    }

    // A coordinator that replayed the state partitions from the log.
    pub(super) async fn recovered(&self) -> BarrierCoordinator {
        let coordinator = self.coordinator().await;
        coordinator
            .recover(&self.source.current_image())
            .await
            .expect("recovery succeeds");
        coordinator
    }
}
