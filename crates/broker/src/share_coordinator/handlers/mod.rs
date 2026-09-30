//! KIP-932 share-state persister RPC handlers (api keys 83–87). Each handler
//! decodes the typed request and delegates every `(topic, partition)` to the
//! matching coordinator method. The method answers per-partition
//! `NOT_COORDINATOR` when this broker does not lead the state partition, and
//! `COORDINATOR_LOAD_IN_PROGRESS` while the state partition loads. The handler
//! maps the result to a per-partition `error_code`.
//!
//! These are inter-broker RPCs. As in Kafka, every handler first checks
//! `ClusterAction` on `Cluster("kafka-cluster")` for the principal of the
//! connection. A broker that calls them needs that grant, or super-user
//! status.

pub(crate) mod delete;
pub(crate) mod initialize;
pub(crate) mod read;
pub(crate) mod read_summary;
pub(crate) mod write;

#[cfg(test)]
mod authorization_tests;

use crate::{broker::Broker, handlers::RequestContext};

/// Kafka's message for `CLUSTER_AUTHORIZATION_FAILED`, which
/// `toGlobalErrorResponse` puts on every partition row.
const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

/// Whether the authorizer denies `ClusterAction` on the cluster to the
/// principal of `ctx`.
fn cluster_action_denied(broker: &Broker, ctx: &RequestContext<'_>) -> bool {
    crate::handlers::cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    )
}

/// Builds Kafka's `toGlobalErrorResponse` for a share-state request: one
/// result row for each requested topic, and one partition row with
/// `CLUSTER_AUTHORIZATION_FAILED` and Kafka's message for each requested
/// partition.
macro_rules! cluster_authorization_failed {
    ($request:expr, $response:ident, $result:ident, $partition:ident) => {
        $response {
            results: $request
                .topics
                .iter()
                .map(|topic| $result {
                    topic_id: topic.topic_id,
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| $partition {
                            partition: partition.partition,
                            error_code: $crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                            error_message: Some(
                                super::CLUSTER_AUTHORIZATION_FAILED_MESSAGE.to_string(),
                            ),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    };
}
use cluster_authorization_failed;

#[cfg(test)]
pub(crate) mod test_support {
    use std::{path::Path, sync::Arc};

    use krabka_ids::{NodeId, PartitionIndex};
    use krabka_log::{Log, LogConfig, Offset};

    use crate::{
        broker::Broker,
        partition_registry::PartitionRegistry,
        share_coordinator::{
            bootstrap, config::ShareCoordinatorConfig, coordinator::ShareCoordinator,
            persistence::StateBatch,
        },
    };

    pub(crate) fn batch(first_offset: i64, last_offset: i64) -> StateBatch {
        StateBatch {
            first_offset: Offset(first_offset),
            last_offset: Offset(last_offset),
            delivery_state: 2,
            delivery_count: 3,
        }
    }

    fn open_state_partition(registry: &PartitionRegistry, log_dir: &Path, partition: i32) {
        let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, partition);
        std::fs::create_dir_all(&part_dir).expect("create state partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open state partition log");
        let part = crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            PartitionIndex(partition),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        registry.insert(bootstrap::TOPIC.into(), PartitionIndex(partition), part);
    }

    pub(crate) fn open_all_state_partitions(
        registry: &PartitionRegistry,
        log_dir: &Path,
        num_partitions: i32,
    ) {
        for partition in 0..num_partitions {
            open_state_partition(registry, log_dir, partition);
        }
    }

    pub(crate) fn coordinator(log_dir: &Path) -> Arc<ShareCoordinator> {
        coordinator_with(log_dir, ShareCoordinatorConfig::default())
    }

    /// [`coordinator`] with `config`, for a test of a rule that Kafka trunk
    /// added to the share coordinator.
    pub(crate) fn coordinator_with(
        log_dir: &Path,
        config: ShareCoordinatorConfig,
    ) -> Arc<ShareCoordinator> {
        let registry = Arc::new(PartitionRegistry::new());
        open_all_state_partitions(&registry, log_dir, config.state_topic_num_partitions);
        Arc::new(ShareCoordinator::with_wall_clock(
            NodeId(1),
            registry,
            config,
            crate::share_coordinator::coordinator::test_support::manual_clock(),
        ))
    }

    /// Creates the real `__share_group_state` topic through the active
    /// controller, with its configured shape, and waits until the share
    /// coordinator of `broker` has loaded every partition of it. A topic that
    /// exists already is kept.
    ///
    /// A test must not seed the leadership by hand on a live broker: the
    /// metadata reconcile loop applies the image again at any time, and an
    /// image without the topic drops every led partition.
    pub(crate) async fn lead_share_state_partitions(broker: &Broker) {
        let partitions = broker.config.share_coordinator.state_topic_num_partitions;
        let request = krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
            topics: vec![crate::auto_topic_creation::creatable_topic(
                &broker.config,
                bootstrap::TOPIC,
            )],
            ..Default::default()
        };
        let response = crate::topic_creator::TopicCreator::new(broker)
            .create_topic_without_principal(request)
            .await
            .expect("the controller answers CreateTopics");
        for row in &response.topics {
            assert2::assert!(
                [crate::codes::NONE, crate::codes::TOPIC_ALREADY_EXISTS].contains(&row.error_code),
                "create __share_group_state: {row:?}"
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                broker
                    .share_coordinator
                    .refresh_leader_partitions(&broker.controller.current_image())
                    .await
                    .finished()
                    .await;
                let mut active = true;
                for partition in 0..partitions {
                    active &= broker
                        .share_coordinator
                        .load_status(PartitionIndex(partition))
                        .await
                        == Some(crate::share_coordinator::coordinator::LoadStatus::Active);
                }
                if active {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every __share_group_state partition loads on this broker");
    }
}
