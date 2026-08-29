//! The periodic broker gauge sampler and the stretch-cluster leader-drift
//! count it reports. The sampler walks the metadata image on a timer, so it is
//! kept apart from the event-driven liveness services that share the same
//! startup phase.

use std::sync::Arc;

use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use crate::{config::BrokerConfig, partition_registry::PartitionRegistry};

/// Counts the partitions `node_id` leads from a site other than the stretch
/// cluster's preferred leader site.
///
/// The count is zero when the cluster pins leadership to no site. A node
/// whose registration names no rack does not sit in the preferred site, so
/// its partitions count as drift. `node_id` leads every partition the count
/// considers, so one rack lookup covers all of them.
fn leader_site_drift_partitions(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> usize {
    let Some(preferred) = crate::config_keys::resolve_preferred_leader_site(image) else {
        return 0;
    };
    let leader_site = image
        .broker(node_id)
        .and_then(|broker| broker.rack.as_deref());
    if leader_site == Some(preferred) {
        return 0;
    }
    image
        .all_partitions()
        .filter(|partition| partition.leader == node_id)
        .count()
}

pub(super) fn spawn_broker_gauge_updater(
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
    node_id: krabka_metadata::NodeId,
    metrics: crate::metrics::BrokerMetrics,
    config: &BrokerConfig,
    shutdown: CancellationToken,
) {
    let poll_interval = config.gauge_poll_interval;
    let default_min_insync_replicas = config.default_min_insync_replicas;
    let static_voter_count = config.controller_quorum_voters.len();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(poll_interval.to_std());
        let mut previous_voted_directory = None;
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                () = shutdown.cancelled() => return,
            }
            let led = partitions
                .arcs()
                .iter()
                .filter(|partition| {
                    partition
                        .current_leader
                        .load(std::sync::atomic::Ordering::Acquire)
                        == node_id
                })
                .count();
            metrics
                .partitions_led
                .set(i64::try_from(led).unwrap_or(i64::MAX));
            metrics
                .partitions_total
                .set(i64::try_from(partitions.len()).unwrap_or(i64::MAX));
            let image = controller.current_image();
            let alive = liveness.alive_snapshot().await;
            let minimum_isr: std::collections::HashMap<&str, i32> = image
                .topics()
                .map(|topic| {
                    let minimum = image
                        .topic_config(&topic.name)
                        .and_then(|config| config.get(crate::config_keys::MIN_INSYNC_REPLICAS))
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(default_min_insync_replicas);
                    (topic.name.as_str(), minimum)
                })
                .collect();
            let mut health = (0_usize, 0_usize, 0_usize);
            for partition in image.all_partitions() {
                if partition.leader == node_id {
                    health.0 += usize::from(partition.isr.len() < partition.replicas.len());
                    let minimum = minimum_isr
                        .get(partition.topic.as_str())
                        .copied()
                        .unwrap_or(default_min_insync_replicas);
                    health.1 += usize::from(
                        i32::try_from(partition.isr.len()).unwrap_or(i32::MAX) < minimum,
                    );
                }
                health.2 += usize::from(
                    partition.replicas.contains(&node_id) && !alive.contains(&partition.leader.0),
                );
            }
            metrics
                .under_replicated_partitions
                .set(i64::try_from(health.0).unwrap_or(i64::MAX));
            metrics
                .under_min_isr_partition_count
                .set(i64::try_from(health.1).unwrap_or(i64::MAX));
            metrics
                .offline_partitions_count
                .set(i64::try_from(health.2).unwrap_or(i64::MAX));
            metrics.leader_site_drift_partitions.set(
                i64::try_from(leader_site_drift_partitions(&image, node_id)).unwrap_or(i64::MAX),
            );
            metrics.witness_role.set(i64::from(u8::from(
                crate::config_keys::resolve_broker_witness(&image, node_id),
            )));
            let is_controller = controller
                .watch_leader()
                .borrow()
                .is_some_and(|leader| leader == node_id);
            metrics
                .active_controller
                .set(i64::from(u8::from(is_controller)));
            let ignored_static_voters =
                usize::from(image.kraft_version() >= 1).saturating_mul(static_voter_count);
            metrics
                .ignored_static_voters
                .set(i64::try_from(ignored_static_voters).unwrap_or(i64::MAX));
            let voted_directory = controller.voted_directory_id();
            if voted_directory != previous_voted_directory {
                if let Some(directory_id) = previous_voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&crate::metrics::DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(0);
                }
                if let Some(directory_id) = voted_directory {
                    metrics
                        .voted_directory
                        .get_or_create(&crate::metrics::DirectoryLabel {
                            directory_id: directory_id.to_string(),
                        })
                        .set(1);
                }
                previous_voted_directory = voted_directory;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{millis, secs};

    use super::*;
    use crate::broker::test_support::MockMetadataSource;

    #[tokio::test]
    async fn broker_gauge_uses_configured_default_min_isr() {
        let node_id = krabka_metadata::NodeId(1);
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "gauge-topic".into(),
                topic_id: uuid::Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            },
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "gauge-topic".into(),
                partition: 0,
                leader: node_id,
                replicas: vec![node_id],
                isr: vec![node_id],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            },
        ));
        let metrics = crate::metrics::BrokerMetrics::new();
        let mut config = BrokerConfig::for_tests(std::path::PathBuf::new());
        config.gauge_poll_interval = millis(1);
        config.default_min_insync_replicas = 2;
        let shutdown = CancellationToken::new();
        spawn_broker_gauge_updater(
            Arc::new(PartitionRegistry::new()),
            Arc::new(MockMetadataSource::new(image, None)),
            Arc::new(crate::heartbeat::controller_state::ControllerLivenessState::new(secs(10))),
            node_id,
            metrics.clone(),
            &config,
            shutdown.child_token(),
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while metrics.under_min_isr_partition_count.get() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("gauge observes configured minimum ISR");

        assert!(metrics.under_min_isr_partition_count.get() == 1);
        shutdown.cancel();
    }

    /// An image where node 1 sits in `rack` and leads `led` partitions of one
    /// topic, and node 2 sits in `dc-a` and leads one more.
    fn stretch_image(rack: &str, led: i32) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node_id, node_rack) in [(1_u64, rack), (2_u64, "dc-a")] {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    node_id: krabka_metadata::NodeId(node_id),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: Some(node_rack.to_string()),
                    log_dirs: Vec::new(),
                    endpoints: Vec::new(),
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: Some("dc-a".into()),
            },
        ));
        for partition in 0..=led {
            let leader = if partition < led { 1 } else { 2 };
            image.apply(&krabka_metadata::MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    topic: "stretch-topic".into(),
                    partition,
                    leader: krabka_metadata::NodeId(leader),
                    replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                    isr: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: Vec::new(),
                    removing_replicas: Vec::new(),
                    directories: Vec::new(),
                    partition_epoch: 0,
                },
            ));
        }
        image
    }

    #[test]
    fn leader_site_drift_counts_only_partitions_led_outside_the_preferred_site() {
        for (rack, led, expected) in [("dc-b", 2, 2), ("dc-a", 2, 0)] {
            let image = stretch_image(rack, led);

            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == expected,
                "rack {rack}"
            );
            // Node 2 leads one partition from the preferred site, so it never
            // drifts whatever node 1 does.
            check!(
                leader_site_drift_partitions(&image, krabka_metadata::NodeId(2)) == 0,
                "rack {rack}"
            );
        }
    }

    #[test]
    fn leader_site_drift_is_zero_when_the_cluster_pins_no_leader_site() {
        let mut image = stretch_image("dc-b", 2);
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: "stretch.preferred.leader.site".into(),
                config_value: None,
            },
        ));

        assert!(leader_site_drift_partitions(&image, krabka_metadata::NodeId(1)) == 0);
    }
}
