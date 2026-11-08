//! Propagation of a topic's config overrides from the metadata image onto
//! every locally-hosted partition of that topic.

use {
    super::TopicPartition,
    crate::{
        api_catalog::UnstableApiVersions,
        config_keys::{apply_to_log_config, dynamic_log_base, log_max_decompressed_record},
        log_dir_status::LogDirRegistry,
        partition_registry::PartitionRegistry,
    },
    krabka_ids::PartitionIndex,
    krabka_log::LogConfig,
    krabka_metadata::MetadataImage,
    std::collections::{BTreeMap, HashSet},
    tracing::warn,
};

/// Resolve the same effective settings before opening a log and on later updates.
pub fn topic_log_config(
    image: &MetadataImage,
    base: &LogConfig,
    node: krabka_metadata::NodeId,
    topic: &str,
    unstable: UnstableApiVersions,
) -> LogConfig {
    let base = LogConfig {
        max_decompressed_record: log_max_decompressed_record(image, node, topic, unstable),
        ..dynamic_log_base(image, node, base)
    };
    apply_to_log_config(image.topic_config(topic).unwrap_or(&BTreeMap::new()), &base)
}

/// Push topic-config overrides onto every locally-hosted partition in
/// `desired`. The call is idempotent, because the same `LogConfig` sent twice
/// is a cheap noop. Configuration swaps hold the log lock and never await
/// the writer's WAL durability work.
///
/// `base` is the static `LogConfig` the process started with. The dynamic
/// broker-wide defaults of `node` and of the cluster go over it first, so a
/// partition whose topic overrides a key gets the override, and every other
/// key gets `kafka-configs --entity-type brokers`' value, the way Kafka's
/// `DynamicLogConfig` rebuilds each log's config when one changes. The
/// reconcile that calls this runs on every metadata image, so a change to a
/// broker config reaches the logs on the next one.
///
/// Kafka trunk's `max.decompressed.message.bytes` reaches the log the same way,
/// resolved per topic through the topic override, the node's dynamic config and
/// the cluster default, and only when `unstable` says the broker serves trunk's
/// topic keys: under Kafka 4.3.1 a stored value of it is an unknown key that
/// nothing reads, so the log runs with no limit.
///
/// A partition on a directory `log_dir_status` holds offline is left alone, as
/// Kafka's `ReplicaManager.handleLogDirFailure` keeps it offline until the
/// broker restarts. A partition whose log cannot write the start checkpoint
/// that enabling remote storage needs keeps its old config, and a storage
/// failure takes its directory offline; see
/// [`Partition::apply_log_config_overrides`](crate::partition::Partition::apply_log_config_overrides).
pub async fn push_topic_configs(
    desired: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    image: &MetadataImage,
    base: &LogConfig,
    node: krabka_metadata::NodeId,
    unstable: UnstableApiVersions,
    log_dir_status: &LogDirRegistry,
) {
    let base = &dynamic_log_base(image, node, base);
    let empty: BTreeMap<String, String> = BTreeMap::new();
    for (topic, partition) in desired {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        if log_dir_status.is_offline(&part.log_dir.load()) {
            continue;
        }
        let overrides = image.topic_config(topic).unwrap_or(&empty);
        // `apply_to_log_config` leaves the field alone, so the topic's own
        // limit travels in the base it merges over.
        let topic_base = LogConfig {
            max_decompressed_record: log_max_decompressed_record(image, node, topic, unstable),
            ..base.clone()
        };
        if let Err(error) = part
            .apply_log_config_overrides(overrides, &topic_base, log_dir_status)
            .await
        {
            warn!(topic = %topic, partition, %error, "supervisor: log configuration update failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            api_catalog::UnstableApiVersions,
            replicator_supervisor::test_support::{
                await_until, default_log_config, materialized_partition_with_config,
                single_partition_image,
            },
        },
        assert2::assert,
        krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
        },
        std::collections::BTreeMap,
        uuid::Uuid,
    };

    #[bon::builder]
    async fn pushed_config(
        image: &MetadataImage,
        #[builder(default = default_log_config())] base: &'_ LogConfig,
        #[builder(default = UnstableApiVersions::Disabled)] unstable: UnstableApiVersions,
        #[builder(default = "log config updated")] what: &'_ str,
        ready: impl Fn(&LogConfig) -> bool,
    ) -> LogConfig {
        let (_dir, partitions) = materialized_partition_with_config(base);
        let desired = HashSet::from([("t".to_owned(), 0)]);
        push_topic_configs(
            &desired,
            &partitions,
            image,
            base,
            NodeId(1),
            unstable,
            &LogDirRegistry::default(),
        )
        .await;
        let part = partitions.get("t", PartitionIndex(0)).expect("partition");
        await_until(what, || {
            ready(&part.log.lock().expect("log lock").config_snapshot())
        })
        .await;
        part.log.lock().expect("log lock").config_snapshot()
    }

    /// An image with partition `t-0` led by node 1, whose topic overrides
    /// `retention.ms` to a minute.
    fn retention_minute_image() -> MetadataImage {
        let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
        let mut overrides = BTreeMap::new();
        overrides.insert("retention.ms".to_owned(), "60000".to_owned());
        img.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));
        img
    }

    #[tokio::test]
    async fn push_topic_configs_pushes_overrides_to_local_partition() {
        let img = retention_minute_image();

        let base = LogConfig {
            segment_size: krabka_units::mebibytes(1),
            ..LogConfig::default()
        };
        let snap = pushed_config()
            .image(&img)
            .base(&base)
            .what("retention.ms=60s applied to partition log")
            .ready(|config| config.retention == Some(krabka_units::minutes(1)))
            .call()
            .await;
        assert!(snap.retention == Some(krabka_units::minutes(1)));
        assert!(snap.segment_size == krabka_units::mebibytes(1));
    }

    /// A partition on an offline log directory stays as it was, as Kafka's
    /// `handleLogDirFailure` keeps it until the broker restarts: the reconcile
    /// does not write to a directory the broker has declared failed.
    #[tokio::test]
    async fn push_topic_configs_skips_a_partition_on_an_offline_dir() {
        let base = LogConfig::default();
        let (_dir, partitions) = materialized_partition_with_config(&base);
        let part = partitions.get("t", PartitionIndex(0)).expect("partition");
        let before = part.log.lock().expect("log lock").config_snapshot();
        let registry = LogDirRegistry::default();
        registry.mark_offline(&part.log_dir.load(), "test: disk failed");

        push_topic_configs(
            &HashSet::from([("t".to_owned(), 0)]),
            &partitions,
            &retention_minute_image(),
            &base,
            NodeId(1),
            UnstableApiVersions::Disabled,
            &registry,
        )
        .await;

        assert!(part.log.lock().expect("log lock").config_snapshot() == before);
    }

    /// Kafka 4.4's `max.decompressed.message.bytes` reaches the log a
    /// partition runs with, so the cleaner and the by-timestamp lookups read it,
    /// regardless of the unstable API switch.
    /// The limit is the topic's own, else the node's, else the cluster's.
    #[tokio::test]
    async fn push_topic_configs_carries_the_decompressed_record_limit_without_unstable_apis() {
        const KEY: &str = "max.decompressed.message.bytes";
        let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
        let node = NodeId(1);
        // (name, unstable flag, the topic's override, the broker layers, the
        // limit the log runs with).
        for (name, unstable, topic_override, broker, expected) in [
            (
                "trunk, the topic's own",
                UnstableApiVersions::Enabled,
                Some("4096"),
                vec![(cluster, "512")],
                Some(krabka_units::bytes(4096)),
            ),
            (
                "trunk, the cluster default",
                UnstableApiVersions::Enabled,
                None,
                vec![(cluster, "512")],
                Some(krabka_units::bytes(512)),
            ),
            (
                "trunk, the node beats the cluster",
                UnstableApiVersions::Enabled,
                None,
                vec![(cluster, "512"), (node, "2048")],
                Some(krabka_units::bytes(2048)),
            ),
            (
                "trunk, nothing set",
                UnstableApiVersions::Enabled,
                None,
                vec![],
                None,
            ),
            (
                "default broker, topic override",
                UnstableApiVersions::Disabled,
                Some("4096"),
                vec![],
                Some(krabka_units::bytes(4096)),
            ),
            (
                "default broker, cluster default",
                UnstableApiVersions::Disabled,
                None,
                vec![(cluster, "512")],
                Some(krabka_units::bytes(512)),
            ),
        ] {
            let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
            for (node_id, value) in broker {
                img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id,
                    config_name: KEY.into(),
                    config_value: Some(value.into()),
                }));
            }
            // A retention override rides along so the wait below can tell the
            // push has landed even when the limit stays unset.
            let mut overrides = BTreeMap::from([("retention.ms".to_owned(), "60000".to_owned())]);
            if let Some(value) = topic_override {
                overrides.insert(KEY.to_owned(), value.to_owned());
            }
            img.apply(&MetadataRecord::V1TopicConfig(
                krabka_metadata::TopicConfigRecord {
                    topic: "t".into(),
                    overrides,
                },
            ));

            let snap = pushed_config()
                .image(&img)
                .unstable(unstable)
                .what("the push reached the partition log")
                .ready(|config| config.retention == Some(krabka_units::minutes(1)))
                .call()
                .await;
            assert!(snap.max_decompressed_record == expected, "{name}");
        }
    }

    #[tokio::test]
    async fn push_topic_configs_with_no_overrides_uses_defaults() {
        let img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));

        // No overrides → the writer retains the default log config.
        let snap = pushed_config()
            .image(&img)
            .what("default retention applied to partition log")
            .ready(|config| config.retention == LogConfig::default().retention)
            .call()
            .await;
        assert!(snap.retention == LogConfig::default().retention);
    }

    /// Kafka's `DynamicLogConfig` rebuilds every live log's config from the
    /// broker's dynamic defaults. A key the topic does not override takes the
    /// cluster-wide default (`--entity-default`), a node's own value beats it,
    /// and a topic override beats both.
    #[tokio::test]
    async fn push_topic_configs_applies_the_dynamic_broker_defaults() {
        let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
        for (node_id, name, value) in [
            (DEFAULT_BROKER_CONFIG_NODE_ID, "log.retention.ms", "120000"),
            (
                DEFAULT_BROKER_CONFIG_NODE_ID,
                "message.max.bytes",
                "2097152",
            ),
            (NodeId(1), "message.max.bytes", "4194304"),
            (
                DEFAULT_BROKER_CONFIG_NODE_ID,
                "log.segment.bytes",
                "2097152",
            ),
        ] {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: name.into(),
                config_value: Some(value.into()),
            }));
        }
        // The topic overrides the segment size and nothing else.
        img.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides: BTreeMap::from([("segment.bytes".to_owned(), "1048576".to_owned())]),
            },
        ));

        let snap = pushed_config()
            .image(&img)
            .what("dynamic broker defaults applied to the partition log")
            .ready(|config| config.max_message_size == krabka_units::bytes(4_194_304))
            .call()
            .await;
        // The cluster default, the node's own override over it, and the
        // topic's override over the cluster default.
        assert!(snap.retention == Some(krabka_units::minutes(2)));
        assert!(snap.max_message_size == krabka_units::bytes(4_194_304));
        assert!(snap.segment_size == krabka_units::mebibytes(1));
    }
}
