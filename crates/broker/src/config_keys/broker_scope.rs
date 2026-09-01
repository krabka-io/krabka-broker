//! The broker-scoped dynamic config keys: the controller-managed witness
//! role, broker fencing state and stretch-site role that `DescribeConfigs`
//! reports read-only, and the KIP-1075 deadline for remote `ListOffsets` work.

use std::time::Duration;

/// Marks a node as a data-bearing witness. The broker publishes this key for
/// itself, in the same metadata batch that carries its registration record,
/// so the controller can read the role from the metadata image.
///
/// The key is controller-managed and read-only. `AlterConfigs` and
/// `IncrementalAlterConfigs` reject it with `INVALID_CONFIG`, and
/// `DescribeConfigs` returns it with `read_only` set. An operator reads it
/// with `kafka-configs --entity-type brokers --describe`.
///
/// A witness replicates partition data and votes in `KRaft`, so it counts
/// toward `min.insync.replicas`. It serves no client and leads no partition.
pub(crate) const BROKER_WITNESS: &str = "broker.witness";

/// Names the site that should hold partition leadership in a stretch
/// cluster. The controller leader publishes it as a cluster-default broker
/// config, so every node that later becomes controller reads the same value.
///
/// The value is a `broker.rack` value. Site-aware placement puts a broker
/// from this site first in the replica list. In Kafka the preferred leader
/// is `replicas[0]`, so that ordering is what pins leadership.
///
/// The key is controller-managed and read-only, like [`BROKER_WITNESS`].
pub(crate) const STRETCH_PREFERRED_LEADER_SITE: &str = "stretch.preferred.leader.site";

/// The value krabka writes for [`BROKER_WITNESS`] on a witness node.
pub(crate) const WITNESS_TRUE: &str = "true";

/// Marks a node the controller currently treats as fenced: it is past its
/// KIP-500 heartbeat deadline, or it has not yet proved metadata catch-up.
/// This is Kafka's `BrokerRegistration.fenced()`, which the JVM controller
/// keeps in the metadata image and every broker reads back out of it.
///
/// `BrokerRegistrationRecord` lives in the protocol crate and carries no
/// fencing flag, so krabka publishes the state as a per-broker config
/// instead, exactly as it publishes the witness role. The controller leader
/// is the only writer; it republishes the state whenever its heartbeat
/// registry changes, and tombstones the key when the broker becomes
/// available again.
///
/// Replicating it is what lets a `Metadata` or `DescribeTopicPartitions`
/// request served by *any* node report the same `offline_replicas` as the
/// controller: `KRaftMetadataCache.isReplicaOffline` is
/// `fenced() || !hasOnlineDir(...)`, and only the second half of that was
/// quorum-replicated before this key existed.
///
/// The key is controller-managed and read-only, like [`BROKER_WITNESS`].
pub(crate) const BROKER_FENCED: &str = "broker.fenced";

/// The value krabka writes for [`BROKER_FENCED`] on a fenced node.
pub(crate) const FENCED_TRUE: &str = "true";

/// Broker-scoped config keys that only the controller writes. `AlterConfigs`
/// and `IncrementalAlterConfigs` must reject every key in this list, and
/// `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_BROKER_CONFIGS: [&str; 3] =
    [BROKER_WITNESS, BROKER_FENCED, STRETCH_PREFERRED_LEADER_SITE];

/// `true` when `key` is a broker config that only the controller writes.
pub(crate) fn is_controller_managed_broker_config(key: &str) -> bool {
    CONTROLLER_MANAGED_BROKER_CONFIGS.contains(&key)
}

/// Resolve [`BROKER_WITNESS`] for one node. A missing or unparseable value
/// resolves to `false`, so a cluster with no witness behaves as it did
/// before the role existed.
pub(crate) fn resolve_broker_witness(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> bool {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(BROKER_WITNESS))
        .map(String::as_str)
        == Some(WITNESS_TRUE)
}

/// Every registered node that carries the witness role.
///
/// The controller builds this set once for each scan and then excludes its
/// members from leader selection. Building it once keeps the scan a single
/// walk over the image rather than a lookup for each partition replica.
pub(crate) fn witness_node_ids(
    image: &krabka_metadata::MetadataImage,
) -> std::collections::HashSet<krabka_metadata::NodeId> {
    image
        .brokers()
        .filter(|broker| resolve_broker_witness(image, broker.node_id))
        .map(|broker| broker.node_id)
        .collect()
}

/// Resolve [`BROKER_FENCED`] for one node. Anything but the published
/// `true` resolves to "not fenced", so a node the controller has never
/// fenced reads as available on every broker.
pub(crate) fn resolve_broker_fenced(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> bool {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(BROKER_FENCED))
        .map(String::as_str)
        == Some(FENCED_TRUE)
}

/// Every registered node the controller has published as fenced.
///
/// This is the replicated half of the offline-replica projection, so it is
/// the same set on every node that holds the image, controller leader or
/// not. One walk over the image builds it, as [`witness_node_ids`] does.
pub(crate) fn fenced_node_ids(
    image: &krabka_metadata::MetadataImage,
) -> std::collections::HashSet<u64> {
    image
        .brokers()
        .filter(|broker| resolve_broker_fenced(image, broker.node_id))
        .map(|broker| broker.node_id.0)
        .collect()
}

/// Resolve [`STRETCH_PREFERRED_LEADER_SITE`] from the cluster defaults.
/// `None` means the cluster pins leadership to no site.
pub(crate) fn resolve_preferred_leader_site(
    image: &krabka_metadata::MetadataImage,
) -> Option<&str> {
    image
        .default_broker_config()?
        .get(STRETCH_PREFERRED_LEADER_SITE)
        .map(String::as_str)
}

/// KIP-98: how long a transactional id may sit in a terminal or idle state
/// before the transaction coordinator tombstones it out of
/// `__transaction_state`. Kafka defaults it to 604800000 ms (7 days).
///
/// The key is static in Kafka: `kafka-configs --alter` refuses it with
/// `Cannot update these configs dynamically`, and `DescribeConfigs` reports it
/// read-only from the node's own configuration. krabka reports it the same
/// way, out of [`crate::config::BrokerConfig::txn_id_expiration`].
pub(crate) const TRANSACTIONAL_ID_EXPIRATION_MS: &str = "transactional.id.expiration.ms";

/// KIP-98: how often the transactional-id expiry sweep runs. Kafka defaults it
/// to 3600000 ms (1 hour). Static and read-only, like
/// [`TRANSACTIONAL_ID_EXPIRATION_MS`].
pub(crate) const TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS: &str =
    "transaction.remove.expired.transaction.cleanup.interval.ms";

/// KIP-1075: server-side deadline for remote `ListOffsets` work when an older
/// request does not carry `timeout_ms`. Kafka exposes this as a dynamic broker
/// config and defaults it to 30 seconds.
pub(crate) const REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS: &str =
    "remote.list.offsets.request.timeout.ms";
pub(crate) const DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Parse KIP-1075's dynamic broker timeout.
pub(crate) fn parse_remote_list_offsets_timeout(value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<i32>()
        .map_err(|_| format!("{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be a positive int"))?;
    if millis <= 0 {
        return Err(format!(
            "{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be in 1..={}",
            i32::MAX
        ));
    }
    Ok(Duration::from_millis(
        u64::try_from(millis).expect("positive i32 fits u64"),
    ))
}

/// Resolve the per-broker KIP-1075 timeout over the cluster default.
pub(crate) fn resolve_remote_list_offsets_timeout(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> Duration {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        .or_else(|| {
            image
                .default_broker_config()
                .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        })
        .and_then(|value| parse_remote_list_offsets_timeout(value).ok())
        .unwrap_or(DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::{
        super::validation::{is_recognized, validate_topic_config},
        *,
    };

    /// Register `node_id` and, when `witness` is set, publish
    /// `broker.witness=true` for it. This is the path the broker takes at
    /// registration.
    fn register_node(
        img: &mut krabka_metadata::MetadataImage,
        node_id: u64,
        witness: Option<&str>,
    ) {
        use krabka_metadata::{
            BrokerConfigRecord, BrokerRegistrationRecord, MetadataRecord, NodeId,
        };
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".into(),
                port: 9_092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![],
                features: BTreeMap::new(),
            },
        ));
        if let Some(value) = witness {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(node_id),
                config_name: BROKER_WITNESS.into(),
                config_value: Some(value.into()),
            }));
        }
    }

    #[test]
    fn resolve_broker_witness_reads_only_an_exact_true() {
        use krabka_metadata::NodeId;
        // (published value, expected role)
        let cases = [
            (None, false),
            (Some(WITNESS_TRUE), true),
            (Some("false"), false),
            (Some("TRUE"), false),
            (Some(""), false),
            (Some("yes"), false),
        ];
        for (value, want) in cases {
            let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            register_node(&mut img, 1, value);
            assert!(
                resolve_broker_witness(&img, NodeId(1)) == want,
                "broker.witness={value:?}"
            );
        }
    }

    #[test]
    fn resolve_broker_witness_is_false_for_an_unregistered_node() {
        use krabka_metadata::NodeId;
        let img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(!resolve_broker_witness(&img, NodeId(7)));
    }

    #[test]
    fn resolve_broker_witness_does_not_read_the_cluster_default() {
        use krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
        };
        // The role is per node. A cluster default must not turn every broker
        // into a witness.
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        register_node(&mut img, 1, None);
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: BROKER_WITNESS.into(),
            config_value: Some(WITNESS_TRUE.into()),
        }));
        assert!(!resolve_broker_witness(&img, NodeId(1)));
    }

    #[test]
    fn witness_node_ids_collects_every_marked_node() {
        use std::collections::HashSet;

        use krabka_metadata::NodeId;
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(witness_node_ids(&img) == HashSet::new());
        register_node(&mut img, 1, None);
        register_node(&mut img, 2, Some(WITNESS_TRUE));
        register_node(&mut img, 3, Some("false"));
        register_node(&mut img, 4, Some(WITNESS_TRUE));
        assert!(witness_node_ids(&img) == maplit::hashset! {NodeId(2), NodeId(4)});
    }

    #[test]
    fn broker_witness_is_controller_managed_and_not_a_topic_config() {
        assert!(is_controller_managed_broker_config(BROKER_WITNESS));
        assert!(!is_recognized(BROKER_WITNESS));
        assert!(validate_topic_config(BROKER_WITNESS, WITNESS_TRUE).is_err());
    }

    #[test]
    fn broker_fenced_is_controller_managed_and_not_a_topic_config() {
        assert!(is_controller_managed_broker_config(BROKER_FENCED));
        assert!(!is_recognized(BROKER_FENCED));
        assert!(validate_topic_config(BROKER_FENCED, FENCED_TRUE).is_err());
    }

    /// Publish `broker.fenced` for `node_id`, the way the controller does.
    fn set_fenced(img: &mut krabka_metadata::MetadataImage, node_id: u64, value: Option<&str>) {
        use krabka_metadata::{BrokerConfigRecord, MetadataRecord, NodeId};
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(node_id),
            config_name: BROKER_FENCED.into(),
            config_value: value.map(Into::into),
        }));
    }

    #[test]
    fn fenced_nodes_are_the_registered_ones_the_controller_published() {
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        register_node(&mut img, 1, None);
        register_node(&mut img, 2, None);
        register_node(&mut img, 3, None);
        set_fenced(&mut img, 2, Some(FENCED_TRUE));
        // A tombstone means available again, and an unregistered node is
        // already offline by the registration rule.
        set_fenced(&mut img, 3, Some(FENCED_TRUE));
        set_fenced(&mut img, 3, None);
        set_fenced(&mut img, 9, Some(FENCED_TRUE));

        assert!(fenced_node_ids(&img) == maplit::hashset! {2});
        assert!(resolve_broker_fenced(&img, krabka_metadata::NodeId(2)));
        assert!(!resolve_broker_fenced(&img, krabka_metadata::NodeId(3)));
    }
}
