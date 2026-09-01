//! The resolution of `min.insync.replicas` that the controller and the
//! produce path share.
//!
//! Kafka resolves a topic's `min.insync.replicas` through one layered lookup
//! -- `KafkaConfigSchema.resolveEffectiveTopicConfigs(staticNodeConfig,
//! dynamicClusterConfigs, dynamicNodeConfigs, dynamicTopicConfigs)` -- and
//! both halves of KIP-966 read it through that: the controller's
//! `ReplicationControlManager.getTopicEffectiveMinIsr` calls
//! `ConfigurationControlManager.getTopicConfig`, and the broker's produce
//! gate reads the same value off the partition's `LogConfig`. The two
//! agreeing is load-bearing. ELR names the replicas that are still known to
//! hold every committed record, and what is committed is exactly what the
//! produce gate accepted, so a controller that maintained ELR against a
//! higher threshold than the gate enforced would keep naming a replica that
//! writes had already moved past.
//!
//! Kafka does not leave the agreement to chance. Reconstructed from
//! `kafka-metadata-4.3.1.jar`, `ConfigurationControlManager` refuses two
//! alterations outright while the ELR feature is enabled:
//! `isDisallowedBrokerMinIsrTransition` rejects any *per-node*
//! `min.insync.replicas`, and `isDisallowedClusterMinIsrTransition` rejects
//! *removing* the cluster-wide one -- removal would drop resolution back to
//! each node's static config, which the controller cannot see.
//!
//! [`configured_min_insync_replicas`] is the layer both krabka paths share:
//! the topic override, then the cluster-wide dynamic broker default. krabka
//! has no per-node layer to disagree over, because it stores no per-node
//! `min.insync.replicas`. The one layer that stays split is the last one:
//! the broker falls back to its own `default_min_insync_replicas`
//! command-line value, and the controller cannot, because the answer decides
//! what it writes into the metadata log and a value that lives on one node's
//! command line is not what another node would compute from the same image.
//! So the controller falls back to Kafka's own default of 1.
//!
//! That residue can only run one way. The broker's fallback is used only
//! when neither dynamic layer names a value, where the controller resolves
//! 1, and a broker default is at least 1; every other case resolves the same
//! number on both sides, modulo the replication-factor cap the controller
//! applies and Kafka applies with it. So the controller's threshold is never
//! above the gate's, and the rule it drives -- clear the ELR once the ISR
//! reaches min ISR -- can only clear the set early. It cannot leave a
//! replica in it that an accepted write has moved past.

use super::{MIN_INSYNC_REPLICAS, lookup::topic_or_cluster_default};

/// Apache Kafka's `min.insync.replicas` default, used when neither the topic
/// nor the cluster-wide broker config names one.
const KAFKA_DEFAULT_MIN_INSYNC_REPLICAS: usize = 1;

/// The `min.insync.replicas` the metadata image names for `topic`: the topic
/// override, else the cluster-wide dynamic broker default, else `None`.
///
/// Both KIP-966 halves resolve through this, so that the threshold the
/// controller maintains the ELR against is the threshold the produce gate
/// enforces. `None` leaves each caller its own last resort: the broker's
/// command-line default on the produce side, Kafka's own default of 1 on the
/// controller side. See the module docs for why that residue is safe.
///
/// An unparseable value reads as `None`. The alter paths reject those, so a
/// string here that does not parse means a corrupt metadata image, and both
/// callers would rather fall back than fail the request.
pub(crate) fn configured_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<i32> {
    topic_or_cluster_default(image, topic, MIN_INSYNC_REPLICAS)?
        .parse::<i32>()
        .ok()
}

/// The effective `min.insync.replicas` of one partition, as Kafka's
/// `ReplicationControlManager.getTopicEffectiveMinIsr` computes it: the
/// resolved config value, capped by the replication factor.
///
/// The cap is what makes the KIP-966 rule "ELR is empty while the ISR is at
/// or above min ISR" reachable on a topic whose `min.insync.replicas` exceeds
/// its replication factor. Without it such a topic could never leave the
/// below-min state and every ISR change would move replicas into the ELR.
///
/// Kafka reads the replica count of partition 0 and applies it to the whole
/// topic; krabka passes the replica count of the partition it is deciding,
/// which is the same number on a topic whose partitions share a replication
/// factor.
pub(crate) fn effective_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    replication_factor: usize,
) -> usize {
    let configured = configured_min_insync_replicas(image, topic)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(KAFKA_DEFAULT_MIN_INSYNC_REPLICAS);
    configured.min(replication_factor)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
        TopicConfigRecord, TopicRecord,
    };

    use super::*;

    fn image(topic_override: Option<&str>, cluster_default: Option<&str>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 3,
        }));
        if let Some(value) = topic_override {
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), value.to_string())]
                    .into_iter()
                    .collect(),
            }));
        }
        if let Some(value) = cluster_default {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: MIN_INSYNC_REPLICAS.to_string(),
                config_value: Some(value.to_string()),
            }));
        }
        image
    }

    #[test]
    fn the_topic_override_wins_then_the_cluster_default_then_kafkas_own() {
        for (label, topic_override, cluster_default, replication_factor, expected) in [
            ("nothing published", None, None, 3, 1),
            ("cluster default only", None, Some("2"), 3, 2),
            ("topic override wins", Some("3"), Some("2"), 3, 3),
            ("an unparseable value falls back", Some("many"), None, 3, 1),
            ("the replication factor caps it", Some("5"), None, 3, 3),
            ("a cluster default is capped too", None, Some("9"), 2, 2),
        ] {
            check!(
                effective_min_insync_replicas(
                    &image(topic_override, cluster_default),
                    "t",
                    replication_factor,
                ) == expected,
                "{label}"
            );
        }
    }
}
