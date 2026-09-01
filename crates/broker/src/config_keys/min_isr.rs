//! The controller-side resolution of `min.insync.replicas`.
//!
//! The produce path resolves the key against the broker's own
//! `default_min_insync_replicas` command-line value, because a rejected
//! `acks=all` write is that broker's decision. The controller cannot: the
//! answer decides what it writes into the metadata log, and a value that
//! lives on one node's command line is not what another node would compute
//! from the same image. So this resolver reads the topic override, then the
//! cluster-wide default broker config, and finally Kafka's own default of 1.

use super::{MIN_INSYNC_REPLICAS, lookup::topic_or_cluster_default};

/// Apache Kafka's `min.insync.replicas` default, used when neither the topic
/// nor the cluster-wide broker config names one.
const KAFKA_DEFAULT_MIN_INSYNC_REPLICAS: usize = 1;

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
    let configured = topic_or_cluster_default(image, topic, MIN_INSYNC_REPLICAS)
        .and_then(|value| value.parse::<usize>().ok())
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
