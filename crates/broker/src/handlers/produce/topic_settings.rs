//! The per-topic produce settings that the handler resolves once per topic
//! out of the metadata image, before it walks that topic's partitions.

use crate::config_keys::{COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, parse_compression_type};

/// Resolve `min.insync.replicas` for a topic from the metadata image.
///
/// On a malformed value the function falls back to the broker default without
/// a message. The `AlterConfigs` validator already rejected the invalid
/// values, so any string here that does not parse means a corrupt metadata
/// image.
pub(super) fn topic_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    default_min_insync_replicas: i32,
) -> i32 {
    image
        .topic_config(topic)
        .and_then(|m| m.get(MIN_INSYNC_REPLICAS))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(default_min_insync_replicas)
}

/// Resolve a topic's broker-side `compression.type` from the metadata image.
///
/// `None` means Kafka's `producer` pass-through, with no recompression.
/// `Some(codec)` forces recompression of the batches whose codec differs. The
/// result matches the resolution that the partition writer applies through its
/// `LogConfig::compression_type`.
pub(super) fn resolve_topic_compression(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<krabka_compression::CompressionType> {
    image
        .topic_config(topic)
        .and_then(|m| m.get(COMPRESSION_TYPE))
        .and_then(|v| parse_compression_type(v).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_compression::CompressionType;
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
    use uuid::Uuid;

    use super::*;
    use crate::handlers::produce::test_support::{image_with_topic, set_min_isr};

    #[test]
    fn topic_min_isr_defaults_to_one_when_unset() {
        let img = image_with_topic("t", &[1, 2, 3]);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn topic_min_isr_reads_override_when_set() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        set_min_isr(&mut img, "t", 3);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 3);
    }

    #[test]
    fn topic_min_isr_uses_broker_fallback_unless_valid_override_exists() {
        let cases = [(None, 2), (Some(3), 3)];

        for (override_value, expected) in cases {
            let mut img = image_with_topic("t", &[1, 2, 3]);
            if let Some(value) = override_value {
                set_min_isr(&mut img, "t", value);
            }

            assert!(topic_min_insync_replicas(&img, "t", 2) == expected);
        }
    }

    #[test]
    fn topic_min_isr_default_one_on_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());
        assert!(
            topic_min_insync_replicas(&img, "ghost", 1) == 1,
            "missing topic_config must default to 1, not crash"
        );
    }

    #[test]
    fn topic_min_isr_default_one_on_malformed_value() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), "not-a-number".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(
            topic_min_insync_replicas(&img, "t", 1) == 1,
            "unparseable value must fall back to permissive default 1"
        );
    }

    #[test]
    fn topic_min_isr_handles_topic_config_without_min_isr_key() {
        // Topic has *some* override (e.g. retention.ms) but no
        // min.insync.replicas — still defaults to 1.
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert("retention.ms".into(), "60000".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn resolve_topic_compression_distinguishes_producer_and_forced_codecs() {
        let cases = [
            // "producer" keeps the producer's codec → no forced compression.
            ("producer", None),
            // A concrete codec forces recompression to that codec.
            ("zstd", Some(CompressionType::Zstd)),
        ];
        for (config_value, want) in cases {
            let mut img = image_with_topic("t", &[1]);
            let mut overrides = BTreeMap::new();
            overrides.insert(
                crate::config_keys::COMPRESSION_TYPE.into(),
                config_value.into(),
            );
            img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides,
            }));
            assert!(
                resolve_topic_compression(&img, "t") == want,
                "compression.type {config_value:?}"
            );
        }
    }
}
