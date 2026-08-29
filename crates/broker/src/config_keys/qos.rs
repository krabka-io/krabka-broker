//! Krabka's `qos.tier` topic key, which partitions the producer quota buckets,
//! with the value check and the topic lookup that read it.

/// Krabka extension: per-topic `QoS` tier used to partition producer quota
/// buckets. Unset topics resolve to [`DEFAULT_QOS_TIER`].
pub(crate) const QOS_TIER: &str = "qos.tier";
pub(crate) const DEFAULT_QOS_TIER: &str = "default";

pub(super) fn validate_qos_tier(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("qos.tier must not be empty".into());
    }
    if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(format!(
            "qos.tier={value} not supported; expected non-empty ASCII letters, digits, '.', '_' or '-'"
        ))
    }
}

/// Resolve a topic's `QoS` tier, which partitions producer quota buckets.
/// Missing or corrupt values fall back to `default`. This matches the
/// permissive runtime behavior of other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_qos_tier<'a>(
    image: &'a krabka_metadata::MetadataImage,
    topic: &str,
) -> &'a str {
    image
        .topic_config(topic)
        .and_then(|m| m.get(QOS_TIER))
        .filter(|v| validate_qos_tier(v).is_ok())
        .map_or(DEFAULT_QOS_TIER, String::as_str)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{super::validation::validate_topic_config, *};

    #[test]
    fn validate_qos_tier_accepts_ascii_identifiers() {
        for v in ["default", "gold", "bulk_1", "critical-prod", "tier.2"] {
            assert!(validate_topic_config(QOS_TIER, v).is_ok(), "qos.tier={v}");
        }
    }

    #[test]
    fn validate_qos_tier_rejects_empty_or_unsafe_values() {
        for v in ["", "has space", "../escape", "ümlaut"] {
            assert!(validate_topic_config(QOS_TIER, v).is_err(), "qos.tier={v}");
        }
    }

    #[test]
    fn resolve_qos_tier_defaults_when_unset() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(resolve_qos_tier(&image, "t") == DEFAULT_QOS_TIER);
    }
}
