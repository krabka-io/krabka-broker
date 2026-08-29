//! The synthesised `write.freeze` entry a topic resource reports (KFC-9).
//!
//! A freeze lives in the freeze registry and never in a topic's override map,
//! so the topic branch of `describe_one` cannot read this key the way it reads
//! a stored override. This module answers it from the registry instead, and
//! holds the vocabulary the value is spelled in.

use krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult;

use super::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_TOPIC, make_entry};
use crate::config_keys;

/// The `write.freeze` value on a frozen topic, before the scope that matched.
///
/// The rest of the value is [`crate::freeze::freeze_target`], so a frozen
/// topic reads `frozen:prefixed:tenant-a.` or `frozen:literal:orders`.
const WRITE_FREEZE_FROZEN_PREFIX: &str = "frozen:";

/// The `write.freeze` value on a topic that accepts writes.
const WRITE_FREEZE_NOT_FROZEN: &str = "false";

/// The synthesised `write.freeze` entry for one topic (KFC-9).
///
/// The freeze registry answers the question, because a freeze is never stored
/// as a topic config. A frozen topic reports the scope that matched at
/// `DYNAMIC_TOPIC_CONFIG`, and every other topic reports
/// [`WRITE_FREEZE_NOT_FROZEN`] at `DEFAULT_CONFIG`. Both are read-only.
pub(super) fn write_freeze_entry(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> DescribeConfigsResourceResult {
    let (value, config_source) = crate::freeze::resolve::resolve_freeze_verdict(image, topic)
        .map_or_else(
            || (WRITE_FREEZE_NOT_FROZEN.to_owned(), CONFIG_SOURCE_DEFAULT),
            |verdict| {
                let target = crate::freeze::freeze_target(verdict.pattern_type, &verdict.scope);
                (
                    format!("{WRITE_FREEZE_FROZEN_PREFIX}{target}"),
                    CONFIG_SOURCE_DYNAMIC_TOPIC,
                )
            },
        );
    let mut entry = make_entry(config_keys::WRITE_FREEZE, &value, config_source);
    // Only the controller writes this key, so `kafka-configs` must show it
    // the way it shows every other read-only config.
    entry.read_only = true;
    entry
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::{assert, check};
    use krabka_metadata::{
        MetadataImage, MetadataRecord, PatternType, TopicConfigRecord, TopicFreezeRecord,
    };
    use krabka_protocol::{
        UnknownTaggedFields, owned::describe_configs_response::DescribeConfigsResult,
    };
    use uuid::Uuid;

    use super::*;
    use crate::handlers::describe_configs::{resources::describe_one, wire::RESOURCE_TYPE_TOPIC};

    /// A metadata image whose freeze registry holds `entries`, and nothing
    /// else. A freeze never reaches a topic-config record, so the image needs
    /// no topic and no override map to answer the synthesised key.
    fn image_with_freezes(entries: &[(&str, PatternType)]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for (scope, pattern_type) in entries {
            image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
                scope: (*scope).to_owned(),
                pattern_type: *pattern_type,
                frozen: true,
                reason: "DR cutover".to_owned(),
                set_by: "User:alice".to_owned(),
                set_at_ms: 1_770_000_000_000,
                proposal_id: Uuid::nil(),
                key_id: String::new(),
                signature: Vec::new(),
            }));
        }
        image
    }

    fn describe_topic(
        image: &MetadataImage,
        topic: &str,
        configuration_keys: Option<Vec<String>>,
    ) -> DescribeConfigsResult {
        describe_one(
            image,
            krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.into(),
                configuration_keys,
                ..Default::default()
            },
            300_000,
            &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        )
    }

    /// The synthesised entry an operator should read, spelled out rather than
    /// built by the handler's own helper.
    fn freeze_entry(value: &str, config_source: i8) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult {
            name: config_keys::WRITE_FREEZE.to_string(),
            value: Some(value.to_string()),
            read_only: true,
            config_source,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0,
            documentation: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    fn topic_result(
        topic: &str,
        configs: Vec<DescribeConfigsResourceResult>,
    ) -> DescribeConfigsResult {
        DescribeConfigsResult {
            error_code: crate::codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: topic.to_string(),
            configs,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    #[test]
    fn topic_describe_synthesises_write_freeze_from_the_registry() {
        let image = image_with_freezes(&[
            ("orders", PatternType::Literal),
            ("tenant-a.", PatternType::Prefixed),
        ]);

        for (label, topic, value, config_source) in [
            (
                "a topic that a literal scope names",
                "orders",
                "frozen:literal:orders",
                CONFIG_SOURCE_DYNAMIC_TOPIC,
            ),
            (
                "a topic that a namespace prefix covers",
                "tenant-a.billing",
                "frozen:prefixed:tenant-a.",
                CONFIG_SOURCE_DYNAMIC_TOPIC,
            ),
            (
                "a topic that no scope covers, while the registry holds other entries",
                "tenant-b.orders",
                "false",
                CONFIG_SOURCE_DEFAULT,
            ),
            (
                "an internal topic, which is never freezable",
                "__consumer_offsets",
                "false",
                CONFIG_SOURCE_DEFAULT,
            ),
        ] {
            check!(
                describe_topic(&image, topic, None)
                    == topic_result(topic, vec![freeze_entry(value, config_source)]),
                "{label}"
            );
        }
    }

    #[test]
    fn topic_describe_reports_write_freeze_on_a_cluster_with_no_freeze() {
        let image = MetadataImage::new(Uuid::nil());

        // The key must not disappear when nothing is frozen. An absent key
        // reads the same as a broker without the feature.
        assert!(
            describe_topic(&image, "orders", None)
                == topic_result("orders", vec![freeze_entry("false", CONFIG_SOURCE_DEFAULT)])
        );
    }

    #[test]
    fn topic_describe_places_write_freeze_beside_the_stored_overrides() {
        let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::CLEANUP_POLICY.to_string() => "compact".to_string(),
            config_keys::RETENTION_MS.to_string() => "60000".to_string()},
        }));

        assert!(
            describe_topic(&image, "orders", None)
                == topic_result(
                    "orders",
                    vec![
                        make_entry(
                            config_keys::CLEANUP_POLICY,
                            "compact",
                            CONFIG_SOURCE_DYNAMIC_TOPIC,
                        ),
                        make_entry(
                            config_keys::RETENTION_MS,
                            "60000",
                            CONFIG_SOURCE_DYNAMIC_TOPIC,
                        ),
                        freeze_entry("frozen:literal:orders", CONFIG_SOURCE_DYNAMIC_TOPIC),
                    ],
                )
        );
    }

    #[test]
    fn topic_describe_applies_the_key_filter_to_the_synthesised_key() {
        let mut image = image_with_freezes(&[("orders", PatternType::Literal)]);
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {config_keys::RETENTION_MS.to_string() => "60000".to_string()},
        }));
        let retention = make_entry(
            config_keys::RETENTION_MS,
            "60000",
            CONFIG_SOURCE_DYNAMIC_TOPIC,
        );
        let frozen = freeze_entry("frozen:literal:orders", CONFIG_SOURCE_DYNAMIC_TOPIC);

        for (label, keys, expected) in [
            (
                "a filter that names the synthesised key alone",
                vec![config_keys::WRITE_FREEZE],
                vec![frozen.clone()],
            ),
            (
                "a filter that leaves the synthesised key out",
                vec![config_keys::RETENTION_MS],
                vec![retention.clone()],
            ),
            (
                "a filter that names both",
                vec![config_keys::RETENTION_MS, config_keys::WRITE_FREEZE],
                vec![retention.clone(), frozen.clone()],
            ),
            (
                "a filter that names neither",
                vec![config_keys::SEGMENT_BYTES],
                Vec::new(),
            ),
        ] {
            let configuration_keys = Some(keys.iter().map(|key| (*key).to_string()).collect());
            check!(
                describe_topic(&image, "orders", configuration_keys)
                    == topic_result("orders", expected),
                "{label}"
            );
        }
    }
}
