//! Topic-scoped resources for `IncrementalAlterConfigs`. The handler merges
//! the per-key operations onto the topic's current override map, validates
//! each new value and the resulting combination, and returns the
//! `V1TopicConfig` record that carries the merged map.

use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
use krabka_protocol::owned::incremental_alter_configs_request::AlterConfigsResource;

use super::{OP_DELETE, OP_SET};
use crate::{codes, config_keys};

pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
) -> Result<MetadataRecord, (i16, String)> {
    if image.topic(&resource.resource_name).is_none() {
        return Err((
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            format!("unknown topic `{}`", resource.resource_name),
        ));
    }
    let mut merged = image
        .topic_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for config in &resource.configs {
        // A controller-managed key is refused before the operation is read.
        // A DELETE of it is an attempt to clear the freeze, so every
        // operation gets the same refusal.
        if config_keys::is_controller_managed_topic_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&config.name),
            ));
        }
        match config.config_operation {
            OP_SET => {
                let value = config.value.clone().unwrap_or_default();
                config_keys::validate_topic_config(&config.name, &value)
                    .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
                merged.insert(config.name.clone(), value);
            }
            OP_DELETE => {
                if !config_keys::is_recognized(&config.name) {
                    return Err((
                        codes::INVALID_CONFIG,
                        format!("unrecognized config key `{}`", config.name),
                    ));
                }
                merged.remove(&config.name);
            }
            operation => {
                return Err((
                    codes::INVALID_CONFIG,
                    format!(
                        "config_operation={operation} (APPEND/SUBTRACT) not supported for key \
                         `{}` — only SET and DELETE are honored on this broker",
                        config.name
                    ),
                ));
            }
        }
    }
    // The ops alone cannot show a conflict: `merged` is what the topic ends up
    // with, so the cross-key rules are checked against that.
    config_keys::validate_config_combination(&merged)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides: merged,
    }))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;
    use crate::handlers::incremental_alter_configs::test_support::{
        image_with_topic_config, make_del_cfg, make_set_cfg, make_topic_resource,
    };

    #[test]
    fn topic_throttle_config_value_validated() {
        // Verify ThrottledReplicas::parse rejects malformed input that
        // the validator delegates to.
        assert!(crate::throttle::ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(crate::throttle::ThrottledReplicas::parse("0:bad").is_err());
    }

    #[test]
    fn controller_managed_topic_configs_are_rejected_whatever_the_operation() {
        let img = image_with_topic_config("orders", &[(config_keys::RETENTION_MS, "60000")]);

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS {
            for (label, config) in [
                ("a SET of the key", make_set_cfg(key, "true")),
                (
                    "a DELETE of the key, which asks to clear the freeze",
                    make_del_cfg(key),
                ),
                (
                    "an APPEND of the key",
                    AlterableConfig {
                        name: key.into(),
                        config_operation: 2,
                        value: Some("true".into()),
                        ..Default::default()
                    },
                ),
                (
                    "a SUBTRACT of the key",
                    AlterableConfig {
                        name: key.into(),
                        config_operation: 3,
                        value: Some("true".into()),
                        ..Default::default()
                    },
                ),
            ] {
                let error = topic_config_record(&make_topic_resource("orders", vec![config]), &img)
                    .expect_err("controller-managed key must be rejected");

                check!(error.0 == codes::INVALID_CONFIG, "{label}, key {key}");
                check!(
                    error.1 == config_keys::controller_managed_topic_config_message(key),
                    "{label}, key {key}"
                );
                check!(!error.1.is_empty(), "{label}, key {key}");
            }
        }
    }

    #[test]
    fn an_ordinary_topic_config_still_merges_onto_the_existing_map() {
        let img = image_with_topic_config("orders", &[(config_keys::RETENTION_MS, "60000")]);

        let record = topic_config_record(
            &make_topic_resource(
                "orders",
                vec![make_set_cfg(config_keys::SEGMENT_BYTES, "1048576")],
            ),
            &img,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_string() => "60000".to_string(),
            config_keys::SEGMENT_BYTES.to_string() => "1048576".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn compaction_op_conflicts_with_scheduled_delivery_already_on_the_topic() {
        // The ops alone are legal. Only the merge with the topic's existing
        // config shows the conflict.
        let img = image_with_topic_config("orders", &[(config_keys::DELIVERY_MODE, "scheduled")]);

        let (code, message) = topic_config_record(
            &make_topic_resource(
                "orders",
                vec![make_set_cfg(config_keys::CLEANUP_POLICY, "compact")],
            ),
            &img,
        )
        .expect_err("compaction merged onto a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(
            message.contains(config_keys::CLEANUP_POLICY),
            "got: {message}"
        );
        assert!(
            message.contains(config_keys::DELIVERY_MODE),
            "got: {message}"
        );
    }

    #[test]
    fn scheduled_delivery_op_conflicts_with_compaction_already_on_the_topic() {
        let img = image_with_topic_config("orders", &[(config_keys::CLEANUP_POLICY, "compact")]);

        let (code, _) = topic_config_record(
            &make_topic_resource(
                "orders",
                vec![make_set_cfg(config_keys::DELIVERY_MODE, "scheduled")],
            ),
            &img,
        )
        .expect_err("scheduling a compacted topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
    }

    #[test]
    fn deleting_the_delivery_mode_clears_the_conflict_in_the_same_request() {
        let img = image_with_topic_config("orders", &[(config_keys::DELIVERY_MODE, "scheduled")]);

        let record = topic_config_record(
            &make_topic_resource(
                "orders",
                vec![
                    make_del_cfg(config_keys::DELIVERY_MODE),
                    make_set_cfg(config_keys::CLEANUP_POLICY, "compact"),
                ],
            ),
            &img,
        )
        .expect("removing the schedule leaves a plain compacted topic");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {config_keys::CLEANUP_POLICY.to_string() => "compact".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn scheduled_delivery_keys_merge_onto_an_existing_topic_config() {
        let img = image_with_topic_config("retries", &[(config_keys::RETENTION_MS, "60000")]);

        let record = topic_config_record(
            &make_topic_resource(
                "retries",
                vec![
                    make_set_cfg(config_keys::DELIVERY_MODE, "scheduled"),
                    make_set_cfg(config_keys::DELIVERY_MAX_DELAY_MS, "3600000"),
                    make_set_cfg(config_keys::DELIVERY_SCHEDULE_MONOTONIC, "true"),
                ],
            ),
            &img,
        )
        .expect("valid scheduled delivery ops");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "retries".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_string() => "60000".to_string(),
            config_keys::DELIVERY_MODE.to_string() => "scheduled".to_string(),
            config_keys::DELIVERY_MAX_DELAY_MS.to_string() => "3600000".to_string(),
            config_keys::DELIVERY_SCHEDULE_MONOTONIC.to_string() => "true".to_string()},
        });
        assert!(record == expected);
    }
}
