//! The authoritative topic override record an `AlterConfigs` topic resource
//! becomes.
//!
//! `AlterConfigs` replaces a topic's whole override map, so the per-key
//! validation and the cross-key combination rules both apply to the map this
//! module builds.
//!
//! The create-only keys are the one thing here that does read the topic's
//! current overrides. A replacement that omitted one would drop it, and for
//! `krabka.diskless` that silently un-disklesses a live topic, so those keys
//! are carried forward instead. See
//! [`config_keys::preserve_create_only_topic_configs`].

use krabka_metadata::{MetadataRecord, TopicConfigRecord};
use krabka_protocol::owned::alter_configs_request::AlterConfigsResource;

use crate::{codes, config_keys};

/// Build the authoritative `V1TopicConfig` record for a topic resource. The
/// request carries the *complete* set of non-default values, so the map this
/// builds is the whole override map and the cross-key rules apply to it.
pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Result<MetadataRecord, (i16, String)> {
    if image.topic(&resource.resource_name).is_none() {
        return Err((
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            format!("unknown topic `{}`", resource.resource_name),
        ));
    }
    let mut overrides = std::collections::BTreeMap::new();
    for cfg in &resource.configs {
        // A controller-managed key is never stored, so it cannot take part
        // in the replacement. The check comes before the whitelist, so the
        // operator reads the refusal that names `krabka-guard` and not
        // `unrecognized config key`.
        if config_keys::is_controller_managed_topic_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&cfg.name),
            ));
        }
        let value = cfg.value.clone().unwrap_or_default();
        config_keys::validate_topic_config(&cfg.name, &value)
            .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
        overrides.insert(cfg.name.clone(), value);
    }
    // Carry every create-only override the request left out into the
    // replacement, and refuse the request outright if it changes one. This
    // runs before the cross-key rules, so a preserved `krabka.diskless=true`
    // still has to survive its exclusion with `remote.storage.enable=true`.
    config_keys::preserve_create_only_topic_configs(
        &mut overrides,
        image.topic_config(&resource.resource_name),
    )
    .map_err(|key| {
        (
            codes::INVALID_CONFIG,
            config_keys::create_only_topic_config_message(key),
        )
    })?;
    config_keys::validate_config_combination(&overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides,
    }))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::handlers::alter_configs::test_support::{
        image_with_topic, image_with_topic_config, topic_resource,
    };

    #[test]
    fn topic_replacement_rejects_compaction_on_a_scheduled_topic() {
        let image = image_with_topic("orders");

        let (code, message) = topic_config_record(
            &topic_resource(
                "orders",
                &[
                    (crate::config_keys::CLEANUP_POLICY, "compact"),
                    (crate::config_keys::DELIVERY_MODE, "scheduled"),
                ],
            ),
            &image,
        )
        .expect_err("compaction on a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(
            message.contains(crate::config_keys::CLEANUP_POLICY),
            "got: {message}"
        );
        assert!(
            message.contains(crate::config_keys::DELIVERY_MODE),
            "got: {message}"
        );
    }

    #[test]
    fn topic_replacement_accepts_a_scheduled_topic_without_compaction() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource(
                "orders",
                &[
                    (crate::config_keys::CLEANUP_POLICY, "delete"),
                    (crate::config_keys::DELIVERY_MODE, "scheduled"),
                ],
            ),
            &image,
        )
        .expect("a scheduled delete-policy topic is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            crate::config_keys::CLEANUP_POLICY.to_string() => "delete".to_string(),
            crate::config_keys::DELIVERY_MODE.to_string() => "scheduled".to_string()},
        });
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_rejects_a_changed_create_only_config() {
        for (key, default) in config_keys::CREATE_ONLY_TOPIC_CONFIGS {
            let image = image_with_topic_config("orders", &[(key, "true")]);

            let error = topic_config_record(&topic_resource("orders", &[(key, default)]), &image)
                .expect_err("turning a create-only key off must be rejected");

            check!(error.0 == codes::INVALID_CONFIG, "key {key}");
            check!(
                error.1 == config_keys::create_only_topic_config_message(key),
                "key {key}"
            );
        }
    }

    #[test]
    fn topic_replacement_carries_an_omitted_create_only_config_forward() {
        // The regression this guards: `AlterConfigs` sends the topic's whole
        // override map, so a request that changes only `retention.ms` leaves
        // `krabka.diskless` out. Dropping it there un-disklesses a live topic,
        // and the next supervisor reconcile tears down its WAL placement, stops
        // its followers and prunes its shard directories.
        for (key, _) in config_keys::CREATE_ONLY_TOPIC_CONFIGS {
            let image = image_with_topic_config("orders", &[(key, "true")]);

            let record = topic_config_record(
                &topic_resource("orders", &[(crate::config_keys::RETENTION_MS, "60000")]),
                &image,
            )
            .expect("an unrelated config change is valid on a create-only topic");

            let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".into(),
                overrides: maplit::btreemap! {
                key.to_string() => "true".to_string(),
                crate::config_keys::RETENTION_MS.to_string() => "60000".to_string()},
            });
            assert!(record == expected, "key {key}");
        }
    }

    #[test]
    fn topic_replacement_accepts_a_restated_create_only_config() {
        // `kafka-configs --describe` then `--alter` round-trips every value it
        // read back, so the request names the key with the value the topic
        // already has. That is not a change and must not be refused.
        for (key, _) in config_keys::CREATE_ONLY_TOPIC_CONFIGS {
            let image = image_with_topic_config("orders", &[(key, "true")]);

            let record = topic_config_record(
                &topic_resource(
                    "orders",
                    &[(key, "true"), (crate::config_keys::RETENTION_MS, "60000")],
                ),
                &image,
            )
            .expect("restating the current value is not a change");

            let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".into(),
                overrides: maplit::btreemap! {
                key.to_string() => "true".to_string(),
                crate::config_keys::RETENTION_MS.to_string() => "60000".to_string()},
            });
            assert!(record == expected, "key {key}");
        }
    }

    #[test]
    fn topic_replacement_rejects_turning_a_create_only_config_on() {
        for (key, _) in config_keys::CREATE_ONLY_TOPIC_CONFIGS {
            let image = image_with_topic("orders");

            let error = topic_config_record(&topic_resource("orders", &[(key, "true")]), &image)
                .expect_err("turning a create-only key on after creation must be rejected");

            check!(error.0 == codes::INVALID_CONFIG, "key {key}");
            check!(
                error.1 == config_keys::create_only_topic_config_message(key),
                "key {key}"
            );
        }
    }

    #[test]
    fn topic_replacement_rejects_controller_managed_configs() {
        let image = image_with_topic("orders");

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS {
            // A full replacement carries a value for every key it names, so
            // both a `true` and an empty value are a write of the key.
            for value in ["true", "false", ""] {
                let error = topic_config_record(&topic_resource("orders", &[(key, value)]), &image)
                    .expect_err("controller-managed key must be rejected");

                check!(
                    error.0 == codes::INVALID_CONFIG,
                    "key {key} value {value:?}"
                );
                check!(
                    error.1 == config_keys::controller_managed_topic_config_message(key),
                    "key {key} value {value:?}"
                );
                check!(!error.1.is_empty(), "key {key} value {value:?}");
            }
        }
    }

    #[test]
    fn topic_replacement_leaves_an_ordinary_config_unaffected() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[(crate::config_keys::RETENTION_MS, "60000")]),
            &image,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {crate::config_keys::RETENTION_MS.to_string() => "60000".to_string()},
        });
        assert!(record == expected);
    }
}
