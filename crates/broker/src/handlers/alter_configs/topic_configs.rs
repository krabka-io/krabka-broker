//! The authoritative topic override record an `AlterConfigs` topic resource
//! becomes.
//!
//! `AlterConfigs` replaces a topic's whole override map, so the per-key
//! validation and the cross-key combination rules both apply to the map this
//! module builds, and neither reads the topic's current overrides.
//!
//! Two rules do read them. `krabka.diskless` is fixed when the topic is
//! created, and a replacement that simply omits it would otherwise turn a
//! diskless topic back into a local-log one, so the resulting map is compared
//! against the stored one. See
//! [`crate::config_keys::validate_diskless_unchanged`].
//!
//! A stored controller-managed key is carried over. No client may name one, so
//! a replacement can never restate it, and a replacement that dropped it would
//! erase state the controller published: KIP-966's
//! [`ELIGIBLE_LEADER_REPLICAS`](crate::config_keys::ELIGIBLE_LEADER_REPLICAS)
//! would vanish the first time an operator set `retention.ms`, and every
//! `DescribeTopicPartitions` after that would report the partition as having
//! no eligible leader. Kafka has no such exposure: it carries the ELR on
//! `PartitionRegistration`, where no config path can reach it. The keys the
//! client sends are the whole *client* map; the record this builds is that map
//! plus what only the controller writes.

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
    let current = image.topic_config(&resource.resource_name);
    let mut overrides = std::collections::BTreeMap::new();
    for cfg in &resource.configs {
        // A controller-managed key has no client writer, so it cannot take
        // part in the replacement. The check comes before the whitelist, so
        // the operator reads the refusal that names what does write the key
        // and not `unrecognized config key`.
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
    config_keys::validate_config_combination(&overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    config_keys::validate_diskless_unchanged(current, &overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    // Both validations read the client's map alone, so the carry-over comes
    // after them: a controller-managed key is not the client's to be judged
    // on, and it takes part in no cross-key rule.
    overrides.extend(
        current
            .into_iter()
            .flatten()
            .filter(|(key, _)| config_keys::is_controller_managed_topic_config(key))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
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

    /// KIP-966 state survives a replacement that does not mention it. A
    /// client cannot name the key, so an `AlterConfigs` that replaces a
    /// topic's overrides would otherwise delete the ELR the controller keeps
    /// and leave `DescribeTopicPartitions` reporting an empty set until the
    /// next ISR change rebuilt one.
    #[test]
    fn topic_replacement_carries_the_controller_managed_state_forward() {
        let image = image_with_topic_config(
            "orders",
            &[
                (config_keys::ELIGIBLE_LEADER_REPLICAS, "0:2,3:"),
                (config_keys::RETENTION_MS, "60000"),
            ],
        );

        let record = topic_config_record(
            &topic_resource("orders", &[(config_keys::RETENTION_MS, "120000")]),
            &image,
        )
        .expect("an ordinary replacement is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::ELIGIBLE_LEADER_REPLICAS.to_string() => "0:2,3:".to_string(),
            config_keys::RETENTION_MS.to_string() => "120000".to_string()},
        });
        assert!(record == expected);
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
    fn topic_replacement_cannot_move_a_topic_between_data_paths() {
        /// The topic's stored overrides, the complete replacement the request
        /// carries, and whether the replacement is accepted.
        type Replacement<'a> = (
            &'a str,
            &'a [(&'a str, &'a str)],
            &'a [(&'a str, &'a str)],
            bool,
        );

        let cases: [Replacement<'_>; 6] = [
            (
                "a replacement that restates the diskless flag",
                &[(config_keys::DISKLESS, "true")],
                &[
                    (config_keys::DISKLESS, "true"),
                    (config_keys::RETENTION_MS, "60000"),
                ],
                true,
            ),
            (
                "a replacement that silently drops the diskless flag",
                &[(config_keys::DISKLESS, "true")],
                &[(config_keys::RETENTION_MS, "60000")],
                false,
            ),
            (
                "a replacement that turns the diskless flag off",
                &[(config_keys::DISKLESS, "true")],
                &[(config_keys::DISKLESS, "false")],
                false,
            ),
            (
                "a replacement that turns the diskless flag on",
                &[(config_keys::RETENTION_MS, "60000")],
                &[(config_keys::DISKLESS, "true")],
                false,
            ),
            (
                "a replacement that drops an explicit false, which changes nothing",
                &[(config_keys::DISKLESS, "false")],
                &[(config_keys::RETENTION_MS, "60000")],
                true,
            ),
            (
                "a replacement that adds tiered storage to a diskless topic",
                &[(config_keys::DISKLESS, "true")],
                &[
                    (config_keys::DISKLESS, "true"),
                    ("remote.storage.enable", "true"),
                ],
                false,
            ),
        ];

        for (label, stored, replacement, want_ok) in cases {
            let image = image_with_topic_config("orders", stored);

            let result = topic_config_record(&topic_resource("orders", replacement), &image);

            check!(result.is_ok() == want_ok, "{label}");
            if let Err((code, message)) = result {
                check!(code == codes::INVALID_CONFIG, "{label}");
                check!(
                    message.contains(config_keys::DISKLESS),
                    "{label}: {message}"
                );
            }
        }
    }

    /// A replacement drops every stored key it does not restate, and no client
    /// may restate a controller-managed one. The stored KIP-966 ELR state must
    /// therefore survive a replacement that names only ordinary keys;
    /// otherwise `kafka-configs --alter` on `retention.ms` would silently
    /// erase it and every later `DescribeTopicPartitions` would report the
    /// partition as having no eligible leader.
    #[test]
    fn topic_replacement_keeps_the_controller_managed_state_it_cannot_restate() {
        let image = image_with_topic_config(
            "orders",
            &[
                (config_keys::ELIGIBLE_LEADER_REPLICAS, "0:2:3"),
                (config_keys::RETENTION_MS, "60000"),
            ],
        );

        let record = topic_config_record(
            &topic_resource("orders", &[(config_keys::CLEANUP_POLICY, "delete")]),
            &image,
        )
        .expect("an ordinary replacement is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
                config_keys::CLEANUP_POLICY.to_string() => "delete".to_string(),
                config_keys::ELIGIBLE_LEADER_REPLICAS.to_string() => "0:2:3".to_string(),
            },
        });
        assert!(record == expected);
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
