//! The per-key broker config records an `AlterConfigs` broker resource
//! becomes, including the tombstones for overrides the replacement omits.
//!
//! Kafka's `AlterConfigs` is a full replacement, so this module both writes
//! the requested keys and deletes the ones the request left out. Keys that
//! only the controller writes stand outside that replacement: naming one is an
//! error, and the tombstone sweep skips them.

use krabka_metadata::{BrokerConfigRecord, MetadataRecord};
use krabka_protocol::owned::alter_configs_request::AlterConfigsResource;

use crate::{codes, config_keys};

pub(super) fn broker_config_records(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id = crate::handlers::incremental_alter_configs::broker_config_node_id(
        &resource.resource_name,
        image,
    )?;
    let mut replacement = std::collections::BTreeMap::new();
    for config in &resource.configs {
        if config_keys::is_controller_managed_broker_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is controller-managed and read-only",
                    config.name
                ),
            ));
        }
        if !crate::handlers::incremental_alter_configs::is_known_broker_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!("unknown broker config {}", config.name),
            ));
        }
        if node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
            && crate::handlers::incremental_alter_configs::is_cluster_default_topic_config(
                &config.name,
            )
        {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    config.name
                ),
            ));
        }
        let value = config.value.as_deref().ok_or_else(|| {
            (
                codes::INVALID_CONFIG,
                format!("broker config {} requires a value", config.name),
            )
        })?;
        crate::handlers::incremental_alter_configs::validate_broker_config_value(
            &config.name,
            value,
        )
        .map_err(|message| (codes::INVALID_CONFIG, message))?;
        replacement.insert(config.name.clone(), value.to_owned());
    }

    let current = image.broker_config(node_id);
    let capacity = replacement.len() + current.map_or(0, std::collections::BTreeMap::len);
    let mut records = Vec::with_capacity(capacity);
    records.extend(replacement.iter().map(|(name, value)| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: Some(value.clone()),
        })
    }));
    if let Some(current) = current {
        records.extend(
            current
                .keys()
                .filter(|name| {
                    !replacement.contains_key(*name)
                        && !config_keys::is_controller_managed_broker_config(name)
                })
                .map(|name| {
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id,
                        config_name: name.clone(),
                        config_value: None,
                    })
                }),
        );
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::handlers::alter_configs::test_support::{broker_resource, image_with_broker};

    #[test]
    fn broker_full_replacement_sets_requested_and_deletes_omitted_configs() {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
            config_value: Some("1024".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
            config_value: Some("512".into()),
        }));

        let records = broker_config_records(
            &broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
            &image,
        )
        .expect("valid broker replacement");

        let expected = vec![
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::NodeId(1),
                config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
                config_value: Some("2048".into()),
            }),
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::NodeId(1),
                config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                config_value: None,
            }),
        ];
        assert!(records == expected);
    }

    #[test]
    fn broker_full_replacement_accepts_cluster_default_resource() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let records = broker_config_records(
            &broker_resource(
                "",
                &[
                    (crate::throttle::FOLLOWER_THROTTLED_RATE_KEY, "4096"),
                    (crate::config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
                ],
            ),
            &image,
        )
        .expect("valid broker default replacement");

        assert!(
            records
                == vec![
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                        config_value: Some("4096".into()),
                    }),
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: crate::config_keys::UNCLEAN_RECOVERY_STRATEGY.into(),
                        config_value: Some("Balanced".into()),
                    }),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_controller_managed_configs() {
        let image = image_with_broker(1);
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for resource_name in ["1", ""] {
                let error = broker_config_records(
                    &broker_resource(resource_name, &[(key, "true")]),
                    &image,
                )
                .expect_err("controller-managed key must be rejected");

                check!(error.0 == codes::INVALID_CONFIG, "key {key}");
                check!(
                    error.1 == format!("broker config {key} is controller-managed and read-only"),
                    "key {key}"
                );
            }
        }
    }

    #[test]
    fn broker_full_replacement_leaves_controller_managed_configs_alone() {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
            config_value: Some("512".into()),
        }));

        let records = broker_config_records(
            &broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
            &image,
        )
        .expect("valid broker replacement");

        assert!(
            records
                == vec![
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::NodeId(1),
                        config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
                        config_value: Some("2048".into()),
                    }),
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::NodeId(1),
                        config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                        config_value: None,
                    }),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_per_broker_recovery_setting() {
        let image = image_with_broker(1);

        let error = broker_config_records(
            &broker_resource(
                "1",
                &[(crate::config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced")],
            ),
            &image,
        )
        .expect_err("per-broker recovery setting must be rejected");

        assert!(error.0 == codes::INVALID_CONFIG);
    }
}
