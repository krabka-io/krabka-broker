//! Group resources for `IncrementalAlterConfigs`, the KIP-848, KIP-932 and
//! KIP-1071 group configs. The handler merges the per-key operations onto
//! the group's current override map, checks the merged map with Kafka's
//! `GroupConfig.validate` against the broker's `StreamsGroupConfig` bounds,
//! and stages a `V1GroupConfig` record with that map.

use krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{
    OP_DELETE, OP_SET,
    topic_scope::{merge_list_op, not_a_list},
};
use crate::{
    api_catalog::UnstableApiVersions,
    codes,
    config_keys::{
        group::{kafka_group_key, validate_group_configs},
        registry::ConfigType,
    },
    coordinator::unified::streams::config::StreamsGroupConfig,
};

fn group_record(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    defaults: &StreamsGroupConfig,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    let mut merged = image
        .group_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for cfg in &resource.configs {
        let value = cfg.value.as_deref().unwrap_or_default();
        match cfg.config_operation {
            OP_SET => {
                merged.insert(cfg.name.clone(), value.to_owned());
            }
            OP_DELETE => {
                merged.remove(&cfg.name);
            }
            operation => {
                let key = kafka_group_key(&cfg.name, unstable)
                    .filter(|key| key.config_type == ConfigType::List)
                    .ok_or_else(|| not_a_list(operation, &cfg.name))?;
                let next = merge_list_op(
                    operation,
                    merged.get(&cfg.name).map(String::as_str),
                    key.default,
                    value,
                );
                merged.insert(cfg.name.clone(), next);
            }
        }
    }
    // `ControllerConfigurationValidator.validateGroupName`, then the map.
    if resource.resource_name.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default group resources are not allowed.".into(),
        ));
    }
    validate_group_configs(&merged, defaults, unstable)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: resource.resource_name.clone(),
        configs: merged,
    }))
}

pub(super) fn handle_group_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    defaults: &StreamsGroupConfig,
    unstable: UnstableApiVersions,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    match group_record(resource, image, defaults, unstable) {
        Ok(record) => to_submit.push(record),
        Err((code, message)) => {
            out.error_code = code;
            out.error_message = Some(message);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;
    use crate::{
        coordinator::unified::streams::config::{
            KEY_NUM_STANDBY_REPLICAS, KEY_SESSION_TIMEOUT_MS, KEY_SHARE_AUTO_OFFSET_RESET,
        },
        handlers::incremental_alter_configs::RESOURCE_TYPE_GROUP,
    };

    #[test]
    fn group_config_set_validates_and_stages_authoritative_map() {
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".into(),
            configs: vec![AlterableConfig {
                name: KEY_NUM_STANDBY_REPLICAS.into(),
                config_operation: OP_SET,
                value: Some("1".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut records = Vec::new();
        handle_group_scoped(
            &resource,
            &MetadataImage::new(uuid::Uuid::nil()),
            &StreamsGroupConfig::default(),
            crate::api_catalog::UnstableApiVersions::Enabled,
            &mut out,
            &mut records,
        );
        assert!(out.error_code == codes::NONE);
        assert!(matches!(
            records.as_slice(),
            [MetadataRecord::V1GroupConfig(record)]
                if record.group_id == "streams-app"
                    && record.configs.get(KEY_NUM_STANDBY_REPLICAS).map(String::as_str)
                        == Some("1")
        ));
    }

    #[test]
    fn group_config_takes_every_share_offset_reset_strategy_kafka_accepts() {
        // `ShareGroupAutoOffsetResetStrategy` accepts `latest`, `earliest`,
        // and `by_duration:<ISO-8601 duration>`, and refuses anything else.
        for (value, want_code) in [
            ("latest", codes::NONE),
            ("earliest", codes::NONE),
            ("by_duration:PT1H", codes::NONE),
            ("by_duration:-PT1H", codes::INVALID_CONFIG),
            ("by_duration:", codes::INVALID_CONFIG),
            ("none", codes::INVALID_CONFIG),
        ] {
            let resource = AlterConfigsResource {
                resource_type: RESOURCE_TYPE_GROUP,
                resource_name: "share-workers".into(),
                configs: vec![AlterableConfig {
                    name: KEY_SHARE_AUTO_OFFSET_RESET.into(),
                    config_operation: OP_SET,
                    value: Some(value.into()),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut out = AlterConfigsResourceResponse::default();
            let mut records = Vec::new();
            handle_group_scoped(
                &resource,
                &MetadataImage::new(uuid::Uuid::nil()),
                &StreamsGroupConfig::default(),
                crate::api_catalog::UnstableApiVersions::Enabled,
                &mut out,
                &mut records,
            );
            assert!(
                out.error_code == want_code,
                "{KEY_SHARE_AUTO_OFFSET_RESET}={value}"
            );
            if want_code == codes::NONE {
                assert!(
                    matches!(
                        records.as_slice(),
                        [MetadataRecord::V1GroupConfig(record)]
                            if record.configs.get(KEY_SHARE_AUTO_OFFSET_RESET).map(String::as_str)
                                == Some(value)
                    ),
                    "{KEY_SHARE_AUTO_OFFSET_RESET}={value}"
                );
            } else {
                assert!(records.is_empty(), "{KEY_SHARE_AUTO_OFFSET_RESET}={value}");
            }
        }
    }

    #[test]
    fn group_config_rejects_values_outside_broker_bounds() {
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".into(),
            configs: vec![AlterableConfig {
                name: KEY_SESSION_TIMEOUT_MS.into(),
                config_operation: OP_SET,
                value: Some("1000".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut records = Vec::new();
        handle_group_scoped(
            &resource,
            &MetadataImage::new(uuid::Uuid::nil()),
            &StreamsGroupConfig::default(),
            crate::api_catalog::UnstableApiVersions::Enabled,
            &mut out,
            &mut records,
        );
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(records.is_empty());
    }

    /// Kafka's controller order for a `GROUP` resource: the operations merge,
    /// then the group name, then `GroupConfig.validate`. Each row is the whole
    /// outcome.
    #[test]
    fn group_resources_follow_kafkas_merge_and_validation_order() {
        let set = |key: &str, value: &str| AlterableConfig {
            name: key.into(),
            config_operation: OP_SET,
            value: Some(value.into()),
            ..Default::default()
        };
        let append = |key: &str| AlterableConfig {
            name: key.into(),
            config_operation: 2,
            value: Some("1".into()),
            ..Default::default()
        };
        let cases = [
            (
                "g",
                vec![set("not.a.group.key", "1")],
                Err((
                    codes::INVALID_CONFIG,
                    "Unknown group config name: not.a.group.key".to_owned(),
                )),
            ),
            (
                "g",
                vec![append(KEY_SESSION_TIMEOUT_MS)],
                Err((
                    codes::INVALID_CONFIG,
                    "Can't APPEND to key streams.session.timeout.ms because its type is not LIST."
                        .to_owned(),
                )),
            ),
            (
                "",
                vec![set(KEY_NUM_STANDBY_REPLICAS, "1")],
                Err((
                    codes::INVALID_REQUEST,
                    "Default group resources are not allowed.".to_owned(),
                )),
            ),
            (
                "g",
                vec![set(KEY_NUM_STANDBY_REPLICAS, "1")],
                Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
                    group_id: "g".into(),
                    configs: maplit::btreemap! {
                        KEY_NUM_STANDBY_REPLICAS.to_owned() => "1".to_owned(),
                    },
                })),
            ),
        ];
        for (name, configs, want) in cases {
            let resource = AlterConfigsResource {
                resource_type: RESOURCE_TYPE_GROUP,
                resource_name: name.into(),
                configs: configs.clone(),
                ..Default::default()
            };
            assert!(
                group_record(
                    &resource,
                    &MetadataImage::new(uuid::Uuid::nil()),
                    &StreamsGroupConfig::default(),
                    crate::api_catalog::UnstableApiVersions::Enabled,
                ) == want,
                "{name:?} {configs:?}"
            );
        }
    }
}
