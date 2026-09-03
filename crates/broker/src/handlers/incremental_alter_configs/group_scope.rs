//! Group resources for `IncrementalAlterConfigs`, the KIP-1071 group configs.
//! The handler merges the per-key operations onto the group's current override
//! map, checks the merged map against the broker's `StreamsGroupConfig`
//! defaults and bounds, and stages a `V1GroupConfig` record with that map.

use krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{OP_DELETE, OP_SET};
use crate::{
    codes,
    coordinator::unified::streams::config::{GROUP_CONFIG_KEYS, StreamsGroupConfig},
};

pub(super) fn handle_group_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    defaults: &StreamsGroupConfig,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    if resource.resource_name.is_empty() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some("group id must not be empty".into());
        return;
    }
    let mut merged = image
        .group_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for cfg in &resource.configs {
        if !GROUP_CONFIG_KEYS.contains(&cfg.name.as_str()) {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(format!("unknown group config `{}`", cfg.name));
            return;
        }
        match cfg.config_operation {
            OP_SET => {
                merged.insert(cfg.name.clone(), cfg.value.clone().unwrap_or_default());
            }
            OP_DELETE => {
                merged.remove(&cfg.name);
            }
            op => {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(format!(
                    "config_operation={op} is not valid for group config `{}`",
                    cfg.name
                ));
                return;
            }
        }
    }
    if let Err(reason) = defaults.with_group_overrides(&merged) {
        out.error_code = codes::INVALID_CONFIG;
        out.error_message = Some(reason);
        return;
    }
    to_submit.push(MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: resource.resource_name.clone(),
        configs: merged,
    }));
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
            &mut out,
            &mut records,
        );
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(records.is_empty());
    }
}
