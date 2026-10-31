//! Group resources for `IncrementalAlterConfigs`, the KIP-848, KIP-932 and
//! KIP-1071 group configs. The handler merges the per-key operations onto
//! the group's current override map, checks the merged map with Kafka's
//! `GroupConfig.validate` against the broker's group bounds,
//! and stages a `V1GroupConfig` record with that map.

use {
    super::{
        OP_DELETE, OP_SET,
        topic_scope::{merge_list_op, not_a_list},
    },
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::{
            group::{GroupBounds, kafka_group_key, validate_group_configs},
            registry::ConfigType,
        },
    },
    krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord},
    krabka_protocol::owned::incremental_alter_configs_request::AlterConfigsResource,
};

pub(super) fn group_record(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    bounds: &GroupBounds,
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
    validate_group_configs(&merged, bounds, unstable)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: resource.resource_name.clone(),
        configs: merged,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            api_catalog,
            coordinator::unified::streams::config::{
                KEY_NUM_STANDBY_REPLICAS, KEY_SESSION_TIMEOUT_MS, KEY_SHARE_AUTO_OFFSET_RESET,
            },
            handlers::incremental_alter_configs::{
                RESOURCE_TYPE_GROUP,
                test_support::{make_scoped_resource, make_set_cfg},
            },
        },
        assert2::assert,
        krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig,
    };

    fn drive(
        resource: &AlterConfigsResource,
        unstable: UnstableApiVersions,
    ) -> Result<MetadataRecord, (i16, String)> {
        group_record(
            resource,
            &MetadataImage::new(uuid::Uuid::nil()),
            &GroupBounds::default(),
            unstable,
        )
    }

    struct GroupConfigScenario {
        group: String,
        configs: Vec<AlterableConfig>,
        unstable: UnstableApiVersions,
        handled: Option<Result<MetadataRecord, (i16, String)>>,
        record: Option<Result<MetadataRecord, (i16, String)>>,
    }
    #[rstest::fixture]
    fn group_config() -> GroupConfigScenario {
        let group = "g".to_owned();
        GroupConfigScenario {
            group,
            configs: Vec::new(),
            unstable: UnstableApiVersions::Enabled,
            handled: None,
            record: None,
        }
    }
    #[rstest_bdd_macros::given(
        "the stable group configuration request sets these Kafka-supported keys"
    )]
    fn stable_group_configs(group_config: &mut GroupConfigScenario, datatable: Vec<Vec<String>>) {
        group_config.unstable = UnstableApiVersions::Disabled;
        group_config.configs = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [key, value] = row.as_slice() else {
                    panic!("configuration name and value required")
                };
                make_set_cfg(key, value)
            })
            .collect();
    }
    #[rstest_bdd_macros::given("share group share-workers sets offset reset strategy {strategy}")]
    fn share_offset_strategy(group_config: &mut GroupConfigScenario, strategy: String) {
        group_config.group = "share-workers".into();
        group_config.configs = vec![make_set_cfg(KEY_SHARE_AUTO_OFFSET_RESET, &strategy)];
    }
    #[rstest_bdd_macros::given(
        "group {group} requests operation {operation} on key {key} with value 1"
    )]
    fn group_operation(
        group_config: &mut GroupConfigScenario,
        group: String,
        operation: i8,
        key: String,
    ) {
        group_config.group = if group == "empty" {
            String::new()
        } else {
            group
        };
        group_config.configs = vec![AlterableConfig {
            name: key,
            config_operation: operation,
            value: Some("1".into()),
            ..Default::default()
        }];
    }
    #[rstest_bdd_macros::when("the group configuration handler stages the request")]
    fn stage_group_configs(group_config: &mut GroupConfigScenario) {
        let resource = make_scoped_resource(
            RESOURCE_TYPE_GROUP,
            &group_config.group,
            group_config.configs.clone(),
        );
        group_config.handled = Some(drive(&resource, group_config.unstable));
    }
    #[rstest_bdd_macros::when("the controller merges operations and validates the group resource")]
    fn validate_group_record(group_config: &mut GroupConfigScenario) {
        let resource = make_scoped_resource(
            RESOURCE_TYPE_GROUP,
            &group_config.group,
            group_config.configs.clone(),
        );
        group_config.record = Some(group_record(
            &resource,
            &MetadataImage::new(uuid::Uuid::nil()),
            &GroupBounds::default(),
            api_catalog::UnstableApiVersions::Enabled,
        ));
    }
    #[rstest_bdd_macros::then(
        "one authoritative group configuration record preserves every requested key and value"
    )]
    fn check_complete_group_record(group_config: &GroupConfigScenario) {
        let expected = group_config
            .configs
            .iter()
            .map(|cfg| (cfg.name.clone(), cfg.value.clone().unwrap()))
            .collect();
        assert!(
            group_config.handled
                == Some(Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
                    group_id: group_config.group.clone(),
                    configs: expected
                })))
        );
    }
    #[rstest_bdd_macros::then(
        "offset reset validation returns error {code} and stages a record only on success"
    )]
    fn check_share_strategy(group_config: &GroupConfigScenario, code: i16) {
        let handled = group_config.handled.as_ref().unwrap();
        if code == codes::NONE {
            let value = group_config.configs[0].value.as_deref().unwrap();
            assert!(
                matches!(handled, Ok(MetadataRecord::V1GroupConfig(record)) if record.configs.get(KEY_SHARE_AUTO_OFFSET_RESET).map(String::as_str)==Some(value))
            );
        } else {
            assert!(matches!(handled, Err((refused, _)) if *refused == code));
        }
    }
    #[rstest_bdd_macros::then(
        "the group resource is refused with code {code} and message {message}"
    )]
    fn check_group_resource_refusal(
        group_config: &GroupConfigScenario,
        code: i16,
        message: String,
    ) {
        assert!(group_config.record.as_ref().unwrap() == &Err((code, message)));
    }
    #[rstest_bdd_macros::then("the resulting record sets one standby replica for group g")]
    fn check_group_standby_record(group_config: &GroupConfigScenario) {
        assert!(
            group_config.record.as_ref().unwrap()
                == &Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
                    group_id: "g".into(),
                    configs: maplit::btreemap! {KEY_NUM_STANDBY_REPLICAS.to_owned()=>"1".to_owned()}
                }))
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_group_config",fixtures=[group_config:GroupConfigScenario]);

    #[test]
    fn group_config_set_validates_and_stages_authoritative_map() {
        let resource =
            make_scoped_resource(RESOURCE_TYPE_GROUP, "streams-app", vec![make_set_cfg(
                KEY_NUM_STANDBY_REPLICAS,
                "1",
            )]);
        let record = drive(&resource, UnstableApiVersions::Enabled);
        assert!(matches!(
            record,
            Ok(MetadataRecord::V1GroupConfig(record))
                if record.group_id == "streams-app"
                    && record.configs.get(KEY_NUM_STANDBY_REPLICAS).map(String::as_str)
                        == Some("1")
        ));
    }

    #[test]
    fn group_config_rejects_values_outside_broker_bounds() {
        let resource =
            make_scoped_resource(RESOURCE_TYPE_GROUP, "streams-app", vec![make_set_cfg(
                KEY_SESSION_TIMEOUT_MS,
                "1000",
            )]);
        let record = drive(&resource, UnstableApiVersions::Enabled);
        assert!(matches!(record, Err((codes::INVALID_CONFIG, _))));
    }
}
