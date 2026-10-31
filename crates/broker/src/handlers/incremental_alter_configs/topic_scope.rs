//! Topic-scoped resources for `IncrementalAlterConfigs`. The handler merges
//! the per-key operations onto the topic's current override map, validates
//! the resulting map, and returns the `V1TopicConfig` record that carries it.
//!
//! The order is Kafka's `ConfigurationControlManager`: the operations merge
//! first (an APPEND or SUBTRACT of a key that is not a `LIST` is refused
//! there), then `ControllerConfigurationValidator` checks the topic name and
//! the merged map, and only then does the existence check run.
//!
//! `krabka.diskless` is fixed when the topic is created, so the merged map is
//! also compared against the topic's current one: a SET that restates the
//! value the topic already has is a no-op and passes, and any op that would
//! move a partition between the diskless WAL runtime and the local-log
//! runtime is refused. See
//! [`crate::config_keys::validate_diskless_unchanged`].

use {
    super::{OP_APPEND, OP_DELETE, OP_SET},
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::{
            self,
            registry::{self, ConfigScope, ConfigType},
        },
        handlers::alter_configs::finish_topic_overrides,
        topic_policy::TopicPolicy,
    },
    krabka_log::topic_name,
    krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord},
    krabka_protocol::owned::incremental_alter_configs_request::AlterConfigsResource,
    std::collections::BTreeMap,
};

/// Kafka's `ConfigurationControlManager` APPEND and SUBTRACT: a `LIST` key's
/// current value (or its default) split on commas, with each item of the
/// operation's value added when absent or removed.
pub(in crate::handlers::incremental_alter_configs) fn merge_list_op(
    operation: i8,
    current: Option<&str>,
    default: Option<&str>,
    value: &str,
) -> String {
    let base = current.or(default).unwrap_or_default();
    let mut parts: Vec<&str> = base.split(',').filter(|part| !part.is_empty()).collect();
    // Java's `String.split` drops trailing empty strings.
    let items: Vec<&str> = value.trim_end_matches(',').split(',').collect();
    for item in items {
        if operation == OP_APPEND {
            if !parts.contains(&item) {
                parts.push(item);
            }
        } else if let Some(at) = parts.iter().position(|part| *part == item) {
            parts.remove(at);
        }
    }
    parts.join(",")
}

/// Kafka's refusal of an APPEND or SUBTRACT on a key that is not a `LIST`.
pub(in crate::handlers::incremental_alter_configs) fn not_a_list(
    operation: i8,
    key: &str,
) -> (i16, String) {
    let verb = if operation == OP_APPEND {
        "APPEND"
    } else {
        "SUBTRACT"
    };
    (
        codes::INVALID_CONFIG,
        format!("Can't {verb} to key {key} because its type is not LIST."),
    )
}

pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    policy: &TopicPolicy,
    remote_storage_system_enabled: bool,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    let topic = resource.resource_name.as_str();
    let current = image.topic_config(topic);
    let mut merged = current.cloned().unwrap_or_default();
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
        let value = config.value.as_deref().unwrap_or_default();
        match config.config_operation {
            OP_SET => {
                merged.insert(config.name.clone(), value.to_owned());
            }
            // A DELETE of a key the topic does not hold changes nothing, and
            // Kafka writes no record for it, whatever the key is.
            OP_DELETE => {
                merged.remove(&config.name);
            }
            operation => {
                let row = registry::lookup(ConfigScope::Topic, &config.name)
                    .filter(|row| row.is_alterable() && row.config_type == ConfigType::List)
                    .ok_or_else(|| not_a_list(operation, &config.name))?;
                let next = merge_list_op(
                    operation,
                    merged.get(&config.name).map(String::as_str),
                    row.default,
                    value,
                );
                merged.insert(config.name.clone(), next);
            }
        }
    }
    // `ControllerConfigurationValidator.validateTopicName`, then the map.
    if topic.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default topic resources are not allowed.".into(),
        ));
    }
    if let Err(invalid) = topic_name::validate_topic_name(topic) {
        return Err((codes::INVALID_TOPIC_EXCEPTION, invalid.to_string()));
    }
    let (audit_epochs, user_configs): (BTreeMap<_, _>, _) = merged
        .into_iter()
        .partition(|(key, _)| key.starts_with(config_keys::AUDIT_PLACEMENT_EPOCH_PREFIX));
    let mut merged = config_keys::canonical_topic_config_map(
        &user_configs,
        &config_keys::TopicDefaults::from_image(image, unstable),
        remote_storage_system_enabled,
        unstable,
    )
    .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    finish_topic_overrides(image, topic, current, &merged, policy)?;
    merged.extend(audit_epochs);
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides: merged,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::{
                incremental_alter_configs::{
                    OP_SUBTRACT,
                    test_support::{
                        AlterableConfig, StoredOverride, assert, check, image_with_topic_config,
                        make_del_cfg, make_set_cfg, make_topic_resource, overrides_map,
                    },
                },
                test_support::{check_config_result, diskless_turned_on_refusal},
            },
            throttle::ThrottledReplicas,
        },
    };

    /// The record builder under the empty policy, which is the state of a
    /// broker with no `[topic_policy]` section. It shadows the glob-imported
    /// name so the cases below read as the two-argument call they were
    /// written as; the policy's own cases pass a real one to
    /// [`super::topic_config_record`].
    fn topic_config_record(
        resource: &AlterConfigsResource,
        image: &MetadataImage,
    ) -> Result<MetadataRecord, (i16, String)> {
        use crate::handlers::test_support::build_topic_config_record_with_default_policy;

        build_topic_config_record_with_default_policy(resource, image, super::topic_config_record)
    }

    /// `ConfigurationControlManager.incrementalAlterConfigResource`: the
    /// operation's value goes through Java's `String.split(",")`, which drops
    /// trailing empty strings (so `","` yields no items and `""` yields one
    /// empty item), and SUBTRACT is `List.remove(Object)`, which removes the
    /// first occurrence only.
    #[rstest::rstest]
    #[case::append_with_a_trailing_comma(OP_APPEND, Some("delete"), "compact,", "delete,compact")]
    #[case::append_of_only_commas(OP_APPEND, Some("delete"), ",,", "delete")]
    #[case::append_of_an_empty_value(OP_APPEND, Some("delete"), "", "delete,")]
    #[case::append_from_the_default(OP_APPEND, None, "compact", "delete,compact")]
    #[case::subtract_removes_one_occurrence(
        OP_SUBTRACT,
        Some("delete,compact,delete"),
        "delete",
        "compact,delete"
    )]
    #[case::subtract_with_a_trailing_comma(
        OP_SUBTRACT,
        Some("delete,compact"),
        "delete,",
        "compact"
    )]
    fn a_list_op_merges_as_kafkas_controller_does(
        #[case] operation: i8,
        #[case] current: Option<&str>,
        #[case] value: &str,
        #[case] expected: &str,
    ) {
        check!(merge_list_op(operation, current, Some("delete"), value) == expected);
    }

    #[test]
    fn topic_throttle_config_value_validated() {
        // Verify ThrottledReplicas::parse rejects malformed input that
        // the validator delegates to.
        assert!(ThrottledReplicas::parse("not-a-pair").is_err());
        assert!(ThrottledReplicas::parse("0:bad").is_err());
    }

    #[test]
    fn controller_managed_topic_configs_are_rejected_whatever_the_operation() {
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::RETENTION_MS,
            "60000",
        )]);

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS
            .into_iter()
            .chain(["krabka.audit.placement.epoch.3"])
        {
            for (label, config) in [
                ("a SET of the key", make_set_cfg(key, "true")),
                (
                    "a DELETE of the key, which asks to clear the freeze",
                    make_del_cfg(key),
                ),
                ("an APPEND of the key", AlterableConfig {
                    name: key.into(),
                    config_operation: 2,
                    value: Some("true".into()),
                    ..Default::default()
                }),
                ("a SUBTRACT of the key", AlterableConfig {
                    name: key.into(),
                    config_operation: 3,
                    value: Some("true".into()),
                    ..Default::default()
                }),
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
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::RETENTION_MS,
            "60000",
        )]);

        let record = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::SEGMENT_BYTES,
                "1048576",
            )]),
            &img,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_owned() => "60000".to_owned(),
            config_keys::SEGMENT_BYTES.to_owned() => "1048576".to_owned()},
        });
        assert!(record == expected);
    }

    #[test]
    fn an_incremental_update_keeps_the_audit_placement_epoch() {
        let image = image_with_topic_config("__krabka_audit", &[
            StoredOverride::new("krabka.audit.placement.epoch.3", "17"),
            StoredOverride::new(config_keys::RETENTION_MS, "1000"),
        ]);
        let resource = make_topic_resource("__krabka_audit", vec![make_set_cfg(
            config_keys::RETENTION_MS,
            "2000",
        )]);
        let MetadataRecord::V1TopicConfig(record) = topic_config_record(&resource, &image).unwrap()
        else {
            panic!("expected a topic config record");
        };
        assert!(
            record.overrides
                == overrides_map(&[
                    StoredOverride::new("krabka.audit.placement.epoch.3", "17"),
                    StoredOverride::new(config_keys::RETENTION_MS, "2000"),
                ])
        );
    }

    #[test]
    fn compaction_op_conflicts_with_scheduled_delivery_already_on_the_topic() {
        // The ops alone are legal. Only the merge with the topic's existing
        // config shows the conflict.
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::DELIVERY_MODE,
            "scheduled",
        )]);

        let (code, message) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::CLEANUP_POLICY,
                "compact",
            )]),
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
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::CLEANUP_POLICY,
            "compact",
        )]);

        let (code, _) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::DELIVERY_MODE,
                "scheduled",
            )]),
            &img,
        )
        .expect_err("scheduling a compacted topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
    }

    #[test]
    fn deleting_the_delivery_mode_clears_the_conflict_in_the_same_request() {
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::DELIVERY_MODE,
            "scheduled",
        )]);

        let record = topic_config_record(
            &make_topic_resource("orders", vec![
                make_del_cfg(config_keys::DELIVERY_MODE),
                make_set_cfg(config_keys::CLEANUP_POLICY, "compact"),
            ]),
            &img,
        )
        .expect("removing the schedule leaves a plain compacted topic");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {config_keys::CLEANUP_POLICY.to_owned() => "compact".to_owned()},
        });
        assert!(record == expected);
    }

    #[test]
    fn no_operation_can_move_a_topic_between_data_paths() {
        /// The topic's stored overrides, the ops the request carries, and
        /// whether the merged map is accepted.
        struct DisklessOps<'a> {
            label: &'a str,
            stored: &'a [StoredOverride<'a>],
            ops: Vec<AlterableConfig>,
            want_ok: bool,
        }

        let cases: [DisklessOps<'_>; 5] = [
            DisklessOps {
                label: "a SET that restates the value the topic already has",
                stored: &[StoredOverride::new(config_keys::DISKLESS, "true")],
                ops: vec![make_set_cfg(config_keys::DISKLESS, "true")],
                want_ok: true,
            },
            DisklessOps {
                label: "an ordinary SET alongside the untouched flag",
                stored: &[StoredOverride::new(config_keys::DISKLESS, "true")],
                ops: vec![make_set_cfg(config_keys::RETENTION_MS, "60000")],
                want_ok: true,
            },
            DisklessOps {
                label: "a SET that turns the flag off",
                stored: &[StoredOverride::new(config_keys::DISKLESS, "true")],
                ops: vec![make_set_cfg(config_keys::DISKLESS, "false")],
                want_ok: false,
            },
            DisklessOps {
                label: "a DELETE of the flag",
                stored: &[StoredOverride::new(config_keys::DISKLESS, "true")],
                ops: vec![make_del_cfg(config_keys::DISKLESS)],
                want_ok: false,
            },
            DisklessOps {
                label: "a SET that turns the flag on for a local-log topic",
                stored: &[StoredOverride::new(config_keys::RETENTION_MS, "60000")],
                ops: vec![make_set_cfg(config_keys::DISKLESS, "true")],
                want_ok: false,
            },
        ];

        for DisklessOps {
            label,
            stored,
            ops,
            want_ok,
        } in cases
        {
            let img = image_with_topic_config("orders", stored);

            let result = topic_config_record(&make_topic_resource("orders", ops), &img);

            check_config_result(result, want_ok, label, config_keys::DISKLESS);
        }
    }

    /// A diskless change answers before the existence check, so a topic that
    /// does not exist reports the diskless refusal.
    #[test]
    fn a_diskless_change_on_an_unknown_topic_answers_before_existence() {
        let resource =
            make_topic_resource("missing", vec![make_set_cfg(config_keys::DISKLESS, "true")]);
        let image = MetadataImage::new(uuid::Uuid::nil());
        check!(topic_config_record(&resource, &image) == Err(diskless_turned_on_refusal()));
    }

    #[test]
    fn tiered_storage_cannot_be_turned_on_for_a_diskless_topic() {
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::DISKLESS,
            "true",
        )]);

        let (code, message) = topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                "remote.storage.enable",
                "true",
            )]),
            &img,
        )
        .expect_err("two object-store data paths on one topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(message.contains(config_keys::DISKLESS), "got: {message}");
        assert!(message.contains("remote.storage.enable"), "got: {message}");
    }

    #[test]
    fn scheduled_delivery_keys_merge_onto_an_existing_topic_config() {
        let img = image_with_topic_config("retries", &[StoredOverride::new(
            config_keys::RETENTION_MS,
            "60000",
        )]);

        let record = topic_config_record(
            &make_topic_resource("retries", vec![
                make_set_cfg(config_keys::DELIVERY_MODE, "scheduled"),
                make_set_cfg(config_keys::DELIVERY_MAX_DELAY_MS, "3600000"),
                make_set_cfg(config_keys::DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ]),
            &img,
        )
        .expect("valid scheduled delivery ops");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "retries".into(),
            overrides: maplit::btreemap! {
            config_keys::RETENTION_MS.to_owned() => "60000".to_owned(),
            config_keys::DELIVERY_MODE.to_owned() => "scheduled".to_owned(),
            config_keys::DELIVERY_MAX_DELAY_MS.to_owned() => "3600000".to_owned(),
            config_keys::DELIVERY_SCHEDULE_MONOTONIC.to_owned() => "true".to_owned()},
        });
        assert!(record == expected);
    }

    #[test]
    fn a_merged_map_that_breaks_the_policy_is_a_policy_violation() {
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::MIN_INSYNC_REPLICAS,
            "2",
        )]);
        let policy = TopicPolicy {
            min_insync_replicas: Some(2),
            ..TopicPolicy::default()
        };

        let (code, message) = super::topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::MIN_INSYNC_REPLICAS,
                "1",
            )]),
            &img,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect_err("a merged map below the policy floor must be refused");

        check!(code == codes::POLICY_VIOLATION);
        check!(
            message.contains(config_keys::MIN_INSYNC_REPLICAS),
            "{message}"
        );
    }

    #[test]
    fn a_merged_map_that_satisfies_the_policy_still_commits() {
        let img = image_with_topic_config("orders", &[StoredOverride::new(
            config_keys::MIN_INSYNC_REPLICAS,
            "1",
        )]);
        let policy = TopicPolicy {
            min_insync_replicas: Some(2),
            ..TopicPolicy::default()
        };

        let record = super::topic_config_record(
            &make_topic_resource("orders", vec![make_set_cfg(
                config_keys::MIN_INSYNC_REPLICAS,
                "2",
            )]),
            &img,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect("a merge that lifts the topic to the floor is accepted");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::MIN_INSYNC_REPLICAS.to_owned() => "2".to_owned()},
        });
        check!(record == expected);
    }

    #[derive(Default)]
    struct TopicConfigurationMerge {
        image: Option<krabka_metadata::MetadataImage>,
        name: String,
        result: Option<Result<MetadataRecord, (i16, String)>>,
    }
    #[rstest::fixture]
    fn topic_merge() -> TopicConfigurationMerge {
        let image = None;
        TopicConfigurationMerge {
            image,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "topic orders exists with these stored overrides and the requested topic is \"{name}\""
    )]
    fn existing_topic_overrides(
        topic_merge: &mut TopicConfigurationMerge,
        name: String,
        datatable: Vec<Vec<String>>,
    ) {
        let pairs = datatable.into_iter().skip(1).collect::<Vec<_>>();
        let overrides = pairs
            .iter()
            .map(|cells| StoredOverride::new(&cells[0], &cells[1]))
            .collect::<Vec<_>>();
        topic_merge.image = Some(image_with_topic_config("orders", &overrides));
        topic_merge.name = if name == "<empty>" {
            String::new()
        } else {
            name
        };
    }
    #[rstest_bdd_macros::when(
        "topic configuration merges operation {operation} on {key} with value \"{value}\""
    )]
    fn incremental_topic_operation(
        topic_merge: &mut TopicConfigurationMerge,
        operation: i8,
        key: String,
        value: String,
    ) {
        let config = AlterableConfig {
            name: key,
            config_operation: operation,
            value: (value != "null").then_some(value),
            ..Default::default()
        };
        topic_merge.result = Some(topic_config_record(
            &make_topic_resource(&topic_merge.name, vec![config]),
            topic_merge.image.as_ref().expect("existing topic"),
        ));
    }
    #[rstest_bdd_macros::then("the whole topic config record contains exactly these overrides")]
    fn merged_topic_overrides(topic_merge: &TopicConfigurationMerge, datatable: Vec<Vec<String>>) {
        let overrides = datatable
            .into_iter()
            .skip(1)
            .map(|cells| (cells[0].clone(), cells[1].clone()))
            .collect();
        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides,
        });
        check!(topic_merge.result.as_ref().expect("merge result") == &Ok(expected));
    }
    #[rstest_bdd_macros::then(
        "the topic configuration merge refuses with code {code} and message \"{message}\""
    )]
    fn refused_topic_merge(topic_merge: &TopicConfigurationMerge, code: i16, message: String) {
        check!(topic_merge.result.as_ref().expect("merge result") == &Err((code, message)));
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_incremental_topic_merge.feature", fixtures = [topic_merge: TopicConfigurationMerge]);
}
