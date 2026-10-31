//! The authoritative topic override record an `AlterConfigs` topic resource
//! becomes.
//!
//! `AlterConfigs` replaces a topic's whole override map, so the per-key
//! validation and the cross-key combination rules both apply to the map this
//! module builds, and neither reads the topic's current overrides.
//!
//! Storage transition rules do read them. `krabka.diskless` is fixed when the topic is
//! created, and a replacement that simply omits it would otherwise turn a
//! diskless topic back into a local-log one, so the resulting map is compared
//! against the stored one. Disabling remote storage also checks whether
//! the stored config requires explicit deletion consent. See
//! [`crate::config_keys::validate_diskless_unchanged`].
//!
//! ELR is partition metadata, as it is in Kafka, so config replacement cannot
//! erase it.

use {
    super::finish_topic_overrides,
    crate::{api_catalog::UnstableApiVersions, codes, config_keys, topic_policy::TopicPolicy},
    krabka_log::topic_name,
    krabka_metadata::{MetadataRecord, TopicConfigRecord},
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
    std::collections::BTreeMap,
};

/// Build the authoritative `V1TopicConfig` record for a topic resource. The
/// request carries the *complete* set of non-default values, so the map this
/// builds is the whole override map and the cross-key rules apply to it.
pub(super) fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
    policy: &TopicPolicy,
    remote_storage_system_enabled: bool,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    // `ControllerConfigurationValidator.validateTopicName` runs before the
    // configs are checked, and the existence check runs after them.
    if resource.resource_name.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default topic resources are not allowed.".into(),
        ));
    }
    if let Err(invalid) = topic_name::validate_topic_name(&resource.resource_name) {
        return Err((codes::INVALID_TOPIC_EXCEPTION, invalid.to_string()));
    }
    let current = image.topic_config(&resource.resource_name);
    let mut overrides = BTreeMap::new();
    for cfg in &resource.configs {
        // A controller-managed key has no client writer, so it cannot take
        // part in the replacement. The check comes before the whitelist, so
        // the operator reads the refusal that names what does write the key
        // and not `Unknown topic config name`.
        if config_keys::is_controller_managed_topic_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&cfg.name),
            ));
        }
        let value = config_keys::canonical_topic_config(
            &cfg.name,
            cfg.value.as_deref().unwrap_or_default(),
            unstable,
        )
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
        overrides.insert(cfg.name.clone(), value);
    }
    config_keys::validate_config_combination(
        &overrides,
        &config_keys::TopicDefaults::from_image(image, unstable),
        remote_storage_system_enabled,
    )
    .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    finish_topic_overrides(image, &resource.resource_name, current, &overrides, policy)?;
    overrides.extend(
        current
            .into_iter()
            .flatten()
            .filter(|(key, _)| key.starts_with(config_keys::AUDIT_PLACEMENT_EPOCH_PREFIX))
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config_keys::{CLEANUP_POLICY, DELIVERY_MODE, REMOTE_STORAGE_ENABLE, RETENTION_MS},
            handlers::{
                alter_configs::test_support::{
                    image_with_topic, image_with_topic_config, topic_resource,
                },
                test_support::check_config_result,
            },
        },
        assert2::{assert, check},
    };

    /// The record builder under the empty policy, which is the state of a
    /// broker with no `[topic_policy]` section. It shadows the glob-imported
    /// name so the cases below read as the two-argument call they were
    /// written as; the policy's own cases pass a real one to
    /// [`super::topic_config_record`].
    fn topic_config_record(
        resource: &AlterConfigsResource,
        image: &krabka_metadata::MetadataImage,
    ) -> Result<MetadataRecord, (i16, String)> {
        use crate::handlers::test_support::build_topic_config_record_with_default_policy;

        build_topic_config_record_with_default_policy(resource, image, super::topic_config_record)
    }

    /// A diskless change answers before the existence check, so a topic that
    /// does not exist reports the diskless refusal.
    #[test]
    fn a_diskless_change_on_an_unknown_topic_answers_before_existence() {
        let resource = topic_resource("missing", &[(config_keys::DISKLESS, "true")]);
        let diskless = maplit::btreemap! {
            config_keys::DISKLESS.to_owned() => "true".to_owned(),
        };
        let expected = config_keys::validate_diskless_unchanged(None, &diskless)
            .expect_err("turning diskless on is a change");
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        check!(topic_config_record(&resource, &image) == Err((codes::INVALID_CONFIG, expected)));
    }

    #[test]
    fn topic_replacement_rejects_compaction_on_a_scheduled_topic() {
        let image = image_with_topic("orders");

        let (code, message) = topic_config_record(
            &topic_resource("orders", &[
                (CLEANUP_POLICY, "compact"),
                (DELIVERY_MODE, "scheduled"),
            ]),
            &image,
        )
        .expect_err("compaction on a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(message.contains(CLEANUP_POLICY), "got: {message}");
        assert!(message.contains(DELIVERY_MODE), "got: {message}");
    }

    /// Kafka's `LogConfig.validateRemoteStorageRequiresDeleteCleanupPolicy`
    /// refuses tiered storage on a topic whose policy is anything but `delete`
    /// or the empty list, and `AlterConfigs` is one of the three paths that
    /// surface the `ConfigException` as `INVALID_CONFIG`.
    #[test]
    fn topic_replacement_rejects_tiered_storage_on_a_compacted_topic() {
        let image = image_with_topic("orders");

        for policy in ["compact", "compact,delete", "delete,compact"] {
            let (code, message) = topic_config_record(
                &topic_resource("orders", &[
                    (CLEANUP_POLICY, policy),
                    (REMOTE_STORAGE_ENABLE, "true"),
                ]),
                &image,
            )
            .expect_err("tiered storage on a compacted topic must be rejected");

            check!(code == codes::INVALID_CONFIG, "cleanup.policy={policy}");
            check!(
                message == config_keys::REMOTE_STORAGE_POLICY_MESSAGE,
                "cleanup.policy={policy}"
            );
        }
    }

    /// Independent expected replacement for the ordinary delete policy.
    fn expected_delete_policy_record(extra: (&str, &str)) -> MetadataRecord {
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
                CLEANUP_POLICY.to_owned() => "delete".to_owned(),
                extra.0.to_owned() => extra.1.to_owned(),
            },
        })
    }

    /// The same replacement without compaction is ordinary, so the refusal
    /// above is the pair and not the tiered-storage key alone.
    #[test]
    fn topic_replacement_accepts_tiered_storage_on_a_delete_policy_topic() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[
                (CLEANUP_POLICY, "delete"),
                (REMOTE_STORAGE_ENABLE, "true"),
            ]),
            &image,
        )
        .expect("a tiered delete-policy topic is valid");

        let expected = expected_delete_policy_record((REMOTE_STORAGE_ENABLE, "true"));
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_accepts_a_scheduled_topic_without_compaction() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[
                (CLEANUP_POLICY, "delete"),
                (DELIVERY_MODE, "scheduled"),
            ]),
            &image,
        )
        .expect("a scheduled delete-policy topic is valid");

        let expected = expected_delete_policy_record((DELIVERY_MODE, "scheduled"));
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_rejects_controller_managed_configs() {
        let image = image_with_topic("orders");

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS
            .into_iter()
            .chain(["krabka.audit.placement.epoch.3"])
        {
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
    fn topic_replacement_preserves_the_controllers_audit_epoch() {
        let topic = "__krabka_audit";
        let image = image_with_topic_config(topic, &[
            ("krabka.audit.placement.epoch.3", "17"),
            (RETENTION_MS, "1000"),
        ]);
        let record =
            topic_config_record(&topic_resource(topic, &[(RETENTION_MS, "2000")]), &image).unwrap();
        assert!(
            record
                == MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: topic.into(),
                    overrides: maplit::btreemap! {
                        "krabka.audit.placement.epoch.3".into() => "17".into(),
                        RETENTION_MS.into() => "2000".into(),
                    },
                })
        );
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

            check_config_result(result, want_ok, label, config_keys::DISKLESS);
        }
    }

    #[test]
    fn topic_replacement_leaves_an_ordinary_config_unaffected() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[(RETENTION_MS, "60000")]),
            &image,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {RETENTION_MS.to_owned() => "60000".to_owned()},
        });
        assert!(record == expected);
    }

    fn forbid_unclean_election() -> TopicPolicy {
        TopicPolicy {
            forbidden: [(
                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
                "true".to_owned(),
            )]
            .into(),
            ..TopicPolicy::default()
        }
    }

    #[test]
    fn a_replacement_that_breaks_the_policy_is_a_policy_violation() {
        let image = image_with_topic("orders");
        let policy = forbid_unclean_election();

        let (code, message) = super::topic_config_record(
            &topic_resource("orders", &[(
                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                "true",
            )]),
            &image,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect_err("a forbidden value must be refused");

        check!(code == codes::POLICY_VIOLATION);
        check!(
            message.contains(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE),
            "{message}"
        );
    }

    #[test]
    fn a_replacement_the_policy_allows_still_commits() {
        let image = image_with_topic("orders");
        let policy = forbid_unclean_election();

        let record = super::topic_config_record(
            &topic_resource("orders", &[(
                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                "false",
            )]),
            &image,
            &policy,
            true,
            UnstableApiVersions::Disabled,
        )
        .expect("the other value of a forbidden key is accepted");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: maplit::btreemap! {
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned() => "false".to_owned()},
        });
        check!(record == expected);
    }

    #[derive(Default)]
    struct TopicConfigOrderScenario {
        image: Option<krabka_metadata::MetadataImage>,
        name: String,
        retention: String,
        result: Option<Result<MetadataRecord, (i16, String)>>,
    }
    #[rstest::fixture]
    fn topic_config_order() -> TopicConfigOrderScenario {
        let image = None;
        TopicConfigOrderScenario {
            image,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "topic orders exists and a replacement for topic {name} sets retention.ms to {retention}"
    )]
    fn requested_topic_retention(
        topic_config_order: &mut TopicConfigOrderScenario,
        name: String,
        retention: String,
    ) {
        topic_config_order.image = Some(image_with_topic("orders"));
        topic_config_order.name = if name == "empty" { String::new() } else { name };
        topic_config_order.retention = retention;
    }
    #[rstest_bdd_macros::when(
        "the controller validates the authoritative topic configuration replacement"
    )]
    fn validate_topic_replacement_order(topic_config_order: &mut TopicConfigOrderScenario) {
        topic_config_order.result = Some(topic_config_record(
            &topic_resource(&topic_config_order.name, &[(
                config_keys::RETENTION_MS,
                &topic_config_order.retention,
            )]),
            topic_config_order.image.as_ref().unwrap(),
        ));
    }
    #[rstest_bdd_macros::then("topic replacement returns code {code} and message {message}")]
    fn check_topic_replacement_order(
        topic_config_order: &TopicConfigOrderScenario,
        code: i16,
        message: String,
    ) {
        check!(topic_config_order.result.as_ref().unwrap() == &Err((code, message)));
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_topic_config_order",fixtures=[topic_config_order:TopicConfigOrderScenario]);
}
