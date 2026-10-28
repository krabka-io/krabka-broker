//! The per-key broker config records an `AlterConfigs` broker resource
//! becomes, including the tombstones for overrides the replacement omits.
//!
//! Kafka's `AlterConfigs` is a full replacement, so this module both writes
//! the requested keys and deletes the ones the request left out. Keys that
//! only the controller writes stand outside that replacement: naming one is an
//! error, and the tombstone sweep skips them.
//!
//! The dynamic-config rules are Kafka's `DynamicBrokerConfig.validateConfigs`,
//! shared with `IncrementalAlterConfigs` in
//! [`crate::config_keys::broker_dynamic`].

use {
    crate::{
        codes,
        config_keys::{
            self,
            broker_dynamic::{
                BrokerConfigContext, CLUSTER_DEFAULT_ONLY, cordoned_log_dirs_disabled_error,
                cordoned_log_dirs_error, elr_min_isr_error, stored_dynamic_configs,
                validate_dynamic_broker_configs,
            },
            listener_reconfig::OwnBrokerChecks,
        },
        cordoned_log_dirs::CORDONED_LOG_DIRS,
    },
    krabka_metadata::{BrokerConfigRecord, MetadataRecord},
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
    std::collections::BTreeMap,
};

/// Check broker identity, directory membership and, for this node's own
/// resource on the client-addressed node, its listener values, the TLS
/// listeners' new keystores and truststores, and the listeners it keeps
/// ([`OwnBrokerChecks`]). `AlterConfigs` replaces the whole set, so the
/// replacement is the proposed config.
pub(super) fn preprocess_broker_resource(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
    checks: OwnBrokerChecks<'_>,
) -> Result<(), (i16, String)> {
    let node_id = context.resource_node(&resource.resource_name)?;
    if node_id == krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID {
        return Ok(());
    }
    let values = resource
        .configs
        .iter()
        .filter(|c| c.name == CORDONED_LOG_DIRS)
        .map(|c| (c.name.clone(), c.value.clone().unwrap_or_default()))
        .collect();
    cordoned_log_dirs_error(&values, context.log_dirs)?;
    let proposed: BTreeMap<String, String> = resource
        .configs
        .iter()
        .map(|c| (c.name.clone(), c.value.clone().unwrap_or_default()))
        .collect();
    validate_dynamic_broker_configs(&proposed, true, context.unstable)?;
    checks.check(
        context.image,
        &stored_dynamic_configs(context.image, node_id),
        &proposed,
    )
}

/// The records one `AlterConfigs` `BROKER` resource stages. `log_dirs` are
/// this node's log directories, which a named-broker `cordoned.log.dirs` must
/// name, and are empty on a node without the broker role.
pub(super) fn broker_config_records(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id = context.resource_node(&resource.resource_name)?;
    let per_broker = node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
    let mut replacement = BTreeMap::new();
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
        if per_broker && CLUSTER_DEFAULT_ONLY.contains(&config.name.as_str()) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    config.name
                ),
            ));
        }
        // `ConfigAdminManager.preprocess` has already refused a null value.
        replacement.insert(
            config.name.clone(),
            config.value.clone().unwrap_or_default(),
        );
    }
    // The records carry the client's strings, as Kafka's do: validation
    // parses a value and keeps nothing of the parse.
    validate_dynamic_broker_configs(&replacement, per_broker, context.unstable)?;
    if per_broker {
        cordoned_log_dirs_error(&replacement, context.log_dirs)?;
    }

    let current = context.image.broker_config(node_id);
    let deleted: Vec<&String> = current
        .into_iter()
        .flat_map(BTreeMap::keys)
        .filter(|name| {
            !replacement.contains_key(*name)
                && !config_keys::is_controller_managed_broker_config(name)
        })
        .collect();
    // Kafka's `ConfigurationControlManager` checks the ELR rules on every
    // record, the written ones and the implicit deletions.
    for (name, value) in replacement
        .iter()
        .map(|(name, value)| (name, Some(value.as_str())))
        .chain(deleted.iter().map(|name| (*name, None)))
    {
        if let Some(error) = elr_min_isr_error(context.image, node_id, name, value) {
            return Err(error);
        }
        if let Some(error) = cordoned_log_dirs_disabled_error(context.image, name) {
            return Err(error);
        }
    }
    let mut records = Vec::with_capacity(replacement.len() + deleted.len());
    records.extend(replacement.iter().map(|(name, value)| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: Some(value.clone()),
        })
    }));
    records.extend(deleted.into_iter().map(|name| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: None,
        })
    }));
    Ok(records)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config_keys::{BROKER_WITNESS, UNCLEAN_RECOVERY_STRATEGY, WITNESS_TRUE},
            handlers::{
                alter_configs::test_support::{broker_resource, image_with_broker},
                test_support::broker_config_context,
            },
            test_support::{broker_registration, finalize_elr_version},
            throttle::{FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY},
        },
        assert2::{assert, check},
        krabka_metadata::NodeId,
    };

    const SERVING: NodeId = NodeId(1);

    fn record(node_id: NodeId, name: &str, value: Option<&str>) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.into(),
            config_value: value.map(str::to_owned),
        })
    }

    /// Replace this serving broker's dynamic config with stable APIs enabled.
    fn replacement_records(
        image: &krabka_metadata::MetadataImage,
        name: &str,
        configs: &[(&str, &str)],
    ) -> Result<Vec<MetadataRecord>, (i16, String)> {
        broker_config_records(
            &broker_resource(name, configs),
            broker_config_context().image(image).call(),
        )
    }

    fn replace_follower_throttle_with_leader(
        image: &mut krabka_metadata::MetadataImage,
    ) -> Vec<MetadataRecord> {
        image.apply(&record(SERVING, FOLLOWER_THROTTLED_RATE_KEY, Some("512")));

        replacement_records(image, "1", &[(LEADER_THROTTLED_RATE_KEY, "2048")])
            .expect("valid broker replacement")
    }

    #[test]
    fn broker_full_replacement_sets_requested_and_deletes_omitted_configs() {
        let mut image = image_with_broker(1);
        image.apply(&record(SERVING, LEADER_THROTTLED_RATE_KEY, Some("1024")));
        let records = replace_follower_throttle_with_leader(&mut image);

        let expected = vec![
            record(SERVING, LEADER_THROTTLED_RATE_KEY, Some("2048")),
            record(SERVING, FOLLOWER_THROTTLED_RATE_KEY, None),
        ];
        assert!(records == expected);
    }

    #[derive(Default)]
    struct BrokerReplacementScenario {
        image: Option<krabka_metadata::MetadataImage>,
        name: String,
        configs: Vec<(String, String)>,
        result: Option<Result<Vec<MetadataRecord>, (i16, String)>>,
    }
    #[rstest::fixture]
    fn broker_replacement() -> BrokerReplacementScenario {
        let image = None;
        BrokerReplacementScenario {
            image,
            ..Default::default()
        }
    }
    fn broker_config_fixture_value(value: &str) -> String {
        value
            .strip_prefix('"')
            .and_then(|text| text.strip_suffix('"'))
            .unwrap_or(value)
            .to_owned()
    }
    #[rstest_bdd_macros::given(
        "brokers 1 and 2 are registered and broker 1 serves the configuration replacement"
    )]
    fn registered_replacement_brokers(broker_replacement: &mut BrokerReplacementScenario) {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                port: 9093,
                ..broker_registration(krabka_raft::NodeId(2))
            },
        ));
        broker_replacement.image = Some(image);
    }
    #[rstest_bdd_macros::given(
        "broker replacement targets {resource} with key {key} value {value}"
    )]
    fn broker_replacement_input(
        broker_replacement: &mut BrokerReplacementScenario,
        resource: String,
        key: String,
        value: String,
    ) {
        broker_replacement.name = if resource == "cluster-default" {
            String::new()
        } else {
            resource
        };
        broker_replacement.configs = vec![(key, value)];
    }
    #[rstest_bdd_macros::given("broker replacement targets {resource} with configurations")]
    fn broker_replacement_configs(
        broker_replacement: &mut BrokerReplacementScenario,
        resource: String,
        datatable: Vec<Vec<String>>,
    ) {
        broker_replacement.name = if resource == "cluster-default" {
            String::new()
        } else {
            resource
        };
        broker_replacement.configs = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [key, value] = row.as_slice() else {
                    panic!("broker configuration key and value required")
                };
                (key.clone(), broker_config_fixture_value(value))
            })
            .collect();
    }
    #[rstest_bdd_macros::when(
        "the broker validates the complete dynamic configuration replacement"
    )]
    fn replace_broker_configurations(broker_replacement: &mut BrokerReplacementScenario) {
        let configs: Vec<_> = broker_replacement
            .configs
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        broker_replacement.result = Some(replacement_records(
            broker_replacement.image.as_ref().unwrap(),
            &broker_replacement.name,
            &configs,
        ));
    }
    #[rstest_bdd_macros::then("broker replacement refuses with code {code} and message {message}")]
    fn check_broker_replacement_refusal(
        broker_replacement: &BrokerReplacementScenario,
        code: i16,
        message: String,
    ) {
        check!(broker_replacement.result.as_ref().unwrap() == &Err((code, message)));
    }
    #[rstest_bdd_macros::then("the exact ordered broker configuration records are")]
    fn check_broker_replacement_records(
        broker_replacement: &BrokerReplacementScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [node, key, value] = row.as_slice() else {
                    panic!("broker node configuration key and value required")
                };
                let node = if node == "cluster" {
                    krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                } else {
                    NodeId(node.parse().unwrap())
                };
                record(node, key, Some(&broker_config_fixture_value(value)))
            })
            .collect();
        check!(broker_replacement.result.as_ref().unwrap() == &Ok(expected));
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_broker_config_replacement",fixtures=[broker_replacement:BrokerReplacementScenario]);

    #[test]
    fn broker_full_replacement_rejects_controller_managed_configs() {
        let image = image_with_broker(1);
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for resource_name in ["1", ""] {
                check!(
                    replacement_records(&image, resource_name, &[(key, "true")])
                        == Err((
                            codes::INVALID_CONFIG,
                            format!("broker config {key} is controller-managed and read-only"),
                        )),
                    "key {key}"
                );
            }
        }
    }

    #[test]
    fn broker_full_replacement_leaves_controller_managed_configs_alone() {
        let mut image = image_with_broker(1);
        image.apply(&record(SERVING, BROKER_WITNESS, Some(WITNESS_TRUE)));
        let records = replace_follower_throttle_with_leader(&mut image);

        assert!(
            records
                == vec![
                    record(SERVING, LEADER_THROTTLED_RATE_KEY, Some("2048")),
                    record(SERVING, FOLLOWER_THROTTLED_RATE_KEY, None),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_per_broker_recovery_setting() {
        let image = image_with_broker(1);

        let error = replacement_records(&image, "1", &[(UNCLEAN_RECOVERY_STRATEGY, "Balanced")])
            .expect_err("per-broker recovery setting must be rejected");

        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn elr_requires_the_cluster_min_isr_override_to_survive_replacement() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        finalize_elr_version(&mut image);
        image.apply(&record(
            krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_keys::MIN_INSYNC_REPLICAS,
            Some("2"),
        ));

        let error = replacement_records(&image, "", &[]);

        assert!(
            error
                == Err((
                    codes::INVALID_CONFIG,
                    "Cluster-level min.insync.replicas cannot be removed while ELR is enabled."
                        .to_owned()
                ))
        );
    }

    // BDD scenario entry points; acceptance steps are defined above.
}
