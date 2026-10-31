//! Broker-scoped resources for `IncrementalAlterConfigs`: Kafka's
//! `ConfigAdminManager.validateBrokerConfigChange` over the resource's stored
//! dynamic configs with the operations applied, then one `V1BrokerConfig`
//! record per altered key.
//!
//! The key rules themselves -- which keys are dynamic, which need a named
//! broker, and each key's value check -- are shared with `AlterConfigs` and
//! live in [`crate::config_keys::broker_dynamic`].

use {
    super::{OP_APPEND, OP_DELETE, OP_SET, OP_SUBTRACT},
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::{
            self,
            broker_dynamic::{
                BrokerConfigContext, BrokerKeyKind, CLUSTER_DEFAULT_ONLY, broker_key_kind,
                cordoned_log_dirs_disabled_error, cordoned_log_dirs_error, elr_min_isr_error,
                stored_dynamic_configs, validate_dynamic_broker_configs,
            },
            listener_reconfig::OwnBrokerChecks,
        },
        cordoned_log_dirs::CORDONED_LOG_DIRS,
    },
    krabka_metadata::{BrokerConfigRecord, MetadataRecord},
    krabka_protocol::owned::incremental_alter_configs_request::AlterConfigsResource,
    std::collections::BTreeMap,
};

/// The default a `LIST` broker key starts from when the resource does not
/// hold it, as Kafka's `prepareIncrementalConfigs` reads it from
/// `ConfigKey.defaultValue`. Only a `LIST` key reaches here, and every
/// topic key typed `LIST` is one Kafka 4.3.1 has.
fn list_default(name: &str) -> &'static str {
    config_keys::broker_dynamic::broker_key_row(name, UnstableApiVersions::Disabled)
        .and_then(|row| row.default)
        .unwrap_or("")
}

/// Kafka's `prepareIncrementalConfigs`: apply one resource's operations to its
/// stored dynamic configs.
fn apply_operations(
    resource: &AlterConfigsResource,
    props: &mut BTreeMap<String, String>,
    unstable: UnstableApiVersions,
) -> Result<(), (i16, String)> {
    for cfg in &resource.configs {
        let name = cfg.name.as_str();
        let value = cfg.value.as_deref().unwrap_or_default();
        match cfg.config_operation {
            OP_SET => {
                props.insert(name.to_owned(), value.to_owned());
            }
            OP_DELETE => {
                props.remove(name);
            }
            operation => {
                let verb = if operation == OP_APPEND {
                    "append"
                } else {
                    "subtract"
                };
                match broker_key_kind(name, unstable) {
                    BrokerKeyKind::Unknown => {
                        return Err((
                            codes::INVALID_CONFIG,
                            format!("Unknown config name: {name}"),
                        ));
                    }
                    BrokerKeyKind::Scalar => {
                        return Err((
                            codes::INVALID_CONFIG,
                            format!("Config value {verb} is not allowed for config key: {name}"),
                        ));
                    }
                    BrokerKeyKind::List => {}
                }
                let old = props
                    .get(name)
                    .map_or_else(|| list_default(name).to_owned(), Clone::clone);
                let mut parts: Vec<&str> = old.split(',').filter(|part| !part.is_empty()).collect();
                let items: Vec<&str> = value.split(',').collect();
                if operation == OP_APPEND {
                    for item in items {
                        if !parts.contains(&item) {
                            parts.push(item);
                        }
                    }
                } else {
                    parts.retain(|part| !items.contains(part));
                }
                props.insert(name.to_owned(), parts.join(","));
            }
        }
    }
    Ok(())
}

/// The records one `BROKER` resource stages, or the error it answers with.
/// `log_dirs` are this node's log directories, which a named-broker
/// `cordoned.log.dirs` must name, and are empty on a node without the broker
/// role.
pub(super) fn broker_records(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id = context.resource_node(&resource.resource_name)?;
    if let Some(cfg) = resource.configs.iter().find(|cfg| {
        !matches!(
            cfg.config_operation,
            OP_SET | OP_DELETE | OP_APPEND | OP_SUBTRACT
        )
    }) {
        return Err((
            codes::INVALID_REQUEST,
            format!("Unknown operations type {}", cfg.config_operation),
        ));
    }
    for cfg in &resource.configs {
        // krabka's controller-published keys stand outside the alter paths.
        if config_keys::is_controller_managed_broker_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is controller-managed and read-only",
                    cfg.name
                ),
            ));
        }
        if node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
            && CLUSTER_DEFAULT_ONLY.contains(&cfg.name.as_str())
        {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    cfg.name
                ),
            ));
        }
    }
    let mut props = stored_dynamic_configs(context.image, node_id);
    apply_operations(resource, &mut props, context.unstable)?;
    let per_broker = node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
    // The records carry the client's strings, as Kafka's do: validation
    // parses a value and keeps nothing of the parse.
    validate_dynamic_broker_configs(&props, per_broker, context.unstable)?;
    if per_broker {
        cordoned_log_dirs_error(&props, context.log_dirs)?;
    }

    // Kafka writes a record for every key the request names, changed or not
    // (KAFKA-14136), and checks the ELR rules on each.
    let mut records = Vec::with_capacity(resource.configs.len());
    for cfg in &resource.configs {
        let value = props.get(&cfg.name).cloned();
        if let Some(error) = elr_min_isr_error(context.image, node_id, &cfg.name, value.as_deref())
        {
            return Err(error);
        }
        if let Some(error) = cordoned_log_dirs_disabled_error(context.image, &cfg.name) {
            return Err(error);
        }
        records.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: cfg.name.clone(),
            config_value: value,
        }));
    }
    Ok(records)
}

/// Broker-side checks whose inputs belong to the client-addressed node.
/// The controller applies the operations and validates the complete config
/// again against its authoritative image.
///
/// For this node's own resource that includes Kafka's reconfigurable checks,
/// which only the addressed node can make, over the stored dynamic configs
/// with the operations applied, after the key rules those configs must pass
/// first: the listener values of the config the node would run with, then the
/// TLS listeners' new keystores and truststores, then the listeners it keeps
/// ([`OwnBrokerChecks`]).
pub(super) fn preprocess_broker_resource(
    resource: &AlterConfigsResource,
    context: BrokerConfigContext<'_>,
    checks: OwnBrokerChecks<'_>,
) -> Result<(), (i16, String)> {
    if context.is_cluster_default_resource(&resource.resource_name)? {
        return Ok(());
    }
    // Check only newly introduced directories. Existing stored values may be
    // stale here; their merge and wildcard exclusivity are controller checks.
    for config in &resource.configs {
        if config.name == CORDONED_LOG_DIRS && matches!(config.config_operation, OP_SET | OP_APPEND)
        {
            let props = BTreeMap::from([(
                CORDONED_LOG_DIRS.to_owned(),
                config.value.clone().unwrap_or_default(),
            )]);
            cordoned_log_dirs_error(&props, context.log_dirs)?;
        }
    }
    let current = stored_dynamic_configs(context.image, context.serving);
    let mut proposed = current.clone();
    apply_operations(resource, &mut proposed, context.unstable)?;
    validate_dynamic_broker_configs(&proposed, true, context.unstable)?;
    checks.check(context.image, &current, &proposed)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            features::{ELR_VERSION, METADATA_VERSION},
            handlers::{
                incremental_alter_configs::test_support::{
                    make_del_cfg, make_image_with_broker, make_resource, make_set_cfg,
                },
                test_support::broker_config_context,
            },
            test_support::broker_registration,
        },
        assert2::check,
        krabka_metadata::{MetadataImage, NodeId},
        krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig,
        std::path::PathBuf,
    };

    const SERVING: NodeId = NodeId(1);

    fn record(node: NodeId, key: &str, value: Option<&str>) -> MetadataRecord {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: node,
            config_name: key.into(),
            config_value: value.map(str::to_owned),
        })
    }

    /// One stored dynamic config of a broker resource.
    #[derive(Clone, Copy)]
    struct StoredConfig<'a> {
        node: NodeId,
        key: &'a str,
        value: &'a str,
    }

    fn image_with(configs: &[StoredConfig<'_>], elr: bool) -> MetadataImage {
        let mut image = make_image_with_broker(SERVING);
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                port: 9093,
                ..broker_registration(krabka_raft::NodeId(2))
            },
        ));
        for StoredConfig { node, key, value } in configs {
            image.apply(&record(*node, key, Some(value)));
        }
        if elr {
            image.apply(&MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: ELR_VERSION.into(),
                    level: 1,
                },
            ));
        }
        image
    }

    #[derive(Default)]
    struct IncrementalBrokerConfiguration {
        image: Option<MetadataImage>,
        resource: AlterConfigsResource,
        log_dirs: Vec<PathBuf>,
        result: Option<Result<Vec<MetadataRecord>, (i16, String)>>,
    }
    #[rstest::fixture]
    fn incremental_broker() -> IncrementalBrokerConfiguration {
        let image = None;
        IncrementalBrokerConfiguration {
            image,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "broker 1 serves resource \"{resource}\" with ELR enabled {elr} at metadata.version {level}"
    )]
    fn broker_configuration_context(
        incremental_broker: &mut IncrementalBrokerConfiguration,
        resource: String,
        elr: bool,
        level: i16,
    ) {
        let mut image = image_with(&[], elr);
        image.apply(&MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: METADATA_VERSION.into(),
                level,
            },
        ));
        incremental_broker.image = Some(image);
        incremental_broker.resource = make_resource(
            if resource == "<empty>" { "" } else { &resource },
            Vec::new(),
        );
    }
    #[rstest_bdd_macros::given("the broker has configured log directories /tmp/d1 and /tmp/d2")]
    fn configured_cordon_directories(incremental_broker: &mut IncrementalBrokerConfiguration) {
        incremental_broker.log_dirs = vec![PathBuf::from("/tmp/d1"), PathBuf::from("/tmp/d2")];
    }
    #[rstest_bdd_macros::given(
        "broker configuration already stores node {node}, key {key} and value \"{value}\""
    )]
    fn stored_incremental_broker_config(
        incremental_broker: &mut IncrementalBrokerConfiguration,
        node: i64,
        key: String,
        value: String,
    ) {
        let node = if node == -1 {
            krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
        } else {
            NodeId(u64::try_from(node).expect("broker ID"))
        };
        incremental_broker
            .image
            .as_mut()
            .expect("configuration context")
            .apply(&record(node, &key, Some(&value)));
    }
    #[rstest_bdd_macros::when(
        "IncrementalAlterConfigs applies operation {operation} to key {key} with value \"{value}\""
    )]
    fn alter_incremental_broker_config(
        incremental_broker: &mut IncrementalBrokerConfiguration,
        operation: i8,
        key: String,
        value: String,
    ) {
        let value = if value == "<empty>" {
            String::new()
        } else {
            value
        };
        incremental_broker.resource.configs = vec![AlterableConfig {
            name: key,
            config_operation: operation,
            value: (value != "null").then_some(value),
            ..Default::default()
        }];
        incremental_broker.result = Some(broker_records(
            &incremental_broker.resource,
            broker_config_context()
                .image(
                    incremental_broker
                        .image
                        .as_ref()
                        .expect("configuration image"),
                )
                .log_dirs(&incremental_broker.log_dirs)
                .call(),
        ));
    }
    #[rstest_bdd_macros::then(
        "the staged broker config is node {node}, key {key} and value \"{value}\""
    )]
    fn staged_incremental_broker_config(
        incremental_broker: &IncrementalBrokerConfiguration,
        node: i64,
        key: String,
        value: String,
    ) {
        let node = if node == -1 {
            krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
        } else {
            NodeId(u64::try_from(node).expect("broker ID"))
        };
        check!(
            incremental_broker
                .result
                .as_ref()
                .expect("configuration result")
                == &Ok(vec![record(
                    node,
                    &key,
                    (value != "null").then_some(value.as_str())
                )])
        );
    }
    #[rstest_bdd_macros::then(
        "the incremental broker config is refused with code {code} and message \"{message}\""
    )]
    fn refused_incremental_broker_config(
        incremental_broker: &IncrementalBrokerConfiguration,
        code: i16,
        message: String,
    ) {
        check!(
            incremental_broker
                .result
                .as_ref()
                .expect("configuration result")
                == &Err((code, message))
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_incremental_broker_configuration.feature", fixtures = [incremental_broker: IncrementalBrokerConfiguration]);

    /// Kafka stores the string the client sent
    /// (`ConfigurationControlManager.incrementalAlterConfigResource` takes the
    /// operation's value as it is, and `DynamicConfig.Broker.validate` drops
    /// the parse), so a describe returns what the alter wrote and a tool that
    /// diffs the two does not loop: a `DOUBLE` that `Double.toString` prints as
    /// `1.0`, a `LONG` it prints as `1.048576E8`, a padded `INT`, and a list
    /// with spaces are all kept as they came.
    #[test]
    fn a_broker_resource_stores_the_string_the_client_sent() {
        let cluster = krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
        let sent = [
            ("log.cleaner.min.cleanable.ratio", "1"),
            ("log.cleaner.io.max.bytes.per.second", "104857600"),
            ("num.io.threads", " 16 "),
            ("log.cleanup.policy", "compact , delete"),
        ];
        let resource = make_resource(
            "",
            sent.iter()
                .map(|(key, value)| make_set_cfg(key, value))
                .collect(),
        );

        let records = broker_records(
            &resource,
            broker_config_context()
                .image(&image_with(&[], false))
                .call(),
        );

        check!(
            records
                == Ok(sent
                    .iter()
                    .map(|(key, value)| record(cluster, key, Some(value)))
                    .collect())
        );
    }

    #[test]
    fn controller_managed_broker_configs_are_rejected_as_read_only() {
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for cfg in [make_set_cfg(key, "true"), make_del_cfg(key)] {
                let resource = make_resource("1", vec![cfg]);
                check!(
                    broker_records(
                        &resource,
                        broker_config_context()
                            .image(&image_with(&[], false))
                            .call()
                    ) == Err((
                        codes::INVALID_CONFIG,
                        format!("broker config {key} is controller-managed and read-only"),
                    )),
                    "key {key}"
                );
            }
        }
    }
}
