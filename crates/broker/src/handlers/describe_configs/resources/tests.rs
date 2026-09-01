//! What `describe_one` reports for each resource type: the effective value,
//! the layer it came from, the synonym chain beneath it, and the typed
//! metadata the JVM `AdminClient` reads.

use assert2::{assert, check};
use krabka_metadata::{
    BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
    TopicConfigRecord,
};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    },
};
use uuid::Uuid;

use super::{
    super::{
        entry::EntryOptions,
        wire::{
            CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_BROKER,
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_TOPIC,
            CONFIG_SOURCE_STATIC_BROKER,
        },
    },
    *,
};
use crate::config_keys::registry::ConfigType;

/// A request that asks for everything, the way `kafka-configs --describe
/// --all` does.
pub(super) const EVERYTHING: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: true,
};

/// A request that asks for values alone.
const VALUES_ONLY: EntryOptions = EntryOptions {
    include_synonyms: false,
    include_documentation: false,
};

fn describe(
    image: &MetadataImage,
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
    options: EntryOptions,
) -> DescribeConfigsResult {
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info");
    describe_with_loggers(
        image,
        resource_type,
        resource_name,
        configuration_keys,
        options,
        BrokerLoggers {
            node_id: 1,
            levels: &levels,
        },
    )
}

/// The same, with the node id and live filter a `BROKER_LOGGER` resource is
/// resolved against.
fn describe_with_loggers(
    image: &MetadataImage,
    resource_type: i8,
    resource_name: &str,
    configuration_keys: Option<Vec<String>>,
    options: EntryOptions,
    loggers: BrokerLoggers<'_>,
) -> DescribeConfigsResult {
    describe_one(
        image,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type,
            resource_name: resource_name.to_owned(),
            configuration_keys,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        loggers,
        options,
    )
}

/// A `BROKER_LOGGER` describe against a node whose id is 7.
#[test]
fn broker_logger_resource_names_this_node_or_is_refused() {
    let image = MetadataImage::new(Uuid::nil());
    let (levels, _filter) = krabka_telemetry::LogLevelController::new("info,krabka_broker=debug");

    let result = describe_with_loggers(
        &image,
        RESOURCE_TYPE_BROKER_LOGGER,
        "7",
        None,
        VALUES_ONLY,
        BrokerLoggers {
            node_id: 7,
            levels: &levels,
        },
    );
    check!(result.error_code == crate::codes::NONE);
    check!(
        result
            .configs
            .iter()
            .any(|c| c.name == "krabka_broker" && c.value.as_deref() == Some("DEBUG"))
    );

    let refused = describe_with_loggers(
        &image,
        RESOURCE_TYPE_BROKER_LOGGER,
        "8",
        None,
        VALUES_ONLY,
        BrokerLoggers {
            node_id: 7,
            levels: &levels,
        },
    );
    check!(refused.error_code == crate::codes::INVALID_REQUEST);
    check!(
        refused.error_message.as_deref() == Some("Unexpected broker id, expected 7 but received 8")
    );
    check!(refused.configs.is_empty());
}

/// Describe one topic the way `kafka-configs --describe --all` does.
pub(super) fn describe_topic(
    image: &MetadataImage,
    topic: &str,
    configuration_keys: Option<Vec<String>>,
) -> DescribeConfigsResult {
    describe(
        image,
        RESOURCE_TYPE_TOPIC,
        topic,
        configuration_keys,
        EVERYTHING,
    )
}

/// The one entry a result holds for `name`.
pub(super) fn entry_named<'a>(
    result: &'a DescribeConfigsResult,
    name: &str,
) -> &'a DescribeConfigsResourceResult {
    result
        .configs
        .iter()
        .find(|entry| entry.name == name)
        .unwrap_or_else(|| panic!("no `{name}` entry in {:?}", result.configs))
}

fn synonym(name: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    DescribeConfigsSynonym {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        source,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn image_with_broker_config(
    node_id: krabka_metadata::NodeId,
    pairs: &[(&str, &str)],
) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for (name, value) in pairs {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: (*name).to_owned(),
            config_value: Some((*value).to_owned()),
        }));
    }
    image
}

/// The case the whole change exists for: a topic override sitting above a
/// cluster default, described in full.
///
/// The two entries are compared whole, because every field of them is the
/// answer: the effective value, the layer it came from, the chain beneath it
/// in Kafka's precedence order, the `ConfigDef` type the JVM `AdminClient`
/// parses the value with, and the documentation `include_documentation` asked
/// for. Verified against `apache/kafka:4.3.1`, which answers the same shape
/// for `retention.ms` over `log.retention.ms`.
#[test]
fn a_topic_reports_its_override_above_the_cluster_default_with_the_whole_chain() {
    let mut image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
    );
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: maplit::btreemap! {
        config_keys::RETENTION_MS.to_string() => "60000".to_string()},
    }));

    let result = describe_topic(
        &image,
        "orders",
        Some(vec![
            config_keys::RETENTION_MS.to_owned(),
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
            config_keys::CLEANUP_POLICY.to_owned(),
        ]),
    );

    let retention =
        registry::lookup(ConfigScope::Topic, config_keys::RETENTION_MS).expect("retention.ms");
    let unclean = registry::lookup(
        ConfigScope::Topic,
        config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
    )
    .expect("unclean.leader.election.enable");
    let policy =
        registry::lookup(ConfigScope::Topic, config_keys::CLEANUP_POLICY).expect("cleanup.policy");

    assert!(
        result
            == DescribeConfigsResult {
                error_code: crate::codes::NONE,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".to_owned(),
                configs: vec![
                    // Set nowhere, and krabka reads no broker-level
                    // `log.cleanup.policy`, so the key reports its built-in
                    // default under an empty chain. `apache/kafka:4.3.1`
                    // answers that same shape for a topic key it names no
                    // broker config for, such as `remote.storage.enable`.
                    DescribeConfigsResourceResult {
                        name: config_keys::CLEANUP_POLICY.to_owned(),
                        value: Some("delete".to_owned()),
                        read_only: false,
                        config_source: CONFIG_SOURCE_DEFAULT,
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: ConfigType::List.wire(),
                        documentation: Some(policy.doc.to_owned()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                    DescribeConfigsResourceResult {
                        name: config_keys::RETENTION_MS.to_owned(),
                        value: Some("60000".to_owned()),
                        read_only: false,
                        config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                        is_sensitive: false,
                        synonyms: vec![synonym(
                            config_keys::RETENTION_MS,
                            "60000",
                            CONFIG_SOURCE_DYNAMIC_TOPIC
                        )],
                        config_type: ConfigType::Long.wire(),
                        documentation: Some(retention.doc.to_owned()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                    // Not set on the topic, so the cluster default wins and
                    // the built-in default sits below it.
                    DescribeConfigsResourceResult {
                        name: config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
                        value: Some("true".to_owned()),
                        read_only: false,
                        config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                        is_sensitive: false,
                        synonyms: vec![
                            synonym(
                                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                                "true",
                                CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                            ),
                            synonym(
                                config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                                "false",
                                CONFIG_SOURCE_DEFAULT
                            ),
                        ],
                        config_type: ConfigType::Boolean.wire(),
                        documentation: Some(unclean.doc.to_owned()),
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
}

#[test]
fn a_topic_override_stays_at_the_head_of_the_chain_above_the_cluster_default() {
    // The other half of the precedence rule: when the topic *does* set the
    // key, the cluster default drops to a synonym and the source says
    // DYNAMIC_TOPIC_CONFIG. An operator reads the override and the value it
    // displaced in one response.
    let mut image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true")],
    );
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: maplit::btreemap! {
        config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_string() => "false".to_string()},
    }));

    let result = describe_topic(
        &image,
        "orders",
        Some(vec![config_keys::UNCLEAN_LEADER_ELECTION_ENABLE.to_owned()]),
    );
    let entry = entry_named(&result, config_keys::UNCLEAN_LEADER_ELECTION_ENABLE);

    check!(entry.value == Some("false".to_owned()));
    check!(entry.config_source == CONFIG_SOURCE_DYNAMIC_TOPIC);
    check!(
        entry.synonyms
            == vec![
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "false",
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                ),
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "true",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                ),
                synonym(
                    config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
                    "false",
                    CONFIG_SOURCE_DEFAULT
                ),
            ]
    );
}

#[test]
fn a_topic_with_no_overrides_reports_every_key_at_its_default() {
    // `kafka-configs --describe --all` shows effective configuration, so a
    // topic that overrides nothing still answers with every key it has.
    let image = MetadataImage::new(Uuid::nil());
    let result = describe_topic(&image, "orders", None);

    let reported: Vec<&str> = result.configs.iter().map(|e| e.name.as_str()).collect();
    let mut expected: Vec<&str> = registry::keys_in(ConfigScope::Topic)
        .map(|row| row.name)
        .collect();
    expected.sort_unstable();

    check!(reported == expected);
    check!(
        result
            .configs
            .iter()
            .all(|entry| entry.config_source == CONFIG_SOURCE_DEFAULT)
    );
    check!(result.configs.iter().all(|entry| entry.config_type != 0));
    check!(result.configs.iter().all(|entry| !entry.is_sensitive));
    check!(
        result
            .configs
            .iter()
            .all(|entry| entry.documentation.is_some())
    );
}

#[test]
fn the_fixed_data_path_key_is_read_only_and_typed() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "events".into(),
        overrides: maplit::btreemap! {
        config_keys::DISKLESS.to_string() => "true".to_string()},
    }));

    let result = describe_topic(
        &image,
        "events",
        Some(vec![config_keys::DISKLESS.to_owned()]),
    );

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: config_keys::DISKLESS.to_owned(),
                value: Some("true".to_owned()),
                read_only: true,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: vec![synonym(
                    config_keys::DISKLESS,
                    "true",
                    CONFIG_SOURCE_DYNAMIC_TOPIC
                )],
                config_type: ConfigType::Boolean.wire(),
                documentation: Some(
                    registry::lookup(ConfigScope::Topic, config_keys::DISKLESS)
                        .expect("krabka.diskless")
                        .doc
                        .to_owned()
                ),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }]
    );
}

#[test]
fn a_broker_reports_its_per_node_override_above_the_cluster_default() {
    let mut image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024")],
    );
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.to_owned(),
        config_value: Some("512".to_owned()),
    }));

    let result = describe(
        &image,
        RESOURCE_TYPE_BROKER,
        "2",
        Some(vec![
            crate::throttle::LEADER_THROTTLED_RATE_KEY.to_owned(),
            NODE_ID.to_owned(),
        ]),
        EVERYTHING,
    );

    assert!(
        result.configs
            == vec![
                DescribeConfigsResourceResult {
                    name: crate::throttle::LEADER_THROTTLED_RATE_KEY.to_owned(),
                    value: Some("1024".to_owned()),
                    read_only: false,
                    config_source: CONFIG_SOURCE_DYNAMIC_BROKER,
                    is_sensitive: false,
                    synonyms: vec![
                        synonym(
                            crate::throttle::LEADER_THROTTLED_RATE_KEY,
                            "1024",
                            CONFIG_SOURCE_DYNAMIC_BROKER
                        ),
                        synonym(
                            crate::throttle::LEADER_THROTTLED_RATE_KEY,
                            "512",
                            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                        ),
                    ],
                    config_type: ConfigType::Long.wire(),
                    documentation: Some(
                        registry::lookup(
                            ConfigScope::Broker,
                            crate::throttle::LEADER_THROTTLED_RATE_KEY
                        )
                        .expect("leader.replication.throttled.rate")
                        .doc
                        .to_owned()
                    ),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                DescribeConfigsResourceResult {
                    name: NODE_ID.to_owned(),
                    value: Some("2".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_STATIC_BROKER,
                    is_sensitive: false,
                    synonyms: vec![synonym(NODE_ID, "2", CONFIG_SOURCE_STATIC_BROKER)],
                    config_type: ConfigType::Int.wire(),
                    documentation: Some(
                        registry::lookup(ConfigScope::Broker, NODE_ID)
                            .expect("node.id")
                            .doc
                            .to_owned()
                    ),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ]
    );
}

#[test]
fn the_cluster_default_resource_reports_the_defaults_and_no_node_id() {
    // An empty resource name is Kafka's cluster-wide default broker
    // resource. It holds no per-node layer, and no node runs it, so the
    // static `node.id` entry a numeric name carries has no place in it.
    let image = image_with_broker_config(
        DEFAULT_BROKER_CONFIG_NODE_ID,
        &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024")],
    );

    let result = describe(&image, RESOURCE_TYPE_BROKER, "", None, EVERYTHING);

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: crate::throttle::LEADER_THROTTLED_RATE_KEY.to_owned(),
                value: Some("1024".to_owned()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                is_sensitive: false,
                synonyms: vec![synonym(
                    crate::throttle::LEADER_THROTTLED_RATE_KEY,
                    "1024",
                    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER
                )],
                config_type: ConfigType::Long.wire(),
                documentation: Some(
                    registry::lookup(
                        ConfigScope::Broker,
                        crate::throttle::LEADER_THROTTLED_RATE_KEY
                    )
                    .expect("leader.replication.throttled.rate")
                    .doc
                    .to_owned()
                ),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }]
    );
}

#[test]
fn a_broker_that_overrides_nothing_still_reports_its_static_node_id() {
    // `node.id` never reaches the metadata image, so a node with no dynamic
    // override at all is the case where the static layer is the whole
    // response. `apache/kafka:4.3.1` answers `node.id` the same way: value
    // from the static configuration, `STATIC_BROKER_CONFIG`, read-only.
    let result = describe(
        &MetadataImage::new(Uuid::nil()),
        RESOURCE_TYPE_BROKER,
        "7",
        None,
        VALUES_ONLY,
    );

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: NODE_ID.to_owned(),
                value: Some("7".to_owned()),
                read_only: true,
                config_source: CONFIG_SOURCE_STATIC_BROKER,
                is_sensitive: false,
                // The request asked for neither, so the entry carries
                // neither, even though the registry has both.
                synonyms: Vec::new(),
                config_type: ConfigType::Int.wire(),
                documentation: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }]
    );
}

#[test]
fn the_key_filter_decides_what_a_broker_resource_reports() {
    let image = image_with_broker_config(
        krabka_metadata::NodeId(2),
        &[
            (crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024"),
            (crate::throttle::FOLLOWER_THROTTLED_RATE_KEY, "512"),
        ],
    );

    for (label, filter, expected) in [
        (
            "no filter reports every stored key beside the static node id",
            None,
            vec![
                crate::throttle::FOLLOWER_THROTTLED_RATE_KEY,
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                NODE_ID,
            ],
        ),
        (
            "a filter narrows the response to the keys it names",
            Some(vec![crate::throttle::LEADER_THROTTLED_RATE_KEY]),
            vec![crate::throttle::LEADER_THROTTLED_RATE_KEY],
        ),
        (
            "a filter that names no key this broker holds reports nothing",
            Some(vec!["no.such.key"]),
            Vec::new(),
        ),
    ] {
        let configuration_keys =
            filter.map(|keys| keys.iter().map(|key| (*key).to_owned()).collect());
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "2",
            configuration_keys,
            VALUES_ONLY,
        );
        let names: Vec<&str> = result.configs.iter().map(|e| e.name.as_str()).collect();

        check!(names == expected, "{label}");
    }
}

#[test]
fn every_key_an_alter_can_store_on_a_broker_comes_back_with_its_value() {
    // The registry's own hazard: a stored key with no row is a key the
    // entry builder must not disclose, so it would come back null. Every
    // key the alter path accepts therefore has to have a row, and the
    // read-only rows are exactly the ones the alter path refuses.
    use crate::handlers::incremental_alter_configs::is_known_broker_config;

    for row in registry::keys_in(ConfigScope::Broker) {
        check!(
            is_known_broker_config(row.name) == !row.read_only,
            "{} is alterable={} but read_only={}",
            row.name,
            is_known_broker_config(row.name),
            row.read_only
        );
        if row.read_only {
            continue;
        }
        let image = image_with_broker_config(krabka_metadata::NodeId(1), &[(row.name, "1")]);
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![row.name.to_owned()]),
            VALUES_ONLY,
        );
        let entry = entry_named(&result, row.name);

        check!(entry.value == Some("1".to_owned()), "{}", row.name);
        check!(!entry.is_sensitive, "{}", row.name);
        check!(entry.config_type == row.config_type.wire(), "{}", row.name);
    }
}

#[test]
fn a_controller_managed_broker_key_is_read_only_wherever_it_is_reported() {
    // The registry and `is_controller_managed_broker_config` have to agree:
    // the alter paths refuse the key by the second, and `kafka-configs` must
    // say so by the first.
    for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
        let image = image_with_broker_config(krabka_metadata::NodeId(1), &[(key, "true")]);
        let result = describe(
            &image,
            RESOURCE_TYPE_BROKER,
            "1",
            Some(vec![key.to_owned()]),
            VALUES_ONLY,
        );
        check!(entry_named(&result, key).read_only, "{key}");
        check!(
            config_keys::is_controller_managed_broker_config(key),
            "{key}"
        );
    }
}

#[test]
fn a_non_numeric_broker_resource_name_is_refused() {
    let image = MetadataImage::new(Uuid::nil());
    let result = describe(
        &image,
        RESOURCE_TYPE_BROKER,
        "not-a-number",
        None,
        EVERYTHING,
    );

    assert!(
        result
            == DescribeConfigsResult {
                error_code: crate::codes::INVALID_REQUEST,
                error_message: Some(
                    "resource_name `not-a-number` is not a valid broker id".to_owned()
                ),
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "not-a-number".to_owned(),
                configs: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
}

#[test]
fn a_client_metrics_subscription_reports_all_three_keys_typed() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1ClientMetricsConfig(
        krabka_metadata::ClientMetricsConfigRecord {
            name: "sub-1".to_owned(),
            configs: maplit::btreemap! {
                crate::client_metrics::config::KEY_METRICS.to_string() => "org.apache.kafka".to_string()},
        },
    ));

    let result = describe(
        &image,
        RESOURCE_TYPE_CLIENT_METRICS,
        "sub-1",
        None,
        EVERYTHING,
    );
    let metrics = entry_named(&result, crate::client_metrics::config::KEY_METRICS);
    let interval = entry_named(&result, crate::client_metrics::config::KEY_INTERVAL_MS);
    let matcher = entry_named(&result, crate::client_metrics::config::KEY_MATCH);

    check!(metrics.value == Some("org.apache.kafka".to_owned()));
    check!(metrics.config_source == CONFIG_SOURCE_CLIENT_METRICS);
    check!(metrics.config_type == ConfigType::List.wire());
    // Unset keys report the broker's effective default, not a blank.
    check!(interval.value == Some("300000".to_owned()));
    check!(interval.config_source == CONFIG_SOURCE_DEFAULT);
    check!(interval.config_type == ConfigType::Int.wire());
    check!(matcher.value == Some(String::new()));
    check!(matcher.config_type == ConfigType::List.wire());
}

#[test]
fn a_group_reports_its_override_above_the_streams_default() {
    use crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS;

    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1GroupConfig(
        krabka_metadata::GroupConfigRecord {
            group_id: "streams-1".to_owned(),
            configs: maplit::btreemap! {
            KEY_NUM_STANDBY_REPLICAS.to_string() => "2".to_string()},
        },
    ));

    let defaults = crate::coordinator::unified::streams::config::StreamsGroupConfig::default()
        .group_config_values();
    let fallback = defaults
        .iter()
        .find(|(key, _)| *key == KEY_NUM_STANDBY_REPLICAS)
        .map(|(_, value)| value.clone())
        .expect("streams.num.standby.replicas has a default");

    let result = describe(&image, RESOURCE_TYPE_GROUP, "streams-1", None, EVERYTHING);
    let entry = entry_named(&result, KEY_NUM_STANDBY_REPLICAS);

    check!(entry.value == Some("2".to_owned()));
    check!(entry.config_source == CONFIG_SOURCE_DYNAMIC_GROUP);
    check!(entry.config_type == ConfigType::Int.wire());
    check!(
        entry.synonyms
            == vec![
                synonym(KEY_NUM_STANDBY_REPLICAS, "2", CONFIG_SOURCE_DYNAMIC_GROUP),
                synonym(KEY_NUM_STANDBY_REPLICAS, &fallback, CONFIG_SOURCE_DEFAULT),
            ]
    );
}

#[test]
fn every_key_a_group_or_a_subscription_answers_with_is_typed_and_disclosed() {
    // The same hazard as the broker resource, over the two key sets the
    // broker itself supplies: `StreamsGroupConfig` decides which group keys
    // a response holds, and a key it names that the registry does not would
    // come back untyped and with no value at all.
    let image = MetadataImage::new(Uuid::nil());
    let group = describe(&image, RESOURCE_TYPE_GROUP, "streams-1", None, EVERYTHING);
    let subscription = describe(
        &image,
        RESOURCE_TYPE_CLIENT_METRICS,
        "sub-1",
        None,
        EVERYTHING,
    );

    let defaults = crate::coordinator::unified::streams::config::StreamsGroupConfig::default()
        .group_config_values();
    let reported: Vec<&str> = group.configs.iter().map(|e| e.name.as_str()).collect();
    let expected: Vec<&str> = defaults.keys().map(String::as_str).collect();

    check!(reported == expected);
    for entry in group.configs.iter().chain(&subscription.configs) {
        check!(entry.config_type != 0i8, "{} is untyped", entry.name);
        check!(!entry.is_sensitive, "{} is withheld", entry.name);
        check!(entry.value.is_some(), "{} has no value", entry.name);
        check!(entry.documentation.is_some(), "{} has no doc", entry.name);
    }
}

#[test]
fn an_unhandled_resource_type_reports_nothing_and_no_error() {
    let image = MetadataImage::new(Uuid::nil());
    let result = describe(&image, 99, "whatever", None, EVERYTHING);

    assert!(
        result
            == DescribeConfigsResult {
                error_code: crate::codes::NONE,
                error_message: None,
                resource_type: 99,
                resource_name: "whatever".to_owned(),
                configs: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
    );
}
