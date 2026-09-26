//! What the two KIP-98 static broker entries say: the value and source a node
//! on Kafka's default reports, the `STATIC_BROKER_CONFIG` head an operator
//! override adds over the retained `DEFAULT_CONFIG` synonym, the typed
//! metadata the registry supplies, and the request's key filter.

use assert2::{assert, check};
use krabka_protocol::{
    UnknownTaggedFields, owned::describe_configs_response::DescribeConfigsSynonym,
};

use super::{super::super::wire::CONFIG_SOURCE_DEFAULT, *};

const BOTH: EntryOptions = EntryOptions {
    include_synonyms: true,
    include_documentation: true,
};
const NEITHER: EntryOptions = EntryOptions {
    include_synonyms: false,
    include_documentation: false,
};

/// Both KIP-98 keys wanted. The module's subject is that pair, so the cases
/// below ask for it and leave the KIP-211 pair `static_broker_entries` also
/// reports to [`super::super::tests`].
fn both_expiry_keys(key: &str) -> bool {
    key == config_keys::TRANSACTIONAL_ID_EXPIRATION_MS
        || key == config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
}

fn doc_for(key: &str) -> String {
    registry::lookup(ConfigScope::Broker, key)
        .expect("a registered broker key")
        .doc
        .to_owned()
}

/// `ConfigDef.Type::INT`, which is what Kafka defines both keys as.
const INT: i8 = 3;

/// Both keys as an operator supplied them.
fn supplied(expiration_ms: i32, cleanup_interval_ms: i32) -> StaticBrokerConfigs<'static> {
    StaticBrokerConfigs {
        txn_id_expiration: StaticBrokerSetting {
            value_ms: expiration_ms,
            supplied: true,
        },
        txn_id_expiration_cleanup_interval: StaticBrokerSetting {
            value_ms: cleanup_interval_ms,
            supplied: true,
        },
        ..kafka_default_static_broker()
    }
}

/// A node still on Kafka's built-in defaults reports both keys at
/// `DEFAULT_CONFIG`, with the default as their one synonym.
#[test]
fn kafka_defaults_report_at_default_config_source() {
    let entries = static_broker_entries(kafka_default_static_broker(), &both_expiry_keys, BOTH);

    assert!(
        entries
            == vec![
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                    value: Some("604800000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_DEFAULT,
                    is_sensitive: false,
                    synonyms: vec![DescribeConfigsSynonym {
                        name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                        value: Some("604800000".to_owned()),
                        source: CONFIG_SOURCE_DEFAULT,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    config_type: INT,
                    documentation: Some(doc_for(config_keys::TRANSACTIONAL_ID_EXPIRATION_MS)),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS.to_owned(),
                    value: Some("3600000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_DEFAULT,
                    is_sensitive: false,
                    synonyms: vec![DescribeConfigsSynonym {
                        name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                            .to_owned(),
                        value: Some("3600000".to_owned()),
                        source: CONFIG_SOURCE_DEFAULT,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    config_type: INT,
                    documentation: Some(doc_for(
                        config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                    )),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ]
    );
}

/// The second `apache/kafka:4.3.1` output quoted on the module: the operator's
/// value heads the chain at `STATIC_BROKER_CONFIG`, and Kafka's default stays
/// under it as a `DEFAULT_CONFIG` synonym.
#[test]
fn an_operator_override_heads_the_chain_over_the_retained_default() {
    let entries = static_broker_entries(supplied(120_000, 60_000), &both_expiry_keys, BOTH);

    assert!(
        entries
            == vec![
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                    value: Some("120000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_STATIC_BROKER,
                    is_sensitive: false,
                    synonyms: vec![
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                            value: Some("120000".to_owned()),
                            source: CONFIG_SOURCE_STATIC_BROKER,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                            value: Some("604800000".to_owned()),
                            source: CONFIG_SOURCE_DEFAULT,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    config_type: INT,
                    documentation: Some(doc_for(config_keys::TRANSACTIONAL_ID_EXPIRATION_MS)),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS.to_owned(),
                    value: Some("60000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_STATIC_BROKER,
                    is_sensitive: false,
                    synonyms: vec![
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                                .to_owned(),
                            value: Some("60000".to_owned()),
                            source: CONFIG_SOURCE_STATIC_BROKER,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                                .to_owned(),
                            value: Some("3600000".to_owned()),
                            source: CONFIG_SOURCE_DEFAULT,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    config_type: INT,
                    documentation: Some(doc_for(
                        config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                    )),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ]
    );
}

/// A request that asked for neither synonyms nor documentation gets neither,
/// and still gets the value and the typed metadata.
#[test]
fn a_bare_request_carries_the_value_without_synonyms_or_documentation() {
    let entries = static_broker_entries(kafka_default_static_broker(), &both_expiry_keys, NEITHER);

    assert!(
        entries
            == vec![
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                    value: Some("604800000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_DEFAULT,
                    is_sensitive: false,
                    synonyms: Vec::new(),
                    config_type: INT,
                    documentation: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS.to_owned(),
                    value: Some("3600000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_DEFAULT,
                    is_sensitive: false,
                    synonyms: Vec::new(),
                    config_type: INT,
                    documentation: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ]
    );
}

/// `configuration_keys` narrows the static entries the way it narrows every
/// other entry a broker resource reports.
#[test]
fn the_request_key_filter_narrows_the_static_entries() {
    let only_expiry = |key: &str| key == config_keys::TRANSACTIONAL_ID_EXPIRATION_MS;

    let entries = static_broker_entries(kafka_default_static_broker(), &only_expiry, NEITHER);

    let names: Vec<&str> = entries.iter().map(|entry| entry.name.as_str()).collect();
    check!(names == vec![config_keys::TRANSACTIONAL_ID_EXPIRATION_MS]);
}

/// Config source is provenance, not a value comparison: a key the operator
/// supplied heads the chain at `STATIC_BROKER_CONFIG` even when the value it
/// supplies is Kafka's own default.
///
/// `apache/kafka:4.3.1` started with
/// `KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS=604800000` -- the built-in default,
/// written out -- answers `kafka-configs --entity-type brokers --entity-name 1
/// --describe --all` with
///
/// ```text
/// transactional.id.expiration.ms=604800000 sensitive=false
///   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=604800000,
///             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
/// ```
///
/// while the key it was *not* given keeps the one-synonym default chain. An
/// operator who reads only `DEFAULT_CONFIG` there would conclude the broker
/// had ignored their setting.
#[test]
fn a_supplied_value_identical_to_the_default_still_reports_as_static() {
    let configs = StaticBrokerConfigs {
        txn_id_expiration: StaticBrokerSetting {
            value_ms: 604_800_000,
            supplied: true,
        },
        txn_id_expiration_cleanup_interval: StaticBrokerSetting {
            value_ms: 3_600_000,
            supplied: false,
        },
        ..kafka_default_static_broker()
    };

    let entries = static_broker_entries(configs, &both_expiry_keys, BOTH);

    assert!(
        entries
            == vec![
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                    value: Some("604800000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_STATIC_BROKER,
                    is_sensitive: false,
                    synonyms: vec![
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                            value: Some("604800000".to_owned()),
                            source: CONFIG_SOURCE_STATIC_BROKER,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        DescribeConfigsSynonym {
                            name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_owned(),
                            value: Some("604800000".to_owned()),
                            source: CONFIG_SOURCE_DEFAULT,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    config_type: INT,
                    documentation: Some(doc_for(config_keys::TRANSACTIONAL_ID_EXPIRATION_MS)),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                DescribeConfigsResourceResult {
                    name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS.to_owned(),
                    value: Some("3600000".to_owned()),
                    read_only: true,
                    config_source: CONFIG_SOURCE_DEFAULT,
                    is_sensitive: false,
                    synonyms: vec![DescribeConfigsSynonym {
                        name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                            .to_owned(),
                        value: Some("3600000".to_owned()),
                        source: CONFIG_SOURCE_DEFAULT,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    config_type: INT,
                    documentation: Some(doc_for(
                        config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                    )),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ]
    );
}

/// The KIP-464 pair (#728). A node that never named either key reports
/// Kafka's default of 1 at `DEFAULT_CONFIG`. A named value heads the chain at
/// `STATIC_BROKER_CONFIG` with the default beneath it.
#[test]
fn topic_creation_defaults_report_their_provenance() {
    let wanted = |key: &str| {
        key == config_keys::NUM_PARTITIONS || key == config_keys::DEFAULT_REPLICATION_FACTOR
    };
    let default_synonym = |key: &str| DescribeConfigsSynonym {
        name: key.to_owned(),
        value: Some("1".to_owned()),
        source: CONFIG_SOURCE_DEFAULT,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let entry = |key: &str, named: Option<&str>| DescribeConfigsResourceResult {
        name: key.to_owned(),
        value: Some(named.unwrap_or("1").to_owned()),
        read_only: true,
        config_source: if named.is_some() {
            CONFIG_SOURCE_STATIC_BROKER
        } else {
            CONFIG_SOURCE_DEFAULT
        },
        is_sensitive: false,
        synonyms: named
            .map(|value| DescribeConfigsSynonym {
                name: key.to_owned(),
                value: Some(value.to_owned()),
                source: CONFIG_SOURCE_STATIC_BROKER,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            })
            .into_iter()
            .chain(std::iter::once(default_synonym(key)))
            .collect(),
        config_type: INT,
        documentation: Some(doc_for(key)),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };

    for (label, num_partitions, default_replication_factor, expected) in [
        (
            "neither named",
            None,
            None,
            vec![
                entry(config_keys::NUM_PARTITIONS, None),
                entry(config_keys::DEFAULT_REPLICATION_FACTOR, None),
            ],
        ),
        (
            "both named",
            Some(4),
            Some(3),
            vec![
                entry(config_keys::NUM_PARTITIONS, Some("4")),
                entry(config_keys::DEFAULT_REPLICATION_FACTOR, Some("3")),
            ],
        ),
    ] {
        let configs = StaticBrokerConfigs {
            num_partitions,
            default_replication_factor,
            ..kafka_default_static_broker()
        };

        let entries = static_broker_entries(configs, &wanted, BOTH);

        check!(entries == expected, "{label}");
    }
}

/// #743 `delete.topic.enable` and `auto.create.topics.enable`: each reports
/// Kafka's default of `true` at `DEFAULT_CONFIG` on a node that never named
/// it, and a named value at `STATIC_BROKER_CONFIG` with the default beneath
/// it.
#[test]
fn static_boolean_keys_report_their_provenance() {
    let synonym = |key: &str, value: &str, source| DescribeConfigsSynonym {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        source,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let entry = |key: &str, value: &str, source, synonyms| DescribeConfigsResourceResult {
        name: key.to_owned(),
        value: Some(value.to_owned()),
        read_only: true,
        config_source: source,
        is_sensitive: false,
        synonyms,
        config_type: registry::ConfigType::Boolean.wire(),
        documentation: Some(doc_for(key)),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let not_named = |key: &'static str| {
        vec![entry(
            key,
            "true",
            CONFIG_SOURCE_DEFAULT,
            vec![synonym(key, "true", CONFIG_SOURCE_DEFAULT)],
        )]
    };
    let named_false = |key: &'static str| {
        vec![entry(
            key,
            "false",
            CONFIG_SOURCE_STATIC_BROKER,
            vec![
                synonym(key, "false", CONFIG_SOURCE_STATIC_BROKER),
                synonym(key, "true", CONFIG_SOURCE_DEFAULT),
            ],
        )]
    };
    let delete = config_keys::DELETE_TOPIC_ENABLE;
    let auto_create = config_keys::AUTO_CREATE_TOPICS_ENABLE;
    let defaults = kafka_default_static_broker;
    let cases = [
        ("delete not named", delete, defaults(), not_named(delete)),
        (
            "delete named false",
            delete,
            StaticBrokerConfigs {
                delete_topic_enable: Some(false),
                ..defaults()
            },
            named_false(delete),
        ),
        (
            "auto-create not named",
            auto_create,
            defaults(),
            not_named(auto_create),
        ),
        (
            "auto-create named false",
            auto_create,
            StaticBrokerConfigs {
                auto_create_topics_enable: Some(false),
                ..defaults()
            },
            named_false(auto_create),
        ),
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, key, configs, entries) in cases {
        let wanted = |candidate: &str| candidate == key;
        actual.push((label, static_broker_entries(configs, &wanted, BOTH)));
        expected.push((label, entries));
    }
    assert!(actual == expected);
}
