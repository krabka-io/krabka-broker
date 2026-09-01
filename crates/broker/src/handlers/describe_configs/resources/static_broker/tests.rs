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

/// Every key wanted, which is what a request with no `configuration_keys`
/// asks for.
fn all_keys(_: &str) -> bool {
    true
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
fn supplied(expiration_ms: i32, cleanup_interval_ms: i32) -> StaticBrokerConfigs {
    StaticBrokerConfigs {
        txn_id_expiration: StaticBrokerSetting {
            value_ms: expiration_ms,
            supplied: true,
        },
        txn_id_expiration_cleanup_interval: StaticBrokerSetting {
            value_ms: cleanup_interval_ms,
            supplied: true,
        },
    }
}

/// A node still on Kafka's built-in defaults reports both keys at
/// `DEFAULT_CONFIG`, with the default as their one synonym.
#[test]
fn kafka_defaults_report_at_default_config_source() {
    let entries = static_broker_entries(kafka_default_static_broker(), &all_keys, BOTH);

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
    let entries = static_broker_entries(supplied(120_000, 60_000), &all_keys, BOTH);

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
    let entries = static_broker_entries(kafka_default_static_broker(), &all_keys, NEITHER);

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
    };

    let entries = static_broker_entries(configs, &all_keys, BOTH);

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
