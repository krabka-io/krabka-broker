//! The static broker configs `DescribeConfigs` reports beside a node's
//! dynamic overrides.
//!
//! Kafka answers `kafka-configs --entity-type brokers --entity-name <id>
//! --describe --all` from the node's own `server.properties`, so a key the
//! operator never set still comes back, at its built-in default. The two
//! KIP-98 transactional-id expiry keys behave exactly that way, and neither is
//! dynamically reconfigurable:
//!
//! ```text
//! transactional.id.expiration.ms=604800000 synonyms={DEFAULT_CONFIG:...}
//! transactional.id.expiration.ms=120000    synonyms={STATIC_BROKER_CONFIG:...,
//!                                                    DEFAULT_CONFIG:604800000}
//! ```
//!
//! (apache/kafka:4.3.1, first with the key unset and then with
//! `KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS=120000`. Altering it returns
//! `Cannot update these configs dynamically`, which is why both entries are
//! `read_only`.)
//!
//! The cluster-default broker resource carries only dynamic defaults in Kafka,
//! so these entries belong to a named broker resource alone.

use krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult;

use super::super::wire::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_STATIC_BROKER, make_entry};
use crate::config_keys;

/// Kafka's `transactional.id.expiration.ms` default, in milliseconds.
const KAFKA_DEFAULT_TRANSACTIONAL_ID_EXPIRATION_MS: i64 = 604_800_000;

/// Kafka's `transaction.remove.expired.transaction.cleanup.interval.ms`
/// default, in milliseconds.
const KAFKA_DEFAULT_TRANSACTION_REMOVE_EXPIRED_INTERVAL_MS: i64 = 3_600_000;

/// The effective values this broker runs with for the static broker configs
/// it reports. The handler fills it from [`crate::config::BrokerConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerConfigs {
    /// `transactional.id.expiration.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_ms: i64,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_cleanup_interval_ms: i64,
}

/// One static broker entry: read-only, reported at `STATIC_BROKER_CONFIG`
/// when the operator moved it off Kafka's built-in default and at
/// `DEFAULT_CONFIG` when they did not.
fn static_entry(key: &str, value: i64, kafka_default: i64) -> DescribeConfigsResourceResult {
    let source = if value == kafka_default {
        CONFIG_SOURCE_DEFAULT
    } else {
        CONFIG_SOURCE_STATIC_BROKER
    };
    let mut entry = make_entry(key, &value.to_string(), source);
    // Neither key is dynamically reconfigurable, in Kafka or here.
    entry.read_only = true;
    entry
}

/// Every static broker entry for one node, in key order.
pub(in crate::handlers::describe_configs) fn static_broker_entries(
    configs: StaticBrokerConfigs,
) -> Vec<DescribeConfigsResourceResult> {
    vec![
        static_entry(
            config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            configs.txn_id_expiration_ms,
            KAFKA_DEFAULT_TRANSACTIONAL_ID_EXPIRATION_MS,
        ),
        static_entry(
            config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            configs.txn_id_expiration_cleanup_interval_ms,
            KAFKA_DEFAULT_TRANSACTION_REMOVE_EXPIRED_INTERVAL_MS,
        ),
    ]
}

/// The values a broker that never touched either key runs with. The
/// `describe_one` tests pass it so their expectations read as Kafka's
/// out-of-the-box `--describe --all` output.
#[cfg(test)]
pub(in crate::handlers::describe_configs) fn kafka_default_static_broker() -> StaticBrokerConfigs {
    StaticBrokerConfigs {
        txn_id_expiration_ms: KAFKA_DEFAULT_TRANSACTIONAL_ID_EXPIRATION_MS,
        txn_id_expiration_cleanup_interval_ms: KAFKA_DEFAULT_TRANSACTION_REMOVE_EXPIRED_INTERVAL_MS,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;

    #[test]
    fn kafka_defaults_report_at_default_config_source() {
        let entries = static_broker_entries(StaticBrokerConfigs {
            txn_id_expiration_ms: KAFKA_DEFAULT_TRANSACTIONAL_ID_EXPIRATION_MS,
            txn_id_expiration_cleanup_interval_ms:
                KAFKA_DEFAULT_TRANSACTION_REMOVE_EXPIRED_INTERVAL_MS,
        });

        assert!(
            entries
                == vec![
                    DescribeConfigsResourceResult {
                        name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_string(),
                        value: Some("604800000".to_string()),
                        read_only: true,
                        config_source: CONFIG_SOURCE_DEFAULT,
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: 0,
                        documentation: None,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                    DescribeConfigsResourceResult {
                        name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                            .to_string(),
                        value: Some("3600000".to_string()),
                        read_only: true,
                        config_source: CONFIG_SOURCE_DEFAULT,
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: 0,
                        documentation: None,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                ]
        );
    }

    #[test]
    fn an_operator_override_reports_at_static_broker_config_source() {
        let entries = static_broker_entries(StaticBrokerConfigs {
            txn_id_expiration_ms: 120_000,
            txn_id_expiration_cleanup_interval_ms: 60_000,
        });

        assert!(
            entries
                == vec![
                    DescribeConfigsResourceResult {
                        name: config_keys::TRANSACTIONAL_ID_EXPIRATION_MS.to_string(),
                        value: Some("120000".to_string()),
                        read_only: true,
                        config_source: CONFIG_SOURCE_STATIC_BROKER,
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: 0,
                        documentation: None,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                    DescribeConfigsResourceResult {
                        name: config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                            .to_string(),
                        value: Some("60000".to_string()),
                        read_only: true,
                        config_source: CONFIG_SOURCE_STATIC_BROKER,
                        is_sensitive: false,
                        synonyms: Vec::new(),
                        config_type: 0,
                        documentation: None,
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                ]
        );
    }
}
