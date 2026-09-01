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
//! transaction.remove.expired.transaction.cleanup.interval.ms=3600000 sensitive=false
//!   synonyms={DEFAULT_CONFIG:transaction.remove.expired.transaction.cleanup.interval.ms=3600000}
//! transactional.id.expiration.ms=604800000 sensitive=false
//!   synonyms={DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! That is `apache/kafka:4.3.1` with neither key set. Setting one moves it to
//! the head of the chain and keeps the built-in default beneath it:
//!
//! ```text
//! transactional.id.expiration.ms=120000 sensitive=false
//!   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=120000,
//!             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! Altering either one on the same broker answers
//! `Cannot update these configs dynamically: [transactional.id.expiration.ms]`,
//! which is why the registry marks both rows read-only. The cluster-default
//! broker resource reports neither key at all -- the same probe against
//! `--entity-default` returns no row for them -- so these entries belong to a
//! named broker resource alone.
//!
//! Everything else about an entry -- the type byte, the documentation, the
//! read-only flag -- comes from [`crate::config_keys::registry`], the one
//! table every config surface reads. Both keys are `ConfigDef.Type::INT` with
//! an `atLeast(1)` validator there, read out of the image's
//! `kafka-transaction-coordinator-4.3.1.jar`.

use krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult;

use super::super::{
    entry::{DefaultLayer, EntryOptions, Layer, config_entry},
    wire::CONFIG_SOURCE_STATIC_BROKER,
};
use crate::config_keys::{
    self,
    registry::{self, ConfigScope},
};

/// The effective values this node runs with for the static broker configs it
/// reports. The handler fills it from [`crate::config::BrokerConfig`].
///
/// Both keys are `ConfigDef.Type::INT` in Kafka, so both are milliseconds in
/// an `i32`: the broker's own config validation refuses a wider value, and a
/// typed client could not parse one back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerConfigs {
    /// `transactional.id.expiration.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_ms: i32,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_cleanup_interval_ms: i32,
}

/// One static broker entry.
///
/// The value the node runs with sits at `STATIC_BROKER_CONFIG` when the
/// operator moved it off Kafka's built-in default, with the registry default
/// underneath it as the `DEFAULT_CONFIG` synonym. A node still on the default
/// reports that default alone, which is the first of the two Kafka outputs
/// quoted above.
fn static_entry(key: &str, value: i32, options: EntryOptions) -> DescribeConfigsResourceResult {
    let row = registry::lookup(ConfigScope::Broker, key);
    let default = row.and_then(|row| row.default);
    let rendered = value.to_string();
    let layers: Vec<Layer<'_>> = (default != Some(rendered.as_str()))
        .then_some(Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: key,
            value: &rendered,
        })
        .into_iter()
        .collect();
    config_entry(
        row,
        key,
        &layers,
        DefaultLayer {
            value: default,
            name: Some(key),
        },
        options,
    )
}

/// Every static broker entry for one node, filtered by the request's
/// `configuration_keys`.
pub(super) fn static_broker_entries(
    configs: StaticBrokerConfigs,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    [
        (
            config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            configs.txn_id_expiration_ms,
        ),
        (
            config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            configs.txn_id_expiration_cleanup_interval_ms,
        ),
    ]
    .into_iter()
    .filter(|(key, _)| wanted(key))
    .map(|(key, value)| static_entry(key, value, options))
    .collect()
}

/// The values a broker that never touched either key runs with. The
/// `describe_one` tests pass it so their expectations read as Kafka's
/// out-of-the-box `--describe --all` output.
#[cfg(test)]
pub(in crate::handlers::describe_configs) fn kafka_default_static_broker() -> StaticBrokerConfigs {
    StaticBrokerConfigs {
        txn_id_expiration_ms: 604_800_000,
        txn_id_expiration_cleanup_interval_ms: 3_600_000,
    }
}

#[cfg(test)]
mod tests;
