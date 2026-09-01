//! The static broker configs `DescribeConfigs` reports beside a node's
//! dynamic overrides: the two KIP-98 transactional-id expiry keys and the two
//! KIP-211 offset-retention keys.
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
//! Provenance, not a value comparison, is what puts it there. The same image
//! started with `KAFKA_TRANSACTIONAL_ID_EXPIRATION_MS=604800000` -- the
//! built-in default, written out -- still answers
//!
//! ```text
//! transactional.id.expiration.ms=604800000 sensitive=false
//!   synonyms={STATIC_BROKER_CONFIG:transactional.id.expiration.ms=604800000,
//!             DEFAULT_CONFIG:transactional.id.expiration.ms=604800000}
//! ```
//!
//! while the key it was not given keeps the one-synonym default chain above.
//! Kafka reads that from `KafkaConfig.originals`, the properties the operator
//! wrote; krabka records it as [`crate::config::StaticConfigOrigins`] while it
//! loads the configuration.
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
//!
//! The KIP-211 pair -- `offsets.retention.minutes` and
//! `offsets.retention.check.interval.ms` -- reaches this module already
//! reduced to what the operator named, as an [`Option`]: `None` is a key the
//! process never saw and so reports its registry default alone, and `Some` is
//! a value that arrived through the file, CLI, or environment overlay, which
//! Kafka reports at `STATIC_BROKER_CONFIG` whatever the value is. Verified
//! against the same image, where a broker whose properties carry
//! `offsets.retention.minutes=10080` -- Kafka's own default -- answers
//! `synonyms={STATIC_BROKER_CONFIG:...=10080, DEFAULT_CONFIG:...=10080}`.

use krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult;
use krabka_units::{Time, convert::TimeExt as _};

use super::super::{
    entry::{DefaultLayer, EntryOptions, Layer, config_entry},
    wire::CONFIG_SOURCE_STATIC_BROKER,
};
use crate::config_keys::{
    self,
    registry::{self, ConfigScope},
};

/// One static broker setting as this node holds it: the value it runs with,
/// and whether its own configuration named the key.
///
/// The key is `ConfigDef.Type::INT` in Kafka, so the value is milliseconds in
/// an `i32`: the broker's own config validation refuses a wider value, and a
/// typed client could not parse one back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerSetting {
    /// The effective value, in milliseconds.
    pub(in crate::handlers::describe_configs) value_ms: i32,
    /// Whether the operator supplied the key. See
    /// [`crate::config::StaticConfigOrigins`]: a config source is provenance,
    /// so a supplied setting heads the chain even when it equals the built-in
    /// default.
    pub(in crate::handlers::describe_configs) supplied: bool,
}

/// The static broker configs this node reports. The handler fills it from
/// [`crate::config::BrokerConfig`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::handlers::describe_configs) struct StaticBrokerConfigs {
    /// `transactional.id.expiration.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration: StaticBrokerSetting,
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`.
    pub(in crate::handlers::describe_configs) txn_id_expiration_cleanup_interval:
        StaticBrokerSetting,
    /// `offsets.retention.minutes`, as the operator named it. The
    /// configuration refuses a retention that is not a whole number of
    /// minutes, so the reported value is exact.
    pub(in crate::handlers::describe_configs) offsets_retention: Option<Time>,
    /// `offsets.retention.check.interval.ms`, as the operator named it.
    pub(in crate::handlers::describe_configs) offsets_retention_check_interval: Option<Time>,
}

/// One static broker entry.
///
/// The value the node runs with sits at `STATIC_BROKER_CONFIG`, with the
/// registry default underneath it as the `DEFAULT_CONFIG` synonym, whenever
/// the value did not reach the node through that default alone. That is the
/// case in either of two ways: the operator named the key -- provenance,
/// which is what Kafka reads out of `KafkaConfig.originals` and reports even
/// for a value identical to the default -- or the node runs something the
/// built-in default does not supply, which is how krabka's own test profile
/// moves the sweep cadence. A node inheriting the default reports it alone,
/// which is the first of the two Kafka outputs quoted above.
fn static_entry(
    key: &str,
    setting: StaticBrokerSetting,
    options: EntryOptions,
) -> DescribeConfigsResourceResult {
    let row = registry::lookup(ConfigScope::Broker, key);
    let default = row.and_then(|row| row.default);
    let rendered = setting.value_ms.to_string();
    let layers: Vec<Layer<'_>> = (setting.supplied || default != Some(rendered.as_str()))
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

/// One static broker entry the caller has already reduced to what the
/// operator named.
///
/// `Some` heads the chain at `STATIC_BROKER_CONFIG` with the registry default
/// beneath it; `None` reports that default alone. This is the same rule
/// [`static_entry`] applies, stated over a provenance the caller resolved
/// rather than over a value the node runs with.
fn named_entry(
    key: &'static str,
    set_by_operator: Option<String>,
    options: EntryOptions,
) -> DescribeConfigsResourceResult {
    let row = registry::lookup(ConfigScope::Broker, key);
    let layers: Vec<Layer<'_>> = set_by_operator
        .iter()
        .map(|value| Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: key,
            value: value.as_str(),
        })
        .collect();
    config_entry(
        row,
        key,
        &layers,
        DefaultLayer {
            value: row.and_then(|row| row.default),
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
    let mut entries: Vec<DescribeConfigsResourceResult> = [
        (
            config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
            configs.txn_id_expiration,
        ),
        (
            config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            configs.txn_id_expiration_cleanup_interval,
        ),
    ]
    .into_iter()
    .filter(|(key, _)| wanted(key))
    .map(|(key, setting)| static_entry(key, setting, options))
    .collect();
    entries.extend(
        [
            (
                config_keys::OFFSETS_RETENTION_MINUTES,
                configs
                    .offsets_retention
                    .map(|retention| (retention.millis_i64() / 60_000).to_string()),
            ),
            (
                config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                configs
                    .offsets_retention_check_interval
                    .map(|interval| interval.millis_i64().to_string()),
            ),
        ]
        .into_iter()
        .filter(|(key, _)| wanted(key))
        .map(|(key, set_by_operator)| named_entry(key, set_by_operator, options)),
    );
    entries
}

/// The values a broker that never touched either key runs with. The
/// `describe_one` tests pass it so their expectations read as Kafka's
/// out-of-the-box `--describe --all` output.
#[cfg(test)]
pub(in crate::handlers::describe_configs) fn kafka_default_static_broker() -> StaticBrokerConfigs {
    StaticBrokerConfigs {
        txn_id_expiration: StaticBrokerSetting {
            value_ms: 604_800_000,
            supplied: false,
        },
        txn_id_expiration_cleanup_interval: StaticBrokerSetting {
            value_ms: 3_600_000,
            supplied: false,
        },
        offsets_retention: None,
        offsets_retention_check_interval: None,
    }
}

#[cfg(test)]
mod tests;
