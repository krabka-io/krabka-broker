//! Resolving one `DescribeConfigs` resource entry into the configs the broker
//! reports for it.
//!
//! This is the whole source-precedence decision, one resource type at a time:
//! a topic's dynamic overrides, a broker's per-node override over the cluster
//! default plus the static `node.id`, a client-metrics subscription's
//! effective values, and a group's dynamic overrides over the streams
//! defaults. Every other resource type gets an empty configs list and no
//! error.

use krabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use krabka_units::convert::TimeExt as _;

use super::wire::{
    CONFIG_SOURCE_CLIENT_METRICS, CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_DYNAMIC_BROKER,
    CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_GROUP, CONFIG_SOURCE_DYNAMIC_TOPIC,
    CONFIG_SOURCE_STATIC_BROKER, RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS,
    RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC, make_entry,
};
use crate::{codes, config_keys};

mod write_freeze;

#[cfg(test)]
mod tests;

use self::write_freeze::write_freeze_entry;

/// The broker-scoped values that live in the process's static configuration
/// rather than in the metadata image. `DescribeConfigs` synthesises one entry
/// for each against `STATIC_BROKER_CONFIG`, the way it synthesises `node.id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StaticBrokerConfigs {
    /// `offsets.retention.minutes`, in whole minutes.
    pub(super) offsets_retention_minutes: i64,
    /// `offsets.retention.check.interval.ms`, in whole milliseconds.
    pub(super) offsets_retention_check_interval_ms: i64,
}

impl StaticBrokerConfigs {
    /// The `(key, value, source)` triples, in the order `DescribeConfigs`
    /// emits them before the final sort.
    fn entries(self) -> [(&'static str, String, i8); 2] {
        [
            (
                config_keys::OFFSETS_RETENTION_MINUTES,
                self.offsets_retention_minutes.to_string(),
                source_of(
                    self.offsets_retention_minutes,
                    crate::config::DEFAULT_OFFSETS_RETENTION.millis_i64() / 60_000,
                ),
            ),
            (
                config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
                self.offsets_retention_check_interval_ms.to_string(),
                source_of(
                    self.offsets_retention_check_interval_ms,
                    crate::config::DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL.millis_i64(),
                ),
            ),
        ]
    }
}

/// The source Kafka reports for a static, non-reconfigurable broker config: an
/// untouched knob reads `DEFAULT_CONFIG` and one the operator set reads
/// `STATIC_BROKER_CONFIG`. Verified against `apache/kafka:4.3.1`, where
/// `kafka-configs --describe --all` reports both retention keys as
/// `DEFAULT_CONFIG` on a broker whose properties do not mention them.
fn source_of(value: i64, default: i64) -> i8 {
    if value == default {
        CONFIG_SOURCE_DEFAULT
    } else {
        CONFIG_SOURCE_STATIC_BROKER
    }
}

/// Dispatches one resource entry from a `DescribeConfigs` request.
pub(super) fn describe_one(
    image: &krabka_metadata::MetadataImage,
    r: krabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
    client_metrics_default_interval_ms: i32,
    streams_defaults: &crate::coordinator::unified::streams::config::StreamsGroupConfig,
    static_broker: StaticBrokerConfigs,
) -> DescribeConfigsResult {
    let ok = |configs| DescribeConfigsResult {
        error_code: codes::NONE,
        error_message: None,
        resource_type: r.resource_type,
        resource_name: r.resource_name.clone(),
        configs,
        ..Default::default()
    };

    if r.resource_type == RESOURCE_TYPE_TOPIC {
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let wanted = |key: &str| key_filter.is_none_or(|keys| keys.iter().any(|f| f == key));
        let mut configs: Vec<DescribeConfigsResourceResult> = image
            .topic_config(&r.resource_name)
            .into_iter()
            .flatten()
            .filter(|(key, _)| wanted(key.as_str()))
            .map(|(key, value)| {
                let mut entry = make_entry(key, value, CONFIG_SOURCE_DYNAMIC_TOPIC);
                // The data path is fixed when the topic is created, so
                // `kafka-configs` must show the key the way it shows every
                // other config no alter can change.
                entry.read_only = key == config_keys::DISKLESS;
                entry
            })
            .collect();
        if wanted(config_keys::WRITE_FREEZE) {
            configs.push(write_freeze_entry(image, &r.resource_name));
            // The stored overrides arrive in `BTreeMap` order, so one sort
            // puts the synthesised key back into that order.
            configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        }
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_BROKER {
        let node_id = if r.resource_name.is_empty() {
            None
        } else {
            let Ok(node_id) = r.resource_name.parse::<u64>() else {
                return DescribeConfigsResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(format!(
                        "resource_name `{}` is not a valid broker id",
                        r.resource_name
                    )),
                    resource_type: r.resource_type,
                    resource_name: r.resource_name,
                    configs: Vec::new(),
                    ..Default::default()
                };
            };
            Some(krabka_metadata::NodeId(node_id))
        };
        let defaults = image.default_broker_config();
        let per_broker = node_id.and_then(|node_id| image.broker_config(node_id));
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut keys = std::collections::BTreeSet::new();
        keys.extend(
            defaults
                .into_iter()
                .flat_map(std::collections::BTreeMap::keys),
        );
        keys.extend(
            per_broker
                .into_iter()
                .flat_map(std::collections::BTreeMap::keys),
        );
        let mut configs: Vec<DescribeConfigsResourceResult> = keys
            .into_iter()
            .filter(|key| key_filter.is_none_or(|ks| ks.iter().any(|filter| filter == *key)))
            .map(|key| {
                let mut entry = per_broker.and_then(|configs| configs.get(key)).map_or_else(
                    || {
                        make_entry(
                            key,
                            defaults
                                .and_then(|configs| configs.get(key))
                                .expect("key came from broker defaults"),
                            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                        )
                    },
                    |value| make_entry(key, value, CONFIG_SOURCE_DYNAMIC_BROKER),
                );
                // Only the controller writes these keys, so `kafka-configs`
                // must show them the way it shows every other read-only
                // broker config.
                entry.read_only = config_keys::is_controller_managed_broker_config(key);
                entry
            })
            .collect();
        if let Some(node_id) = node_id {
            if key_filter.is_none_or(|keys| keys.iter().any(|key| key == "node.id")) {
                let mut entry =
                    make_entry("node.id", &node_id.to_string(), CONFIG_SOURCE_STATIC_BROKER);
                entry.read_only = true;
                configs.push(entry);
            }
            // The process reads both retention knobs once at startup, so an
            // operator must see them the way it sees every other config no
            // alter can change.
            for (key, value, source) in static_broker.entries() {
                if key_filter.is_none_or(|keys| keys.iter().any(|filter| filter == key)) {
                    // Kafka refuses to reconfigure either key dynamically —
                    // `kafka-configs --alter` answers "Cannot update these
                    // configs dynamically" — so both report read-only whatever
                    // their source.
                    let mut entry = make_entry(key, &value, source);
                    entry.read_only = true;
                    configs.push(entry);
                }
            }
            configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        }
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        use crate::client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS};
        let overrides = image
            .client_metrics_config(&r.resource_name)
            .cloned()
            .unwrap_or_default();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let mut configs = Vec::new();
        // Emit all three keys: set values use CLIENT_METRICS_CONFIG source;
        // unset keys report their default value/source (KAFKA-17516 — tooling
        // needs effective values, not blanks).
        let default_interval = client_metrics_default_interval_ms.to_string();
        let mut emit = |key: &str, default: &str| {
            if key_filter.is_some_and(|ks| !ks.iter().any(|f| f == key)) {
                return;
            }
            match overrides.get(key) {
                Some(v) => configs.push(make_entry(key, v, CONFIG_SOURCE_CLIENT_METRICS)),
                None => configs.push(make_entry(key, default, CONFIG_SOURCE_DEFAULT)),
            }
        };
        emit(KEY_METRICS, "");
        emit(KEY_INTERVAL_MS, &default_interval);
        emit(KEY_MATCH, "");
        return ok(configs);
    }

    if r.resource_type == RESOURCE_TYPE_GROUP {
        let overrides = image
            .group_config(&r.resource_name)
            .cloned()
            .unwrap_or_default();
        let effective = streams_defaults
            .with_group_overrides(&overrides)
            .unwrap_or_else(|_| streams_defaults.clone())
            .group_config_values();
        let key_filter: Option<&[String]> = r.configuration_keys.as_deref();
        let configs = effective
            .iter()
            .filter(|(key, _)| key_filter.is_none_or(|keys| keys.iter().any(|k| k == *key)))
            .map(|(key, value)| {
                let source = if overrides.contains_key(key) {
                    CONFIG_SOURCE_DYNAMIC_GROUP
                } else {
                    CONFIG_SOURCE_DEFAULT
                };
                make_entry(key, value, source)
            })
            .collect();
        return ok(configs);
    }

    // All other resource types: empty configs, no error.
    ok(Vec::new())
}
