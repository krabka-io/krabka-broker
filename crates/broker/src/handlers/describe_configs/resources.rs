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

/// Dispatches one resource entry from a `DescribeConfigs` request.
pub(super) fn describe_one(
    image: &krabka_metadata::MetadataImage,
    r: krabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
    client_metrics_default_interval_ms: i32,
    streams_defaults: &crate::coordinator::unified::streams::config::StreamsGroupConfig,
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
                // A create-only key cannot be altered, so `kafka-configs` must
                // show it the way it shows every other read-only config.
                entry.read_only = config_keys::is_create_only_topic_config(key);
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
        if let Some(node_id) = node_id
            && key_filter.is_none_or(|keys| keys.iter().any(|key| key == "node.id"))
        {
            let mut entry =
                make_entry("node.id", &node_id.to_string(), CONFIG_SOURCE_STATIC_BROKER);
            entry.read_only = true;
            configs.push(entry);
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
