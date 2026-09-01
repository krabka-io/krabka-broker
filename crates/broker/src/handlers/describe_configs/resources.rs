//! Resolving one `DescribeConfigs` resource entry into the configs the broker
//! reports for it.
//!
//! This is the whole source-precedence decision, one resource type at a time:
//! a topic's effective configuration, a broker's per-node override over the
//! cluster default plus the static `node.id`, a client-metrics subscription's
//! effective values, and a group's dynamic overrides over the streams
//! defaults. Every other resource type gets an empty configs list and no
//! error.
//!
//! A topic reports every key in the registry, not only the ones it overrides.
//! `kafka-configs --describe --all` and `AdminClient.describeConfigs` exist to
//! show effective configuration and where each value came from, and a response
//! that holds only the overrides answers neither question. The layer each
//! value came from travels with it, as the synonym chain
//! [`super::entry::config_entry`] builds.

use krabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};

use super::{
    entry::{DefaultLayer, EntryOptions, Layer, config_entry},
    wire::{
        CONFIG_SOURCE_CLIENT_METRICS, CONFIG_SOURCE_DYNAMIC_BROKER,
        CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_GROUP,
        CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER, RESOURCE_TYPE_BROKER,
        RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    },
};
use crate::{
    codes,
    config_keys::{
        self,
        registry::{self, ConfigScope, NODE_ID},
    },
};

mod write_freeze;

#[cfg(test)]
mod tests;

use self::write_freeze::write_freeze_override;

/// Dispatches one resource entry from a `DescribeConfigs` request.
pub(super) fn describe_one(
    image: &krabka_metadata::MetadataImage,
    r: krabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
    client_metrics_default_interval_ms: i32,
    streams_defaults: &crate::coordinator::unified::streams::config::StreamsGroupConfig,
    options: EntryOptions,
) -> DescribeConfigsResult {
    let ok = |configs| DescribeConfigsResult {
        error_code: codes::NONE,
        error_message: None,
        resource_type: r.resource_type,
        resource_name: r.resource_name.clone(),
        configs,
        ..Default::default()
    };
    // An empty key list asks for every key, not for none: Kafka's
    // `ConfigHelperUtils.toDescribeConfigsResult` filters with
    // `keys == null || keys.isEmpty() || keys.contains(name)`.
    let key_filter: Option<&[String]> = r
        .configuration_keys
        .as_deref()
        .filter(|keys| !keys.is_empty());
    let wanted = |key: &str| key_filter.is_none_or(|keys| keys.iter().any(|f| f == key));

    if r.resource_type == RESOURCE_TYPE_TOPIC {
        return ok(topic_configs(image, &r.resource_name, &wanted, options));
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
        return ok(broker_configs(image, node_id, &wanted, options));
    }

    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        return ok(client_metrics_configs(
            image,
            &r.resource_name,
            client_metrics_default_interval_ms,
            &wanted,
            options,
        ));
    }

    if r.resource_type == RESOURCE_TYPE_GROUP {
        return ok(group_configs(
            image,
            &r.resource_name,
            streams_defaults,
            &wanted,
            options,
        ));
    }

    // All other resource types: empty configs, no error.
    ok(Vec::new())
}

/// A topic's effective configuration: every registry key, with the layer its
/// value came from.
///
/// `write.freeze` is the one key that is never stored. It lives in the freeze
/// registry, and [`write_freeze_override`] reads it from there.
fn topic_configs(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let overrides = image.topic_config(topic);
    let cluster_defaults = image.default_broker_config();
    let freeze = write_freeze_override(image, topic);

    let mut configs: Vec<DescribeConfigsResourceResult> = registry::keys_in(ConfigScope::Topic)
        .filter(|row| wanted(row.name))
        .map(|row| {
            let stored = if row.name == config_keys::WRITE_FREEZE {
                freeze.as_deref()
            } else {
                overrides
                    .and_then(|configs| configs.get(row.name))
                    .map(String::as_str)
            };
            let mut layers = Vec::with_capacity(2);
            if let Some(value) = stored {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                    name: row.name,
                    value,
                });
            }
            if let Some(cluster_default) = row.cluster_default
                && let Some(value) = cluster_defaults
                    .and_then(|configs| configs.get(cluster_default))
                    .map(String::as_str)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    name: cluster_default,
                    value,
                });
            }
            config_entry(
                Some(row),
                row.name,
                &layers,
                DefaultLayer {
                    value: row.default,
                    name: row.cluster_default,
                },
                options,
            )
        })
        .collect();
    configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    configs
}

/// A broker resource: the dynamic overrides it holds, plus the static
/// `node.id` when the request named one node.
///
/// An empty resource name is Kafka's cluster-wide default resource, which
/// reports the cluster defaults alone. Only keys that hold a value are
/// reported, which is what a Kafka broker does for this resource type.
fn broker_configs(
    image: &krabka_metadata::MetadataImage,
    node_id: Option<krabka_metadata::NodeId>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let defaults = image.default_broker_config();
    let per_broker = node_id.and_then(|node_id| image.broker_config(node_id));
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
        .filter(|key| wanted(key))
        .map(|key| {
            let mut layers = Vec::with_capacity(2);
            if let Some(value) = per_broker
                .and_then(|configs| configs.get(key))
                .map(String::as_str)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_BROKER,
                    name: key,
                    value,
                });
            }
            if let Some(value) = defaults
                .and_then(|configs| configs.get(key))
                .map(String::as_str)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    name: key,
                    value,
                });
            }
            let row = registry::lookup(ConfigScope::Broker, key);
            let default = row
                .and_then(|row| row.default)
                .map(|value| DefaultLayer {
                    value: Some(value),
                    name: Some(key.as_str()),
                })
                .unwrap_or_default();
            let mut entry = config_entry(row, key, &layers, default, options);
            entry.read_only |= config_keys::is_controller_managed_broker_config(key);
            entry
        })
        .collect();

    if let Some(node_id) = node_id
        && wanted(NODE_ID)
    {
        let value = node_id.to_string();
        configs.push(config_entry(
            registry::lookup(ConfigScope::Broker, NODE_ID),
            NODE_ID,
            &[Layer {
                source: CONFIG_SOURCE_STATIC_BROKER,
                name: NODE_ID,
                value: &value,
            }],
            DefaultLayer::default(),
            options,
        ));
        configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    }
    configs
}

/// A KIP-714 client-metrics subscription: all three keys, with the ones the
/// subscription does not set reported at their default (KAFKA-17516 — tooling
/// needs effective values, not blanks).
fn client_metrics_configs(
    image: &krabka_metadata::MetadataImage,
    subscription: &str,
    default_interval_ms: i32,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    use crate::client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS};

    let overrides = image
        .client_metrics_config(subscription)
        .cloned()
        .unwrap_or_default();
    let default_interval = default_interval_ms.to_string();

    [KEY_METRICS, KEY_INTERVAL_MS, KEY_MATCH]
        .into_iter()
        .filter(|key| wanted(key))
        .map(|key| {
            let row = registry::lookup(ConfigScope::ClientMetrics, key);
            let default = if key == KEY_INTERVAL_MS {
                Some(default_interval.as_str())
            } else {
                row.and_then(|row| row.default)
            };
            let layers: Vec<Layer<'_>> = overrides
                .get(key)
                .map(|value| Layer {
                    source: CONFIG_SOURCE_CLIENT_METRICS,
                    name: key,
                    value,
                })
                .into_iter()
                .collect();
            config_entry(
                row,
                key,
                &layers,
                DefaultLayer {
                    value: default,
                    name: None,
                },
                options,
            )
        })
        .collect()
}

/// A KIP-1071 group resource: the streams defaults this broker runs with, and
/// the per-group overrides that sit above them.
fn group_configs(
    image: &krabka_metadata::MetadataImage,
    group: &str,
    streams_defaults: &crate::coordinator::unified::streams::config::StreamsGroupConfig,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let overrides = image.group_config(group).cloned().unwrap_or_default();
    let defaults = streams_defaults.group_config_values();

    defaults
        .iter()
        .filter(|(key, _)| wanted(key))
        .map(|(key, default)| {
            let layers: Vec<Layer<'_>> = overrides
                .get(key)
                .map(|value| Layer {
                    source: CONFIG_SOURCE_DYNAMIC_GROUP,
                    name: key,
                    value,
                })
                .into_iter()
                .collect();
            config_entry(
                registry::lookup(ConfigScope::Group, key),
                key,
                &layers,
                DefaultLayer {
                    value: Some(default),
                    name: Some(key),
                },
                options,
            )
        })
        .collect()
}
