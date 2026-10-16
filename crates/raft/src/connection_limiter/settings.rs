//! The connection quotas of Kafka's `ConnectionQuotas`, read from the dynamic
//! broker configs of the metadata image.
//!
//! Kafka layers a node's own dynamic config over the cluster default, and a
//! listener's `listener.name.<listener>.` value over the broker-wide one
//! (`KafkaConfig.valuesWithPrefixOverride`). A value Kafka would not apply
//! leaves the value in force before it: `DynamicBrokerConfig` keeps its
//! previous config when the new one fails `KafkaConfig` validation or a
//! reconfigurable's `validateReconfiguration`. Kafka drops the whole update;
//! krabka keeps the previous value of the refused key alone. With no previous
//! value, at startup, the refused value is skipped for the next layer, as
//! `DynamicBrokerConfig.initialize` drops an invalid value from its layer.

use {crate::types::NodeId, krabka_metadata::MetadataImage, std::collections::BTreeMap};

/// Kafka's `SocketServerConfigs.MAX_CONNECTIONS_CONFIG`.
pub const MAX_CONNECTIONS: &str = "max.connections";

/// Kafka's `SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG`.
pub const MAX_CONNECTIONS_PER_IP: &str = "max.connections.per.ip";

/// Kafka's `SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG`.
pub const MAX_CONNECTIONS_PER_IP_OVERRIDES: &str = "max.connections.per.ip.overrides";

/// Kafka's `SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG`.
pub const MAX_CONNECTION_CREATION_RATE: &str = "max.connection.creation.rate";

/// The two caps a node is started with. `usize::MAX` means unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLimits {
    /// `max.connections`: live connections across the node's listeners.
    pub max_connections: usize,
    /// `max.connections.per.ip`: live connections from one client IP.
    pub max_connections_per_ip: usize,
}

/// The quotas in force for one listener, before the hosts of
/// `max.connections.per.ip.overrides` are resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionQuotaSettings {
    /// `max.connections`: the broker-wide cap.
    pub max_connections: usize,
    /// `max.connections.per.ip`: the cap for an IP with no override.
    pub max_connections_per_ip: usize,
    /// `max.connections.per.ip.overrides`: the cap of each host, before the
    /// host is resolved to an address.
    pub per_ip_overrides: BTreeMap<String, usize>,
    /// `listener.name.<listener>.max.connections`, else `max.connections`.
    pub listener_max_connections: usize,
    /// `max.connection.creation.rate` per second; `None` is Kafka's
    /// `Int.MaxValue` default, no limit.
    pub max_connection_creation_rate: Option<u32>,
    /// `listener.name.<listener>.max.connection.creation.rate`, else the
    /// broker-wide rate.
    pub listener_max_connection_creation_rate: Option<u32>,
}

/// Kafka's `ListenerName.configPrefix` applied to `key`: the listener name is
/// lowercased.
fn listener_key(listener: &str, key: &str) -> String {
    format!("listener.name.{}.{key}", listener.to_ascii_lowercase())
}

/// `String.trim`, which `ConfigDef` applies to every value.
fn java_trim(value: &str) -> &str { value.trim_matches(|c: char| c <= ' ') }

/// An `INT` as `ConfigDef` parses it.
fn parse_int(value: &str) -> Option<i32> { java_trim(value).parse().ok() }

/// A connection cap of at least `min`.
fn parse_cap(value: &str, min: i32) -> Option<usize> {
    parse_int(value)
        .filter(|cap| *cap >= min)
        .and_then(|cap| usize::try_from(cap).ok())
}

/// A connection creation rate: an `INT` of at least 1, as
/// `ListenerConnectionQuota.validateReconfiguration` requires of every
/// listener, which inherits the broker-wide value.
fn parse_rate(value: &str) -> Option<u32> {
    parse_int(value)
        .filter(|rate| *rate >= 1)
        .map(i32::unsigned_abs)
}

/// A rate as a limit: Kafka's `Int.MaxValue` default is no limit.
fn limited(rate: u32) -> Option<u32> { (rate != i32::MAX.unsigned_abs()).then_some(rate) }

/// `Utils.validHostPattern`: `[0-9a-zA-Z\-%._:]*`.
fn valid_host(host: &str) -> bool {
    host.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '%' | '.' | '_' | ':'))
}

/// `max.connections.per.ip.overrides` as Kafka reads it: `Csv.parseCsvMap`
/// splits on commas and each entry on its last colon, `toInt` reads the
/// count, and `KafkaConfig.validateValues` refuses a host outside
/// `validHostPattern`. `None` is a value Kafka refuses. A negative count
/// refuses every connection from the host, as `count >= max` always holds.
pub(super) fn parse_overrides(value: &str) -> Option<BTreeMap<String, usize>> {
    let value = java_trim(value);
    if value.is_empty() {
        return Some(BTreeMap::new());
    }
    // Java's `String.split` drops trailing empty entries and keeps the rest.
    let mut entries: Vec<&str> = value.split(',').map(java_trim).collect();
    while entries.last().is_some_and(|entry| entry.is_empty()) {
        entries.pop();
    }
    entries
        .into_iter()
        .map(|entry| {
            let (host, count) = entry.rsplit_once(':')?;
            let host = java_trim(host);
            let count: i32 = java_trim(count).parse().ok()?;
            valid_host(host).then(|| (host.to_owned(), usize::try_from(count).unwrap_or(0)))
        })
        .collect()
}

/// The value of a key in force: the most preferred layer's value when it
/// parses, else the `previous` value, else the next layer that parses, else
/// `fallback`.
fn pick<T>(
    values: &[&str],
    parse: impl Fn(&str) -> Option<T>,
    previous: Option<T>,
    fallback: T,
) -> T {
    let Some((first, rest)) = values.split_first() else {
        return fallback;
    };
    parse(first)
        .or(previous)
        .or_else(|| rest.iter().find_map(|value| parse(value)))
        .unwrap_or(fallback)
}

impl ConnectionQuotaSettings {
    /// The quotas in force on `listener` of `node_id` as of `image`, over the
    /// caps the node was started with and the `previous` settings of the
    /// listener, which a refused value leaves in force.
    #[must_use]
    pub fn resolve(
        image: &MetadataImage,
        node_id: NodeId,
        listener: &str,
        configured: ConnectionLimits,
        previous: Option<&Self>,
    ) -> Self {
        let layers = [image.broker_config(node_id), image.default_broker_config()];
        // The values of `key`, most preferred layer first.
        let value = |key: &str| -> Vec<&str> {
            layers
                .iter()
                .filter_map(|configs| configs.and_then(|configs| configs.get(key)))
                .map(String::as_str)
                .collect()
        };
        // Every listener inherits `max.connections`, and Kafka refuses a
        // listener cap below 1, so the broker-wide value needs 1 as well.
        let max_connections = pick(
            &value(MAX_CONNECTIONS),
            |v| parse_cap(v, 1),
            previous.map(|p| p.max_connections),
            configured.max_connections,
        );
        let per_ip_overrides = pick(
            &value(MAX_CONNECTIONS_PER_IP_OVERRIDES),
            parse_overrides,
            previous.map(|p| p.per_ip_overrides.clone()),
            BTreeMap::new(),
        );
        // `KafkaConfig.validateValues` allows a per-IP cap of 0 only beside
        // overrides.
        let per_ip_min = i32::from(per_ip_overrides.is_empty());
        let max_connections_per_ip = pick(
            &value(MAX_CONNECTIONS_PER_IP),
            |v| parse_cap(v, per_ip_min),
            previous.map(|p| p.max_connections_per_ip),
            configured.max_connections_per_ip,
        );
        let listener_max_connections = pick(
            &value(&listener_key(listener, MAX_CONNECTIONS)),
            |v| parse_cap(v, 1),
            previous.map(|p| p.listener_max_connections),
            max_connections,
        );
        let max_connection_creation_rate = pick(
            &value(MAX_CONNECTION_CREATION_RATE),
            |v| parse_rate(v).map(limited),
            previous.map(|p| p.max_connection_creation_rate),
            None,
        );
        let listener_max_connection_creation_rate = pick(
            &value(&listener_key(listener, MAX_CONNECTION_CREATION_RATE)),
            |v| parse_rate(v).map(limited),
            previous.map(|p| p.listener_max_connection_creation_rate),
            max_connection_creation_rate,
        );
        Self {
            max_connections,
            max_connections_per_ip,
            per_ip_overrides,
            listener_max_connections,
            max_connection_creation_rate,
            listener_max_connection_creation_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Apply one dynamic broker config, for node 1 or the cluster default.
    fn set_broker_config(image: &mut MetadataImage, node: Option<u64>, name: &str, value: &str) {
        image.apply(&krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: node.map_or(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID, NodeId),
                config_name: name.into(),
                config_value: Some(value.into()),
            },
        ));
    }

    const CONFIGURED: ConnectionLimits = ConnectionLimits {
        max_connections: 10,
        max_connections_per_ip: 5,
    };

    /// The settings with nothing dynamic, over [`CONFIGURED`].
    fn unset() -> ConnectionQuotaSettings {
        ConnectionQuotaSettings {
            max_connections: 10,
            max_connections_per_ip: 5,
            per_ip_overrides: BTreeMap::new(),
            listener_max_connections: 10,
            max_connection_creation_rate: None,
            listener_max_connection_creation_rate: None,
        }
    }

    fn overrides(entries: &[(&str, usize)]) -> BTreeMap<String, usize> {
        entries
            .iter()
            .map(|(host, count)| ((*host).to_owned(), *count))
            .collect()
    }

    #[yare::parameterized(
        static_only = { &[], unset() },
        cluster_default = {
            &[(None, MAX_CONNECTIONS, "3")],
            ConnectionQuotaSettings { max_connections: 3, listener_max_connections: 3, ..unset() }
        },
        per_broker_over_default = {
            &[(None, MAX_CONNECTIONS_PER_IP, "2"), (Some(1), MAX_CONNECTIONS_PER_IP, "4")],
            ConnectionQuotaSettings { max_connections_per_ip: 4, ..unset() }
        },
        other_broker_ignored = { &[(Some(2), MAX_CONNECTIONS, "1")], unset() },
        zero_global_not_applied = { &[(None, MAX_CONNECTIONS, "0")], unset() },
        trimmed = {
            &[(None, MAX_CONNECTIONS, " 7 ")],
            ConnectionQuotaSettings { max_connections: 7, listener_max_connections: 7, ..unset() }
        },
        unparsable_falls_back_a_layer = {
            &[(None, MAX_CONNECTIONS, "2"), (Some(1), MAX_CONNECTIONS, "lots")],
            ConnectionQuotaSettings { max_connections: 2, listener_max_connections: 2, ..unset() }
        },
        zero_per_ip_needs_overrides = { &[(None, MAX_CONNECTIONS_PER_IP, "0")], unset() },
        zero_per_ip_beside_overrides = {
            &[(None, MAX_CONNECTIONS_PER_IP, "0"), (None, MAX_CONNECTIONS_PER_IP_OVERRIDES, "127.0.0.1:2")],
            ConnectionQuotaSettings {
                max_connections_per_ip: 0,
                per_ip_overrides: overrides(&[("127.0.0.1", 2)]),
                ..unset()
            }
        },
        listener_cap_over_broker_wide = {
            &[(None, MAX_CONNECTIONS, "8"), (Some(1), "listener.name.plaintext.max.connections", "3")],
            ConnectionQuotaSettings { max_connections: 8, listener_max_connections: 3, ..unset() }
        },
        other_listener_ignored = {
            &[(None, "listener.name.external.max.connections", "3")],
            unset()
        },
        zero_listener_cap_not_applied = {
            &[(None, "listener.name.plaintext.max.connections", "0")],
            unset()
        },
        broker_rate = {
            &[(None, MAX_CONNECTION_CREATION_RATE, "20")],
            ConnectionQuotaSettings {
                max_connection_creation_rate: Some(20),
                listener_max_connection_creation_rate: Some(20),
                ..unset()
            }
        },
        listener_rate_over_broker_rate = {
            &[
                (None, MAX_CONNECTION_CREATION_RATE, "20"),
                (None, "listener.name.plaintext.max.connection.creation.rate", "5"),
            ],
            ConnectionQuotaSettings {
                max_connection_creation_rate: Some(20),
                listener_max_connection_creation_rate: Some(5),
                ..unset()
            }
        },
        int_max_rate_is_unlimited = {
            &[(None, MAX_CONNECTION_CREATION_RATE, "2147483647")],
            unset()
        },
        zero_rate_not_applied = { &[(None, MAX_CONNECTION_CREATION_RATE, "0")], unset() },
    )]
    fn settings_layer_broker_default_listener_and_static(
        configs: &[(Option<u64>, &str, &str)],
        want: ConnectionQuotaSettings,
    ) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (node, name, value) in configs {
            set_broker_config(&mut image, *node, name, value);
        }
        assert2::assert!(
            ConnectionQuotaSettings::resolve(&image, NodeId(1), "PLAINTEXT", CONFIGURED, None)
                == want
        );
    }

    /// A refused value keeps the value in force before it, which an unset
    /// key does not: after `max.connections=2`, a stored 0 keeps 2, and a
    /// deleted key returns to the static cap.
    #[yare::parameterized(
        refused_keeps_previous = { Some("0"), 2 },
        unparsable_keeps_previous = { Some("lots"), 2 },
        deleted_returns_to_static = { None, 10 },
        valid_replaces_previous = { Some("4"), 4 },
    )]
    fn a_refused_value_keeps_the_previous_one(next: Option<&str>, want: usize) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        set_broker_config(&mut image, None, MAX_CONNECTIONS, "2");
        let previous =
            ConnectionQuotaSettings::resolve(&image, NodeId(1), "PLAINTEXT", CONFIGURED, None);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(value) = next {
            set_broker_config(&mut image, None, MAX_CONNECTIONS, value);
        }
        let got = ConnectionQuotaSettings::resolve(
            &image,
            NodeId(1),
            "PLAINTEXT",
            CONFIGURED,
            Some(&previous),
        );
        assert2::assert!((got.max_connections, got.listener_max_connections) == (want, want));
    }

    #[yare::parameterized(
        empty = { "", Some(overrides(&[])) },
        one = { "127.0.0.1:2", Some(overrides(&[("127.0.0.1", 2)])) },
        spaced = { " a.example : 3 , b:4 ", Some(overrides(&[("a.example", 3), ("b", 4)])) },
        ipv6_splits_on_last_colon = { "::1:5", Some(overrides(&[("::1", 5)])) },
        trailing_comma = { "a:1,", Some(overrides(&[("a", 1)])) },
        last_duplicate_wins = { "a:1,a:2", Some(overrides(&[("a", 2)])) },
        negative_refuses_all = { "a:-1", Some(overrides(&[("a", 0)])) },
        leading_comma = { ",a:1", None },
        no_colon = { "a", None },
        not_a_number = { "a:x", None },
        bad_host = { "a/b:1", None },
    )]
    fn overrides_parse_as_kafka_parses_them(value: &str, want: Option<BTreeMap<String, usize>>) {
        assert2::assert!(parse_overrides(value) == want);
    }
}
