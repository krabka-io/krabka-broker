//! The connection quotas of Kafka's `ConnectionQuotas`, read from the dynamic
//! broker configs of the metadata image.
//!
//! Kafka layers a node's own dynamic config over the cluster default, and a
//! listener's `listener.name.<listener>.` value over the broker-wide one
//! (`KafkaConfig.valuesWithPrefixOverride`). A value Kafka would not apply
//! leaves the next layer in force: `DynamicBrokerConfig` keeps its previous
//! config when the new one fails `KafkaConfig` validation or a
//! reconfigurable's `validateReconfiguration`.

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

impl ConnectionQuotaSettings {
    /// The quotas in force on `listener` of `node_id` as of `image`, over the
    /// caps the node was started with.
    #[must_use]
    pub fn resolve(
        image: &MetadataImage,
        node_id: NodeId,
        listener: &str,
        configured: ConnectionLimits,
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
        let max_connections = value(MAX_CONNECTIONS)
            .into_iter()
            .find_map(|v| parse_cap(v, 1))
            .unwrap_or(configured.max_connections);
        let per_ip_overrides = value(MAX_CONNECTIONS_PER_IP_OVERRIDES)
            .into_iter()
            .find_map(parse_overrides)
            .unwrap_or_default();
        // `KafkaConfig.validateValues` allows a per-IP cap of 0 only beside
        // overrides.
        let per_ip_min = i32::from(per_ip_overrides.is_empty());
        let max_connections_per_ip = value(MAX_CONNECTIONS_PER_IP)
            .into_iter()
            .find_map(|v| parse_cap(v, per_ip_min))
            .unwrap_or(configured.max_connections_per_ip);
        let listener_max_connections = value(&listener_key(listener, MAX_CONNECTIONS))
            .into_iter()
            .find_map(|v| parse_cap(v, 1))
            .unwrap_or(max_connections);
        let max_connection_creation_rate = value(MAX_CONNECTION_CREATION_RATE)
            .into_iter()
            .find_map(parse_rate)
            .and_then(limited);
        let listener_max_connection_creation_rate =
            value(&listener_key(listener, MAX_CONNECTION_CREATION_RATE))
                .into_iter()
                .find_map(parse_rate)
                .map_or(max_connection_creation_rate, limited);
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
            ConnectionQuotaSettings::resolve(&image, NodeId(1), "PLAINTEXT", CONFIGURED) == want
        );
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
