//! The static broker configs `DescribeConfigs` reports beside the dynamic
//! overrides the metadata image holds.
//!
//! Kafka answers a named BROKER resource with every value the node actually
//! runs with, static ones included, and answers the cluster-default
//! (`--entity-default`) resource with the dynamic cluster-wide values only.
//! krabka reports the static subset an operator sets and then has to verify
//! from the outside: today that is `connections.max.idle.ms` and its
//! per-listener overrides. They go out beside the static `node.id` entry, for
//! the same resources it does.
//!
//! Every shape below was read off `apache/kafka:4.3.1` running with
//! `connections.max.idle.ms=30000` and
//! `listener.name.plaintext.connections.max.idle.ms=15000`:
//!
//! - `kafka-configs --describe --entity-type brokers --entity-name 1 --all`
//!   reports `connections.max.idle.ms=30000` with the synonym chain
//!   `STATIC_BROKER_CONFIG:…=30000, DEFAULT_CONFIG:…=600000`. So the key is
//!   spelled that way, an operator-set value reads `STATIC_BROKER_CONFIG`, an
//!   unset one falls back to `DEFAULT_CONFIG`, and the default is 600000.
//! - The same describe reports
//!   `listener.name.plaintext.connections.max.idle.ms=15000` as a separate
//!   entry, also at `STATIC_BROKER_CONFIG`, under a lowercased listener name.
//! - `kafka-configs --alter … --add-config connections.max.idle.ms=20000`
//!   fails with `InvalidRequestException: Cannot update these configs
//!   dynamically`, so the key is read-only.
//!
//! One behaviour is krabka's own, and the container is what settled it: with
//! the per-listener key set to 15000 above, a raw TCP connection to the
//! PLAINTEXT listener that sent nothing was closed after 30 seconds, not 15.
//! Kafka reports `listener.name.<name>.connections.max.idle.ms` and then
//! ignores it -- its `DataPlaneAcceptor` takes `max.connections` and
//! `max.connection.creation.rate` per listener, but the idle window only
//! broker-wide. krabka honours the override, because a listener facing the
//! internet and a listener facing the cluster want different windows. The
//! reported shape is Kafka's; only the effect is a superset, and nothing on
//! the wire changes.

use krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult;
use krabka_units::convert::TimeExt as _;

use super::wire::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_STATIC_BROKER, make_entry};
use crate::config::DEFAULT_CONNECTIONS_MAX_IDLE;

/// Kafka's spelling of the broker-wide idle window.
pub(crate) const CONNECTIONS_MAX_IDLE_MS: &str = "connections.max.idle.ms";

/// The per-listener key for `listener_name`. The name in the middle is
/// lowercased, which is what Kafka's `ListenerName.configPrefix` does.
fn listener_connections_max_idle_key(listener_name: &str) -> String {
    format!(
        "listener.name.{}.{CONNECTIONS_MAX_IDLE_MS}",
        listener_name.to_ascii_lowercase()
    )
}

/// The static entries for a named broker resource, in key order.
pub(super) fn static_broker_entries(
    config: &crate::config::BrokerConfig,
) -> Vec<DescribeConfigsResourceResult> {
    let source = if config.connections_max_idle == DEFAULT_CONNECTIONS_MAX_IDLE {
        CONFIG_SOURCE_DEFAULT
    } else {
        CONFIG_SOURCE_STATIC_BROKER
    };
    let mut entries = vec![make_entry(
        CONNECTIONS_MAX_IDLE_MS,
        &config.connections_max_idle.millis_i64().to_string(),
        source,
    )];
    entries.extend(
        config
            .connections_max_idle_overrides
            .iter()
            .map(|(listener, idle)| {
                make_entry(
                    &listener_connections_max_idle_key(listener),
                    &idle.millis_i64().to_string(),
                    CONFIG_SOURCE_STATIC_BROKER,
                )
            }),
    );
    for entry in &mut entries {
        // Neither key is in Kafka's dynamically-updatable set, so
        // `kafka-configs` must show them the way it shows every other broker
        // config no alter can change.
        entry.read_only = true;
    }
    entries.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    entries
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;
    use krabka_units::{Time, millis, secs};

    use super::*;

    /// A broker-wide window of `idle` with the named per-listener overrides.
    fn config_with(idle: Time, overrides: &[(&str, Time)]) -> crate::config::BrokerConfig {
        crate::config::BrokerConfig {
            connections_max_idle: idle,
            connections_max_idle_overrides: overrides
                .iter()
                .map(|(name, value)| ((*name).to_string(), *value))
                .collect(),
            ..crate::config::BrokerConfig::default()
        }
    }

    fn expected(name: &str, value: &str, source: i8) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult {
            name: name.to_string(),
            value: Some(value.to_string()),
            read_only: true,
            config_source: source,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0,
            documentation: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    #[test]
    fn default_idle_window_reports_kafkas_600000_at_default_config() {
        assert!(
            static_broker_entries(&config_with(DEFAULT_CONNECTIONS_MAX_IDLE, &[]))
                == vec![expected(
                    CONNECTIONS_MAX_IDLE_MS,
                    "600000",
                    CONFIG_SOURCE_DEFAULT
                )]
        );
    }

    #[test]
    fn a_set_idle_window_and_its_listener_overrides_report_as_static() {
        let entries = static_broker_entries(&config_with(
            secs(30),
            &[("PLAINTEXT", millis(15_000)), ("EXTERNAL", secs(45))],
        ));

        assert!(
            entries
                == vec![
                    expected(
                        CONNECTIONS_MAX_IDLE_MS,
                        "30000",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                    expected(
                        "listener.name.external.connections.max.idle.ms",
                        "45000",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                    expected(
                        "listener.name.plaintext.connections.max.idle.ms",
                        "15000",
                        CONFIG_SOURCE_STATIC_BROKER
                    ),
                ]
        );
    }
}
