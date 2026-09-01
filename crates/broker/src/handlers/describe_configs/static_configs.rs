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
//! Two finer points about the source and the chain were settled the same way,
//! against the same image, by running it twice — once with
//! `connections.max.idle.ms=600000` spelled out and once with the key absent —
//! and diffing the same `--all` describe:
//!
//! - The source is provenance, never a value comparison. The explicit
//!   `connections.max.idle.ms=600000` — the default, spelled out — answers
//!   `synonyms={STATIC_BROKER_CONFIG:connections.max.idle.ms=600000,
//!   DEFAULT_CONFIG:connections.max.idle.ms=600000}`. With the key absent the
//!   same describe answers `synonyms={DEFAULT_CONFIG:…=600000}`. The two
//!   values are identical and the two chains are not, and since an entry's
//!   `config_source` is the source of its chain's head, the two sources
//!   differ too — so an auditor can tell a pinned setting from an inherited
//!   one.
//! - A per-listener entry's chain is its own value first, then the same
//!   broker-wide tail: with both keys set it reads
//!   `{STATIC_BROKER_CONFIG:listener.name.plaintext.connections.max.idle.ms
//!   =15000, STATIC_BROKER_CONFIG:connections.max.idle.ms=600000,
//!   DEFAULT_CONFIG:connections.max.idle.ms=600000}`, and with the
//!   broker-wide key absent the middle element is simply gone. `node.id`,
//!   which has no key behind it, gets the one-element chain
//!   `{STATIC_BROKER_CONFIG:node.id=1}`.
//!
//! The chain itself is conditional: Kafka builds it only for a request that
//! sets `include_synonyms`, and answers every entry with an empty list
//! otherwise, so a caller that was not asked for synonyms must not
//! synthesise any.
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

use krabka_protocol::owned::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsSynonym,
};
use krabka_units::convert::TimeExt as _;

use super::wire::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_STATIC_BROKER, make_entry, synonym};
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
///
/// `include_synonyms` is the request's flag. When it is unset every entry
/// carries an empty chain, which is what the container answers.
pub(super) fn static_broker_entries(
    config: &crate::config::BrokerConfig,
    include_synonyms: bool,
) -> Vec<DescribeConfigsResourceResult> {
    let configured = config.connections_max_idle.is_some();
    let broker_wide = config
        .effective_connections_max_idle()
        .millis_i64()
        .to_string();
    // The tail every idle key falls back through once its own value is spent:
    // the broker-wide value when an operator wrote one, then Kafka's default.
    let mut fallback: Vec<DescribeConfigsSynonym> = Vec::new();
    if include_synonyms {
        if configured {
            fallback.push(synonym(
                CONNECTIONS_MAX_IDLE_MS,
                &broker_wide,
                CONFIG_SOURCE_STATIC_BROKER,
            ));
        }
        fallback.push(synonym(
            CONNECTIONS_MAX_IDLE_MS,
            &DEFAULT_CONNECTIONS_MAX_IDLE.millis_i64().to_string(),
            CONFIG_SOURCE_DEFAULT,
        ));
    }

    // Provenance, not a value comparison: an operator who spells out Kafka's
    // own 600000 has still set the key statically, and Kafka says so.
    let source = if configured {
        CONFIG_SOURCE_STATIC_BROKER
    } else {
        CONFIG_SOURCE_DEFAULT
    };
    let mut broker_wide_entry = make_entry(CONNECTIONS_MAX_IDLE_MS, &broker_wide, source);
    broker_wide_entry.synonyms.clone_from(&fallback);
    let mut entries = vec![broker_wide_entry];
    entries.extend(
        config
            .connections_max_idle_overrides
            .iter()
            .map(|(listener, idle)| {
                let key = listener_connections_max_idle_key(listener);
                let value = idle.millis_i64().to_string();
                let mut entry = make_entry(&key, &value, CONFIG_SOURCE_STATIC_BROKER);
                if include_synonyms {
                    entry.synonyms =
                        std::iter::once(synonym(&key, &value, CONFIG_SOURCE_STATIC_BROKER))
                            .chain(fallback.iter().cloned())
                            .collect();
                }
                entry
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
    fn config_with(idle: Option<Time>, overrides: &[(&str, Time)]) -> crate::config::BrokerConfig {
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

    /// The broker-wide key's source follows where the value came from, not
    /// what it is: the two configurations below run the same ten-minute
    /// window and report different sources, exactly as the container does.
    #[test]
    fn the_idle_windows_source_is_its_provenance_and_not_its_value() {
        let cases = [
            ("unset", None, CONFIG_SOURCE_DEFAULT),
            (
                "set to Kafka's own default",
                Some(DEFAULT_CONNECTIONS_MAX_IDLE),
                CONFIG_SOURCE_STATIC_BROKER,
            ),
        ];
        for (case, idle, source) in cases {
            assert!(
                static_broker_entries(&config_with(idle, &[]), false)
                    == vec![expected(CONNECTIONS_MAX_IDLE_MS, "600000", source)],
                "{case}"
            );
        }
    }

    #[test]
    fn a_set_idle_window_and_its_listener_overrides_report_as_static() {
        let entries = static_broker_entries(
            &config_with(
                Some(secs(30)),
                &[("PLAINTEXT", millis(15_000)), ("EXTERNAL", secs(45))],
            ),
            false,
        );

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

    /// The chains the container answers with, key by key, for the two
    /// configurations that differ in whether the broker-wide key was set.
    #[test]
    fn requested_synonyms_carry_the_chain_the_container_reports() {
        let listener_key = "listener.name.plaintext.connections.max.idle.ms";
        let broker_wide_default = synonym(CONNECTIONS_MAX_IDLE_MS, "600000", CONFIG_SOURCE_DEFAULT);
        let broker_wide_static = synonym(
            CONNECTIONS_MAX_IDLE_MS,
            "30000",
            CONFIG_SOURCE_STATIC_BROKER,
        );
        let listener_static = synonym(listener_key, "15000", CONFIG_SOURCE_STATIC_BROKER);

        let cases = [
            (
                "broker-wide unset",
                None,
                vec![
                    (
                        CONNECTIONS_MAX_IDLE_MS,
                        "600000",
                        CONFIG_SOURCE_DEFAULT,
                        vec![broker_wide_default.clone()],
                    ),
                    (
                        listener_key,
                        "15000",
                        CONFIG_SOURCE_STATIC_BROKER,
                        vec![listener_static.clone(), broker_wide_default.clone()],
                    ),
                ],
            ),
            (
                "broker-wide set",
                Some(secs(30)),
                vec![
                    (
                        CONNECTIONS_MAX_IDLE_MS,
                        "30000",
                        CONFIG_SOURCE_STATIC_BROKER,
                        vec![broker_wide_static.clone(), broker_wide_default.clone()],
                    ),
                    (
                        listener_key,
                        "15000",
                        CONFIG_SOURCE_STATIC_BROKER,
                        vec![
                            listener_static.clone(),
                            broker_wide_static.clone(),
                            broker_wide_default.clone(),
                        ],
                    ),
                ],
            ),
        ];

        for (case, idle, want) in cases {
            let config = config_with(idle, &[("PLAINTEXT", millis(15_000))]);
            let want: Vec<DescribeConfigsResourceResult> = want
                .into_iter()
                .map(
                    |(name, value, source, synonyms)| DescribeConfigsResourceResult {
                        synonyms,
                        ..expected(name, value, source)
                    },
                )
                .collect();
            assert!(static_broker_entries(&config, true) == want, "{case}");
        }
    }

    /// A request that does not ask for synonyms gets none, on every entry.
    #[test]
    fn unrequested_synonyms_are_never_synthesised() {
        let config = config_with(Some(secs(30)), &[("PLAINTEXT", millis(15_000))]);
        let entries = static_broker_entries(&config, false);

        assert!(entries.len() == 2);
        assert!(entries.iter().all(|entry| entry.synonyms.is_empty()));
    }
}
