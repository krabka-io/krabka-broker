//! Applying the listener-shaped file values to a `BrokerConfig`.
//!
//! [`ListenerSettings`] carries the `[[listeners]]` array together with the
//! top-level keys that only make sense beside it — the inter-broker listener
//! name, the connection ceilings, the raw `server_properties`, and the
//! controller listener's protocol and TLS material. `apply_listener_settings`
//! writes them under the fill-or-replace rules the file config uses.

use krabka_security::ListenerProtocol;
use krabka_units::Time;

use super::{FileClientAuthMode, FileConfigError, FileListener, FileTlsConfig};

pub(super) struct ListenerSettings {
    pub(super) listeners: Vec<FileListener>,
    pub(super) inter_broker_listener_name: Option<String>,
    pub(super) max_connections: Option<usize>,
    pub(super) max_connections_per_ip: Option<usize>,
    pub(super) connections_max_idle: Option<Time>,
    pub(super) connections_max_reauth: Option<Time>,
    pub(super) server_properties: std::collections::BTreeMap<String, String>,
    pub(super) controller_listener_protocol: Option<ListenerProtocol>,
    pub(super) tls_config: Option<FileTlsConfig>,
}

/// KIP-464: `num.partitions` and `default.replication.factor` under their
/// Kafka names. A dedicated `[runtime]` key or CLI flag wins, so a property
/// applies only when no dedicated source named the setting. A value that is
/// not a positive integer refuses the configuration, as Kafka's `ConfigDef`
/// refuses it at startup.
fn apply_topic_creation_properties(
    properties: &std::collections::BTreeMap<String, String>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let origins = &mut cfg.static_config_origins.topic_creation;
    if !origins.num_partitions
        && let Some(value) = properties.get(crate::config_keys::NUM_PARTITIONS)
    {
        cfg.num_partitions = parse_positive(crate::config_keys::NUM_PARTITIONS, value)?;
        origins.num_partitions = true;
    }
    if !origins.default_replication_factor
        && let Some(value) = properties.get(crate::config_keys::DEFAULT_REPLICATION_FACTOR)
    {
        cfg.default_replication_factor =
            parse_positive(crate::config_keys::DEFAULT_REPLICATION_FACTOR, value)?;
        origins.default_replication_factor = true;
    }
    Ok(())
}

/// A positive integer `server_properties` value.
fn parse_positive<T: std::str::FromStr + Default + PartialOrd>(
    name: &str,
    value: &str,
) -> Result<T, FileConfigError> {
    value
        .trim()
        .parse::<T>()
        .ok()
        .filter(|parsed| *parsed > T::default())
        .ok_or_else(|| {
            FileConfigError::InvalidConfig(format!(
                "server_properties `{name}` must be a positive integer, got `{value}`"
            ))
        })
}

pub(super) fn apply_listener_settings(
    settings: ListenerSettings,
    cfg: &mut crate::config::BrokerConfig,
    defaults: &crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let had_file_listeners = !settings.listeners.is_empty();
    if had_file_listeners {
        // Read the per-listener idle overrides before `into_spec` consumes
        // each entry. They are keyed by listener name, which is how the
        // dispatch loop and `DescribeConfigs` both look one up.
        cfg.connections_max_idle_overrides = settings
            .listeners
            .iter()
            .filter_map(|listener| {
                listener
                    .connections_max_idle
                    .map(|idle| (listener.name.clone(), idle))
            })
            .collect();
        cfg.connections_max_reauth_overrides = settings
            .listeners
            .iter()
            .filter_map(|listener| {
                listener
                    .connections_max_reauth
                    .map(|reauth| (listener.name.clone(), reauth))
            })
            .collect();
        cfg.listeners = settings
            .listeners
            .into_iter()
            .map(FileListener::into_spec)
            .collect::<Result<_, _>>()?;
    }
    if let Some(name) = settings.inter_broker_listener_name {
        cfg.inter_broker_listener_name = name;
    }
    if had_file_listeners
        && let Some(advertised) = cfg
            .listeners
            .iter()
            .find(|listener| listener.name == cfg.inter_broker_listener_name)
            .or_else(|| cfg.listeners.first())
            .map(|listener| listener.advertised.clone())
    {
        cfg.advertised_listener = advertised;
    }
    if let Some(maximum) = settings.max_connections
        && cfg.max_connections == defaults.max_connections
    {
        cfg.max_connections = maximum;
    }
    if let Some(maximum) = settings.max_connections_per_ip
        && cfg.max_connections_per_ip == defaults.max_connections_per_ip
    {
        cfg.max_connections_per_ip = maximum;
    }
    if let Some(idle) = settings.connections_max_idle
        && cfg.connections_max_idle.is_none()
    {
        cfg.connections_max_idle = Some(idle);
    }
    if let Some(reauth) = settings.connections_max_reauth
        && cfg.connections_max_reauth.is_none()
    {
        cfg.connections_max_reauth = Some(reauth);
    }
    if cfg.features.transaction_two_phase_commit_enable
        == defaults.features.transaction_two_phase_commit_enable
        && let Some(value) = settings
            .server_properties
            .get("transaction.two.phase.commit.enable")
    {
        cfg.features.transaction_two_phase_commit_enable =
            value.trim().eq_ignore_ascii_case("true");
    }
    apply_topic_creation_properties(&settings.server_properties, cfg)?;
    let num_val = settings
        .server_properties
        .get("quota.window.num")
        .and_then(|v| v.trim().parse::<u32>().ok());
    let size_val = settings
        .server_properties
        .get("quota.window.size.seconds")
        .and_then(|v| v.trim().parse::<u64>().ok());
    if num_val.is_some() || size_val.is_some() {
        let num = num_val.unwrap_or(11);
        let size_secs = size_val.unwrap_or(1);
        if cfg.quota_throttle_max == defaults.quota_throttle_max {
            let s = u32::try_from(size_secs * u64::from(num.saturating_sub(1))).unwrap_or(u32::MAX);
            cfg.quota_throttle_max = krabka_units::secs(s);
        }
        if cfg.quota_window == defaults.quota_window {
            let s = u32::try_from(size_secs * u64::from(num)).unwrap_or(u32::MAX);
            cfg.quota_window = krabka_units::secs(s);
        }
    }
    if let Some(protocol) = settings.controller_listener_protocol
        && cfg.controller_listener_protocol == defaults.controller_listener_protocol
    {
        cfg.controller_listener_protocol = protocol;
    }
    if let Some(tls) = settings.tls_config
        && cfg.tls_config.is_none()
    {
        use krabka_security::{ClientAuthMode, TlsConfig};
        cfg.tls_principal_mapper = crate::SslPrincipalMapper::parse(&tls.principal_mapping_rules)
            .map_err(|error| {
            FileConfigError::InvalidConfig(format!(
                "invalid ssl principal mapping rule in tls_config: {error}"
            ))
        })?;
        cfg.tls_config = Some(TlsConfig {
            cert_chain_path: tls.cert_path,
            private_key_path: tls.key_path,
            trust_roots_path: tls.trust_roots_path,
            client_ca_path: tls.client_ca_path,
            client_auth: match tls.client_auth {
                FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                FileClientAuthMode::Optional => ClientAuthMode::Optional,
                FileClientAuthMode::Required => ClientAuthMode::Required,
            },
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use crate::file_config::FileConfig;

    #[test]
    fn apply_to_populates_listeners() {
        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        check!(cfg.listeners.len() == 1);
        check!(cfg.listeners[0].name.as_str() == "PLAIN");
        check!(cfg.listeners[0].advertised.as_str() == "demo-0:9092");
        check!(cfg.inter_broker_listener_name.as_str() == "PLAIN");
    }
    #[test]
    fn apply_to_maps_connection_caps() {
        use crate::config::BrokerConfig;

        let src = r"
max_connections = 100
max_connections_per_ip = 8
";
        let file: FileConfig = toml::from_str(src).unwrap();
        assert!(file.max_connections == Some(100));
        assert!(file.max_connections_per_ip == Some(8));

        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.max_connections == 100);
        assert!(cfg.max_connections_per_ip == 8);
    }
    #[test]
    fn apply_to_omitted_connection_caps_keep_default_unlimited() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.max_connections == None);
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // Omitted → unchanged from the (unlimited) BrokerConfig default.
        assert!(cfg.max_connections == usize::MAX);
        assert!(cfg.max_connections_per_ip == usize::MAX);
    }
    /// The idle window comes in two places: a top-level key for the broker
    /// and a per-listener key that wins for the listener that carries it.
    #[test]
    fn apply_to_maps_the_idle_window_and_its_per_listener_override() {
        use std::time::Duration;

        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "INTERNAL"
connections_max_idle = "45s"

[[listeners]]
name = "INTERNAL"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"
connections_max_idle = "5s"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        check!(cfg.connections_max_idle == Some(krabka_units::secs(45)));
        check!(
            cfg.connections_max_idle_overrides
                == maplit::btreemap! {"EXTERNAL".to_string() => krabka_units::secs(5)}
        );
        check!(cfg.connections_max_idle_for("EXTERNAL") == Some(Duration::from_secs(5)));
        check!(cfg.connections_max_idle_for("INTERNAL") == Some(Duration::from_secs(45)));
    }

    /// KIP-464: `num.partitions` and `default.replication.factor` under their
    /// Kafka names in `[server_properties]`. A dedicated `[runtime]` key wins
    /// over the property, and a value that is not a positive integer refuses
    /// the file.
    #[test]
    fn apply_to_reads_the_topic_creation_defaults_from_server_properties() {
        use crate::config::{BrokerConfig, TopicCreationOrigins};

        for (label, src, expected) in [
            (
                "neither named",
                "broker_id = 0\n",
                Ok((1, 1, TopicCreationOrigins::default())),
            ),
            (
                "both as properties",
                "[server_properties]\n\"num.partitions\" = \"6\"\n\
                 \"default.replication.factor\" = \"3\"\n",
                Ok((
                    6,
                    3,
                    TopicCreationOrigins {
                        num_partitions: true,
                        default_replication_factor: true,
                    },
                )),
            ),
            (
                "a dedicated key wins over the property",
                "[runtime]\nnum_partitions = 4\n\
                 [server_properties]\n\"num.partitions\" = \"6\"\n",
                Ok((
                    4,
                    1,
                    TopicCreationOrigins {
                        num_partitions: true,
                        default_replication_factor: false,
                    },
                )),
            ),
            (
                "a zero is refused",
                "[server_properties]\n\"default.replication.factor\" = \"0\"\n",
                Err(
                    "server_properties `default.replication.factor` must be a positive integer, got `0`",
                ),
            ),
            (
                "a word is refused",
                "[server_properties]\n\"num.partitions\" = \"many\"\n",
                Err("server_properties `num.partitions` must be a positive integer, got `many`"),
            ),
        ] {
            let file: FileConfig = toml::from_str(src).expect("parse");
            let mut cfg = BrokerConfig::default();
            let result = file.apply_to(&mut cfg).map(|()| {
                (
                    cfg.num_partitions,
                    cfg.default_replication_factor,
                    cfg.static_config_origins.topic_creation,
                )
            });
            let result = result.map_err(|error| error.to_string());

            check!(
                result
                    .as_ref()
                    .map_err(|message| message.contains(expected.err().unwrap_or("-")))
                    == expected.as_ref().map_err(|_| true),
                "{label}: {result:?}"
            );
        }
    }

    /// Omitted everywhere, the broker keeps Kafka's 600000 default and no
    /// listener carries an override.
    #[test]
    fn apply_to_omitted_idle_window_keeps_kafkas_default() {
        use crate::config::{BrokerConfig, DEFAULT_CONNECTIONS_MAX_IDLE};

        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.connections_max_idle.is_none());

        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // Unset stays unset -- that is what `DescribeConfigs` reports as
        // DEFAULT_CONFIG -- and the window in force is Kafka's default.
        assert!(cfg.connections_max_idle.is_none());
        assert!(cfg.effective_connections_max_idle() == DEFAULT_CONNECTIONS_MAX_IDLE);
        assert!(cfg.connections_max_idle_overrides.is_empty());
    }

    #[test]
    fn apply_to_reads_two_phase_commit_enable_from_server_properties() {
        use crate::config::BrokerConfig;

        // KIP-939: the `transaction.two.phase.commit.enable` server property
        // flips the cluster 2PC gate on; absent / "false" leaves it off.
        let on: FileConfig = toml::from_str(
            "[server_properties]\n\"transaction.two.phase.commit.enable\" = \"true\"\n",
        )
        .unwrap();
        let mut cfg = BrokerConfig::default();
        assert!(!cfg.features.transaction_two_phase_commit_enable); // default
        on.apply_to(&mut cfg).unwrap();
        assert!(cfg.features.transaction_two_phase_commit_enable);

        // Omitted → unchanged (stays at the default false).
        let absent: FileConfig = toml::from_str("broker_id = 0").unwrap();
        let mut cfg2 = BrokerConfig::default();
        absent.apply_to(&mut cfg2).unwrap();
        assert!(!cfg2.features.transaction_two_phase_commit_enable);
    }

    #[test]
    fn apply_to_reads_quota_window_properties() {
        use krabka_units::secs;

        use crate::config::BrokerConfig;

        let toml = r#"
[server_properties]
"quota.window.num" = "6"
"quota.window.size.seconds" = "2"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // size * (num - 1) = 2 * 5 = 10s
        assert!(cfg.quota_throttle_max == secs(10));
        // size * num = 2 * 6 = 12s
        assert!(cfg.quota_window == secs(12));
    }
    #[test]
    fn apply_to_propagates_tls_config() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.controller_listener_protocol == krabka_security::ListenerProtocol::Ssl);
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(tls.cert_chain_path == std::path::PathBuf::from("/c"));
    }
    #[test]
    fn apply_to_threads_trust_roots_and_controller_server_name() {
        // The operator renders the cluster CA as the dialer trust root and
        // the shared headless FQDN as the controller SNI so KIP-595 peers can
        // mTLS to each other.
        let src = r#"
controller_server_name = "demo-broker-headless.default.svc.cluster.local"
[tls_config]
cert_path = "/etc/krabka/broker-tls/0.crt"
key_path = "/etc/krabka/broker-tls/0.key"
trust_roots_path = "/etc/krabka/cluster-ca/ca.crt"
client_ca_path = "/etc/krabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(
            cfg.controller_server_name.as_deref()
                == Some("demo-broker-headless.default.svc.cluster.local")
        );
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(
            tls.trust_roots_path.as_deref()
                == Some(std::path::Path::new("/etc/krabka/cluster-ca/ca.crt"))
        );
    }
    /// The top-level `[tls_config]` carries the KIP-371 principal mapping
    /// rules that the controller listener applies to a peer certificate.
    #[test]
    fn apply_to_parses_top_level_principal_mapping_rules() {
        let base = r#"
[tls_config]
cert_path = "/etc/krabka/tls/node.crt"
key_path = "/etc/krabka/tls/node.key"
"#;
        let dn = "CN=node-1,OU=brokers,O=krabka";
        let cases = [
            ("no rules", String::new(), Some(dn)),
            (
                "a rule",
                "principal_mapping_rules = [\"RULE:^CN=(.*?),.*$/$1/\"]\n".to_owned(),
                Some("node-1"),
            ),
            (
                "a rule that matches nothing",
                "principal_mapping_rules = [\"RULE:^OU=(.*?)$/$1/\"]\n".to_owned(),
                None,
            ),
        ];
        for (name, rules, expected) in cases {
            let file: FileConfig = toml::from_str(&format!("{base}{rules}")).expect("parse");
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            check!(
                cfg.tls_principal_mapper.apply(dn).as_deref() == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn apply_to_empty_listeners_does_not_clear_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = BrokerConfig {
            listeners: vec![crate::config::ListenerSpec {
                name: "X".into(),
                bind_addr: "0.0.0.0:9094".parse().unwrap(),
                advertised: "h:9094".into(),
                protocol: krabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
                principal_mapper: crate::SslPrincipalMapper::default(),
            }],
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.listeners.len() == 1);
        assert!(cfg.listeners[0].name == "X");
    }
    #[test]
    fn apply_to_syncs_advertised_listener_from_inter_broker_listener() {
        use crate::config::BrokerConfig;

        // Two listeners; the inter-broker one ("PLAIN") is NOT declared first.
        // `advertised_listener` (used by FindCoordinator + broker
        // self-registration) must be taken from the inter-broker listener's
        // `advertised` (the pod FQDN), not left at the CLI default
        // 127.0.0.1:9092 and not taken from the first-declared listener.
        let toml = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "ext.example.com:9094"
protocol = "Plaintext"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.advertised_listener == "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
        );
        // The inter-broker listener wins over the first-declared EXTERNAL one.
        assert!(cfg.advertised_listener != "ext.example.com:9094");
    }
}
