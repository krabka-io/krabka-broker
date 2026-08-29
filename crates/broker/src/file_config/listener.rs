//! The listener TOML shapes: `[[listeners]]`, `[tls_config]`, and the
//! per-listener SASL table.
//!
//! [`FileListener`] is the one entry a broker binds, and
//! [`FileListener::into_spec`] converts it into the [`ListenerSpec`] the broker
//! configuration holds. The TLS and SASL tables live here too because both are
//! written either at the top level or inline on a listener.

use std::net::SocketAddr;

use krabka_security::ListenerProtocol;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::config::ListenerSpec;

#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    /// PEM file of CA(s) this broker trusts when validating a PEER's server
    /// cert as an outbound inter-broker / controller-quorum dialer. The
    /// operator renders the cluster CA here so KIP-595 controller peers can
    /// mutually authenticate over the controller listener. Maps to
    /// [`krabka_security::TlsConfig::trust_roots_path`].
    pub trust_roots_path: Option<std::path::PathBuf>,
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileListenerSaslConfig {
    #[serde(default, deserialize_with = "deserialize_sasl_mechanisms")]
    #[schemars(with = "Vec<String>")]
    pub enabled_mechanisms: Vec<krabka_security::SaslMechanism>,
}

fn deserialize_sasl_mechanisms<'de, D>(
    deserializer: D,
) -> Result<Vec<krabka_security::SaslMechanism>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let names: Vec<String> = Vec::deserialize(deserializer)?;
    names
        .into_iter()
        .map(|s| {
            krabka_security::SaslMechanism::from_wire(&s)
                .ok_or_else(|| D::Error::custom(format!("unknown SASL mechanism: {s}")))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
pub struct FileListener {
    pub name: String,
    #[schemars(with = "String")]
    pub bind_addr: SocketAddr,
    pub advertised: String,
    #[schemars(with = "String")]
    pub protocol: ListenerProtocol,
    pub tls_config: Option<FileTlsConfig>,
    pub sasl_config: Option<FileListenerSaslConfig>,
}

impl FileListener {
    #[must_use]
    pub fn into_spec(self) -> ListenerSpec {
        use krabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol,
            tls_config: self.tls_config.map(|t| BrokerTlsConfig {
                cert_chain_path: t.cert_path,
                private_key_path: t.key_path,
                trust_roots_path: t.trust_roots_path,
                client_ca_path: t.client_ca_path,
                client_auth: match t.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            }),
            sasl_mechanisms: self.sasl_config.map(|s| s.enabled_mechanisms),
        }
    }
}

#[cfg(test)]
mod listener_auth_tests {
    use assert2::assert;

    use super::*;
    use crate::file_config::FileConfig;

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.listeners.len() == 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        let expected = FileTlsConfig {
            cert_path: std::path::PathBuf::from("/tls/broker.crt"),
            key_path: std::path::PathBuf::from("/tls/broker.key"),
            trust_roots_path: None,
            client_ca_path: Some(std::path::PathBuf::from("/tls/clients-ca.crt")),
            client_auth: FileClientAuthMode::Required,
        };
        assert!(*data_tls == expected);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Disabled" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert!(sasl.enabled_mechanisms == vec![krabka_security::SaslMechanism::ScramSha512]);
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::file_config::FileConfig;

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "SaslSsl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert!(cfg.listeners[0].protocol == ListenerProtocol::SaslSsl);
    }
    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "Plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_config: None,
        };
        let spec = fl.into_spec();
        check!(spec.name == "X");
        check!(spec.bind_addr == "0.0.0.0:9094".parse::<SocketAddr>().unwrap());
        check!(spec.advertised == "h:9094");
        check!(spec.protocol == ListenerProtocol::Plaintext);
        check!(spec.tls_config.is_none());
        check!(spec.sasl_mechanisms.is_none());
    }
    #[test]
    fn tls_keys_round_trip() {
        let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/krabka/broker-tls/0.crt"
key_path  = "/etc/krabka/broker-tls/0.key"
client_ca_path = "/etc/krabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
        assert!(cfg.controller_listener_protocol == Some(ListenerProtocol::Ssl));
        let tls = cfg.tls_config.expect("tls_config present");
        assert!(tls.cert_path == std::path::PathBuf::from("/etc/krabka/broker-tls/0.crt"));
        assert!(tls.client_auth == FileClientAuthMode::Required);
    }
    #[test]
    fn tls_keys_absent_round_trips() {
        let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
        assert!(cfg.controller_listener_protocol == None);
        assert!(cfg.tls_config.is_none());
    }
}
