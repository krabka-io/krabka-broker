//! Named listeners and inter-broker credentials: what the broker binds and
//! advertises, what it authenticates outbound connections with, and the
//! check that the two agree.

use std::{net::SocketAddr, path::PathBuf};

use krabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

use crate::{BrokerError, config::BrokerConfig};

/// A single named listener: the port the broker binds and the address it
/// gives to clients.
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    /// Listener name (e.g. `"PLAINTEXT"`, `"SSL"`, `"SASL_SSL"`).
    pub name: String,
    /// Local address to bind.
    pub bind_addr: SocketAddr,
    /// `host:port` advertised to clients in `Metadata` responses.
    pub advertised: String,
    /// Wire protocol (Plaintext / Ssl / `SaslPlaintext` / `SaslSsl`).
    pub protocol: ListenerProtocol,
    /// Per-listener TLS material. When `Some`, overrides the top-level
    /// `BrokerConfig::tls_config` for this listener's accept loop.
    pub tls_config: Option<TlsConfig>,
    /// SASL mechanisms enabled on this listener. When `Some`, overrides
    /// the top-level `BrokerConfig::enabled_sasl_mechanisms`.
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
    /// KIP-371 `ssl.principal.mapping.rules` for this listener, parsed. It
    /// rewrites the Subject DN of an mTLS peer certificate into the principal
    /// name ACLs are written against. The default is an empty rule list, which
    /// passes the DN through exactly as Kafka's `DEFAULT` rule does.
    pub principal_mapper: crate::network::auth::SslPrincipalMapper,
}

/// Credentials the broker uses to connect *to* other brokers.
///
/// There is one variant for each SASL mechanism the inter-broker client
/// supports.
#[derive(Clone, PartialEq, Eq)]
pub enum InterBrokerCredentials {
    /// SASL/PLAIN: `\0username\0password`.
    Plain { username: String, password: String },
    /// SASL/SCRAM (SHA-256 or SHA-512).
    Scram {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    /// SASL/GSSAPI: authenticate as `client_principal` with the long-term key
    /// in `keytab_path`. This mechanism uses no password. `service_name` is
    /// the target broker's SPN primary; the client combines it with the
    /// dialed host into `service_name/host` at connect time. `kdc_url` is the
    /// KDC endpoint, for example `tcp://kdc:88`.
    Gssapi {
        keytab_path: PathBuf,
        client_principal: String,
        service_name: String,
        kdc_url: String,
    },
    /// SASL/OAUTHBEARER. The token file is read on every new outbound
    /// connection so credential rotation does not require a broker restart.
    OAuthBearer { token_path: PathBuf },
}

/// Hand-written so the `Plain` and `Scram` passwords render as `<redacted>`;
/// `BrokerConfig` derives `Debug`, and this enum sits inside it.
impl std::fmt::Debug for InterBrokerCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain { username, .. } => f
                .debug_struct("Plain")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Scram {
                mechanism,
                username,
                ..
            } => f
                .debug_struct("Scram")
                .field("mechanism", mechanism)
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Gssapi {
                keytab_path,
                client_principal,
                service_name,
                kdc_url,
            } => f
                .debug_struct("Gssapi")
                .field("keytab_path", keytab_path)
                .field("client_principal", client_principal)
                .field("service_name", service_name)
                .field("kdc_url", kdc_url)
                .finish(),
            Self::OAuthBearer { token_path } => f
                .debug_struct("OAuthBearer")
                .field("token_path", token_path)
                .finish(),
        }
    }
}

impl InterBrokerCredentials {
    /// The SASL mechanism this credential set authenticates with.
    #[must_use]
    pub fn mechanism(&self) -> SaslMechanism {
        match self {
            Self::Plain { .. } => SaslMechanism::Plain,
            Self::Scram { mechanism, .. } => *mechanism,
            Self::Gssapi { .. } => SaslMechanism::Gssapi,
            Self::OAuthBearer { .. } => SaslMechanism::OAuthBearer,
        }
    }
}

impl BrokerConfig {
    /// Returns the effective listener list.
    ///
    /// When [`listeners`][Self::listeners] is empty (the default), this
    /// method builds a single `PLAINTEXT` listener from the legacy
    /// `listen_addr` and `advertised_listener` fields.
    #[must_use]
    pub fn effective_listeners(&self) -> Vec<ListenerSpec> {
        if !self.listeners.is_empty() {
            return self.listeners.clone();
        }
        vec![ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: self.listen_addr,
            advertised: self.advertised_listener.clone(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: crate::SslPrincipalMapper::default(),
        }]
    }

    /// The broker-wide idle window in force: what the operator configured, or
    /// Kafka's `connections.max.idle.ms` default when they configured
    /// nothing. Every path that only wants the window reads it here, so that
    /// `connections_max_idle` can keep carrying the provenance
    /// `DescribeConfigs` reports.
    #[must_use]
    pub fn effective_connections_max_idle(&self) -> krabka_units::Time {
        self.connections_max_idle
            .unwrap_or(crate::config::DEFAULT_CONNECTIONS_MAX_IDLE)
    }

    /// The idle window a connection accepted on `listener_name` is held to.
    ///
    /// A per-listener override wins over the broker-wide
    /// `connections_max_idle`; names are matched without ASCII case, because
    /// Kafka spells the override key with a lowercased listener name
    /// (`listener.name.plaintext.connections.max.idle.ms`) while the listener
    /// itself is conventionally uppercase.
    ///
    /// `None` means this listener expires no connection. A non-positive
    /// configured value asks for that, the way Kafka's `Selector` arms no
    /// `IdleExpiryManager` when `connections.max.idle.ms` is not positive.
    #[must_use]
    pub fn connections_max_idle_for(&self, listener_name: &str) -> Option<std::time::Duration> {
        use krabka_units::convert::TimeExt as _;

        let configured = self
            .connections_max_idle_overrides
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(listener_name))
            .map_or_else(
                || self.effective_connections_max_idle(),
                |(_, value)| *value,
            );
        (configured.millis_i64() > 0).then(|| configured.to_std())
    }

    /// The KIP-368 re-authentication window a SASL session on `listener_name`
    /// is held to, or `None` when that listener expires no session.
    ///
    /// A per-listener override wins over the broker-wide
    /// `connections_max_reauth`; names are matched without ASCII case, for the
    /// same reason [`BrokerConfig::connections_max_idle_for`] does.
    #[must_use]
    pub fn connections_max_reauth_for(&self, listener_name: &str) -> Option<krabka_units::Time> {
        use krabka_units::convert::TimeExt as _;

        self.connections_max_reauth_overrides
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(listener_name))
            .map(|(_, value)| *value)
            .or(self.connections_max_reauth)
            .filter(|value| value.millis_i64() > 0)
    }

    pub(super) fn validate_outbound_sasl(
        &self,
        inter_broker: &ListenerSpec,
    ) -> Result<(), BrokerError> {
        let Some(credentials) = self.inter_broker_credentials.as_ref() else {
            return Ok(());
        };
        let mechanism = credentials.mechanism();

        if inter_broker.protocol.requires_sasl() {
            let enabled = inter_broker
                .sasl_mechanisms
                .as_deref()
                .unwrap_or(&self.enabled_sasl_mechanisms);
            if !enabled.contains(&mechanism) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "inter-broker credential mechanism {} is not enabled on listener {}",
                    mechanism.wire_name(),
                    inter_broker.name
                )));
            }
        }
        if self.controller_listener_protocol.requires_sasl()
            && !self.enabled_sasl_mechanisms.contains(&mechanism)
        {
            return Err(BrokerError::InvalidRuntimeConfig(format!(
                "inter-broker credential mechanism {} is not enabled on the controller listener",
                mechanism.wire_name()
            )));
        }
        if let InterBrokerCredentials::OAuthBearer { token_path } = credentials {
            let token = std::fs::read(token_path).map_err(|error| {
                BrokerError::InvalidRuntimeConfig(format!(
                    "cannot read inter-broker OAUTHBEARER token {}: {error}",
                    token_path.display()
                ))
            })?;
            let token = token.trim_ascii();
            if token.is_empty() || token.contains(&b'\x01') {
                return Err(BrokerError::InvalidRuntimeConfig(
                    "inter-broker OAUTHBEARER token must be non-empty and contain no RFC 7628 separator"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{BrokerError as BrokerStartError, config::test_support::base};

    #[test]
    fn accepts_distinct_listener_bind_addresses() {
        let c = base();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_bind_collision() {
        let mut c = base();
        c.listeners[1].bind_addr = c.listeners[0].bind_addr;
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::ListenerConflict { .. })
        ));
    }

    #[test]
    fn rejects_missing_inter_broker_listener() {
        let mut c = base();
        c.inter_broker_listener_name = "NONESUCH".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::InvalidInterBrokerListener { .. })
        ));
    }

    #[test]
    fn rejects_sasl_listener_without_mechanisms() {
        let mut c = base();
        c.enabled_sasl_mechanisms.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn legacy_default_passes() {
        let c = BrokerConfig::default();
        c.validate().expect("legacy default must validate");
    }

    #[test]
    fn defaults_listen_on_localhost_9092() {
        let c = BrokerConfig::default();
        assert!(c.listen_addr.port() == 9092);
        assert!(c.broker_id == 1);
    }

    #[test]
    fn rejects_controller_tls_without_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Ssl,
            tls_config: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::Tls(_))));
    }

    #[test]
    fn rejects_controller_sasl_without_mechanisms() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::SaslListenerNoMechanisms { .. })
        ));
    }

    #[test]
    fn outbound_oauthbearer_credentials_validate_for_data_and_controller_listeners() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "header.payload.\n").unwrap();
        let credentials = Some(InterBrokerCredentials::OAuthBearer { token_path });
        let data_listener = ListenerSpec {
            name: "OAUTH".into(),
            bind_addr: "127.0.0.1:9094".parse().unwrap(),
            advertised: "broker:9094".into(),
            protocol: ListenerProtocol::SaslPlaintext,
            tls_config: None,
            sasl_mechanisms: Some(vec![SaslMechanism::OAuthBearer]),
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        let data = BrokerConfig {
            listeners: vec![data_listener],
            inter_broker_listener_name: "OAUTH".into(),
            inter_broker_credentials: credentials.clone(),
            ..BrokerConfig::default()
        };
        assert!(data.validate().is_ok());

        let controller = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::OAuthBearer],
            inter_broker_credentials: credentials,
            ..BrokerConfig::default()
        };
        assert!(controller.validate().is_ok());
    }

    #[test]
    fn rejects_outbound_credential_mechanism_not_enabled_on_sasl_targets() {
        let data = BrokerConfig {
            listeners: vec![ListenerSpec {
                name: "INTERNAL".into(),
                bind_addr: "127.0.0.1:9094".parse().unwrap(),
                advertised: "broker:9094".into(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: Some(vec![SaslMechanism::Plain]),
                principal_mapper: crate::SslPrincipalMapper::default(),
            }],
            inter_broker_listener_name: "INTERNAL".into(),
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer {
                token_path: "/unused/token".into(),
            }),
            plain_credentials: [("admin".to_string(), "admin-secret".to_string())]
                .into_iter()
                .collect(),
            ..BrokerConfig::default()
        };
        let error = data
            .validate()
            .expect_err("data-listener mechanism mismatch is rejected");
        assert!(
            error
                .to_string()
                .contains("not enabled on listener INTERNAL")
        );

        let controller = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer {
                token_path: "/unused/token".into(),
            }),
            ..BrokerConfig::default()
        };
        let error = controller
            .validate()
            .expect_err("controller-listener mechanism mismatch is rejected");
        assert!(
            error
                .to_string()
                .contains("not enabled on the controller listener")
        );
    }

    #[test]
    fn rejects_empty_outbound_oauthbearer_token() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "\n").unwrap();
        let c = BrokerConfig {
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer { token_path }),
            ..BrokerConfig::default()
        };
        let error = c.validate().expect_err("empty OAuth token is rejected");
        assert!(error.to_string().contains("token must be non-empty"));
    }

    #[test]
    fn legacy_default_still_passes() {
        BrokerConfig::default()
            .validate()
            .expect("legacy default validates");
    }

    #[test]
    fn per_listener_sasl_mechanisms_satisfy_validation_without_broker_default() {
        let tls = TlsConfig {
            cert_chain_path: std::path::PathBuf::from("/tls/c"),
            private_key_path: std::path::PathBuf::from("/tls/k"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: krabka_security::ClientAuthMode::Disabled,
        };
        let listener = ListenerSpec {
            name: "scram".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "broker-0:9094".into(),
            protocol: ListenerProtocol::SaslSsl,
            tls_config: Some(tls.clone()),
            sasl_mechanisms: Some(vec![SaslMechanism::ScramSha512]),
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        let c = BrokerConfig {
            listeners: vec![listener],
            inter_broker_listener_name: "scram".into(),
            enabled_sasl_mechanisms: vec![],
            tls_config: Some(tls),
            controller_listener_protocol: ListenerProtocol::Plaintext,
            ..BrokerConfig::default()
        };
        c.validate()
            .expect("per-listener mechanisms satisfy SASL validation");
    }

    #[test]
    fn rejects_gssapi_mechanism_without_gssapi_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Plaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::Gssapi],
            gssapi: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::GssapiConfigMissing)
        ));
    }
}

#[cfg(test)]
mod connections_max_idle_tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt as _, millis, secs};

    use crate::config::{BrokerConfig, DEFAULT_CONNECTIONS_MAX_IDLE};

    /// A broker-wide window of `idle` with the named per-listener overrides.
    fn config(idle: Time, overrides: &[(&str, Time)]) -> BrokerConfig {
        BrokerConfig {
            connections_max_idle: Some(idle),
            connections_max_idle_overrides: overrides
                .iter()
                .map(|(name, value)| ((*name).to_string(), *value))
                .collect(),
            ..BrokerConfig::default()
        }
    }

    #[test]
    fn an_unnamed_listener_gets_kafkas_ten_minute_default() {
        // Kafka spells the default 600000, which is the ten minutes below.
        assert!(DEFAULT_CONNECTIONS_MAX_IDLE.millis_i64() == 600_000);
        assert!(
            BrokerConfig::default().connections_max_idle_for("PLAINTEXT")
                == Some(std::time::Duration::from_mins(10))
        );
    }

    #[test]
    fn a_listener_override_wins_over_the_broker_wide_window() {
        let config = config(secs(600), &[("EXTERNAL", secs(20))]);
        assert!(
            config.connections_max_idle_for("EXTERNAL") == Some(std::time::Duration::from_secs(20))
        );
        assert!(
            config.connections_max_idle_for("INTERNAL") == Some(std::time::Duration::from_mins(10))
        );
    }

    /// Kafka lowercases the listener name in `listener.name.<name>.…`, so an
    /// override written the Kafka way still has to find an uppercase listener.
    #[test]
    fn an_override_matches_its_listener_without_ascii_case() {
        let config = config(secs(600), &[("external", secs(20))]);
        assert!(
            config.connections_max_idle_for("EXTERNAL") == Some(std::time::Duration::from_secs(20))
        );
    }

    /// Kafka arms no idle-expiry manager for a non-positive
    /// `connections.max.idle.ms`, so neither does krabka -- broker-wide and
    /// per-listener alike.
    #[test]
    fn a_non_positive_window_expires_nothing() {
        assert!(
            config(millis(0), &[])
                .connections_max_idle_for("PLAINTEXT")
                .is_none()
        );
        assert!(
            config(Time::from_millis(-1), &[])
                .connections_max_idle_for("PLAINTEXT")
                .is_none()
        );
        assert!(
            config(secs(600), &[("EXTERNAL", millis(0))])
                .connections_max_idle_for("EXTERNAL")
                .is_none()
        );
    }
}
