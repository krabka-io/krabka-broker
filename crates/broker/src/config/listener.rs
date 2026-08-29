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
}

/// Credentials the broker uses to connect *to* other brokers.
///
/// There is one variant for each SASL mechanism the inter-broker client
/// supports.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        }]
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
            }],
            inter_broker_listener_name: "INTERNAL".into(),
            inter_broker_credentials: Some(InterBrokerCredentials::OAuthBearer {
                token_path: "/unused/token".into(),
            }),
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
