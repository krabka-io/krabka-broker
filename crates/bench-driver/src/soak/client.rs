//! The client side of a contract run: the paced producers, the live consumer
//! groups, and the read-back after the load.
//!
//! Unlike the judge, this half does IO. It is shared by the two executors that
//! run a contract: the `soak_contract` suite of `krabka-broker`, which runs a
//! soak or a self-check against its own three-broker cluster, and
//! `krabka-soak-drill`, which runs a disaster-recovery contract against the
//! secured cluster of the qualification drill. Both send the same records,
//! keep the same ledgers and read back the same way, so the reconciler judges
//! both by the same rules.
//!
//! - [`load`]: one paced producer per path, one live consumer group per path
//!   and isolation level.
//! - [`readback`]: the read-back from the earliest offset, and the
//!   committed-offset checks.
//! - [`windows`]: steady and disturbed time, and the latency split across it.

pub mod load;
pub mod readback;
pub mod windows;

use {
    krabka_client_core::{
        sasl::SaslCredentials,
        security::{ClientSecurity, TlsConnectorConfig, TrustStore},
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// The wall clock in Unix milliseconds, the coordinate every timestamp of the
/// evidence is in.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Where every client of a run connects, and how it authenticates.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub bootstrap: String,
    /// `None` is the anonymous `PLAINTEXT` listener of the soak cluster.
    pub security: Option<ClientSecurity>,
}

impl Endpoint {
    /// The anonymous `PLAINTEXT` listener at `bootstrap`.
    #[must_use]
    pub fn plaintext(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            security: None,
        }
    }

    /// A `SASL_SSL` listener at `bootstrap` that authenticates with
    /// SCRAM-SHA-512 and verifies the broker against `ca`. The broker's
    /// certificate must name `server_name`: the client dials the address the
    /// bootstrap resolves to, so the name is not taken from the bootstrap.
    #[must_use]
    pub fn sasl_ssl(bootstrap: impl Into<String>, scram: ScramLogin) -> Self {
        let mut tls = TlsConnectorConfig::default();
        tls.trust_store = TrustStore::PemFile(scram.ca);
        tls.server_name = scram.server_name;
        Self {
            bootstrap: bootstrap.into(),
            security: Some(ClientSecurity {
                protocol: ListenerProtocol::SaslSsl,
                tls: Some(tls),
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha512,
                    username: scram.username,
                    password: scram.password,
                    delegation_token: false,
                }),
                sasl_host: None,
            }),
        }
    }
}

/// A SCRAM-SHA-512 login over TLS.
#[derive(Debug, Clone)]
pub struct ScramLogin {
    /// The PEM CA bundle the broker's certificate chains to.
    pub ca: PathBuf,
    /// The name the broker's certificate carries.
    pub server_name: String,
    pub username: String,
    pub password: String,
}

#[cfg(test)]
mod tests {
    use {
        super::{Endpoint, ScramLogin},
        assert2::check,
        krabka_client_core::{sasl::SaslCredentials, security::TrustStore},
        krabka_security::{ListenerProtocol, SaslMechanism},
    };

    #[test]
    fn a_sasl_ssl_endpoint_logs_in_with_scram_over_verified_tls() {
        let endpoint = Endpoint::sasl_ssl("broker-1:9094", ScramLogin {
            ca: "/work/tls/ca.pem".into(),
            server_name: "broker-1".into(),
            username: "soak".into(),
            password: "secret".into(),
        });
        check!(endpoint.bootstrap == "broker-1:9094");
        let security = endpoint.security.expect("a SASL_SSL endpoint is secured");
        check!(security.protocol == ListenerProtocol::SaslSsl);
        let tls = security.tls.expect("SASL_SSL carries a TLS config");
        check!(tls.trust_store == TrustStore::PemFile("/work/tls/ca.pem".into()));
        check!(tls.server_name == "broker-1");
        let Some(SaslCredentials::Scram {
            mechanism,
            username,
            password,
            delegation_token,
        }) = security.sasl
        else {
            panic!("a SASL_SSL endpoint logs in with SCRAM");
        };
        check!(
            (
                mechanism,
                username.as_str(),
                password.as_str(),
                delegation_token
            ) == (SaslMechanism::ScramSha512, "soak", "secret", false)
        );
    }

    #[test]
    fn a_plaintext_endpoint_is_anonymous() {
        let endpoint = Endpoint::plaintext("localhost:9092");
        check!(endpoint.security.is_none());
    }
}
