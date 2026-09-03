//! Inbound TLS + SASL handshake for the controller listener.
//!
//! This module is the mirror image of the outbound auth flow of
//! `network::client::InterBrokerClient`. It reuses the
//! `network::auth::handle_handshake` and `handle_authenticate_*` state
//! machines, so the controller listener and the data plane share one source
//! of truth.
//!
//! The frame helpers `read_kafka_request` and `write_response` are the
//! server-side inverse of `network::client::round_trip`. The header
//! flexibility rules match exactly:
//!   - `SaslHandshake (17)` v0+ uses a non-flexible response header, a bare
//!     `correlation_id`.
//!   - `SaslAuthenticate (36)` v2+ uses a flexible response header, a
//!     `correlation_id` and a 1-byte tagged-fields section.
//!   - The `ApiVersions (18)` response header is *always* v0 by Kafka spec.

use std::{collections::HashMap, sync::Arc};

use krabka_client_core::ClientDuplex;
use krabka_raft::{ControllerHandle, RaftConnection, RaftHandshakeError, RaftListenerHandshake};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{net::TcpStream, sync::OnceCell};
use tokio_rustls::TlsAcceptor;

mod api_versions;
mod authorization;
mod frame;
mod sasl;
#[cfg(test)]
mod test_support;

use self::sasl::run_inbound_sasl;

/// Late-bound handle to the broker's [`ControllerHandle`].
///
/// The broker constructs the handshake *before* `krabka_raft::Controller::start`
/// returns, and moves it into `ControllerConfig::handshake`, so the controller
/// is only available later. This type therefore carries an
/// `Arc<OnceCell<…>>`, and `Broker::start` calls `OnceCell::set` on it once
/// the controller is built. The SCRAM credential lookup, one round for each
/// authenticate, is the only code path that touches the cell.
pub type ControllerHandleArc = Arc<OnceCell<Arc<ControllerHandle>>>;

/// Late-bound handle to the broker's audit log.
///
/// The audit pipeline is built from the metadata source, so it does not exist
/// until after the controller listener is already accepting connections. The
/// cell is therefore filled once `Broker::start` has the log, and the SASL
/// path emits nothing for the handful of connections that may land first.
pub type AuditLogArc = Arc<OnceCell<Arc<krabka_audit::AuditLog>>>;

/// API key constants. They match the wire-protocol IDs used elsewhere.
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_API_VERSIONS: i16 = 18;

/// `SaslAuthenticate (36)` switches to flexible (v2) request *and* response
/// headers at this `api_version`. This is the KIP-482 flexible-versions
/// cutover.
const SASL_AUTHENTICATE_FLEXIBLE_VERSION: i16 = 2;

/// Per-broker handshake adapter. `Broker::start` constructs it and passes it
/// into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub gssapi: Option<krabka_security::gssapi::GssapiConfig>,
    pub oauthbearer_validator: krabka_security::OAuthBearerValidator,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
    /// Audit sink for the controller listener's own credential presentations.
    pub audit_log: AuditLogArc,
    /// Maximum Kafka handshake frame body accepted before authentication.
    pub max_frame_bytes: usize,
    /// Authorizer that gates controller RPCs after authentication (H-1).
    ///
    /// Authentication proves *who* the peer is. This authorizer enforces that
    /// the authenticated principal may drive controller and raft RPCs, that
    /// is, `CLUSTER_ACTION` on `Cluster("kafka-cluster")`. The default
    /// `AllowAllAuthorizer` allows every principal, so it does not change
    /// dev and single-node setups. `SimpleAclAuthorizer` grants super-users.
    pub authorizer: Arc<dyn crate::authorizer::Authorizer>,
}

#[async_trait::async_trait]
impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(&self, stream: TcpStream) -> Result<RaftConnection, RaftHandshakeError> {
        // Capture the peer address before the stream is consumed by TLS
        // termination — it is the `host` of the authorization request.
        let peer = stream
            .peer_addr()
            .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;

        // 1. TLS termination (if the listener protocol requires it).
        let mut stream: Box<dyn ClientDuplex> = if self.protocol.requires_tls() {
            let acceptor = self.tls_acceptor.clone().ok_or_else(|| {
                RaftHandshakeError::Tls("tls_config required for TLS controller listener".into())
            })?;
            let tls = acceptor
                .accept(stream)
                .await
                .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(stream)
        };

        // 2. SASL termination (if the listener protocol requires it).
        //    The SASL exchange authenticates the peer and yields its
        //    `Principal`; H-1 then authorizes that principal for
        //    controller RPCs before the connection is handed to the raft
        //    engine. A non-SASL listener (Plaintext is short-circuited to
        //    `None` upstream, so here that's TLS-only `Ssl`) has no
        //    authenticated identity to authorize at this layer — we do not
        //    extract an mTLS client-cert principal here — so the
        //    CLUSTER_ACTION gate is skipped for it (an unusual config).
        let mut principal = None;
        let mut authenticated_via_token = false;
        let mut cluster_alter_authorized = true;
        if self.protocol.requires_sasl() {
            let (authenticated, via_token) = run_inbound_sasl(&mut *stream, self, &peer).await?;
            self.authorize_cluster_action(&authenticated, &peer)?;
            cluster_alter_authorized = self.authorize_cluster_alter(&authenticated, &peer)?;
            principal = Some(authenticated);
            authenticated_via_token = via_token;
        }
        Ok(RaftConnection {
            stream,
            principal,
            authenticated_via_token,
            cluster_alter_authorized,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Narrow unit coverage.
    //!
    //! The richer behavioural tests live in `tests/raft_sasl.rs`, which starts
    //! a real two-broker raft cluster. Those cover the PLAIN happy path, the
    //! two SCRAM rounds, bad-credential rejection, and TLS termination. These
    //! tests check only the trait connections and the Plaintext
    //! short-circuit predicate, so that this layer catches a regression that
    //! flips `requires_*`.

    use assert2::assert;

    use super::*;

    #[test]
    fn plaintext_passthrough_short_circuits() {
        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![],
            gssapi: None,
            oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
            protocol: ListenerProtocol::Plaintext,
            controller: Arc::new(OnceCell::new()),
            audit_log: Arc::new(OnceCell::new()),
            max_frame_bytes: 4096,
            authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
        };
        // `upgrade(TcpStream)` requires a real TCP socket, so we
        // exercise the short-circuit predicates directly here. The full
        // upgrade-path is exercised end-to-end in integration tests.
        assert!(!cfg.protocol.requires_tls());
        assert!(!cfg.protocol.requires_sasl());
    }
}
