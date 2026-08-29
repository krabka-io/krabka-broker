//! `SaslHandshake` (17): mechanism negotiation and the KIP-368 re-auth entry.
//!
//! This module owns the first request of every SASL connection. It answers
//! with the enabled mechanism list, moves a fresh connection to `Negotiating`,
//! and turns a handshake that arrives on an already-authenticated connection
//! into the `Reauthenticating` state that KIP-368 defines.

use krabka_protocol::owned::{
    sasl_handshake_request::SaslHandshakeRequest, sasl_handshake_response::SaslHandshakeResponse,
};
use krabka_security::SaslMechanism;

use super::state::{AuthenticatedSnapshot, ConnectionAuth, SaslExchange};
use crate::codes::{ILLEGAL_SASL_STATE, UNSUPPORTED_SASL_MECHANISM};

/// Handles `SaslHandshake` (`api_key` 17).
///
/// For a mechanism the broker advertises, this moves `auth` to `Negotiating`
/// and returns a success response that carries the enabled list. For any
/// unknown or disabled mechanism, it returns [`UNSUPPORTED_SASL_MECHANISM`]
/// (33) with the enabled list. The connection stays open, so the client can
/// retry with a supported mechanism.
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
) -> SaslHandshakeResponse {
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_string()).collect();
    let requested = SaslMechanism::from_wire(&req.mechanism);

    // In-band re-auth on an already-authenticated connection.
    // Per KIP-368, only the same mechanism is allowed; a mismatch is
    // ILLEGAL_SASL_STATE and the previous session stays in force (no
    // transition).
    if let ConnectionAuth::Authenticated {
        mechanism: current, ..
    } = auth
    {
        let current = *current;
        match requested {
            Some(m) if m == current => {
                // OK: snapshot the previous Authenticated and transition.
                let prev = std::mem::replace(auth, ConnectionAuth::Anonymous);
                let ConnectionAuth::Authenticated {
                    principal,
                    mechanism,
                    expires_at_ms,
                    authenticated_via_token: _,
                } = prev
                else {
                    unreachable!("matched Authenticated above");
                };
                let exchange = exchange_for_mechanism(m);
                *auth = ConnectionAuth::Reauthenticating {
                    previous: AuthenticatedSnapshot {
                        principal,
                        mechanism,
                        expires_at_ms,
                    },
                    exchange,
                };
                return SaslHandshakeResponse {
                    error_code: 0,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
            _ => {
                // Mechanism switch attempted — reject without transition.
                tracing::debug!(
                    requested = %req.mechanism,
                    "SaslHandshake: mechanism switch on authenticated connection (ILLEGAL_SASL_STATE)"
                );
                return SaslHandshakeResponse {
                    error_code: ILLEGAL_SASL_STATE,
                    mechanisms: enabled_names,
                    ..Default::default()
                };
            }
        }
    }

    match requested {
        Some(m) if enabled.contains(&m) => {
            let exchange = exchange_for_mechanism(m);
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange,
                // Fresh handshake; the token-fallback in
                // `handle_authenticate_scram` may populate this later
                // during SCRAM round 1.
                pending_token_expiry_ms: None,
            };
            SaslHandshakeResponse {
                error_code: 0,
                mechanisms: enabled_names,
                ..Default::default()
            }
        }
        _ => {
            tracing::debug!(
                requested = %req.mechanism,
                "SaslHandshake: unsupported mechanism"
            );
            SaslHandshakeResponse {
                error_code: UNSUPPORTED_SASL_MECHANISM,
                mechanisms: enabled_names,
                ..Default::default()
            }
        }
    }
}

/// Builds the per-mechanism `SaslExchange` initial state.
///
/// It is separate from `handle_handshake` so that the initial-auth path and
/// the re-auth path construct the state in the same way.
fn exchange_for_mechanism(m: SaslMechanism) -> SaslExchange {
    match m {
        SaslMechanism::Plain => SaslExchange::Plain,
        // SCRAM exchange is built lazily on the first SaslAuthenticate
        // round, once the username is known. Until then we sit in
        // `ScramPending`. SHA-256 and SHA-512 share the same dispatch
        // state; the mechanism is preserved on the outer `Negotiating` /
        // `Reauthenticating` variant.
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
        // The token arrives in the first SaslAuthenticate; no pre-built
        // state needed.
        SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
        // GSSAPI exchange is built lazily on the first SaslAuthenticate
        // round, once the client's AP-REQ arrives (we defer reading the
        // keytab until then). Until then we sit in `GssapiPending`.
        SaslMechanism::Gssapi => SaslExchange::GssapiPending,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_security::Principal;

    use super::*;

    #[test]
    fn handshake_oauthbearer_transitions_to_negotiating() {
        let mut auth = ConnectionAuth::Anonymous;
        let req = SaslHandshakeRequest {
            mechanism: "OAUTHBEARER".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(&req, &mut auth, &[SaslMechanism::OAuthBearer]);
        assert!(resp.error_code == 0);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::OAuthBearer,
                exchange: SaslExchange::OAuthBearer,
                ..
            }
        ));
    }

    #[test]
    fn handshake_from_authenticated_with_same_mechanism_transitions_to_reauthenticating() {
        let mut auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        let req = SaslHandshakeRequest {
            mechanism: "OAUTHBEARER".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(&req, &mut auth, &[SaslMechanism::OAuthBearer]);
        assert!(resp.error_code == 0);
        assert!(matches!(
            auth,
            ConnectionAuth::Reauthenticating {
                previous: AuthenticatedSnapshot {
                    mechanism: SaslMechanism::OAuthBearer,
                    ..
                },
                exchange: SaslExchange::OAuthBearer,
            }
        ));
    }

    #[test]
    fn handshake_from_authenticated_with_different_mechanism_rejected_with_illegal_sasl_state() {
        let mut auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        let req = SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-512".to_string(),
            ..Default::default()
        };
        let resp = handle_handshake(
            &req,
            &mut auth,
            &[SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512],
        );
        // ILLEGAL_SASL_STATE = 34 per Apache Kafka protocol.
        assert!(resp.error_code == 34);
        // The state stays Authenticated (not transitioned).
        assert!(matches!(auth, ConnectionAuth::Authenticated { .. }));
    }
}
