//! Per-connection SASL session state and the pre-authentication allowlist.
//!
//! This module holds the data the state machine moves through: the
//! `ConnectionAuth` states themselves, the snapshot a KIP-368 in-band
//! re-authentication takes of the session it replaces, and the per-mechanism
//! `SaslExchange` that carries an exchange between rounds. The mechanism
//! handlers in the sibling modules read and write these types, so they live
//! apart from any one mechanism.

use krabka_protocol::ApiKey;
use krabka_security::{AuthMethod, Principal, SaslMechanism, ScramServerExchange};
use krabka_verified::delegation_token::{TokenApi, TokenApiAdmission, token_api_admission};

use crate::handlers::ApiKeyCode;

// Several variants and the `principal` accessor are exercised by the PLAIN,
// SCRAM, and admin paths — keep the surface in one place.

/// Per-connection SASL state. Transitions:
/// `Anonymous` -> (`SaslHandshake`) -> `Negotiating` -> (`SaslAuthenticate` ok)
///   -> `Authenticated`.
///
/// For PLAINTEXT/SSL listeners, the dispatcher initialises the connection
/// directly to `Authenticated { principal: ANONYMOUS }`, so the pre-auth
/// gate does nothing.
#[derive(Debug)]
pub enum ConnectionAuth {
    /// PLAINTEXT / SSL listener, or pre-handshake on a SASL listener.
    Anonymous,
    /// The broker received `SaslHandshake` and waits for one or more
    /// `SaslAuthenticate` requests.
    Negotiating {
        mechanism: SaslMechanism,
        exchange: SaslExchange,
        /// KIP-48 side-channel: when the SCRAM round-1 lookup falls back to
        /// a delegation token, this field holds the token's
        /// `expiry_timestamp_ms`, so the round-2 success arm can:
        /// 1. Set `ConnectionAuth::Authenticated.expires_at_ms` (the
        ///    KIP-368 re-auth ceiling), and
        /// 2. Set `authenticated_via_token: true` (the KIP-48
        ///    token-to-token chain guard read by `CreateDelegationToken`).
        ///
        /// This field is `None` for every non-token-SCRAM negotiation:
        /// PLAIN, regular SCRAM, OAUTHBEARER, and token-SCRAM round 1 before
        /// the lookup fires. `Some(_)` is the unambiguous "token-authed
        /// session" marker.
        pending_token_expiry_ms: Option<i64>,
    },
    Authenticated {
        principal: Principal,
        /// SASL mechanism this connection authenticated with. KIP-368
        /// in-band re-auth reads it to reject a fresh `SaslHandshake` that
        /// switches mechanisms in the middle of a connection. For mTLS and
        /// anonymous connections, which use no SASL, this is
        /// `SaslMechanism::Plain` as an unused default. The in-band re-auth
        /// path cannot run there, because the listener does not accept
        /// `SaslHandshake` at all.
        mechanism: SaslMechanism,
        /// Session expiry as Unix epoch ms. `None` means no expiry and no
        /// re-auth timer, as for PLAIN, SCRAM, mTLS, and anonymous. `Some`
        /// holds the OAUTHBEARER token's `exp`. The dispatch loop closes the
        /// connection when this time passes.
        expires_at_ms: Option<i64>,
        /// KIP-48: whether this connection authenticated with a delegation
        /// token instead of a "real" principal credential. Token auth uses
        /// SCRAM-SHA-256 with the token's HMAC as the password equivalent.
        ///
        /// The delegation-token RPCs read this flag.
        /// `CreateDelegationToken` rejects token-authed callers with
        /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, because KIP-48 forbids
        /// token-creating-token chains. `DescribeDelegationToken` restricts a
        /// token-authed caller to their own owned tokens, whatever the owner
        /// filter says. Only the token-auth path sets this to `true`; every
        /// other construction site defaults it to `false`.
        authenticated_via_token: bool,
    },
    /// In-band re-authentication in progress: a `SaslHandshake` from a
    /// previously `Authenticated` OAuth connection (KIP-368).
    ///
    /// This variant holds the previous session snapshot. The post-validate
    /// equality check compares against it for the same principal name and the
    /// same mechanism. A failed re-auth also uses it to name the still-current
    /// principal in the error message.
    Reauthenticating {
        previous: AuthenticatedSnapshot,
        exchange: SaslExchange,
    },
}

/// Snapshot of an `Authenticated` connection at the moment a re-auth
/// `SaslHandshake` arrives.
///
/// During re-auth, the `SaslAuthenticate` handler reads it to enforce
/// same-mechanism and same-principal-name semantics (KIP-368).
#[derive(Debug, Clone)]
pub struct AuthenticatedSnapshot {
    pub principal: Principal,
    pub mechanism: SaslMechanism,
    pub expires_at_ms: Option<i64>,
}

/// In-flight SASL exchange.
///
/// `Plain` carries no state, because PLAIN is a single round trip.
/// `ScramPending` is the post-handshake and pre-client-first state for SCRAM;
/// the broker needs the client's `username` to build a
/// `ScramServerExchange`, so it builds the real exchange lazily. `Scram`
/// wraps the live RFC 5802 server state machine once the first client message
/// arrives.
#[derive(Debug)]
pub enum SaslExchange {
    Plain,
    ScramPending,
    /// This variant is boxed because `ScramServerExchange` is larger than
    /// clippy's 200-byte `large_enum_variant` threshold. Its
    /// `principal_override: Option<Principal>` field serves the
    /// delegation-token SCRAM fallback. Boxing keeps this rare variant out of
    /// the size of the whole enum.
    Scram(Box<ScramServerExchange>),
    /// OAUTHBEARER, post-handshake and pre-token. The bearer token arrives
    /// in the first `SaslAuthenticate`, which on success is the only one.
    OAuthBearer,
    /// OAUTHBEARER token validation failed. The broker returned the RFC 7628
    /// error JSON with `error_code = 0` and kept the connection open. It now
    /// waits for the client's single-`\x01` final message before it fails the
    /// connection with `SASL_AUTHENTICATION_FAILED`.
    OAuthBearerFailed,
    /// GSSAPI post-handshake and pre-first-token. The broker builds the
    /// acceptor, and with it the live `GssapiServerExchange`, lazily on the
    /// first `SaslAuthenticate` round, once the client's AP-REQ arrives. This
    /// mirrors the SCRAM `ScramPending` pattern: the broker does not read the
    /// keytab until a client starts a GSSAPI exchange.
    GssapiPending,
    /// GSSAPI multi-round in flight: the live RFC 4752 server state machine,
    /// from GSS context establishment to security-layer negotiation. This
    /// variant is boxed to keep the `sspi`-backed acceptor out of the size of
    /// the whole enum.
    Gssapi(Box<krabka_security::gssapi::server::GssapiServerExchange>),
}

impl ConnectionAuth {
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        if let Self::Authenticated { principal, .. } = self {
            Some(principal)
        } else {
            None
        }
    }

    /// Apply the shared KIP-48 API admission policy to this connection.
    #[must_use]
    pub(crate) fn token_api_admission(&self, api: TokenApi) -> TokenApiAdmission {
        let Self::Authenticated {
            principal,
            authenticated_via_token,
            ..
        } = self
        else {
            return TokenApiAdmission::Reject;
        };

        token_api_admission(
            principal.auth_method != AuthMethod::Anonymous,
            *authenticated_via_token,
            api,
        )
    }

    /// Whether the broker may serve `api_key` in the current auth state.
    /// - `Anonymous` / `Negotiating`: allow the pre-auth allowlist
    ///   (ApiVersions=18, SaslHandshake=17, SaslAuthenticate=36).
    /// - `Reauthenticating`: allow only `SaslAuthenticate=36`. Any other
    ///   request during in-band re-auth is a protocol violation and the
    ///   dispatch layer closes the connection (KIP-368).
    /// - `Authenticated`: allow everything.
    #[must_use]
    pub fn allows_request(&self, api_key: ApiKeyCode) -> bool {
        match self {
            Self::Anonymous | Self::Negotiating { .. } => is_pre_auth_allowed(api_key),
            Self::Reauthenticating { .. } => api_key == ApiKey::SaslAuthenticate as i16,
            Self::Authenticated { .. } => true,
        }
    }
}

/// Pre-auth allowlist: `api_key`s clients may send before completing SASL.
///
/// Mirrors Apache Kafka's pre-auth allowlist. Before it authenticates, a
/// client must be able to negotiate the mechanism (`SaslHandshake` = 17), run
/// the SASL exchange (`SaslAuthenticate` = 36), and discover the supported
/// APIs (`ApiVersions` = 18). The broker rejects everything else with
/// `ILLEGAL_SASL_STATE` (34) and closes the connection.
#[must_use]
pub fn is_pre_auth_allowed(api_key: ApiKeyCode) -> bool {
    matches!(
        ApiKey::from_i16(api_key),
        Some(ApiKey::SaslHandshake | ApiKey::SaslAuthenticate | ApiKey::ApiVersions)
    )
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn pre_auth_allowlist_accepts_sasl_apis_and_rejects_data_plane() {
        let cases = [
            (17, true),  // SaslHandshake
            (36, true),  // SaslAuthenticate
            (18, true),  // ApiVersions
            (0, false),  // Produce
            (1, false),  // Fetch
            (3, false),  // Metadata
            (19, false), // CreateTopics
        ];
        for (api_key, allowed) in cases {
            assert!(is_pre_auth_allowed(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn unauthenticated_states_have_no_principal() {
        let cases = [
            ("anonymous", ConnectionAuth::Anonymous),
            (
                "negotiating_plain",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::Plain,
                    exchange: SaslExchange::Plain,
                    pending_token_expiry_ms: None,
                },
            ),
            (
                "negotiating_scram_pending",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::ScramSha512,
                    exchange: SaslExchange::ScramPending,
                    pending_token_expiry_ms: None,
                },
            ),
        ];
        for (name, a) in cases {
            assert!(!a.is_authenticated(), "{name}");
            assert!(a.principal().is_none(), "{name}");
        }
    }

    #[test]
    fn token_api_admission_maps_connection_auth_states_exactly() {
        use krabka_security::AuthMethod;

        let apis = [
            TokenApi::Create,
            TokenApi::Renew,
            TokenApi::Expire,
            TokenApi::Describe,
        ];
        let authenticated =
            |auth_method, mechanism, authenticated_via_token| ConnectionAuth::Authenticated {
                principal: Principal {
                    name: "alice".into(),
                    auth_method,
                    groups: vec![],
                },
                mechanism,
                expires_at_ms: None,
                authenticated_via_token,
            };

        let rejected = [
            ("anonymous state", ConnectionAuth::Anonymous),
            (
                "negotiating state",
                ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::Plain,
                    exchange: SaslExchange::Plain,
                    pending_token_expiry_ms: None,
                },
            ),
            (
                "reauthenticating state",
                ConnectionAuth::Reauthenticating {
                    previous: AuthenticatedSnapshot {
                        principal: Principal {
                            name: "alice".into(),
                            auth_method: AuthMethod::SaslOAuthBearer,
                            groups: vec![],
                        },
                        mechanism: SaslMechanism::OAuthBearer,
                        expires_at_ms: Some(2_000_000),
                    },
                    exchange: SaslExchange::OAuthBearer,
                },
            ),
            (
                "authenticated anonymous principal",
                authenticated(AuthMethod::Anonymous, SaslMechanism::Plain, false),
            ),
        ];
        for (state, auth) in rejected {
            for api in apis {
                check!(
                    auth.token_api_admission(api) == TokenApiAdmission::Reject,
                    "{state}: {api:?}"
                );
            }
        }

        for (method, mechanism) in [
            (AuthMethod::SaslPlain, SaslMechanism::Plain),
            (AuthMethod::SaslScramSha256, SaslMechanism::ScramSha256),
            (AuthMethod::SaslScramSha512, SaslMechanism::ScramSha512),
            (AuthMethod::SaslOAuthBearer, SaslMechanism::OAuthBearer),
            (AuthMethod::SaslGssapi, SaslMechanism::Gssapi),
            (AuthMethod::MTls, SaslMechanism::Plain),
        ] {
            let auth = authenticated(method, mechanism, false);
            for api in apis {
                check!(auth.token_api_admission(api) == TokenApiAdmission::Allow);
            }
        }

        let token_auth = authenticated(
            AuthMethod::SaslScramSha256,
            SaslMechanism::ScramSha256,
            true,
        );
        for (api, expected) in [
            (TokenApi::Create, TokenApiAdmission::Reject),
            (TokenApi::Renew, TokenApiAdmission::Reject),
            (TokenApi::Expire, TokenApiAdmission::Reject),
            (TokenApi::Describe, TokenApiAdmission::Allow),
        ] {
            check!(token_auth.token_api_admission(api) == expected, "{api:?}");
        }
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                auth_method: krabka_security::AuthMethod::SaslScramSha512,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha512,
            expires_at_ms: None,
            authenticated_via_token: false,
        };
        assert!(a.is_authenticated());
        let p = a.principal().expect("principal");
        assert!(p.name == "alice");
        assert!(p.auth_method == krabka_security::AuthMethod::SaslScramSha512);
    }

    // KIP-368: in-band re-auth tests.

    #[test]
    fn authenticated_state_carries_mechanism_and_expires_at_ms() {
        let auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".to_string(),
                auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: SaslMechanism::OAuthBearer,
            expires_at_ms: Some(2_000_000),
            authenticated_via_token: false,
        };
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token: _,
            } => {
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::OAuthBearer);
                check!(expires_at_ms == Some(2_000_000));
            }
            _ => panic!("expected Authenticated"),
        }
    }

    #[test]
    fn allows_request_during_reauthenticating_only_sasl_authenticate() {
        let auth = ConnectionAuth::Reauthenticating {
            previous: AuthenticatedSnapshot {
                principal: Principal {
                    name: "alice".to_string(),
                    auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                    groups: vec![],
                },
                mechanism: SaslMechanism::OAuthBearer,
                expires_at_ms: Some(2_000_000),
            },
            exchange: SaslExchange::OAuthBearer,
        };
        let cases = [
            (36, true),  // SaslAuthenticate
            (17, false), // SaslHandshake
            (18, false), // ApiVersions
            (3, false),  // Metadata
        ];
        for (api_key, allowed) in cases {
            assert!(auth.allows_request(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn allows_request_anonymous_uses_pre_auth_allowlist() {
        let auth = ConnectionAuth::Anonymous;
        let cases = [(17, true), (36, true), (18, true), (0, false), (3, false)];
        for (api_key, allowed) in cases {
            assert!(auth.allows_request(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn allows_request_authenticated_allows_all() {
        let auth = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                auth_method: krabka_security::AuthMethod::SaslScramSha512,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha512,
            expires_at_ms: None,
            authenticated_via_token: false,
        };
        for api_key in [0, 3, 17, 36] {
            assert!(auth.allows_request(api_key), "api key {api_key}");
        }
    }
}
