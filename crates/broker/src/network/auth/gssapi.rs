//! SASL/GSSAPI (Kerberos, RFC 4752) over `SaslAuthenticate`.
//!
//! GSSAPI is the only multi-round mechanism the broker serves, and it is also
//! the only one that maps its authenticated name through `auth_to_local`, so
//! the context establishment, the security-layer negotiation and the principal
//! mapping sit together here.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::{Principal, SaslMechanism};
use krabka_units::{ByteSize, Time, kibibytes};

use super::{
    response::fail_authenticate,
    state::{ConnectionAuth, SaslExchange, begin_reauth, finish_reauth, session_expiry},
};

/// RFC 4752 server "maximum message size" advertised in the auth-only
/// security-layer offer. 64 KiB matches the JVM broker's default SASL receive
/// buffer; with confidentiality/integrity disabled it only bounds the size of
/// the (empty) wrapped payloads, so the exact value is not load-bearing.
const GSSAPI_MAX_RECV: ByteSize = kibibytes(64);

/// SASL/GSSAPI (Kerberos, RFC 4752) `SaslAuthenticate` handler.
///
/// This handler runs several rounds over Kafka's `SaslAuthenticate`
/// (`api_key` 36) wire envelope. The opaque GSS/SASL tokens travel in
/// `auth_bytes` in both directions.
///
/// Round 1 (client AP-REQ):
///   - `auth_bytes` is the GSS initial context token (AP-REQ). An exchange
///     has now started, so the handler builds the `sspi`-backed acceptor from
///     the broker's keytab, feeds the token to a fresh
///     [`GssapiServerExchange`], and emits the server's context token
///     (AP-REP) as the response `auth_bytes`. `auth` moves from
///     `Negotiating { exchange: GssapiPending }` to
///     `Negotiating { exchange: Gssapi(..) }`, still unauthenticated.
///
/// Middle round or rounds (security-layer negotiation, RFC 4752):
///   - the server emits its GSS-wrapped auth-only offer, and the client
///     replies with its GSS-wrapped choice. Each `ServerStep::Challenge`
///     becomes a success response that carries the next token. `auth` stays
///     `Negotiating`.
///
/// Final round (client layer choice):
///   - the exchange yields `ServerStep::Done { principal }`. The handler maps
///     the raw Kerberos principal through `auth_to_local`, moves to
///     `Authenticated`, and replies with empty `auth_bytes` and
///     `error_code = 0`.
///
/// Any GSS or codec error returns `SASL_AUTHENTICATION_FAILED` (58), and the
/// dispatcher closes the connection.
///
/// A Kerberos context carries no broker-visible lifetime, so `max_reauth` —
/// the listener's `connections.max.reauth.ms` — is what bounds the session
/// (KIP-368). In-band re-authentication runs the same rounds and must land on
/// the same principal.
pub fn handle_authenticate_gssapi(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    config: &krabka_security::gssapi::GssapiConfig,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    let Some(previous) = begin_reauth(auth) else {
        return authenticate_gssapi(req, auth, config, max_reauth);
    };
    let resp = authenticate_gssapi(req, auth, config, max_reauth);
    finish_reauth(auth, previous, resp)
}

fn authenticate_gssapi(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    config: &krabka_security::gssapi::GssapiConfig,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    use krabka_security::gssapi::server::{GssapiServerExchange, ServerStep};

    // Round 1: still `GssapiPending` — build the acceptor-backed exchange now
    // that the first client token (AP-REQ) has arrived.
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::GssapiPending,
        mechanism,
        pending_token_expiry_ms: _,
    } = auth
    {
        let mech = *mechanism;
        let keytab = config.keytab_path.to_string_lossy();
        let acceptor = match krabka_security::gssapi::provider::SspiAcceptor::new(
            &keytab,
            &config.service_name,
            config.max_time_skew,
        ) {
            Ok(a) => a,
            Err(e) => return fail_authenticate(&format!("GSSAPI acceptor init failed: {e}")),
        };
        let exchange = GssapiServerExchange::new(Box::new(acceptor), GSSAPI_MAX_RECV);
        let step = match exchange.step(&req.auth_bytes) {
            Ok(s) => s,
            Err(e) => return fail_authenticate(&format!("GSSAPI accept failed: {e}")),
        };
        return match step {
            ServerStep::Challenge(token, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism: mech,
                    exchange: SaslExchange::Gssapi(Box::new(next)),
                    pending_token_expiry_ms: None,
                };
                gssapi_challenge_response(token)
            }
            // GSSAPI always negotiates the security layer after context
            // establishment, so round 1 never completes the exchange.
            ServerStep::Done { principal } => {
                finish_gssapi(&principal, mech, config, auth, max_reauth)
            }
        };
    }

    // Subsequent rounds: the exchange already exists. `step` consumes it, so
    // extract it by value (mirroring `handle_handshake`'s re-auth snapshot
    // swap) before stepping it with the client's token.
    if let ConnectionAuth::Negotiating {
        exchange: SaslExchange::Gssapi(_),
        ..
    } = auth
    {
        let ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::Gssapi(exchange),
            pending_token_expiry_ms: _,
        } = std::mem::replace(auth, ConnectionAuth::Anonymous)
        else {
            unreachable!("matched Negotiating{{Gssapi}} above");
        };
        let step = match exchange.step(&req.auth_bytes) {
            Ok(s) => s,
            Err(e) => return fail_authenticate(&format!("GSSAPI step failed: {e}")),
        };
        return match step {
            ServerStep::Challenge(token, next) => {
                *auth = ConnectionAuth::Negotiating {
                    mechanism,
                    exchange: SaslExchange::Gssapi(Box::new(next)),
                    pending_token_expiry_ms: None,
                };
                gssapi_challenge_response(token)
            }
            ServerStep::Done { principal } => {
                finish_gssapi(&principal, mechanism, config, auth, max_reauth)
            }
        };
    }

    fail_authenticate("not in GSSAPI negotiation")
}

/// Handles a non-terminal GSSAPI round. It returns the next token to the
/// client with `error_code = 0`. The connection stays open and `auth` stays
/// `Negotiating`.
fn gssapi_challenge_response(token: Vec<u8>) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::from(token),
        session_lifetime_ms: 0,
        ..Default::default()
    }
}

/// Maps the authenticated Kerberos principal through `auth_to_local` and, on
/// success, moves `auth` to `Authenticated`.
fn finish_gssapi(
    raw_principal: &str,
    mech: SaslMechanism,
    config: &krabka_security::gssapi::GssapiConfig,
    auth: &mut ConnectionAuth,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    let short = match map_gssapi_principal(raw_principal, config) {
        Ok(s) => s,
        Err(e) => return fail_authenticate(&format!("GSSAPI principal mapping failed: {e}")),
    };
    let (expires_at_ms, session_lifetime_ms) =
        session_expiry(crate::time_util::now_ms(), None, max_reauth);
    *auth = ConnectionAuth::Authenticated {
        principal: Principal {
            name: short,
            auth_method: krabka_security::AuthMethod::SaslGssapi,
            groups: vec![],
        },
        mechanism: mech,
        // The KDC enforces the ticket lifetime at context-establishment time
        // and the broker never sees it, so the only ceiling on a GSSAPI
        // session is the listener's `connections.max.reauth.ms`. KIP-368
        // re-auth rides the same SaslHandshake path as the other mechanisms.
        expires_at_ms,
        authenticated_via_token: false,
    };
    SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms,
        ..Default::default()
    }
}

/// Applies the configured `auth_to_local` rules to a raw Kerberos principal.
///
/// `sspi` recovers the principal in lower case, for example
/// `alice@crabka.test`. This function canonicalises the realm back to upper
/// case before it matches, because Kerberos realms are conventionally upper
/// case, and because both the configured default realm and the
/// `auth_to_local` rules are written in the upper-case form. When no default
/// realm is configured, the function falls back to the principal's own realm.
/// A single-component principal in its own realm then maps to its primary
/// through the implicit `DEFAULT` rule.
fn map_gssapi_principal(
    raw: &str,
    config: &krabka_security::gssapi::GssapiConfig,
) -> Result<String, krabka_security::gssapi::name::NameError> {
    let (head, realm_raw) = raw.rsplit_once('@').unwrap_or((raw, ""));
    let realm = realm_raw.to_uppercase();
    let components: Vec<&str> = head.split('/').collect();
    let default_realm = config.realm.as_deref().unwrap_or(&realm);
    krabka_security::gssapi::name::apply(
        &config.principal_to_local_rules,
        &realm,
        &components,
        default_realm,
    )
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::network::auth::test_support::{
        assert_failed_authenticate_response, assert_success_authenticate_response,
    };

    #[test]
    fn gssapi_challenge_response_carries_token_and_zero_lifetime() {
        let resp = gssapi_challenge_response(vec![1, 2, 3, 4]);
        assert_success_authenticate_response(&resp, &[1, 2, 3, 4], 0);
    }

    #[test]
    fn finish_gssapi_maps_principal_and_returns_empty_success() {
        let config = krabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![krabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
            max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };

        let resp = finish_gssapi(
            "alice@crabka.test",
            SaslMechanism::Gssapi,
            &config,
            &mut auth,
            None,
        );

        assert_success_authenticate_response(&resp, b"", 0);
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token,
            } => {
                check!(principal.name.as_str() == "alice");
                check!(principal.auth_method == krabka_security::AuthMethod::SaslGssapi);
                check!(mechanism == SaslMechanism::Gssapi);
                check!(expires_at_ms == None);
                check!(!authenticated_via_token);
            }
            _ => panic!("expected GSSAPI authenticated state"),
        }
    }

    #[test]
    fn finish_gssapi_mapping_error_returns_auth_failure() {
        let config = krabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![krabka_security::gssapi::name::Rule::Default],
            realm: Some("OTHER.REALM".to_string()),
            kdc: None,
            max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };

        let resp = finish_gssapi(
            "alice@crabka.test",
            SaslMechanism::Gssapi,
            &config,
            &mut auth,
            None,
        );

        assert_failed_authenticate_response(&resp);
        assert!(matches!(auth, ConnectionAuth::Negotiating { .. }));
    }

    #[test]
    fn handle_authenticate_gssapi_round1_bad_keytab_fails_and_leaves_state_untouched() {
        let config = krabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/nonexistent.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![krabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
            max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::GssapiPending,
            pending_token_expiry_ms: None,
        };
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from_static(b"AP-REQ"),
            ..Default::default()
        };

        let resp = handle_authenticate_gssapi(&req, &mut auth, &config, None);

        assert_failed_authenticate_response(&resp);
        assert!(matches!(
            auth,
            ConnectionAuth::Negotiating {
                exchange: SaslExchange::GssapiPending,
                ..
            }
        ));
    }

    /// Establishes the GSS context on the first token with no trailing
    /// AP-REP, so one `step()` call reaches `AwaitingChoice` directly. This
    /// mirrors `krabka-security`'s own `gssapi::server` unit tests.
    struct FakeAcceptor;

    impl krabka_security::gssapi::GssAcceptor for FakeAcceptor {
        fn accept(
            &mut self,
            _client_token: &[u8],
        ) -> Result<krabka_security::gssapi::AcceptStep, krabka_security::gssapi::GssError>
        {
            Ok(krabka_security::gssapi::AcceptStep::Established(None))
        }
        fn wrap(
            &self,
            plaintext: &[u8],
            _confidential: bool,
        ) -> Result<Vec<u8>, krabka_security::gssapi::GssError> {
            Ok(plaintext.to_vec())
        }
        fn unwrap(&self, token: &[u8]) -> Result<Vec<u8>, krabka_security::gssapi::GssError> {
            Ok(token.to_vec())
        }
        fn src_principal(&self) -> Result<String, krabka_security::gssapi::GssError> {
            Ok("alice@CRABKA.TEST".to_string())
        }
    }

    #[test]
    fn handle_authenticate_gssapi_subsequent_round_completes_and_authenticates() {
        use krabka_security::gssapi::server::{GssapiServerExchange, ServerStep};

        let config = krabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![krabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
            max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
        };

        // Drive the exchange to `AwaitingChoice` up front (mirroring round
        // 1's work), so this test targets `handle_authenticate_gssapi`'s
        // *subsequent round* branch specifically.
        let exchange = GssapiServerExchange::new(Box::new(FakeAcceptor), kibibytes(64));
        let exchange = match exchange.step(b"AP-REQ").expect("round 1 step") {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };

        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Gssapi,
            exchange: SaslExchange::Gssapi(Box::new(exchange)),
            pending_token_expiry_ms: None,
        };

        let mut choice = vec![0x01u8, 0x00, 0x10, 0x00];
        choice.extend_from_slice(b"alice");
        let req = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(choice),
            ..Default::default()
        };

        let resp = handle_authenticate_gssapi(&req, &mut auth, &config, None);

        assert_success_authenticate_response(&resp, b"", 0);
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                ..
            } => {
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::Gssapi);
            }
            _ => panic!("expected GSSAPI authenticated state"),
        }
    }

    #[test]
    fn map_gssapi_principal_uppercases_realm_before_default_rule() {
        let config = krabka_security::gssapi::GssapiConfig {
            keytab_path: std::path::PathBuf::from("/unused.keytab"),
            service_name: "kafka".to_string(),
            principal_to_local_rules: vec![krabka_security::gssapi::name::Rule::Default],
            realm: Some("CRABKA.TEST".to_string()),
            kdc: None,
            max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
        };

        let short = map_gssapi_principal("alice@crabka.test", &config).expect("map principal");

        assert!(short == "alice");
    }
}
