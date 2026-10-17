//! Round-trip tests for the OAUTHBEARER handler, including KIP-368 re-auth.

use {
    super::*,
    crate::network::auth::test_support::{
        assert_failed_authenticate_response, assert_success_authenticate_response,
        oauth_reauthenticating,
    },
    assert2::{assert, check},
    krabka_security::{Principal, SaslMechanism},
    krabka_units::{millis, secs},
};

fn unsecured_token(sub: &str, exp_s: i64) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    format!(
        "{}.{}.",
        B64.encode(b"{\"alg\":\"none\"}"),
        B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_s}}}").as_bytes())
    )
}

fn oauthbearer_client_response(token: &str) -> SaslAuthenticateRequest {
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(
            format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
        ),
        ..Default::default()
    }
}

async fn authenticate(
    request: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    handle_authenticate_oauthbearer(request, auth, validator, || now_ms, max_session_lifetime).await
}

async fn authenticate_state(
    mut auth: ConnectionAuth,
    token: &str,
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
) -> (ConnectionAuth, SaslAuthenticateResponse) {
    let response = authenticate(
        &oauthbearer_client_response(token),
        &mut auth,
        validator,
        now_ms,
        None,
    )
    .await;
    (auth, response)
}

fn signed_validator(cache_expiry: Option<Time>) -> krabka_security::OAuthBearerValidator {
    let mut validator =
        krabka_security::SignedJwsValidator::new(krabka_security::JwksHandle::default());
    validator.cache_expiry = cache_expiry;
    krabka_security::OAuthBearerValidator::Signed(validator)
}

#[yare::parameterized(
    missing_expiry = { oauth_session_decision(None, 100, None, false, true) },
    expiry_at_current_time = { oauth_session_decision(Some(100), 100, None, false, true) },
    unrepresentable_current_time = {
        oauth_session_decision(Some(i64::MAX), -1, None, false, true)
    },
    zero_lifetime = { oauth_session_decision(Some(200), 100, Some(secs(0)), false, true) },
    reauthentication_required = { oauth_session_decision(Some(200), 100, None, true, false) },
)]
fn oauth_session_adapter_rejects_invalid_or_unrepresentable_lifetimes(
    decision: krabka_verified::OAuthSessionDecision,
) {
    assert2::assert!(decision == krabka_verified::OAuthSessionDecision::Reject);
}

#[test]
fn oauth_session_adapter_returns_the_exact_effective_expiry() {
    assert!(
        oauth_session_decision(Some(200), 100, Some(millis(40)), false, true)
            == krabka_verified::OAuthSessionDecision::Admit {
                session_lifetime_ms: 40,
                effective_expires_at_ms: 140,
            }
    );
}

/// The connection state and the response that a re-authentication left.
struct Reauthenticated {
    auth: ConnectionAuth,
    response: SaslAuthenticateResponse,
}

async fn reauthenticate(
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
    token: &str,
) -> Reauthenticated {
    let (auth, response) = authenticate_state(
        oauth_reauthenticating("alice", now_ms + 1_000),
        token,
        validator,
        now_ms,
    )
    .await;
    Reauthenticated { auth, response }
}

#[yare::parameterized(
    changing = { 1, 1_000_000 },
    never_fetched = { 2, 0 },
    stale = { 2, -1 },
)]
#[test_macro(tokio::test)]
async fn signed_validator_fails_closed_for_stale_or_changing_jwks_cache(
    generation: u64,
    last_successful_fetch_ms: i64,
) {
    let fixture = signed_cache_fixture(generation);
    fixture
        .last_successful
        .store(last_successful_fetch_ms, Ordering::Release);
    assert2::assert!(
        fixture.validate().await
            == Err(BearerRejection::InvalidToken {
                reason: "JWKS cache is stale or changing",
                error: krabka_security::AuthError::InvalidToken,
            })
    );
}

#[tokio::test]
async fn signed_validator_uses_a_fresh_stable_jwks_cache() {
    let fixture = signed_cache_fixture(2);
    let result = fixture.validate().await;
    assert!(
        result
            == Err(BearerRejection::InvalidToken {
                reason: "token validation failed",
                error: krabka_security::AuthError::InvalidToken,
            })
    );
}

#[tokio::test]
async fn oauthbearer_valid_token_authenticates() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let token = unsecured_token("svc-account", 1_000_000_900); // exp seconds → future of now
    let (auth, resp) = authenticate_state(oauth_pending(), &token, &validator, now_ms).await;
    assert_success_authenticate_response(&resp, b"", 900_000);
    let p = auth.principal().expect("authenticated");
    assert!(p.name == "svc-account");
    assert!(p.auth_method == krabka_security::AuthMethod::SaslOAuthBearer);
    match auth {
        ConnectionAuth::Authenticated {
            expires_at_ms,
            authenticated_via_token,
            ..
        } => {
            assert!(expires_at_ms == Some(1_000_000_900_000));
            assert!(!authenticated_via_token);
        }
        _ => panic!("expected authenticated state"),
    }
}

#[tokio::test]
async fn oauthbearer_invalid_token_returns_error_json_then_fails_on_dummy() {
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: secs(0),
            ..Default::default()
        });
    let now_ms = 5_000_000_000_000;
    // exp far in the past → expired.
    let token = unsecured_token("admin", 1_000_000_000);
    let (mut auth, resp) = authenticate_state(oauth_pending(), &token, &validator, now_ms).await;
    // Round 1: rejected → error JSON, error_code 0, connection stays open.
    assert_invalid_token_challenge(&resp);
    assert!(matches!(auth, ConnectionAuth::Negotiating {
        exchange: SaslExchange::OAuthBearerFailed { .. },
        ..
    }));
    // Round 2: the client's `\x01` dummy → SASL_AUTHENTICATION_FAILED (58).
    let dummy = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(&[1u8]),
        ..Default::default()
    };
    let resp2 = authenticate(&dummy, &mut auth, &validator, now_ms, None).await;
    assert_failed_authenticate_response(&resp2, Some(r#"{"status":"invalid_token"}"#));
    assert!(!auth.is_authenticated());
}

/// Kafka's `OAuthBearerClientInitialResponse` throws a plain `SaslException`
/// for a response it cannot parse, so the exchange fails at once with the
/// generic message and no RFC 7628 challenge. A re-authentication that fails
/// this way leaves the previous session in place while the dispatcher closes
/// the connection.
#[yare::parameterized(
    initial_garbage = { oauth_pending(), b"not-a-valid-gs2-message", None },
    initial_lone_control_a = { oauth_pending(), b"\x01", None },
    reauth_garbage = {
        oauth_reauthenticating("alice", 1_000_000_001_000),
        b"not-a-valid-gs2-message",
        Some("alice"),
    },
    reauth_lone_control_a = {
        oauth_reauthenticating("alice", 1_000_000_001_000),
        b"\x01",
        Some("alice"),
    },
)]
#[test_macro(tokio::test)]
async fn a_malformed_client_response_fails_at_once_with_the_generic_failure(
    auth: ConnectionAuth,
    auth_bytes: &'static [u8],
    principal_after: Option<&str>,
) {
    let mut auth = auth;
    let validator = krabka_security::OAuthBearerValidator::default();
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(auth_bytes),
        ..Default::default()
    };
    let resp = authenticate(&req, &mut auth, &validator, 1_000_000_000_000, None).await;
    assert_failed_authenticate_response(&resp, None);
    assert2::assert!(auth.principal().map(|p| p.name.as_str()) == principal_after);
}

fn oauthbearer_client_response_for(authzid: &str, token: &str) -> SaslAuthenticateRequest {
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(
            format!("n,a={authzid},\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
        ),
        ..Default::default()
    }
}

/// Kafka's `OAuthBearerSaslServer.process` throws a
/// `SaslAuthenticationException` for an authzid that differs from the token
/// principal, so the exchange fails at once with its message and no RFC 7628
/// challenge.
#[tokio::test]
async fn oauthbearer_authzid_mismatch_fails() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let token = unsecured_token("alice", 1_000_000_900);
    let mut auth = oauth_pending();
    let req = oauthbearer_client_response_for("bob", &token);
    let resp = authenticate(&req, &mut auth, &validator, now_ms, None).await;
    assert_failed_authenticate_response(
        &resp,
        Some(
            "Authentication failed: Client requested an authorization id (bob) that is \
             different from the token's principal name (alice)",
        ),
    );
    assert!(!auth.is_authenticated());
}

#[tokio::test]
async fn oauthbearer_matching_authzid_authenticates() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let token = unsecured_token("alice", 1_000_000_900);
    let mut auth = oauth_pending();
    let req = oauthbearer_client_response_for("alice", &token);
    let resp = authenticate(&req, &mut auth, &validator, now_ms, None).await;
    assert_success_authenticate_response(&resp, b"", 900_000);
    assert!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
}

#[tokio::test]
async fn authenticate_during_reauth_same_principal_transitions_back_to_authenticated() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    // Token's exp is in seconds; the validator computes expires_at_ms = exp * 1000.
    let new_token_exp_seconds: i64 = 1_000_000_900;
    let new_token_exp_millis: i64 = new_token_exp_seconds * 1000;
    let token = unsecured_token("alice", new_token_exp_seconds);
    let Reauthenticated {
        auth,
        response: resp,
    } = reauthenticate(&validator, now_ms, &token).await;
    assert_success_authenticate_response(&resp, b"", new_token_exp_millis - now_ms);
    assert!(matches!(auth, ConnectionAuth::Authenticated {
        mechanism: SaslMechanism::OAuthBearer,
        expires_at_ms: Some(_),
        ..
    }));
    if let ConnectionAuth::Authenticated {
        principal,
        expires_at_ms,
        ..
    } = &auth
    {
        assert!(principal.name == "alice");
        assert!(*expires_at_ms == Some(new_token_exp_millis));
    } else {
        panic!("expected Authenticated");
    }
}

#[tokio::test]
async fn authenticate_during_reauth_different_principal_rejected_with_sasl_auth_failed() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    // Token belongs to "bob", but the prior session is "alice".
    let token = unsecured_token("bob", 1_000_000_900);
    let Reauthenticated {
        auth,
        response: resp,
    } = reauthenticate(&validator, now_ms, &token).await;
    // Kafka's `ensurePrincipalUnchanged` message.
    assert_failed_authenticate_response(
        &resp,
        Some("Cannot change principals during re-authentication from User.alice: User.bob"),
    );
    // Connection remained in Reauthenticating (dispatch will close).
    assert!(matches!(auth, ConnectionAuth::Reauthenticating { .. }));
}

/// A rejected re-authentication token gets the same RFC 7628 error challenge
/// as an initial one, and the client's `\x01` then fails the exchange with
/// that JSON as the message, leaving the previous session in place while the
/// dispatcher closes the connection.
#[tokio::test]
async fn a_rejected_reauth_token_gets_the_error_challenge_then_fails_with_its_json() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let mut auth = oauth_reauthenticating("alice", now_ms + 1_000);
    // exp far in the past → expired.
    let expired = oauthbearer_client_response(&unsecured_token("alice", 1_000_000));
    let challenge = authenticate(&expired, &mut auth, &validator, now_ms, None).await;
    assert_success_authenticate_response(&challenge, br#"{"status":"invalid_token"}"#, 0);

    let dummy = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(&[1u8]),
        ..Default::default()
    };
    let failure = authenticate(&dummy, &mut auth, &validator, now_ms, None).await;
    assert_failed_authenticate_response(&failure, Some(r#"{"status":"invalid_token"}"#));
    assert!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
}

/// An unsecured token for `alice`, valid until 1 000 000 900 s, whose `scope`
/// claim is the JSON `scope`.
fn scoped_token(scope: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    format!(
        "{}.{}.",
        B64.encode(b"{\"alg\":\"none\"}"),
        B64.encode(
            format!("{{\"sub\":\"alice\",\"exp\":1000000900,\"scope\":{scope}}}").as_bytes()
        )
    )
}

/// Kafka's `OAuthBearerSaslServer` answers a failed validation with
/// `jsonErrorResponse` for that failure and stores it as `errorMessage`, which
/// it throws on the client's `\x01`. So the challenge and the final failure
/// both carry `insufficient_scope` and the required scope for a token that
/// lacks a required scope item, and `invalid_token` for any other failure.
#[yare::parameterized(
    initial_missing_scope_item = {
        oauth_pending(),
        scoped_token(r#"["a"]"#),
        r#"{"status":"insufficient_scope", "scope":"[a, b]"}"#,
        None,
    },
    initial_no_scope = {
        oauth_pending(),
        scoped_token(r#""""#),
        r#"{"status":"insufficient_scope", "scope":"[a, b]"}"#,
        None,
    },
    initial_numeric_scope_claim = {
        oauth_pending(),
        scoped_token("7"),
        r#"{"status":"invalid_token"}"#,
        None,
    },
    initial_expired = {
        oauth_pending(),
        unsecured_token("alice", 1_000_000),
        r#"{"status":"invalid_token"}"#,
        None,
    },
    reauth_missing_scope_item = {
        oauth_reauthenticating("alice", 1_000_000_001_000),
        scoped_token(r#"["b"]"#),
        r#"{"status":"insufficient_scope", "scope":"[a, b]"}"#,
        Some("alice"),
    },
    reauth_expired = {
        oauth_reauthenticating("alice", 1_000_000_001_000),
        unsecured_token("alice", 1_000_000),
        r#"{"status":"invalid_token"}"#,
        Some("alice"),
    },
)]
#[test_macro(tokio::test)]
async fn the_error_challenge_and_the_final_failure_carry_the_validation_error_json(
    auth: ConnectionAuth,
    token: String,
    expected_json: &str,
    principal_after: Option<&str>,
) {
    let mut auth = auth;
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            required_scope: vec!["a".to_owned(), "b".to_owned()],
            ..Default::default()
        });
    let now_ms = 1_000_000_000_000;
    let challenge = authenticate(
        &oauthbearer_client_response(&token),
        &mut auth,
        &validator,
        now_ms,
        None,
    )
    .await;
    assert_success_authenticate_response(&challenge, expected_json.as_bytes(), 0);

    let dummy = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(&[1u8]),
        ..Default::default()
    };
    let failure = authenticate(&dummy, &mut auth, &validator, now_ms, None).await;
    assert_failed_authenticate_response(&failure, Some(expected_json));
    assert2::assert!(auth.principal().map(|p| p.name.as_str()) == principal_after);
}

/// A token that carries every required scope item authenticates.
#[tokio::test]
async fn a_token_with_every_required_scope_item_authenticates() {
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            required_scope: vec!["a".to_owned(), "b".to_owned()],
            ..Default::default()
        });
    let mut auth = oauth_pending();
    let resp = authenticate(
        &oauthbearer_client_response(&scoped_token(r#"["b","a","c"]"#)),
        &mut auth,
        &validator,
        1_000_000_000_000,
        None,
    )
    .await;
    assert_success_authenticate_response(&resp, b"", 900_000);
    assert!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
}

/// Kafka's `OAuthBearerSaslServer` throws its stored error only for a lone
/// `\x01` after a challenge. Any other reply clears the error and runs as a
/// fresh client initial response, so a valid token then authenticates.
#[yare::parameterized(
    initial = { oauth_pending() },
    reauth = { oauth_reauthenticating("alice", 1_000_000_001_000) },
)]
#[test_macro(tokio::test)]
async fn a_client_response_after_the_error_challenge_runs_as_a_fresh_attempt(auth: ConnectionAuth) {
    let mut auth = auth;
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    // exp far in the past → expired.
    let expired = oauthbearer_client_response(&unsecured_token("alice", 1_000_000));
    let challenge = authenticate(&expired, &mut auth, &validator, now_ms, None).await;
    assert_invalid_token_challenge(&challenge);

    let valid = oauthbearer_client_response(&unsecured_token("alice", 1_000_000_900));
    let resp = authenticate(&valid, &mut auth, &validator, now_ms, None).await;
    assert_success_authenticate_response(&resp, b"", 900_000);
    let ConnectionAuth::Authenticated {
        principal,
        mechanism,
        expires_at_ms,
        authenticated_via_token,
    } = auth
    else {
        panic!("expected Authenticated, got {auth:?}");
    };
    let alice = Principal {
        name: "alice".into(),
        auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
        groups: vec![],
    };
    assert2::assert!(
        (principal, mechanism, expires_at_ms, authenticated_via_token)
            == (
                alice,
                SaslMechanism::OAuthBearer,
                Some(1_000_000_900_000),
                false
            )
    );
}

// KIP-368 ceiling: the server-side
// `connections.max.reauth.ms` cap clamps both the response field and
// the `Authenticated.expires_at_ms` stored on the connection.

#[tokio::test]
async fn handle_authenticate_oauthbearer_applies_max_session_lifetime_cap() {
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: secs(0),
            ..Default::default()
        });
    let now_ms = 1_000_000_i64;
    let exp_ms = now_ms + 60_000; // token good for 60s
    let token = unsecured_token("alice", exp_ms / 1000);
    let req = oauthbearer_client_response(&token);

    // (server cap in seconds, expected session lifetime in ms). The
    // stored expires_at_ms must reflect the clamped value too, not the
    // raw token exp.
    let cases = [
        (Some(secs(30)), 30_000_i64), // cap below the token's 60s exp → clamped
        (None, 60_000),               // unset cap → raw token exp
        (Some(secs(600)), 60_000),    // cap above exp → no effect
    ];
    for (cap, want_lifetime_ms) in cases {
        let mut auth = oauth_pending();
        let resp = authenticate(&req, &mut auth, &validator, now_ms, cap).await;
        check!(resp.error_code == 0, "cap {cap:?}");
        check!(resp.session_lifetime_ms == want_lifetime_ms, "cap {cap:?}");
        match auth {
            ConnectionAuth::Authenticated { expires_at_ms, .. } => {
                assert!(
                    expires_at_ms == Some(now_ms + want_lifetime_ms),
                    "cap {cap:?}: expires_at_ms must reflect the clamped value"
                );
            }
            _ => panic!("cap {cap:?}: expected Authenticated"),
        }
    }
}

#[tokio::test]
async fn handle_authenticate_oauthbearer_rejects_a_zero_session_cap() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_i64;
    let token = unsecured_token("alice", 2_000);
    let mut auth = oauth_pending();

    let response = authenticate(
        &oauthbearer_client_response(&token),
        &mut auth,
        &validator,
        now_ms,
        Some(secs(0)),
    )
    .await;

    assert!(response.error_code == 0);
    assert!(response.session_lifetime_ms == 0);
    assert!(response.auth_bytes == bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#));
    assert!(matches!(auth, ConnectionAuth::Negotiating {
        exchange: SaslExchange::OAuthBearerFailed { .. },
        ..
    }));
}

mod completion;

fn oauth_pending() -> ConnectionAuth {
    ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::OAuthBearer,
        exchange: SaslExchange::OAuthBearer,
        pending_token_expiry_ms: None,
    }
}

fn assert_invalid_token_challenge(response: &SaslAuthenticateResponse) {
    let expected = SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
        session_lifetime_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(response == &expected);
}

/// A signed validator, a request for it, and the JWKS cache state to check it
/// against.
struct SignedCacheFixture {
    now_ms: i64,
    request: SaslAuthenticateRequest,
    validator: krabka_security::OAuthBearerValidator,
    generation: AtomicU64,
    last_successful: AtomicI64,
}

impl SignedCacheFixture {
    async fn validate(&self) -> Result<ValidatedBearer, BearerRejection> {
        validate_bearer(
            &self.request.auth_bytes,
            &self.validator,
            Some(JwksCache {
                generation: &self.generation,
                last_successful_fetch_ms: &self.last_successful,
            }),
            || self.now_ms,
        )
        .await
    }
}

fn signed_cache_fixture(generation: u64) -> SignedCacheFixture {
    let now_ms = 1_000_000;
    let request = oauthbearer_client_response(&unsecured_token("alice", 2_000));
    let validator = signed_validator(Some(secs(1)));
    SignedCacheFixture {
        now_ms,
        request,
        validator,
        generation: AtomicU64::new(generation),
        last_successful: AtomicI64::new(now_ms),
    }
}
