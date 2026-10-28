//! SASL/OAUTHBEARER token validation end-to-end.
//!
//! Two validators run against the live `SaslAuthenticate` path: the
//! unsecured `alg:none` JWS validator, and the signed validator whose key
//! set comes from an in-memory JWKS. Each is exercised on the accept path
//! and on the RFC 7628 two-round failure handshake.

use {
    crate::{
        harness::metadata_probe,
        oauthbearer::{
            OauthbearerSession, now_unix_secs, oauthbearer_authenticate, oauthbearer_fixture,
            oauthbearer_initial, oauthbearer_session_start, start_oauthbearer_broker,
            unsecured_jws,
        },
    },
    assert2::assert,
    base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64},
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
    krabka_security::{OAuthBearerValidator, UnsecuredJwsValidator},
    ring::{
        rand::SystemRandom,
        signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::io,
    tempfile::TempDir,
};

/// Happy path: a valid unsecured token authenticates in a single round.
///
/// A post-auth Metadata round-trip proves that the connection survived and
/// that the broker accepted the principal.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_tokens.feature",
    name = "sasl oauthbearer happy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_happy_path(bearer: BearerTokenScenario) {
    bearer
        .broker
        .expect("an OAUTHBEARER listener")
        .shutdown()
        .await;
}

/// Failure path: an expired token triggers the RFC 7628 two-round failure
/// handshake.
///
/// Round 1 returns the `invalid_token` JSON with `error_code = 0` and keeps
/// the connection open. Round 2, the client's `\x01` dummy, returns
/// `SASL_AUTHENTICATION_FAILED` (58).
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_tokens.feature",
    name = "sasl oauthbearer invalid token two round failure"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_invalid_token_two_round_failure(bearer: BearerTokenScenario) {
    bearer
        .broker
        .expect("an OAUTHBEARER listener")
        .shutdown()
        .await;
}

/// A signing key pair and the JWKS document that advertises its public key.
struct Es256Key {
    key_pair: EcdsaKeyPair,
    jwks: String,
}

/// Generate a fresh ES256 key and return it with its JWKS.
///
/// The JWKS advertises the matching public key under `kid`.
fn es256_key(kid: &str) -> Es256Key {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let kp =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let point = kp.public_key().as_ref(); // 0x04 || x || y
    let jwks = format!(
        "{{\"keys\":[{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"{kid}\",\"x\":\"{}\",\"y\":\"{}\"}}]}}",
        B64.encode(&point[1..33]),
        B64.encode(&point[33..65]),
    );
    Es256Key { key_pair: kp, jwks }
}

/// Sign an ES256 JWS with `kp`, `kid` in the header and `claims` as the payload.
fn es256_token(kp: &EcdsaKeyPair, kid: &str, claims: &str) -> String {
    let header = B64.encode(format!("{{\"alg\":\"ES256\",\"kid\":\"{kid}\"}}").as_bytes());
    let payload = B64.encode(claims.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig = kp
        .sign(&SystemRandom::new(), signing_input.as_bytes())
        .unwrap();
    format!("{signing_input}.{}", B64.encode(sig.as_ref()))
}

/// A `Signed` validator whose key set comes from `jwks_json`.
///
/// The test needs no network fetch.
fn signed_validator(jwks_json: &str) -> krabka_security::OAuthBearerValidator {
    let handle = krabka_security::JwksHandle::new(
        krabka_security::Jwks::from_json(jwks_json, false).unwrap(),
    );
    krabka_security::OAuthBearerValidator::Signed(krabka_security::SignedJwsValidator::new(handle))
}

/// Happy path: a real signed ES256 token, verified against an in-memory JWKS,
/// authenticates in a single round.
///
/// This proves that the `Signed` validator is wired through the live
/// `SaslAuthenticate` path.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_tokens.feature",
    name = "sasl oauthbearer signed token happy path"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_happy_path(bearer: BearerTokenScenario) {
    bearer
        .broker
        .expect("an OAUTHBEARER listener")
        .shutdown()
        .await;
}

/// Failure path: a token signed by a *different* key than the JWKS advertises
/// triggers the RFC 7628 two-round failure handshake.
///
/// Round 1 carries the `invalid_token` JSON. Round 2, the `\x01` dummy,
/// returns 58.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/oauthbearer_tokens.feature",
    name = "sasl oauthbearer signed token wrong key two round failure"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_wrong_key_two_round_failure(bearer: BearerTokenScenario) {
    bearer
        .broker
        .expect("an OAUTHBEARER listener")
        .shutdown()
        .await;
}

struct BearerTokenScenario {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    signing_key: Option<Es256Key>,
    token: Option<String>,
    session: Option<OauthbearerSession>,
    initial_response: Option<io::Result<SaslAuthenticateResponse>>,
    final_response: Option<io::Result<SaslAuthenticateResponse>>,
    metadata: Option<io::Result<()>>,
}

#[fixture]
fn bearer() -> BearerTokenScenario {
    // Start this scenario with fresh inputs and observations.

    BearerTokenScenario {
        broker: None,
        directory: None,
        signing_key: None,
        token: None,
        session: None,
        initial_response: None,
        final_response: None,
        metadata: None,
    }
}

#[given("an OAUTHBEARER listener accepting unsecured JWS tokens")]
async fn unsecured_listener(bearer: &mut BearerTokenScenario) {
    let (directory, broker, _addr) =
        oauthbearer_fixture(OAuthBearerValidator::default(), None).await;
    bearer.broker = Some(broker);
    bearer.directory = Some(directory);
}

#[given("an unsecured OAUTHBEARER listener allowing zero clock skew")]
async fn zero_skew_listener(bearer: &mut BearerTokenScenario) {
    let directory = tempfile::tempdir().unwrap();
    let validator = OAuthBearerValidator::Unsecured(UnsecuredJwsValidator {
        allowable_clock_skew: krabka_units::secs(0),
        ..Default::default()
    });
    bearer.broker = Some(start_oauthbearer_broker(directory.path(), validator).await);
    bearer.directory = Some(directory);
}

#[given("an OAUTHBEARER listener trusting ES256 key A as kid k1 in an in-memory JWKS")]
async fn signed_listener(bearer: &mut BearerTokenScenario) {
    let key = es256_key("k1");
    let (directory, broker, _addr) = oauthbearer_fixture(signed_validator(&key.jwks), None).await;
    bearer.broker = Some(broker);
    bearer.directory = Some(directory);
    bearer.signing_key = Some(key);
}

#[given("an unsecured svc-account token expiring one hour from now")]
fn valid_unsecured_token(bearer: &mut BearerTokenScenario) {
    bearer.token = Some(unsecured_jws("svc-account", now_unix_secs() + 3600));
}

#[given("an unsecured admin token that expired one hour ago")]
fn expired_token(bearer: &mut BearerTokenScenario) {
    bearer.token = Some(unsecured_jws("admin", now_unix_secs() - 3600));
}

#[given("a svc-account ES256 token signed by key A and expiring one hour from now")]
fn trusted_signed_token(bearer: &mut BearerTokenScenario) {
    let key = bearer.signing_key.as_ref().expect("the trusted ES256 key");
    let claims = format!(
        "{{\"sub\":\"svc-account\",\"exp\":{}}}",
        now_unix_secs() + 3600
    );
    bearer.token = Some(es256_token(&key.key_pair, "k1", &claims));
}

#[given("an admin ES256 token signed by different key B under the same kid k1")]
fn wrong_signed_token(bearer: &mut BearerTokenScenario) {
    let key = es256_key("k1");
    let claims = format!("{{\"sub\":\"admin\",\"exp\":{}}}", now_unix_secs() + 3600);
    bearer.token = Some(es256_token(&key.key_pair, "k1", &claims));
}

#[given("the client has selected OAUTHBEARER on a new connection")]
async fn selected_bearer_session(bearer: &mut BearerTokenScenario) {
    let addr = bearer
        .broker
        .as_ref()
        .expect("an OAUTHBEARER listener")
        .listen_addr();
    bearer.session = Some(
        oauthbearer_session_start(addr)
            .await
            .expect("the OAUTHBEARER handshake"),
    );
}

#[when("the bearer token is sent as the initial SASL response")]
async fn initial_token(bearer: &mut BearerTokenScenario) {
    let token = bearer.token.as_ref().expect("a bearer token");
    let session = bearer
        .session
        .as_mut()
        .expect("a selected OAUTHBEARER session");
    bearer.initial_response = Some(
        oauthbearer_authenticate(
            &mut session.stream,
            &mut session.corr,
            oauthbearer_initial(token),
        )
        .await,
    );
}

#[then("the token is accepted in one round without challenge bytes")]
fn bearer_accepted(bearer: &BearerTokenScenario) {
    let response = bearer
        .initial_response
        .as_ref()
        .expect("the initial bearer token was submitted")
        .as_ref()
        .expect("the OAUTHBEARER authentication round must complete");
    assert!(
        response.error_code == 0,
        "authenticate failed: code={} msg={:?}",
        response.error_code,
        response.error_message
    );
    assert!(
        response.auth_bytes.is_empty(),
        "a successful token must not produce an OAUTHBEARER challenge"
    );
}

#[when("the bearer-authenticated connection requests Metadata")]
async fn bearer_metadata(bearer: &mut BearerTokenScenario) {
    let session = bearer
        .session
        .as_mut()
        .expect("an accepted OAUTHBEARER session");
    bearer.metadata = Some(metadata_probe(&mut session.stream, session.corr).await);
}

#[then("Metadata succeeds after admitting the bearer principal")]
fn usable_bearer_session(bearer: &BearerTokenScenario) {
    bearer
        .metadata
        .as_ref()
        .expect("the authenticated session requested Metadata")
        .as_ref()
        .expect("OAUTHBEARER session must succeed end-to-end");
}

#[then("round 1 keeps the connection open and returns only the invalid_token JSON challenge")]
fn invalid_token_challenge(bearer: &BearerTokenScenario) {
    let response = bearer
        .initial_response
        .as_ref()
        .expect("the initial bearer token was submitted")
        .as_ref()
        .expect("OAUTHBEARER failure round 1 must complete");
    assert!(response.error_code == 0, "round 1 must not close yet");
    assert!(
        &response.auth_bytes[..] == br#"{"status":"invalid_token"}"#,
        "round 1 must carry the RFC 7628 error JSON"
    );
}

#[when("the client sends the RFC 7628 dummy byte 1")]
async fn bearer_failure_dummy(bearer: &mut BearerTokenScenario) {
    let session = bearer
        .session
        .as_mut()
        .expect("an OAUTHBEARER challenge session");
    bearer.final_response = Some(
        oauthbearer_authenticate(
            &mut session.stream,
            &mut session.corr,
            Bytes::from_static(&[1u8]),
        )
        .await,
    );
}

#[then("round 2 returns SASL_AUTHENTICATION_FAILED")]
fn bearer_failure_completed(bearer: &BearerTokenScenario) {
    let response = bearer
        .final_response
        .as_ref()
        .expect("the RFC 7628 dummy was submitted")
        .as_ref()
        .expect("OAUTHBEARER failure round 2 must complete");
    assert!(
        response.error_code == 58,
        "round 2 must fail the connection"
    );
}
