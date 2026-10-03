//! A cryptographically valid token must still pass the controller cache guard.

use std::sync::atomic::Ordering;

use assert2::assert;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use krabka_protocol::{Encode, owned::sasl_authenticate_response::SaslAuthenticateResponse};
use krabka_security::{Jwks, JwksHandle, OAuthBearerValidator, SignedJwsValidator};
use ring::{
    rand::SystemRandom,
    signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
};
use tokio::io::AsyncWriteExt;

use super::*;
use crate::raft_handshake::test_support::{read_response_frame, request_frame, sasl_test_config};

struct NoApiVersions;

impl ControllerApiVersions for NoApiVersions {
    fn respond(&self, _: i16, _: &[u8]) -> Result<bytes::Bytes, RaftHandshakeError> {
        panic!("fixture sends only SASL frames")
    }
}

fn signed_token() -> (OAuthBearerValidator, String) {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let key =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let point = key.public_key().as_ref();
    let jwks = serde_json::json!({"keys": [{"kty": "EC", "crv": "P-256", "kid": "controller",
        "x": B64.encode(&point[1..33]), "y": B64.encode(&point[33..65])}]});
    let handle = JwksHandle::new(Jwks::from_json(&jwks.to_string(), false).unwrap());
    let mut validator = SignedJwsValidator::new(handle);
    validator.cache_expiry = Some(krabka_units::millis(60_000));
    let header = B64.encode(br#"{"alg":"ES256","kid":"controller"}"#);
    let expiry = crate::time_util::now_ms() / 1000 + 3600;
    let payload =
        B64.encode(serde_json::json!({"sub": "controller-peer", "exp": expiry}).to_string());
    let input = format!("{header}.{payload}");
    let signature = key.sign(&rng, input.as_bytes()).unwrap();
    (
        OAuthBearerValidator::Signed(validator),
        format!("{input}.{}", B64.encode(signature.as_ref())),
    )
}

#[tokio::test]
async fn signed_validation_rechecks_the_publication_after_crypto_success() {
    use std::sync::atomic::{AtomicI64, AtomicU64};

    let (validator, token) = signed_token();
    for (finished_generation, admitted) in [(2, true), (3, false), (4, false)] {
        let generation = AtomicU64::new(2);
        let fetched = AtomicI64::new(1000);
        let calls = std::cell::Cell::new(0);
        let mut auth = ConnectionAuth::Anonymous;
        let handshake = handle_handshake(
            &SaslHandshakeRequest {
                mechanism: "OAUTHBEARER".into(),
                ..Default::default()
            },
            &mut auth,
            &[SaslMechanism::OAuthBearer],
            &mut ReauthClock {
                now_ms: 0,
                last_start_ms: &mut None,
            },
        );
        assert!(handshake.response.error_code == 0);
        let request = SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(format!("n,,\x01auth=Bearer {token}\x01\x01")),
            ..Default::default()
        };
        let response = handle_authenticate_oauthbearer_with_jwks_cache(
            &request,
            &mut auth,
            &validator,
            &generation,
            &fetched,
            || {
                if calls.get() == 1 {
                    // The completion clock is read after successful signature validation.
                    generation.store(finished_generation, Ordering::Release);
                }
                calls.set(calls.get() + 1);
                1000
            },
            None,
        )
        .await;
        assert!(
            calls.get() == 2,
            "signature validation must succeed before the second observation"
        );
        assert!(response.auth_bytes.is_empty() == admitted);
        assert!(auth.is_authenticated() == admitted);
    }
}

#[tokio::test]
async fn controller_signed_oauth_requires_a_fresh_published_cache() {
    let (validator, token) = signed_token();
    // Positive crypto control: rejection below cannot be attributed to a bad token.
    assert!(
        validator
            .validate(&token, crate::time_util::now_ms())
            .await
            .is_ok()
    );
    for (generation, fetched, admitted) in [
        (2, crate::time_util::now_ms(), true),
        (2, 1, false),                          // installed keys with a stale timestamp
        (0, 0, false), // installed keys with no successful-fetch publication
        (3, crate::time_util::now_ms(), false), // in-flight writer
    ] {
        let mut cfg = sasl_test_config();
        cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
        cfg.oauthbearer_validator = validator.clone();
        cfg.oauthbearer_jwks_cache_generation
            .store(generation, Ordering::Release);
        cfg.oauthbearer_jwks_last_successful_fetch_ms
            .store(fetched, Ordering::Release);
        let (mut client, mut server) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            run_inbound_sasl(
                &mut server,
                &cfg,
                &"192.0.2.11:9093".parse().unwrap(),
                &NoApiVersions,
            )
            .await
        });
        let mut body = bytes::BytesMut::new();
        SaslHandshakeRequest {
            mechanism: "OAUTHBEARER".into(),
            ..Default::default()
        }
        .encode(&mut body, 1)
        .unwrap();
        client
            .write_all(&request_frame(
                API_KEY_SASL_HANDSHAKE,
                1,
                1,
                None,
                false,
                &body,
            ))
            .await
            .unwrap();
        let handshake = read_response_frame(&mut client).await;
        assert!(&handshake[4..6] == &0_i16.to_be_bytes());
        body.clear();
        SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(format!("n,,\x01auth=Bearer {token}\x01\x01")),
            ..Default::default()
        }
        .encode(&mut body, 2)
        .unwrap();
        client
            .write_all(&request_frame(
                API_KEY_SASL_AUTHENTICATE,
                2,
                2,
                None,
                true,
                &body,
            ))
            .await
            .unwrap();
        let frame = read_response_frame(&mut client).await;
        let response = SaslAuthenticateResponse::decode(&mut &frame[5..], 2).unwrap();
        assert!(response.error_code == 0);
        assert!(
            response.auth_bytes.is_empty() == admitted,
            "generation={generation}, fetched={fetched}"
        );
        assert!((response.session_lifetime_ms > 0) == admitted);
        if admitted {
            let (principal, via_token) = task.await.unwrap().unwrap();
            assert!(principal.name == "controller-peer");
            assert!(!via_token);
        } else {
            assert!(response.auth_bytes == krabka_security::invalid_token_json().as_bytes());
            body.clear();
            SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from_static(b"\x01"),
                ..Default::default()
            }
            .encode(&mut body, 2)
            .unwrap();
            client
                .write_all(&request_frame(
                    API_KEY_SASL_AUTHENTICATE,
                    2,
                    3,
                    None,
                    true,
                    &body,
                ))
                .await
                .unwrap();
            let frame = read_response_frame(&mut client).await;
            let response = SaslAuthenticateResponse::decode(&mut &frame[5..], 2).unwrap();
            assert!(response.error_code == crate::codes::SASL_AUTHENTICATION_FAILED);
            assert!(task.await.unwrap().is_err());
        }
    }
}
