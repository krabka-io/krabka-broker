//! A cryptographically valid token must still pass the controller cache guard.

use {
    super::*,
    crate::{
        raft_handshake::test_support::{
            HandshakeHeader, read_response_frame, request_frame, sasl_test_config,
        },
        test_support::Es256SigningKey,
        time_util::now_ms,
    },
    assert2::assert,
    base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64},
    krabka_protocol::{Encode, owned::sasl_authenticate_response::SaslAuthenticateResponse},
    krabka_raft::RaftListenerHandshake as _,
    krabka_security::{JwksHandle, OAuthBearerValidator, SignedJwsValidator},
    std::sync::atomic::Ordering,
    tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    },
};

struct NoApiVersions;

impl ControllerApiVersions for NoApiVersions {
    fn respond(&self, _: i16, _: &[u8]) -> Result<bytes::Bytes, RaftHandshakeError> {
        panic!("fixture sends only SASL frames")
    }
}

/// A signed-JWS validator and a token it admits.
struct SignedToken {
    validator: OAuthBearerValidator,
    token: String,
}

fn signed_token() -> SignedToken {
    let key = Es256SigningKey::generate("controller");
    let mut validator = SignedJwsValidator::new(JwksHandle::new(key.jwks()));
    validator.cache_expiry = Some(krabka_units::millis(60_000));
    let token = key.sign(&serde_json::json!({
        "sub": "controller-peer",
        "exp": now_ms() / 1000 + 3600,
    }));
    SignedToken {
        validator: OAuthBearerValidator::Signed(validator),
        token,
    }
}

#[tokio::test]
async fn signed_validation_rechecks_the_publication_after_crypto_success() {
    use std::sync::atomic::{AtomicI64, AtomicU64};

    let SignedToken { validator, token } = signed_token();
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
    let SignedToken { validator, token } = signed_token();
    // Positive crypto control: rejection below cannot be attributed to a bad token.
    let expected_expiry = validator
        .validate(&token, now_ms())
        .await
        .unwrap()
        .expires_at_ms;
    assert!(expected_expiry.is_some());
    for (generation, fetched, admitted) in [
        (2, now_ms(), true),
        (2, 1, false),        // installed keys with a stale timestamp
        (0, 0, false),        // installed keys with no successful-fetch publication
        (3, now_ms(), false), // in-flight writer
    ] {
        let mut cfg = sasl_test_config();
        cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
        cfg.oauthbearer_validator = validator.clone();
        cfg.oauthbearer_jwks_cache_generation
            .store(generation, Ordering::Release);
        cfg.oauthbearer_jwks_last_successful_fetch_ms
            .store(fetched, Ordering::Release);
        let ControllerExchange { mut client, task } = start_oauthbearer_exchange(cfg).await;
        let mut body = bytes::BytesMut::new();
        SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(format!("n,,\x01auth=Bearer {token}\x01\x01")),
            ..Default::default()
        }
        .encode(&mut body, 2)
        .unwrap();
        let response = authenticate_round_trip(&mut client, 2, &body).await;
        assert!(response.error_code == 0);
        assert!(
            response.auth_bytes.is_empty() == admitted,
            "generation={generation}, fetched={fetched}"
        );
        assert!((response.session_lifetime_ms > 0) == admitted);
        if admitted {
            let connection = task.await.unwrap().unwrap();
            assert!(connection.principal.unwrap().name == "controller-peer");
            assert!(!connection.authenticated_via_token);
            assert!(connection.expires_at_ms == expected_expiry);
        } else {
            assert!(
                response.auth_bytes
                    == krabka_security::oauth_error_json(&krabka_security::AuthError::InvalidToken)
                        .as_bytes()
            );
            body.clear();
            SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from_static(b"\x01"),
                ..Default::default()
            }
            .encode(&mut body, 2)
            .unwrap();
            let response = authenticate_round_trip(&mut client, 3, &body).await;
            assert!(response.error_code == SASL_AUTHENTICATION_FAILED);
            assert!(task.await.unwrap().is_err());
        }
    }
}

/// The controller listener's OAUTHBEARER challenge and the failure on the
/// client's `\x01` both carry Kafka's `insufficient_scope` JSON for a token
/// that lacks a required scope item.
#[tokio::test]
async fn controller_listener_challenges_a_token_without_the_required_scope() {
    let expected_json = r#"{"status":"insufficient_scope", "scope":"[a, b]"}"#;
    let mut cfg = sasl_test_config();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator =
        OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            required_scope: vec!["a".to_owned(), "b".to_owned()],
            ..Default::default()
        });
    let ControllerExchange { mut client, task } = start_oauthbearer_exchange(cfg).await;
    let header = B64.encode(br#"{"alg":"none"}"#);
    let payload = B64.encode(
        serde_json::json!({"sub": "controller-peer", "exp": now_ms() / 1000 + 3600, "scope": ["a"]})
            .to_string(),
    );
    let mut body = bytes::BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(format!("n,,\x01auth=Bearer {header}.{payload}.\x01\x01")),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .unwrap();
    let challenge = authenticate_round_trip(&mut client, 2, &body).await;
    assert!(
        challenge
            == SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: bytes::Bytes::from_static(expected_json.as_bytes()),
                session_lifetime_ms: 0,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );

    body.clear();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(b"\x01"),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .unwrap();
    let failure = authenticate_round_trip(&mut client, 3, &body).await;
    assert!(
        failure
            == SaslAuthenticateResponse {
                error_code: SASL_AUTHENTICATION_FAILED,
                error_message: Some(expected_json.to_owned()),
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms: 0,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );
    assert!(task.await.unwrap().is_err());
}

/// A controller-listener connection past its OAUTHBEARER `SaslHandshake`, and
/// the task that runs the listener's side.
struct ControllerExchange {
    client: TcpStream,
    task: tokio::task::JoinHandle<Result<krabka_raft::RaftConnection, RaftHandshakeError>>,
}

async fn start_oauthbearer_exchange(cfg: BrokerRaftHandshake) -> ControllerExchange {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let task = tokio::spawn(async move { cfg.upgrade(server.into(), &NoApiVersions).await });
    let mut body = bytes::BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".into(),
        ..Default::default()
    }
    .encode(&mut body, 1)
    .unwrap();
    client
        .write_all(&request_frame().maybe_client_id(None).body(&body).call())
        .await
        .unwrap();
    let handshake = read_response_frame(&mut client).await;
    assert!(&handshake[4..6] == &0_i16.to_be_bytes());
    ControllerExchange { client, task }
}

async fn authenticate_round_trip(
    client: &mut TcpStream,
    correlation_id: i32,
    body: &[u8],
) -> SaslAuthenticateResponse {
    client
        .write_all(
            &request_frame()
                .api_key(API_KEY_SASL_AUTHENTICATE)
                .api_version(2)
                .correlation(correlation_id)
                .maybe_client_id(None)
                .header(HandshakeHeader::Flexible)
                .body(body)
                .call(),
        )
        .await
        .unwrap();
    let frame = read_response_frame(client).await;
    SaslAuthenticateResponse::decode(&mut &frame[5..], 2).unwrap()
}
