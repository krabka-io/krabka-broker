//! Live-controller tests for the SCRAM handler's KIP-48 token fallback.
//!
//! These cases need a running raft controller to hold the metadata image, so
//! they are slower than the rest of the auth unit tests and sit in their own
//! file.

// KIP-48 — SCRAM-SHA-256 delegation-token fallback tests.
//
// The tests below spin up a single-voter raft controller so we can
// append a `DelegationTokenRecord` and then exercise
// `handle_authenticate_scram` against the live image.

mod token_scram_fallback {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
    use krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    };
    use krabka_security::{
        KafkaPrincipal, SaslMechanism, ScramClientExchange, scram::hash_scram_password_with_salt,
    };
    use tempfile::TempDir;

    use crate::{
        codes::SASL_AUTHENTICATION_FAILED,
        network::auth::{
            ConnectionAuth, SaslExchange, handle_authenticate_scram,
            test_support::assert_failed_authenticate_response,
        },
    };

    async fn test_controller(log_dir: std::path::PathBuf) -> Arc<krabka_raft::ControllerHandle> {
        let cfg = krabka_raft::ControllerConfig {
            election_timeout: krabka_units::millis(200),
            heartbeat_interval: Some(krabka_units::millis(50)),
            client_id: "test".into(),
            ..krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(1), log_dir)
        };
        let handle = Arc::new(krabka_raft::Controller::start(cfg).await.unwrap());
        let mut rx = handle.watch_leader();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while rx.borrow().is_none() {
            assert!(std::time::Instant::now() < deadline, "no leader in 5s");
            let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
        }
        handle
    }

    /// Appends a delegation token to the controller's image.
    async fn append_token(
        controller: &krabka_raft::ControllerHandle,
        token_id: &str,
        owner_name: &str,
        hmac: Vec<u8>,
        expiry_timestamp_ms: i64,
    ) {
        let rec = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: token_id.into(),
            owner: KafkaPrincipal {
                principal_type: "User".into(),
                name: owner_name.into(),
            },
            hmac,
            issue_timestamp_ms: 0,
            expiry_timestamp_ms,
            max_timestamp_ms: expiry_timestamp_ms,
            renewers: vec![],
        });
        controller.submit_change(vec![rec]).await.unwrap();
    }

    /// Drives the SCRAM client through both rounds against the broker's
    /// `handle_authenticate_scram`. Returns the final `auth` state and
    /// the round-2 server response, so callers can assert on
    /// `error_code`, `session_lifetime_ms`, and other fields.
    fn drive_scram_to_done(
        controller: &krabka_raft::ControllerHandle,
        scram_username: &str,
        password: &[u8],
        mechanism: SaslMechanism,
    ) -> (ConnectionAuth, SaslAuthenticateResponse) {
        let mut auth = ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let client = ScramClientExchange::new(scram_username.into(), password.to_vec(), mechanism);

        // Round 1: client-first
        let (c1, client) = client.client_first().expect("client first");
        let resp1 = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c1),
                ..Default::default()
            },
            &mut auth,
            controller,
        );
        assert!(resp1.error_code == 0, "round 1 must succeed for happy path");

        // Round 2: client-final
        let (c2, _client) = client.step(&resp1.auth_bytes).expect("client final");
        let resp2 = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c2),
                ..Default::default()
            },
            &mut auth,
            controller,
        );
        (auth, resp2)
    }

    /// Happy path: image contains a delegation token, no matching
    /// regular SCRAM user, SCRAM-SHA-256 round-1 falls back to the
    /// token table and round-2 succeeds.
    #[tokio::test]
    async fn scram_sha256_falls_back_to_delegation_token_when_no_scram_user() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let hmac = vec![0xABu8; 32];
        let expiry_ms = crate::time_util::now_ms() + 60_000;
        append_token(&controller, "tok-uuid", "alice", hmac.clone(), expiry_ms).await;

        let password = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&hmac)
        };

        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha256,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let client = ScramClientExchange::new(
            "tok-uuid".into(),
            password.as_bytes().to_vec(),
            SaslMechanism::ScramSha256,
        );
        let (c1, _client) = client.client_first().unwrap();
        let resp1 = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c1),
                ..Default::default()
            },
            &mut auth,
            &*controller,
        );
        // The server-first message is nonce-dependent, so pin
        // non-emptiness rather than exact bytes.
        let round1 = "round 1 must succeed: token-fallback synthesizes the credential";
        check!(resp1.error_code == 0, "{round1}");
        check!(resp1.error_message.as_deref() == None, "{round1}");
        check!(!resp1.auth_bytes.is_empty(), "{round1}");
        check!(resp1.session_lifetime_ms == 0, "{round1}");
        // Negotiating state now carries pending_token_expiry_ms.
        match &auth {
            ConnectionAuth::Negotiating {
                pending_token_expiry_ms,
                ..
            } => {
                assert!(
                    *pending_token_expiry_ms == Some(expiry_ms),
                    "round 1 must thread the token expiry through"
                );
            }
            other => panic!("expected Negotiating, got {other:?}"),
        }
        controller.cancel().await;
    }

    #[tokio::test]
    async fn scram_sha256_rejects_expired_delegation_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let hmac = vec![0xACu8; 32];
        append_token(
            &controller,
            "expired-token",
            "alice",
            hmac.clone(),
            crate::time_util::now_ms() - 1,
        )
        .await;

        let password = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&hmac)
        };
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha256,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let client = ScramClientExchange::new(
            "expired-token".into(),
            password.into_bytes(),
            SaslMechanism::ScramSha256,
        );
        let (c1, _) = client.client_first().unwrap();
        let response = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c1),
                ..Default::default()
            },
            &mut auth,
            &*controller,
        );

        check!(response.error_code == SASL_AUTHENTICATION_FAILED);
        assert_failed_authenticate_response(&response);
        controller.cancel().await;
    }

    /// Round-2 success: full two-round-trip drive ends in
    /// `Authenticated` whose principal is the token's owner (`alice`),
    /// with `authenticated_via_token: true` and `expires_at_ms` set
    /// to the token's `expiry_timestamp_ms`.
    #[tokio::test]
    async fn token_authed_connection_has_authenticated_via_token_true_and_owner_principal() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let hmac = vec![0x42u8; 32];
        let expiry_ms = crate::time_util::now_ms() + 60_000;
        append_token(&controller, "tok-xyz", "alice", hmac.clone(), expiry_ms).await;

        let password = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&hmac)
        };

        let (auth, resp2) = drive_scram_to_done(
            &controller,
            "tok-xyz",
            password.as_bytes(),
            SaslMechanism::ScramSha256,
        );

        // The server-final message is nonce-dependent (non-empty), and
        // token SCRAM reports the remaining token lifetime (0, 60s].
        check!(resp2.error_code == 0, "round 2 must succeed");
        check!(
            resp2.error_message.as_deref() == None,
            "round 2 must succeed"
        );
        check!(!resp2.auth_bytes.is_empty(), "round 2 must succeed");
        check!(
            resp2.session_lifetime_ms > 0 && resp2.session_lifetime_ms <= 60_000,
            "round 2 must succeed"
        );
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token,
            } => {
                // principal is the token OWNER, not the tokenId
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::ScramSha256);
                // expires_at_ms = token expiry (KIP-368 ceiling)
                check!(expires_at_ms == Some(expiry_ms));
                // token-fallback must mark the session as token-authed
                check!(authenticated_via_token);
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
        controller.cancel().await;
    }

    /// Token-fallback must NOT fire for an unknown SCRAM username
    /// when the image has no matching token either.
    #[tokio::test]
    async fn scram_sha256_token_fallback_does_not_fire_for_unknown_token_id() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        // No tokens appended.

        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha256,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let client = ScramClientExchange::new(
            "no-such-token".into(),
            b"whatever".to_vec(),
            SaslMechanism::ScramSha256,
        );
        let (c1, _client) = client.client_first().unwrap();
        let resp = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c1),
                ..Default::default()
            },
            &mut auth,
            &*controller,
        );
        assert!(
            resp.error_code == SASL_AUTHENTICATION_FAILED,
            "no SCRAM user + no token = unknown-user failure"
        );
        assert_failed_authenticate_response(&resp);
        controller.cancel().await;
    }

    /// SCRAM-SHA-512 must NOT read the delegation-token table, even when
    /// the SCRAM username matches a token's id. KIP-48 scopes token-SCRAM
    /// to SHA-256 only.
    #[tokio::test]
    async fn scram_sha512_does_not_fall_back_to_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        // Image has a token with id "tok-xyz".
        let hmac = vec![0x55u8; 32];
        let expiry_ms = crate::time_util::now_ms() + 60_000;
        append_token(&controller, "tok-xyz", "alice", hmac, expiry_ms).await;

        // Client requests SHA-512 with the tokenId as the username.
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha512,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let client = ScramClientExchange::new(
            "tok-xyz".into(),
            b"whatever".to_vec(),
            SaslMechanism::ScramSha512,
        );
        let (c1, _client) = client.client_first().unwrap();
        let resp = handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(c1),
                ..Default::default()
            },
            &mut auth,
            &*controller,
        );
        assert!(
            resp.error_code == SASL_AUTHENTICATION_FAILED,
            "SCRAM-SHA-512 must not consult the delegation-token table"
        );
        assert_failed_authenticate_response(&resp);
        controller.cancel().await;
    }

    /// Regular SCRAM, without a token, keeps
    /// `Authenticated.authenticated_via_token = false` and
    /// `expires_at_ms = None`.
    #[tokio::test]
    async fn regular_scram_user_authentication_does_not_set_token_flag() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        // Append a regular SCRAM credential for `alice` directly via
        // metadata records. PBKDF2 is deterministic for a fixed salt.
        let salt = (0..16).collect::<Vec<u8>>();
        let cred = hash_scram_password_with_salt(
            b"alice-password",
            SaslMechanism::ScramSha256,
            4096,
            salt.clone(),
        );
        let scram_rec = MetadataRecord::V1ScramCredential(krabka_metadata::ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha256,
            salt,
            stored_key: cred.stored_key.clone(),
            server_key: cred.server_key.clone(),
            iterations: cred.iterations,
        });
        controller.submit_change(vec![scram_rec]).await.unwrap();

        let (auth, resp2) = drive_scram_to_done(
            &controller,
            "alice",
            b"alice-password",
            SaslMechanism::ScramSha256,
        );
        assert!(resp2.error_code == 0);
        assert!(
            resp2.session_lifetime_ms == 0,
            "regular SCRAM has no session lifetime"
        );
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                expires_at_ms,
                authenticated_via_token,
                ..
            } => {
                let msg = "regular SCRAM is NOT a token-authed session";
                check!(principal.name.as_str() == "alice", "{msg}");
                check!(expires_at_ms == None, "{msg}");
                check!(!authenticated_via_token, "{msg}");
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
        controller.cancel().await;
    }
}
