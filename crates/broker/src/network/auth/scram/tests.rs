//! Tests for the SCRAM handler's KIP-48 delegation-token login.
//!
//! The login cases need a running raft controller to hold the metadata image,
//! so they are slower than the rest of the auth unit tests and sit in their
//! own file.

mod client_first_parsing {
    use assert2::check;

    use super::super::{ClientFirst, parse_scram_client_first};

    fn parsed(username: &str, token_auth_requested: bool) -> ClientFirst {
        ClientFirst {
            username: username.into(),
            token_auth_requested,
        }
    }

    #[test]
    fn reads_username_and_the_tokenauth_extension_as_kafka_does() {
        for (message, expected) in [
            ("n,,n=alice,r=abc", Some(parsed("alice", false))),
            ("n,,n=tok,r=abc,tokenauth=true", Some(parsed("tok", true))),
            // `Boolean.parseBoolean` ignores case and reads anything else
            // as false.
            ("n,,n=tok,r=abc,tokenauth=TRUE", Some(parsed("tok", true))),
            ("n,,n=tok,r=abc,tokenauth=false", Some(parsed("tok", false))),
            ("n,,n=tok,r=abc,tokenauth=yes", Some(parsed("tok", false))),
            ("n,,n=tok,r=abc,tokenauth=", Some(parsed("tok", false))),
            // Unknown extensions are ignored; a later duplicate wins, as
            // `Utils.parseMap` puts each pair into one map.
            (
                "n,,n=tok,r=abc,other=1,tokenauth=true",
                Some(parsed("tok", true)),
            ),
            (
                "n,,n=tok,r=abc,tokenauth=true,tokenauth=false",
                Some(parsed("tok", false)),
            ),
            // Username and nonce come first, in order; an extension needs `=`.
            ("n,,r=abc,n=tok", None),
            ("n,,n=tok", None),
            ("n,,n=tok,r=abc,tokenauth", None),
            ("n=tok,r=abc", None),
        ] {
            check!(
                parse_scram_client_first(message.as_bytes()) == expected,
                "{message}"
            );
        }
    }
}

mod token_login {
    use std::{num::NonZeroU32, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use base64::{Engine, engine::general_purpose::STANDARD as B64};
    use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
    use krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    };
    use krabka_security::{
        AuthMethod, KafkaPrincipal, Principal, SaslMechanism, scram::hash_scram_password_with_salt,
    };
    use ring::{digest, hmac, pbkdf2};
    use tempfile::TempDir;

    use crate::{
        codes::SASL_AUTHENTICATION_FAILED,
        network::auth::{
            ConnectionAuth, SaslExchange, handle_authenticate_scram,
            test_support::assert_failed_authenticate_response,
        },
    };

    const TOKEN_ID: &str = "tok-uuid";
    const HMAC: [u8; 32] = [0xAB; 32];

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

    /// Appends a delegation token owned by `alice` to the controller's image.
    async fn append_token(controller: &krabka_raft::ControllerHandle, expiry_timestamp_ms: i64) {
        let rec = MetadataRecord::V1DelegationToken(DelegationTokenRecord {
            token_id: TOKEN_ID.into(),
            owner: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            hmac: HMAC.to_vec(),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms,
            max_timestamp_ms: expiry_timestamp_ms,
            renewers: vec![],
        });
        controller.submit_change(vec![rec]).await.unwrap();
    }

    /// Stores a regular SCRAM credential for `user` under both mechanisms.
    async fn append_user(controller: &krabka_raft::ControllerHandle, user: &str, password: &[u8]) {
        let salt = (0..16).collect::<Vec<u8>>();
        let records = [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512]
            .into_iter()
            .map(|mechanism| {
                let cred = hash_scram_password_with_salt(password, mechanism, 4096, salt.clone());
                MetadataRecord::V1ScramCredential(krabka_metadata::ScramCredentialRecord {
                    user: user.into(),
                    mechanism,
                    salt: salt.clone(),
                    stored_key: cred.stored_key,
                    server_key: cred.server_key,
                    iterations: cred.iterations,
                })
            })
            .collect();
        controller.submit_change(records).await.unwrap();
    }

    fn token_password() -> String {
        B64.encode(HMAC)
    }

    /// A SCRAM client that can send Kafka's `tokenauth=true` extension, which
    /// `krabka_security::ScramClientExchange` does not write. RFC 5802 §3.
    ///
    /// `tests/delegation_tokens/scram_client.rs` holds the same client for
    /// the wire-level suite: a library unit test and an integration test
    /// compile from disjoint Bazel source sets, so neither can include the
    /// other's file.
    struct Client {
        mechanism: SaslMechanism,
        password: Vec<u8>,
        first_bare: String,
    }

    impl Client {
        fn first(
            mechanism: SaslMechanism,
            username: &str,
            password: &[u8],
            token_auth: bool,
        ) -> (Self, Vec<u8>) {
            let extension = if token_auth { ",tokenauth=true" } else { "" };
            let first_bare = format!("n={username},r=clientnonce{extension}");
            let message = format!("n,,{first_bare}").into_bytes();
            let client = Self {
                mechanism,
                password: password.to_vec(),
                first_bare,
            };
            (client, message)
        }

        fn last(self, server_first: &[u8]) -> Vec<u8> {
            let server_first = std::str::from_utf8(server_first).unwrap();
            let attr = |prefix: &str| {
                server_first
                    .split(',')
                    .find_map(|a| a.strip_prefix(prefix))
                    .unwrap()
            };
            let nonce = attr("r=");
            let salt = B64.decode(attr("s=")).unwrap();
            let iterations: NonZeroU32 = attr("i=").parse().unwrap();
            let (pbkdf2_alg, hmac_alg, digest_alg, len) = match self.mechanism {
                SaslMechanism::ScramSha256 => (
                    pbkdf2::PBKDF2_HMAC_SHA256,
                    hmac::HMAC_SHA256,
                    &digest::SHA256,
                    32,
                ),
                SaslMechanism::ScramSha512 => (
                    pbkdf2::PBKDF2_HMAC_SHA512,
                    hmac::HMAC_SHA512,
                    &digest::SHA512,
                    64,
                ),
                other => panic!("not a SCRAM mechanism: {other:?}"),
            };
            let mut salted = vec![0; len];
            pbkdf2::derive(pbkdf2_alg, iterations, &salt, &self.password, &mut salted);
            let client_key = hmac::sign(&hmac::Key::new(hmac_alg, &salted), b"Client Key");
            let stored_key = digest::digest(digest_alg, client_key.as_ref());
            let without_proof = format!("c=biws,r={nonce}");
            let auth_message = format!("{},{server_first},{without_proof}", self.first_bare);
            let signature = hmac::sign(
                &hmac::Key::new(hmac_alg, stored_key.as_ref()),
                auth_message.as_bytes(),
            );
            let proof: Vec<u8> = client_key
                .as_ref()
                .iter()
                .zip(signature.as_ref())
                .map(|(k, s)| k ^ s)
                .collect();
            format!("{without_proof},p={}", B64.encode(proof)).into_bytes()
        }
    }

    fn step(
        controller: &krabka_raft::ControllerHandle,
        auth: &mut ConnectionAuth,
        bytes: Vec<u8>,
    ) -> SaslAuthenticateResponse {
        handle_authenticate_scram(
            &SaslAuthenticateRequest {
                auth_bytes: bytes::Bytes::from(bytes),
                ..Default::default()
            },
            auth,
            controller,
            None,
        )
    }

    /// Drives both SCRAM rounds. Returns the final auth state, or the failed
    /// response of the round that refused the login.
    fn login(
        controller: &krabka_raft::ControllerHandle,
        mechanism: SaslMechanism,
        username: &str,
        password: &[u8],
        token_auth: bool,
    ) -> Result<(ConnectionAuth, SaslAuthenticateResponse), SaslAuthenticateResponse> {
        let mut auth = ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };
        let (client, first) = Client::first(mechanism, username, password, token_auth);
        let resp1 = step(controller, &mut auth, first);
        if resp1.error_code != 0 {
            return Err(resp1);
        }
        let resp2 = step(controller, &mut auth, client.last(&resp1.auth_bytes));
        if resp2.error_code != 0 {
            return Err(resp2);
        }
        Ok((auth, resp2))
    }

    /// What an authenticated session carries: principal, mechanism, session
    /// deadline, and whether it came from a token.
    fn session(auth: ConnectionAuth) -> (Principal, SaslMechanism, Option<i64>, bool) {
        let ConnectionAuth::Authenticated {
            principal,
            mechanism,
            expires_at_ms,
            authenticated_via_token,
        } = auth
        else {
            panic!("expected Authenticated, got {auth:?}");
        };
        (principal, mechanism, expires_at_ms, authenticated_via_token)
    }

    fn refused(
        result: Result<(ConnectionAuth, SaslAuthenticateResponse), SaslAuthenticateResponse>,
    ) {
        let Err(resp) = result else {
            panic!("login must be refused");
        };
        check!(resp.error_code == SASL_AUTHENTICATION_FAILED);
        assert_failed_authenticate_response(&resp);
    }

    /// Kafka's `DelegationTokenManager` prepares a token credential for every
    /// SCRAM mechanism, so a `tokenauth` login works under SHA-256 and
    /// SHA-512 alike and yields a session for the token's owner, bounded by
    /// the token's expiry.
    #[tokio::test]
    async fn tokenauth_login_works_under_both_scram_mechanisms() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let expiry_ms = crate::time_util::now_ms() + 60_000;
        append_token(&controller, expiry_ms).await;

        for (mechanism, auth_method) in [
            (SaslMechanism::ScramSha256, AuthMethod::SaslScramSha256),
            (SaslMechanism::ScramSha512, AuthMethod::SaslScramSha512),
        ] {
            let (auth, resp2) = login(
                &controller,
                mechanism,
                TOKEN_ID,
                token_password().as_bytes(),
                true,
            )
            .expect("token login succeeds");
            check!(
                resp2.session_lifetime_ms > 0 && resp2.session_lifetime_ms <= 60_000,
                "{mechanism:?}"
            );
            let owner = Principal {
                name: "alice".into(),
                auth_method,
                groups: vec![],
            };
            check!(session(auth) == (owner, mechanism, Some(expiry_ms), true));
        }
        controller.cancel().await;
    }

    /// Without `tokenauth`, Kafka reads only the SCRAM credential store, so a
    /// token id and its HMAC are not a login under either mechanism.
    #[tokio::test]
    async fn token_credentials_without_tokenauth_are_refused() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        append_token(&controller, crate::time_util::now_ms() + 60_000).await;

        for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
            refused(login(
                &controller,
                mechanism,
                TOKEN_ID,
                token_password().as_bytes(),
                false,
            ));
        }
        controller.cancel().await;
    }

    /// When a token id is also a SCRAM username, the `tokenauth` extension
    /// alone decides which credential the login is checked against.
    #[tokio::test]
    async fn tokenauth_selects_the_store_when_a_token_id_is_also_a_username() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let expiry_ms = crate::time_util::now_ms() + 60_000;
        append_token(&controller, expiry_ms).await;
        append_user(&controller, TOKEN_ID, b"user-password").await;
        let mechanism = SaslMechanism::ScramSha256;

        let (auth, _) = login(
            &controller,
            mechanism,
            TOKEN_ID,
            token_password().as_bytes(),
            true,
        )
        .expect("token login succeeds");
        let owner = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        };
        check!(session(auth) == (owner, mechanism, Some(expiry_ms), true));

        let (auth, resp2) = login(&controller, mechanism, TOKEN_ID, b"user-password", false)
            .expect("user login succeeds");
        check!(resp2.session_lifetime_ms == 0);
        let user = Principal {
            name: TOKEN_ID.into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        };
        check!(session(auth) == (user, mechanism, None, false));

        // Each password is checked only against the store its login selects.
        refused(login(
            &controller,
            mechanism,
            TOKEN_ID,
            b"user-password",
            true,
        ));
        refused(login(
            &controller,
            mechanism,
            TOKEN_ID,
            token_password().as_bytes(),
            false,
        ));
        controller.cancel().await;
    }

    /// A `tokenauth` login for a missing or expired token is refused in round
    /// 1, and never falls back to a SCRAM user of the same name.
    #[tokio::test]
    async fn tokenauth_login_for_a_missing_or_expired_token_is_refused() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        append_user(&controller, "no-such-token", b"user-password").await;
        refused(login(
            &controller,
            SaslMechanism::ScramSha256,
            "no-such-token",
            b"user-password",
            true,
        ));

        append_token(&controller, crate::time_util::now_ms() - 1).await;
        refused(login(
            &controller,
            SaslMechanism::ScramSha512,
            TOKEN_ID,
            token_password().as_bytes(),
            true,
        ));
        controller.cancel().await;
    }
}
