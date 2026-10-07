//! Controller and identity fixtures shared by the KIP-48 token handlers.

use std::{path::PathBuf, sync::Arc, time::Duration};

use assert2::assert;
use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
use krabka_raft::{Controller, ControllerConfig, ControllerHandle, NodeId};
use krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};

use crate::network::auth::ConnectionAuth;

/// Kafka's default `delegation.token.expiry.time.ms`.
pub(super) const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) type RefusalToken = (Vec<u8>, i64);

pub(super) struct RefusalFixture {
    pub(super) directory: tempfile::TempDir,
    pub(super) controller: Arc<ControllerHandle>,
    pub(super) secret: krabka_security::SecretBytes,
    pub(super) live: RefusalToken,
    pub(super) expired: RefusalToken,
}

impl RefusalFixture {
    pub(super) async fn new() -> Self {
        let directory = tempfile::TempDir::new().unwrap();
        let controller = test_controller(directory.path().into()).await;
        let (live, expired) = refusal_tokens(&controller).await;
        Self {
            directory,
            controller,
            secret: krabka_security::SecretBytes::new(b"k".to_vec()),
            live,
            expired,
        }
    }
}

/// Starts a single-voter controller and waits for its leader.
pub(super) async fn test_controller(log_dir: PathBuf) -> Arc<ControllerHandle> {
    let config = ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..ControllerConfig::for_tests(NodeId(1), log_dir)
    };
    let controller = Arc::new(Controller::start(config).await.unwrap());
    let mut leader = controller.watch_leader();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while leader.borrow().is_none() {
        assert!(std::time::Instant::now() < deadline, "no leader in 5s");
        let _ = tokio::time::timeout(Duration::from_millis(100), leader.changed()).await;
    }
    controller
}

/// An authenticated SCRAM connection, optionally using a delegation token.
pub(super) fn authed_with_token(name: &str, via_token: bool) -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        },
        mechanism: SaslMechanism::ScramSha256,
        expires_at_ms: None,
        authenticated_via_token: via_token,
    }
}

pub(super) fn authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, false)
}

/// The synthetic authenticated state of PLAINTEXT and SSL without mTLS.
/// Token APIs must refuse it.
pub(super) fn anonymous() -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: crate::test_support::principal("ANONYMOUS"),
        mechanism: SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

pub(super) fn kp(name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".into(),
        name: name.into(),
    }
}

/// The HMAC the fixture secret key `k` gives `token_id`.
pub(super) fn hmac_for(token_id: &str) -> Vec<u8> {
    krabka_security::compute_token_hmac(b"k", token_id)
}

/// Seeds a token owned by `alice`, requested by `minter`, with renewer `bob`.
pub(super) async fn seed_token(
    controller: &ControllerHandle,
    token_id: &str,
    expiry_timestamp_ms: i64,
    max_timestamp_ms: i64,
) {
    let token = DelegationTokenRecord {
        token_id: token_id.into(),
        owner: kp("alice"),
        requester: kp("minter"),
        issue_timestamp_ms: 0,
        expiry_timestamp_ms,
        max_timestamp_ms,
        renewers: vec![kp("bob")],
    };
    controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(token)])
        .await
        .expect("seed token");
}

/// A live token and one just expired, both with the same future maximum.
pub(super) async fn refusal_tokens(
    controller: &ControllerHandle,
) -> ((Vec<u8>, i64), (Vec<u8>, i64)) {
    let now = crate::time_util::now_ms();
    let live = (hmac_for("live"), now + 60_000);
    let expired = (hmac_for("expired"), now - 1);
    for (token_id, (_, expiry)) in [("live", &live), ("expired", &expired)] {
        seed_token(controller, token_id, *expiry, now + DAY_MS).await;
    }
    (live, expired)
}
