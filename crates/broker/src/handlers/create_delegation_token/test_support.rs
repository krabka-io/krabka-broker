//! Fixtures shared by the `CreateDelegationToken` tests: a single-voter
//! controller to submit against, the authenticated-connection builders, and
//! the super-user sets that the act-as cases need.

use std::{collections::HashSet, sync::Arc, time::Duration};

use assert2::assert;
use krabka_raft::ControllerHandle;
use krabka_security::{AuthMethod, Principal, SaslMechanism};

use crate::network::auth::ConnectionAuth;

/// The KIP-48 24 h default. It matches Kafka's
/// `delegation.token.expiry.time.ms`. Tests that do not exercise
/// renew-period clamping pass this value.
pub(super) const RENEW_24H_MS: i64 = 24 * 60 * 60 * 1_000;

/// Helper that produces an empty super-users set, for tests that do not
/// exercise the act-as path.
pub(super) fn empty_super_users() -> HashSet<String> {
    HashSet::new()
}

/// Helper that produces a super-users set with the given names.
pub(super) fn super_users_with(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

/// Starts a single-voter `Controller` for tests and waits for the
/// leader.
pub(super) async fn test_controller(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
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

/// Builds an authenticated SCRAM connection for `name`, flagging whether the
/// caller authenticated with a delegation token.
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

/// Builds an authenticated SCRAM connection for `name` that did not use a
/// delegation token.
pub(super) fn authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, false)
}

/// Builds the synthetic authenticated state used by PLAINTEXT and SSL
/// listeners without mTLS. It must not be admitted to token APIs.
pub(super) fn anonymous() -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: "ANONYMOUS".into(),
            auth_method: AuthMethod::Anonymous,
            groups: vec![],
        },
        mechanism: SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}
