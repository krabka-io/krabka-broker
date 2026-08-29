//! One-broker, one-client boots for the simpler integration tests.
//!
//! Every helper here starts a broker and returns a client already connected to
//! it. They differ only in what the broker is configured with: a caller-owned
//! directory that survives a restart, an audit signing key, a deny-all
//! authorizer, or nothing beyond the `for_tests` defaults.

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use tempfile::TempDir;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}

/// Start a broker rooted at `dir` (caller owns the directory).
///
/// Restart tests use this helper. Pass the same path across two boots to
/// verify that the broker recovers persistent state (audit chain, spool)
/// correctly. The helper detects an existing raft log and then uses `Rejoin`.
pub async fn start_with_dir(dir: &std::path::Path) -> (BrokerHandle, krabka_client_core::Client) {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    // Mirror the production heuristic from `detect_bootstrap_mode` in
    // broker.rs: key Rejoin on `metadata_log_nonempty` (committed
    // quorum-state), NOT bare directory presence.  The segment dir is created
    // before the first raft commit, so dir-existence would re-bootstrap a node
    // killed mid-election instead of letting it rejoin correctly.
    let metadata_dir = dir.join("__cluster_metadata");
    if krabka_raft::metadata_log_nonempty(&metadata_dir) {
        config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    }
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = krabka_client_core::Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client)
}

/// Start a broker configured with an audit signing key and a given checkpoint cadence.
///
/// Uses `every_secs = 3600` so only the count-based trigger fires in tests.
pub fn start_with_audit_key(
    key_path: &std::path::Path,
    key_id: &str,
    every_n: u64,
) -> impl std::future::Future<Output = InProcess> {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.audit_signing_key_path = Some(key_path.to_path_buf());
    config.audit_signing_key_id = Some(key_id.to_string());
    config.audit_checkpoint_every_n = every_n;
    config.audit_checkpoint_every = krabka_units::hours(1); // only count trigger fires
    Box::pin(async move {
        let broker = Broker::start(config).await.expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("krabka-broker-test-audit-key")
            .build()
            .await
            .expect("client build");
        InProcess {
            broker,
            client,
            _tempdir: tempdir,
        }
    })
}

/// Start a broker whose authorizer is `SimpleAclAuthorizer` with no ACLs and no
/// super-users (deny-all for the anonymous test client). The `for_tests`
/// defaults enable audit. The broker denies the anonymous client every admin
/// operation, which produces `AuthorizationDenied` audit events.
pub async fn start_with_deny_all_authz() -> InProcess {
    use std::collections::HashSet;

    use krabka_broker::authorizer::SimpleAclAuthorizer;

    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    // Replace the default AllowAllAuthorizer with a deny-all SimpleAclAuthorizer
    // (empty ACL store, no super-users). The anonymous test client connects
    // with no credentials so it has no super-user bypass — every operation is
    // denied and the auditing decorator emits AuthorizationDenied events.
    config.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("krabka-broker-test-deny")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker,
        client,
        _tempdir: tempdir,
    }
}
