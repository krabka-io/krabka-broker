//! Canonical loopback SASL listener fixtures for integration tests.

use std::{net::SocketAddr, path::PathBuf};

use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

/// A single ephemeral SASL/PLAINTEXT listener, also used for inter-broker traffic.
/// Mechanisms, credentials and authorization remain explicit at the call site.
pub fn sasl_plaintext_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg
}

/// Enable PLAIN and seed a fixture's users and sole super-user.
pub fn sasl_plaintext_with_users(
    log_dir: PathBuf,
    super_user: &str,
    users: &[(&str, &str)],
) -> BrokerConfig {
    let mut cfg = sasl_plaintext_config(log_dir);
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials = users
        .iter()
        .map(|(name, pass)| ((*name).to_string(), (*pass).to_string()))
        .collect();
    cfg.super_users = std::iter::once(super_user.to_string()).collect();
    cfg
}

/// Start a configured fixture, retaining its log directory beside the handle.
pub async fn start_broker(
    cfg: BrokerConfig,
    log_dir: TempDir,
) -> (BrokerHandle, TempDir, SocketAddr) {
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// PLAIN fixture with the default authorizer.
pub fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plaintext_with_users(dir.path().to_path_buf(), super_user, users);
    start_broker(cfg, dir)
}

/// PLAIN fixture with ACL authorization and an anonymous controller heartbeat.
pub fn start_sasl_plaintext_with_acl_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = sasl_plaintext_with_users(dir.path().to_path_buf(), super_user, users);
    cfg.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        cfg.super_users
            .iter()
            .cloned()
            .chain(std::iter::once("ANONYMOUS".to_string()))
            .collect(),
    ));
    start_broker(cfg, dir)
}

/// Hash and persist a SCRAM fixture through the controller, outside the wire admin API.
pub async fn provision_scram(
    broker: &BrokerHandle,
    user: &str,
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) {
    let credential = krabka_security::hash_scram_password(password, mechanism, iterations);
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: user.into(),
                mechanism,
                salt: credential.salt,
                stored_key: credential.stored_key,
                server_key: credential.server_key,
                iterations: credential.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");
}
