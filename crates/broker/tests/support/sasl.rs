//! Canonical loopback SASL listener fixtures for integration tests.

use std::{net::SocketAddr, path::PathBuf};

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

pub fn scram_upsertion(
    name: impl Into<String>,
    mechanism: i8,
    iterations: i32,
    (salt, salted_password): (bytes::Bytes, bytes::Bytes),
) -> krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialUpsertion {
    krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialUpsertion {
        name: name.into(),
        mechanism,
        iterations,
        salt,
        salted_password,
        ..Default::default()
    }
}

/// A single ephemeral SASL/PLAINTEXT listener, also used for inter-broker traffic.
/// Mechanisms, credentials and authorization remain explicit at the call site.
pub fn sasl_plaintext_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![crate::support::listeners::loopback_listener(
        "SASL_PLAINTEXT",
        ListenerProtocol::SaslPlaintext,
    )];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg
}

/// An ephemeral SASL/PLAINTEXT listener with the case's exact enabled mechanisms.
pub fn sasl_plaintext_mechanisms(log_dir: PathBuf, mechanisms: Vec<SaslMechanism>) -> BrokerConfig {
    let mut config = sasl_plaintext_config(log_dir);
    config.enabled_sasl_mechanisms = mechanisms;
    config
}

/// Retain an ephemeral log directory beside its mechanism-specific configuration.
///
/// # Panics
/// Panics if the temporary directory cannot be created.
pub fn sasl_temp_config(mechanisms: Vec<SaslMechanism>) -> (TempDir, BrokerConfig) {
    let dir = tempfile::tempdir().unwrap();
    let config = sasl_plaintext_mechanisms(dir.path().to_path_buf(), mechanisms);
    (dir, config)
}

/// The KIP-48 fixture's secret, absolute lifetime ceiling and initial renew window.
pub fn delegation_token_defaults(config: &mut BrokerConfig, key: &[u8]) {
    config.delegation_token_secret_key = Some(krabka_security::SecretBytes::new(key.to_vec()));
    config.delegation_token_max_lifetime = krabka_units::days(7);
    config.delegation_token_default_renew_period = krabka_units::hours(24);
}

/// Enable PLAIN and seed a fixture's users and sole super-user.
pub fn sasl_plaintext_with_users(
    log_dir: PathBuf,
    super_user: &str,
    users: &[(&str, &str)],
) -> BrokerConfig {
    let mut cfg =
        crate::support::sasl::sasl_plaintext_mechanisms(log_dir, vec![SaslMechanism::Plain]);
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
    start_configured_users(super_user, users, |_| {})
}

/// PLAIN fixture with ACL authorization and an anonymous controller heartbeat.
pub fn start_sasl_plaintext_with_acl_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    start_configured_users(super_user, users, |config| {
        config.authorizer =
            std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
                config
                    .super_users
                    .iter()
                    .cloned()
                    .chain(std::iter::once("ANONYMOUS".to_string()))
                    .collect(),
            ));
    })
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

/// Start a bare PLAINTEXT broker, with optional caller-owned configuration.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub fn start_plaintext_configured(
    customize: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    customize(&mut cfg);
    start_broker(cfg, log_dir)
}

/// Start a single-broker PLAINTEXT cluster without authentication.
pub async fn start_single_broker_plaintext() -> (BrokerHandle, TempDir, SocketAddr) {
    start_plaintext_configured(|_| {}).await
}

/// Retain the directory and apply authorization before creating the broker-start future.
fn start_configured_users(
    super_user: &str,
    users: &[(&str, &str)],
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let dir = tempfile::tempdir().unwrap();
    let mut config = sasl_plaintext_with_users(dir.path().to_path_buf(), super_user, users);
    configure(&mut config);
    start_broker(config, dir)
}
