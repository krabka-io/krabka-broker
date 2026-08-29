//! Boots the single-broker clusters the suite runs its scenarios against.
//!
//! Three shapes are needed. The quota round-trip talks to a SASL/PLAINTEXT
//! listener because `AlterClientQuotas` needs an authenticated super-user; the
//! accept-path throttle tests use a bare PLAINTEXT listener and seed the quota
//! through the metadata record instead; and the connection-cap test needs a
//! PLAINTEXT listener started with explicit `max_connections` and
//! `max_connections_per_ip` values.

use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

/// Starts a single-broker SASL/PLAINTEXT cluster. Returns
/// `(handle, _dir, addr)`.
pub(crate) fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = std::iter::once(super_user.to_string()).collect();

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

/// Starts a single-broker PLAINTEXT cluster, with no SASL. Returns
/// `(handle, _dir, addr)`.
pub(crate) async fn start_single_broker_plaintext() -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Starts a single-broker PLAINTEXT cluster with explicit connection caps,
/// `max.connections` and `max.connections.per.ip`. Returns
/// `(handle, _dir, addr)`.
pub(crate) async fn start_single_broker_plaintext_with_conn_caps(
    max_connections: usize,
    max_connections_per_ip: usize,
) -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.max_connections = max_connections;
    cfg.max_connections_per_ip = max_connections_per_ip;
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}
