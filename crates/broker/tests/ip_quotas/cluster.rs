//! Boots the single-broker clusters the suite runs its scenarios against.
//!
//! Three shapes are needed. The quota round-trip talks to a SASL/PLAINTEXT
//! listener because `AlterClientQuotas` needs an authenticated super-user; the
//! accept-path throttle tests use a bare PLAINTEXT listener and seed the quota
//! through the metadata record instead; and the connection-cap test needs a
//! PLAINTEXT listener started with explicit `max_connections` and
//! `max_connections_per_ip` values.

/// Starts a single-broker PLAINTEXT cluster, with no SASL. Returns
/// `(handle, _dir, addr)`.
pub use crate::support::sasl::start_single_broker_plaintext;
pub use crate::support::sasl::start_single_broker_sasl_plaintext_with_users;
use {
    crate::support::sasl::start_plaintext_configured, krabka_broker::BrokerHandle,
    std::net::SocketAddr, tempfile::TempDir,
};

/// Starts a single-broker PLAINTEXT cluster with explicit connection caps,
/// `max.connections` and `max.connections.per.ip`. Returns
/// `(handle, _dir, addr)`.
pub async fn start_single_broker_plaintext_with_conn_caps(
    max_connections: usize,
    max_connections_per_ip: usize,
) -> (BrokerHandle, TempDir, SocketAddr) {
    start_plaintext_configured(|cfg| {
        cfg.max_connections = max_connections;
        cfg.max_connections_per_ip = max_connections_per_ip;
    })
    .await
}

/// Starts a single-broker PLAINTEXT cluster whose client quotas measure over
/// Kafka's default window, 11 samples of 1 s, rather than the one sample the
/// test config sets. Returns `(handle, _dir, addr)`.
///
/// Kafka's `Rate` pads a window to `quota.window.num - 1` samples, so with
/// one sample a new `ip` entity's first connection is measured over 1 ms and
/// throttled at any rate. Kafka's default window lets a rate of one a second
/// admit ten connections at once and throttle the eleventh.
pub async fn start_single_broker_plaintext_with_kafka_quota_window()
-> (BrokerHandle, TempDir, SocketAddr) {
    start_plaintext_configured(|cfg| {
        cfg.quota_window = krabka_units::secs(11);
        cfg.quota_throttle_max = krabka_units::secs(1);
    })
    .await
}
