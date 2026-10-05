//! Setup shared by the two static mixed-quorum spikes: the JVM controller image
//! name, the cluster-id encoding both implementations must agree on, the
//! `BrokerConfig` builder for one Krabka controller voter, and the format
//! that pins the voters to the JVM voter's release.
//!
//! Both spikes boot the same topology and differ only in what they do to it
//! afterwards, so the topology lives here and each spike file holds one
//! scenario.

use std::{net::SocketAddr, process::Command};

use base64::Engine as _;
use krabka_broker::{BootstrapMode, BrokerConfig};
use uuid::Uuid;

pub(crate) const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

/// Kafka encodes a 16-byte UUID as URL-safe base64 with no padding. The JVM
/// `--cluster-id` string and Krabka's `uuid::Uuid` must wrap the *same* 16
/// bytes. Otherwise the two sides reject each other on a cluster-id
/// mismatch.
pub(crate) fn kafka_cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

/// Builds a Krabka controller `BrokerConfig` for voter `i` in the shared static
/// 3-voter set, with the shared cluster id. `i` is 0-indexed, and the id is
/// `i+1`.
pub(crate) fn krabka_controller_config(
    i: usize,
    own_client_addr: SocketAddr,
    own_controller_addr: SocketAddr,
    voters: &[(u64, SocketAddr)],
    cluster_id: Uuid,
    log_dir: &std::path::Path,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.node_id = krabka_broker::NodeId(u64::try_from(i + 1).unwrap());
    cfg.listen_addr = own_client_addr;
    cfg.advertised_listener = own_client_addr.to_string();
    cfg.controller_listen_addr = own_controller_addr;
    // Outside the 100 lowest ids, which Kafka reserves and the broker refuses.
    cfg.directory_id = Uuid::from_u64_pair(1, cfg.node_id.0);
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg.cluster_id = Some(cluster_id);
    // The bootstrap log carries the feature levels `format_at_kafka_4_0`
    // formats, so the JVM controller can build its FeaturesImage.
    cfg
}

/// Formats a Krabka voter's log directory at Kafka 4.0's `metadata.version`.
///
/// The JVM voter runs 4.0.0, which supports `metadata.version` only up to
/// `4.0-IV3`. Self-bootstrapped, the Krabka voters would finalize Kafka 4.3's
/// `4.3-IV0`, a level the JVM controller cannot replay, so they format at the
/// release the oldest voter runs, as a Kafka operator mixing in a 4.0 node
/// would. The formatter runs in process because a Bazel test sandbox has no
/// Cargo working tree to spawn it from.
pub(crate) async fn format_at_kafka_4_0(log_dir: &std::path::Path, node: &BrokerConfig) {
    let cluster_id = node.cluster_id.expect("the spikes name their cluster id");
    let argv = vec![
        "krabka-format".to_string(),
        "--log-dir".to_string(),
        log_dir.to_str().unwrap().to_string(),
        "--cluster-id".to_string(),
        kafka_cluster_id_string(cluster_id),
        "--node-id".to_string(),
        node.node_id.0.to_string(),
        "--directory-id".to_string(),
        node.directory_id.to_string(),
        "--release-version".to_string(),
        "4.0".to_string(),
    ];
    let code = krabka_format::run_from_args(argv).await;
    assert2::assert!(code == 0, "krabka-format exited {code}");
}

pub(crate) fn docker_rm(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}
