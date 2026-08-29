//! Setup shared by the two static mixed-quorum spikes: the JVM controller image
//! name, the cluster-id encoding both implementations must agree on, and the
//! `BrokerConfig` builder for one Krabka controller voter.
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
    cfg.directory_id = Uuid::from_u128(u128::from(cfg.node_id.0));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
        .collect();
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg.cluster_id = Some(cluster_id);
    // metadata.version/group.version/transaction.version are seeded into the
    // bootstrap log automatically (KIP-584 `bootstrap_feature_records`, fired
    // when the static voter set is derived), so the JVM controller can build
    // its FeaturesImage.
    cfg
}

pub(crate) fn docker_rm(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}
