//! Boot wrappers and readiness waits for an `n`-broker cluster.
//!
//! [`start_n_node`] and its retrying twin drive the static-voter bootstrap in
//! [`super::cluster`], [`start_reusing_addrs`] brings one node back up on the
//! addresses it has just vacated, and [`wait_for_all_brokers_registered`]
//! blocks until every controller image has seen the whole cluster.
//! [`broker_config`] builds one node's config for the suites that drive
//! membership changes themselves.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use tempfile::TempDir;

use super::cluster::start_n_node_with;

// The functions below are only meaningful on non-Windows targets because
// openraft's debug_assert! races on the hosted Windows task scheduler.
// Individual test files gate their use with ``.

/// Build a `BrokerConfig` for broker `i` (0-indexed) in an `n`-broker
/// cluster from the supplied ephemeral port lists and static voter map.
/// This is the *static-voter* bootstrap-then-join helper. It exists for tests
/// such as `elect_leaders` that drive `add_learner` and `change_membership`
/// manually and need extra config overrides per broker. `start_n_node`'s
/// auto-join path cannot support that flow.
pub fn broker_config(
    i: usize,
    client_addrs: &[SocketAddr],
    controller_addrs: &[SocketAddr],
    voters: &[(u64, SocketAddr)],
    log_dir: &std::path::Path,
    mode: BootstrapMode,
) -> BrokerConfig {
    let listen = client_addrs[i];
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(i + 1).unwrap();
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.node_id = NodeId(u64::try_from(i + 1).unwrap());
    cfg.controller_listen_addr = controller_addrs[i];
    // `controller_quorum_voters` carries `<host>:<port>` strings (the dialer
    // re-resolves per connect); test voter sets are built from `SocketAddr`s,
    // so stringify here.
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, a)| (NodeId(*id), a.to_string()))
        .collect();
    cfg.bootstrap_mode = mode;
    cfg
}

/// Boot an `n`-broker cluster with ephemeral ports and short raft timings
/// through **static multi-voter bootstrap** (KIP-595 Slice 3c):
///
/// * All `n` brokers boot in `Bootstrap` mode (`auto_join = false`), each
///   configured with the *same* `controller_quorum_voters` = the full
///   `[(1, ctrl_addr_1), …, (n, ctrl_addr_n)]` set.
/// * Each node seeds the full static voter set, and the nodes elect a leader
///   among themselves over the real KIP-595 wire. There is no `AddRaftVoter`
///   and no auto-join.
///
/// Blocks until a leader emerges and reports the full `n`-voter committed set.
/// Returns `(handle, config, tempdir)` triples in spawn order.
/// `cluster[0]` is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    start_n_node_with(n, |_, _| {}).await
}

/// Retry `start_n_node` up to 3 times. Short raft timings sometimes
/// split-vote on slow runners. A fresh tempdir and port set on retry
/// clears the openraft state and usually succeeds within 2 attempts.
pub async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "cluster start failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_err:?}");
}

/// Start a broker on listen addresses another broker has just vacated.
///
/// [`BrokerHandle::shutdown`] awaits its listener tasks, so the sockets are
/// closed by the time it returns, but the port can still be unbindable for a
/// moment afterwards, and a concurrently-running test binary can win the race
/// for the freed ephemeral port. Both surface as `AddrInUse` on the re-bind.
/// Retry briefly instead of failing the test on a port-reuse race, in the
/// spirit of [`start_n_node_with_retry`].
pub async fn start_reusing_addrs(cfg: &BrokerConfig, what: &str) -> BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Broker::start(cfg.clone()).await {
            Ok(handle) => return handle,
            Err(BrokerError::Io(e))
                if e.kind() == std::io::ErrorKind::AddrInUse && Instant::now() < deadline =>
            {
                tracing::warn!(%what, error = %e, "vacated port not yet bindable; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("{what}: {e:?}"),
        }
    }
}

/// Await every broker's controller image until each one sees `n` brokers
/// registered. Call this before any test that needs the partition's replica
/// set to include all `n` nodes. `CreateTopics` reads `image.brokers()` to pick
/// replicas, and a race here silently degrades to a smaller replica set.
///
/// This helper uses the panicking `wait_until_brokers_registered` awaiter on
/// purpose. Tests call this helper directly, not through the
/// `start_n_node_with_retry` path, so a timeout must fail the test.
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(n).await;
    }
}
