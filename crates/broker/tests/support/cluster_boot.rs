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

/// Shut brokers down in vector order, retaining the original tuple destructuring.
pub async fn shutdown_cluster(cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>) {
    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

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
    let mut cfg = crate::support::node_config(i, log_dir);
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[i];
    // `controller_quorum_voters` carries `<host>:<port>` strings (the dialer
    // re-resolves per connect); test voter sets are built from `SocketAddr`s,
    // so stringify here.
    cfg.controller_quorum_voters = crate::support::controller_voters(voters);
    cfg.bootstrap_mode = mode;
    cfg
}

/// The held endpoints and voter map shared by the nodes of a role-separated cluster.
pub struct RoleTopology<'a> {
    clients: &'a [SocketAddr],
    controllers: &'a [SocketAddr],
    voters: &'a [(u64, SocketAddr)],
}

impl<'a> RoleTopology<'a> {
    pub fn new(
        clients: &'a [SocketAddr],
        controllers: &'a [SocketAddr],
        voters: &'a [(u64, SocketAddr)],
    ) -> Self {
        Self {
            clients,
            controllers,
            voters,
        }
    }

    /// Apply exactly one role after the ordinary static-voter configuration.
    ///
    /// # Panics
    /// Panics if the node index or checked broker id is out of range.
    pub fn config(
        &self,
        index: usize,
        log_dir: &std::path::Path,
        mode: BootstrapMode,
        role: krabka_broker::config::NodeRole,
    ) -> BrokerConfig {
        let mut config = broker_config(
            index,
            self.clients,
            self.controllers,
            self.voters,
            log_dir,
            mode,
        );
        config.roles = vec![role];
        config
    }
}

/// Consume the held controller and data sockets in the same order before startup.
///
/// # Panics
/// Panics if either listener iterator is exhausted or startup fails.
pub async fn start_held_node(
    config: BrokerConfig,
    controllers: &mut std::vec::IntoIter<tokio::net::TcpListener>,
    clients: &mut std::vec::IntoIter<tokio::net::TcpListener>,
    context: &str,
) -> BrokerHandle {
    // Keep node startup off the cluster fixture's stack as LogConfig grows.
    Box::pin(Broker::start_with_listeners(
        config,
        Some(controllers.next().unwrap()),
        Some(clients.next().unwrap()),
    ))
    .await
    .expect(context)
}

/// Boot an `n`-broker cluster with ephemeral ports and short raft timings
/// through **static multi-voter bootstrap** (KIP-595 static-quorum bootstrap):
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

/// Boot a static-voter cluster and await its registration on every broker.
///
/// # Panics
/// Panics if the cluster cannot start or its broker count cannot fit `usize`.
pub async fn registered_cluster(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let cluster = start_n_node_with_retry(n).await;
    wait_for_all_brokers_registered(&cluster, usize::try_from(n).expect("broker count")).await;
    cluster
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
        let attempt = async {
            // Hold reused ports before startup can initialize the metadata log.
            // A bind failure then leaves a fresh Join log empty for the retry.
            let controller = if cfg.is_controller() && cfg.controller_listen_addr.port() != 0 {
                Some(tokio::net::TcpListener::bind(cfg.controller_listen_addr).await?)
            } else {
                None
            };
            let mut data = Vec::new();
            for spec in cfg.effective_listeners() {
                if spec.bind_addr.port() != 0 {
                    data.push(tokio::net::TcpListener::bind(spec.bind_addr).await?);
                }
            }
            Broker::start_with_listeners(cfg.clone(), controller, data).await
        }
        .await;
        match attempt {
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

/// Selects two non-controller nodes in ascending cluster order.
/// Removing the second leaves the first node's index unchanged.
///
/// # Panics
/// Panics unless exactly two nodes are followers of the elected controller.
pub async fn two_controller_followers(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
) -> (NodeId, usize, usize) {
    let leader = cluster[0].0.wait_until_controller_leader().await;
    let followers: Vec<usize> = (0..cluster.len())
        .filter(|&i| cluster[i].0.node_id() != leader.0)
        .collect();
    assert2::assert!(
        followers.len() == 2,
        "a three-node cluster has two non-controller nodes"
    );
    (leader, followers[0], followers[1])
}

/// Start the first held client/controller pair, releasing spare client sockets first.
///
/// # Panics
/// Panics if either vector is empty or broker startup fails.
pub async fn start_first_held(
    config: BrokerConfig,
    clients: Vec<tokio::net::TcpListener>,
    controllers: Vec<tokio::net::TcpListener>,
    context: &str,
) -> BrokerHandle {
    let data_listener = clients.into_iter().next().unwrap();
    let controller_listener = controllers.into_iter().next().unwrap();
    Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
        .await
        .expect(context)
}
