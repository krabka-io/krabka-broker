//! Component B integration test, with a controller-only node and broker-only
//! observers.
//!
//! An observer replicates metadata through fetch, not through openraft. A
//! `CreateTopics` forwarded through the observer reaches the controller and
//! comes back to the observer's image. The observer never joins the voter
//! set.
//!
//! The second test covers the other half of role separation: the brokers stay
//! *unfenced*. A broker's `BrokerHeartbeat` goes to the controller leader's
//! CONTROLLER listener, which is the only endpoint a controller-only node
//! publishes for itself, and the controller's liveness registry fences every
//! registered broker it does not hear from within `heartbeat_timeout`. It also
//! covers what the metadata surface then advertises: `controller_id` has to
//! name a broker the caller can resolve out of the same response, which the
//! controller-only node's own id never is.

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerHandle, config::NodeRole};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_cluster_request::DescribeClusterRequest,
    metadata_request::MetadataRequest,
};
use tempfile::TempDir;

mod support;

/// A booted role-separated cluster: one controller-only node that is the sole
/// voter, and `n` broker-only observers.
struct RoleSeparated {
    controller: BrokerHandle,
    brokers: Vec<BrokerHandle>,
    // Dropping these removes the log dirs the nodes still hold open.
    _dirs: Vec<TempDir>,
}

impl RoleSeparated {
    /// Every node's handle, controller first. The fencing state is replicated,
    /// so an assertion about it has to hold on all of them.
    fn nodes(&self) -> impl Iterator<Item = &BrokerHandle> {
        std::iter::once(&self.controller).chain(&self.brokers)
    }

    async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

/// Boot node 1 as controller-only and nodes `2..=brokers + 1` as broker-only.
///
/// Node 1 is the whole voter set, so it elects itself and the observers reach
/// it by fetching `__cluster_metadata`. The controller is up and leading
/// before the first observer starts, so an observer's first fetch already has
/// a committed log to replicate.
async fn start_role_separated(brokers: usize) -> RoleSeparated {
    let nodes = brokers + 1;
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(nodes).await;
    let voters = vec![(1u64, controller_addrs[0])];
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let mut dirs = Vec::with_capacity(nodes);

    let ctrl_dir = TempDir::new().unwrap();
    let mut ctrl_cfg = support::broker_config(
        0,
        &client_addrs,
        &controller_addrs,
        &voters,
        ctrl_dir.path(),
        BootstrapMode::Bootstrap,
    );
    ctrl_cfg.roles = vec![NodeRole::Controller];
    let controller = Broker::start_with_listeners(
        ctrl_cfg,
        Some(ctrl_ls.next().unwrap()),
        Some(data_ls.next().unwrap()),
    )
    .await
    .expect("controller-only start");
    dirs.push(ctrl_dir);
    controller.wait_until_controller_leader().await;

    let mut observers = Vec::with_capacity(brokers);
    for index in 1..nodes {
        let dir = TempDir::new().unwrap();
        let mut cfg = support::broker_config(
            index,
            &client_addrs,
            &controller_addrs,
            &voters,
            dir.path(),
            BootstrapMode::Join,
        );
        cfg.roles = vec![NodeRole::Broker];
        observers.push(
            Broker::start_with_listeners(
                cfg,
                Some(ctrl_ls.next().unwrap()),
                Some(data_ls.next().unwrap()),
            )
            .await
            .expect("broker-only start"),
        );
        dirs.push(dir);
    }

    // A broker-only node self-registers (it IS a broker) by forwarding the
    // registration to the controller. Wait until the controller's committed
    // image reflects every one of them, so `CreateTopics` has brokers to place
    // replicas on.
    controller.wait_until_brokers_registered(brokers).await;
    RoleSeparated {
        controller,
        brokers: observers,
        _dirs: dirs,
    }
}

/// Long enough for a controller to notice that a broker has gone silent:
/// `heartbeat_timeout` (2s under the test config) for the session to expire,
/// plus a `liveness_tick_interval` (1s) for the tick that publishes the
/// decision, plus slack.
const FENCING_WINDOW: Duration = Duration::from_secs(4);

/// Every broker any node currently reports as fenced.
///
/// The fencing decision is replicated as the `broker.fenced` broker config, so
/// an observer's image carries it as surely as the controller's.
fn fenced_anywhere(cluster: &RoleSeparated) -> BTreeSet<u64> {
    cluster
        .nodes()
        .flat_map(BrokerHandle::fenced_broker_ids_for_test)
        .collect()
}

/// Sleep past [`FENCING_WINDOW`], then require that no broker is fenced.
///
/// Sampling right after boot proves nothing: a leadership change seeds every
/// registered broker alive, so a cluster whose heartbeats never arrive still
/// looks healthy until the first session expires. Waiting out that window
/// first is what makes the assertion mean "the heartbeats are landing".
async fn assert_settled_unfenced(cluster: &RoleSeparated) {
    tokio::time::sleep(FENCING_WINDOW).await;
    let fenced = fenced_anywhere(cluster);
    assert!(
        fenced.is_empty(),
        "brokers fenced in a role-separated cluster: {fenced:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_observes_and_forwards() {
    support::init_tracing();

    let cluster = start_role_separated(1).await;
    let broker_only = &cluster.brokers[0];
    let broker_only_id = broker_only.node_id();

    // Settle before asserting anything else. Without this the suite finishes
    // inside the seed window, so it would pass while every broker was on its
    // way to being fenced forever.
    assert_settled_unfenced(&cluster).await;

    // CreateTopics against the broker-only node — forwarded to the controller
    // quorum via the observer's write path.
    let topic = "rolesep-observed";
    let client = Client::builder()
        .bootstrap(broker_only.listen_addr().to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 0,
        "create via broker-only node forwards to the controller and succeeds"
    );

    // Assertion 1: the topic propagates back to the broker-only node's image
    // via observer fetch (it is not a voter, so this cannot be a raft apply).
    broker_only.wait_until_partition_present(topic, 0).await;

    // Assertion 2: the controller itself committed the forwarded topic.
    assert!(
        cluster.controller.has_partition(topic, 0),
        "controller committed the forwarded CreateTopics"
    );

    // Assertion 3: the broker-only node is NOT in the controller's voter set.
    let quorum_voters: BTreeSet<u64> = cluster
        .controller
        .quorum_voters_for_test()
        .into_iter()
        .map(|n| n.0)
        .collect();
    assert!(quorum_voters.contains(&1), "the controller is a voter");
    assert!(
        !quorum_voters.contains(&broker_only_id),
        "the broker-only node must never join the voter quorum"
    );

    cluster.shutdown().await;
}

/// A controller-only node never registers itself as a broker, so the only
/// address it publishes is the CONTROLLER endpoint of its
/// `ControllerRegistrationRecord`. When the heartbeat client looked the leader
/// up as a broker instead, every tick bailed out, no heartbeat ever reached
/// the controller, and roughly one `liveness_tick_interval` after boot the
/// controller published `broker.fenced=true` for every broker in the cluster —
/// permanently, since nothing could ever unfence them.
///
/// So this holds the assertion across several liveness ticks and past
/// `heartbeat_timeout`, rather than sampling it once: the broken behaviour
/// takes a session expiry to show up, and a single early sample would see the
/// seeded-alive state and pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_keep_brokers_unfenced_in_a_role_separated_cluster() {
    support::init_tracing();

    let cluster = start_role_separated(2).await;
    assert_settled_unfenced(&cluster).await;

    // Then hold it across several more liveness ticks, so a cluster that
    // fences on any later tick — rather than on the first expiry — is caught
    // too.
    let hold_until = Instant::now() + FENCING_WINDOW;
    while Instant::now() < hold_until {
        let fenced = fenced_anywhere(&cluster);
        assert!(
            fenced.is_empty(),
            "brokers fenced while every one of them was heartbeating: {fenced:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The client-visible half (KIP-1073): `DescribeCluster` hides fenced
    // brokers unless the caller opts in, so a fenced cluster answers with no
    // broker rows at all.
    for broker in &cluster.brokers {
        let client = Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .build()
            .await
            .unwrap();
        let resp = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        assert!(resp.error_code == 0);
        let rows: BTreeSet<(i32, bool)> = resp
            .brokers
            .iter()
            .map(|row| (row.broker_id, row.is_fenced))
            .collect();
        assert!(
            rows == BTreeSet::from([(2, false), (3, false)]),
            "DescribeCluster on node {} must list both brokers unfenced",
            broker.node_id()
        );
        // The advertised controller has to be one a client can resolve. A
        // client reads `controller_id` back out of the `brokers` array of the
        // same response, so the controller-only node's own id would be as
        // useless to it as the -1 a wholly fenced cluster answers with.
        assert!(
            resp.controller_id == 2 || resp.controller_id == 3,
            "DescribeCluster on node {} advertised controller_id {}, which is not a listed broker",
            broker.node_id(),
            resp.controller_id
        );
    }

    assert_metadata_names_a_reachable_controller(&cluster).await;

    cluster.shutdown().await;
}

/// `Metadata` has to name a `controller_id` the caller can resolve.
///
/// In `KRaft` the field is not the quorum leader: `apache/kafka:4.3.1` answers it
/// with `metadataCache.getRandomAliveBrokerId().orElse(-1)`, an unfenced
/// registered broker. A role-separated cluster is where the difference bites,
/// because the quorum leader is a controller-only node that never appears in
/// the `brokers` array the client resolves the id against.
///
/// So this asserts the id resolves *within the same response*, and that the
/// endpoint it resolves to is one a client can actually reach.
async fn assert_metadata_names_a_reachable_controller(cluster: &RoleSeparated) {
    for broker in &cluster.brokers {
        let client = Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .build()
            .await
            .unwrap();
        let resp = client.send(MetadataRequest::default()).await.unwrap();

        let listed: BTreeSet<i32> = resp.brokers.iter().map(|row| row.node_id).collect();
        assert!(
            listed == BTreeSet::from([2, 3]),
            "Metadata on node {} must list both brokers: {listed:?}",
            broker.node_id()
        );
        let named = resp
            .brokers
            .iter()
            .find(|row| row.node_id == resp.controller_id);
        assert!(
            named.is_some(),
            "Metadata on node {} advertised controller_id {}, which is absent from {listed:?}",
            broker.node_id(),
            resp.controller_id
        );

        // Reachable, not merely listed: the advertised endpoint answers.
        let endpoint = named.unwrap();
        let controller_client = Client::builder()
            .bootstrap(format!("{}:{}", endpoint.host, endpoint.port))
            .build()
            .await
            .unwrap();
        let echoed = controller_client
            .send(MetadataRequest::default())
            .await
            .unwrap();
        assert!(echoed.cluster_id == resp.cluster_id);
    }
}
