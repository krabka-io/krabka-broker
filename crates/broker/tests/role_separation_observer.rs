//! Role separation: a controller-only node plus broker-only observers.
//!
//! The observers replicate metadata through fetch, not through openraft, and
//! never join the voter set. Two behaviours share that topology.
//!
//! [`broker_only_node_observes_and_forwards`] is the write side. A
//! `CreateTopics` forwarded through an observer reaches the controller and
//! comes back to the observer's image.
//!
//! [`advertised_controller_id_resolves_to_a_broker_row`] is the read side. A
//! client reads `controller_id` out of a `Metadata` or `DescribeCluster`
//! response and resolves it against the broker list in that same response. The
//! controller-only node registers no broker endpoint and so never appears in
//! that list, which is why both APIs advertise a broker instead of the raft
//! leader, the way a `KRaft` broker does.

use std::collections::BTreeSet;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerHandle, config::NodeRole};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    metadata_request::MetadataRequest,
    metadata_response::{MetadataResponse, MetadataResponseBroker},
};
use tempfile::TempDir;

mod support;

/// A booted role-separated cluster: one controller-only node and
/// `broker_count` broker-only observers.
struct RoleSeparated {
    /// Node 1. The sole voter, and never a registered broker.
    controller: BrokerHandle,
    /// Nodes 2..=n. Observers, never voters.
    brokers: Vec<BrokerHandle>,
    /// Held so the log directories outlive the brokers that write them.
    _dirs: Vec<TempDir>,
}

/// Boots one controller-only node (node 1, the only voter, bootstrapped as a
/// singleton so it elects itself) and `broker_count` broker-only nodes that
/// keep their metadata image current by fetching `__cluster_metadata` and
/// forward writes to the controller quorum.
///
/// Returns once every broker's registration is committed on the controller.
async fn start_role_separated(broker_count: usize) -> RoleSeparated {
    support::init_tracing();

    let nodes = broker_count + 1;
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(nodes).await;
    // Only the controller (node 1) is a voter. The broker-only nodes observe
    // via fetch and must never appear in the quorum.
    let voters = vec![(1u64, controller_addrs[0])];

    // Unpack the held listeners for every node up front, before any broker
    // starts, so no node races another for a port.
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let mut held: Vec<(tokio::net::TcpListener, tokio::net::TcpListener)> =
        Vec::with_capacity(nodes);
    for _ in 0..nodes {
        held.push((data_ls.next().unwrap(), ctrl_ls.next().unwrap()));
    }
    let mut held = held.into_iter();

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
    let (data, ctrl) = held.next().unwrap();
    let controller = Broker::start_with_listeners(ctrl_cfg, Some(ctrl), Some(data))
        .await
        .expect("controller-only start");
    dirs.push(ctrl_dir);

    // Wait until the controller is leader before starting the observers, so
    // the first observer fetch already has a committed log to replicate.
    controller.wait_until_controller_leader().await;

    let mut brokers = Vec::with_capacity(broker_count);
    for i in 1..nodes {
        let dir = TempDir::new().unwrap();
        let mut cfg = support::broker_config(
            i,
            &client_addrs,
            &controller_addrs,
            &voters,
            dir.path(),
            BootstrapMode::Join,
        );
        cfg.roles = vec![NodeRole::Broker];
        let (data, ctrl) = held.next().unwrap();
        brokers.push(
            Broker::start_with_listeners(cfg, Some(ctrl), Some(data))
                .await
                .expect("broker-only start"),
        );
        dirs.push(dir);
    }

    // A broker-only node self-registers by forwarding the registration to the
    // controller. Wait until the controller's committed image reflects every
    // one of them, and then until each observer has replicated that image
    // back: an observer answers `Metadata` out of its own copy, which lags the
    // controller by one fetch.
    controller.wait_until_brokers_registered(broker_count).await;
    for broker in &brokers {
        broker.wait_until_brokers_registered(broker_count).await;
    }

    RoleSeparated {
        controller,
        brokers,
        _dirs: dirs,
    }
}

impl RoleSeparated {
    async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_observes_and_forwards() {
    let cluster = start_role_separated(1).await;
    let broker_only = &cluster.brokers[0];
    let broker_only_id = broker_only.node_id();

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

/// The `controller_id` in `Metadata` and `DescribeCluster` must name a node
/// the same response also gives an endpoint for. The controller-only node
/// (node 1) has no broker endpoint anywhere in this cluster, so naming it
/// would hand an `AdminClient` an id it cannot connect to.
///
/// Both responses are compared whole. The broker array is sorted by node id
/// first: the metadata image stores registrations in a hash map, so the wire
/// order of that array is unspecified.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advertised_controller_id_resolves_to_a_broker_row() {
    const REQUESTS: usize = 6;

    let cluster = start_role_separated(2).await;
    let cluster_id = cluster
        .controller
        .controller_image_for_test()
        .cluster_id()
        .to_string();

    // The two broker-only nodes, as the rows every response must carry.
    let mut metadata_rows: Vec<MetadataResponseBroker> = cluster
        .brokers
        .iter()
        .map(|b| MetadataResponseBroker {
            node_id: i32::try_from(b.node_id()).unwrap(),
            host: b.listen_addr().ip().to_string(),
            port: i32::from(b.listen_addr().port()),
            rack: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        })
        .collect();
    metadata_rows.sort_by_key(|row| row.node_id);
    let broker_ids: BTreeSet<i32> = metadata_rows.iter().map(|row| row.node_id).collect();

    // The only responses this cluster may produce: one per advertised broker.
    // Looking a `controller_id` up here is what proves it resolves — an id
    // that is not a broker row has no expected response at all.
    let expected_metadata = |controller_id: i32| -> MetadataResponse {
        assert!(
            broker_ids.contains(&controller_id),
            "advertised controller_id {controller_id} is not one of the brokers {broker_ids:?} \
             this response lists"
        );
        MetadataResponse {
            throttle_time_ms: 0,
            brokers: metadata_rows.clone(),
            cluster_id: Some(cluster_id.clone()),
            controller_id,
            topics: vec![],
            cluster_authorized_operations: i32::MIN,
            error_code: 0,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    };

    let client = Client::builder()
        .bootstrap(cluster.brokers[0].listen_addr().to_string())
        .build()
        .await
        .unwrap();

    // Every response names a broker, and consecutive requests rotate over all
    // of them rather than pinning every client to one node.
    let mut advertised = BTreeSet::new();
    for _ in 0..REQUESTS {
        let mut resp: MetadataResponse = client
            .send(MetadataRequest {
                topics: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();
        resp.brokers.sort_by_key(|row| row.node_id);
        advertised.insert(resp.controller_id);
        assert!(resp == expected_metadata(resp.controller_id));
    }
    assert!(
        advertised == broker_ids,
        "{REQUESTS} Metadata requests should name every broker in turn"
    );

    // DescribeCluster answers with the same node, projected into its own shape.
    let describe_rows: Vec<DescribeClusterBroker> = metadata_rows
        .iter()
        .map(|row| DescribeClusterBroker {
            broker_id: row.node_id,
            host: row.host.clone(),
            port: row.port,
            rack: None,
            is_fenced: false,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        })
        .collect();
    let expected_describe = |controller_id: i32| -> DescribeClusterResponse {
        assert!(
            broker_ids.contains(&controller_id),
            "advertised controller_id {controller_id} is not one of the brokers {broker_ids:?} \
             this response lists"
        );
        DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            endpoint_type: 1,
            cluster_id: cluster_id.clone(),
            controller_id,
            brokers: describe_rows.clone(),
            cluster_authorized_operations: i32::MIN,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    };

    let mut advertised = BTreeSet::new();
    for _ in 0..REQUESTS {
        let mut resp: DescribeClusterResponse = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        resp.brokers.sort_by_key(|row| row.broker_id);
        advertised.insert(resp.controller_id);
        assert!(resp == expected_describe(resp.controller_id));
    }
    assert!(
        advertised == broker_ids,
        "{REQUESTS} DescribeCluster requests should name every broker in turn"
    );

    cluster.shutdown().await;
}

/// The controller-only node serves client APIs on its data listener, and must
/// not name itself there either: it is the raft leader and still has no broker
/// endpoint to offer.
///
/// This response is not compared whole. The node is the active controller, so
/// its broker rows are narrowed by the heartbeat registry and the exact set is
/// a function of timing rather than of the behaviour under test. What is
/// asserted is the property the client depends on: the id it gets back is one
/// of the rows it also got, with a host and a port.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_only_node_never_advertises_itself_as_controller() {
    let cluster = start_role_separated(2).await;
    let controller_node_id = i32::try_from(cluster.controller.node_id()).unwrap();

    let client = Client::builder()
        .bootstrap(cluster.controller.listen_addr().to_string())
        .build()
        .await
        .unwrap();
    let resp: MetadataResponse = client
        .send(MetadataRequest {
            topics: Some(vec![]),
            ..Default::default()
        })
        .await
        .unwrap();

    let named = resp
        .brokers
        .iter()
        .find(|row| row.node_id == resp.controller_id)
        .unwrap_or_else(|| {
            panic!(
                "controller_id {} resolves to no broker row in {:?}",
                resp.controller_id, resp.brokers
            )
        });
    assert!(!named.host.is_empty() && named.port > 0);
    assert!(
        resp.controller_id != controller_node_id,
        "a controller-only node has no broker endpoint and must not name itself"
    );

    cluster.shutdown().await;
}
