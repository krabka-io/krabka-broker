//! A KIP-853 dynamic quorum grown from a `--standalone` controller, end to
//! end, as Apache Kafka's `quorum_reconfiguration_test.py` builds and changes
//! it.
//!
//! Every node names the quorum through `controller.quorum.bootstrap.servers`
//! alone, and none through `controller.quorum.voters`. Controller 3001 is
//! formatted with `--standalone` and elects itself. Brokers 1 and 2, which
//! hold no voter set of their own, have to find it through the bootstrap
//! servers, and then reach its controller listener for their registrations and
//! heartbeats through the voter set they read from the log. Controller 3002 is
//! formatted with `--no-initial-controllers` and starts as an observer.
//! `AddRaftVoter` then makes it a voter, and `RemoveRaftVoter` removes 3001, so
//! 3002 leads and 3001 observes. Every step reads the quorum back with
//! `DescribeQuorum` through a broker, which is what `kafka-metadata-quorum
//! describe --status` sends.
//!
//! The nodes are configured the way the broker binary configures them from a
//! formatted directory: the cluster and directory ids come from the
//! `meta.properties` that `krabka-format` wrote, read as the binary reads it,
//! and a fresh directory starts in `Bootstrap` mode.
#![cfg(test)]

use {
    crate::support::addressed_node_config,
    assert2::assert,
    krabka_broker::{
        BootstrapMode, Broker, BrokerHandle, NodeId, bootstrap, codes, config::NodeRole,
    },
    krabka_client_admin::{AdminClient, AdminClientLike as _, AdminError, RaftVoterEndpoint},
    std::{
        collections::BTreeSet,
        net::SocketAddr,
        path::{Path, PathBuf},
        slice,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{
        net::{TcpListener, TcpSocket, TcpStream},
        time,
    },
};

mod support;

/// The cluster id `kafka-storage format` is given in the system test.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node gets to start, and the quorum to reach a described state.
const DEADLINE: Duration = Duration::from_secs(30);

/// Long enough for a controller to fence a broker it does not hear from: the
/// two-second `heartbeat_timeout` of the test config, a one-second liveness
/// tick to publish the decision, and slack.
const FENCING_WINDOW: Duration = Duration::from_secs(4);

/// One node's listeners and log directory.
struct Node {
    id: u64,
    client: TcpListener,
    /// The controller listener, held from the start. `None` for a node whose
    /// port stays closed until it starts, so a dial to it is refused.
    controller: Option<TcpListener>,
    client_addr: SocketAddr,
    controller_addr: SocketAddr,
    dir: TempDir,
}

/// Formats this node's log directory with `krabka-format` and the dynamic
/// quorum flags in `quorum`, in process, as the system test runs
/// `kafka-storage format` before it starts each node.
async fn format(node: &Node, quorum: &[&str]) -> PathBuf {
    let log_dir = node.dir.path().join("log");
    let node_id = node.id.to_string();
    let mut argv = vec![
        "krabka-format",
        "--log-dir",
        log_dir.to_str().expect("utf-8 temp path"),
        "--cluster-id",
        CLUSTER_ID,
        "--node-id",
        &node_id,
    ];
    argv.extend_from_slice(quorum);
    let code = krabka_format::run_from_args(argv).await;
    assert!(
        code == 0,
        "krabka-format exited {code} for node {}",
        node.id
    );
    log_dir
}

/// A node that finished starting, and the directory that backs it.
struct StartedNode {
    handle: BrokerHandle,
    dir: TempDir,
}

/// Starts `node` with `roles` on its formatted `log_dir`, naming the quorum
/// through `bootstrap` alone, and fails the test when it does not finish
/// starting.
async fn start(
    node: Node,
    roles: Vec<NodeRole>,
    log_dir: &Path,
    bootstrap: &[SocketAddr],
) -> StartedNode {
    let meta =
        bootstrap::initialize_log_dirs(log_dir, &[log_dir.to_path_buf()], NodeId(node.id), None)
            .expect("krabka-format wrote meta.properties");
    let mut config = addressed_node_config()
        .log_dir(log_dir)
        .node(node.id)
        .client(node.client_addr)
        .controller(node.controller_addr)
        .call();
    config.controller_quorum_voters = vec![];
    config.bootstrap_servers = bootstrap.iter().map(ToString::to_string).collect();
    config.roles = roles;
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.cluster_id = Some(meta.cluster_id);
    config.directory_id = meta.directory_id;
    let handle = time::timeout(
        DEADLINE,
        Broker::start_with_listeners(config, node.controller, Some(node.client)),
    )
    .await
    .unwrap_or_else(|_| panic!("node {} did not finish starting in {DEADLINE:?}", node.id))
    .unwrap_or_else(|error| panic!("node {} failed to start: {error}", node.id));
    StartedNode {
        handle,
        dir: node.dir,
    }
}

/// The quorum as `kafka-metadata-quorum describe --status` reads it through a
/// broker: the leader id, the voter ids and the observer ids.
#[derive(Debug, PartialEq)]
struct Described {
    leader_id: i32,
    voters: BTreeSet<i32>,
    observers: BTreeSet<i32>,
}

async fn describe(broker: SocketAddr) -> Result<Described, String> {
    let mut admin = AdminClient::connect(&[broker.to_string()])
        .await
        .map_err(|error| error.to_string())?;
    let quorum = admin
        .describe_metadata_quorum()
        .await
        .map_err(|error| error.to_string())?;
    let ids = |replicas: &[krabka_client_admin::QuorumReplica]| {
        replicas.iter().map(|replica| replica.node_id).collect()
    };
    Ok(Described {
        leader_id: quorum.leader_id,
        voters: ids(&quorum.voters),
        observers: ids(&quorum.observers),
    })
}

/// Waits until `DescribeQuorum` through `broker` names `leader`, exactly the
/// `voters` and exactly the `observers`, as the system test's
/// `check_describe_quorum_output` does.
async fn wait_for_quorum(broker: SocketAddr, leader: i32, voters: &[i32], observers: &[i32]) {
    let expected = Described {
        leader_id: leader,
        voters: voters.iter().copied().collect(),
        observers: observers.iter().copied().collect(),
    };
    let deadline = Instant::now() + DEADLINE;
    loop {
        let described = describe(broker).await;
        if described.as_ref() == Ok(&expected) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the quorum described through {broker} is {described:?}, not {expected:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// Retries `operation` until it succeeds. A voter change is refused while the
/// leader's epoch or an earlier change is uncommitted, or while the new voter
/// has not caught up, and `kafka-metadata-quorum` is simply run again.
async fn until_ok<F, Fut>(what: &str, mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let deadline = Instant::now() + DEADLINE;
    loop {
        let outcome = operation().await;
        if outcome.is_ok() {
            return;
        }
        assert!(Instant::now() <= deadline, "{what}: {outcome:?}");
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// The result of one voter change. A retry after an attempt whose response was
/// lost answers `applied`, `DUPLICATE_VOTER` for an add or `VOTER_NOT_FOUND`
/// for a remove, because the change is already in. That counts as done:
/// `wait_for_quorum` then checks the whole voter set.
fn voter_change(result: Result<(), AdminError>, applied: i16) -> Result<(), String> {
    match result {
        Err(AdminError::Broker { code, .. }) if code == applied => Ok(()),
        other => other.map_err(|error| error.to_string()),
    }
}

#[derive(Default)]
struct BootstrapQuorumScenario {
    first: Option<StartedNode>,
    second: Option<StartedNode>,
    brokers: Vec<StartedNode>,
    pending_second: Option<Node>,
    broker_nodes: Vec<Node>,
    pending_controller: Option<TcpSocket>,
    bootstrap: Vec<SocketAddr>,
    through: Option<SocketAddr>,
    second_directory: Option<uuid::Uuid>,
}
#[rstest::fixture]
fn bootstrap_quorum() -> BootstrapQuorumScenario {
    let first = None;
    BootstrapQuorumScenario {
        first,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "standalone controller 3001 leads alone, controller 3002's bootstrap port refuses connections and broker nodes 1 and 2 have only both bootstrap controller endpoints"
)]
async fn standalone_bootstrap_quorum(bootstrap_quorum: &mut BootstrapQuorumScenario) {
    support::init_tracing();
    let (client_addrs, controller_addrs, clients, controllers) =
        support::bind_and_hold_ports(4).await;
    let mut nodes = [3001, 3002, 1, 2]
        .into_iter()
        .zip(clients.into_iter().zip(controllers))
        .zip(client_addrs.into_iter().zip(controller_addrs))
        .map(
            |((id, (client, controller)), (client_addr, controller_addr))| Node {
                id,
                client,
                controller: Some(controller),
                client_addr,
                controller_addr,
                dir: TempDir::new().expect("tempdir"),
            },
        );
    let (first, mut second, broker_one, broker_two) = (
        nodes.next().expect("node 3001"),
        nodes.next().expect("node 3002"),
        nodes.next().expect("node 1"),
        nodes.next().expect("node 2"),
    );
    // Reserve the stopped controller's port without accepting connections.
    let pending_controller = TcpSocket::new_v4().expect("controller socket");
    pending_controller
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("reserve controller port");
    second.controller_addr = pending_controller.local_addr().expect("controller address");
    let (first_controller, second_controller) = (first.controller_addr, second.controller_addr);
    // Every node names both controllers, as the system test does, while 3002
    // is not running yet: its port refuses, and it is in its own list.
    drop(second.controller.take());
    assert!(TcpListener::bind(second_controller).await.is_err());
    assert!(TcpStream::connect(second_controller).await.is_err());
    let bootstrap = [first_controller, second_controller];

    // The standalone controller is the whole voter set and elects itself.
    let listener = first_controller.to_string();
    let log_dir = format(&first, &[
        "--standalone",
        "--controller-listener",
        &listener,
    ])
    .await;
    let first = start(first, vec![NodeRole::Controller], &log_dir, &bootstrap).await;
    first
        .handle
        .wait_until_controller_leader()
        .await
        .expect("standalone leader");

    bootstrap_quorum.first = Some(first);
    bootstrap_quorum.pending_second = Some(second);
    bootstrap_quorum.broker_nodes = vec![broker_one, broker_two];
    bootstrap_quorum.pending_controller = Some(pending_controller);
    bootstrap_quorum.bootstrap = bootstrap.to_vec();
}
#[rstest_bdd_macros::when(
    "brokers 1 and 2 start using controller bootstrap discovery without a local voter set"
)]
async fn discover_standalone_quorum(bootstrap_quorum: &mut BootstrapQuorumScenario) {
    for broker in mem::take(&mut bootstrap_quorum.broker_nodes) {
        let log_dir = format(&broker, &[]).await;
        bootstrap_quorum.brokers.push(
            start(
                broker,
                vec![NodeRole::Broker],
                &log_dir,
                &bootstrap_quorum.bootstrap,
            )
            .await,
        );
    }
    bootstrap_quorum
        .first
        .as_ref()
        .expect("standalone controller")
        .handle
        .wait_until_brokers_registered(2)
        .await;
    bootstrap_quorum.through = Some(bootstrap_quorum.brokers[0].handle.listen_addr());
}
#[rstest_bdd_macros::then(
    "DescribeQuorum through broker 1 names leader {leader}, voters {voters} and observers {observers}"
)]
async fn described_bootstrap_quorum(
    bootstrap_quorum: &BootstrapQuorumScenario,
    leader: i32,
    voters: String,
    observers: String,
) {
    let ids = |value: &str| {
        value
            .split(',')
            .map(|id| id.parse::<i32>().expect("quorum node ID"))
            .collect::<Vec<_>>()
    };
    wait_for_quorum(
        bootstrap_quorum.through.expect("broker discovery endpoint"),
        leader,
        &ids(&voters),
        &ids(&observers),
    )
    .await;
}
#[rstest_bdd_macros::when(
    "controller 3002 starts with no initial controllers and discovers the standalone quorum as an observer"
)]
async fn start_observing_controller(bootstrap_quorum: &mut BootstrapQuorumScenario) {
    let mut node = bootstrap_quorum
        .pending_second
        .take()
        .expect("reserved observer node");
    let log_dir = format(&node, &["--no-initial-controllers"]).await;
    node.controller = Some(
        bootstrap_quorum
            .pending_controller
            .take()
            .expect("reserved controller socket")
            .listen(1024)
            .expect("controller listener"),
    );
    bootstrap_quorum.second = Some(
        start(
            node,
            vec![NodeRole::Controller],
            &log_dir,
            &bootstrap_quorum.bootstrap,
        )
        .await,
    );
    bootstrap_quorum.second_directory =
        Some(bootstrap::read_directory_id(&log_dir).expect("joining directory ID"));
}
#[rstest_bdd_macros::when(
    "the admin adds controller 3002 as a voter through bootstrap controller 3001"
)]
async fn add_bootstrap_controller_voter(bootstrap_quorum: &mut BootstrapQuorumScenario) {
    let first = bootstrap_quorum.bootstrap[0];
    let second = bootstrap_quorum.bootstrap[1];
    let directory = bootstrap_quorum
        .second_directory
        .expect("joining voter directory");
    let endpoint = RaftVoterEndpoint::new("CONTROLLER", second.ip().to_string(), second.port())
        .expect("controller endpoint");
    until_ok("AddRaftVoter(3002)", || async {
        let mut admin = AdminClient::connect_controller(&[first.to_string()])
            .await
            .map_err(|error| error.to_string())?;
        voter_change(
            admin
                .add_raft_voter(None, 3002, directory, slice::from_ref(&endpoint))
                .await,
            codes::DUPLICATE_VOTER,
        )
    })
    .await;
}
#[rstest_bdd_macros::when(
    "the admin removes current leader 3001 using both controller bootstrap endpoints"
)]
async fn remove_bootstrap_controller_leader(bootstrap_quorum: &mut BootstrapQuorumScenario) {
    let directory = bootstrap_quorum
        .first
        .as_ref()
        .expect("initial controller")
        .handle
        .voter_directory_id_for_test(NodeId(3001))
        .expect("initial voter directory");
    until_ok("RemoveRaftVoter(3001)", || async {
        let mut admin = AdminClient::connect_controller(
            &bootstrap_quorum
                .bootstrap
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(|error| error.to_string())?;
        voter_change(
            admin.remove_raft_voter(None, 3001, directory).await,
            codes::VOTER_NOT_FOUND,
        )
    })
    .await;
}
#[rstest_bdd_macros::then(
    "neither broker is fenced after a full heartbeat session under controller 3002"
)]
async fn live_brokers_after_controller_reconfiguration(
    bootstrap_quorum: &mut BootstrapQuorumScenario,
) {
    time::sleep(FENCING_WINDOW).await;
    let fenced = bootstrap_quorum
        .second
        .as_ref()
        .expect("replacement controller")
        .handle
        .fenced_broker_ids_for_test();
    assert!(
        fenced.is_empty(),
        "fenced under replacement controller: {fenced:?}"
    );
    for StartedNode { handle, dir: _dir } in mem::take(&mut bootstrap_quorum.brokers) {
        handle.shutdown().await;
    }
    bootstrap_quorum
        .second
        .take()
        .expect("replacement controller")
        .handle
        .shutdown()
        .await;
    bootstrap_quorum
        .first
        .take()
        .expect("former controller")
        .handle
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/bootstrap_quorum_reconfiguration.feature",
    name = "Bootstrap discovery survives adding an observer and removing the original leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_and_controllers_find_a_standalone_quorum_through_bootstrap_servers(
    bootstrap_quorum: BootstrapQuorumScenario,
) {
}

#[derive(Default)]
struct WildcardControllerAdvertisement {
    node: Option<Node>,
    log_dir: Option<PathBuf>,
    controller: Option<StartedNode>,
    reachable: Option<SocketAddr>,
}
#[rstest::fixture]
fn wildcard_advertisement() -> WildcardControllerAdvertisement {
    let node = None;
    WildcardControllerAdvertisement {
        node,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "standalone controller 3001 is formatted with a loopback voter endpoint but binds its controller listener to 0.0.0.0"
)]
async fn wildcard_controller_endpoint(
    wildcard_advertisement: &mut WildcardControllerAdvertisement,
) {
    support::init_tracing();
    let controller = TcpListener::bind("0.0.0.0:0").await.expect("bind");
    let controller_addr = controller.local_addr().expect("bound address");
    let client = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let node = Node {
        id: 3001,
        client_addr: client.local_addr().expect("bound address"),
        client,
        controller: Some(controller),
        controller_addr,
        dir: TempDir::new().expect("tempdir"),
    };
    let reachable: SocketAddr = ([127, 0, 0, 1], controller_addr.port()).into();
    let listener = reachable.to_string();
    let log_dir = format(&node, &["--standalone", "--controller-listener", &listener]).await;
    wildcard_advertisement.node = Some(node);
    wildcard_advertisement.log_dir = Some(log_dir);
    wildcard_advertisement.reachable = Some(reachable);
}
#[rstest_bdd_macros::when("the wildcard-bound controller starts and elects itself")]
async fn start_wildcard_controller(wildcard_advertisement: &mut WildcardControllerAdvertisement) {
    let node = wildcard_advertisement
        .node
        .take()
        .expect("formatted wildcard node");
    let started = start(
        node,
        vec![NodeRole::Controller],
        wildcard_advertisement
            .log_dir
            .as_ref()
            .expect("formatted log directory"),
        &[wildcard_advertisement
            .reachable
            .expect("reachable controller")],
    )
    .await;
    started
        .handle
        .wait_until_controller_leader()
        .await
        .expect("standalone leader");
    wildcard_advertisement.controller = Some(started);
}
#[rstest_bdd_macros::then(
    "the voter set advertises this machine's hostname on the bound controller port with listener name CONTROLLER"
)]
async fn advertised_wildcard_hostname(
    wildcard_advertisement: &mut WildcardControllerAdvertisement,
) {
    let reachable = wildcard_advertisement
        .reachable
        .expect("reachable controller");
    let controller_addr = reachable;
    let host_name = hostname::get()
        .expect("this machine's host name")
        .into_string()
        .expect("a UTF-8 host name");
    let advertised = vec![krabka_client_admin::QuorumNode {
        node_id: 3001,
        endpoints: vec![
            RaftVoterEndpoint::new("CONTROLLER", host_name, controller_addr.port())
                .expect("endpoint"),
        ],
    }];
    let deadline = Instant::now() + DEADLINE;
    loop {
        let nodes = async {
            let mut admin = AdminClient::connect_controller(&[reachable.to_string()])
                .await
                .map_err(|error| error.to_string())?;
            admin
                .describe_metadata_quorum()
                .await
                .map(|quorum| quorum.nodes)
                .map_err(|error| error.to_string())
        }
        .await;
        if nodes.as_ref() == Ok(&advertised) {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "the voter set advertises {nodes:?}, not {advertised:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
    wildcard_advertisement
        .controller
        .take()
        .expect("wildcard controller")
        .handle
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/bootstrap_quorum_reconfiguration.feature",
    name = "Wildcard controller bindings advertise a reachable hostname"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wildcard_controller_advertises_the_host_name_in_the_voter_set(
    wildcard_advertisement: WildcardControllerAdvertisement,
) {
}

use std::mem;
