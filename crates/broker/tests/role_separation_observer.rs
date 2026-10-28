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
//! controller-only node's own id never is. The controller-only node itself
//! opens no client listener, so a client never reaches it at all.
#![cfg(test)]

use {
    crate::support::{
        NodeIndex, RoleNodeSetup,
        client::{connect_client, create_topic},
        quorum::metadata_quorum_request,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle,
        config::{DEFAULT_READINESS_MAX_METADATA_LAG, NodeRole},
        health::HealthState,
    },
    krabka_client_core::Client,
    krabka_protocol::owned::{
        create_topics_response::CreateTopicsResponse,
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse,
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_quorum_response::DescribeQuorumResponse, metadata_request::MetadataRequest,
        unregister_broker_request::UnregisterBrokerRequest,
        unregister_broker_response::UnregisterBrokerResponse,
    },
    rstest_bdd::datatable::Rows,
    std::{
        collections::BTreeSet,
        fs,
        io::ErrorKind,
        iter, mem,
        net::SocketAddr,
        path::{Path, PathBuf},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{net::TcpStream, time},
};

mod support;

/// A booted role-separated cluster: one controller-only node that is the sole
/// voter, and `n` broker-only observers.
struct RoleSeparated {
    controller: BrokerHandle,
    /// The client address in the controller-only node's config. The harness
    /// hands the node a listener bound on it, which the node closes.
    controller_client_addr: SocketAddr,
    brokers: Vec<BrokerHandle>,
    /// The broker-only nodes' configs, index-aligned with `brokers`, so a test
    /// can stop one and start it again on the same ports and log dir.
    broker_configs: Vec<BrokerConfig>,
    /// The controller-only node's metadata log directory, where its
    /// `__cluster_metadata-0` log and checkpoints live.
    controller_metadata_dir: PathBuf,
    // Dropping these removes the log dirs the nodes still hold open.
    _dirs: Vec<TempDir>,
}

impl RoleSeparated {
    /// Every node's handle, controller first. The fencing state is replicated,
    /// so an assertion about it has to hold on all of them.
    fn nodes(&self) -> impl Iterator<Item = &BrokerHandle> {
        iter::once(&self.controller).chain(&self.brokers)
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
    start_role_separated_with(brokers, |_, _| {}).await
}

/// Like [`start_role_separated`], but each node's config passes through
/// `customize` first. It is called with the node's index, so a test can settle
/// something on the controller (index 0) without also changing the observers.
async fn start_role_separated_with(
    brokers: usize,
    customize: impl Fn(usize, &mut BrokerConfig),
) -> RoleSeparated {
    let nodes = brokers + 1;
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(nodes).await;
    let voters = vec![(1u64, controller_addrs[0])];
    let topology = support::RoleTopology::new(&client_addrs, &controller_addrs, &voters);
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let mut dirs = Vec::with_capacity(nodes);

    let ctrl_dir = TempDir::new().unwrap();
    let mut ctrl_cfg = topology.config(ctrl_dir.path(), RoleNodeSetup {
        role: NodeRole::Controller,
        ..Default::default()
    });
    customize(0, &mut ctrl_cfg);
    let controller_metadata_dir = ctrl_cfg.metadata_dir().to_path_buf();
    let controller_client_addr = ctrl_cfg.listen_addr;
    // Boxed, as is each broker's start below: a node's start future holds
    // its whole `BrokerConfig`, and every test that awaits this helper would
    // otherwise carry it inline, past `clippy::large_futures`.
    let controller = Box::pin(support::start_held_node(
        ctrl_cfg,
        &mut ctrl_ls,
        &mut data_ls,
        "controller-only start",
    ))
    .await;
    dirs.push(ctrl_dir);
    controller.wait_until_controller_leader().await.unwrap();

    let mut observers = Vec::with_capacity(brokers);
    let mut broker_configs = Vec::with_capacity(brokers);
    for index in 1..nodes {
        let dir = TempDir::new().unwrap();
        let mut cfg = topology.config(dir.path(), RoleNodeSetup {
            index: NodeIndex(index),
            mode: BootstrapMode::Join,
            ..Default::default()
        });
        customize(index, &mut cfg);
        broker_configs.push(cfg.clone());
        observers.push(
            Box::pin(support::start_held_node(
                cfg,
                &mut ctrl_ls,
                &mut data_ls,
                "broker-only start",
            ))
            .await,
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
        controller_client_addr,
        brokers: observers,
        broker_configs,
        controller_metadata_dir,
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
/// The fencing decision is replicated on the broker's registration, so
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
    time::sleep(FENCING_WINDOW).await;
    let fenced = fenced_anywhere(cluster);
    assert!(
        fenced.is_empty(),
        "brokers fenced in a role-separated cluster: {fenced:?}"
    );
}

/// A restarted node and the health state its startup marked.
struct RestartedBroker {
    handle: BrokerHandle,
    health: HealthState,
}

/// A broker row of a `DescribeCluster` answer.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ListedBroker {
    broker_id: i32,
    is_fenced: bool,
}

/// Restart a broker-only node on the same ports and log dir it was using, and
/// return the [`HealthState`] its startup marked, so the caller can read the
/// readiness verdict the node would serve on `/readyz`.
///
/// The vacated ports are not always immediately bindable on Linux, so this
/// retries `AddrInUse` the way `support::start_reusing_addrs` does.
async fn restart_broker_only(cfg: &BrokerConfig) -> RestartedBroker {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let health = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
        match Broker::start_with_health(cfg.clone(), health.clone()).await {
            Ok(handle) => return RestartedBroker { handle, health },
            Err(error) if Instant::now() < deadline => {
                tracing::warn!(%error, "broker-only restart failed; retrying");
                time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("broker-only restart: {error:?}"),
        }
    }
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
        let client = connect_client(broker.listen_addr().to_string(), None).await;
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
        let controller_client =
            connect_client(format!("{}:{}", endpoint.host, endpoint.port), None).await;
        let echoed = controller_client
            .send(MetadataRequest::default())
            .await
            .unwrap();
        assert!(echoed.cluster_id == resp.cluster_id);
    }
}

/// The names of the entries directly in `dir`, sorted.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir).map_or_default(|entries| {
        entries
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect()
    });
    names.sort();
    names
}

/// Whether `dir` holds a KIP-630 snapshot, the file Kafka's system test looks
/// for with `ls __cluster_metadata-0/*.checkpoint`.
fn holds_a_checkpoint(dir: &Path) -> bool {
    entries(dir).iter().any(|name| {
        Path::new(name)
            .extension()
            .is_some_and(|ext| ext == "checkpoint")
    })
}

async fn create_topics(client: &Client, topics: &[String]) {
    for topic in topics {
        create_topic(client, topic, 1).await;
    }
}

#[derive(Default)]
struct ObserverBehavior {
    cluster: Option<RoleSeparated>,
    client: Option<Client>,
    created: Option<CreateTopicsResponse>,
    quorum: Option<DescribeQuorumResponse>,
    connected: rstest_bdd::Slot<Result<(), ErrorKind>>,
    unregistered: Option<UnregisterBrokerResponse>,
    doomed: Option<u64>,
    restarted: Option<RestartedBroker>,
    topics: Vec<String>,
    cluster_responses: Vec<(u64, DescribeClusterResponse)>,
}

#[rstest::fixture]
fn observer_behavior() -> ObserverBehavior {
    // Broker-only observers and their controller have independent fresh images.
    ObserverBehavior::default()
}

impl ObserverBehavior {
    async fn stop_observer(&mut self) -> BrokerConfig {
        drop(self.client.take());
        let config = self.cluster().broker_configs[0].clone();
        self.cluster
            .as_mut()
            .expect("role-separated cluster")
            .brokers
            .remove(0)
            .shutdown()
            .await;
        config
    }

    fn cluster(&self) -> &RoleSeparated { self.cluster.as_ref().expect("role-separated cluster") }

    fn client(&self) -> &Client { self.client.as_ref().expect("observer connection") }

    async fn connect(&mut self) {
        self.client =
            Some(connect_client(self.cluster().brokers[0].listen_addr().to_string(), None).await);
    }

    async fn shutdown(&mut self) {
        let Self {
            cluster,
            client,
            restarted,
            ..
        } = mem::take(self);
        if let Some(client) = client {
            client.close();
        }
        if let Some(restarted) = restarted {
            restarted.handle.shutdown().await;
        }
        cluster.expect("role-separated cluster").shutdown().await;
    }
}

#[rstest_bdd_macros::given("controller-only node 1 has {brokers} broker-only observers")]
async fn observer_cluster(observer_behavior: &mut ObserverBehavior, brokers: usize) {
    support::init_tracing();
    observer_behavior.cluster = Some(start_role_separated(brokers).await);
    observer_behavior.connect().await;
}

#[rstest_bdd_macros::given(
    "controller-only node 1 alone requires break-glass approvers User:alice and User:bob, with broker-only nodes 2 and 3"
)]
async fn gated_observer_cluster(observer_behavior: &mut ObserverBehavior) {
    support::init_tracing();
    observer_behavior.cluster = Some(
        start_role_separated_with(2, |index, cfg| {
            if index == 0 {
                cfg.break_glass.approvers = vec!["User:alice".into(), "User:bob".into()];
            }
        })
        .await,
    );
    observer_behavior.connect().await;
}

#[rstest_bdd_macros::given("the broker heartbeats remain unfenced beyond the seed window")]
async fn settled_observers(observer_behavior: &ObserverBehavior) {
    assert_settled_unfenced(observer_behavior.cluster()).await;
}

#[rstest_bdd_macros::when("CreateTopics sends rolesep-observed through broker-only node 2")]
async fn create_via_observer(observer_behavior: &mut ObserverBehavior) {
    observer_behavior.created = Some(
        observer_behavior
            .client()
            .send(create_topic_request(
                creatable_topic().name("rolesep-observed".into()).call(),
            ))
            .await
            .unwrap(),
    );
}

#[rstest_bdd_macros::then(
    "the controller commits rolesep-observed, the observer fetches it and node 2 remains outside the voter quorum"
)]
async fn observed_forwarded_topic(observer_behavior: &ObserverBehavior) {
    assert!(
        observer_behavior
            .created
            .as_ref()
            .expect("CreateTopics answered")
            .topics[0]
            .error_code
            == 0,
        "create via broker-only node forwards to the controller and succeeds"
    );
    observer_behavior.cluster().brokers[0]
        .wait_until_partition_present("rolesep-observed", 0)
        .await;
    assert!(
        observer_behavior
            .cluster()
            .controller
            .has_partition("rolesep-observed", 0),
        "controller committed the forwarded CreateTopics"
    );
    let voters: BTreeSet<u64> = observer_behavior
        .cluster()
        .controller
        .quorum_voters_for_test()
        .into_iter()
        .map(|node| node.0)
        .collect();
    assert!(voters.contains(&1), "the controller is a voter");
    assert!(
        !voters.contains(&observer_behavior.cluster().brokers[0].node_id()),
        "the broker-only node must never join the voter quorum"
    );
}

#[rstest_bdd_macros::when("a TCP client connects to the controller's configured client address")]
async fn connect_controller_client_listener(observer_behavior: &ObserverBehavior) {
    observer_behavior.connected.set(
        TcpStream::connect(observer_behavior.cluster().controller_client_addr)
            .await
            .map(drop)
            .map_err(|error| error.kind()),
    );
}

#[rstest_bdd_macros::then(
    "the connection is refused, the data-plane address is absent and listen_addr names only the controller listener"
)]
fn closed_controller_client_listener(observer_behavior: &ObserverBehavior) {
    assert!(
        (
            observer_behavior.cluster().controller.data_plane_addr(),
            observer_behavior.cluster().controller.listen_addr(),
            observer_behavior
                .connected
                .get()
                .expect("connection attempted")
        ) == (
            None,
            observer_behavior.cluster().controller.controller_addr(),
            Err(ErrorKind::ConnectionRefused)
        )
    );
}

#[rstest_bdd_macros::when(
    "DescribeQuorum requests the metadata partition through broker-only node 2"
)]
async fn describe_observer_quorum(observer_behavior: &mut ObserverBehavior) {
    observer_behavior.quorum = Some(
        observer_behavior
            .client()
            .send(metadata_quorum_request())
            .await
            .unwrap(),
    );
}

#[rstest_bdd_macros::then(
    "the quorum response succeeds for only __cluster_metadata partition 0, led by voter 1 in an epoch of at least 1"
)]
fn forwarded_observer_quorum(observer_behavior: &ObserverBehavior) {
    let response = observer_behavior.quorum.as_ref().expect("quorum described");
    assert!(
        response.error_code == 0,
        "top-level error_code must be NONE"
    );
    assert!(response.topics.len() == 1);
    let topic = &response.topics[0];
    assert!(topic.topic_name == "__cluster_metadata");
    assert!(topic.partitions.len() == 1);
    let partition = &topic.partitions[0];
    assert!(partition.error_code == 0);
    assert!(
        partition.leader_id == 1,
        "controller-only node 1 is the leader; got {}",
        partition.leader_id
    );
    assert!(partition.leader_epoch >= 1, "leader epoch must be >= 1");
    assert!(
        partition
            .current_voters
            .iter()
            .any(|voter| voter.replica_id == 1),
        "voters must include node 1"
    );
}

#[rstest_bdd_macros::when("UnregisterBroker targets node {node} through broker-only node 2")]
async fn unregister_via_observer(observer_behavior: &mut ObserverBehavior, node: u64) {
    observer_behavior.doomed = Some(node);
    observer_behavior.unregistered = Some(
        observer_behavior
            .client()
            .send(UnregisterBrokerRequest {
                broker_id: i32::try_from(node).expect("small node id"),
                ..Default::default()
            })
            .await
            .unwrap(),
    );
}

#[rstest_bdd_macros::then("UnregisterBroker returns error code {code}")]
fn observer_unregister_code(observer_behavior: &ObserverBehavior, code: i16) {
    assert!(
        observer_behavior
            .unregistered
            .as_ref()
            .expect("unregister answered")
            .error_code
            == code
    );
}

#[rstest_bdd_macros::then(
    "the controller retains node 3 and refuses the unapproved request with message"
)]
fn gated_observer_unregister(observer_behavior: &ObserverBehavior, docstring: String) {
    assert!(
        observer_behavior
            .unregistered
            .as_ref()
            .expect("unregister answered")
            .error_message
            .as_deref()
            == Some(docstring.trim())
    );
    assert!(
        observer_behavior
            .cluster()
            .controller
            .controller_image_for_test()
            .broker(krabka_broker::NodeId(
                observer_behavior.doomed.expect("unregistered node")
            ))
            .is_some(),
        "the refused unregistration left the registration alone"
    );
}

#[rstest_bdd_macros::then("the controller removes the target broker from its committed image")]
async fn committed_observer_unregister(observer_behavior: &ObserverBehavior) {
    observer_behavior
        .cluster()
        .controller
        .wait_for_image(|image| {
            image
                .broker(krabka_broker::NodeId(
                    observer_behavior.doomed.expect("unregistered node"),
                ))
                .is_none()
        })
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "broker only node observes and forwards"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_observes_and_forwards(mut observer_behavior: ObserverBehavior) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "controller only node opens no client listener"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_only_node_opens_no_client_listener(mut observer_behavior: ObserverBehavior) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "broker only node forwards describe quorum to controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_forwards_describe_quorum_to_controller(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "unregister broker sent to a broker only node is decided by the controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_broker_sent_to_a_broker_only_node_is_decided_by_the_controller(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "unregister broker sent to a broker only node reaches the controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_broker_sent_to_a_broker_only_node_reaches_the_controller(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[derive(rstest_bdd_macros::DataTableRow)]
struct ObserverTopicInput {
    topic: String,
}

#[rstest_bdd_macros::given(
    "controller 1 snapshots every four records and retains zero metadata-log bytes while broker-only node 2 keeps default checkpoints"
)]
async fn pruning_observer_controller(observer_behavior: &mut ObserverBehavior) {
    support::init_tracing();
    observer_behavior.cluster = Some(
        start_role_separated_with(1, |index, cfg| {
            if index == 0 {
                cfg.metadata_snapshot_interval_records = 4;
                cfg.metadata_log.max_retention_size = Some(krabka_units::bytes(0));
            }
        })
        .await,
    );
    observer_behavior.connect().await;
}

#[rstest_bdd_macros::given(
    "controller 1 and observer 2 separate data and metadata directories, with 2048-byte snapshots and retention, one-second rolls and 50-millisecond metadata idle records"
)]
async fn separate_observer_directories(observer_behavior: &mut ObserverBehavior) {
    support::init_tracing();
    observer_behavior.cluster = Some(
        start_role_separated_with(1, |index, cfg| {
            let root = cfg.log_dir.clone();
            cfg.log_dir = root.join("data");
            cfg.metadata_log_dir = Some(root.join("metadata"));
            if index == 0 {
                cfg.metadata_max_bytes_between_snapshots = krabka_units::bytes(2048);
                cfg.metadata_log = krabka_raft::MetadataLogConfig {
                    segment_roll_interval: krabka_units::secs(1),
                    max_retention_size: Some(krabka_units::bytes(2048)),
                    max_idle_interval: krabka_units::millis(50),
                    ..krabka_raft::MetadataLogConfig::default()
                };
            }
        })
        .await,
    );
    observer_behavior.connect().await;
}

#[rstest_bdd_macros::when("the broker-only observer creates these single-partition topics")]
async fn create_observer_topics(
    observer_behavior: &mut ObserverBehavior,
    #[datatable] rows: Rows<ObserverTopicInput>,
) {
    observer_behavior.topics = rows.into_iter().map(|row| row.topic).collect();
    create_topics(observer_behavior.client(), &observer_behavior.topics).await;
    for topic in &observer_behavior.topics {
        observer_behavior.cluster().brokers[0]
            .wait_until_local_log_end_offset(topic, 0, 0)
            .await;
    }
}

#[rstest_bdd_macros::then("the controller has a checkpoint replacing pruned metadata records")]
fn pruned_observer_checkpoint(observer_behavior: &ObserverBehavior) {
    let checkpoints =
        krabka_raft::metadata_partition_dir(&observer_behavior.cluster().controller_metadata_dir);
    let snapshot_written = fs::read_dir(&checkpoints).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".checkpoint"))
        })
    });
    assert!(
        snapshot_written,
        "the controller should have snapshotted and pruned at a 4-record interval; {checkpoints:?}"
    );
}

#[rstest_bdd_macros::when(
    "broker-only node 2 restarts on its existing addresses and log directories"
)]
async fn restart_pruned_observer(observer_behavior: &mut ObserverBehavior) {
    let config = observer_behavior.stop_observer().await;
    observer_behavior.restarted = Some(restart_broker_only(&config).await);
}

#[rstest_bdd_macros::when(
    "broker-only node 2 stops, both its data and metadata directories are wiped and it restarts on the same addresses"
)]
async fn wipe_pruned_observer(observer_behavior: &mut ObserverBehavior) {
    let config = observer_behavior.stop_observer().await;
    fs::remove_dir_all(&config.log_dir).unwrap();
    fs::remove_dir_all(config.metadata_dir()).unwrap();
    observer_behavior.restarted = Some(restart_broker_only(&config).await);
}

#[rstest_bdd_macros::then(
    "the restarted observer restores every topic, re-registers with the controller and reports ready"
)]
async fn recovered_pruned_observer(observer_behavior: &ObserverBehavior) {
    for topic in &observer_behavior.topics {
        observer_behavior
            .restarted
            .as_ref()
            .expect("restarted observer")
            .handle
            .wait_until_partition_present(topic, 0)
            .await;
    }
    observer_behavior
        .restarted
        .as_ref()
        .expect("restarted observer")
        .handle
        .wait_until_brokers_registered(1)
        .await;
    assert!(
        observer_behavior
            .cluster()
            .controller
            .controller_image_for_test()
            .brokers()
            .any(|broker| broker.node_id.0
                == observer_behavior
                    .restarted
                    .as_ref()
                    .expect("restarted observer")
                    .handle
                    .node_id()),
        "the restarted broker-only node re-registers with the controller"
    );
    assert!(
        observer_behavior
            .restarted
            .as_ref()
            .expect("restarted observer")
            .health
            .readiness()
            .is_ok(),
        "restarted broker-only node is not ready: {:?}",
        observer_behavior
            .restarted
            .as_ref()
            .expect("restarted observer")
            .health
            .readiness()
    );
}

#[rstest_bdd_macros::then(
    "the controller cleans its first metadata segment within 100 seconds, data partitions occupy only the data directory and DescribeLogDirs reports only that directory"
)]
async fn separated_observer_log_directories(observer_behavior: &ObserverBehavior) {
    let broker_data = observer_behavior.cluster().broker_configs[0]
        .log_dir
        .clone();
    let broker_metadata = observer_behavior.cluster().broker_configs[0]
        .metadata_dir()
        .to_path_buf();
    let topic_names = &observer_behavior.topics;
    // The controller cleans its metadata log: the first segment goes and a
    // snapshot stays, within the snapshot test's 100 seconds.
    let partition_dir =
        krabka_raft::metadata_partition_dir(&observer_behavior.cluster().controller_metadata_dir);
    let deadline = Instant::now() + Duration::from_secs(100);
    while partition_dir.join("00000000000000000000.log").exists()
        || !holds_a_checkpoint(&partition_dir)
    {
        assert!(
            Instant::now() < deadline,
            "the first metadata segment was never cleaned: {:?}",
            entries(&partition_dir)
        );
        time::sleep(Duration::from_millis(200)).await;
    }

    // The partitions are in the data directory. The controller's metadata
    // directory holds the metadata partition only, and the broker-only node's
    // holds no partition: its observer keeps a checkpoint there only once it
    // installs or writes one.
    let mut want_data: Vec<String> = topic_names
        .iter()
        .map(|topic| format!("{topic}-0"))
        .collect();
    want_data.sort();
    let partitions_in = |dir: &Path| -> Vec<String> {
        entries(dir)
            .into_iter()
            .filter(|name| name.starts_with("metadata-dir-topic-"))
            .collect()
    };
    assert!(
        (
            partitions_in(&broker_data),
            partitions_in(&broker_metadata),
            entries(&observer_behavior.cluster().controller_metadata_dir),
            krabka_raft::metadata_partition_dir(&broker_data).exists(),
        ) == (
            want_data,
            Vec::<String>::new(),
            vec![krabka_raft::METADATA_PARTITION_DIR.to_owned()],
            false,
        )
    );
    let described = observer_behavior
        .client()
        .send(DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        described
            .results
            .iter()
            .map(|result| result.log_dir.clone())
            .collect::<Vec<_>>()
            == vec![broker_data.display().to_string()]
    );
}

#[rstest_bdd_macros::then(
    "all broker registrations stay unfenced throughout four additional seconds sampled every 200 milliseconds"
)]
async fn continually_unfenced_observers(observer_behavior: &ObserverBehavior) {
    let hold_until = Instant::now() + FENCING_WINDOW;
    while Instant::now() < hold_until {
        let fenced = fenced_anywhere(observer_behavior.cluster());
        assert!(
            fenced.is_empty(),
            "brokers fenced while every one of them was heartbeating: {fenced:?}"
        );
        time::sleep(Duration::from_millis(200)).await;
    }
}

#[rstest_bdd_macros::when("DescribeCluster reads the default broker listing from both observers")]
async fn describe_unfenced_observers(observer_behavior: &mut ObserverBehavior) {
    observer_behavior.cluster_responses.clear();
    for index in 0..observer_behavior.cluster().brokers.len() {
        let client = connect_client(
            observer_behavior.cluster().brokers[index]
                .listen_addr()
                .to_string(),
            None,
        )
        .await;
        let response = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        client.close();
        observer_behavior.cluster_responses.push((
            observer_behavior.cluster().brokers[index].node_id(),
            response,
        ));
    }
}

#[rstest_bdd_macros::then(
    "both listings contain exactly unfenced brokers 2 and 3 and name a listed broker as controller"
)]
fn listed_unfenced_observers(observer_behavior: &ObserverBehavior) {
    assert!(observer_behavior.cluster_responses.len() == 2);
    for (node, response) in &observer_behavior.cluster_responses {
        assert!(response.error_code == 0);
        let rows: BTreeSet<ListedBroker> = response
            .brokers
            .iter()
            .map(|row| ListedBroker {
                broker_id: row.broker_id,
                is_fenced: row.is_fenced,
            })
            .collect();
        assert!(
            rows == BTreeSet::from([
                ListedBroker {
                    broker_id: 2,
                    is_fenced: false
                },
                ListedBroker {
                    broker_id: 3,
                    is_fenced: false
                }
            ]),
            "DescribeCluster on node {node} must list both brokers unfenced"
        );
        assert!(
            response.controller_id == 2 || response.controller_id == 3,
            "DescribeCluster on node {node} advertised controller_id {}, which is not a listed broker",
            response.controller_id
        );
    }
}

#[rstest_bdd_macros::then(
    "Metadata from either observer resolves its controller id to a reachable broker returning the same cluster id"
)]
async fn reachable_metadata_observer_controller(observer_behavior: &ObserverBehavior) {
    assert_metadata_names_a_reachable_controller(observer_behavior.cluster()).await;
}

#[rstest_bdd_macros::when(
    "DescribeQuorum is polled for up to thirty seconds until observers 2 and 3 have fetched and caught up"
)]
async fn poll_observer_quorum(observer_behavior: &mut ObserverBehavior) {
    let deadline = Instant::now() + Duration::from_secs(30);
    observer_behavior.quorum = Some(loop {
        let response = observer_behavior
            .client()
            .send(metadata_quorum_request())
            .await
            .unwrap();
        let partition = &response.topics[0].partitions[0];
        let listed: BTreeSet<i32> = partition
            .observers
            .iter()
            .map(|observer| observer.replica_id)
            .collect();
        if partition.error_code == 0
            && listed == BTreeSet::from([2, 3])
            && partition
                .observers
                .iter()
                .all(|observer| observer.last_caught_up_timestamp > 0)
        {
            break response;
        }
        assert!(
            Instant::now() < deadline,
            "the brokers never appeared as observers: {:?}",
            partition.observers
        );
        time::sleep(Duration::from_millis(200)).await;
    });
}

#[rstest_bdd_macros::then(
    "the quorum lists exactly observers 2 and 3 with nonnegative log ends, fetch and caught-up times after 1700000000000, and caught-up times no later than their fetch times"
)]
fn caught_up_quorum_observers(observer_behavior: &ObserverBehavior) {
    let partition = &observer_behavior
        .quorum
        .as_ref()
        .expect("quorum observers read")
        .topics[0]
        .partitions[0];
    assert!(partition.error_code == 0);
    assert!(
        partition
            .observers
            .iter()
            .map(|observer| observer.replica_id)
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([2, 3])
    );
    for observer in &partition.observers {
        assert!(
            observer.last_fetch_timestamp > 1_700_000_000_000,
            "observer {} has a real fetch time, got {}",
            observer.replica_id,
            observer.last_fetch_timestamp
        );
        assert!(
            observer.last_caught_up_timestamp > 1_700_000_000_000,
            "observer {} has a real caught-up time, got {}",
            observer.replica_id,
            observer.last_caught_up_timestamp
        );
        assert!(
            observer.last_caught_up_timestamp <= observer.last_fetch_timestamp,
            "observer {} caught up at {}, after its last fetch at {}",
            observer.replica_id,
            observer.last_caught_up_timestamp,
            observer.last_fetch_timestamp
        );
        assert!(
            observer.log_end_offset >= 0,
            "observer {} has a fetch offset",
            observer.replica_id
        );
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "a broker only node recovers after the controller prunes past its fetch offset"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_only_node_recovers_after_the_controller_prunes_past_its_fetch_offset(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "heartbeats keep brokers unfenced in a role separated cluster"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_keep_brokers_unfenced_in_a_role_separated_cluster(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "broker only nodes are described as quorum observers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_nodes_are_described_as_quorum_observers(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/role_separation_observer.feature",
    name = "a separate metadata log directory rolls cleans and restores a wiped node"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_separate_metadata_log_directory_rolls_cleans_and_restores_a_wiped_node(
    mut observer_behavior: ObserverBehavior,
) {
    observer_behavior.shutdown().await;
    drop(observer_behavior);
}
