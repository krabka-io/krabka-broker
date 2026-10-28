//! The bootstrap records of a new cluster reach the metadata log once, from
//! the active controller, as Kafka's `QuorumController` writes them on its
//! activation with `ActivationRecordsGenerator.recordsForEmptyLog`. A broker
//! never writes them, and waits until it reads them from the log.
//!
//! Each case formats every node with `krabka-format` in process, as Kafka's
//! system tests run `kafka-storage format` before they start a node. It starts
//! the controllers and then the brokers, and reads the committed metadata log
//! back off the controller listener of the leader.
//!
//! When the bootstrap records enable ELR, the controller also writes the
//! cluster-level `min.insync.replicas`, at its static value, in the same
//! batch, and `DescribeConfigs` reports it as Kafka reports it.
//!
//! The dynamic case is the topology of Kafka's `quorum_reconfiguration_test.py`.
//! Its standalone controller holds the bootstrap records in its bootstrap
//! checkpoint, and its brokers hold none. A controller that kept those records
//! in its image and out of the log left the brokers without a
//! `metadata.version`, and each broker then wrote its own copy.
#![cfg(test)]

use {
    crate::support::{
        addressed_node_config,
        client::{connect_owned, metadata_fetch},
        configs::describe_resource,
        records::metadata_batches,
    },
    assert2::{assert, check},
    futures_util::future::join_all,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId, bootstrap, config::NodeRole,
    },
    krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
        metadata_version::ELR_VERSION_FEATURE,
    },
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{
            DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
        },
    },
    std::{
        collections::BTreeMap,
        mem,
        net::SocketAddr,
        path::{Path, PathBuf},
        slice,
        time::Duration,
    },
    tempfile::TempDir,
    tokio::{net::TcpListener, time},
};

mod support;

/// The cluster id `kafka-storage format` is given in the system tests.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node gets to finish starting.
const DEADLINE: Duration = Duration::from_secs(60);

/// Kafka's `TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG`.
const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// Kafka's `ConfigResource.Type.BROKER`.
const RESOURCE_TYPE_BROKER: i8 = 4;

/// Kafka's `DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG`.
const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;

/// Kafka's `DescribeConfigsResponse.ConfigType.INT`.
const CONFIG_TYPE_INT: i8 = 3;

/// How a case's nodes find the controller quorum.
#[derive(Debug, Clone, Copy)]
enum Quorum {
    /// KIP-853: the first controller is formatted with `--standalone`, and
    /// every node names the controllers through
    /// `controller.quorum.bootstrap.servers` alone.
    Dynamic,
    /// KIP-595: every node names the controllers through
    /// `controller.quorum.voters`, and no format names a quorum.
    Static,
}

/// One node of a case: its node id and its roles.
struct NodeSpec {
    id: NodeId,
    roles: &'static [NodeRole],
}

/// A controller's node id and the address of its controller listener.
struct ControllerAddress {
    id: NodeId,
    addr: SocketAddr,
}

/// A node that finished starting, and the directory that backs it.
struct StartedNode {
    id: NodeId,
    handle: BrokerHandle,
    dir: TempDir,
}

const CONTROLLER: &[NodeRole] = &[NodeRole::Controller];
const BROKER: &[NodeRole] = &[NodeRole::Broker];
const COMBINED: &[NodeRole] = &[NodeRole::Broker, NodeRole::Controller];

/// A node that is formatted and not yet started.
struct Node {
    id: NodeId,
    roles: &'static [NodeRole],
    client: TcpListener,
    controller: TcpListener,
    client_addr: SocketAddr,
    controller_addr: SocketAddr,
    dir: TempDir,
    log_dir: PathBuf,
}

/// Formats `log_dir` for node `id` with `krabka-format` and the quorum and
/// feature flags in `flags`, in process.
async fn format(log_dir: &Path, id: NodeId, flags: &[String]) {
    let node_id = id.to_string();
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.to_str().expect("utf-8 temp path").to_owned(),
        "--cluster-id".to_owned(),
        CLUSTER_ID.to_owned(),
        "--node-id".to_owned(),
        node_id,
    ];
    argv.extend_from_slice(flags);
    let code = krabka_format::run_from_args(argv).await;
    assert!(code == 0, "krabka-format exited {code} for node {id}");
}

#[derive(derive_more::From, derive_more::Into)]
/// The config the broker binary builds for `node` from its formatted
/// directory: the ids of its `meta.properties`, `Bootstrap` mode for a fresh
/// directory, and the static `min.insync.replicas` of the case.
#[derive(Clone, Copy, derive_more::Display)]
struct MinimumInsyncReplicas(i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ActivationSetup<'a> {
    #[default(Quorum::Dynamic)]
    quorum: Quorum,
    controllers: &'a [ControllerAddress],
    #[default(MinimumInsyncReplicas(1))]
    min_insync_replicas: MinimumInsyncReplicas,
}

fn config(node: &Node, setup: ActivationSetup<'_>) -> BrokerConfig {
    let meta = bootstrap::initialize_log_dirs(
        &node.log_dir,
        slice::from_ref(&node.log_dir),
        node.id,
        None,
    )
    .expect("krabka-format wrote meta.properties");
    let mut config = addressed_node_config()
        .log_dir(&node.log_dir)
        .node(node.id)
        .client(node.client_addr)
        .controller(node.controller_addr)
        .call();
    config.roles = node.roles.to_vec();
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.cluster_id = Some(meta.cluster_id);
    config.directory_id = meta.directory_id;
    config.default_min_insync_replicas = setup.min_insync_replicas.0;
    match setup.quorum {
        Quorum::Dynamic => {
            config.controller_quorum_voters = vec![];
            config.bootstrap_servers = setup
                .controllers
                .iter()
                .map(|controller| controller.addr.to_string())
                .collect();
        }
        Quorum::Static => {
            config.controller_quorum_voters = setup
                .controllers
                .iter()
                .map(|controller| (controller.id, controller.addr.to_string()))
                .collect();
        }
    }
    config
}

/// Starts every node of `nodes` at the same time, and fails the test when one
/// does not finish starting.
#[bon::builder]
async fn start_all(
    nodes: Vec<Node>,
    #[builder(default = Quorum :: Dynamic)] quorum: Quorum,
    #[builder(default)] controllers: &'_ [ControllerAddress],
    #[builder(into, default = MinimumInsyncReplicas (1))]
    min_insync_replicas: MinimumInsyncReplicas,
) -> Vec<StartedNode> {
    let setup = ActivationSetup {
        quorum,
        controllers,
        min_insync_replicas,
    };
    join_all(nodes.into_iter().map(|node| async move {
        let config = config(&node, setup);
        let id = node.id;
        let handle = time::timeout(
            DEADLINE,
            Broker::start_with_listeners(config, Some(node.controller), Some(node.client)),
        )
        .await
        .unwrap_or_else(|_| panic!("node {id} did not finish starting in {DEADLINE:?}"))
        .unwrap_or_else(|error| panic!("node {id} failed to start: {error}"));
        StartedNode {
            id,
            handle,
            dir: node.dir,
        }
    }))
    .await
}

/// One batch of the committed metadata log.
#[derive(Debug, Clone, PartialEq)]
enum Batch {
    /// A control batch: a `LeaderChange` marker and the KIP-853 controls.
    Control,
    /// A batch of metadata records.
    Metadata(Vec<MetadataRecord>),
}

/// Reads the committed metadata log from its start off the controller
/// listener at `controller`, with each value decoded against the image that
/// the values before it produce.
async fn committed_batches(controller: SocketAddr) -> Vec<Batch> {
    let response = metadata_fetch(controller, "bootstrap-activation-test", 0).await;

    let mut image = MetadataImage::new(uuid::Uuid::nil());
    let mut batches = Vec::new();
    for batch in metadata_batches(&response.records) {
        if batch.attributes.is_control_batch() {
            batches.push(Batch::Control);
            continue;
        }
        let mut records = Vec::new();
        for value in batch
            .records
            .iter()
            .filter_map(|record| record.value.as_ref())
        {
            if krabka_raft::is_kip835_noop(value) {
                continue;
            }
            let record =
                krabka_metadata::from_kraft_value(value, &image).expect("decode a metadata value");
            image.apply(&record);
            records.push(record);
        }
        batches.push(Batch::Metadata(records));
    }
    batches
}

/// What the committed log says about the bootstrap records: whether the
/// first metadata batch follows the leader's control batch, that batch, and
/// every batch that finalizes a feature level.
#[derive(Debug, PartialEq)]
struct Seeding {
    follows_control_batch: bool,
    first_metadata_batch: Option<Batch>,
    finalizing: Vec<Vec<MetadataRecord>>,
}

fn seeding(batches: &[Batch]) -> Seeding {
    let leading_controls = batches
        .iter()
        .take_while(|batch| **batch == Batch::Control)
        .count();
    let finalizing = batches
        .iter()
        .filter_map(|batch| match batch {
            Batch::Metadata(records)
                if records
                    .iter()
                    .any(|record| matches!(record, MetadataRecord::V1FeatureLevel(_))) =>
            {
                Some(records.clone())
            }
            _ => None,
        })
        .collect();
    Seeding {
        follows_control_batch: leading_controls > 0,
        first_metadata_batch: batches.get(leading_controls).cloned(),
        finalizing,
    }
}

/// `DescribeConfigs` of the cluster-level `min.insync.replicas` through the
/// client listener at `broker`, as `kafka-configs --describe --entity-type
/// brokers --entity-default` sends it, with the synonyms.
async fn describe_cluster_min_insync_replicas(broker: SocketAddr) -> DescribeConfigsResult {
    let client = connect_owned(broker.to_string(), "bootstrap-activation-test", "client").await;
    let response = client
        .send(DescribeConfigsRequest {
            resources: vec![describe_resource(
                RESOURCE_TYPE_BROKER,
                String::new(),
                Some(vec![MIN_INSYNC_REPLICAS.to_owned()]),
            )],
            include_synonyms: true,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let mut results = response.results;
    assert!(results.len() == 1, "one result per resource: {results:?}");
    results.remove(0)
}

/// What Kafka's `DescribeConfigs` answers for the cluster-level
/// `min.insync.replicas` at `value`: a `DYNAMIC_DEFAULT_BROKER_CONFIG` entry,
/// writable, of type `INT`, whose only synonym is the cluster default itself.
/// Without a value there is no entry.
fn kafkas_cluster_min_insync_replicas(
    value: Option<MinimumInsyncReplicas>,
) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code: 0,
        error_message: None,
        resource_type: RESOURCE_TYPE_BROKER,
        resource_name: String::new(),
        configs: value
            .map(|value| DescribeConfigsResourceResult {
                name: MIN_INSYNC_REPLICAS.to_owned(),
                value: Some(value.to_string()),
                read_only: false,
                config_source: DYNAMIC_DEFAULT_BROKER_CONFIG,
                is_sensitive: false,
                synonyms: vec![DescribeConfigsSynonym {
                    name: MIN_INSYNC_REPLICAS.to_owned(),
                    value: Some(value.to_string()),
                    source: DYNAMIC_DEFAULT_BROKER_CONFIG,
                    ..Default::default()
                }],
                config_type: CONFIG_TYPE_INT,
                documentation: None,
                ..Default::default()
            })
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

/// One case: its topology, whether its format enables ELR, and the static
/// `min.insync.replicas` of every node.
#[derive(Clone, Copy)]
enum EligibleLeaderReplicas {
    Enabled,
    Disabled,
}

const TWO_BROKERS: &[NodeSpec] = &[
    NodeSpec {
        id: NodeId(1),
        roles: BROKER,
    },
    NodeSpec {
        id: NodeId(2),
        roles: BROKER,
    },
];

#[derive(Clone, Copy)]
struct Case {
    what: &'static str,
    quorum: Quorum,
    controllers: &'static [NodeSpec],
    brokers: &'static [NodeSpec],
    eligible_leader_replicas: EligibleLeaderReplicas,
    min_insync_replicas: MinimumInsyncReplicas,
}

struct BootstrapActivation {
    case: Option<Case>,
    pending_controllers: Vec<Node>,
    pending_brokers: Vec<Node>,
    addresses: Vec<ControllerAddress>,
    controllers: Vec<StartedNode>,
    brokers: Vec<StartedNode>,
    expected_activation: Vec<MetadataRecord>,
    expected_levels: BTreeMap<String, i16>,
    expected_minimum: Option<MinimumInsyncReplicas>,
    committed: Option<Vec<Batch>>,
    config: Option<DescribeConfigsResult>,
}
#[rstest::fixture]
fn activation() -> BootstrapActivation {
    // Start this scenario with fresh inputs and observations.

    BootstrapActivation {
        case: None,
        pending_controllers: Vec::new(),
        pending_brokers: Vec::new(),
        addresses: Vec::new(),
        controllers: Vec::new(),
        brokers: Vec::new(),
        expected_activation: Vec::new(),
        expected_levels: BTreeMap::new(),
        expected_minimum: None,
        committed: None,
        config: None,
    }
}
#[rstest_bdd_macros::given(
    "formatted {quorum_name} quorum nodes with topology {topology} ELR {elr} and static minimum ISR {minimum}"
)]
async fn formatted_activation_nodes(
    activation: &mut BootstrapActivation,
    quorum_name: String,
    topology: String,
    elr: String,
    minimum: i32,
) {
    support::init_tracing();
    let (what, controllers, brokers): (&'static str, &'static [NodeSpec], &'static [NodeSpec]) =
        match topology.as_str() {
            "one-controller-two-brokers" => (
                "a standalone controller and two brokers",
                &[NodeSpec {
                    id: NodeId(3001),
                    roles: CONTROLLER,
                }],
                TWO_BROKERS,
            ),
            "three-controllers-two-brokers" => (
                "three isolated controllers and two brokers",
                &[
                    NodeSpec {
                        id: NodeId(3001),
                        roles: CONTROLLER,
                    },
                    NodeSpec {
                        id: NodeId(3002),
                        roles: CONTROLLER,
                    },
                    NodeSpec {
                        id: NodeId(3003),
                        roles: CONTROLLER,
                    },
                ],
                TWO_BROKERS,
            ),
            "one-combined-node" => (
                "one combined node",
                &[NodeSpec {
                    id: NodeId(1),
                    roles: COMBINED,
                }],
                &[],
            ),
            other => panic!("unknown activation topology {other}"),
        };
    let case = Case {
        what,
        controllers,
        brokers,
        quorum: match quorum_name.as_str() {
            "dynamic" => Quorum::Dynamic,
            "static" => Quorum::Static,
            other => panic!("unknown quorum {other}"),
        },
        eligible_leader_replicas: match elr.as_str() {
            "enabled" => EligibleLeaderReplicas::Enabled,
            "disabled" => EligibleLeaderReplicas::Disabled,
            other => panic!("unknown ELR mode {other}"),
        },
        min_insync_replicas: MinimumInsyncReplicas(minimum),
    };

    let Case {
        quorum,
        controllers: controller_specs,
        brokers: broker_specs,
        eligible_leader_replicas,
        min_insync_replicas,
        ..
    } = case;
    // A default format finalizes every feature at the default level of the
    // latest production release, `metadata.version` first. That enables ELR.
    // `--feature eligible.leader.replicas.version=0` leaves it out.
    let disabled: BTreeMap<String, i16> =
        if matches!(eligible_leader_replicas, EligibleLeaderReplicas::Enabled) {
            BTreeMap::new()
        } else {
            [(ELR_VERSION_FEATURE.to_owned(), 0)].into()
        };
    let bootstrap = krabka_metadata::bootstrap_feature_records_with_overrides(
        krabka_format::LATEST_PRODUCTION_METADATA_VERSION,
        &disabled,
    );
    let cluster_min_isr = matches!(eligible_leader_replicas, EligibleLeaderReplicas::Enabled)
        .then_some(min_insync_replicas);
    let mut activation_records = bootstrap.clone();
    activation_records.extend(cluster_min_isr.map(|value| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: MIN_INSYNC_REPLICAS.to_owned(),
            config_value: Some(value.to_string()),
        })
    }));
    let expected_levels: BTreeMap<String, i16> = bootstrap
        .iter()
        .filter_map(|record| match record {
            MetadataRecord::V1FeatureLevel(feature) => Some((feature.name.clone(), feature.level)),
            _ => None,
        })
        .collect();

    let count = controller_specs.len() + broker_specs.len();
    let (client_addrs, controller_addrs, clients, controller_listeners) =
        support::bind_and_hold_ports(count).await;
    let mut nodes: Vec<Node> = controller_specs
        .iter()
        .chain(broker_specs)
        .zip(clients.into_iter().zip(controller_listeners))
        .zip(client_addrs.into_iter().zip(controller_addrs))
        .map(
            |((&NodeSpec { id, roles }, (client, controller)), (client_addr, controller_addr))| {
                let dir = TempDir::new().expect("tempdir");
                let log_dir = dir.path().join("log");
                Node {
                    id,
                    roles,
                    client,
                    controller,
                    client_addr,
                    controller_addr,
                    dir,
                    log_dir,
                }
            },
        )
        .collect();
    let controllers: Vec<ControllerAddress> = nodes[..controller_specs.len()]
        .iter()
        .map(|node| ControllerAddress {
            id: node.id,
            addr: node.controller_addr,
        })
        .collect();
    for (index, node) in nodes.iter().enumerate() {
        let mut flags = match quorum {
            Quorum::Dynamic if index == 0 => vec![
                "--standalone".to_owned(),
                "--controller-listener".to_owned(),
                node.controller_addr.to_string(),
            ],
            Quorum::Dynamic | Quorum::Static => vec![],
        };
        if matches!(eligible_leader_replicas, EligibleLeaderReplicas::Disabled) {
            flags.extend(["--feature".to_owned(), format!("{ELR_VERSION_FEATURE}=0")]);
        }
        format(&node.log_dir, node.id, &flags).await;
    }

    activation.pending_brokers = nodes.split_off(controller_specs.len());
    activation.pending_controllers = nodes;
    activation.addresses = controllers;
    activation.expected_activation = activation_records;
    activation.expected_levels = expected_levels;
    activation.expected_minimum = cluster_min_isr;
    activation.case = Some(case);
}
#[rstest_bdd_macros::when("the formatted controllers activate before the brokers join")]
async fn activate_formatted_controllers(activation: &mut BootstrapActivation) {
    let Case {
        quorum,
        min_insync_replicas,
        controllers: controller_specs,
        brokers: broker_specs,
        ..
    } = activation.case.expect("nodes formatted");
    let started_controllers = start_all()
        .nodes(mem::take(&mut activation.pending_controllers))
        .quorum(quorum)
        .controllers(&activation.addresses)
        .min_insync_replicas(min_insync_replicas)
        .call()
        .await;
    let leader_id = started_controllers[0]
        .handle
        .wait_until_controller_leader()
        .await
        .unwrap();
    let started_brokers = start_all()
        .nodes(mem::take(&mut activation.pending_brokers))
        .quorum(quorum)
        .controllers(&activation.addresses)
        .min_insync_replicas(min_insync_replicas)
        .call()
        .await;
    let leader = started_controllers
        .iter()
        .find(|node| node.id == leader_id)
        .map(|node| &node.handle)
        .expect("the leader is one of the controllers");
    let broker_role_count = controller_specs
        .iter()
        .chain(broker_specs)
        .filter(|node| node.roles.contains(&NodeRole::Broker))
        .count();
    leader
        .wait_until_brokers_registered(broker_role_count)
        .await;

    activation.controllers = started_controllers;
    activation.brokers = started_brokers;
}
#[rstest_bdd_macros::when("a client reads the active controller committed metadata log")]
async fn read_activation_log(activation: &mut BootstrapActivation) {
    let leader = activation
        .controllers
        .iter()
        .map(|node| &node.handle)
        .find(|handle| handle.controller_leader_id() == Some(krabka_raft::NodeId(handle.node_id())))
        .expect("active controller leader");
    activation.committed = Some(committed_batches(leader.controller_addr()).await);
}
#[rstest_bdd_macros::then(
    "bootstrap features occur exactly once in the first metadata batch after the leader control batch"
)]
fn bootstrap_seeded_once(activation: &BootstrapActivation) {
    let committed = activation.committed.as_ref().expect("committed log read");
    let expected = &activation.expected_activation;
    let what = activation.case.expect("nodes formatted").what;
    check!(
        seeding(committed)
            == Seeding {
                follows_control_batch: true,
                first_metadata_batch: Some(Batch::Metadata(expected.clone())),
                finalizing: vec![expected.clone()]
            },
        "{what}: {committed:#?}"
    );
}
#[rstest_bdd_macros::then(
    "every controller and broker image converges to those feature levels and cluster minimum ISR"
)]
async fn activation_image_convergence(activation: &BootstrapActivation) {
    let expected_min_isr = activation.expected_minimum.map(|value| value.to_string());
    for node in activation.controllers.iter().chain(&activation.brokers) {
        node.handle
            .wait_for_image(|image| {
                *image.finalized_features() == activation.expected_levels
                    && image
                        .default_broker_config()
                        .and_then(|configs| configs.get(MIN_INSYNC_REPLICAS))
                        == expected_min_isr.as_ref()
            })
            .await;
    }
}
#[rstest_bdd_macros::when("a broker describes its cluster default minimum ISR configuration")]
async fn read_activated_minimum(activation: &mut BootstrapActivation) {
    let broker = activation
        .brokers
        .iter()
        .chain(&activation.controllers)
        .map(|node| &node.handle)
        .find(|handle| handle.listen_addr().port() != 0)
        .expect("a node with the broker role");
    activation.config = Some(describe_cluster_min_insync_replicas(broker.listen_addr()).await);
}
#[rstest_bdd_macros::then(
    "DescribeConfigs matches Kafka cluster minimum ISR value source type and synonyms"
)]
fn activated_minimum_described(activation: &BootstrapActivation) {
    check!(
        activation.config.as_ref().expect("minimum ISR described")
            == &kafkas_cluster_min_insync_replicas(activation.expected_minimum)
    );
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/bootstrap_activation.feature",
    name = "the active controller writes the bootstrap records once"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_active_controller_writes_the_bootstrap_records_once(
    activation: BootstrapActivation,
    quorum_name: String,
    topology: String,
    elr: String,
    minimum: i32,
) {
    let _ = (quorum_name, topology, elr, minimum);
    for StartedNode { handle, dir, .. } in
        activation.brokers.into_iter().chain(activation.controllers)
    {
        handle.shutdown().await;
        drop(dir);
    }
}
