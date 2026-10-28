//! The coordinator topics on a cold three-broker cluster.
//!
//! Kafka's `KafkaApis.getCoordinator` creates `__consumer_offsets` and
//! `__transaction_state` on the first `FindCoordinator` that needs them, with
//! their configured replication factor, through
//! `DefaultAutoTopicCreationManager`. Until the topic exists and has leaders,
//! the lookup answers `COORDINATOR_NOT_AVAILABLE`. No broker creates either
//! topic when it starts, so the first broker to boot cannot create them with
//! a replication factor of one.
//!
//! The first case proves the whole failure it guards against: a group whose
//! coordinator stops still has a coordinator, because every partition of
//! `__consumer_offsets` has a replica on every broker. The second proves the
//! replication factor is never lowered to fit the cluster. The third proves
//! that a broker stopped by a controlled shutdown still counts toward it.
#![cfg(test)]

use {
    crate::support::{
        client::connect_owned,
        discovery::coordinator_lookup_request,
        shutdown_cluster,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::Client,
    krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse,
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
};

mod support;

const OFFSETS_TOPIC: &str = "__consumer_offsets";
const TRANSACTION_TOPIC: &str = "__transaction_state";
const SHARE_STATE_TOPIC: &str = "__share_group_state";

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

const COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// The time a lookup may take to see a creation or a failover settle.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// The sorted replicas and in-sync replicas of one partition.
#[derive(Clone, Debug, PartialEq)]
struct Placement {
    replicas: Vec<NodeId>,
    isr: Vec<NodeId>,
}

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "coordinator-topics",
        "client",
    )
    .await
}

async fn find(client: &Client, key_type: i8, key: &str) -> FindCoordinatorResponse {
    client
        .send(coordinator_lookup_request(key, key_type, vec![key.into()]))
        .await
        .expect("FindCoordinator")
}

/// Look `key` up until a broker other than `excluded` answers as its
/// coordinator, and return that broker's node id.
async fn coordinator_of(client: &Client, key_type: i8, key: &str, excluded: Option<i32>) -> i32 {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = find(client, key_type, key).await;
        let [row] = found.coordinators.as_slice() else {
            panic!("one coordinator row: {found:?}");
        };
        if row.error_code == 0 && Some(row.node_id) != excluded {
            return row.node_id;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: a client has no awaiter for the creation or the
        // failover; it retries, as Kafka's clients do.
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// Three brokers whose coordinator topics are sized as Kafka sizes them,
/// with `configure` applied on top.
async fn start_three(configure: impl Fn(&mut BrokerConfig)) -> Cluster {
    let cluster = support::start_n_node_with(3, |_, config| {
        *config = config.clone().with_internal_topics_for(3);
        config.offsets_topic_num_partitions = 3;
        config.transaction_state_num_partitions = 3;
        configure(config);
    })
    .await
    .expect("start the cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    cluster
}

struct CoordinatorTopics {
    cluster: Cluster,
    first: Option<FindCoordinatorResponse>,
    coordinator: Option<i32>,
    successor: Option<i32>,
    transaction_lookups: Vec<FindCoordinatorResponse>,
    victim: Option<usize>,
    stopped_id: Option<NodeId>,
    stopped_directory: Option<TempDir>,
}
impl CoordinatorTopics {
    async fn shutdown(self) { shutdown_cluster(self.cluster).await; }
}
#[rstest::fixture]
fn coordinators() -> CoordinatorTopics {
    // Start this scenario with fresh inputs and observations.

    CoordinatorTopics {
        cluster: Vec::new(),
        first: None,
        coordinator: None,
        successor: None,
        transaction_lookups: Vec::new(),
        victim: None,
        stopped_id: None,
        stopped_directory: None,
    }
}
#[rstest_bdd_macros::given(
    "a cold three broker cluster uses replication factor 3 for coordinator topics"
)]
async fn cold_coordinator_cluster(coordinators: &mut CoordinatorTopics) {
    coordinators.cluster = start_three(|_| {}).await;
}
#[rstest_bdd_macros::given(
    "a cold three broker cluster requires transaction state replication factor 4"
)]
async fn unsatisfiable_transaction_cluster(coordinators: &mut CoordinatorTopics) {
    coordinators.cluster = start_three(|config| {
        config.transaction_state_replication_factor = 4;
    })
    .await;
}
#[rstest_bdd_macros::given("a cold three broker cluster uses three share-state partitions")]
async fn cold_share_coordinator_cluster(coordinators: &mut CoordinatorTopics) {
    coordinators.cluster = start_three(|config| {
        config.share_coordinator.state_topic_num_partitions = 3;
    })
    .await;
}
#[rstest_bdd_macros::then("the offsets topic is absent before any group coordinator lookup")]
fn offsets_not_created_at_boot(coordinators: &CoordinatorTopics) {
    assert!(
        coordinators.cluster[0]
            .0
            .controller_image_for_test()
            .topic(OFFSETS_TOPIC)
            .is_none()
    );
}
#[rstest_bdd_macros::when("a client first looks up the coordinator for group g")]
async fn first_group_coordinator_lookup(coordinators: &mut CoordinatorTopics) {
    let admin = client(&coordinators.cluster[0].0).await;
    coordinators.first = Some(find(&admin, KEY_TYPE_GROUP, "g").await);
    admin.close();
}
#[rstest_bdd_macros::then("the initial lookup reports coordinator not available error 15")]
fn cold_coordinator_lookup_pending(coordinators: &CoordinatorTopics) {
    check!(
        coordinators
            .first
            .as_ref()
            .expect("initial lookup sent")
            .coordinators[0]
            .error_code
            == COORDINATOR_NOT_AVAILABLE
    );
}
#[rstest_bdd_macros::when("the client retries group g until its coordinator is available")]
async fn group_coordinator_ready(coordinators: &mut CoordinatorTopics) {
    let admin = client(&coordinators.cluster[0].0).await;
    coordinators.coordinator = Some(coordinator_of(&admin, KEY_TYPE_GROUP, "g", None).await);
    admin.close();
}
#[rstest_bdd_macros::then("all three offsets partitions have replicas on brokers 1 2 and 3")]
fn offsets_replicated_to_every_broker(coordinators: &CoordinatorTopics) {
    let image = coordinators.cluster[0].0.controller_image_for_test();
    let replicas: Vec<Vec<NodeId>> = (0..3)
        .map(|partition| {
            let mut replicas = image
                .partition(OFFSETS_TOPIC, partition)
                .expect("an offsets partition")
                .replicas
                .clone();
            replicas.sort_unstable();
            replicas
        })
        .collect();
    let every_broker = vec![NodeId(1), NodeId(2), NodeId(3)];
    assert!(replicas == vec![every_broker; 3]);
}
#[rstest_bdd_macros::when("the broker coordinating group g shuts down")]
async fn stop_group_coordinator(coordinators: &mut CoordinatorTopics) {
    let coordinator = coordinators
        .coordinator
        .expect("group coordinator available");
    let position = coordinators
        .cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == u64::try_from(coordinator).unwrap())
        .expect("coordinator is a cluster member");
    let (stopped, _, directory) = coordinators.cluster.remove(position);
    stopped.shutdown().await;
    coordinators.stopped_directory = Some(directory);
}
#[rstest_bdd_macros::when("a survivor looks up group g until a different coordinator is available")]
async fn replacement_group_coordinator(coordinators: &mut CoordinatorTopics) {
    let survivor = client(&coordinators.cluster[0].0).await;
    coordinators.successor =
        Some(coordinator_of(&survivor, KEY_TYPE_GROUP, "g", coordinators.coordinator).await);
    survivor.close();
}
#[rstest_bdd_macros::then("the surviving coordinator differs from the stopped broker")]
fn coordinator_failover_visible(coordinators: &CoordinatorTopics) {
    check!(
        coordinators
            .successor
            .expect("replacement lookup completed")
            != coordinators
                .coordinator
                .expect("original coordinator known")
    );
}
#[rstest_bdd_macros::then("the group coordinator belongs to the running cluster")]
fn group_coordinator_served(coordinators: &CoordinatorTopics) {
    let coordinator = coordinators
        .coordinator
        .expect("group coordinator lookup completed");
    assert!(
        coordinators
            .cluster
            .iter()
            .any(|(broker, _, _)| i32::try_from(broker.node_id()).unwrap() == coordinator)
    );
}
#[rstest_bdd_macros::when("the client performs ten transaction coordinator lookups for t")]
async fn unavailable_transaction_coordinator(coordinators: &mut CoordinatorTopics) {
    let admin = client(&coordinators.cluster[0].0).await;
    for _ in 0..10 {
        coordinators
            .transaction_lookups
            .push(find(&admin, KEY_TYPE_TRANSACTION, "t").await);
        time::sleep(Duration::from_millis(100)).await;
    }
    admin.close();
}
#[rstest_bdd_macros::then(
    "all ten lookups report error 15 and no broker image contains a transaction state topic"
)]
fn transaction_replication_not_lowered(coordinators: &CoordinatorTopics) {
    assert!(coordinators.transaction_lookups.len() == 10);
    for found in &coordinators.transaction_lookups {
        check!(found.coordinators[0].error_code == COORDINATOR_NOT_AVAILABLE);
    }
    for (broker, _, _) in &coordinators.cluster {
        check!(
            broker
                .controller_image_for_test()
                .topic(TRANSACTION_TOPIC)
                .is_none()
        );
    }
}
#[rstest_bdd_macros::given(
    "a non-quorum-leader broker is selected after led is created with three partitions and replication factor 3"
)]
async fn controlled_shutdown_coordinator_setup(coordinators: &mut CoordinatorTopics) {
    const LED_TOPIC: &str = "led";
    let cluster = &coordinators.cluster;
    // Stop a broker that does not lead the raft quorum. The topic gives every
    // broker one partition to lead, so the stop goes through controlled
    // shutdown rather than straight to a fence.
    let raft_leader = cluster[0].0.wait_until_controller_leader().await.unwrap();
    let position = cluster
        .iter()
        .position(|(_, config, _)| config.node_id != raft_leader)
        .expect("a broker that is not the raft leader");
    let admin = client(&cluster[(position + 1) % cluster.len()].0).await;
    let created = admin
        .send(create_topic_request(
            creatable_topic()
                .name((LED_TOPIC).into())
                .partitions(3)
                .replicas(3)
                .call(),
        ))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    admin.close();
    for (handle, _, _) in cluster {
        for partition in 0..3 {
            handle
                .wait_until_partition_present(LED_TOPIC, partition)
                .await;
        }
    }

    coordinators.victim = Some(position);
}
#[rstest_bdd_macros::when("the selected broker completes a controlled shutdown within 30 seconds")]
async fn controlled_shutdown_before_coordinator_creation(coordinators: &mut CoordinatorTopics) {
    let (stopped, _, directory) = coordinators
        .cluster
        .remove(coordinators.victim.expect("nonleader selected"));
    coordinators.stopped_id = Some(NodeId(stopped.node_id()));
    stopped
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("the controlled shutdown drains");
    coordinators.stopped_directory = Some(directory);
}
#[rstest_bdd_macros::then(
    "the stopped broker stays registered fenced and in controlled shutdown on every survivor"
)]
async fn controlled_coordinator_registration_retained(coordinators: &CoordinatorTopics) {
    let stopped_id = coordinators.stopped_id.expect("controlled broker stopped");
    for (handle, _, _) in &coordinators.cluster {
        handle
            .wait_for_image(|image| image.broker(stopped_id).is_some_and(|broker| broker.fenced))
            .await;
    }
    let registration = coordinators.cluster[0]
        .0
        .controller_image_for_test()
        .broker(stopped_id)
        .cloned()
        .expect("stopped broker stays registered");
    assert!((registration.fenced, registration.in_controlled_shutdown) == (true, true));
}
#[rstest_bdd_macros::when("a survivor looks up share partition 0 with topic UUID bytes all 7")]
async fn create_coordinator_after_controlled_shutdown(coordinators: &mut CoordinatorTopics) {
    let survivor = client(&coordinators.cluster[0].0).await;
    let key = format!("share-group:{}:0", URL_SAFE_NO_PAD.encode([7u8; 16]));
    coordinator_of(&survivor, KEY_TYPE_SHARE, &key, None).await;
    survivor.close();
}
#[rstest_bdd_macros::then(
    "all three share-state partitions retain replicas 1 2 3 with only the survivors in ISR"
)]
fn controlled_shutdown_coordinator_placement(coordinators: &CoordinatorTopics) {
    let cluster = &coordinators.cluster;
    let image = cluster[0].0.controller_image_for_test();
    let mut survivors = cluster
        .iter()
        .map(|(handle, _, _)| NodeId(handle.node_id()))
        .collect::<Vec<_>>();
    survivors.sort_unstable();
    let sorted = |nodes: &[NodeId]| {
        let mut nodes = nodes.to_vec();
        nodes.sort_unstable();
        nodes
    };
    let placed: Vec<Placement> = (0..3)
        .map(|partition| {
            let record = image
                .partition(SHARE_STATE_TOPIC, partition)
                .expect("a share-state partition");
            Placement {
                replicas: sorted(&record.replicas),
                isr: sorted(&record.isr),
            }
        })
        .collect();
    let every_broker = vec![NodeId(1), NodeId(2), NodeId(3)];
    assert!(
        placed
            == vec![
                Placement {
                    replicas: every_broker,
                    isr: survivors
                };
                3
            ]
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/coordinator_topics.feature",
    name = "a cold cluster replicates the offsets topic on every broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_cluster_replicates_the_offsets_topic_on_every_broker(
    coordinators: CoordinatorTopics,
) {
    coordinators.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/coordinator_topics.feature",
    name = "a replication factor above the broker count leaves the coordinator unavailable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replication_factor_above_the_broker_count_leaves_the_coordinator_unavailable(
    coordinators: CoordinatorTopics,
) {
    coordinators.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/coordinator_topics.feature",
    name = "a cleanly stopped broker still counts toward the replication factor"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cleanly_stopped_broker_still_counts_toward_the_replication_factor(
    coordinators: CoordinatorTopics,
) {
    coordinators.shutdown().await;
}
