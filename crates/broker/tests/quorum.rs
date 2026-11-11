//! Multi-node in-process Krabka cluster tests. Each test starts
//! 3 brokers on distinct loopback ports. The config of each broker
//! lists all three as voters.
//!
//! These tests exercise the metadata quorum end-to-end: leader
//! election, log replication, follower-forwarding with `submit_change`,
//! and openraft's per-leader `client_write` serialization.
//!
//! Deadlines are 2 minutes throughout. A 3-broker cluster that starts
//! cold on a hosted GitHub Actions runner can need tens of seconds for
//! openraft to converge on a leader, and `cluster_lock` serializes the
//! tests, so three slow startups accumulate.
#![cfg(test)]

// Test-file pragmatism: broker ids are formed with plain
// `u64::try_from(i+1).unwrap()`-style casts when turning 1-based `i` into broker
// ids, and topics are built with `..Default::default()`. Hoisting these into
// named helpers would obscure the per-test narrative.

use {
    crate::support::{
        client::connect_client,
        discovery::topic_metadata_request,
        shutdown_cluster,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    futures_util::{StreamExt as _, stream::FuturesUnordered},
    krabka_broker::{BrokerConfig, BrokerHandle},
    std::{collections::BTreeMap, time::Duration},
    tokio::time,
};

mod support;

use {support::cluster_lock, tempfile::TempDir};

/// Waits until every broker reports an elected controller leader with a
/// non-zero id.
///
/// This function is event-driven. Each handle awaits its leader watch channel.
/// The check is stricter than the old "any one node" poll, and it is exactly
/// the precondition the callers need before they send client requests to a
/// specific node.
async fn wait_for_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await.unwrap();
    }
}

fn controller_views(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) -> ControllerViews {
    cluster
        .iter()
        .map(|(handle, config, _)| (config.node_id, handle.controller_leader_id()))
        .collect()
}

#[derive(Debug)]
enum ControllerObservationError {
    TimedOut(ControllerViews),
    WatchClosed(ControllerViews),
}

async fn observe_controller_agreement(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    departed: Option<NodeId>,
) -> Result<ControllerViews, ControllerObservationError> {
    let mut watches: Vec<_> = cluster
        .iter()
        .map(|(handle, config, _)| (config.node_id, handle.watch_leader_for_test()))
        .collect();
    let mut last_views = controller_views(cluster);
    let observed = time::timeout(Duration::from_secs(120), async {
        loop {
            last_views = watches
                .iter_mut()
                .map(|(node, watch)| (*node, *watch.borrow_and_update()))
                .collect();
            if let Some(leader) = last_views.values().find_map(|leader| *leader)
                && leader != NodeId(0)
                && Some(leader) != departed
                && last_views.contains_key(&leader)
                && last_views.len() == cluster.len()
                && last_views.values().all(|view| *view == Some(leader))
            {
                return Ok(last_views.clone());
            }
            let mut changes: FuturesUnordered<_> = watches
                .iter_mut()
                .map(|(_, watch)| watch.changed())
                .collect();
            match changes.next().await {
                Some(Ok(())) => {}
                Some(Err(_)) | None => {
                    return Err(ControllerObservationError::WatchClosed(last_views.clone()));
                }
            }
        }
    })
    .await;
    observed.unwrap_or(Err(ControllerObservationError::TimedOut(last_views)))
}

use {
    krabka_bdd::{Given, Outcome, ScenarioSpec, World, given, then, when},
    krabka_broker::NodeId,
    krabka_protocol::owned::metadata_response::MetadataResponse,
    rstest::fixture,
    tokio::sync::MutexGuard,
};

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;
type ControllerViews = BTreeMap<NodeId, Option<NodeId>>;

struct MetadataQuorum {
    cluster: Given<Cluster>,
    guard: Given<MutexGuard<'static, ()>>,
    killed: Given<NodeId>,
    retired_directory: Given<TempDir>,
    leaders: Given<Result<ControllerViews, ControllerObservationError>>,
    creates: Given<Vec<i16>>,
    metadata: Given<MetadataResponse>,
}

impl World for MetadataQuorum {
    async fn after(&mut self, _scenario: &ScenarioSpec, _outcome: &Outcome) {
        if self.cluster.is_set() {
            shutdown_cluster(self.cluster.take()).await;
        }
    }
}

#[fixture]
fn metadata_quorum() -> MetadataQuorum {
    MetadataQuorum {
        cluster: Given::new(
            "metadata quorum",
            "Given three brokers share a static metadata voter set",
        ),
        guard: Given::new(
            "cluster execution slot",
            "Given three brokers share a static metadata voter set",
        ),
        killed: Given::new(
            "departed controller",
            "When the elected metadata controller is shut down",
        ),
        retired_directory: Given::new(
            "departed controller log",
            "When the elected metadata controller is shut down",
        ),
        leaders: Given::new(
            "controller observations",
            "When all metadata voters observe their elected controller",
        ),
        creates: Given::new("topic creation results", "When a client creates a topic"),
        metadata: Given::new(
            "topic metadata",
            "When a client reads committed topic metadata",
        ),
    }
}

#[given("three brokers share a static metadata voter set")]
async fn metadata_voters(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum.guard.set(cluster_lock().lock().await);
    metadata_quorum
        .cluster
        .set(support::start_n_node_with_retry(3).await);
}

#[given("a three-broker metadata quorum has elected a controller")]
async fn ready_metadata_quorum(metadata_quorum: &mut MetadataQuorum) {
    metadata_voters(metadata_quorum).await;
    controller_ready(metadata_quorum).await;
}

#[given("topic prop has been created with three partitions through a metadata voter")]
async fn existing_propagated_topic(metadata_quorum: &mut MetadataQuorum) {
    create_on_voter(metadata_quorum, "prop".into(), 3, 0).await;
}

#[given("the metadata quorum has recovered from a controller shutdown")]
async fn recovered_metadata_quorum(metadata_quorum: &mut MetadataQuorum) {
    ready_metadata_quorum(metadata_quorum).await;
    stop_controller(metadata_quorum).await;
    if let Err(error) = metadata_quorum.leaders.get() {
        panic!("the metadata quorum did not recover before topic creation: {error:?}");
    }
}

async fn controller_ready(metadata_quorum: &MetadataQuorum) {
    wait_for_leader(metadata_quorum.cluster.get()).await;
}

#[given("every voter has registered all three brokers")]
async fn voters_registered(metadata_quorum: &MetadataQuorum) {
    for (broker, _, _) in metadata_quorum.cluster.get() {
        broker.wait_until_brokers_registered(3).await;
    }
}

#[when("all metadata voters observe their elected controller")]
async fn observe_election(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum
        .leaders
        .set(observe_controller_agreement(metadata_quorum.cluster.get(), None).await);
}

#[then("every surviving voter reports the same surviving nonzero controller")]
fn elected_controller(metadata_quorum: &MetadataQuorum) {
    let observation = metadata_quorum.leaders.get();
    let leaders = match observation {
        Ok(views) => views,
        Err(ControllerObservationError::TimedOut(views)) => {
            panic!("controller agreement timed out with complete voter observations: {views:?}");
        }
        Err(ControllerObservationError::WatchClosed(views)) => {
            panic!("controller leader watch closed before agreement: {views:?}");
        }
    };
    let leader = leaders
        .values()
        .find_map(|leader| *leader)
        .expect("at least one voter reported a controller");
    assert!(leader != NodeId(0));
    assert!(
        leaders.contains_key(&leader),
        "the elected controller is a surviving voter: {leaders:?}"
    );
    let expected: BTreeMap<_, _> = metadata_quorum
        .cluster
        .get()
        .iter()
        .map(|(_, config, _)| (config.node_id, Some(leader)))
        .collect();
    assert!(
        leaders == &expected,
        "every surviving voter must report the same controller"
    );
    assert!(!metadata_quorum.killed.is_set() || *metadata_quorum.killed.get() != leader);
}

#[when("a client creates topic {topic} with {partitions} partitions through voter index {index}")]
async fn create_on_voter(
    metadata_quorum: &mut MetadataQuorum,
    topic: String,
    partitions: i32,
    index: usize,
) {
    let client = connect_client(
        metadata_quorum.cluster.get()[index]
            .1
            .listen_addr
            .to_string(),
        None,
    )
    .await;
    let response = client
        .send(create_topic_request(
            creatable_topic().name(topic).partitions(partitions).call(),
        ))
        .await
        .unwrap();
    metadata_quorum
        .creates
        .set(vec![response.topics[0].error_code]);
    client.close();
}

#[when("a client creates topic via-follower through a metadata follower")]
async fn create_on_follower(metadata_quorum: &mut MetadataQuorum) {
    let index = metadata_quorum
        .cluster
        .get()
        .iter()
        .position(|(broker, config, _)| broker.controller_leader_id() != Some(config.node_id))
        .expect("metadata follower");
    create_on_voter(metadata_quorum, "via-follower".into(), 1, index).await;
}

#[then("the latest topic creation succeeds")]
fn quorum_topic_created(metadata_quorum: &MetadataQuorum) {
    assert!(
        metadata_quorum.creates.get().last() == Some(&0),
        "{:?}",
        metadata_quorum.creates.get()
    );
}

#[when("a client reads metadata through voter index 2 after prop reaches its image")]
async fn propagated_topic_metadata(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum.cluster.get()[2]
        .0
        .wait_until_partition_present("prop", 0)
        .await;
    let client = connect_client(
        metadata_quorum.cluster.get()[2].1.listen_addr.to_string(),
        None,
    )
    .await;
    metadata_quorum
        .metadata
        .set(client.send(topic_metadata_request(None)).await.unwrap());
    client.close();
}

#[then("voter index 2 reports topic prop in client metadata")]
fn propagated_topic(metadata_quorum: &MetadataQuorum) {
    assert!(
        metadata_quorum
            .metadata
            .get()
            .topics
            .iter()
            .any(|topic| topic.name.as_deref() == Some("prop")),
        "topic prop not visible to voter index 2"
    );
}

#[when("the elected metadata controller is shut down")]
async fn stop_controller(metadata_quorum: &mut MetadataQuorum) {
    let index = metadata_quorum
        .cluster
        .get()
        .iter()
        .position(|(broker, config, _)| broker.controller_leader_id() == Some(config.node_id))
        .expect("self-identified controller");
    let (broker, config, directory) = metadata_quorum.cluster.get_mut().remove(index);
    metadata_quorum.killed.set(config.node_id);
    metadata_quorum.retired_directory.set(directory);
    broker.shutdown().await;
    replace_controller(metadata_quorum).await;
}

async fn replace_controller(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum.leaders.set(
        observe_controller_agreement(
            metadata_quorum.cluster.get(),
            Some(*metadata_quorum.killed.get()),
        )
        .await,
    );
}

#[cfg(not(target_os = "macos"))]
#[when("clients concurrently create topic race through all three voters")]
async fn concurrent_topic_requests(metadata_quorum: &mut MetadataQuorum) {
    let mut clients = Vec::new();
    for (_, config, _) in metadata_quorum.cluster.get() {
        clients.push(connect_client(config.listen_addr.to_string(), None).await);
    }
    let mut joins = Vec::new();
    for client in clients {
        joins.push(tokio::spawn(async move {
            client
                .send(create_topic_request(
                    creatable_topic().name("race".into()).call(),
                ))
                .await
                .unwrap()
        }));
    }
    let mut results = Vec::new();
    for join in joins {
        results.push(join.await.unwrap().topics[0].error_code);
    }
    metadata_quorum.creates.set(results);
    for (broker, _, _) in metadata_quorum.cluster.get() {
        broker.wait_until_partition_present("race", 0).await;
    }
}

#[cfg(not(target_os = "macos"))]
#[then("the race returns one success plus two TOPIC_ALREADY_EXISTS refusals")]
fn one_creation_wins(metadata_quorum: &MetadataQuorum) {
    let mut codes = metadata_quorum.creates.get().clone();
    codes.sort_unstable();
    assert!(codes == vec![0, 36, 36]);
}

krabka_bdd::scenarios!(
    "tests/features/migrated/tests/quorum.feature",
    fixtures = [metadata_quorum: MetadataQuorum],
    world = metadata_quorum,
    runtime = "tokio-multi-thread",
    workers = 4,
);

#[cfg(not(target_os = "macos"))]
krabka_bdd::scenarios!(
    "tests/features/metadata_quorum/concurrent_creation.feature",
    fixtures = [metadata_quorum: MetadataQuorum],
    world = metadata_quorum,
    runtime = "tokio-multi-thread",
    workers = 4,
);
