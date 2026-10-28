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
    krabka_broker::{BrokerConfig, BrokerHandle},
    std::collections::HashSet,
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

fn controller_leaders(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
) -> HashSet<krabka_broker::NodeId> {
    cluster
        .iter()
        .filter_map(|(handle, _, _)| handle.controller_leader_id())
        .collect()
}

use {
    krabka_broker::NodeId,
    krabka_protocol::owned::metadata_response::MetadataResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tokio::sync::MutexGuard,
};

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

#[derive(Default)]
struct MetadataQuorum {
    cluster: Cluster,
    guard: Option<MutexGuard<'static, ()>>,
    killed: Option<NodeId>,
    retired_directory: Option<TempDir>,
    leaders: Option<HashSet<NodeId>>,
    creates: Vec<i16>,
    metadata: Option<MetadataResponse>,
}

#[fixture]
fn metadata_quorum() -> MetadataQuorum {
    // Every quorum scenario owns its members and retains observed wire results.
    MetadataQuorum::default()
}

#[given("three brokers share a static metadata voter set")]
async fn metadata_voters(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum.guard = Some(cluster_lock().lock().await);
    metadata_quorum.cluster = support::start_n_node_with_retry(3).await;
}

#[given("all three metadata voters have observed a controller leader")]
async fn controller_ready(metadata_quorum: &MetadataQuorum) {
    wait_for_leader(&metadata_quorum.cluster).await;
}

#[given("every voter has registered all three brokers")]
async fn voters_registered(metadata_quorum: &MetadataQuorum) {
    for (broker, _, _) in &metadata_quorum.cluster {
        broker.wait_until_brokers_registered(3).await;
    }
}

#[when("all metadata voters observe their elected controller")]
async fn observe_election(metadata_quorum: &mut MetadataQuorum) {
    wait_for_leader(&metadata_quorum.cluster).await;
    metadata_quorum.leaders = Some(controller_leaders(&metadata_quorum.cluster));
}

#[then("every surviving voter agrees on one nonzero controller")]
fn elected_controller(metadata_quorum: &MetadataQuorum) {
    let leaders = metadata_quorum
        .leaders
        .as_ref()
        .expect("controller views observed");
    assert!(
        leaders.len() == 1 && !leaders.contains(&NodeId(0)),
        "leader not converged: {leaders:?}"
    );
    if let Some(killed) = metadata_quorum.killed {
        assert!(
            !leaders.contains(&killed),
            "removed leader remains elected: {leaders:?}"
        );
    }
}

#[when("a client creates topic {topic} with {partitions} partitions through voter index {index}")]
async fn create_on_voter(
    metadata_quorum: &mut MetadataQuorum,
    topic: String,
    partitions: i32,
    index: usize,
) {
    let client = connect_client(
        metadata_quorum.cluster[index].1.listen_addr.to_string(),
        None,
    )
    .await;
    let response = client
        .send(create_topic_request(
            creatable_topic().name(topic).partitions(partitions).call(),
        ))
        .await
        .unwrap();
    metadata_quorum.creates.push(response.topics[0].error_code);
    client.close();
}

#[when("a client creates topic via-follower through a metadata follower")]
async fn create_on_follower(metadata_quorum: &mut MetadataQuorum) {
    let index = metadata_quorum
        .cluster
        .iter()
        .position(|(broker, config, _)| broker.controller_leader_id() != Some(config.node_id))
        .expect("metadata follower");
    create_on_voter(metadata_quorum, "via-follower".into(), 1, index).await;
}

#[then("the latest topic creation succeeds")]
fn quorum_topic_created(metadata_quorum: &MetadataQuorum) {
    assert!(
        metadata_quorum.creates.last() == Some(&0),
        "{:?}",
        metadata_quorum.creates
    );
}

#[when("a client reads metadata through voter index 2 after prop reaches its image")]
async fn propagated_topic_metadata(metadata_quorum: &mut MetadataQuorum) {
    metadata_quorum.cluster[2]
        .0
        .wait_until_partition_present("prop", 0)
        .await;
    let client = connect_client(metadata_quorum.cluster[2].1.listen_addr.to_string(), None).await;
    metadata_quorum.metadata = Some(client.send(topic_metadata_request(None)).await.unwrap());
    client.close();
}

#[then("voter index 2 reports topic prop in client metadata")]
fn propagated_topic(metadata_quorum: &MetadataQuorum) {
    assert!(
        metadata_quorum
            .metadata
            .as_ref()
            .expect("metadata read")
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
        .iter()
        .position(|(broker, config, _)| broker.controller_leader_id() == Some(config.node_id))
        .expect("self-identified controller");
    let (broker, config, directory) = metadata_quorum.cluster.remove(index);
    metadata_quorum.killed = Some(config.node_id);
    metadata_quorum.retired_directory = Some(directory);
    broker.shutdown().await;
}

#[when("the surviving voters elect a replacement controller")]
async fn replace_controller(metadata_quorum: &mut MetadataQuorum) {
    for (broker, _, _) in &metadata_quorum.cluster {
        support::await_controller_replacement(
            broker,
            metadata_quorum.killed.expect("removed controller"),
            "no new leader within 30s after kill",
        )
        .await;
    }
    metadata_quorum.leaders = Some(controller_leaders(&metadata_quorum.cluster));
}

#[cfg(not(target_os = "macos"))]
#[when("clients concurrently create topic race through all three voters")]
async fn concurrent_topic_requests(metadata_quorum: &mut MetadataQuorum) {
    let mut clients = Vec::new();
    for (_, config, _) in &metadata_quorum.cluster {
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
    for join in joins {
        metadata_quorum
            .creates
            .push(join.await.unwrap().topics[0].error_code);
    }
    for (broker, _, _) in &metadata_quorum.cluster {
        broker.wait_until_partition_present("race", 0).await;
    }
}

#[cfg(not(target_os = "macos"))]
#[then("exactly one race creation succeeds and the other two report TOPIC_ALREADY_EXISTS")]
fn one_creation_wins(metadata_quorum: &MetadataQuorum) {
    let zero = metadata_quorum
        .creates
        .iter()
        .filter(|code| **code == 0)
        .count();
    let already = metadata_quorum
        .creates
        .iter()
        .filter(|code| **code == 36)
        .count();
    assert!(metadata_quorum.creates.len() == 3);
    assert!(
        zero == 1,
        "exactly one winner, got zero={zero} already={already}"
    );
    assert!(
        already == 2,
        "two losers see TOPIC_ALREADY_EXISTS, got zero={zero} already={already}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/quorum.feature",
    name = "three node cluster elects leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader(metadata_quorum: MetadataQuorum) {
    shutdown_cluster(take(&mut metadata_quorum.cluster)).await;
    drop(metadata_quorum);
}

#[scenario(
    path = "tests/features/migrated/tests/quorum.feature",
    name = "create topic on any node propagates"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_topic_on_any_node_propagates(metadata_quorum: MetadataQuorum) {
    shutdown_cluster(take(&mut metadata_quorum.cluster)).await;
    drop(metadata_quorum);
}

#[scenario(
    path = "tests/features/migrated/tests/quorum.feature",
    name = "leader kill recovers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_recovers(metadata_quorum: MetadataQuorum) {
    shutdown_cluster(take(&mut metadata_quorum.cluster)).await;
    drop(metadata_quorum);
}

#[scenario(
    path = "tests/features/migrated/tests/quorum.feature",
    name = "follower forwards create topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_forwards_create_topic(metadata_quorum: MetadataQuorum) {
    shutdown_cluster(take(&mut metadata_quorum.cluster)).await;
    drop(metadata_quorum);
}

#[cfg(not(target_os = "macos"))]
#[scenario(
    path = "tests/features/migrated/tests/quorum.feature",
    name = "concurrent topic creates one wins"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_topic_creates_one_wins(metadata_quorum: MetadataQuorum) {
    shutdown_cluster(take(&mut metadata_quorum.cluster)).await;
    drop(metadata_quorum);
}

use std::mem::take;
