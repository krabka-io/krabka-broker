//! A running follower removes a divergent local suffix and matches its leader again.
use {
    crate::{
        cluster_lock,
        epoch_harness::record,
        support::{
            self,
            client::connect_client,
            produce::{batch_request, replicated_topic_produce_for},
            registered_cluster, shutdown_cluster,
            topics::AssignedTopicSetup,
        },
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};
type ReplicaCluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;
struct DivergentFollower {
    cluster: Option<ReplicaCluster>,
    permit: Option<MutexGuard<'static, ()>>,
    topic: String,
    follower: Option<usize>,
    divergent_end: Option<i64>,
}
#[fixture]
fn follower() -> DivergentFollower {
    // Start this scenario with fresh inputs and observations.

    DivergentFollower {
        cluster: None,
        permit: None,
        topic: String::new(),
        follower: None,
        divergent_end: None,
    }
}
#[given(
    "three replicas of topic {topic} have acknowledged {records} numbered epoch zero records led by broker one"
)]
async fn replicated_prefix(follower: &mut DivergentFollower, topic: String, records: i64) {
    follower.permit = Some(cluster_lock().lock().await);
    let cluster = registered_cluster(3).await;
    let address = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(address.clone(), None).await;
    let assignment = AssignedTopicSetup::three_replicas(&topic);
    let id = support::topics::create_assigned_partition()
        .client(&admin)
        .name(assignment.name)
        .replicas(assignment.replicas)
        .brokers(cluster.iter().map(|(broker, _, _)| broker))
        .call()
        .await;
    let producer = connect_client(address, None).await;
    for index in 0..records {
        let response = producer
            .send(batch_request(
                record(&format!("v{index}")),
                replicated_topic_produce_for(&topic, id.0),
            ))
            .await
            .expect("replicated prefix produce");
        assert!(response.responses[0].partition_responses[0].error_code == 0);
    }
    for (broker, _, _) in &cluster {
        broker
            .wait_until_local_log_end_offset(&topic, 0, records)
            .await;
    }
    let leader = cluster[0]
        .0
        .partition_leader_for_test(&topic, 0)
        .expect("partition leader known");
    follower.follower = Some(
        cluster
            .iter()
            .position(|(broker, _, _)| broker.node_id() != leader)
            .expect("non leader replica exists"),
    );
    follower.cluster = Some(cluster);
    follower.topic = topic;
}
#[when(
    "a follower's local log receives a divergent suffix of {records} records without a new epoch checkpoint"
)]
async fn inject(follower: &mut DivergentFollower, records: usize) {
    let cluster = follower.cluster.as_ref().expect("replica cluster ready");
    let broker = &cluster[follower.follower.expect("follower selected")].0;
    broker
        .produce_records_for_test(&follower.topic, 0, records)
        .await
        .expect("inject divergent suffix");
    follower.divergent_end = broker.local_log_end_offset(&follower.topic, 0);
}
#[then("the divergent follower initially has log end offset {offset}")]
fn divergent(follower: &DivergentFollower, offset: i64) {
    assert!(
        follower.divergent_end == Some(offset),
        "follower must actually hold the injected divergent suffix"
    );
}
#[then("the running follower converges to its leader at log end offset {offset}")]
async fn converged(follower: &DivergentFollower, offset: i64) {
    let cluster = follower.cluster.as_ref().expect("replica cluster ready");
    let broker = &cluster[follower.follower.expect("follower selected")].0;
    broker
        .wait_until_local_log_end_offset_eq(&follower.topic, 0, offset)
        .await;
    let follower_end = broker
        .local_log_end_offset(&follower.topic, 0)
        .unwrap_or(-1);
    let leader_end = cluster[0]
        .0
        .local_log_end_offset(&follower.topic, 0)
        .unwrap_or(-1);
    assert!(
        follower_end == leader_end && follower_end == offset,
        "follower did not converge (follower={follower_end},leader={leader_end},expected={offset})"
    );
    assert!(
        broker.local_log_end_offset(&follower.topic, 0)
            == cluster[0].0.local_log_end_offset(&follower.topic, 0)
    );
}
#[scenario(
    path = "tests/features/migrated/tests/leader_epoch/epoch_diverge_follower.feature",
    name = "A running follower removes its divergent suffix and matches the leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_truncates_in_band_on_diverging_epoch(follower: DivergentFollower) {
    shutdown_cluster(follower.cluster.take().expect("replica cluster started")).await;
    drop(follower);
}
