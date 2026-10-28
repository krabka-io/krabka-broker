//! The per-partition error paths, where the response still carries a row for
//! every partition the request named.
//!
//! An unknown topic and an out-of-range index both report
//! `UNKNOWN_TOPIC_OR_PARTITION`, while a partition the broker knows about but
//! does not host reports `NOT_LEADER_OR_FOLLOWER`.

use {
    crate::{
        producers_harness::{ProducerSnapshot, create_topic, snapshot},
        support::{client::connect_with_context, registered_cluster},
    },
    assert2::{assert, check},
};

#[rstest_bdd_macros::then("both unknown-topic partition rows return error 3 and no producers")]
fn unknown_producer_topic(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 2);
    for part in &resp.topics[0].partitions {
        assert!(
            part.error_code == 3,
            "unknown topic must surface UNKNOWN_TOPIC_OR_PARTITION (3) per partition, got {part:?}"
        );
        assert!(part.active_producers.is_empty());
    }
}
#[rstest_bdd_macros::then("partition 0 succeeds while nonexistent partition 5 returns error 3")]
fn out_of_range_producer_partition(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    assert!(resp.topics[0].partitions.len() == 2);
    // Partition 0 exists → error_code 0.
    let p0 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .expect("p0");
    assert!(p0.error_code == 0, "{p0:?}");
    // Partition 5 doesn't → UNKNOWN_TOPIC_OR_PARTITION.
    let p5 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 5)
        .expect("p5");
    assert!(p5.error_code == 3, "{p5:?}");
}
#[rstest_bdd_macros::then("the non-hosted partition row has index 0 error 6 and no producers")]
fn nonhosted_producer_partition(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    let partition = &resp.topics[0].partitions[0];
    assert!(partition.partition_index == 0);
    check!(partition.error_code == 6, "expected NOT_LEADER_OR_FOLLOWER");
    check!(partition.active_producers.is_empty());
}
#[rstest_bdd_macros::given(
    "a two broker cluster knows remote partition 0 but the connected broker does not host it"
)]
async fn nonhosted_producer_setup(snapshot: &mut ProducerSnapshot) {
    let cluster = registered_cluster(2).await;

    let admin =
        connect_with_context(cluster[0].0.listen_addr().to_string(), None, "admin client").await;
    create_topic(&admin, "remote").await;
    cluster[0].0.wait_until_partition_present("remote", 0).await;

    let leader = cluster[0]
        .0
        .partition_leader_for_test("remote", 0)
        .expect("remote-0 leader");
    let nonleader = cluster
        .iter()
        .find(|(broker, _, _)| broker.node_id() != leader)
        .expect("two-node cluster has a nonleader");
    nonleader.0.wait_until_partition_present("remote", 0).await;
    assert!(
        !nonleader.0.partition_exists_for_test("remote", 0),
        "rf=1 nonleader must not host remote-0"
    );

    let client = connect_with_context(
        nonleader.0.listen_addr().to_string(),
        None,
        "nonleader client",
    )
    .await;

    snapshot.cluster = cluster;
    snapshot.connection = Some(client);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_errors.feature",
    name = "unknown topic returns unknown topic or partition"
)]
#[tokio::test]
async fn unknown_topic_returns_unknown_topic_or_partition(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_errors.feature",
    name = "out of range partition returns unknown topic or partition"
)]
#[tokio::test]
async fn out_of_range_partition_returns_unknown_topic_or_partition(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_errors.feature",
    name = "metadata known partition not hosted locally returns not leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_known_partition_not_hosted_locally_returns_not_leader(
    snapshot: ProducerSnapshot,
) {
    snapshot.shutdown().await;
}
