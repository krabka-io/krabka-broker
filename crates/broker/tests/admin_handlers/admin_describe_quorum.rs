//! `DescribeQuorum` (`api_key` 55, KIP-595): the dispatch glue, the ACL allow
//! path, and the response encoding, driven against the `__cluster_metadata`
//! topic of a one-broker cluster; and the broker-listener forwarding to the
//! active controller (#1034) on a two-broker combined cluster.

use {
    crate::{
        admin_harness::{AdminCluster, admin, build_client},
        support::quorum::{
            describe_quorum_request as support_describe_quorum_request, follower_config,
            metadata_quorum_request as describe_quorum_request, two_node_leader,
        },
    },
    assert2::{assert, check},
    krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

/// Zero out the parts of a response that legitimately keep advancing between
/// two requests answered moments apart by the same live leader: the
/// per-replica wall-clock timestamps, each replica's raft log position, and
/// the partition's high watermark. The leader's raft log keeps taking new
/// entries (periodic quorum-internal writes) the whole time the cluster
/// runs, so two `DescribeQuorum` snapshots taken microseconds apart can
/// legitimately disagree on exactly how far the log has moved even though
/// they name the same leader, epoch, and voter set -- that agreement is what
/// this comparison is for.
fn without_replica_progress(mut resp: DescribeQuorumResponse) -> DescribeQuorumResponse {
    for topic in &mut resp.topics {
        for partition in &mut topic.partitions {
            partition.high_watermark = 0;
            for replica in partition
                .current_voters
                .iter_mut()
                .chain(&mut partition.observers)
            {
                replica.log_end_offset = 0;
                replica.last_fetch_timestamp = 0;
                replica.last_caught_up_timestamp = 0;
            }
        }
    }
    resp
}

struct QuorumDiscovery {
    response: Option<DescribeQuorumResponse>,
    baseline: Option<DescribeQuorumResponse>,
    leader: Option<i32>,
}

#[fixture]
fn quorum() -> QuorumDiscovery {
    // Start this scenario with fresh inputs and observations.

    QuorumDiscovery {
        response: None,
        baseline: None,
        leader: None,
    }
}

#[given("the active controller of the two-broker quorum is known")]
async fn known_controller(admin: &AdminCluster, quorum: &mut QuorumDiscovery) {
    let response = admin
        .client()
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from node 0");
    assert!(response.error_code == 0, "top-level error_code");
    quorum.leader = Some(two_node_leader(&response, (1, 2)));
    quorum.baseline = Some(response);
}

#[when("the admin describes the cluster metadata quorum")]
async fn describe_metadata_quorum(admin: &AdminCluster, quorum: &mut QuorumDiscovery) {
    quorum.response = Some(
        admin
            .client()
            .send(describe_quorum_request())
            .await
            .expect("describe_quorum"),
    );
}

#[when("the non-controller broker receives DescribeQuorum")]
async fn describe_through_follower(admin: &AdminCluster, quorum: &mut QuorumDiscovery) {
    let cfg = follower_config(
        admin.cluster(),
        quorum.leader.expect("a known active controller"),
    );
    let client = build_client(cfg.listen_addr).await;
    quorum.response = Some(
        client
            .send(describe_quorum_request())
            .await
            .expect("describe_quorum from the follower"),
    );
}

#[when("the admin asks DescribeQuorum for a non-metadata topic")]
async fn describe_unknown_quorum(admin: &AdminCluster, quorum: &mut QuorumDiscovery) {
    let request = support_describe_quorum_request("not-the-metadata-topic", vec![0]);
    quorum.response = Some(admin.client().send(request).await.expect("describe_quorum"));
}

#[then("the metadata quorum names broker 1 as its only voter and elected leader")]
fn single_metadata_voter(quorum: &QuorumDiscovery) {
    let resp = quorum
        .response
        .as_ref()
        .expect("the metadata quorum was described");
    check!(resp.error_code == 0, "top-level error_code");
    assert!(resp.topics.len() == 1, "exactly one topic row");
    check!(resp.topics[0].topic_name == "__cluster_metadata");
    let pd = &resp.topics[0].partitions[0];
    check!(pd.partition_index == 0);
    check!(pd.error_code == 0, "metadata partition 0 succeeds");
    check!(
        pd.leader_id == 1,
        "1-broker cluster: bootstrap voter id=1 is leader"
    );
    check!(
        pd.leader_epoch >= 1,
        "openraft term must be >= 1 once a leader is elected; got {}",
        pd.leader_epoch,
    );
    check!(
        pd.high_watermark >= 0,
        "last_applied_index is non-negative once any record applies; got {}",
        pd.high_watermark,
    );
    assert!(
        pd.current_voters.len() == 1,
        "single voter for 1-broker cluster"
    );
    check!(pd.current_voters[0].replica_id == 1);
    check!(
        pd.current_voters[0].log_end_offset >= 0,
        "leader knows its own matched index; got {}",
        pd.current_voters[0].log_end_offset,
    );
    check!(
        pd.observers.is_empty(),
        "a lone combined node has no observer fetching from it"
    );
}

#[then("the forwarded quorum response matches the active controller with real voter progress")]
fn forwarded_response(quorum: &QuorumDiscovery) {
    let follower_resp = quorum.response.as_ref().expect("the follower was queried");
    let first_resp = quorum
        .baseline
        .as_ref()
        .expect("the active controller is known");
    let leader_id = quorum.leader.expect("the active controller is known");
    check!(follower_resp.error_code == 0, "top-level error_code");
    let follower_partition = &follower_resp.topics[0].partitions[0];
    check!(
        follower_partition.error_code == 0,
        "forwarded to the leader instead of answered locally"
    );
    check!(
        follower_partition.leader_id == leader_id,
        "the follower's forwarded answer names the same leader"
    );
    assert!(
        follower_partition
            .current_voters
            .iter()
            .all(|v| v.log_end_offset >= 0),
        "a follower answering locally would report -1 for every voter; \
         the forwarded answer carries the leader's real matched indexes: {:?}",
        follower_partition.current_voters,
    );

    assert!(
        without_replica_progress(follower_resp.clone())
            == without_replica_progress(first_resp.clone())
    );
}

#[then("the unknown quorum partition returns Kafka UNKNOWN_TOPIC_OR_PARTITION and its message")]
fn unknown_partition(quorum: &QuorumDiscovery) {
    let resp = quorum
        .response
        .as_ref()
        .expect("the unknown quorum topic was requested");
    check!(resp.error_code == 0, "top-level error_code");
    assert!(resp.topics.len() == 1);
    let pd = &resp.topics[0].partitions[0];
    check!(pd.error_code == 3, "UNKNOWN_TOPIC_OR_PARTITION, not 17");
    check!(
        pd.error_message.as_deref() == Some("This server does not host this topic-partition."),
        "Kafka's Errors.UNKNOWN_TOPIC_OR_PARTITION.message(); got {:?}",
        pd.error_message,
    );
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_quorum.feature",
    name = "describe quorum reports cluster metadata voter set"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_quorum_reports_cluster_metadata_voter_set(
    admin: AdminCluster,
    quorum: QuorumDiscovery,
) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_quorum.feature",
    name = "combined follower forwards describe quorum to the active controller"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_follower_forwards_describe_quorum_to_the_active_controller(
    admin: AdminCluster,
    quorum: QuorumDiscovery,
) {
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_describe_quorum.feature",
    name = "describe quorum for another topic is unknown topic or partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_quorum_for_another_topic_is_unknown_topic_or_partition(
    admin: AdminCluster,
    quorum: QuorumDiscovery,
) {
    admin.shutdown().await;
}
