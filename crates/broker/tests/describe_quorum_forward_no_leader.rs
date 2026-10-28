//! `DescribeQuorum` forwarding failure answers a typed `NOT_LEADER_OR_FOLLOWER`
//! `DescribeQuorumResponse`, not a dropped connection (review of #1034).
//!
//! `describe_quorum::handle`'s forward path used to `map_err(BrokerError::from)`
//! and propagate the failure. The registry dispatch loop
//! (`network::dispatch::registry::send_registry_response`) has no response
//! shape to build for a bare `Err(BrokerError)` from a
//! `DispatchKind::Context` handler: it logs a warning and closes the
//! connection. A Kafka client asking `DescribeQuorum` during a dial failure
//! or before a leader is known expects the typed answer instead.
#![cfg(test)]

mod support;

use {
    crate::support::{client::connect_owned, shutdown_cluster, start_n_node_with_retry},
    assert2::{assert, check},
};

/// Kafka's `NOT_LEADER_OR_FOLLOWER`.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

async fn build_client(addr: SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "describe-quorum-no-leader-test",
        "client build",
    )
    .await
}

use crate::support::quorum::metadata_quorum_request as describe_quorum_request;

#[derive(Default)]
struct DeadLeaderForwarding {
    cluster: Vec<(
        krabka_broker::BrokerHandle,
        krabka_broker::BrokerConfig,
        tempfile::TempDir,
    )>,
    leader: i32,
    response: Option<DescribeQuorumResponse>,
}
#[rstest::fixture]
fn dead_leader_forwarding() -> DeadLeaderForwarding {
    let cluster = Vec::new();
    DeadLeaderForwarding {
        cluster,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "two controller voters have an elected leader and the follower knows its identity"
)]
async fn elected_two_voter_quorum(dead_leader_forwarding: &mut DeadLeaderForwarding) {
    let cluster = start_n_node_with_retry(2).await;
    let client = build_client(cluster[0].1.listen_addr).await;
    let first = client
        .send(describe_quorum_request())
        .await
        .expect("initial DescribeQuorum");
    check!(first.error_code == 0, "top-level error_code");
    let leader = first.topics[0].partitions[0].leader_id;
    assert!(
        leader == 1 || leader == 2,
        "two-voter elected leader: {leader}"
    );
    client.close();
    dead_leader_forwarding.cluster = cluster;
    dead_leader_forwarding.leader = leader;
}
#[rstest_bdd_macros::when(
    "the leader crashes and a client immediately sends DescribeQuorum to the surviving follower"
)]
async fn describe_quorum_after_leader_crash(dead_leader_forwarding: &mut DeadLeaderForwarding) {
    let index = dead_leader_forwarding
        .cluster
        .iter()
        .position(|(_, config, _)| config.broker_id == dead_leader_forwarding.leader)
        .expect("leader broker");
    let (leader, _, _directory) = dead_leader_forwarding.cluster.remove(index);
    leader.crash_for_test().await;
    let client = build_client(dead_leader_forwarding.cluster[0].1.listen_addr).await;
    dead_leader_forwarding.response = Some(
        client
            .send(describe_quorum_request())
            .await
            .expect("typed DescribeQuorum response rather than dropped connection"),
    );
}
#[rstest_bdd_macros::then(
    "the follower returns a typed NOT_LEADER_OR_FOLLOWER response instead of closing the client connection"
)]
async fn failed_quorum_forward_response(dead_leader_forwarding: &mut DeadLeaderForwarding) {
    let response = dead_leader_forwarding
        .response
        .as_ref()
        .expect("typed response");
    check!(
        response.error_code == NOT_LEADER_OR_FOLLOWER,
        "{response:?}"
    );
    shutdown_cluster(mem::take(&mut dead_leader_forwarding.cluster)).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/failed_quorum_forward.feature",
    name = "A dead leader hint produces a typed quorum error"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_whose_leader_just_died_answers_not_leader_instead_of_closing(
    dead_leader_forwarding: DeadLeaderForwarding,
) {
}

use {
    krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse,
    std::{mem, net::SocketAddr},
};
