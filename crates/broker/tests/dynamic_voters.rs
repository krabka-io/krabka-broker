//! KIP-853 dynamic-voters end-to-end integration tests.
//!
//! These tests exercise the *real* auto-join path. Broker 0 self-bootstraps as
//! the sole voter. Brokers 1 to n then boot in `Join` mode with
//! `auto_join = true` and grow the quorum by sending `AddRaftVoter(self)` to
//! the leader over the wire. The shrink test then removes one voter through
//! the controller leader and asserts that the committed voter set contracts on
//! every node.
//!
//! openraft's debug assertions race on the hosted Windows scheduler, so these
//! tests are gated off Windows, like the other multi-node suites.
#![cfg(test)]

use {
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId},
    krabka_raft::{
        RaftError,
        reconfig::{ReconfigOutcome, RemoveVoter},
    },
    krabka_units::Time,
    std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{net::TcpListener, time},
};

/// The controller fetch timeout of every node, pinned here rather than
/// inherited from `BrokerConfig::for_tests` because the tests depend on it.
///
/// Every node runs in this one process, so a stall of the process, from a
/// loaded runner or a `SIGSTOP`, stalls the whole quorum at once. The leader
/// resigns when a majority has not fetched within its check-quorum window,
/// 1.5 times this timeout as Kafka's `LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR`
/// sets it. A stall at least that long expires the window while the
/// followers are frozen too, so the leader resigns at every resume and the
/// quorum never keeps a leader long enough to add or remove a voter (#621).
/// At 200 ms, the window was 300 ms, and the 400 ms freezes of #621 stalled
/// every run. At 500 ms, the window is 750 ms: a stall of 400 ms, or of 600
/// ms, still lets the quorum converge.
const FETCH_TIMEOUT: Time = krabka_units::millis(500);

/// A started node, and the directory that backs it.
struct DynamicNode {
    handle: BrokerHandle,
    dir: TempDir,
}

async fn start_dynamic_cluster(n: u64) -> Vec<DynamicNode> {
    let mut cluster = Vec::new();
    join_dynamic_nodes(&mut cluster, n).await;
    cluster
}
async fn join_dynamic_nodes(cluster: &mut Vec<DynamicNode>, n: u64) {
    let cluster_id = uuid::Uuid::from_u128(853);
    let mut bootstrap_controller = cluster.first().map(|node| node.handle.controller_addr());
    let first_id = u64::try_from(cluster.len()).expect("existing voter count") + 1;
    for id in first_id..=n {
        let data_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_addr = data_listener.local_addr().unwrap();
        let controller_addr = controller_listener.local_addr().unwrap();
        let dir = TempDir::new().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.broker_id = i32::try_from(id).unwrap();
        config.node_id = NodeId(id);
        config.directory_id = uuid::Uuid::from_u128(u128::from(id));
        config.cluster_id = Some(cluster_id);
        config.listen_addr = data_addr;
        config.advertised_listener = data_addr.to_string();
        config.controller_listen_addr = controller_addr;
        config.controller_fetch_timeout = FETCH_TIMEOUT;
        config.auto_join_retry_backoff = krabka_units::millis(20);
        config.startup_leader_wait_timeout = krabka_units::secs(10);

        if let Some(bootstrap) = bootstrap_controller {
            config.bootstrap_mode = BootstrapMode::Join;
            config.controller_quorum_voters = vec![(NodeId(1), bootstrap.to_string())];
            config.bootstrap_servers = vec![bootstrap.to_string()];
            config.auto_join = true;
        } else {
            config.bootstrap_mode = BootstrapMode::Bootstrap;
            config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
        }

        let handle = time::timeout(
            Duration::from_secs(15),
            Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener)),
        )
        .await
        .expect("dynamic controller start timed out")
        .expect("dynamic controller start");
        if bootstrap_controller.is_none() {
            bootstrap_controller = Some(handle.controller_addr());
            let outcome = handle
                .finalize_kraft_version_for_test(1)
                .await
                .expect("activate kraft.version 1");
            assert!(matches!(outcome, ReconfigOutcome::Committed));
        }
        eprintln!(
            "started node {id}: voters={} kraft.version={} leader={:?}",
            handle.voter_count_for_test(),
            handle.kraft_version_for_test(),
            handle.controller_leader_id()
        );
        cluster.push(DynamicNode { handle, dir });
    }
}

#[derive(Default)]
struct DynamicVoterScenario {
    cluster: Vec<DynamicNode>,
    removal: Option<RemoveVoter>,
    remaining: BTreeSet<NodeId>,
    answers: Vec<String>,
}
#[rstest::fixture]
fn dynamic_voter() -> DynamicVoterScenario {
    let cluster = Vec::new();
    DynamicVoterScenario {
        cluster,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "node 1 bootstraps alone with kraft.version 1 and a 500ms controller fetch timeout"
)]
async fn initial_dynamic_voter(dynamic_voter: &mut DynamicVoterScenario) {
    dynamic_voter.cluster = start_dynamic_cluster(1).await;
}
#[rstest_bdd_macros::when(
    "nodes 2 and 3 join using AddRaftVoter auto-join over the controller listener"
)]
async fn grow_dynamic_voter_set(dynamic_voter: &mut DynamicVoterScenario) {
    join_dynamic_nodes(&mut dynamic_voter.cluster, 3).await;
}
#[rstest_bdd_macros::then("the leader and every joined node commit exactly voter IDs 1, 2 and 3")]
async fn committed_dynamic_voter_growth(dynamic_voter: &mut DynamicVoterScenario) {
    let leader = wait_for_leader(&dynamic_voter.cluster).await;
    leader
        .wait_for_image(|image| image.voters().len() == 3)
        .await;
    let expected = BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]);
    for node in &dynamic_voter.cluster {
        node.handle
            .wait_for_image(|image| image.voters().ids() == expected)
            .await;
        assert!(node.handle.voter_ids_for_test() == expected);
    }
    for DynamicNode { handle, dir: _dir } in mem::take(&mut dynamic_voter.cluster) {
        handle.shutdown().await;
    }
}
#[rstest_bdd_macros::given(
    "a three-voter dynamic quorum selects its {role} for removal by its committed directory ID"
)]
async fn selected_dynamic_voter(dynamic_voter: &mut DynamicVoterScenario, role: String) {
    let cluster = start_dynamic_cluster(3).await;
    for node in &cluster {
        node.handle
            .wait_for_image(|image| image.voters().len() == 3)
            .await;
    }
    let leader = wait_for_leader(&cluster).await;
    let voters = leader.voter_ids_for_test();
    let leader_id = NodeId(leader.node_id());
    let victim = match role.as_str() {
        "leader" => leader_id,
        "follower" => voters
            .iter()
            .copied()
            .find(|id| *id != leader_id)
            .expect("follower voter"),
        other => panic!("unknown voter role {other}"),
    };
    dynamic_voter.removal = Some(RemoveVoter {
        id: victim,
        directory_id: leader
            .voter_directory_id_for_test(victim)
            .expect("committed voter directory"),
    });
    dynamic_voter.remaining = voters.into_iter().filter(|id| *id != victim).collect();
    dynamic_voter.cluster = cluster;
}
#[rstest_bdd_macros::when(
    "the admin removes that voter starting at a follower and follows controller leadership changes"
)]
async fn remove_selected_dynamic_voter(dynamic_voter: &mut DynamicVoterScenario) {
    dynamic_voter.answers = remove_through_leader(
        &dynamic_voter.cluster,
        dynamic_voter.removal.as_ref().expect("selected voter"),
        &dynamic_voter.remaining,
    )
    .await;
}
#[rstest_bdd_macros::then(
    "every node commits the remaining two-voter set, the removed node follows as an observer and a remaining voter leads"
)]
async fn committed_dynamic_voter_removal(dynamic_voter: &mut DynamicVoterScenario) {
    let victim = dynamic_voter.removal.as_ref().expect("removed voter").id;
    let expected = &dynamic_voter.remaining;
    assert!(expected.len() == 2);
    for entry in &dynamic_voter.cluster {
        if NodeId(entry.handle.node_id()) != victim {
            entry
                .handle
                .wait_for_image(|image| image.voters().ids() == *expected)
                .await;
        }
    }
    node(&dynamic_voter.cluster, victim)
        .expect("removed observer")
        .wait_for_image(|image| image.voters().ids() == *expected)
        .await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !self_reported_leader(&dynamic_voter.cluster)
        .is_some_and(|handle| expected.contains(&NodeId(handle.node_id())))
    {
        assert!(
            Instant::now() <= deadline,
            "no remaining voter leads after removal of {victim}; answers {:?}",
            dynamic_voter.answers
        );
        time::sleep(Duration::from_millis(20)).await;
    }
    for DynamicNode { handle, dir: _dir } in mem::take(&mut dynamic_voter.cluster) {
        handle.shutdown().await;
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/dynamic_voters.feature",
    name = "Auto-join grows a standalone quorum to three"
)]
#[tokio::test]
async fn auto_join_grows_quorum_to_three(dynamic_voter: DynamicVoterScenario) {}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/dynamic_voters.feature",
    name = "Removing either controller role shrinks the committed quorum"
)]
#[tokio::test]
async fn remove_voter_shrinks_quorum(dynamic_voter: DynamicVoterScenario, role: String) {}

/// The node that believes that it leads the controller quorum, if one does.
fn self_reported_leader(cluster: &[DynamicNode]) -> Option<&BrokerHandle> {
    cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| handle.controller_leader_id() == Some(NodeId(handle.node_id())))
}

/// Wait until a node believes that it leads the controller quorum. An
/// election can be in progress when the caller looks.
async fn wait_for_leader(cluster: &[DynamicNode]) -> &BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(leader) = self_reported_leader(cluster) {
            return leader;
        }
        assert!(
            Instant::now() <= deadline,
            "no controller leader within 30 s"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

fn node(cluster: &[DynamicNode], id: NodeId) -> Option<&BrokerHandle> {
    cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| NodeId(handle.node_id()) == id)
}

/// Remove `victim` the way a Kafka admin client that uses
/// `bootstrap.controllers` does. The first attempt goes to a controller that
/// does not lead. Every later attempt goes to the leader that the last answer
/// named, or to a node that believes that it leads.
///
/// The loop retries these answers:
///
/// - `Ok(NotLeader)`: the node did not lead when it read the request, so it
///   appended nothing. Kafka answers `NOT_LEADER_OR_FOLLOWER`.
/// - `Err(ReconfigInProgress)`: an earlier voter change or the leader's new
///   epoch is not committed yet. Kafka answers `REQUEST_TIMED_OUT`.
/// - `Err(NotLeader)`: the node appended the removal and then lost the
///   leadership before the removal committed. Kafka's `LeaderState.close`
///   answers the pending request with `NOT_LEADER_OR_FOLLOWER`. A later leader
///   can still commit that record, because the record can be in its log.
///
/// `Err(VoterNotFound(victim))` ends the loop only after an `Err(NotLeader)`,
/// and only when the answering leader's committed voter set is the expected
/// set. A leader decides `VoterNotFound` after the check that its own epoch
/// is committed, so its committed voter set holds every voter change from
/// before its epoch. The victim can be missing from that set only because an
/// earlier attempt's removal committed. Kafka's `RemoveVoterHandler` answers
/// `VOTER_NOT_FOUND` in the same case.
///
/// Returns the answers, in order, for the test log.
async fn remove_through_leader(
    cluster: &[DynamicNode],
    request: &RemoveVoter,
    expected: &BTreeSet<NodeId>,
) -> Vec<String> {
    let first = cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| handle.controller_leader_id() != Some(NodeId(handle.node_id())))
        .expect("a controller that does not lead");
    let mut target = Some(NodeId(first.node_id()));
    let mut appended_then_lost_leadership = false;
    let mut answers = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() <= deadline,
            "the removal of {} did not finish; answers: {answers:?}",
            request.id,
        );
        let Some(handle) = target
            .and_then(|id| node(cluster, id))
            .or_else(|| self_reported_leader(cluster))
        else {
            time::sleep(Duration::from_millis(20)).await;
            continue;
        };
        let outcome = handle.remove_voter_for_test(request.clone()).await;
        answers.push(format!("node {}: {outcome:?}", handle.node_id()));
        target = match outcome {
            Ok(ReconfigOutcome::Committed) => return answers,
            Ok(ReconfigOutcome::NotLeader { leader }) => leader,
            Err(RaftError::ReconfigInProgress) => Some(NodeId(handle.node_id())),
            Err(RaftError::NotLeader { current_leader }) => {
                appended_then_lost_leadership = true;
                current_leader
            }
            Err(RaftError::VoterNotFound(id)) if id == request.id => {
                let committed = handle.voter_ids_for_test();
                assert!(
                    appended_then_lost_leadership && committed == *expected,
                    "VoterNotFound({id}) with no earlier removal that could commit, or with \
                     committed voters {committed:?}; answers: {answers:?}",
                );
                return answers;
            }
            Err(error) => panic!("remove_voter RPC: {error:?}; answers: {answers:?}"),
        };
        time::sleep(Duration::from_millis(20)).await;
    }
}

use std::mem;
