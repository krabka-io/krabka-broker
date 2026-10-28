//! A share group's coordinator answers a heartbeat only once the records it
//! wrote are committed, so a member's epoch survives the coordinator moving.
//!
//! Kafka's `CoordinatorRuntime` completes a write when the high watermark of
//! the group's `__consumer_offsets` partition passes it. In
//! `ShareConsumerTest.test_broker_failure`, the coordinator of the group
//! answered three joins with epochs whose records were in its log only, and
//! then stopped cleanly. The broker that took the partition over had none of
//! them, and it answered every member's next heartbeat with
//! `GROUP_ID_NOT_FOUND`, which a share consumer cannot recover from.
#![cfg(test)]

use {
    crate::support::{
        client::connect_owned, discovery::coordinator_lookup_request, share::crash_follower,
        shutdown_cluster,
    },
    assert2::{assert, check},
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
};

mod support;

const GROUP: &str = "share-move";
const OFFSETS_TOPIC: &str = "__consumer_offsets";
const KEY_TYPE_GROUP: i8 = 0;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// The time a lookup may take to see a creation or a coordinator move.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// Three brokers with one `__consumer_offsets` partition, replicated on all
/// of them. A stopped broker stays in its ISRs: the leader does not shrink
/// them on lag, and the controller does not fence the broker, for longer
/// than any test here runs.
async fn start_three() -> Cluster {
    support::fixed_internal_isr_cluster(|config| {
        config.offsets_topic_num_partitions = 1;
    })
    .await
}

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "share-coordinator-move",
        "client",
    )
    .await
}

/// Look `GROUP` up until a broker other than `excluded` answers as its
/// coordinator, and return that broker's node id.
async fn coordinator_of(client: &Client, excluded: Option<u64>) -> u64 {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = client
            .send(coordinator_lookup_request(GROUP, KEY_TYPE_GROUP, vec![
                GROUP.into(),
            ]))
            .await
            .expect("FindCoordinator");
        if let [row] = found.coordinators.as_slice()
            && row.error_code == 0
            && let Ok(node_id) = u64::try_from(row.node_id)
            && Some(node_id) != excluded
        {
            return node_id;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: a client has no awaiter for the creation or the
        // move; it retries, as Kafka's clients do.
        time::sleep(Duration::from_millis(100)).await;
    }
}

fn heartbeat(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: GROUP.into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec!["orders".into()]),
        ..Default::default()
    }
}

/// Send `request` until the coordinator has loaded the group's partition.
///
/// `FindCoordinator` names a broker once it leads the partition, which can be
/// before it has replayed it, and the coordinator answers
/// `COORDINATOR_LOAD_IN_PROGRESS` until then.
async fn heartbeat_once_loaded(
    client: &Client,
    request: ShareGroupHeartbeatRequest,
) -> ShareGroupHeartbeatResponse {
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client
            .send(request.clone())
            .await
            .expect("ShareGroupHeartbeat");
        if response.error_code != COORDINATOR_LOAD_IN_PROGRESS || Instant::now() >= deadline {
            return response;
        }
        // intentional: the client retries a coordinator that is loading.
        time::sleep(Duration::from_millis(100)).await;
    }
}

fn position_of(cluster: &Cluster, node_id: u64) -> usize {
    for (idx, (handle, _, _)) in cluster.iter().enumerate() {
        if handle.node_id() == node_id {
            return idx;
        }
    }
    panic!("a cluster member");
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::mem::take,
};

#[derive(Default)]
struct ShareCoordinatorMove {
    cluster: Cluster,
    coordinator: u64,
    retired_directory: Option<TempDir>,
    joined: Option<ShareGroupHeartbeatResponse>,
    resumed: Option<ShareGroupHeartbeatResponse>,
}

#[fixture]
fn share_coordinator_move() -> ShareCoordinatorMove {
    // Retain the acknowledged member epoch while ownership of the offsets partition changes.
    ShareCoordinatorMove::default()
}

#[given("three brokers replicate share-move's offsets partition without shrinking its ISR")]
async fn replicated_share_coordinator(share_coordinator_move: &mut ShareCoordinatorMove) {
    share_coordinator_move.cluster = start_three().await;
    let lookup = client(&share_coordinator_move.cluster[0].0).await;
    share_coordinator_move.coordinator = coordinator_of(&lookup, None).await;
    lookup.close();
}

#[given("the share coordinator has installed the full three-replica offsets ISR")]
async fn installed_share_offsets_isr(share_coordinator_move: &ShareCoordinatorMove) {
    let broker = &share_coordinator_move.cluster[position_of(
        &share_coordinator_move.cluster,
        share_coordinator_move.coordinator,
    )]
    .0;
    broker
        .wait_until_isr_len(OFFSETS_TOPIC, 0, share_coordinator_move.cluster.len())
        .await;
    broker
        .wait_until_local_partition_leader(
            OFFSETS_TOPIC,
            0,
            NodeId(share_coordinator_move.coordinator),
        )
        .await;
}

#[when("an offsets follower crashes while remaining in the share coordinator's ISR")]
async fn stop_share_offsets_follower(share_coordinator_move: &mut ShareCoordinatorMove) {
    share_coordinator_move.retired_directory = Some(
        crash_follower(
            &mut share_coordinator_move.cluster,
            share_coordinator_move.coordinator,
        )
        .await,
    );
}

#[when("member-1 joins the share coordinator at epoch zero")]
async fn join_moving_share_group(share_coordinator_move: &mut ShareCoordinatorMove) {
    let member = client(
        &share_coordinator_move.cluster[position_of(
            &share_coordinator_move.cluster,
            share_coordinator_move.coordinator,
        )]
        .0,
    )
    .await;
    share_coordinator_move.joined =
        Some(heartbeat_once_loaded(&member, heartbeat("member-1", 0)).await);
    member.close();
}

#[then("the uncommitted join returns only COORDINATOR_NOT_AVAILABLE without an epoch")]
fn uncommitted_share_join(share_coordinator_move: &ShareCoordinatorMove) {
    assert!(
        share_coordinator_move
            .joined
            .as_ref()
            .expect("join answered")
            == &ShareGroupHeartbeatResponse {
                error_code: COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            }
    );
}

#[then("the share join succeeds with a positive member epoch")]
fn committed_share_join(share_coordinator_move: &ShareCoordinatorMove) {
    let joined = share_coordinator_move
        .joined
        .as_ref()
        .expect("join answered");
    assert!(joined.error_code == 0, "{joined:?}");
    assert!(joined.member_epoch > 0, "{joined:?}");
}

#[when("the share coordinator completes a controlled shutdown within thirty seconds")]
async fn drain_share_coordinator(share_coordinator_move: &mut ShareCoordinatorMove) {
    let position = position_of(
        &share_coordinator_move.cluster,
        share_coordinator_move.coordinator,
    );
    let (broker, _, directory) = share_coordinator_move.cluster.remove(position);
    share_coordinator_move.retired_directory = Some(directory);
    broker
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("controlled shutdown drains");
}

#[when("member-1 resumes at its acknowledged epoch through the replacement coordinator")]
async fn resume_moving_share_group(share_coordinator_move: &mut ShareCoordinatorMove) {
    let survivor = client(&share_coordinator_move.cluster[0].0).await;
    let next = coordinator_of(&survivor, Some(share_coordinator_move.coordinator)).await;
    survivor.close();
    let member = client(
        &share_coordinator_move.cluster[position_of(&share_coordinator_move.cluster, next)].0,
    )
    .await;
    share_coordinator_move.resumed = Some(
        heartbeat_once_loaded(
            &member,
            heartbeat(
                "member-1",
                share_coordinator_move
                    .joined
                    .as_ref()
                    .expect("acknowledged join")
                    .member_epoch,
            ),
        )
        .await,
    );
    member.close();
}

#[then("the recovered member succeeds with the same identity and an epoch at least as new")]
fn resumed_share_join(share_coordinator_move: &ShareCoordinatorMove) {
    let response = share_coordinator_move
        .resumed
        .as_ref()
        .expect("resumed heartbeat answered");
    check!(response.error_code == 0, "{response:?}");
    check!(response.member_id.as_deref() == Some("member-1"));
    check!(
        response.member_epoch
            >= share_coordinator_move
                .joined
                .as_ref()
                .expect("acknowledged join")
                .member_epoch
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_group_coordinator_move.feature",
    name = "a join that does not commit is not answered"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_that_does_not_commit_is_not_answered(share_coordinator_move: ShareCoordinatorMove) {
    shutdown_cluster(take(&mut share_coordinator_move.cluster)).await;
    drop(share_coordinator_move);
}

#[scenario(
    path = "tests/features/migrated/tests/share_group_coordinator_move.feature",
    name = "a member keeps its epoch when its coordinator stops cleanly"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_keeps_its_epoch_when_its_coordinator_stops_cleanly(
    share_coordinator_move: ShareCoordinatorMove,
) {
    shutdown_cluster(take(&mut share_coordinator_move.cluster)).await;
    drop(share_coordinator_move);
}
