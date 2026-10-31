//! A group coordinator that moves keeps every offset it acknowledged, and
//! its clients see only retriable coordinator errors while it moves.
//!
//! Kafka's `CoordinatorRuntime` completes a group write, an `OffsetCommit`
//! among them, only once the high watermark of the group's
//! `__consumer_offsets` partition passes it. A write that a follower in the
//! ISR never fetches times out after `offsets.commit.timeout.ms`, and
//! `CoordinatorOperationExceptionHelper.handleOperationException` answers that
//! `REQUEST_TIMED_OUT` as `COORDINATOR_NOT_AVAILABLE`. A read that does not
//! ask for stable offsets is a `scheduleReadOperation`, served at the last
//! committed offset of the partition, so it answers the offset acknowledged
//! before the write.
//!
//! When the coordinator dies, a follower leads the partition, replays it, and
//! takes the groups over. A broker that does not lead the partition answers
//! `NOT_COORDINATOR`, and the new leader answers
//! `COORDINATOR_LOAD_IN_PROGRESS` until it has loaded the partition. A client
//! retries both, and `COORDINATOR_NOT_AVAILABLE` too. Once loaded, the new
//! coordinator holds every offset the old one acknowledged and none that timed
//! out. `GroupMetadataManager.onLoaded` schedules the session of every member
//! again, so a KIP-848 member's next heartbeat at its epoch, and a classic
//! member's heartbeat and commit at its generation, are accepted.
//!
//! The barrier is the one `txn_uncommitted_writes` builds: the coordinator's
//! data listener sits behind a relay, and a cut relay stops its followers from
//! fetching while they stay in the ISR.
#![cfg(test)]

use {
    crate::support::{
        KEY_TYPE_GROUP,
        classic::classic_join_sync,
        client::{connect_owned, raw_connection},
        consumer_groups::{consumer_heartbeat, joining_consumer},
        find_coordinator,
        offsets::{
            offset_commit_partition, offset_commit_topic, offset_fetch_group, offset_fetch_topic,
        },
        relayed_cluster::RelayedCluster,
        topics::AssignedTopicSetup,
        transaction_wire::{create_assigned_topic, partition_leader},
    },
    assert2::assert,
    krabka_broker::NodeId,
    krabka_client_core::{Client, Connection},
    krabka_protocol::{
        owned::{
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
            heartbeat_request::HeartbeatRequest,
            heartbeat_response::HeartbeatResponse,
            offset_commit_request::OffsetCommitRequest,
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_fetch_request::OffsetFetchRequest,
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
                OffsetFetchResponseTopics,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        collections::BTreeSet,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tokio::{task::JoinHandle, time},
};

mod support;

const TOPIC: &str = "group-failover";
const CONSUMER_GROUP: &str = "group-failover-consumer";
const CLASSIC_GROUP: &str = "group-failover-classic";
/// The KIP-848 member id, which the client chooses for itself.
const MEMBER: &str = "group-failover-member";
const OFFSETS_TOPIC: &str = "__consumer_offsets";

const NONE: i16 = 0;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
/// The errors a Kafka client retries while its group coordinator moves.
const RETRIABLE: [i16; 3] = [
    COORDINATOR_LOAD_IN_PROGRESS,
    COORDINATOR_NOT_AVAILABLE,
    NOT_COORDINATOR,
];

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

/// The member's rebalance timeout, in milliseconds.
const REBALANCE_TIMEOUT_MS: i32 = 30_000;

/// The offsets each group commits, per commit: acknowledged before the
/// barrier, timed out behind it, and committed again to the new coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Offsets {
    consumer: i64,
    classic: i64,
}

const ACKNOWLEDGED: Offsets = Offsets {
    consumer: 5,
    classic: 7,
};
const TIMED_OUT: Offsets = Offsets {
    consumer: 9,
    classic: 11,
};
const RECOMMITTED: Offsets = Offsets {
    consumer: 13,
    classic: 17,
};

/// One connection to the broker that binds `address`, past any relay, so a
/// request reaches that broker whatever the cluster's metadata advertises.
async fn connect(address: SocketAddr) -> Connection {
    raw_connection(address, "group-coordinator-failover").await
}

async fn client(address: SocketAddr) -> Client {
    connect_owned(address.to_string(), "group-coordinator-failover", "client").await
}

fn other_node(excluded: u64) -> u64 {
    (1..=3)
        .find(|node| *node != excluded)
        .expect("another broker")
}

/// The member a commit names, and the generation (classic) or member epoch
/// (KIP-848) it commits at.
#[derive(Debug, Clone)]
struct Committer {
    group: &'static str,
    member_id: String,
    generation_or_epoch: i32,
}

fn commit_request(committer: &Committer, topic_id: WireUuid, offset: i64) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: committer.group.into(),
        generation_id_or_member_epoch: committer.generation_or_epoch,
        member_id: committer.member_id.clone(),
        topics: vec![offset_commit_topic(TOPIC, topic_id, vec![
            offset_commit_partition(0, offset, None),
        ])],
        ..Default::default()
    }
}

/// The answer to a [`commit_request`]: the topic by its id, as `OffsetCommit`
/// version 10 names it, and one partition row with `error_code`.
fn commit_answer(topic_id: WireUuid, error_code: i16) -> OffsetCommitResponse {
    OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            topic_id,
            partitions: vec![OffsetCommitResponsePartition {
                partition_index: 0,
                error_code,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn commit(
    connection: &Connection,
    committer: &Committer,
    topic_id: WireUuid,
    offset: i64,
) -> OffsetCommitResponse {
    connection
        .send(commit_request(committer, topic_id, offset))
        .await
        .expect("OffsetCommit")
}

/// An `OffsetFetch` of partition 0 for both groups, as an admin client sends
/// it: no member, and no `require_stable`.
fn fetch_request(topic_id: WireUuid) -> OffsetFetchRequest {
    let group = |group_id| {
        offset_fetch_group(
            group_id,
            Some(vec![offset_fetch_topic(TOPIC, topic_id, vec![0])]),
        )
    };
    OffsetFetchRequest {
        groups: vec![group(CONSUMER_GROUP), group(CLASSIC_GROUP)],
        require_stable: false,
        ..Default::default()
    }
}

/// The answer to a [`fetch_request`] that reads `offsets`.
fn fetch_answer(topic_id: WireUuid, offsets: Offsets) -> OffsetFetchResponse {
    let group = |group_id: &str, offset| OffsetFetchResponseGroup {
        group_id: group_id.into(),
        topics: vec![OffsetFetchResponseTopics {
            topic_id,
            partitions: vec![OffsetFetchResponsePartitions {
                partition_index: 0,
                committed_offset: offset,
                metadata: Some(String::new()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    OffsetFetchResponse {
        groups: vec![
            group(CONSUMER_GROUP, offsets.consumer),
            group(CLASSIC_GROUP, offsets.classic),
        ],
        ..Default::default()
    }
}

/// The answer to a [`fetch_request`] that refuses both groups with
/// `error_code`.
fn fetch_refusal(error_code: i16) -> OffsetFetchResponse {
    let group = |group_id: &str| OffsetFetchResponseGroup {
        group_id: group_id.into(),
        error_code,
        ..Default::default()
    };
    OffsetFetchResponse {
        groups: vec![group(CONSUMER_GROUP), group(CLASSIC_GROUP)],
        ..Default::default()
    }
}

async fn fetch(connection: &Connection, topic_id: WireUuid) -> OffsetFetchResponse {
    connection
        .send(fetch_request(topic_id))
        .await
        .expect("OffsetFetch")
}

fn classic_heartbeat(classic: &Committer) -> HeartbeatRequest {
    HeartbeatRequest {
        group_id: classic.group.into(),
        generation_id: classic.generation_or_epoch,
        member_id: classic.member_id.clone(),
        ..Default::default()
    }
}

fn member_heartbeat(consumer: &Committer) -> ConsumerGroupHeartbeatRequest {
    consumer_heartbeat(
        consumer.group,
        consumer.member_id.clone(),
        consumer.generation_or_epoch,
    )
}

/// A KIP-848 member of [`CONSUMER_GROUP`], subscribed to [`TOPIC`], as its
/// coordinator answered its join.
struct JoinedMember {
    committer: Committer,
    heartbeat_interval_ms: i32,
}

async fn join_consumer_group(coordinator: &Connection) -> JoinedMember {
    let joined = coordinator
        .send(ConsumerGroupHeartbeatRequest {
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..joining_consumer(CONSUMER_GROUP, MEMBER, REBALANCE_TIMEOUT_MS)
        })
        .await
        .expect("ConsumerGroupHeartbeat");
    assert!(joined.error_code == NONE, "{joined:?}");
    assert!(joined.member_id.as_deref() == Some(MEMBER), "{joined:?}");
    assert!(joined.member_epoch > 0, "{joined:?}");
    JoinedMember {
        committer: Committer {
            group: CONSUMER_GROUP,
            member_id: MEMBER.into(),
            generation_or_epoch: joined.member_epoch,
        },
        heartbeat_interval_ms: joined.heartbeat_interval_ms,
    }
}

/// What a broker answered for one round of the requests the groups' clients
/// send while their coordinator moves.
struct Round {
    fetched: OffsetFetchResponse,
    member: ConsumerGroupHeartbeatResponse,
    classic: HeartbeatResponse,
}

impl Round {
    async fn send(
        connection: &Connection,
        topic_id: WireUuid,
        consumer: &Committer,
        classic: &Committer,
    ) -> Self {
        Self {
            fetched: fetch(connection, topic_id).await,
            member: connection
                .send(member_heartbeat(consumer))
                .await
                .expect("ConsumerGroupHeartbeat"),
            classic: connection
                .send(classic_heartbeat(classic))
                .await
                .expect("Heartbeat"),
        }
    }

    fn codes(&self) -> Vec<i16> {
        self.fetched
            .groups
            .iter()
            .map(|group| group.error_code)
            .chain([self.member.error_code, self.classic.error_code])
            .collect()
    }
}

/// The broker that took the groups over, its first full answer, and every
/// error the survivors answered before it.
struct Moved {
    coordinator: u64,
    round: Round,
    errors: BTreeSet<i16>,
}

/// Send each survivor a round of the groups' requests until one answers the
/// whole round without an error, as the groups' clients retry.
///
/// # Panics
///
/// Panics on an error a Kafka client would not retry, such as
/// `UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION` or `FENCED_MEMBER_EPOCH`, and if
/// no survivor takes the groups over in time.
async fn retry_until_moved(
    survivors: &[(u64, Connection)],
    topic_id: WireUuid,
    consumer: &Committer,
    classic: &Committer,
) -> Moved {
    let deadline = Instant::now() + SETTLE;
    let mut errors = BTreeSet::new();
    loop {
        for (node, connection) in survivors {
            let round = Round::send(connection, topic_id, consumer, classic).await;
            let codes = round.codes();
            assert!(
                codes
                    .iter()
                    .all(|code| *code == NONE || RETRIABLE.contains(code)),
                "broker {node} answered a non-retriable error during the move: {:?} {:?} {:?}",
                round.fetched,
                round.member,
                round.classic,
            );
            if codes.iter().all(|code| *code == NONE) {
                return Moved {
                    coordinator: *node,
                    round,
                    errors,
                };
            }
            errors.extend(codes.into_iter().filter(|code| *code != NONE));
        }
        assert!(
            Instant::now() < deadline,
            "no survivor took the groups over within {SETTLE:?}; errors so far {errors:?}"
        );
        // intentional: a client has no awaiter for the move; it retries
        // after a backoff, as Kafka's clients do.
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Default)]
struct GroupCoordinatorFailover {
    cluster: Option<RelayedCluster>,
    coordinator: u64,
    topic_id: WireUuid,
    connection: Option<Connection>,
    consumer: Option<Committer>,
    classic: Option<Committer>,
    heartbeat_interval_ms: i32,
    acknowledged_end: i64,
    acknowledged_commits: Vec<OffsetCommitResponse>,
    acknowledged_watermarks: Vec<Option<i64>>,
    pending_commits: Vec<JoinHandle<OffsetCommitResponse>>,
    pending_read: Option<OffsetFetchResponse>,
    timeout_responses: Vec<OffsetCommitResponse>,
    timeout_read: Option<OffsetFetchResponse>,
    timeout_watermarks: Vec<Option<i64>>,
    survivors: Vec<(u64, Connection)>,
    moved: Option<Moved>,
    new_partition_leader: Option<u64>,
    elsewhere: Option<Round>,
    recommitted: Vec<OffsetCommitResponse>,
    recommitted_read: Option<OffsetFetchResponse>,
}
#[rstest::fixture]
fn group_failover() -> GroupCoordinatorFailover {
    let cluster = None;
    GroupCoordinatorFailover {
        cluster,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a three-replica offsets partition coordinates a joined KIP-848 group and a classic group, while their data topic is on another broker and ISR shrink is disabled for one hour"
)]
async fn replicated_group_coordinator(group_failover: &mut GroupCoordinatorFailover) {
    let cluster = RelayedCluster::start(|config| {
        config.offsets_topic_num_partitions = 1;
        // The coordinator keeps its followers in the ISR while they cannot
        // reach it, so the high watermark stops below what it appends then.
        config.isr_scan_interval = krabka_units::hours(1);
    })
    .await;
    for node in 1..=3 {
        cluster
            .handle(node)
            .wait_until_group_coordinator_ready()
            .await;
    }
    let admin = client(cluster.address(1)).await;
    let coordinator = u64::try_from(
        find_coordinator(&admin, KEY_TYPE_GROUP, CONSUMER_GROUP)
            .await
            .node_id,
    )
    .expect("a node id");
    assert!(
        u64::try_from(
            find_coordinator(&admin, KEY_TYPE_GROUP, CLASSIC_GROUP)
                .await
                .node_id
        ) == Ok(coordinator)
    );
    cluster
        .handle(coordinator)
        .wait_until_isr_len(OFFSETS_TOPIC, 0, 3)
        .await;
    // The data partition lives on another broker, so it survives the
    // coordinator.
    let data_node = other_node(coordinator);
    let topic_id = create_assigned_topic()
        .client(&admin)
        .topic(AssignedTopicSetup::on_replicas(TOPIC, vec![
            krabka_metadata::NodeId(data_node),
        ]))
        .call()
        .await;
    admin.close();
    for node in 1..=3 {
        cluster
            .handle(node)
            .wait_until_partition_present(TOPIC, 0)
            .await;
    }

    // Both groups form and commit, and both commits are acknowledged.
    let to_coordinator = connect(cluster.address(coordinator)).await;
    let JoinedMember {
        committer: consumer,
        heartbeat_interval_ms,
    } = join_consumer_group(&to_coordinator).await;
    let classic = {
        let classic_client = client(cluster.address(coordinator)).await;
        let (member_id, generation) = classic_join_sync(&classic_client, CLASSIC_GROUP).await;
        classic_client.close();
        Committer {
            group: CLASSIC_GROUP,
            member_id,
            generation_or_epoch: generation,
        }
    };
    group_failover.cluster = Some(cluster);
    group_failover.coordinator = coordinator;
    group_failover.topic_id = topic_id;
    group_failover.connection = Some(to_coordinator);
    group_failover.consumer = Some(consumer);
    group_failover.classic = Some(classic);
    group_failover.heartbeat_interval_ms = heartbeat_interval_ms;
}
#[rstest_bdd_macros::when(
    "the KIP-848 member commits offset 5 and the classic member commits offset 7 before the replication barrier"
)]
async fn acknowledged_group_commits(group_failover: &mut GroupCoordinatorFailover) {
    let connection = group_failover
        .connection
        .as_ref()
        .expect("coordinator connection");
    let consumer = group_failover.consumer.as_ref().expect("KIP-848 member");
    let classic = group_failover.classic.as_ref().expect("classic member");
    for (committer, offset) in [
        (consumer, ACKNOWLEDGED.consumer),
        (classic, ACKNOWLEDGED.classic),
    ] {
        group_failover
            .acknowledged_commits
            .push(commit(connection, committer, group_failover.topic_id, offset).await);
    }
    let cluster = group_failover.cluster.as_ref().expect("relayed cluster");
    group_failover.acknowledged_end = cluster
        .handle(group_failover.coordinator)
        .local_log_end_offset(OFFSETS_TOPIC, 0)
        .expect("offsets log end");
    group_failover.acknowledged_watermarks.push(
        cluster
            .handle(group_failover.coordinator)
            .high_watermark_for_test(OFFSETS_TOPIC, 0)
            .await,
    );
}
#[rstest_bdd_macros::then(
    "both complete commit responses succeed and the offsets high watermark equals the acknowledged log end"
)]
fn replicated_acknowledged_group_commits(group_failover: &GroupCoordinatorFailover) {
    assert!(
        group_failover.acknowledged_commits
            == vec![
                commit_answer(group_failover.topic_id, NONE),
                commit_answer(group_failover.topic_id, NONE)
            ]
    );
    assert!(
        group_failover.acknowledged_watermarks.as_slice()
            == [Some(group_failover.acknowledged_end)]
    );
}
#[rstest_bdd_macros::when(
    "the coordinator's replication relay is cut and both members submit newer offsets 9 and 11 while a read is sent before either commit completes"
)]
async fn barriered_group_commits(group_failover: &mut GroupCoordinatorFailover) {
    let cluster = group_failover.cluster.as_ref().expect("relayed cluster");
    cluster.relay(group_failover.coordinator).cut();
    let address = cluster.address(group_failover.coordinator);
    let topic_id = group_failover.topic_id;
    for (committer, offset) in [
        (
            group_failover
                .consumer
                .as_ref()
                .expect("KIP-848 member")
                .clone(),
            TIMED_OUT.consumer,
        ),
        (
            group_failover
                .classic
                .as_ref()
                .expect("classic member")
                .clone(),
            TIMED_OUT.classic,
        ),
    ] {
        group_failover
            .pending_commits
            .push(tokio::spawn(async move {
                let connection = connect(address).await;
                commit(&connection, &committer, topic_id, offset).await
            }));
    }
    cluster
        .handle(group_failover.coordinator)
        .wait_until_local_log_end_offset(OFFSETS_TOPIC, 0, group_failover.acknowledged_end + 2)
        .await;
    group_failover.pending_read = Some(
        fetch(
            group_failover
                .connection
                .as_ref()
                .expect("coordinator connection"),
            topic_id,
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the read returns exactly acknowledged offsets 5 and 7 while both newer commit tasks are still pending"
)]
fn read_during_pending_group_commits(group_failover: &GroupCoordinatorFailover) {
    assert!(
        group_failover
            .pending_read
            .as_ref()
            .expect("read during pending writes")
            == &fetch_answer(group_failover.topic_id, ACKNOWLEDGED)
    );
    assert!(group_failover.pending_commits.len() == 2);
    assert!(
        !group_failover
            .pending_commits
            .iter()
            .any(JoinHandle::is_finished)
    );
}
#[rstest_bdd_macros::when(
    "both newer commits reach their write timeout and the client reads offsets again"
)]
async fn timed_out_group_commits(group_failover: &mut GroupCoordinatorFailover) {
    for pending in mem::take(&mut group_failover.pending_commits) {
        group_failover
            .timeout_responses
            .push(pending.await.expect("OffsetCommit task"));
    }
    group_failover.timeout_read = Some(
        fetch(
            group_failover
                .connection
                .as_ref()
                .expect("coordinator connection"),
            group_failover.topic_id,
        )
        .await,
    );
    group_failover.timeout_watermarks.push(
        group_failover
            .cluster
            .as_ref()
            .expect("relayed cluster")
            .handle(group_failover.coordinator)
            .high_watermark_for_test(OFFSETS_TOPIC, 0)
            .await,
    );
}
#[rstest_bdd_macros::then(
    "both complete timeout responses are COORDINATOR_NOT_AVAILABLE 15, the read still returns 5 and 7 and the high watermark is unchanged"
)]
fn refused_unreplicated_group_commits(group_failover: &GroupCoordinatorFailover) {
    assert!(
        group_failover.timeout_responses
            == vec![
                commit_answer(group_failover.topic_id, COORDINATOR_NOT_AVAILABLE),
                commit_answer(group_failover.topic_id, COORDINATOR_NOT_AVAILABLE)
            ]
    );
    assert!(
        group_failover
            .timeout_read
            .as_ref()
            .expect("read after timeouts")
            == &fetch_answer(group_failover.topic_id, ACKNOWLEDGED)
    );
    assert!(
        group_failover.timeout_watermarks.as_slice() == [Some(group_failover.acknowledged_end)]
    );
}
#[rstest_bdd_macros::when(
    "the old coordinator crashes and the clients retry both survivors until one serves both existing members"
)]
async fn move_group_coordinator(group_failover: &mut GroupCoordinatorFailover) {
    group_failover
        .connection
        .take()
        .expect("old coordinator connection")
        .close();
    let cluster = group_failover.cluster.as_mut().expect("relayed cluster");
    cluster
        .take(group_failover.coordinator)
        .crash_for_test()
        .await;
    for node in (1..=3).filter(|node| *node != group_failover.coordinator) {
        group_failover
            .survivors
            .push((node, connect(cluster.address(node)).await));
    }
    let moved = retry_until_moved(
        &group_failover.survivors,
        group_failover.topic_id,
        group_failover.consumer.as_ref().expect("KIP-848 member"),
        group_failover.classic.as_ref().expect("classic member"),
    )
    .await;
    group_failover.new_partition_leader =
        Some(partition_leader(cluster.handle(moved.coordinator), OFFSETS_TOPIC, SETTLE).await);
    group_failover.moved = Some(moved);
}
#[rstest_bdd_macros::then(
    "the new offsets leader preserves offsets 5 and 7, the KIP-848 epoch and classic generation, and every error during the move is 14, 15 or 16"
)]
fn replayed_group_coordinator_state(group_failover: &GroupCoordinatorFailover) {
    let moved = group_failover.moved.as_ref().expect("new coordinator");
    assert!(moved.errors.iter().all(|code| RETRIABLE.contains(code)));
    assert!(group_failover.new_partition_leader == Some(moved.coordinator));
    assert!(moved.round.fetched == fetch_answer(group_failover.topic_id, ACKNOWLEDGED));
    assert!(
        moved.round.member
            == ConsumerGroupHeartbeatResponse {
                member_id: Some(MEMBER.into()),
                member_epoch: group_failover
                    .consumer
                    .as_ref()
                    .expect("KIP-848 member")
                    .generation_or_epoch,
                heartbeat_interval_ms: group_failover.heartbeat_interval_ms,
                ..Default::default()
            }
    );
    assert!(moved.round.classic == HeartbeatResponse::default());
}
#[rstest_bdd_macros::when(
    "the clients send offset fetch and both member heartbeats to the other surviving broker"
)]
async fn query_noncoordinator_survivor(group_failover: &mut GroupCoordinatorFailover) {
    let next = group_failover
        .moved
        .as_ref()
        .expect("new coordinator")
        .coordinator;
    let (other, connection) = group_failover
        .survivors
        .iter()
        .find(|(node, _)| *node != next)
        .expect("other survivor");
    group_failover
        .cluster
        .as_ref()
        .expect("relayed cluster")
        .handle(*other)
        .wait_until_partition_leader_changed(OFFSETS_TOPIC, 0, NodeId(group_failover.coordinator))
        .await;
    group_failover.elsewhere = Some(
        Round::send(
            connection,
            group_failover.topic_id,
            group_failover.consumer.as_ref().expect("KIP-848 member"),
            group_failover.classic.as_ref().expect("classic member"),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "all three complete responses from the other survivor are NOT_COORDINATOR 16"
)]
fn noncoordinator_group_responses(group_failover: &GroupCoordinatorFailover) {
    let elsewhere = group_failover
        .elsewhere
        .as_ref()
        .expect("noncoordinator responses");
    assert!(elsewhere.fetched == fetch_refusal(NOT_COORDINATOR));
    assert!(
        elsewhere.member
            == ConsumerGroupHeartbeatResponse {
                error_code: NOT_COORDINATOR,
                ..Default::default()
            }
    );
    assert!(
        elsewhere.classic
            == HeartbeatResponse {
                error_code: NOT_COORDINATOR,
                ..Default::default()
            }
    );
}
#[rstest_bdd_macros::when(
    "both original members commit offsets 13 and 17 to the new coordinator at their original epoch and generation"
)]
async fn recommit_after_group_failover(group_failover: &mut GroupCoordinatorFailover) {
    let next = group_failover
        .moved
        .as_ref()
        .expect("new coordinator")
        .coordinator;
    let (_, connection) = group_failover
        .survivors
        .iter()
        .find(|(node, _)| *node == next)
        .expect("new coordinator connection");
    for (committer, offset) in [
        (
            group_failover.consumer.as_ref().expect("KIP-848 member"),
            RECOMMITTED.consumer,
        ),
        (
            group_failover.classic.as_ref().expect("classic member"),
            RECOMMITTED.classic,
        ),
    ] {
        group_failover
            .recommitted
            .push(commit(connection, committer, group_failover.topic_id, offset).await);
    }
    group_failover.recommitted_read = Some(fetch(connection, group_failover.topic_id).await);
}
#[rstest_bdd_macros::then(
    "both complete recommit responses succeed and offset fetch returns exactly 13 and 17"
)]
async fn persisted_group_recommits(group_failover: &mut GroupCoordinatorFailover) {
    assert!(
        group_failover.recommitted
            == vec![
                commit_answer(group_failover.topic_id, NONE),
                commit_answer(group_failover.topic_id, NONE)
            ]
    );
    assert!(
        group_failover
            .recommitted_read
            .as_ref()
            .expect("recommitted offset read")
            == &fetch_answer(group_failover.topic_id, RECOMMITTED)
    );
    for (_, connection) in mem::take(&mut group_failover.survivors) {
        connection.close();
    }
    group_failover
        .cluster
        .take()
        .expect("relayed cluster")
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/group_coordinator_failover.feature",
    name = "Only acknowledged group offsets survive replication-barrier coordinator failover"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_offsets_survive_a_coordinator_failover_behind_a_replication_barrier(
    group_failover: GroupCoordinatorFailover,
) {
}

use std::mem;
