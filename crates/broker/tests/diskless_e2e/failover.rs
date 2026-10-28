//! An `acks=all` diskless append survives the loss of the broker that acked it.
//!
//! This is the subsystem's whole value proposition, and it is the one claim
//! that a single-broker or single-process test cannot make. The case asserts
//! three separate things, in this order, because each one is a different way
//! the claim could be false:
//!
//! 1. **A second voter's durable checkpoint advanced.** Bytes can sit in a
//!    file the voter would discard on recovery. The checkpoint is what makes
//!    a prefix survive a restart, so it has to have moved to cover the acked
//!    range and no further.
//! 2. **That voter holds the bytes.** Read out of the broker's own
//!    `__diskless_wal_quorum` tree, byte for byte against what the leader
//!    serves. A local fsync on the leader would satisfy the produce path and
//!    fail here.
//! 3. **The promoted broker serves the same offsets byte for byte.** Not "the
//!    same number of records" and not "the same values": the same batches --
//!    and served from the WAL replica, because both survivors' ordinary
//!    partition logs are emptied first. See the comment on that truncation for
//!    why it is needed and what it does not fully close.

use {
    crate::{
        TOPIC,
        cluster::{DisklessCluster, start_diskless_cluster, wait_for},
        topic::seed_workload,
        voter_dir::{DurableRange, durable_bytes, durable_range, voter_dir},
        wire::{FetchedLog, assert_matches_produced, fetch_log},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::NodeId,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::time::Duration,
};
#[derive(Default)]
struct DisklessFailover {
    cluster: Option<DisklessCluster>,
    admin: Option<Client>,
    producer: Option<Client>,
    topic: Option<WireUuid>,
    leader: Option<NodeId>,
    followers: Vec<NodeId>,
    ranges: Vec<DurableRange>,
    voter_bytes: Vec<Bytes>,
    leader_log: Option<FetchedLog>,
    canonical_ends: Vec<Option<i64>>,
    promoted: Option<NodeId>,
    promoted_log: Option<FetchedLog>,
}
#[rstest::fixture]
fn diskless_failover() -> DisklessFailover {
    // Independent WAL and canonical logs expose exactly which copy survives.
    DisklessFailover::default()
}
#[rstest_bdd_macros::given(
    "a three-voter diskless quorum with 24 acknowledged records and object flushing parked for one hour"
)]
async fn durable_failover_prefix(diskless_failover: &mut DisklessFailover) {
    let cluster = start_diskless_cluster(|config| {
        config.diskless_wal_flush_interval = krabka_units::hours(1);
    })
    .await;
    cluster.await_ready().await;
    let (admin, topic, leader, _values, producer) = seed_workload(&cluster).await;
    diskless_failover.followers = cluster
        .node_ids()
        .into_iter()
        .filter(|node| *node != leader)
        .collect();
    diskless_failover.cluster = Some(cluster);
    diskless_failover.admin = Some(admin);
    diskless_failover.producer = Some(producer);
    diskless_failover.topic = Some(topic);
    diskless_failover.leader = Some(leader);
}
#[rstest_bdd_macros::when(
    "every follower checkpoints the acknowledged range through offset {committed} within one minute"
)]
async fn observe_failover_wal(diskless_failover: &mut DisklessFailover, committed: i64) {
    let cluster = diskless_failover.cluster.as_ref().expect("cluster");
    let topic = uuid::Uuid::from_bytes(diskless_failover.topic.expect("topic").0);
    for node in &diskless_failover.followers {
        let dir = voter_dir(cluster.log_dir_for_node(*node), topic, 0, *node);
        wait_for(
            &format!("broker {} WAL checkpoint", node.0),
            Duration::from_mins(1),
            || async { durable_range(&dir).is_some_and(|range| range.end >= committed) },
        )
        .await;
        diskless_failover
            .ranges
            .push(durable_range(&dir).expect("checkpoint written"));
        diskless_failover
            .voter_bytes
            .push(durable_bytes(&dir, DurableRange {
                start: 0,
                end: committed,
            }));
    }
}
#[rstest_bdd_macros::then("every follower's durable WAL range is exactly {start} through {end}")]
fn durable_failover_ranges(diskless_failover: &DisklessFailover, start: i64, end: i64) {
    assert!(diskless_failover.ranges.len() == diskless_failover.followers.len());
    for range in &diskless_failover.ranges {
        assert!(*range == DurableRange { start, end });
    }
}
#[rstest_bdd_macros::when(
    "the accepting leader serves {count} acknowledged records within one minute"
)]
async fn fetch_accepting_leader(diskless_failover: &mut DisklessFailover, count: usize) {
    let cluster = diskless_failover.cluster.as_ref().expect("cluster");
    diskless_failover.leader_log = Some(
        fetch_log(
            &cluster.bootstrap_for_node(diskless_failover.leader.expect("leader")),
            diskless_failover.topic.expect("topic"),
            0,
            count,
            Duration::from_mins(1),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the leader serves the produced {count} payloads and each voter's WAL holds identical batch bytes"
)]
fn durable_failover_bytes(diskless_failover: &DisklessFailover, count: usize) {
    let leader = diskless_failover.leader_log.as_ref().expect("leader fetch");
    assert_matches_produced(leader, 0, count);
    assert!(diskless_failover.voter_bytes.len() == diskless_failover.followers.len());
    for bytes in &diskless_failover.voter_bytes {
        assert!(*bytes == leader.bytes);
    }
}
#[rstest_bdd_macros::when(
    "both surviving candidates' canonical partition logs are truncated to offset zero"
)]
async fn erase_failover_canonical_logs(diskless_failover: &mut DisklessFailover) {
    let cluster = diskless_failover.cluster.as_ref().expect("cluster");
    for node in &diskless_failover.followers {
        let broker = cluster.handle_for_node(*node).expect("survivor up");
        broker
            .test_truncate_local_log(TOPIC, 0, 0)
            .await
            .expect("truncate canonical log");
        diskless_failover
            .canonical_ends
            .push(broker.local_log_end_offset(TOPIC, 0));
    }
}
#[rstest_bdd_macros::then(
    "every candidate's canonical log end is zero before the accepting broker crashes"
)]
fn empty_failover_canonical_logs(diskless_failover: &DisklessFailover) {
    assert!(diskless_failover.canonical_ends.len() == diskless_failover.followers.len());
    for end in &diskless_failover.canonical_ends {
        assert!(*end == Some(0));
    }
}
#[rstest_bdd_macros::when(
    "the accepting broker crashes and the surviving majority promotes a new partition leader"
)]
async fn crash_accepting_leader(diskless_failover: &mut DisklessFailover) {
    drop(diskless_failover.producer.take());
    drop(diskless_failover.admin.take());
    let leader = diskless_failover.leader.expect("accepting leader");
    let cluster = diskless_failover.cluster.as_mut().expect("cluster");
    cluster.crash(leader).await;
    diskless_failover.promoted = Some(
        cluster
            .promoted_leader(diskless_failover.followers[0], leader)
            .await,
    );
}
#[rstest_bdd_macros::then("the promoted partition leader differs from the accepting broker")]
fn new_failover_leader(diskless_failover: &DisklessFailover) {
    assert!(
        diskless_failover.promoted.expect("promoted leader")
            != diskless_failover.leader.expect("accepting leader")
    );
}
#[rstest_bdd_macros::when("the promoted leader serves {count} records within two minutes")]
async fn fetch_promoted_leader(diskless_failover: &mut DisklessFailover, count: usize) {
    let cluster = diskless_failover.cluster.as_ref().expect("cluster");
    diskless_failover.promoted_log = Some(
        fetch_log(
            &cluster.bootstrap_for_node(diskless_failover.promoted.expect("promoted leader")),
            diskless_failover.topic.expect("topic"),
            0,
            count,
            Duration::from_mins(2),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the promoted leader serves all {count} produced records in the accepting leader's exact batch bytes"
)]
fn recovered_failover_bytes(diskless_failover: &DisklessFailover, count: usize) {
    let log = diskless_failover
        .promoted_log
        .as_ref()
        .expect("promoted fetch");
    assert_matches_produced(log, 0, count);
    assert!(
        log.bytes
            == diskless_failover
                .leader_log
                .as_ref()
                .expect("accepting leader fetch")
                .bytes
    );
}
#[rstest_bdd_macros::then("the diskless failover cluster is shut down")]
async fn finish_diskless_failover(diskless_failover: &mut DisklessFailover) {
    diskless_failover
        .cluster
        .take()
        .expect("cluster")
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/diskless_e2e/failover.feature",
    name = "diskless acks all append survives the acking broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diskless_acks_all_append_survives_the_acking_broker(diskless_failover: DisklessFailover) {
    let _ = diskless_failover;
}
