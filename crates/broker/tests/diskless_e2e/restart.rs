//! A broker crashes mid-flush and comes back.
//!
//! A diskless flush is a multi-step commit: build the object from the
//! committed tails, PUT it, publish the index record naming it, wait for that
//! record to be projected, then trim the local logs behind the frontier. A
//! crash can land between any two of those steps, and each gap leaves a
//! different half-state -- an object nothing points at, an index record whose
//! local prefix is still present, a trim that only got half way through the
//! partitions.
//!
//! The invariant that has to hold across all of them is the only one that
//! matters to a producer: an offset that was acknowledged is still readable.
//! The case acknowledges a known prefix, then keeps a second producer writing
//! and waits until a whole flush cycle has run against that load, so the
//! crash lands on a cycling pipeline rather than on an idle broker between two
//! ticks. It then kills the broker without a controlled shutdown, brings it
//! back on the same addresses, and requires the whole acknowledged prefix
//! back.
//!
//! It does not pin the crash to a chosen step of the cycle -- see the comment
//! on the gate below for why that needs a fault hook the suite does not add.
//! The invariant is asserted because it must hold at every step.
//!
//! Only that prefix is asserted on. How many of the churn records were
//! acknowledged is decided by exactly when the crash landed, so asserting on
//! them would be asserting on the timing rather than on the invariant.

use {
    crate::{
        RECORDS, TOPIC, VOTERS,
        cluster::{DisklessCluster, wait_for},
        topic::{flushing_workload, seeded_leader},
        wire::{FetchedLog, assert_matches_produced, fetch_log, produce_until_stopped},
    },
    assert2::assert,
    krabka_broker::NodeId,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    std::time::Duration,
    tokio_util::sync::CancellationToken,
};
#[derive(Default)]
struct CyclingFlushRecovery {
    cluster: Option<DisklessCluster>,
    admin: Option<Client>,
    producer: Option<Client>,
    topic: Option<WireUuid>,
    leader: Option<NodeId>,
    survivor: Option<NodeId>,
    after_crash: Option<FetchedLog>,
    after_restart: Option<FetchedLog>,
}
#[rstest::fixture]
fn cycling_flush_recovery() -> CyclingFlushRecovery {
    // A real object flusher, WAL quorum and crash recovery belong to one scenario.
    CyclingFlushRecovery::default()
}
#[rstest_bdd_macros::given(
    "a diskless quorum with 24 acknowledged records and a 50 millisecond flush cadence"
)]
async fn cycling_flush_prefix(cycling_flush_recovery: &mut CyclingFlushRecovery) {
    let (cluster, admin, topic, leader, _values, producer) =
        flushing_workload(krabka_units::millis(50)).await;
    cycling_flush_recovery.cluster = Some(cluster);
    cycling_flush_recovery.admin = Some(admin);
    cycling_flush_recovery.producer = Some(producer);
    cycling_flush_recovery.topic = Some(topic);
    cycling_flush_recovery.leader = Some(leader);
}
#[rstest_bdd_macros::when(
    "a whole flush cycle passes the prefix under live churn before the accepting broker crashes"
)]
async fn crash_cycling_flusher(cycling_flush_recovery: &mut CyclingFlushRecovery) {
    let cluster = cycling_flush_recovery.cluster.as_mut().expect("cluster");
    let topic_id = cycling_flush_recovery.topic.expect("topic");
    let leader = cycling_flush_recovery.leader.expect("accepting leader");
    let producer = cycling_flush_recovery
        .producer
        .take()
        .expect("churn client");
    let admin = cycling_flush_recovery.admin.take().expect("admin client");
    let (leader_broker, committed) = seeded_leader(cluster, leader);
    wait_for(
        "the diskless flusher to reach the committed prefix",
        Duration::from_secs(90),
        || async {
            leader_broker
                .diskless_flush_state_for_test(TOPIC, 0)
                .await
                .is_some_and(|(_, _, _, _, _, frontier)| frontier == Some(committed))
        },
    )
    .await;

    // Keep the write path busy, then wait until a flush cycle has demonstrably
    // run against that load before crashing anything.
    //
    // Spawning the producer and crashing immediately would prove little: the
    // wait above ends when the *known prefix's* flush has already completed,
    // so the broker would most likely be killed idle between two ticks, with
    // no churn appended and no further cycle begun. Waiting for the frontier
    // to pass the prefix means at least one whole flush cycle -- build, PUT,
    // publish, project, trim -- has run end to end against live churn, and
    // with a 50 ms interval and a producer that never stops, the next cycle is
    // never more than a tick away when the crash lands.
    //
    // This does not pin the crash to a chosen step of that cycle. Nothing
    // observable from outside the broker can: doing it deterministically needs
    // a fault hook inside the flusher, which is a change to production code
    // that this suite does not justify. What the case asserts is the
    // invariant that has to hold at every step, so it holds wherever the crash
    // actually fell.
    let stop = CancellationToken::new();
    let churn = tokio::spawn(produce_until_stopped(producer, topic_id, stop.clone()));
    wait_for(
        "a flush cycle to complete against live churn",
        Duration::from_secs(90),
        || async {
            leader_broker
                .diskless_flush_state_for_test(TOPIC, 0)
                .await
                .is_some_and(|(_, _, _, _, _, frontier)| {
                    frontier.is_some_and(|frontier| frontier > committed)
                })
        },
    )
    .await;

    // No controlled shutdown: no leadership handover and no final flush.
    cluster.crash(leader).await;
    stop.cancel();
    churn.await.expect("the churn producer task");
    drop(admin);

    // The surviving majority promotes one of the other voters, and it must
    // serve everything the dead broker acknowledged. Some of those offsets
    // come from the promoted broker's own WAL replica and some from the
    // objects the dead broker had already flushed.
    let survivor = cluster
        .node_ids()
        .into_iter()
        .find(|node| *node != leader)
        .expect("a surviving voter");
    let promoted = cluster.promoted_leader(survivor, leader).await;
    let after_crash = fetch_log(
        &cluster.bootstrap_for_node(promoted),
        topic_id,
        0,
        RECORDS,
        Duration::from_mins(2),
    )
    .await;

    cycling_flush_recovery.survivor = Some(survivor);
    cycling_flush_recovery.after_crash = Some(after_crash);
}
#[rstest_bdd_macros::then(
    "the surviving leader serves all {count} acknowledged offsets with the produced payloads"
)]
fn cycling_crash_prefix(cycling_flush_recovery: &CyclingFlushRecovery, count: usize) {
    assert_matches_produced(
        cycling_flush_recovery
            .after_crash
            .as_ref()
            .expect("post-crash fetch"),
        0,
        count,
    );
}
#[rstest_bdd_macros::when(
    "the crashed broker restarts on its original addresses and rejoins all three registrations"
)]
async fn restart_cycling_flusher(cycling_flush_recovery: &mut CyclingFlushRecovery) {
    let cluster = cycling_flush_recovery.cluster.as_mut().expect("cluster");
    let leader = cycling_flush_recovery.leader.expect("accepting leader");
    let survivor = cycling_flush_recovery.survivor.expect("survivor");
    let topic_id = cycling_flush_recovery.topic.expect("topic");
    cluster.restart(leader).await;
    cluster
        .handle_for_node(leader)
        .expect("the restarted broker is up")
        .wait_until_brokers_registered(VOTERS)
        .await;
    cluster
        .handle_for_node(leader)
        .expect("the restarted broker is up")
        .wait_until_partition_present(TOPIC, 0)
        .await;

    // Every acknowledged offset still reads back after the restart, from
    // whichever broker now leads the partition.
    let leader_now = cluster
        .handle_for_node(survivor)
        .expect("the survivor is up")
        .partition_leader_for_test(TOPIC, 0)
        .map(krabka_broker::NodeId)
        .expect("a partition leader");
    let after_restart = fetch_log(
        &cluster.bootstrap_for_node(leader_now),
        topic_id,
        0,
        RECORDS,
        Duration::from_mins(2),
    )
    .await;

    cycling_flush_recovery.after_restart = Some(after_restart);
}
#[rstest_bdd_macros::then(
    "the restarted cluster serves all {count} acknowledged offsets in exactly the post-crash batch bytes"
)]
fn cycling_restart_prefix(cycling_flush_recovery: &CyclingFlushRecovery, count: usize) {
    let log = cycling_flush_recovery
        .after_restart
        .as_ref()
        .expect("post-restart fetch");
    assert_matches_produced(log, 0, count);
    assert!(
        log.bytes
            == cycling_flush_recovery
                .after_crash
                .as_ref()
                .expect("post-crash fetch")
                .bytes
    );
}
#[rstest_bdd_macros::then("the cycling-flush recovery cluster is shut down")]
async fn finish_cycling_recovery(cycling_flush_recovery: &mut CyclingFlushRecovery) {
    cycling_flush_recovery
        .cluster
        .take()
        .expect("cluster")
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/diskless_e2e/restart.feature",
    name = "a broker crashed mid flush loses no acknowledged offset"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_crashed_mid_flush_loses_no_acknowledged_offset(
    cycling_flush_recovery: CyclingFlushRecovery,
) {
    let _ = cycling_flush_recovery;
}
