#![cfg(test)]

//! KIP-966 end-to-end: offset-aware **unclean recovery** elects the survivor
//! that is known to be complete, and only falls back to the most complete
//! surviving log when there is no such replica. Neither answer is "the first
//! alive replica".
//!
//! The tests use a 3-broker PLAINTEXT cluster, for the same reason as
//! `tests/elect_leaders.rs`. A 3-node raft quorum survives one dead node, and
//! the authorizer compat shim lets the wire path through without SASL.
//!
//! The cases share the same divergent-log precondition and differ
//! only in the eligible-leader-replica set the partition carries and the
//! leader that must win:
//!
//! 1. A 3-broker cluster with topic "t", 1 partition, and RF=3, so the
//!    replicas are `[1, 2, 3]`.
//! 2. Set `unclean.recovery.strategy=Aggressive` on the topic, so the UNCLEAN
//!    election routes through the offset-aware Unclean Recovery Manager (URM).
//! 3. Take the partition offline. The test injects a `PartitionRecord` whose
//!    leader and ISR is a dead phantom node, 99. Liveness reports 99 dead, so
//!    the partition has no live leader, and its ISR holds no live member. A
//!    demotion of the leader to a broker that does not exist also stops the
//!    real replication fetchers, which keeps the next step deterministic.
//! 4. Force the replicas' local logs to **diverge** deterministically with the
//!    `produce_records_for_test` accessor. It appends directly to each
//!    broker's hosted partition log, so the per-broker LEOs differ: broker 2
//!    gets the strictly highest LEO, broker 3 the middle one, and broker 1 the
//!    lowest. All three keep `current_leader_epoch == 0`, so only
//!    `log_end_offset` decides the most-complete-log ranking.
//! 5. Publish the partition's eligible-leader-replica set, when the case has
//!    one. It is written last so that no controller path re-derives it between
//!    the write and the election.
//! 6. Trigger recovery with `ElectLeaders(UNCLEAN)` sent to the raft leader.
//!    The URM polls brokers 1, 2, and 3 over the real `GetReplicaLogInfo` wire
//!    path and elects one of them.
//! 7. Assert the new partition leader, and that the ISR collapses to it.
//!
//! With no ELR (`unclean_recovery_elects_longest_log_replica`) the winner is
//! broker 2, the highest LEO, and not broker 1, the first alive replica. With
//! an ELR of `{3}` (`unclean_recovery_prefers_an_eligible_leader_replica`) the
//! winner is broker 3, whose log is *shorter* than broker 2's: an ELR member
//! held every committed record when it left the ISR, so electing it loses
//! nothing, and a longer non-ELR log is only a guess. Broker 3 is neither the
//! first alive replica nor the most complete log, so the assertion separates
//! the ELR rule from both of the others.
//!
//! These tests are gated to non-Windows, to match the multi-broker test
//! convention. The openraft `debug_assert!` races on the hosted Windows
//! scheduler are unrelated.

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_metadata::{
        BrokerConfigRecord, MetadataRecord, PartitionElrRecord, PartitionRecord, TopicConfigRecord,
    },
    std::{iter, net::SocketAddr, time::Duration},
    tokio::time,
};

mod kafka_wire;
mod support;

/// The controller-managed broker config that carries the witness role. It is
/// `crate::config_keys::BROKER_WITNESS`, which a test crate cannot name, and a
/// witness node publishes it for itself at registration.
const WITNESS_CONFIG_KEY: &str = "broker.witness";

// ─────────────────────────────────────────────────────────────────────────────
// Minimal wire helpers — bare TCP on PLAINTEXT, no SASL.
// Named election requests use the shared wire driver.
// ─────────────────────────────────────────────────────────────────────────────

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-unclean-test";

/// Creates a topic on a PLAINTEXT broker. The authorizer compat shim, with no
/// `super_users` and no ACLs, lets the request through.
///
/// The topic has one partition on `replicas`, in that order, so the tests
/// know which broker leads it: an automatic placement starts at a random
/// broker.
use crate::kafka_wire::create_assigned_topic_plaintext as create_topic_plaintext;
/// Drives `ElectLeaders` over a fresh PLAINTEXT connection. It asserts that
/// the top-level `error_code == 0`, and returns the per-partition
/// `(partition_id, error_code)` rows for `topic`.
use crate::kafka_wire::elect_leaders_plaintext as drive_elect_leaders;

// ─────────────────────────────────────────────────────────────────────────────
// Polling helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Waits until `handle` sees `(topic, partition)` in its metadata image.
async fn wait_partition_hosted(handle: &BrokerHandle, topic: &str, partition: i32) {
    // Event-driven: `has_partition` reads the committed metadata image, which is
    // exactly the source `wait_until_partition_present` subscribes to.
    handle.wait_until_partition_present(topic, partition).await;
}

use crate::support::{
    admin::{ElectionKind, ElectionSetup},
    partitions::{wait_partition_isr_only, wait_partition_leader},
    registered_cluster, shutdown_cluster,
    topics::AssignedTopicSetup,
};

/// The recovery strategy that routes an UNCLEAN election through the URM.
fn topic_config(topic: &str) -> MetadataRecord {
    MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_owned(),
        overrides: iter::once((
            "unclean.recovery.strategy".to_owned(),
            "Aggressive".to_owned(),
        ))
        .collect(),
    })
}

/// Leaves partition 0 of `topic` offline on an RF=3 cluster, with the three
/// surviving replicas holding logs of three different lengths.
///
/// `handles` are brokers 1, 2 and 3 in that order, and `addr` is any broker's
/// listener. On return the partition's leader is a node that never existed, so
/// nothing will elect a leader on its own, and broker 2 holds the strictly
/// highest LEO with broker 3 in the middle and broker 1 lowest.
async fn offline_partition_with_diverged_logs(
    addr: SocketAddr,
    topic: &str,
    handles: [&BrokerHandle; 3],
) {
    let [h1, h2, h3] = handles;
    // ── Create the RF=3 topic. Partition 0's replica assignment is
    //    [1, 2, 3]; broker 1 is the preferred/first. ──
    create_topic_plaintext()
        .addr(addr)
        .client_id(CLIENT_ID)
        .topic(AssignedTopicSetup::three_replicas(topic))
        .call()
        .await;
    wait_partition_hosted(h1, topic, 0).await;
    wait_partition_hosted(h2, topic, 0).await;
    wait_partition_hosted(h3, topic, 0).await;

    // Event-driven: await the partition's presence in the image, then read the
    // record the loop captured (h1 already saw it via `wait_partition_hosted`).
    h1.wait_until_partition_present(topic, 0).await;
    let pr_before = h1
        .partition_record_for_test(topic, 0)
        .expect("partition record present after wait_until_partition_present");
    eprintln!("partition before divergence: {pr_before:?}");
    assert!(
        pr_before.replicas
            == vec![
                krabka_broker::NodeId(1),
                krabka_broker::NodeId(2),
                krabka_broker::NodeId(3)
            ],
        "expected RF=3 replicas [1,2,3]; got {:?}",
        pr_before.replicas
    );

    // ── Set unclean.recovery.strategy=Aggressive so UNCLEAN routes through
    //    the offset-aware Unclean Recovery Manager. ──
    h1.submit_metadata_record_for_test(topic_config(topic))
        .await
        .expect("set unclean.recovery.strategy=Aggressive");

    // ── Take the partition offline FIRST: inject a PartitionRecord whose
    //    leader and ISR are a dead phantom node (99, never heartbeated →
    //    liveness=dead). Replicas stay [1, 2, 3] so the URM polls the three
    //    real survivors. Crucially, demoting the leader to a non-existent
    //    broker stops the real replication fetchers (no broker is leader),
    //    so the direct-append divergence we set up next stays deterministic
    //    — otherwise the leader's replicator races with our test appends. ──
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_owned(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: vec![
            krabka_broker::NodeId(1),
            krabka_broker::NodeId(2),
            krabka_broker::NodeId(3),
        ],
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    h1.submit_metadata_record_for_test(forged)
        .await
        .expect("inject dead-leader PartitionRecord");
    wait_partition_isr_only(h1, topic, 0, &[99]).await;
    // `wait_partition_hosted` reads the metadata image. Each broker creates its
    // local partition, and installs the dead leader, on its own reconcile
    // after that, and the direct appends below need all three partitions.
    for handle in [h1, h2, h3] {
        handle
            .wait_until_local_partition_target(
                topic,
                0,
                krabka_broker::NodeId(99),
                pr_before.leader_epoch.next(),
            )
            .await;
    }
    // Give the supervisors a beat to observe the leader change and tear down
    // any in-flight replication fetchers before we diverge the logs.
    // intentional: fetcher teardown is a background reconcile action with no
    // metadata-image or metric signal to await; the barrier keeps the direct
    // appends deterministic.
    time::sleep(Duration::from_millis(500)).await;

    // ── Force the surviving replicas' local logs to DIVERGE deterministically.
    //    `produce_records_for_test` appends directly to each broker's hosted
    //    partition log, so we set known, distinct LEOs by adding a different
    //    count of records on top of whatever each already holds:
    //        broker 2 gets the most → strictly-highest LEO.
    //    All keep current_leader_epoch == 0, so selection is decided by LEO.
    //    Broker 2 is NOT the first-alive replica (that's broker 1), which is
    //    what makes the assertion meaningful. ──
    h1.produce_records_for_test(topic, 0, 2)
        .await
        .expect("produce to broker 1");
    h3.produce_records_for_test(topic, 0, 4)
        .await
        .expect("produce to broker 3");
    h2.produce_records_for_test(topic, 0, 20)
        .await
        .expect("produce to broker 2");
    let end1 = h1.local_log_end_offset(topic, 0).expect("leo b1");
    let end2 = h2.local_log_end_offset(topic, 0).expect("leo b2");
    let end3 = h3.local_log_end_offset(topic, 0).expect("leo b3");
    eprintln!("LEO end offsets: b1={end1} b2={end2} b3={end3}");
    assert!(
        end2 > end1 && end2 > end3,
        "broker 2 must hold the strictly-highest LEO (b1={end1} b2={end2} b3={end3})"
    );
}

use {
    krabka_broker::BrokerConfig,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::mem::take,
    tempfile::TempDir,
    tokio::sync::MutexGuard,
};

#[derive(Default)]
struct UncleanRecovery {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    guard: Option<MutexGuard<'static, ()>>,
    result: Option<Vec<(i32, i16)>>,
}

#[fixture]
fn unclean_recovery() -> UncleanRecovery {
    // Preserve the actual divergent replica logs while the controller ranks them.
    UncleanRecovery::default()
}

#[given(
    "an offline aggressive-recovery partition whose broker 2 log is longest and broker 3 log is shorter"
)]
async fn divergent_surviving_replicas(unclean_recovery: &mut UncleanRecovery) {
    unclean_recovery.guard = Some(support::cluster_lock().lock().await);
    unclean_recovery.cluster = registered_cluster(3).await;
    offline_partition_with_diverged_logs(unclean_recovery.cluster[0].1.listen_addr, "t", [
        &unclean_recovery.cluster[0].0,
        &unclean_recovery.cluster[1].0,
        &unclean_recovery.cluster[2].0,
    ])
    .await;
}

#[given("broker {node} is published as a witness before the eligible-replica set")]
async fn recovery_witness(unclean_recovery: &UncleanRecovery, node: u64) {
    let broker = &unclean_recovery.cluster[0].0;
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_broker::NodeId(node),
            config_name: WITNESS_CONFIG_KEY.to_owned(),
            config_value: Some("true".to_owned()),
        }))
        .await
        .expect("publish witness role");
    broker
        .wait_for_image(|image| {
            image
                .broker_config(krabka_broker::NodeId(node))
                .and_then(|config| config.get(WITNESS_CONFIG_KEY))
                .is_some_and(|value| value == "true")
        })
        .await;
}

#[given("the partition's eligible-leader-replica set contains broker {node}")]
async fn recovery_eligible_replica(unclean_recovery: &UncleanRecovery, node: u64) {
    let broker = &unclean_recovery.cluster[0].0;
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: "t".to_owned(),
            partition: 0,
            eligible_leader_replicas: vec![krabka_broker::NodeId(node)],
            last_known_elr: vec![],
        }))
        .await
        .expect("publish eligible leader replica");
    broker
        .wait_for_image(|image| {
            image
                .partition_elr("t", 0)
                .0
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>()
                == vec![node]
        })
        .await;
}

#[when("the active metadata controller receives an UNCLEAN election for t partition 0")]
async fn elect_recovery_replica(unclean_recovery: &mut UncleanRecovery) {
    let leader = unclean_recovery.cluster[0]
        .0
        .wait_until_controller_leader()
        .await
        .unwrap();
    let address = unclean_recovery
        .cluster
        .iter()
        .find(|(_, config, _)| config.node_id == leader)
        .expect("active controller")
        .1
        .listen_addr;
    unclean_recovery.result = Some(
        drive_elect_leaders()
            .addr(address)
            .client_id(CLIENT_ID)
            .election(ElectionSetup {
                topic: "t".to_owned(),
                kind: ElectionKind::Unclean,
                ..Default::default()
            })
            .call()
            .await,
    );
}

#[then("the unclean election succeeds for exactly partition zero")]
fn recovery_election_response(unclean_recovery: &UncleanRecovery) {
    assert!(unclean_recovery.result.as_ref().expect("election answered") == &vec![(0, 0)]);
}

#[then("broker {node} becomes the recovered partition's only ISR member and leader")]
async fn recovery_elected_replica(unclean_recovery: &UncleanRecovery, node: u64) {
    let broker = &unclean_recovery.cluster[0].0;
    wait_partition_leader(broker, "t", 0, node).await;
    assert!(
        broker
            .partition_leader_for_test("t", 0)
            .expect("leader present")
            == node
    );
    wait_partition_isr_only(broker, "t", 0, &[node]).await;
}

#[scenario(
    path = "tests/features/migrated/tests/unclean_recovery.feature",
    name = "unclean recovery elects longest log replica"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_elects_longest_log_replica(unclean_recovery: UncleanRecovery) {
    shutdown_cluster(take(&mut unclean_recovery.cluster)).await;
    drop(unclean_recovery);
}

#[scenario(
    path = "tests/features/migrated/tests/unclean_recovery.feature",
    name = "unclean recovery prefers an eligible leader replica"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_prefers_an_eligible_leader_replica(unclean_recovery: UncleanRecovery) {
    shutdown_cluster(take(&mut unclean_recovery.cluster)).await;
    drop(unclean_recovery);
}

#[scenario(
    path = "tests/features/migrated/tests/unclean_recovery.feature",
    name = "unclean recovery never elects a witness from the elr"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_recovery_never_elects_a_witness_from_the_elr(unclean_recovery: UncleanRecovery) {
    shutdown_cluster(take(&mut unclean_recovery.cluster)).await;
    drop(unclean_recovery);
}
