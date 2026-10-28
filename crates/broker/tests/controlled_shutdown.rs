#![cfg(test)]

//! `BrokerHandle::controlled_shutdown` integration test.
//!
//! The test uses a 3-broker PLAINTEXT cluster and an rf=3 topic. Broker 1 is
//! the preferred leader of every partition. `controlled_shutdown(broker 1)`
//! must:
//! 1. Move leadership of every partition off broker 1, and
//! 2. Return `Ok(())` after the controller acknowledges
//!    `should_shut_down=true`. Broker 1 then leads zero partitions.
//!
//! The test is gated to non-Windows to match the multi-broker convention established by the existing integration suites.`debug_assert!` races on the hosted Windows
//! task scheduler are unrelated to the protocol under test.

use {
    crate::support::{
        registered_cluster, shutdown_cluster,
        topics::{CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor},
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, config::NodeRole},
    krabka_metadata::{MetadataRecord, NodeId as MetadataNodeId, PartitionRecord},
    std::{net::SocketAddr, time::Duration},
    tokio::time,
};

mod kafka_wire;
mod support;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-controlled-shutdown-test";

async fn create_topic(addr: SocketAddr, setup: CreateTopicSetup<'_>) {
    kafka_wire::create_topic_from_setup(addr, CLIENT_ID, setup).await;
}

/// Waits until `(topic, partition)` appears in the metadata image of
/// `handle`.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

#[derive(krabka_macros::FieldDefaults)]
struct ForcedLeadershipSetup<'a> {
    #[default("drain-me")]
    topic: &'a str,
    #[default(TopicPartitionCount(4))]
    partitions: TopicPartitionCount,
    #[default(krabka_metadata::NodeId(1))]
    leader: krabka_metadata::NodeId,
    #[default(vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2), krabka_metadata::NodeId(3)])]
    replicas: Vec<MetadataNodeId>,
}

/// Injects a `PartitionRecord` that makes `target` the leader for every
/// partition of `topic`. It uses `submit_metadata_record_for_test` to bypass
/// the public wire path, so the test does not have to drive `ElectLeaders`.
/// It returns after the image shows `leader=target` for every partition.
#[bon::builder]
async fn force_leadership_for_test(
    leader_handle: &BrokerHandle,
    #[builder(default = "drain-me")] topic: &'_ str,
    #[builder(into, default = TopicPartitionCount(4))] partitions: TopicPartitionCount,
    #[builder(into, default = krabka_metadata :: NodeId (1))] leader: krabka_metadata::NodeId,
    #[builder(default = vec ! [krabka_metadata :: NodeId (1) , krabka_metadata :: NodeId (2) , krabka_metadata :: NodeId (3)])]
    replicas: Vec<MetadataNodeId>,
) {
    let setup = ForcedLeadershipSetup {
        topic,
        partitions,
        leader,
        replicas,
    };
    let topic = setup.topic;
    let partitions = setup.partitions.0;
    let target = setup.leader.0;
    let replicas = setup.replicas;

    let image = leader_handle.controller_image_for_test();
    for p in 0..partitions {
        let Some(pr) = image.partition(topic, p) else {
            continue;
        };
        let record = MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: p,
            leader: krabka_metadata::NodeId(target),
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: pr.leader_epoch.next(),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        });
        leader_handle
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit forced leadership");
    }

    // Wait until every partition reports `target` as leader.
    leader_handle
        .wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target)
            })
        })
        .await;
}

/// Returns the number of partitions in `topic` that `target` currently leads,
/// according to the image of `observer`.
fn leader_count(observer: &BrokerHandle, topic: &str, partitions: i32, target: u64) -> usize {
    let mut count = 0usize;
    for p in 0..partitions {
        if observer.partition_leader_for_test(topic, p) == Some(target) {
            count += 1;
        }
    }
    count
}

#[derive(Default)]
struct LeadershipDrainScenario {
    lock: Option<MutexGuard<'static, ()>>,
    cluster: Vec<(BrokerHandle, krabka_broker::BrokerConfig, tempfile::TempDir)>,
    target: Option<BrokerHandle>,
    target_directory: Option<tempfile::TempDir>,
    target_node: Option<MetadataNodeId>,
    result: Option<Result<(), krabka_broker::BrokerError>>,
}
#[rstest::fixture]
fn leadership_drain() -> LeadershipDrainScenario {
    let lock = None;
    LeadershipDrainScenario {
        lock,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a three-broker cluster has four drain-me partitions replicated on all brokers and led by a noncontroller broker"
)]
async fn concentrated_partition_leadership(leadership_drain: &mut LeadershipDrainScenario) {
    leadership_drain.lock = Some(support::cluster_lock().lock().await);

    support::init_tracing();

    let mut cluster = registered_cluster(3).await;

    // Resolve the raft (controller) leader. We then pick a *follower*
    // broker as the controlled-shutdown target. Picking the controller
    // leader would also exercise the path — its heartbeat to itself is
    // a local round-trip — but the bootstrap broker is also the sole
    // replica of `__consumer_offsets/0` (rf=1, replicas=[node_id]),
    // which cannot be drained. Targeting a follower keeps the test
    // focused on the controllable wire path.
    let raft_leader_idx = {
        let leader = cluster[0].0.wait_until_controller_leader().await.unwrap();
        cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("controller leader must be a cluster member")
    };
    let target_idx = (raft_leader_idx + 1) % cluster.len();
    let target_node_id = cluster[target_idx].1.node_id;
    eprintln!(
        "controlled shutdown target: broker_id={target_node_id} (cluster idx {target_idx}, \
         raft leader is idx {raft_leader_idx})"
    );

    // Create topic on the cluster leader's listen addr.
    let addr = cluster[raft_leader_idx].1.listen_addr;
    let topic = "drain-me";
    let partitions: i32 = 4;
    create_topic(addr, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(partitions),
        replication_factor: TopicReplicationFactor(3),
        ..Default::default()
    })
    .await;
    for p in 0..partitions {
        for (h, _, _) in &cluster {
            wait_partition_exists(h, topic, p).await;
        }
    }

    // Force the target to be leader of every partition. The natural
    // round-robin assignment with 3 brokers / 4 partitions would
    // leader-balance across the cluster — we want to prove draining is
    // exhaustive, so concentrate every leader onto the target first.
    let mut replicas: Vec<u64> = (1..=3).collect();
    // Put target first so its `replicas[0]` is itself (preferred).
    replicas.sort_by_key(|n| i32::from(*n != target_node_id));
    force_leadership_for_test()
        .leader_handle(&cluster[raft_leader_idx].0)
        .topic(topic)
        .partitions(partitions)
        .leader(target_node_id.0)
        .replicas(
            replicas
                .iter()
                .copied()
                .map(krabka_metadata::NodeId)
                .collect(),
        )
        .call()
        .await;

    // Sanity: target leads everything before we start. Use the raft
    // leader's image since the target is about to be drained.
    for (h, _, _) in &cluster {
        h.wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target_node_id)
            })
        })
        .await;
    }
    assert!(
        leader_count(
            &cluster[raft_leader_idx].0,
            topic,
            partitions,
            target_node_id.0
        ) == usize::try_from(partitions).unwrap(),
        "target should lead all partitions before shutdown"
    );

    let (target, _, directory) = cluster.remove(target_idx);
    leadership_drain.cluster = cluster;
    leadership_drain.target = Some(target);
    leadership_drain.target_directory = Some(directory);
    leadership_drain.target_node = Some(target_node_id);
}
#[rstest_bdd_macros::when(
    "the noncontroller broker requests controlled shutdown with a 30-second drain deadline"
)]
async fn drain_partition_leadership(leadership_drain: &mut LeadershipDrainScenario) {
    leadership_drain.result = Some(
        leadership_drain
            .target
            .take()
            .expect("target broker")
            .controlled_shutdown(Duration::from_secs(30))
            .await,
    );
}
#[rstest_bdd_macros::then(
    "shutdown succeeds and every surviving broker observes zero drain-me partitions led by the stopped broker"
)]
async fn drained_partition_leadership(leadership_drain: &mut LeadershipDrainScenario) {
    assert!(let Some(Ok(())) = leadership_drain.result.as_ref());
    let target = leadership_drain.target_node.expect("target broker ID");
    for (observer, _, _) in &leadership_drain.cluster {
        observer
            .wait_for_image(|image| {
                (0..4).all(|partition| {
                    image.partition("drain-me", partition).map(|row| row.leader) != Some(target)
                })
            })
            .await;
        assert!(leader_count(observer, "drain-me", 4, target.0) == 0);
    }
    shutdown_cluster(mem::take(&mut leadership_drain.cluster)).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/controlled_shutdown.feature",
    name = "Shutdown drains every partition from a broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_shutdown_drains_leadership_and_returns_ok(
    leadership_drain: LeadershipDrainScenario,
) {
}

#[derive(Default)]
struct ControllerOnlyShutdown {
    directory: Option<tempfile::TempDir>,
    controller: Option<BrokerHandle>,
    result: Option<Result<Result<(), krabka_broker::BrokerError>, time::error::Elapsed>>,
}
#[rstest::fixture]
fn controller_only_shutdown() -> ControllerOnlyShutdown {
    let directory = None;
    ControllerOnlyShutdown {
        directory,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "an elected controller-only node has no data partitions or broker heartbeat client"
)]
async fn isolated_controller_role(controller_only_shutdown: &mut ControllerOnlyShutdown) {
    let directory = tempfile::TempDir::new().expect("controller directory");
    let mut config = krabka_broker::BrokerConfig::for_tests(directory.path().to_path_buf());
    config.roles = vec![NodeRole::Controller];
    let controller = krabka_broker::Broker::start(config)
        .await
        .expect("controller-only start");
    controller
        .wait_until_controller_leader()
        .await
        .expect("elected controller");
    controller_only_shutdown.controller = Some(controller);
    controller_only_shutdown.directory = Some(directory);
}
#[rstest_bdd_macros::when(
    "the controller-only node requests controlled shutdown with a 10-second drain deadline"
)]
async fn stop_controller_only_role(controller_only_shutdown: &mut ControllerOnlyShutdown) {
    let controller = controller_only_shutdown
        .controller
        .take()
        .expect("controller-only node");
    controller_only_shutdown.result = Some(
        time::timeout(
            Duration::from_secs(20),
            controller.controlled_shutdown(Duration::from_secs(10)),
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "the controller-only shutdown succeeds within the 20-second observation deadline"
)]
fn controller_only_stopped(controller_only_shutdown: &ControllerOnlyShutdown) {
    assert!(let Some(Ok(Ok(()))) = controller_only_shutdown.result.as_ref());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/controlled_shutdown.feature",
    name = "A controller-only node shuts down without waiting for a broker heartbeat"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_shutdown_of_a_controller_only_node_stops_at_once(
    controller_only_shutdown: ControllerOnlyShutdown,
) {
}

use {std::mem, tokio::sync::MutexGuard};
