//! Reassignment lifecycle tests over PLAINTEXT: start a reassignment, observe
//! it in flight, complete it through an ISR catch-up, and cancel it with null
//! replicas.
//!
//! Each test boots its own 3-broker cluster, sends
//! `AlterPartitionReassignments` to the current controller leader, and then
//! reads the outcome back out of the metadata image.

use {
    crate::{
        kafka_wire::create_automatic_topic_plaintext,
        log_dir_wire::{alter_replica_log_dirs, describe_log_dirs},
        plaintext_cluster::{
            ThreeBrokerCluster, broker_id, controller_leader_addr, node_id,
            start_three_broker_plaintext_cluster,
            start_three_broker_plaintext_cluster_with_log_dirs, wait_partition_exists,
        },
        plaintext_wire::CLIENT_ID,
        reassign_rpc::{drive_alter_reassignments, drive_list_reassignments},
        support::{
            lazy_mutex,
            topics::{ConfiguredTopicSetup, TopicReplicationFactor},
        },
    },
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    krabka_metadata::{MetadataRecord, NodeId, PartitionRecord},
    krabka_protocol::owned::list_partition_reassignments_response::OngoingTopicReassignment,
    std::{
        collections::HashSet,
        fs,
        net::SocketAddr,
        sync::OnceLock,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{
        sync::{Mutex, MutexGuard},
        time,
    },
};

#[derive(Default)]
struct ReassignmentBehavior {
    guard: Option<MutexGuard<'static, ()>>,
    brokers: Vec<Option<BrokerHandle>>,
    directories: Vec<TempDir>,
    extra_directories: Vec<TempDir>,
    topic: String,
    original: Vec<NodeId>,
    staying: i32,
    removing: i32,
    new_replica: i32,
    controller: Option<SocketAddr>,
    altered: Vec<(String, Vec<(i32, i16)>)>,
    listed: Vec<OngoingTopicReassignment>,
    partition: Option<PartitionRecord>,
    preference_code: Option<i16>,
}

#[rstest::fixture]
fn reassignment() -> ReassignmentBehavior {
    // The replica target and original assignment remain explicit across each operation.
    ReassignmentBehavior::default()
}

impl ReassignmentBehavior {
    fn install(&mut self, cluster: ThreeBrokerCluster) {
        let ThreeBrokerCluster {
            h1,
            h2,
            h3,
            d1,
            d2,
            d3,
            ..
        } = cluster;
        self.brokers = vec![Some(h1), Some(h2), Some(h3)];
        self.directories = vec![d1, d2, d3];
    }

    fn broker(&self, index: usize) -> &BrokerHandle {
        self.brokers[index]
            .as_ref()
            .expect("live reassignment broker")
    }

    fn handles(&self) -> Vec<&BrokerHandle> { self.brokers.iter().flatten().collect() }

    fn observer(&self) -> &BrokerHandle {
        self.brokers
            .iter()
            .flatten()
            .find(|broker| {
                self.original
                    .iter()
                    .any(|replica| replica.0 == broker.node_id())
            })
            .expect("live original replica")
    }

    fn target_index(&self) -> usize {
        usize::try_from(self.new_replica - 1).expect("positive broker id")
    }

    fn target(&self) -> Vec<i32> { vec![self.staying, self.new_replica] }

    fn select_target(&mut self) {
        self.original = self
            .broker(0)
            .partition_record_for_test(&self.topic, 0)
            .expect("partition")
            .replicas;
        assert!(self.original.len() == 2);
        self.new_replica = (1..=3)
            .find(|id| !self.original.contains(&node_id(*id)))
            .expect("unassigned broker");
        self.staying = broker_id(self.original[0]);
        self.removing = broker_id(*self.original.last().expect("RF2 replicas"));
    }

    async fn shutdown(self) {
        let Self {
            guard,
            brokers,
            directories,
            extra_directories,
            ..
        } = self;
        for broker in brokers.into_iter().flatten() {
            broker.shutdown().await;
        }
        drop((directories, extra_directories));
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "three plaintext brokers host RF2 foo partition 0 and the replacement keeps the first replica while adding the unassigned broker"
)]
async fn foo_reassignment_cluster(reassignment: &mut ReassignmentBehavior) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    reassignment.guard = Some(lazy_mutex(&LOCK).lock().await);
    reassignment.topic = "foo".into();
    reassignment.install(foo_cluster().await);
    reassignment.select_target();
}

#[rstest_bdd_macros::given(
    "three plaintext brokers each have an extra log directory and RF2 preplaced partition 0 leaves one broker unassigned"
)]
async fn preplaced_reassignment_cluster(reassignment: &mut ReassignmentBehavior) {
    reassignment.topic = "preplaced".into();
    reassignment.extra_directories = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let paths = [0, 1, 2].map(|index| reassignment.extra_directories[index].path().to_path_buf());
    reassignment.install(start_three_broker_plaintext_cluster_with_log_dirs(&paths).await);
    create_automatic_topic_plaintext()
        .addr(reassignment.broker(0).listen_addr())
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: reassignment.topic.clone(),
            replicas: TopicReplicationFactor(2),
            ..Default::default()
        })
        .call()
        .await;
    wait_partition_exists(reassignment.broker(0), &reassignment.topic, 0).await;
    reassignment.select_target();
    wait_partition_exists(
        reassignment.broker(reassignment.target_index()),
        &reassignment.topic,
        0,
    )
    .await;
}

#[rstest_bdd_macros::when(
    "AlterReplicaLogDirs prefers the unassigned broker's extra directory for preplaced partition 0"
)]
async fn prefer_preplaced_replica_directory(reassignment: &mut ReassignmentBehavior) {
    let response = alter_replica_log_dirs(
        reassignment
            .broker(reassignment.target_index())
            .listen_addr(),
        reassignment.extra_directories[reassignment.target_index()].path(),
        &reassignment.topic,
        vec![0],
    )
    .await;
    reassignment.preference_code = Some(response.results[0].partitions[0].error_code);
}

#[rstest_bdd_macros::then(
    "the preference returns REPLICA_NOT_AVAILABLE code 9 before the replica exists"
)]
fn unmaterialized_preplaced_replica(reassignment: &ReassignmentBehavior) {
    assert!(reassignment.preference_code == Some(9));
}

#[rstest_bdd_macros::when(
    "AlterPartitionReassignments targets the staying and new replicas through the active controller"
)]
async fn start_replica_reassignment(reassignment: &mut ReassignmentBehavior) {
    let address = if let Some(address) = reassignment.controller {
        address
    } else {
        controller_leader_addr(&reassignment.handles()).await
    };
    reassignment.controller = Some(address);
    reassignment.altered = drive_alter_reassignments(address, vec![(
        &reassignment.topic,
        0,
        Some(reassignment.target()),
    )])
    .await;
}

#[rstest_bdd_macros::then("the partition reassignment response is exactly partition 0 error 0")]
fn accepted_replica_reassignment(reassignment: &ReassignmentBehavior) {
    assert!(
        reassignment.altered[0].1 == vec![(0, 0)],
        "expected error_code=0; got {:?}",
        reassignment.altered
    );
}

#[rstest_bdd_macros::then(
    "the assignment is in flight with the new broker adding and the original last broker removing"
)]
async fn in_flight_replica_reassignment(reassignment: &mut ReassignmentBehavior) {
    reassignment
        .observer()
        .wait_for_image(|image| {
            image
                .partition(&reassignment.topic, 0)
                .is_some_and(|partition| !partition.adding_replicas.is_empty())
        })
        .await;
    reassignment.partition = Some(
        reassignment
            .observer()
            .partition_record_for_test(&reassignment.topic, 0)
            .expect("partition"),
    );
    let partition = reassignment
        .partition
        .as_ref()
        .expect("in-flight partition");
    assert!(
        partition
            .adding_replicas
            .contains(&node_id(reassignment.new_replica)),
        "adding_replicas should contain new_replica; pr={partition:?}"
    );
    assert!(
        partition
            .removing_replicas
            .contains(&node_id(reassignment.removing)),
        "removing_replicas should contain removing; pr={partition:?}"
    );
}

#[rstest_bdd_macros::when("the ISR includes the staying, new and removing replicas")]
async fn catch_up_reassigned_replica(reassignment: &ReassignmentBehavior) {
    let injected = PartitionRecord {
        isr: vec![
            node_id(reassignment.staying),
            node_id(reassignment.new_replica),
            node_id(reassignment.removing),
        ],
        ..reassignment
            .partition
            .as_ref()
            .expect("in-flight partition")
            .clone()
    };
    reassignment
        .observer()
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(injected))
        .await
        .expect("inject");
}

#[rstest_bdd_macros::then(
    "completion clears adding and removing replicas and leaves exactly the target replica set"
)]
async fn completed_replica_reassignment(reassignment: &ReassignmentBehavior) {
    reassignment
        .observer()
        .wait_for_image(|image| {
            image
                .partition(&reassignment.topic, 0)
                .is_some_and(|partition| {
                    partition.adding_replicas.is_empty() && partition.removing_replicas.is_empty()
                })
        })
        .await;
    let partition = reassignment
        .observer()
        .partition_record_for_test(&reassignment.topic, 0)
        .expect("partition");
    let actual: HashSet<u64> = partition.replicas.iter().map(|node| node.0).collect();
    let expected: HashSet<u64> = reassignment
        .target()
        .into_iter()
        .map(|node| node_id(node).0)
        .collect();
    assert!(
        actual == expected,
        "replicas after completion should match target; pr={partition:?}"
    );
}

#[rstest_bdd_macros::when("ListPartitionReassignments requests every in-flight partition")]
async fn list_replica_reassignment(reassignment: &mut ReassignmentBehavior) {
    reassignment.listed =
        drive_list_reassignments(reassignment.controller.expect("active controller"), None).await;
}

#[rstest_bdd_macros::then(
    "foo has one pending row for partition 0 whose adding replicas contain exactly the new broker"
)]
fn listed_replica_reassignment(reassignment: &ReassignmentBehavior) {
    let topic = reassignment
        .listed
        .iter()
        .find(|topic| topic.name == "foo")
        .expect("foo should appear in list");
    assert!(
        topic.partitions.len() == 1,
        "expected 1 partition in-flight; got {:?}",
        topic.partitions
    );
    check!(
        topic.partitions[0].partition_index == 0,
        "expected partition_index=0"
    );
    check!(
        topic.partitions[0].adding_replicas == vec![reassignment.new_replica],
        "expected adding_replicas=[new_replica]; got {:?}",
        topic.partitions[0].adding_replicas
    );
}

#[rstest_bdd_macros::when(
    "the registered new replica shuts down before joining ISR and a surviving controller is elected"
)]
async fn stop_reassignment_target(reassignment: &mut ReassignmentBehavior) {
    let index = reassignment.target_index();
    reassignment.brokers[index]
        .take()
        .expect("new replica handle")
        .shutdown()
        .await;
    let mut leader_rx = reassignment.observer().watch_leader_for_test();
    let leader = time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(leader) = *leader_rx.borrow_and_update()
                && reassignment
                    .brokers
                    .iter()
                    .flatten()
                    .any(|broker| broker.node_id() == leader.0)
            {
                return leader;
            }
            leader_rx
                .changed()
                .await
                .expect("leader watch remains open");
        }
    })
    .await
    .expect("surviving controller leader");
    reassignment.controller = Some(
        reassignment
            .brokers
            .iter()
            .flatten()
            .find(|broker| broker.node_id() == leader.0)
            .expect("leader handle")
            .listen_addr(),
    );
}

#[rstest_bdd_macros::when("AlterPartitionReassignments cancels partition 0 with null replicas")]
async fn cancel_replica_reassignment(reassignment: &mut ReassignmentBehavior) {
    reassignment.altered = drive_alter_reassignments(
        reassignment.controller.expect("surviving controller"),
        vec![(&reassignment.topic, 0, None)],
    )
    .await;
}

#[rstest_bdd_macros::then(
    "cancellation restores the original ordered replica set and clears adding and removing replicas"
)]
async fn cancelled_replica_reassignment(reassignment: &ReassignmentBehavior) {
    reassignment
        .observer()
        .wait_for_image(|image| {
            image
                .partition(&reassignment.topic, 0)
                .is_some_and(|partition| {
                    partition.replicas == reassignment.original
                        && partition.adding_replicas.is_empty()
                        && partition.removing_replicas.is_empty()
                })
        })
        .await;
    let partition = reassignment
        .observer()
        .partition_record_for_test(&reassignment.topic, 0)
        .expect("partition");
    assert!(
        partition.replicas == reassignment.original,
        "replicas should revert to original after cancel; pr={partition:?}"
    );
}

#[rstest_bdd_macros::then(
    "the new replica materializes as a non-future partition in its preferred extra directory within sixty seconds"
)]
async fn materialized_preplaced_replica(reassignment: &ReassignmentBehavior) {
    let target_dir = reassignment.extra_directories[reassignment.target_index()].path();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let described = describe_log_dirs(
            reassignment
                .broker(reassignment.target_index())
                .listen_addr(),
        )
        .await;
        if described.results.iter().any(|result| {
            fs::canonicalize(&result.log_dir).ok().as_deref()
                == fs::canonicalize(target_dir).ok().as_deref()
                && result.topics.iter().any(|topic| {
                    topic.name == "preplaced"
                        && topic.partitions.iter().any(|partition| {
                            partition.partition_index == 0 && !partition.is_future_key
                        })
                })
        }) {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "replica was not materialized in the preferred log dir"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
    assert!(target_dir.join("preplaced-0").is_dir());
}

async fn foo_cluster() -> ThreeBrokerCluster {
    let cluster = start_three_broker_plaintext_cluster().await;
    create_automatic_topic_plaintext()
        .addr(cluster.addr1)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: "foo".into(),
            replicas: TopicReplicationFactor(2),
            ..Default::default()
        })
        .call()
        .await;
    wait_partition_exists(&cluster.h1, "foo", 0).await;
    cluster
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/partition_reassignment/reassign_lifecycle.feature",
    name = "preplacement log dir preference survives until reassignment materializes the replica"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preplacement_log_dir_preference_survives_until_reassignment_materializes_the_replica(
    reassignment: ReassignmentBehavior,
) {
    reassignment.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/partition_reassignment/reassign_lifecycle.feature",
    name = "alter then complete via isr catchup"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_then_complete_via_isr_catchup(reassignment: ReassignmentBehavior) {
    reassignment.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/partition_reassignment/reassign_lifecycle.feature",
    name = "list in flight returns pending rows"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_in_flight_returns_pending_rows(reassignment: ReassignmentBehavior) {
    reassignment.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/partition_reassignment/reassign_lifecycle.feature",
    name = "cancel via null replicas reverts"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_via_null_replicas_reverts(reassignment: ReassignmentBehavior) {
    reassignment.shutdown().await;
}
