//! The plain `kafka-reassign-partitions --execute` and `--verify` round-trip
//! against a three-broker SASL cluster.
//!
//! The move completes only after the added replica fetches the real log and
//! joins the ISR; no metadata record is injected by the test.

use {
    crate::{
        cluster::{
            OfflineReplicaCommand, RegisteredCluster, assert_reassigned, assignment, execute_plan,
            offline_for_partition, reassignment_cluster, removed_replica,
        },
        jvm_acceptance::{
            ClientPropsFile, KAFKA_IMAGE_TXN, TempFileMount, broker0_advertised,
            verify_console_reassignment,
        },
        oracle::{AssignedTopicCommandSetup, CliRun, OracleComparison},
        support::{ContainerInput, jvm_docker_command},
    },
    assert2::assert,
    krabka_metadata::NodeId,
    std::{
        io::Write as _,
        process::{Output, Stdio},
        time::Duration,
    },
    tokio::time,
};

struct MovingReplica {
    cluster: Option<RegisteredCluster>,
    props: Option<ClientPropsFile>,
    plan: Option<TempFileMount>,
    admin_mount: String,
    json_mount: String,
    topic: String,
    staying: u64,
    added: u64,
    removing: Option<NodeId>,
    verify: Option<Output>,
}
#[rstest::fixture]
fn moving() -> MovingReplica {
    // Start this scenario with fresh inputs and observations.

    MovingReplica {
        cluster: None,
        props: None,
        plan: None,
        admin_mount: String::new(),
        json_mount: String::new(),
        topic: String::new(),
        staying: 0,
        added: 0,
        removing: None,
        verify: None,
    }
}
#[rstest_bdd_macros::given("a replicated SASL topic {topic} contains record before-reassignment")]
async fn replicated_record(moving: &mut MovingReplica, topic: String) {
    let (cluster, props, mount) = Box::pin(reassignment_cluster(&topic)).await;
    produce_one_record(&mount, &topic);
    moving.cluster = Some(cluster);
    moving.props = Some(props);
    moving.admin_mount = mount;
    moving.topic = topic;
}
#[rstest_bdd_macros::when(
    "the JVM executes a replica move retaining the leader and adding the unassigned broker"
)]
fn move_replica(moving: &mut MovingReplica) {
    let cluster = moving.cluster.as_ref().expect("cluster");
    let (initial, added, staying) = assignment(&cluster.h1, &moving.topic, true);
    let (file, mount) = execute_plan(&moving.admin_mount, &moving.topic, staying, added, false);
    let record = cluster
        .h1
        .partition_record_for_test(&moving.topic, 0)
        .expect("record after alter");
    moving.removing = Some(removed_replica(&record, &initial));
    moving.added = added;
    moving.staying = staying;
    moving.plan = Some(file);
    moving.json_mount = mount;
}
#[rstest_bdd_macros::then(
    "the added replica catches up with real records, the target assignment completes, and the removed replica directory is pruned"
)]
async fn replicas_moved(moving: &MovingReplica) {
    let cluster = moving.cluster.as_ref().expect("cluster");
    wait_for_catch_up(cluster, &moving.topic, moving.staying, moving.added).await;
    wait_for_drained_reassignment(cluster, &moving.topic).await;
    assert_reassigned(&cluster.h1, &moving.topic, moving.staying, moving.added);
    wait_for_pruned_replica(
        cluster,
        &moving.topic,
        moving.removing.expect("removed replica"),
    )
    .await;
}
#[rstest_bdd_macros::when("the JVM verifies the executed reassignment plan")]
fn verify_move(moving: &mut MovingReplica) {
    moving.verify = Some(verify_console_reassignment(
        &moving.admin_mount,
        &moving.json_mount,
    ));
}
#[rstest_bdd_macros::then("the verification output reports complete")]
fn complete_move(moving: &MovingReplica) {
    assert!(
        String::from_utf8_lossy(&moving.verify.as_ref().expect("verified").stdout)
            .to_ascii_lowercase()
            .contains("complete")
    );
}

/// Write one record into `topic` with `kafka-console-producer`.
fn produce_one_record(admin_mount: &str, topic: &str) {
    let mut producer = jvm_docker_command()
        .image(KAFKA_IMAGE_TXN)
        .mounts(&[admin_mount])
        .args(&[
            "kafka-console-producer",
            "--topic",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
            "--producer.config",
            "/client.properties",
        ])
        .input(ContainerInput::Attached)
        .call()
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn kafka-console-producer");
    producer
        .stdin
        .as_mut()
        .expect("producer stdin")
        .write_all(b"before-reassignment\n")
        .expect("write record");
    drop(producer.stdin.take());
    assert!(producer.wait().expect("producer exit").success());
}

/// Wait until the added replica reaches the leader's log end offset.
async fn wait_for_catch_up(cluster: &RegisteredCluster, topic: &str, staying: u64, new_node: u64) {
    let handles = [&cluster.h1, &cluster.h2, &cluster.h3];
    let leader_leo = handles[usize::try_from(staying - 1).unwrap()]
        .local_log_end_offset(topic, 0)
        .expect("leader log");
    assert!(leader_leo > 0, "the reassignment must move real records");
    time::timeout(Duration::from_secs(30), async {
        loop {
            if handles[usize::try_from(new_node - 1).unwrap()].local_log_end_offset(topic, 0)
                == Some(leader_leo)
            {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("added replica reaches the leader LEO");
}

/// Wait until the reassignment's adding and removing replicas are drained.
async fn wait_for_drained_reassignment(cluster: &RegisteredCluster, topic: &str) {
    // Wait until adding_replicas and removing_replicas are both drained from
    // the committed metadata image.
    let completed = time::timeout(Duration::from_secs(30), async {
        loop {
            if cluster
                .h1
                .partition_record_for_test(topic, 0)
                .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
            {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "reassignment did not complete: {:?}",
        cluster.h1.partition_record_for_test(topic, 0)
    );
}

/// Wait until the removed replica's partition directory is pruned.
async fn wait_for_pruned_replica(
    cluster: &RegisteredCluster,
    topic: &str,
    removing_replica: NodeId,
) {
    let dirs = [cluster.d1.path(), cluster.d2.path(), cluster.d3.path()];
    let removed_dir =
        dirs[usize::try_from(removing_replica.0 - 1).unwrap()].join(format!("{topic}-0"));
    time::timeout(Duration::from_secs(30), async {
        while removed_dir.exists() {
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("removed replica directory is pruned");
}

// ── `--generate` and `--additional` ─────────────────────────────────────────
//
// The case above is `--execute` and `--verify`, which is the middle of the
// tool's workflow. The two ends were untested: `--generate`, which is how an
// operator obtains the document the other verbs read, and `--additional`,
// which is what stops a second `--execute` from cancelling the first one's
// work.

use {
    crate::{
        oracle::{Side, ToolFile},
        tool_output::{
            Assignment, TopicPartition, parse_generate, reassignment_json, topics_to_move_json,
        },
    },
    std::collections::BTreeSet,
};

/// Where the documents these cases write are placed inside the container.
const TOPICS_JSON: &str = "/tmp/krabka-topics-to-move.json";
const PLAN_JSON: &str = "/tmp/krabka-reassignment.json";
/// Where the SASL client configuration is mounted for the cluster cases.
const CLIENT_PROPS: &str = "/client.properties";

/// One `kafka-reassign-partitions` invocation, with the files it names.
fn reassign(side: &Side<'_>, props: Option<&str>, args: &[&str], files: Vec<ToolFile>) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    let mut files = files;
    if let Some(props) = props {
        full.extend_from_slice(&["--command-config", CLIENT_PROPS]);
        files.push(ToolFile::new(CLIENT_PROPS, props));
    }
    side.run_with_files("kafka-reassign-partitions", &full, &files, None)
}

/// What a `--generate` answer must be true of, whichever broker produced it.
///
/// Stated once and applied to both sides, so krabka's answer cannot be held to
/// a weaker rule than Kafka's. The proposal itself is not compared between the
/// sides: the two clusters have different broker sets on purpose -- one node
/// against three -- and `--generate` is a round-robin over whatever
/// `--broker-list` names, so equal proposals would mean the case had stopped
/// testing anything.
fn assert_generated_plan_is_usable(
    side: &str,
    topic: &str,
    partitions: i32,
    replication_factor: usize,
    brokers: &BTreeSet<i32>,
    current: &[Assignment],
    proposed: &[Assignment],
) {
    let expected: BTreeSet<TopicPartition> = (0..partitions)
        .map(|index| TopicPartition::new(topic, index))
        .collect();
    let covered = |plan: &[Assignment]| -> BTreeSet<TopicPartition> {
        plan.iter().map(|a| a.partition.clone()).collect()
    };
    assert!(
        covered(current) == expected,
        "{side}: the current assignment must cover every partition of {topic}: {current:?}",
    );
    assert!(
        covered(proposed) == expected,
        "{side}: the proposal must cover every partition of {topic}: {proposed:?}",
    );
    for assignment in current.iter().chain(proposed) {
        let replicas: BTreeSet<i32> = assignment.replicas.iter().copied().collect();
        assert!(
            replicas.len() == assignment.replicas.len()
                && replicas.len() == replication_factor
                && replicas.is_subset(brokers),
            "{side}: {assignment:?} must be {replication_factor} distinct brokers out of \
             {brokers:?}",
        );
    }
}

struct GeneratedPlans {
    comparison: Option<OracleComparison>,
    topic: String,
    partitions: i32,
    outputs: Vec<CliRun>,
}
#[rstest::fixture]
fn generated() -> GeneratedPlans {
    // Start this scenario with fresh inputs and observations.

    GeneratedPlans {
        comparison: None,
        topic: String::new(),
        partitions: 0,
        outputs: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka and Krabka each have single-replica topic {topic} with {partitions} partitions"
)]
async fn plan_topics(generated: &mut GeneratedPlans, topic: String, partitions: i32) {
    generated.comparison = Some(OracleComparison::start("reassign-generate").await);
    for side in generated.comparison.as_ref().expect("comparison").sides() {
        side.run("kafka-topics", &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            &topic,
            "--partitions",
            &partitions.to_string(),
            "--replication-factor",
            "1",
        ])
        .expect_success();
    }
    generated.topic = topic;
    generated.partitions = partitions;
}
#[rstest_bdd_macros::when(
    "the JVM generates reassignment plans using broker list 1 on both brokers"
)]
fn generate_plans(generated: &mut GeneratedPlans) {
    let document = topics_to_move_json(&[&generated.topic]);
    for side in generated.comparison.as_ref().expect("comparison").sides() {
        generated.outputs.push(reassign(
            &side,
            None,
            &[
                "--generate",
                "--topics-to-move-json-file",
                TOPICS_JSON,
                "--broker-list",
                "1",
            ],
            vec![ToolFile::new(TOPICS_JSON, &document)],
        ));
    }
}
#[rstest_bdd_macros::then(
    "both current and proposed plans cover every topic partition exactly with one distinct replica from broker 1"
)]
fn usable_plans(generated: &GeneratedPlans) {
    assert!(generated.outputs.len() == 2);
    for (index, output) in generated.outputs.iter().enumerate() {
        assert!(output.succeeded(), "{}", output.text());
        let (current, proposed) =
            parse_generate(&output.stdout).expect("current and proposed plans");
        assert_generated_plan_is_usable(
            &index.to_string(),
            &generated.topic,
            generated.partitions,
            1,
            &BTreeSet::from([1]),
            &current,
            &proposed,
        );
    }
}

#[rstest::fixture]
fn additional() -> OfflineReplicaCommand {
    // Each scenario owns a fresh replica topology and its pending command output.
    OfflineReplicaCommand::default()
}
#[rstest_bdd_macros::given(
    "topic {topic} has two partitions on broker 1 and partition 0 is moving to an offline registered target"
)]
async fn pending_first_move(additional: &mut OfflineReplicaCommand, topic: String) {
    let brokers = additional.start(topic).await;
    let side = additional.side();
    let topic = additional.topic.as_str();
    let props = &additional.props;

    side.create_assigned_topic(AssignedTopicCommandSetup {
        topic: topic,
        assignment: "1,1",
        props_path: CLIENT_PROPS,
        files: &[ToolFile::new(CLIENT_PROPS, &props)],
    });
    for partition in 0..2 {
        brokers
            .h1
            .wait_until_partition_present(topic, partition)
            .await;
    }

    // Stop a registered, non-bootstrap target so the first move cannot race
    // replica catch-up and complete before the second command runs.
    let (offline_node, handles, first) = offline_for_partition(
        [brokers.h1, brokers.h2, brokers.h3],
        topic,
        "partition record",
    )
    .await;
    let h1 = handles[0].as_ref().expect("bootstrap broker stays live");

    let staying = i32::try_from(first.replicas[0].0).expect("a node id fits");
    let first_plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(topic, 0),
        replicas: vec![
            staying,
            i32::try_from(offline_node).expect("a node id fits"),
        ],
    }]);
    reassign(
        &side,
        Some(&props),
        &["--execute", "--reassignment-json-file", PLAN_JSON],
        vec![ToolFile::new(PLAN_JSON, &first_plan)],
    )
    .expect_success();
    h1.wait_for_image(|image| {
        image
            .partition(topic, 0)
            .is_some_and(|record| !record.adding_replicas.is_empty())
    })
    .await;

    additional.handles = handles;
    additional.dirs = Some([brokers.d1, brokers.d2, brokers.d3]);
}
#[rstest_bdd_macros::when("the JVM executes a no-op assignment for partition 1 with additional")]
fn add_move(additional: &mut OfflineReplicaCommand) {
    let h1 = additional.handles[0].as_ref().expect("bootstrap broker");
    let props = &additional.props;
    let side = Side::Krabka {
        bootstrap: &additional.advertised,
    };
    // A no-op assignment is enough to exercise the client's `--additional`
    // path. Its contract here is that it must not cancel partition 0.
    let second = h1
        .partition_record_for_test(&additional.topic, 1)
        .expect("second partition record");
    let second_plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(&additional.topic, 1),
        replicas: second
            .replicas
            .iter()
            .map(|node| i32::try_from(node.0).expect("a node id fits"))
            .collect(),
    }]);
    additional.output = Some(reassign(
        &side,
        Some(&props),
        &[
            "--execute",
            "--reassignment-json-file",
            PLAN_JSON,
            "--additional",
        ],
        vec![ToolFile::new(PLAN_JSON, &second_plan)],
    ));
}
#[rstest_bdd_macros::then(
    "the additional command succeeds and partition 0 still has adding replicas"
)]
fn first_move_preserved(additional: &OfflineReplicaCommand) {
    let output = additional.output.as_ref().expect("additional command ran");
    assert!(output.succeeded(), "{}", output.text());
    let record = additional.handles[0]
        .as_ref()
        .expect("bootstrap broker")
        .partition_record_for_test(&additional.topic, 0)
        .expect("partition");
    assert!(!record.adding_replicas.is_empty());
}

// BDD scenario entry points; acceptance steps are defined above.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/reassign_execute.feature",
    name = "jvm kafka reassign partitions end to end"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_reassign_partitions_end_to_end(moving: MovingReplica) {
    let cluster = moving.cluster.take().expect("cluster");
    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
    drop(moving);
}

/// `--generate` produces a usable plan on krabka and on Apache Kafka.
///
/// The tool builds the proposal itself, out of `DescribeTopics` and
/// `DescribeCluster`; what a broker contributes is the current assignment and
/// the broker set. So the rule the two sides share is that the plan is
/// *usable* -- it covers the topic's partitions, and every replica in it is a
/// broker the operator named -- and a broker that mis-reported either would
/// hand the operator a document that `--execute` then refuses.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/reassign_execute.feature",
    name = "reassign partitions generate produces a usable plan on both"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reassign_partitions_generate_produces_a_usable_plan_on_both(generated: GeneratedPlans) {
    generated
        .comparison
        .take()
        .expect("comparison")
        .broker
        .shutdown()
        .await;
    drop(generated);
}

/// A second `--execute --additional` leaves the first reassignment running.
///
/// Without `--additional` the tool cancels every reassignment its document
/// does not mention, which is the behaviour that loses an operator's
/// half-finished move when they start a second one. The flag is client-side,
/// but what it protects is server state, so the assertion is made against the
/// metadata image rather than against what the tool said about itself.
///
/// # Why this half has no oracle
///
/// A reassignment that is still running is one whose new replica has not
/// caught up, and on a single stock node there is no second broker to move a
/// replica to. The oracle in this file cannot host the premise.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/reassign_execute.feature",
    name = "reassign partitions additional keeps the reassignment already running"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reassign_partitions_additional_keeps_the_reassignment_already_running(
    additional: OfflineReplicaCommand,
) {
    additional.shutdown().await;
}
