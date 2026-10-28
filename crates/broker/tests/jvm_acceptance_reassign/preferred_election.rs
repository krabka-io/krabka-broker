//! Preferred-leader election driven by `kafka-leader-election
//! --election-type preferred` against a three-broker SASL cluster.
//!
//! The scenario needs a partition whose preferred replica is alive, in the ISR,
//! and not currently the leader, which this file reaches by metadata injection
//! rather than by killing a broker; the test's own doc comment explains why.

use {
    crate::{
        cluster::{OfflineReplicaCommand, RegisteredCluster, admin_cluster, election_partition},
        jvm_acceptance::{
            ClientPropsFile, KAFKA_IMAGE_TXN, broker0_advertised,
            docker_run_kafka_tool_with_image_and_mount, wait_jvm_isr_contains,
            wait_jvm_partition_any_leader, wait_jvm_partition_leader,
        },
        oracle::OracleComparison,
    },
    assert2::assert,
    std::{collections::BTreeMap, process::Output, slice},
};

struct PreferredElection {
    cluster: Option<RegisteredCluster>,
    props: Option<ClientPropsFile>,
    topic: String,
    output: Option<Output>,
}
#[rstest::fixture]
fn election() -> PreferredElection {
    // Start this scenario with fresh inputs and observations.

    PreferredElection {
        cluster: None,
        props: None,
        topic: String::new(),
        output: None,
    }
}
#[rstest_bdd_macros::given(
    "topic {topic} has preferred replica 1 alive in ISR while metadata names replica 2 as leader"
)]
async fn displaced_preferred(election: &mut PreferredElection, topic: String) {
    let topic = topic.as_str();

    let (cluster, admin_props) = Box::pin(admin_cluster()).await;
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic as super-user via the 7.5 JVM image.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--replica-assignment",
        "1:2",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
    ]);

    // Wait for broker 1 to see the partition in the committed metadata image.
    cluster.h1.wait_until_partition_present(topic, 0).await;

    // Record the initial leader (should be broker 1 as preferred replica).
    let initial_leader = wait_jvm_partition_any_leader(&cluster.h1, topic, 0).await;
    eprintln!("KRABKA[test] initial partition leader: {initial_leader}");

    // For the preferred election to do anything interesting we need broker 1
    // to be the preferred (replicas[0]). The assignment above is [1, 2], since
    // an automatic placement would start at a random broker. Assert it held.
    assert!(
        initial_leader == 1,
        "expected broker 1 to be the initial/preferred leader; got {initial_leader}"
    );

    // Inject a PartitionRecord that makes broker 2 the current leader while
    // keeping broker 1 in the ISR as a non-leader replica.
    //
    // This simulates the "preferred replica is not current leader" scenario
    // that `kafka-leader-election --election-type preferred` is designed to
    // fix. We use metadata injection rather than an organic leader change
    // because:
    //
    // 1. An organic leader change requires killing broker 1, and the raft
    //    re-election that follows leaves the surviving controller without a
    //    heartbeat record for broker 2 until broker 2 heartbeats to it. The
    //    controller publishes that gap as a fencing, so `ElectLeaders` refuses
    //    with `PreferredNotAlive` for the width of the window.
    //
    // 2. Under WSL2, inter-broker replication flows through the Windows-host IP
    //    (`host.docker.internal` = 192.168.65.254), not back into the WSL VM
    //    where the peers live, so organic ISR expansion would time out anyway.
    //
    // Metadata injection bypasses both limitations and matches the technique
    // used by `tests/elect_leaders.rs::unclean_election_via_wire_picks_alive_replica`.
    cluster
        .h1
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
            election_partition(
                topic,
                krabka_broker::NodeId(2),
                vec![krabka_broker::NodeId(1), krabka_broker::NodeId(2)],
                vec![krabka_broker::NodeId(2), krabka_broker::NodeId(1)],
            ),
        ))
        .await
        .expect("inject PartitionRecord making broker 2 the leader");

    // Wait for the injected state to propagate to broker 2's metadata image:
    // leader=2, ISR contains both 1 and 2.
    wait_jvm_partition_leader(&cluster.h2, topic, 0, 2).await;
    wait_jvm_isr_contains(&cluster.h2, topic, 0, 1).await;

    // A PREFERRED election refuses with PREFERRED_LEADER_NOT_AVAILABLE until
    // broker 1 is electable, and neither wait above implies that: the leader
    // and the ISR are replicated partition state, while a broker becomes
    // electable only once the controller has seen its heartbeat and published
    // it unfenced. `controllerId` rotates over the unfenced brokers, so the
    // `AdminClient` sends this election to any of the three and every one of
    // them must agree. Settle on all three rather than on whichever the
    // rotation happens to name.
    for handle in [&cluster.h1, &cluster.h2, &cluster.h3] {
        handle.wait_until_broker_electable(1).await;
    }
    eprintln!(
        "KRABKA[test] broker 2 is current leader; broker 1 is in ISR and electable \
         everywhere — running preferred election"
    );

    election.topic = topic.to_owned();
    election.cluster = Some(cluster);
    election.props = Some(admin_props);
}
#[rstest_bdd_macros::when("the SASL JVM tool requests preferred election of partition 0")]
fn elect_preferred(election: &mut PreferredElection) {
    let admin_mount = election
        .props
        .as_ref()
        .expect("admin properties")
        .mount_str();
    let out = jvm_docker_command()
        .image(KAFKA_IMAGE_TXN)
        .mounts(&[&admin_mount])
        .args(&[
            "kafka-leader-election",
            "--election-type",
            "preferred",
            "--topic",
            &election.topic,
            "--partition",
            "0",
            "--bootstrap-server",
            broker0_advertised(),
            "--admin.config",
            "/client.properties",
        ])
        .call()
        .output()
        .expect("spawn kafka-leader-election");

    let election_stdout = String::from_utf8_lossy(&out.stdout);
    let election_stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!(
        "KRABKA[test] kafka-leader-election status={} stdout={election_stdout} stderr={election_stderr}",
        out.status
    );
    election.output = Some(out);
}
#[rstest_bdd_macros::then(
    "the JVM command succeeds and broker 2 observes preferred replica 1 leading"
)]
async fn preferred_leads(election: &PreferredElection) {
    let output = election.output.as_ref().expect("election requested");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    wait_jvm_partition_leader(
        &election.cluster.as_ref().expect("cluster").h2,
        &election.topic,
        0,
        1,
    )
    .await;
}

// ── the two partition selectors, and the two per-partition codes ────────────
//
// The case above drives `--election-type preferred --topic --partition`, which
// is one partition named three ways over. `kafka-leader-election` has two more
// selectors and neither reached a suite: `--all-topic-partitions`, which sends
// a null `topic_partitions` and means every partition the broker knows, and
// `--path-to-json-file`, which names a set in a document.
//
// The codes matter as much as the selectors. `ElectLeaders` reports per
// partition, so a request can succeed while every partition in it failed, and
// the tool sorts the rows into three different sentences: an election it
// performed, an `ELECTION_NOT_NEEDED` (84) it folds into `Valid replica
// already elected`, and anything else as an error naming the exception
// `Errors.forCode` built. A broker that answered 80 where Kafka answers 84
// prints an incident where Kafka prints a no-op, and no in-process test sees
// the difference.

use crate::{
    oracle::{Side, ToolFile},
    support::jvm_docker_command,
    tool_output::{ElectionOutcome, TopicPartition, election_json, parse_election},
};

/// The topic the two selectors are pointed at.
const SELECTOR_TOPIC: &str = "krabka-elect-selectors-itest";

/// Its partition count. More than one, so a selector that answered for a
/// single partition would be visibly short.
const SELECTOR_PARTITIONS: i32 = 3;

/// Where the `--path-to-json-file` document is placed inside whichever
/// container the tool runs in.
const ELECTION_JSON: &str = "/tmp/krabka-election.json";

struct SelectorElection {
    comparison: Option<OracleComparison>,
    partitions: Vec<TopicPartition>,
    by_file: Vec<BTreeMap<TopicPartition, ElectionOutcome>>,
    by_all: Vec<BTreeMap<TopicPartition, ElectionOutcome>>,
}
#[rstest::fixture]
fn selectors() -> SelectorElection {
    // Start this scenario with fresh inputs and observations.

    SelectorElection {
        comparison: None,
        partitions: Vec::new(),
        by_file: Vec::new(),
        by_all: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "Kafka and Krabka expose the healthy single-replica election partitions"
)]
async fn healthy_partitions(selectors: &mut SelectorElection, datatable: Vec<Vec<String>>) {
    selectors.comparison = Some(OracleComparison::start("elect-selectors").await);
    selectors.partitions = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [topic, partition]: [String; 2] = row.try_into().expect("topic partition");
            TopicPartition::new(&topic, partition.parse().expect("partition"))
        })
        .collect();
    for side in selectors.comparison.as_ref().expect("comparison").sides() {
        side.run("kafka-topics", &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            SELECTOR_TOPIC,
            "--partitions",
            &SELECTOR_PARTITIONS.to_string(),
            "--replication-factor",
            "1",
        ])
        .expect_success();
    }
}
#[rstest_bdd_macros::when(
    "both JVM clients elect by JSON partition list and by all-topic-partitions"
)]
fn election_selectors(selectors: &mut SelectorElection) {
    let document = election_json(&selectors.partitions);
    for side in selectors.comparison.as_ref().expect("comparison").sides() {
        let by_file = side.run_with_files(
            "kafka-leader-election",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--election-type",
                "preferred",
                "--path-to-json-file",
                ELECTION_JSON,
            ],
            &[ToolFile::new(ELECTION_JSON, &document)],
            None,
        );
        selectors.by_file.push(parse_election(&by_file.text()));
        let by_all = side.run("kafka-leader-election", &[
            "--bootstrap-server",
            side.bootstrap(),
            "--election-type",
            "preferred",
            "--all-topic-partitions",
        ]);
        selectors.by_all.push(
            parse_election(&by_all.text())
                .into_iter()
                .filter(|(partition, _)| partition.topic == SELECTOR_TOPIC)
                .collect(),
        );
    }
}
#[rstest_bdd_macros::then(
    "each JSON-selected partition reports AlreadyElected and all-topic-partitions agrees between brokers"
)]
fn no_election_needed(selectors: &SelectorElection) {
    let expected: BTreeMap<_, _> = selectors
        .partitions
        .iter()
        .cloned()
        .map(|partition| (partition, ElectionOutcome::AlreadyElected))
        .collect();
    assert!(selectors.by_file.len() == 2);
    for result in &selectors.by_file {
        assert!(*result == expected);
    }
    assert!(selectors.by_all.len() == 2);
    assert!(selectors.by_all[0] == selectors.by_all[1]);
}

#[rstest::fixture]
fn unavailable() -> OfflineReplicaCommand {
    // Each scenario owns a fresh replica topology and its pending command output.
    OfflineReplicaCommand::default()
}
#[rstest_bdd_macros::given(
    "topic {topic} is led by replica 1 with its preferred replica offline and outside ISR"
)]
async fn preferred_offline(unavailable: &mut OfflineReplicaCommand, topic: String) {
    let brokers = unavailable.start(topic).await;
    let side = unavailable.side();
    let topic = unavailable.topic.as_str();
    let props = &unavailable.props;
    let props_file = ToolFile::new("/client.properties", props);

    side.run_with_files(
        "kafka-topics",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--command-config",
            "/client.properties",
        ],
        slice::from_ref(&props_file),
        None,
    )
    .expect_success();
    brokers.h1.wait_until_partition_present(topic, 0).await;

    // Keep a registered non-controller broker offline so the injected
    // preferred replica cannot race back into the ISR before the JVM command.
    let controller_leader = brokers.h1.wait_until_controller_leader().await.unwrap().0;
    let preferred = (2_u64..=3)
        .find(|node| *node != controller_leader)
        .expect("a non-bootstrap, non-controller broker");
    let mut handles = [Some(brokers.h1), Some(brokers.h2), Some(brokers.h3)];
    handles[usize::try_from(preferred - 1).unwrap()]
        .take()
        .expect("preferred replica handle")
        .shutdown()
        .await;
    let h1 = handles[0].as_ref().expect("bootstrap broker stays live");

    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
        election_partition(
            topic,
            krabka_broker::NodeId(1),
            vec![krabka_broker::NodeId(preferred), krabka_broker::NodeId(1)],
            vec![krabka_broker::NodeId(1)],
        ),
    ))
    .await
    .expect("inject a partition whose preferred replica is outside the ISR");
    h1.wait_for_image(|image| {
        image.partition(topic, 0).is_some_and(|record| {
            record.leader == krabka_broker::NodeId(1)
                && record.replicas.first() == Some(&krabka_broker::NodeId(preferred))
                && record.isr == [krabka_broker::NodeId(1)]
        })
    })
    .await;
    wait_jvm_partition_leader(h1, topic, 0, 1).await;

    unavailable.handles = handles;
    unavailable.dirs = Some([brokers.d1, brokers.d2, brokers.d3]);
}
#[rstest_bdd_macros::when(
    "the JVM elects the unavailable preferred replica through the JSON selector"
)]
fn elect_unavailable(unavailable: &mut OfflineReplicaCommand) {
    let props = &unavailable.props;
    let side = Side::Krabka {
        bootstrap: &unavailable.advertised,
    };
    let document = election_json(&[TopicPartition::new(&unavailable.topic, 0)]);
    let run = side.run_with_files(
        "kafka-leader-election",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--election-type",
            "preferred",
            "--path-to-json-file",
            ELECTION_JSON,
            "--admin.config",
            "/client.properties",
        ],
        &[
            ToolFile::new("/client.properties", props),
            ToolFile::new(ELECTION_JSON, &document),
        ],
        None,
    );
    unavailable.output = Some(run);
}
#[rstest_bdd_macros::then("the tool reports exception {exception} for the partition")]
fn unavailable_error(unavailable: &OfflineReplicaCommand, exception: String) {
    let output = unavailable.output.as_ref().expect("election requested");
    assert!(
        parse_election(&output.text())
            == BTreeMap::from([(
                TopicPartition::new(&unavailable.topic, 0),
                ElectionOutcome::Failed(exception)
            )]),
        "{}",
        output.text()
    );
}

// BDD scenario entry points; acceptance steps are defined above.
/// JVM acceptance test for `kafka-leader-election --election-type preferred`.
///
/// The cluster has three brokers because `controllerId` rotates over the
/// unfenced brokers rather than naming the quorum leader, so the
/// `AdminClient` inside the tool may send `ElectLeaders` to any of them. A
/// two-broker cluster would hide an election that only one node answers
/// correctly, which is the regression this case exists to catch.
///
/// Scenario:
/// 1. Boot a 3-broker `SASL_PLAINTEXT` cluster and create an rf=2 topic.
/// 2. Wait for the cluster to assign a leader. Expect broker 1, the
///    preferred replica.
/// 3. Move leadership to broker 2 by injecting a `PartitionRecord`, keeping
///    broker 1 in the ISR. The comment at the injection says why the test
///    does not kill a broker to get there.
/// 4. Wait until every broker would let an operator elect broker 1.
/// 5. Run `kafka-leader-election --election-type preferred` from the JVM CLI
///    image cp-kafka:7.5.0. Older images do not ship this tool.
/// 6. Assert Docker exits 0.
/// 7. Poll until broker 1 is leader again.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/preferred_election.feature",
    name = "jvm kafka leader election preferred"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_leader_election_preferred(election: PreferredElection) {
    let cluster = election.cluster.take().expect("cluster");
    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
    drop(election);
}

/// `--all-topic-partitions` and `--path-to-json-file` over a cluster where
/// every partition already sits on its preferred leader, compared with Apache
/// Kafka.
///
/// A topic at replication factor one is preferred-elected by construction: its
/// only replica is `replicas[0]` and it leads. So both selectors are asked a
/// question whose answer is `ELECTION_NOT_NEEDED` for every partition, which
/// is the one per-partition code an operator meets on a healthy cluster and
/// the one this suite could not otherwise reach.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/preferred_election.feature",
    name = "election selectors report election not needed as apache kafka does"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_selectors_report_election_not_needed_as_apache_kafka_does(
    selectors: SelectorElection,
) {
    selectors
        .comparison
        .take()
        .expect("comparison")
        .broker
        .shutdown()
        .await;
    drop(selectors);
}

/// A partition whose preferred replica is out of the ISR reports
/// `PREFERRED_LEADER_NOT_AVAILABLE` (80), and the tool builds Kafka's own
/// exception from it.
///
/// # Why this half has no oracle
///
/// Code 80 needs a partition whose `replicas[0]` is alive but not electable,
/// which needs a replica set larger than one, which needs more than one
/// broker. The oracle in this file is a single stock node, so it cannot be put
/// in that shape at all -- and a multi-node stock cluster is a different
/// harness, not a variation on this one.
///
/// What is still cross-validated is the half that matters most: `Errors` is
/// Kafka's, and `LeaderElectionCommand` is Kafka's, so the class name asserted
/// below is what Kafka's own client built out of the number krabka sent. A
/// broker that answered 84, or 41, or an unassigned code would print a
/// different class here, or none.
#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_reassign/preferred_election.feature",
    name = "a preferred replica outside the isr reports code 80"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_preferred_replica_outside_the_isr_reports_code_80(unavailable: OfflineReplicaCommand) {
    unavailable.shutdown().await;
}
