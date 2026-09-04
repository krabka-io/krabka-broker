//! Preferred-leader election driven by `kafka-leader-election
//! --election-type preferred` against a three-broker SASL cluster.
//!
//! The scenario needs a partition whose preferred replica is alive, in the ISR,
//! and not currently the leader, which this file reaches by metadata injection
//! rather than by killing a broker; the test's own doc comment explains why.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_three_broker_sasl_plaintext_jvm_cluster,
    wait_jvm_isr_contains, wait_jvm_partition_any_leader, wait_jvm_partition_leader,
    wait_three_brokers_registered, write_client_props,
};

/// JVM acceptance test for `kafka-leader-election --election-type preferred`.
///
/// The test uses a **3-broker** `SASL_PLAINTEXT` cluster so that the raft
/// quorum (2/3) survives the kill of broker 1, the preferred replica. A
/// 2-broker cluster would lose quorum (1/2) when broker 1 dies and could not
/// commit the partition-leader change that the PREFERRED election needs.
///
/// Scenario:
/// 1. Boot a 3-broker `SASL_PLAINTEXT` cluster and create an rf=2 topic.
/// 2. Wait for the cluster to assign a leader. Expect broker 1, the
///    preferred replica.
/// 3. Kill broker 1. Broker 2 or broker 3 then leads partition 0 through
///    automatic failover.
/// 4. Revive broker 1 with Rejoin. Wait for it to re-enter the ISR in
///    broker 2's view.
/// 5. Run `kafka-leader-election --election-type preferred` from the JVM CLI
///    image cp-kafka:7.5.0. Older images do not ship this tool.
/// 6. Assert Docker exits 0.
/// 7. Poll until broker 1 is leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_leader_election_preferred() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-elect-preferred-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Wait for all three brokers to register in the metadata image.
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic as super-user via the 7.5 JVM image.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition in the committed metadata image.
    h1.wait_until_partition_present(TOPIC, 0).await;

    // Record the initial leader (should be broker 1 as preferred replica).
    let initial_leader = wait_jvm_partition_any_leader(&h1, TOPIC, 0).await;
    eprintln!("KRABKA[test] initial partition leader: {initial_leader}");

    // For the preferred election to do anything interesting we need broker 1
    // to be the preferred (replicas[0]). The scheduler should assign [1, 2]
    // since broker 1 is node_id=1 (lowest). Assert this assumption.
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
    // 1. An organic leader change requires killing broker 1, which causes the
    //    raft-leader-dependent `ControllerLivenessState` to lose broker 2's
    //    heartbeat record for the window between raft re-election and broker 2's
    //    first heartbeat to the new raft leader — making `ElectLeaders` fail
    //    with `PreferredNotAlive` during that window.
    //
    // 2. Under WSL2, inter-broker replication flows through the Windows-host IP
    //    (`host.docker.internal` = 192.168.65.254), not back into the WSL VM
    //    where the peers live, so organic ISR expansion would time out anyway.
    //
    // Metadata injection bypasses both limitations and matches the technique
    // used by `tests/elect_leaders.rs::unclean_election_via_wire_picks_alive_replica`.
    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
        krabka_metadata::PartitionRecord {
            topic: TOPIC.to_string(),
            partition: 0,
            // Make broker 2 the current leader — so broker 1 (replicas[0])
            // is no longer the leader but is still alive and in the ISR.
            leader: krabka_broker::NodeId(2),
            replicas: vec![krabka_broker::NodeId(1), krabka_broker::NodeId(2)],
            isr: vec![krabka_broker::NodeId(2), krabka_broker::NodeId(1)],
            leader_epoch: krabka_metadata::LeaderEpoch(1),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        },
    ))
    .await
    .expect("inject PartitionRecord making broker 2 the leader");

    // Wait for the injected state to propagate to broker 2's metadata image:
    // leader=2, ISR contains both 1 and 2.
    wait_jvm_partition_leader(&h2, TOPIC, 0, 2).await;
    wait_jvm_isr_contains(&h2, TOPIC, 0, 1).await;
    eprintln!(
        "KRABKA[test] broker 2 is current leader; broker 1 is in ISR — running preferred election"
    );

    // Run kafka-leader-election via the 7.5 JVM image.
    // kafka-leader-election is NOT present in cp-kafka:6.1.1 (Kafka 2.7).
    // cp-kafka:7.5.0 (Kafka 3.5) ships it. The tool sends `ElectLeaders`
    // (api_key 43) which the Rust broker now handles via T4/T5.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-leader-election",
            "--election-type",
            "preferred",
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--bootstrap-server",
            broker0_advertised(),
            "--admin.config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-leader-election");

    let election_stdout = String::from_utf8_lossy(&out.stdout);
    let election_stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!(
        "KRABKA[test] kafka-leader-election status={} stdout={election_stdout} stderr={election_stderr}",
        out.status
    );
    assert!(
        out.status.success(),
        "kafka-leader-election failed: stdout={election_stdout} stderr={election_stderr}",
    );

    // Poll until broker 1 is the leader again on broker 2's view.
    wait_jvm_partition_leader(&h2, TOPIC, 0, 1).await;
    eprintln!("KRABKA[test] preferred election confirmed: broker 1 is leader again");

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
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
    jvm_acceptance::start_host_broker,
    oracle::{Oracle, Side, ToolFile},
    tool_output::{ElectionOutcome, TopicPartition, election_json, parse_election},
};

/// The topic the two selectors are pointed at.
const SELECTOR_TOPIC: &str = "krabka-elect-selectors-itest";

/// Its partition count. More than one, so a selector that answered for a
/// single partition would be visibly short.
const SELECTOR_PARTITIONS: i32 = 3;

/// Where the `--path-to-json-file` document is placed inside whichever
/// container the tool runs in.
const ELECTION_JSON: &str = "/krabka-election.json";

/// Every partition of [`SELECTOR_TOPIC`].
fn selector_partitions() -> Vec<TopicPartition> {
    (0..SELECTOR_PARTITIONS)
        .map(|index| TopicPartition::new(SELECTOR_TOPIC, index))
        .collect()
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn election_selectors_report_election_not_needed_as_apache_kafka_does() {
    // Kafka first: the claim that a healthy partition answers 84 rather than
    // succeeding is a claim about Kafka, and this is where a wrong one fails.
    let oracle = tokio::task::spawn_blocking(|| Oracle::start("elect-selectors"))
        .await
        .expect("oracle boot");
    let oracle_side = Side::Oracle(&oracle);

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();
    let advertised = broker0_advertised().to_owned();
    let krabka_side = Side::Krabka {
        bootstrap: &advertised,
    };

    let document = election_json(&selector_partitions());
    let mut answers = Vec::new();
    for side in [&oracle_side, &krabka_side] {
        side.run(
            "kafka-topics",
            &[
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
            ],
        )
        .expect_success();

        // The document selector first. It names exactly this topic's
        // partitions, so its answer is this topic's answer and nothing else's.
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
        let outcomes = parse_election(&by_file.text());
        let expected: std::collections::BTreeMap<_, _> = selector_partitions()
            .into_iter()
            .map(|partition| (partition, ElectionOutcome::AlreadyElected))
            .collect();
        assert!(
            outcomes == expected,
            "{}: --path-to-json-file must report 84 for every partition, got {outcomes:?}\n{}",
            side.label(),
            by_file.text(),
        );

        // And the whole-cluster selector. Its answer covers the internal
        // topics too, so it is narrowed to this topic's partitions before it
        // is compared -- the coordinator topics differ between the two sides
        // and say nothing about the selector.
        let by_all = side.run(
            "kafka-leader-election",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--election-type",
                "preferred",
                "--all-topic-partitions",
            ],
        );
        let mine: std::collections::BTreeMap<_, _> = parse_election(&by_all.text())
            .into_iter()
            .filter(|(partition, _)| partition.topic == SELECTOR_TOPIC)
            .collect();
        answers.push(mine);
    }
    assert!(
        answers[0] == answers[1],
        "--all-topic-partitions: krabka and Apache Kafka disagreed: {answers:?}",
    );

    broker.shutdown().await;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn a_preferred_replica_outside_the_isr_reports_code_80() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-elect-unavailable-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();
    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    );
    let advertised = broker0_advertised().to_owned();
    let side = Side::Krabka {
        bootstrap: &advertised,
    };
    let props_file = ToolFile::new("/client.properties", &props);

    side.run_with_files(
        "kafka-topics",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--command-config",
            "/client.properties",
        ],
        std::slice::from_ref(&props_file),
        None,
    )
    .expect_success();
    h1.wait_until_partition_present(TOPIC, 0).await;

    // Broker 3 is the preferred replica and is not in the ISR, while broker 2
    // leads. Injection rather than an organic shrink, for the reasons the
    // first case in this file sets out: inter-broker replication does not
    // route back into the VM under WSL2, so an ISR this test waited for would
    // never form.
    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
        krabka_metadata::PartitionRecord {
            topic: TOPIC.to_string(),
            partition: 0,
            leader: krabka_broker::NodeId(2),
            replicas: vec![krabka_broker::NodeId(3), krabka_broker::NodeId(2)],
            isr: vec![krabka_broker::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(1),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        },
    ))
    .await
    .expect("inject a partition whose preferred replica is outside the ISR");
    wait_jvm_partition_leader(&h2, TOPIC, 0, 2).await;

    let document = election_json(&[TopicPartition::new(TOPIC, 0)]);
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
            ToolFile::new("/client.properties", &props),
            ToolFile::new(ELECTION_JSON, &document),
        ],
        None,
    );
    let outcomes = parse_election(&run.text());
    assert!(
        outcomes
            == std::collections::BTreeMap::from([(
                TopicPartition::new(TOPIC, 0),
                ElectionOutcome::Failed(
                    "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException"
                        .to_owned(),
                ),
            )]),
        "the election must report code 80 as Kafka's own exception, got {outcomes:?}\n{}",
        run.text(),
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
