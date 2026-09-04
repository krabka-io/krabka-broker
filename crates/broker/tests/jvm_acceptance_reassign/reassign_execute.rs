//! The plain `kafka-reassign-partitions --execute` and `--verify` round-trip
//! against a three-broker SASL cluster.
//!
//! The test injects the post-move ISR rather than waiting for inter-broker
//! replication, which does not route back into the VM under WSL2; the throttled
//! variant of the same flow lives in the sibling `reassign_throttle` module.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    nc_check_connectivity, plain_jaas, start_three_broker_sasl_plaintext_jvm_cluster,
    wait_three_brokers_registered, write_client_props, write_temp_file,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-reassign-itest";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic.
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

    // Determine initial replicas and pick the third broker as the new target.
    // Broker node IDs are i32 on the wire but stored as u64 in PartitionRecord.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let initial = pr.replicas.clone();
    // node IDs are 1-3; find the one not in the initial replica set.
    let new_node: u64 = (1u64..=3)
        .find(|n| !initial.contains(&krabka_metadata::NodeId(*n)))
        .expect("free broker");
    let staying: u64 = initial.first().unwrap().0;
    eprintln!("KRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");

    // Write reassignment JSON: move partition 0 to [staying, new_node].
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path());

    // Execute reassignment.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--execute",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute");
    eprintln!(
        "KRABKA[test] --execute status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kafka-reassign-partitions --execute failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // the reassignment tests use the same technique).
    let pr_after = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after alter");
    let removing_replica = pr_after
        .removing_replicas
        .first()
        .copied()
        .unwrap_or_else(|| {
            initial
                .last()
                .copied()
                .unwrap_or(krabka_metadata::NodeId(0))
        });
    let injected = krabka_metadata::PartitionRecord {
        isr: vec![
            krabka_metadata::NodeId(staying),
            krabka_metadata::NodeId(new_node),
            removing_replica,
        ],
        ..pr_after.clone()
    };
    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR for reassignment completion");

    // Wait until adding_replicas and removing_replicas are both drained from
    // the committed metadata image.
    h1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
    })
    .await;
    // After completion the replica set must match [staying, new_node].
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after reassignment");
    let got: std::collections::HashSet<u64> = pr.replicas.iter().map(|n| n.0).collect();
    let want: std::collections::HashSet<u64> = maplit::hashset! {staying, new_node};
    assert!(
        got == want,
        "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
    );
    eprintln!("KRABKA[test] reassignment completed; running --verify");

    // --verify should report completion.
    let verify_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "-v",
            &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --verify");
    eprintln!(
        "KRABKA[test] --verify status={} stdout={} stderr={}",
        verify_out.status,
        String::from_utf8_lossy(&verify_out.stdout),
        String::from_utf8_lossy(&verify_out.stderr),
    );
    // Broker-scoped IncrementalAlterConfigs (resource_type=4) is supported,
    // so --verify can clear throttles and exit 0.
    assert!(
        verify_out.status.success(),
        "kafka-reassign-partitions --verify failed: stderr={}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

// ── `--generate` and `--additional` ─────────────────────────────────────────
//
// The case above is `--execute` and `--verify`, which is the middle of the
// tool's workflow. The two ends were untested: `--generate`, which is how an
// operator obtains the document the other verbs read, and `--additional`,
// which is what stops a second `--execute` from cancelling the first one's
// work.

use std::collections::BTreeSet;

use crate::{
    jvm_acceptance::start_host_broker,
    oracle::{Oracle, Side, ToolFile},
    tool_output::{
        Assignment, TopicPartition, parse_generate, reassignment_json, topics_to_move_json,
    },
};

/// Where the documents these cases write are placed inside the container.
const TOPICS_JSON: &str = "/krabka-topics-to-move.json";
const PLAN_JSON: &str = "/krabka-reassignment.json";
/// Where the SASL client configuration is mounted for the cluster cases.
const CLIENT_PROPS: &str = "/client.properties";

/// One `kafka-reassign-partitions` invocation, with the files it names.
fn reassign(
    side: &Side<'_>,
    props: Option<&str>,
    args: &[&str],
    files: Vec<ToolFile>,
) -> crate::oracle::CliRun {
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

/// `--generate` produces a usable plan on krabka and on Apache Kafka.
///
/// The tool builds the proposal itself, out of `DescribeTopics` and
/// `DescribeCluster`; what a broker contributes is the current assignment and
/// the broker set. So the rule the two sides share is that the plan is
/// *usable* -- it covers the topic's partitions, and every replica in it is a
/// broker the operator named -- and a broker that mis-reported either would
/// hand the operator a document that `--execute` then refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reassign_partitions_generate_produces_a_usable_plan_on_both() {
    const TOPIC: &str = "krabka-generate-itest";
    const PARTITIONS: i32 = 3;

    let oracle = tokio::task::spawn_blocking(|| Oracle::start("reassign-generate"))
        .await
        .expect("oracle boot");
    let oracle_side = Side::Oracle(&oracle);

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();
    let advertised = broker0_advertised().to_owned();
    let krabka_side = Side::Krabka {
        bootstrap: &advertised,
    };

    let document = topics_to_move_json(&[TOPIC]);
    for side in [&oracle_side, &krabka_side] {
        side.run(
            "kafka-topics",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--create",
                "--if-not-exists",
                "--topic",
                TOPIC,
                "--partitions",
                &PARTITIONS.to_string(),
                "--replication-factor",
                "1",
            ],
        )
        .expect_success();

        let generated = reassign(
            side,
            None,
            &[
                "--generate",
                "--topics-to-move-json-file",
                TOPICS_JSON,
                "--broker-list",
                "1",
            ],
            vec![ToolFile::new(TOPICS_JSON, &document)],
        );
        assert!(
            generated.succeeded(),
            "{}: --generate failed:\n{}",
            side.label(),
            generated.text(),
        );
        let plans = parse_generate(&generated.stdout);
        let Some((current, proposed)) = plans else {
            panic!(
                "{}: --generate printed neither plan:\n{}",
                side.label(),
                generated.stdout,
            );
        };
        assert_generated_plan_is_usable(
            side.label(),
            TOPIC,
            PARTITIONS,
            1,
            &BTreeSet::from([1]),
            &current,
            &proposed,
        );
    }

    broker.shutdown().await;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reassign_partitions_additional_keeps_the_reassignment_already_running() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-additional-itest";

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
            "2",
            "--replication-factor",
            "1",
            "--command-config",
            CLIENT_PROPS,
        ],
        &[ToolFile::new(CLIENT_PROPS, &props)],
        None,
    )
    .expect_success();
    for partition in 0..2 {
        h1.wait_until_partition_present(TOPIC, partition).await;
    }

    // Move each partition onto the two brokers it is not on. Nothing in this
    // harness makes the new replica catch up -- inter-broker replication does
    // not route back into the VM -- so both moves stay in flight, which is the
    // state `--additional` is about.
    for partition in 0..2 {
        let current = h1
            .partition_record_for_test(TOPIC, partition)
            .expect("partition record");
        let held: BTreeSet<u64> = current.replicas.iter().map(|node| node.0).collect();
        let targets: Vec<i32> = (1..=3)
            .filter(|node| !held.contains(node))
            .map(|node| i32::try_from(node).expect("a node id fits"))
            .collect();
        let plan = reassignment_json(&[Assignment {
            partition: TopicPartition::new(TOPIC, partition),
            replicas: targets,
        }]);
        let mut args = vec!["--execute", "--reassignment-json-file", PLAN_JSON];
        // The second move is the one under test: without `--additional` it
        // would cancel the first.
        if partition == 1 {
            args.push("--additional");
        }
        let run = reassign(
            &side,
            Some(&props),
            &args,
            vec![ToolFile::new(PLAN_JSON, &plan)],
        );
        assert!(
            run.succeeded(),
            "--execute for partition {partition} failed:\n{}",
            run.text(),
        );
    }

    for partition in 0..2 {
        let record = h1
            .partition_record_for_test(TOPIC, partition)
            .expect("partition record after the second execute");
        assert!(
            !record.adding_replicas.is_empty(),
            "partition {partition} must still be reassigning after --additional: {record:?}",
        );
    }

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
