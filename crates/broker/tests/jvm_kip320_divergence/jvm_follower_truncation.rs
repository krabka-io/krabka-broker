//! Scenario 2: a JVM follower truncates a divergent suffix from a Krabka leader.
//!
//! The scenario forces a real divergent suffix on a mixed cluster and asserts
//! that the JVM follower removes it and converges on the Krabka leader's exact
//! log end offset, and that a `kafka-console-consumer` then recovers. Its
//! divergence recipe is specific to this direction, so it has its own file.

use {
    crate::{
        docker::{KAFKA_IMAGE, docker_run_kafka_tool_with_image},
        dump_log::grep_base_offsets,
        mixed_cluster::MixedCluster,
        test_support::truncation::*,
    },
    krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord},
    std::{
        process::Output,
        time::{Duration, Instant},
    },
    tokio::time,
};
const TOPIC: &str = "krabka-kip320-jvm-follower";
#[derive(Default)]
struct JvmFollowerRecovery {
    cluster: Option<MixedCluster>,
    container: String,
    bootstrap_all: String,
    prefix_leo: i64,
    partition: Option<PartitionRecord>,
    jvm_epoch: Option<LeaderEpoch>,
    parked_epoch: Option<LeaderEpoch>,
    before_dump: Option<String>,
    before_rewrite: Option<i64>,
    after_rewrite: Option<i64>,
    recovered_dump: Option<String>,
    consumer: Option<Output>,
}
#[rstest::fixture]
fn jvm_follower_recovery() -> JvmFollowerRecovery {
    // Independent mixed-cluster logs prove truncation rather than equal-length catch-up.
    JvmFollowerRecovery::default()
}
#[rstest_bdd_macros::given(
    "a mixed topic krabka-kip320-jvm-follower with a committed shared prefix"
)]
async fn jvm_divergence_prefix(jvm_follower_recovery: &mut JvmFollowerRecovery) {
    let (container, cluster, bootstrap_all, prefix_leo) =
        prepare_divergence(TOPIC, DivergenceDirection::JvmFollower).await;
    jvm_follower_recovery.cluster = Some(cluster);
    jvm_follower_recovery.container = container;
    jvm_follower_recovery.bootstrap_all = bootstrap_all;
    jvm_follower_recovery.prefix_leo = prefix_leo;
}
#[rstest_bdd_macros::when("the promoted JVM leader appends divergent payloads {records}")]
async fn write_jvm_divergence(jvm_follower_recovery: &mut JvmFollowerRecovery, records: String) {
    let records: Vec<String> = serde_json::from_str(&records).expect("JVM divergent payloads");
    let cluster = jvm_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster");
    let c1 = &cluster.krabka[0].0;
    let bootstrap_all = &jvm_follower_recovery.bootstrap_all;
    let container = &jvm_follower_recovery.container;
    let prefix_leo = jvm_follower_recovery.prefix_leo;
    let pr = {
        // Wait for the partition to materialize in the Krabka leader's image.
        c1.wait_until_partition_present(TOPIC, 0).await;
        c1.partition_record_for_test(TOPIC, 0)
            .expect("partition record present after wait")
    };
    eprintln!("KRABKA[kip320] partition before divergence: {pr:?}");

    let jvm_epoch = LeaderEpoch(pr.leader_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(
        single_leader_record()
            .previous(&pr)
            .topic(TOPIC)
            .leader(3)
            .epoch(jvm_epoch)
            .call(),
    ))
    .await
    .expect("promote JVM broker for divergent suffix");
    wait_for_described_leader(bootstrap_all, TOPIC, 3, LEADER_WAIT).await;

    produce_lines_via_jvm(bootstrap_all, TOPIC, &records);
    c1.wait_until_local_log_end_offset(
        TOPIC,
        0,
        prefix_leo + i64::try_from(records.len()).expect("payload count"),
    )
    .await;
    let jvm_before = dump_log_in_container(container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));

    jvm_follower_recovery.partition = Some(pr);
    jvm_follower_recovery.jvm_epoch = Some(jvm_epoch);
    jvm_follower_recovery.before_dump = Some(jvm_before);
}
#[rstest_bdd_macros::then("the JVM on-disk log contains divergent payload {payload}")]
fn original_jvm_suffix(jvm_follower_recovery: &JvmFollowerRecovery, payload: String) {
    let dump = jvm_follower_recovery
        .before_dump
        .as_ref()
        .expect("original JVM dump");
    assert2::assert!(dump.contains(&payload), "JVM suffix missing: {dump}");
}
#[rstest_bdd_macros::when(
    "the paused JVM is isolated behind phantom leader 99 and Krabka replaces its suffix with {count} native records"
)]
async fn replace_jvm_suffix(jvm_follower_recovery: &mut JvmFollowerRecovery, count: usize) {
    let cluster = jvm_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster");
    let c1 = &cluster.krabka[0].0;
    let container = &jvm_follower_recovery.container;
    let prefix_leo = jvm_follower_recovery.prefix_leo;
    let pr = jvm_follower_recovery
        .partition
        .as_ref()
        .expect("partition metadata");
    let jvm_epoch = jvm_follower_recovery.jvm_epoch.expect("JVM epoch");
    set_container_paused(container, true);

    // Take the partition offline behind a dead phantom leader (id 99). Keep
    // the assignment and directory vector intact so this record changes only
    // leadership/epoch state.
    let parked_epoch = LeaderEpoch(jvm_epoch.0 + 1);
    let forged = MetadataRecord::V1Partition(
        single_leader_record()
            .previous(pr)
            .topic(TOPIC)
            .leader(99)
            .epoch(parked_epoch)
            .partition_epoch_delta(2)
            .call(),
    );
    c1.submit_metadata_record_for_test(forged)
        .await
        .expect("inject dead-leader PartitionRecord");
    let parked_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if c1.partition_record_for_test(TOPIC, 0).is_some_and(|p| {
            p.leader == krabka_broker::NodeId(99) && p.leader_epoch == parked_epoch
        }) {
            break;
        }
        assert2::assert!(
            Instant::now() <= parked_deadline,
            "dead-leader metadata did not apply before divergent rewrite"
        );
        time::sleep(Duration::from_millis(100)).await;
    }

    // Remove the JVM suffix from broker 1, then append the Krabka suffix at the
    // parked epoch. The test append helper stamps that current epoch on every
    // batch, giving KIP-320 a real epoch boundary at prefix_leo.
    c1.test_truncate_local_log(TOPIC, 0, prefix_leo)
        .await
        .expect("truncate Krabka copy of JVM suffix");
    let krabka_leo_before = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    c1.produce_records_for_test(TOPIC, 0, count)
        .await
        .expect("append divergent suffix on Krabka leader");
    let krabka_leo_after = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "KRABKA[kip320] Krabka leader LEO {krabka_leo_before} -> {krabka_leo_after} (divergent suffix)"
    );

    jvm_follower_recovery.parked_epoch = Some(parked_epoch);
    jvm_follower_recovery.before_rewrite = Some(krabka_leo_before);
    jvm_follower_recovery.after_rewrite = Some(krabka_leo_after);
}
#[rstest_bdd_macros::then(
    "the Krabka rewrite starts at the shared prefix and adds exactly {count} offsets"
)]
fn replaced_suffix_bounds(jvm_follower_recovery: &JvmFollowerRecovery, count: i64) {
    assert2::assert!(
        jvm_follower_recovery.before_rewrite.expect("rewrite start")
            == jvm_follower_recovery.prefix_leo
    );
    assert2::assert!(
        jvm_follower_recovery.after_rewrite.expect("rewrite end")
            == jvm_follower_recovery.prefix_leo + count
    );
}
#[rstest_bdd_macros::when(
    "Krabka becomes leader in the next epoch and the resumed JVM follower recovers"
)]
async fn recover_jvm_follower(jvm_follower_recovery: &mut JvmFollowerRecovery) {
    let cluster = jvm_follower_recovery
        .cluster
        .as_ref()
        .expect("mixed cluster");
    let c1 = &cluster.krabka[0].0;
    let container = &jvm_follower_recovery.container;
    let bootstrap_all = &jvm_follower_recovery.bootstrap_all;
    let pr = jvm_follower_recovery
        .partition
        .as_ref()
        .expect("partition metadata");
    let parked_epoch = jvm_follower_recovery.parked_epoch.expect("parked epoch");
    let restore = MetadataRecord::V1Partition(
        single_leader_record()
            .previous(pr)
            .topic(TOPIC)
            .epoch(parked_epoch.0 + 1)
            .partition_epoch_delta(3)
            .call(),
    );
    c1.submit_metadata_record_for_test(restore)
        .await
        .expect("restore Krabka leader");

    set_container_paused(container, false);

    wait_for_described_leader(bootstrap_all, TOPIC, 1, LEADER_WAIT).await;

    // 5. Poll the JVM broker's actual on-disk bytes until its old suffix is
    //    gone and the Krabka suffix is present. Equal LEOs alone cannot prove
    //    truncation because both divergent tails contain four records.
    let convergence_deadline = Instant::now() + Duration::from_mins(1);
    let jvm_dump = loop {
        let dump = dump_log_in_container(container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
        if dump.contains("test-record-3") && !dump.contains("jvm-divergent-") {
            break dump;
        }
        assert2::assert!(
            Instant::now() <= convergence_deadline,
            "JVM follower retained its divergent suffix after KIP-320 recovery:\n{dump}"
        );
        time::sleep(Duration::from_millis(500)).await;
    };

    jvm_follower_recovery.recovered_dump = Some(jvm_dump);
}
#[rstest_bdd_macros::then(
    "the JVM log contains {authoritative} and excludes {divergent} at the Krabka leader's exact end offset"
)]
fn recovered_jvm_suffix(
    jvm_follower_recovery: &JvmFollowerRecovery,
    authoritative: String,
    divergent: String,
) {
    let dump = jvm_follower_recovery
        .recovered_dump
        .as_ref()
        .expect("recovered JVM dump");
    eprintln!(
        "KRABKA[kip320] jvm dump baseOffset lines:\n{}",
        grep_base_offsets(dump)
    );
    assert2::assert!(
        dump.contains(&authoritative) && !dump.contains(&divergent),
        "JVM divergent suffix remains: {dump}"
    );
    assert2::assert!(
        max_offset_in_dump(dump)
            == Some(jvm_follower_recovery.after_rewrite.expect("leader LEO") - 1)
    );
}
#[rstest_bdd_macros::when(
    "Kafka console consumer reads from partition zero's beginning after recovery"
)]
fn consume_recovered_jvm(jvm_follower_recovery: &mut JvmFollowerRecovery) {
    let bootstrap_all = &jvm_follower_recovery.bootstrap_all;
    let consume = docker_run_kafka_tool_with_image(KAFKA_IMAGE, &[
        "kafka-console-consumer",
        "--bootstrap-server",
        bootstrap_all,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "20000",
    ]);

    jvm_follower_recovery.consumer = Some(consume);
}
#[rstest_bdd_macros::then(
    "the consumer returns at least {minimum} nonempty records without failures {failures}"
)]
fn recovered_jvm_consumption(
    jvm_follower_recovery: &JvmFollowerRecovery,
    minimum: usize,
    failures: String,
) {
    let failures: Vec<String> = serde_json::from_str(&failures).expect("fatal consumer failures");
    let output = jvm_follower_recovery
        .consumer
        .as_ref()
        .expect("consumer executed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for failure in failures {
        assert2::assert!(!stderr.contains(&failure), "consumer fatal error: {stderr}");
    }
    assert2::assert!(
        stdout
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
            >= minimum,
        "consumer returned no records: {stdout} {stderr}"
    );
}
#[rstest_bdd_macros::then("the JVM divergence cluster is shut down")]
async fn finish_jvm_recovery(jvm_follower_recovery: &mut JvmFollowerRecovery) {
    jvm_follower_recovery
        .cluster
        .take()
        .expect("mixed cluster")
        .shutdown()
        .await;
}
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_kip320_divergence/jvm_follower_truncation.feature",
    name = "JVM truncates an equal-length divergent suffix and resumes consumption"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kip320_jvm_follower_truncates_from_krabka_leader(
    jvm_follower_recovery: JvmFollowerRecovery,
) {
    let _ = jvm_follower_recovery;
}
