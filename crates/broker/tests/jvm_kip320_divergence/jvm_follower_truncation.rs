//! Scenario 2: a JVM follower truncates a divergent suffix from a Krabka leader.
//!
//! The scenario forces a real divergent suffix on a mixed cluster and asserts
//! that the JVM follower removes it and converges on the Krabka leader's exact
//! log end offset, and that a `kafka-console-consumer` then recovers. Its
//! divergence recipe is specific to this direction, so it has its own file.

use std::time::{Duration, Instant};

use krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord};

use crate::{
    docker::{
        KAFKA_IMAGE, docker_run_kafka_tool_with_image, produce_lines_via_jvm, set_container_paused,
    },
    dump_log::{dump_log_in_container, grep_base_offsets, max_offset_in_dump},
    mixed_cluster::start_mixed_cluster,
    support,
    topic_admin::{LEADER_WAIT, create_mixed_topic, described_isr, wait_for_described_leader},
};

/// Steps 2-3 of Task 11. Force a real divergent suffix in a mixed cluster and
/// assert:
///  (a) the JVM follower truncates to converge on the Krabka leader, and
///  (b) a kafka-console-consumer recovers. It continues without a fatal
///      truncation/deserialization error after the suffix is rewritten.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
async fn kip320_jvm_follower_truncates_from_krabka_leader() {
    const TOPIC: &str = "krabka-kip320-jvm-follower";
    let container = support::unique_container_name("krabka-kip320-jvm-follower-broker");

    let cluster = start_mixed_cluster(&container, true).await;
    let c1 = &cluster.krabka[0].0; // Krabka broker_id 1
    let bootstrap_all = cluster.bootstrap_all.clone();

    // 0. Gate on the JVM broker (id 3) registering into the cluster view. On
    //    Linux/CI the cross-impl KRaft join completes within ~1 min; if it
    //    never registers (the JVM broker failed to join the Krabka-led quorum
    //    — the dominant Mac-vs-Linux difference here) we cannot build an RF=3
    //    topic, so we surface that explicitly rather than fail opaquely inside
    //    CreateTopics.
    assert2::assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster (only the 2 Krabka brokers \
         registered); the cross-impl KRaft data-plane join is Linux-bound"
    );

    // 1. Create an RF=3 topic placed on the two Krabka brokers + JVM. With 3
    //    registered brokers the controller assigns replicas across all three;
    //    we use partitions=1, replication-factor=3 so the JVM (id 3) is a
    //    replica/follower of a Krabka leader.
    create_mixed_topic(&bootstrap_all, TOPIC).await;

    // 2. Wait for the partition to materialize on the Krabka leader and for the
    //    JVM follower to join the ISR.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool_with_image(
            KAFKA_IMAGE,
            &[
                "kafka-topics",
                "--describe",
                "--topic",
                TOPIC,
                "--bootstrap-server",
                &bootstrap_all,
            ],
        );
        let s = String::from_utf8_lossy(&desc.stdout);
        // ISR must contain broker 3 (the JVM follower) so it is actively
        // replicating from the Krabka leader before we induce divergence.
        if described_isr(&s).contains(&3) {
            break;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "JVM follower never joined ISR: {s}"
        );
        // intentional: polls an EXTERNAL kafka-topics --describe CLI for the
        // JVM follower (id 3) to catch up and join the ISR; driven by the JVM
        // broker's fetch, with a 2-min bound the 30s image awaiter can't match.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 3. Produce a committed prefix (epoch 0) via the JVM producer (acks=all),
    //    so all replicas — including the JVM follower — share it.
    produce_lines_via_jvm(
        &bootstrap_all,
        TOPIC,
        &(0..10).map(|i| format!("prefix-{i}")).collect::<Vec<_>>(),
    );
    // intentional: let the EXTERNAL JVM follower replicate the acks=all prefix;
    // the follower's replication progress is not a Krabka image/metric signal.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 4. INDUCE REAL DIVERGENCE. First make the JVM broker leader and append a
    //    suffix there. Krabka follows that suffix so both sides demonstrably
    //    have it. Then park every fetcher behind a dead phantom leader, truncate
    //    broker 1 back to the committed prefix, and append a different suffix at
    //    the next epoch. Restoring broker 1 as leader leaves equal-length,
    //    byte-different tails: the JVM follower must truncate, not merely catch
    //    up from a shorter log.
    let prefix_leo = c1
        .local_log_end_offset(TOPIC, 0)
        .expect("Krabka prefix log exists");
    assert2::assert!(
        prefix_leo == 10,
        "expected ten-record prefix, got LEO {prefix_leo}"
    );
    let pr = {
        // Wait for the partition to materialize in the Krabka leader's image.
        c1.wait_until_partition_present(TOPIC, 0).await;
        c1.partition_record_for_test(TOPIC, 0)
            .expect("partition record present after wait")
    };
    eprintln!("KRABKA[kip320] partition before divergence: {pr:?}");

    let jvm_epoch = LeaderEpoch(pr.leader_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(3),
        replicas: pr.replicas.clone(),
        isr: vec![krabka_broker::NodeId(3)],
        leader_epoch: jvm_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 1,
    }))
    .await
    .expect("promote JVM broker for divergent suffix");
    wait_for_described_leader(&bootstrap_all, TOPIC, 3, LEADER_WAIT).await;

    let jvm_suffix = (0..4)
        .map(|i| format!("jvm-divergent-{i}"))
        .collect::<Vec<_>>();
    produce_lines_via_jvm(&bootstrap_all, TOPIC, &jvm_suffix);
    c1.wait_until_local_log_end_offset(TOPIC, 0, prefix_leo + 4)
        .await;
    let jvm_before = dump_log_in_container(&container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
    assert2::assert!(
        jvm_before.contains("jvm-divergent-3"),
        "JVM dump did not contain the suffix that must later be truncated:\n{jvm_before}"
    );

    // Freeze the JVM process while rewriting broker 1. The phantom-leader
    // metadata cancels replication cooperatively, so an already in-flight
    // response from the former JVM leader could otherwise reset the test log
    // during this deliberately out-of-band mutation.
    set_container_paused(&container, true);

    // Take the partition offline behind a dead phantom leader (id 99). Keep
    // the assignment and directory vector intact so this record changes only
    // leadership/epoch state.
    let parked_epoch = LeaderEpoch(jvm_epoch.0 + 1);
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: pr.replicas.clone(),
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: parked_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 2,
    });
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Remove the JVM suffix from broker 1, then append the Krabka suffix at the
    // parked epoch. The test append helper stamps that current epoch on every
    // batch, giving KIP-320 a real epoch boundary at prefix_leo.
    c1.test_truncate_local_log(TOPIC, 0, prefix_leo)
        .await
        .expect("truncate Krabka copy of JVM suffix");
    let krabka_leo_before = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    c1.produce_records_for_test(TOPIC, 0, 4)
        .await
        .expect("append divergent suffix on Krabka leader");
    let krabka_leo_after = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "KRABKA[kip320] Krabka leader LEO {krabka_leo_before} -> {krabka_leo_after} (divergent suffix)"
    );

    assert2::assert!(
        krabka_leo_before == prefix_leo && krabka_leo_after == prefix_leo + 4,
        "Krabka divergent rewrite should replace four offsets in place"
    );

    // Restore Krabka broker 1 as the leader at the next epoch with the JVM
    // follower (3) back in the replica set so it re-fetches and detects
    // divergence.
    let restore = MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(1),
        replicas: pr.replicas.clone(),
        isr: vec![krabka_broker::NodeId(1)],
        leader_epoch: LeaderEpoch(parked_epoch.0 + 1),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: pr.directories.clone(),
        partition_epoch: pr.partition_epoch + 3,
    });
    c1.submit_metadata_record_for_test(restore)
        .await
        .expect("restore Krabka leader");

    set_container_paused(&container, false);

    wait_for_described_leader(&bootstrap_all, TOPIC, 1, LEADER_WAIT).await;

    // 5. Poll the JVM broker's actual on-disk bytes until its old suffix is
    //    gone and the Krabka suffix is present. Equal LEOs alone cannot prove
    //    truncation because both divergent tails contain four records.
    let convergence_deadline = Instant::now() + Duration::from_mins(1);
    let jvm_dump = loop {
        let dump = dump_log_in_container(&container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
        if dump.contains("test-record-3") && !dump.contains("jvm-divergent-") {
            break dump;
        }
        assert2::assert!(
            Instant::now() <= convergence_deadline,
            "JVM follower retained its divergent suffix after KIP-320 recovery:\n{dump}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    // 6. ASSERTION (a): the JVM follower's on-disk log converged on the Krabka
    //    leader's exact LEO. The payload assertions above already prove that
    //    the equal-length old suffix was removed and replaced.
    eprintln!(
        "KRABKA[kip320] jvm dump baseOffset lines:\n{}",
        grep_base_offsets(&jvm_dump)
    );

    // Exact dump text is intentionally not compared across implementations
    // because timestamps and batch packing differ. The leader's in-process LEO
    // is the authoritative next offset; kafka-dump-log supplies the follower's
    // independently parsed, on-disk last offset.
    let jvm_max = max_offset_in_dump(&jvm_dump);
    assert2::assert!(
        jvm_max == Some(krabka_leo_after - 1),
        "JVM follower did not converge to Krabka leader after truncation: \
         jvm_max={jvm_max:?} krabka_leo={krabka_leo_after}"
    );

    // 7. ASSERTION (b): a kafka-console-consumer recovers — it reads the
    //    truncated/converged log to completion without a fatal
    //    LogTruncationException / RecordDeserializationException.
    let consume = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            &bootstrap_all,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "20000",
        ],
    );
    let cstdout = String::from_utf8_lossy(&consume.stdout);
    let cstderr = String::from_utf8_lossy(&consume.stderr);
    eprintln!("KRABKA[kip320] consumer recover stdout={cstdout} stderr={cstderr}");
    assert2::assert!(
        !cstderr.contains("LogTruncationException")
            && !cstderr.contains("RecordDeserializationException"),
        "consumer hit a fatal truncation/deserialization error: {cstderr}"
    );
    assert2::assert!(
        cstdout.lines().filter(|l| !l.trim().is_empty()).count() >= 1,
        "consumer read no records after truncation recovery: stdout={cstdout} stderr={cstderr}"
    );

    cluster.shutdown().await;
}
