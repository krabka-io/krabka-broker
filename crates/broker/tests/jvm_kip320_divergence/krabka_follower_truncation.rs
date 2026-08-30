//! Scenario 3: a Krabka follower truncates a divergent suffix from a JVM leader.
//!
//! This is the reverse direction of scenario 2. The scenario parks replication
//! behind a phantom leader, appends a Krabka-only suffix, then promotes the JVM
//! replica, and asserts that the Krabka follower truncates to the shared prefix
//! and resumes at the JVM leader's exact log end offset.

use std::time::{Duration, Instant};

use krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord};

use crate::{
    docker::{
        KAFKA_IMAGE, docker_run_kafka_tool_with_image, produce_lines_via_jvm, set_container_paused,
    },
    dump_log::{dump_log_in_container, max_offset_in_dump},
    mixed_cluster::start_mixed_cluster,
    support,
    topic_admin::{LEADER_WAIT, create_mixed_topic, described_isr, wait_for_described_leader},
};

/// Step 2 of Task 11, reverse direction. A Krabka follower replicates from a
/// JVM leader. The test parks replication behind a phantom leader, appends a
/// suffix only to the Krabka replica at a new epoch, and then promotes the JVM
/// replica. It asserts that the Krabka follower observes the JVM leader's
/// `diverging_epoch`, truncates to their shared prefix, and subsequently copies
/// a fresh JVM-authored suffix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
async fn kip320_krabka_follower_truncates_from_jvm_leader() {
    const TOPIC: &str = "krabka-kip320-krabka-follower";
    let container = support::unique_container_name("krabka-kip320-krabka-follower-broker");

    let cluster = start_mixed_cluster(&container, true).await;
    let c1 = &cluster.krabka[0].0;
    let bootstrap_all = cluster.bootstrap_all.clone();

    // 0. Gate on the JVM broker registering (see scenario 2); RF=3 needs all
    //    three brokers in the cluster view. Linux-bound.
    assert2::assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster; cross-impl KRaft join is Linux-bound"
    );

    // 1. Create the topic and wait for replicas to converge across all three
    //    brokers.
    create_mixed_topic(&bootstrap_all, TOPIC).await;

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
        let isr = described_isr(&s);
        if isr.contains(&1) && isr.contains(&3) {
            break;
        }
        assert2::assert!(Instant::now() <= deadline, "replicas never converged: {s}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 2. Produce a committed prefix via the JVM producer (acks=all) so the
    //    Krabka follower shares it.
    produce_lines_via_jvm(
        &bootstrap_all,
        TOPIC,
        &(0..8).map(|i| format!("rev-{i}")).collect::<Vec<_>>(),
    );
    // intentional: let the EXTERNAL JVM producer/replication settle so the
    // Krabka follower shares the prefix; no Krabka image/metric signal for it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 3. Park replication behind a phantom leader before appending the
    //    Krabka-only suffix. This makes the divergent state deterministic:
    //    neither the JVM replica nor broker 2 can copy the forged records.
    let prefix_leo = c1
        .local_log_end_offset(TOPIC, 0)
        .expect("Krabka prefix log exists");
    assert2::assert!(
        prefix_leo == 8,
        "expected eight-record prefix, got LEO {prefix_leo}"
    );
    c1.wait_until_partition_present(TOPIC, 0).await;
    let partition = c1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record present after wait");
    let parked_epoch = LeaderEpoch(partition.leader_epoch.0 + 1);

    // Freeze the JVM replica before parking replication. Otherwise it can
    // copy part of the deliberately forged suffix while the phantom-leader
    // metadata is still propagating, making its authoritative prefix longer.
    set_container_paused(&container, true);

    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(99),
        replicas: partition.replicas.clone(),
        isr: vec![krabka_broker::NodeId(99)],
        leader_epoch: parked_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: partition.directories.clone(),
        partition_epoch: partition.partition_epoch + 1,
    }))
    .await
    .expect("park reverse-direction replicas behind phantom leader");
    c1.wait_until_local_partition_target(TOPIC, 0, krabka_broker::NodeId(99), parked_epoch)
        .await;

    c1.produce_records_for_test(TOPIC, 0, 5)
        .await
        .expect("append divergent suffix on parked Krabka replica");
    let krabka_leo_diverged = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "KRABKA[kip320] reverse: Krabka replica LEO {prefix_leo} -> {krabka_leo_diverged} (forced divergent suffix)"
    );
    assert2::assert!(
        krabka_leo_diverged == prefix_leo + 5,
        "Krabka-only divergent suffix should add five records"
    );

    // 4. Promote the JVM replica at the next epoch. Its log still ends at the
    //    shared prefix, so the Krabka follower must truncate before fetching.
    let jvm_epoch = LeaderEpoch(parked_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
        topic: TOPIC.to_string(),
        partition: 0,
        leader: krabka_broker::NodeId(3),
        replicas: partition.replicas.clone(),
        isr: vec![krabka_broker::NodeId(3)],
        leader_epoch: jvm_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: partition.directories.clone(),
        partition_epoch: partition.partition_epoch + 2,
    }))
    .await
    .expect("promote JVM broker for reverse-direction recovery");

    set_container_paused(&container, false);

    wait_for_described_leader(&bootstrap_all, TOPIC, 3, LEADER_WAIT).await;

    // 5. Observe the truncation itself, before adding any new leader records.
    //    Equal final LEOs alone would not distinguish truncate-and-refetch from
    //    leaving the bogus suffix in place.
    let dl = Instant::now() + Duration::from_secs(45);
    let mut final_leo = krabka_leo_diverged;
    loop {
        final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(final_leo);
        if final_leo == prefix_leo {
            break;
        }
        assert2::assert!(
            Instant::now() <= dl,
            "Krabka follower did not truncate to JVM prefix LEO {prefix_leo}; current LEO={final_leo}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let jvm_prefix_dump =
        dump_log_in_container(&container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
    assert2::assert!(
        max_offset_in_dump(&jvm_prefix_dump) == Some(prefix_leo - 1),
        "JVM leader should retain exactly the shared prefix:\n{jvm_prefix_dump}"
    );
    assert2::assert!(
        !jvm_prefix_dump.contains("test-record-"),
        "Krabka-only divergent suffix leaked to JVM leader:\n{jvm_prefix_dump}"
    );

    // 6. Prove that replication resumes from the truncated boundary by writing
    //    a shorter, JVM-authored suffix and waiting for Krabka's exact LEO.
    let authoritative = (0..3)
        .map(|i| format!("jvm-authoritative-{i}"))
        .collect::<Vec<_>>();
    produce_lines_via_jvm(&bootstrap_all, TOPIC, &authoritative);
    c1.wait_until_local_log_end_offset(TOPIC, 0, prefix_leo + 3)
        .await;
    final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    assert2::assert!(
        final_leo == prefix_leo + 3,
        "Krabka follower did not resume at the JVM leader's exact LEO"
    );
    eprintln!(
        "KRABKA[kip320] reverse: truncated from {krabka_leo_diverged} to {prefix_leo}, then followed JVM to {final_leo}"
    );

    cluster.shutdown().await;
}
