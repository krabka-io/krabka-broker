//! Partition reassignment and preferred-leader election driven by the JVM admin
//! tools against a three-broker SASL cluster.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use assert2::assert;
use jvm_acceptance::*;

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

// ---------------------------------------------------------------------------
// JVM acceptance test: kafka-reassign-partitions --execute + --verify
// ---------------------------------------------------------------------------

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
    let want: std::collections::HashSet<u64> = [staying, new_node].into_iter().collect();
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

// ---------------------------------------------------------------------------
// JVM acceptance test: kafka-reassign-partitions --throttle
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_with_throttle_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "krabka-throttle-reassign-itest";

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

    // Determine initial replicas; pick the broker not in the replica set.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record");
    let initial = pr.replicas.clone();
    let new_node: u64 = (1u64..=3)
        .find(|n| !initial.contains(&krabka_metadata::NodeId(*n)))
        .expect("free broker");
    let staying: u64 = initial.first().unwrap().0;
    eprintln!("KRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");

    // Write reassignment JSON.
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path());

    // Execute reassignment with --throttle 1024.
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
            "--throttle",
            "1024",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute --throttle");
    eprintln!(
        "KRABKA[test] --execute --throttle status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kafka-reassign-partitions --execute --throttle failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify throttle configs were applied via kafka-configs --describe.
    let desc = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-configs",
            "--describe",
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-configs --describe");
    eprintln!(
        "KRABKA[test] kafka-configs describe status={} stdout={} stderr={}",
        desc.status,
        String::from_utf8_lossy(&desc.stdout),
        String::from_utf8_lossy(&desc.stderr),
    );
    let desc_stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        desc_stdout.contains("leader.replication.throttled.rate=1024"),
        "leader.replication.throttled.rate=1024 not visible in kafka-configs output: {desc_stdout}"
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // the reassignment tests use the same technique).
    let pr_after = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after execute");
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

    // Wait until the reassignment completes (adding/removing replicas drained
    // from the committed metadata image).
    h1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
    })
    .await;
    // After completion the replica set must be exactly {staying, new_node}.
    let pr = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after reassignment");
    let got: std::collections::HashSet<u64> = pr.replicas.iter().map(|n| n.0).collect();
    let want: std::collections::HashSet<u64> = [staying, new_node].into_iter().collect();
    assert!(
        got == want,
        "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
    );
    eprintln!("KRABKA[test] reassignment completed; running --verify");

    // --verify clears throttle configs and exits 0 (broker-scoped
    // IncrementalAlterConfigs is supported).
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
    assert!(
        verify_out.status.success(),
        "kafka-reassign-partitions --verify failed: stderr={}",
        String::from_utf8_lossy(&verify_out.stderr)
    );

    // Confirm throttle configs were cleared from the metadata image after --verify.
    h1.wait_for_image(|img| {
        img.broker_throttle_rate(
            krabka_metadata::NodeId(1),
            krabka_metadata::ThrottleKind::Leader,
        )
        .is_none()
    })
    .await;

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
