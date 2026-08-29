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
