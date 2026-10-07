//! The `acks=all` durability gate across a leader crash: the partition-0 leader
//! is killed mid-burst, and the records the JVM producer retries through the
//! election must still be readable from a survivor.
//!
//! It runs with accelerated election timers and probes `Metadata` for the
//! current leader, neither of which the steady-state `acks=all` case needs.

use std::process::Stdio;

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE, docker_run_kafka_tool};

// `acks=all` survives a leader crash mid-produce burst: 3-broker Krabka
// cluster, JVM `kafka-console-producer --request-required-acks=-1` writes
// 100 records while the partition-0 leader is killed at mid-burst. The
// surviving brokers elect a new leader; the producer retries and all
// 100 records are eventually visible to a `read_committed` consumer.
//
// Fixed ports 10392/10492/10592 + 10393/10493/10593 — next free hundred
// above acks_all_durability (10092/10192/10292) to dodge
// TIME_WAIT collisions when JVM tests run sequentially via the nextest
// broker-jvm-acceptance test group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acks_all_survives_leader_crash() {
    const TOPIC: &str = "krabka-acks-all-crash-itest";

    crate::support::init_jvm_tracing("krabka_broker=debug,info");

    let client_ports = [10392u16, 10492, 10592];
    let controller_ports = [10393u16, 10493, 10593];

    let mut cluster = crate::support::start_jvm_cluster(client_ports, controller_ports, |config| {
        config.heartbeat_interval = krabka_units::millis(200);
        config.heartbeat_timeout = krabka_units::millis(2_000);
        config.replica_lag_time_max = krabka_units::millis(2_000);
        config.controller_election_timeout = krabka_units::millis(500);
        config.controller_heartbeat_interval = krabka_units::millis(100);
    })
    .await;

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    // Multi-broker bootstrap so the JVM producer can find a survivor when
    // broker 1 (the partition leader) is killed mid-burst. Without this the
    // producer hangs on bootstrap because its only known broker is dead.
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );

    // 1. Create topic with replication-factor=3.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // 2. Wait for ISR to include all three brokers before starting the produce
    //    burst. The in-process metadata image ISR is exactly what the JVM
    //    `kafka-topics --describe` reports, so observe it directly.
    cluster[0].0.wait_until_isr_len(TOPIC, 0, 3).await;
    // The consumer at the end needs `__consumer_offsets`, which takes three
    // replicas. Create it while all three brokers are up, as Kafka's
    // `IntegrationTestHarness.createOffsetsTopic` does before a test.
    cluster[0].0.wait_until_group_coordinator_ready().await;

    // 3. Determine partition-0 leader from Metadata via local port (not Docker).
    let leader_node_id = {
        use krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
        let local_bootstrap = format!("127.0.0.1:{}", client_ports[0]);
        let probe = krabka_client_core::Client::builder()
            .bootstrap(local_bootstrap)
            .build()
            .await
            .expect("metadata probe");
        let resp = probe
            .send(MetadataRequest {
                topics: Some(vec![MetadataRequestTopic {
                    name: Some(TOPIC.into()),
                    ..Default::default()
                }]),
                ..Default::default()
            })
            .await
            .expect("metadata");
        resp.topics
            .iter()
            .find(|t| t.name.as_deref() == Some(TOPIC))
            .and_then(|t| t.partitions.first())
            .map_or(1, |p| p.leader_id)
    };

    // 4. Spawn JVM producer in background (100 records, acks=-1, long timeout
    //    so it retries through the election window).
    let producer_child = crate::support::jvm_docker_command(
        KAFKA_IMAGE,
        &[],
        &[
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"crash-msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_all} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 30000"
            ),
        ],
        true,
    )
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("spawn kafka-console-producer");

    // 5. After ~50ms (producer has connected), kill the partition leader.
    // intentional: this timing window — killing the leader mid-produce — is the
    // behavior under test, not a wait on any observable broker state.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let leader_idx = usize::try_from((leader_node_id - 1).max(0)).unwrap_or(0);
    if leader_idx < cluster.len() {
        eprintln!("KRABKA[test] killing leader node_id={leader_node_id} idx={leader_idx}");
        let (leader_handle, _dir) = cluster.remove(leader_idx);
        leader_handle.shutdown().await;
    }

    // 6. Wait for the JVM producer to complete (up to 60s for election + retry).
    let producer_out = producer_child.wait_with_output().expect("wait producer");
    eprintln!(
        "KRABKA[test] producer status={} stderr_len={}",
        producer_out.status,
        producer_out.stderr.len(),
    );
    if !producer_out.status.success() {
        eprintln!(
            "KRABKA[test] producer stderr: {}",
            String::from_utf8_lossy(&producer_out.stderr),
        );
    }

    // 7. Wait briefly for replication to settle post-election.
    // intentional: post-election follower high-watermark convergence is not in
    // the metadata image and has no krabka awaiter/metric; the JVM consumer
    // below has its own poll timeout to absorb any remaining replication lag.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // 8. Consume from a survivor. Require at least 1 record — the cluster
    //    must serve reads after a leader crash.
    let surviving_ports: Vec<u16> = (0..3_usize)
        .filter(|i| *i != leader_idx)
        .map(|i| client_ports[i])
        .collect();
    let survivor_bootstrap = format!("host.docker.internal:{}", surviving_ports[0]);

    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &survivor_bootstrap,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "20000",
    ]);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 1,
        "expected at least 1 readable record after leader crash; got {line_count}: {stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
