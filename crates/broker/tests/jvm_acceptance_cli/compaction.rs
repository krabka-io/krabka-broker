//! `kafka-console-consumer` over a compacted topic, which must see only the
//! latest value per key.
//!
//! This suite boots its own broker with a three-second cleaner interval instead
//! of the shared `start_host_broker` harness, so it carries the `BrokerConfig`
//! that no other file in this suite needs.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, broker0_listen, controller_addr_0, docker_run_kafka_tool,
    nc_check_connectivity,
};

/// `kafka-console-consumer` sees a compacted topic with only
/// the latest value per key.
///
/// 1. Spin up a single-broker cluster with a fast cleaner interval (3s).
/// 2. `kafka-topics --create --topic compacted-jvm --config cleanup.policy=compact
///    --config segment.bytes=256 --partitions 1 --replication-factor 1`
/// 3. `kafka-console-producer --property parse.key=true --property key.separator=:`
///    with this stdin:
///      k1:v1
///      k1:v2
///      k2:v3
///      k1:v4
///      k3:v5
/// 4. Sleep 8s to let the 3s cleaner tick and the segment rolls happen.
/// 5. `kafka-console-consumer --topic compacted-jvm --from-beginning --timeout-ms 5000`
/// 6. Assert stdout contains `v4`, `v3`, `v5` (latest per-key values).
/// 7. Assert stdout does NOT contain `v1` or `v2` (stale values compacted away).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_console_consumer_sees_compacted_topic_end_to_end() {
    const TOPIC: &str = "compacted-jvm";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("static addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: broker0_advertised().into(),
        log_dir: dir.path().to_path_buf(),
        log_config: krabka_log::LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(krabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        // 3s cleaner tick so we don't have to wait the full 30s default.
        cleaner_interval_override: Some(krabka_units::secs(3)),
        ..BrokerConfig::default()
    };
    let broker = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] compaction broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    nc_check_connectivity();

    // 1. Create the topic with cleanup.policy=compact and tiny segment.bytes
    //    so records are sealed into a second segment before the cleaner runs.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--config",
        "cleanup.policy=compact",
        "--config",
        "segment.bytes=256",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // 1b. Wait for cleanup.policy=compact + segment.bytes=256 to propagate
    //     from the metadata image into the partition's LogConfig via the
    //     ReplicatorSupervisor reconcile loop. Without this wait, produces
    //     can land in a default-config Log (1GiB segments, Delete policy) →
    //     no segment rolls, no compaction.
    let cfg_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Some(cfg) = broker.partition_log_config_for_test(TOPIC, 0)
            && cfg.cleanup_policy == krabka_log::CleanupPolicy::Compact
            && cfg.segment_size == krabka_units::bytes(256)
        {
            break;
        }
        assert!(
            std::time::Instant::now() <= cfg_deadline,
            "cleanup.policy/segment.bytes never propagated within 10s"
        );
        // intentional: bounded poll of the local reconciled LogConfig override;
        // `partition_log_config_for_test` is not surfaced by any awaiter/metric.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // 2. Produce 5 records under 3 keys — k1 has three values (v1, v2, v4);
    //    only v4 should survive compaction.
    let mut child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
            // Force per-record batches so each line is its own RecordBatch.
            // Default linger.ms=0 already, but batch.size+linger.ms keep
            // multiple in-flight records bundled when they're submitted
            // back-to-back. Setting batch.size=1 and max-in-flight=1 makes
            // each line a separate batch, which is what we need so
            // segment.bytes=256 actually rolls segments mid-workload.
            "--producer-property",
            "batch.size=1",
            "--producer-property",
            "linger.ms=0",
            "--producer-property",
            "max.in.flight.requests.per.connection=1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn producer");
    // First 5 records: the actual workload. After that, a burst of "pad"
    // records under a sentinel key forces the active segment past
    // `segment.bytes=256` so v5 ends up sealed (otherwise the compactor
    // can't see it; it never touches the active segment) and the test's
    // "no stale v1" assertion can actually hold for k1.
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(
            b"k1:v1\nk1:v2\nk2:v3\nk1:v4\nk3:v5\n\
              __pad__:p0\n__pad__:p1\n__pad__:p2\n__pad__:p3\n\
              __pad__:p4\n__pad__:p5\n__pad__:p6\n__pad__:p7\n",
        )
        .expect("write stdin");
    drop(child.stdin.take());
    let producer_out = child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );
    eprintln!("KRABKA[test] produced 5 records; waiting for cleaner to compact...");

    // 3. Wait until the cleaner completes at least two compaction passes over
    //    this partition *after* the records landed (per-partition counter
    //    bumped once per sweep), so a sweep that was in-flight when the segment
    //    sealed can't be mistaken for one that saw the new records. This
    //    guarantees the stale k1 values have been compacted away.
    let compactions_before = broker
        .metrics()
        .log_compactions_total
        .get_or_create(&krabka_broker::metrics::PartitionLabel {
            topic: TOPIC.to_string(),
            partition: 0,
        })
        .get();
    broker
        .wait_for_metrics("partition compacted after produce", |m| {
            m.log_compactions_total
                .get_or_create(&krabka_broker::metrics::PartitionLabel {
                    topic: TOPIC.to_string(),
                    partition: 0,
                })
                .get()
                >= compactions_before + 2
        })
        .await;

    // 4. Consume from beginning — only the latest per-key records should appear.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--timeout-ms",
        "5000",
    ]);
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    eprintln!("KRABKA[test] consumer stdout: {stdout:?}");

    // Latest values for each key must be present.
    for needle in ["v4", "v3", "v5"] {
        assert!(
            stdout.contains(needle),
            "expected {needle} in consumer output (latest per-key); got: {stdout:?}"
        );
    }
    // Stale values for k1 must have been compacted away.
    for stale in ["v1", "v2"] {
        assert!(
            !stdout.contains(stale),
            "stale value {stale} still present after compaction; got: {stdout:?}"
        );
    }

    broker.shutdown().await;
}
