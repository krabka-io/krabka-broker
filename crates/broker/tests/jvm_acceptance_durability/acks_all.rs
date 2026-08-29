//! The steady-state `acks=all` durability gate: 100 records written with
//! `--request-required-acks -1` must all read back from a third broker under
//! `read_committed`.
//!
//! It covers the high-watermark path with the cluster intact. The variant that
//! kills the partition leader mid-burst lives beside it in `leader_crash`.

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE, docker_run_kafka_tool};

// `acks=all` durability gate: 3-broker Krabka cluster, JVM
// `kafka-console-producer --request-required-acks -1` writes 100
// records, then `kafka-console-consumer --isolation-level
// read_committed` reads them all back. Confirms HW+acks=all works
// against an unmodified JVM client.
//
// Fixed ports above 10000 — the other multi-broker tests use 9092-9992;
// this test steps into 10000+ to dodge TIME_WAIT + raft-quorum collisions
// when JVM tests run sequentially via the nextest broker-jvm-acceptance test group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acks_all_durability() {
    const TOPIC: &str = "krabka-acks-all-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    // Ports 10092/10192/10292 + 10093/10193/10293 — the next free hundred
    // above the transactional test (9792-9992). The other multi-broker
    // tests use the 9092-9992 range; we step into 10000+ to avoid TIME_WAIT
    // collisions.
    let client_ports = [10092u16, 10192, 10292];
    let controller_ports = [10093u16, 10193, 10293];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i])
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    // Static cold-boot (KIP-595): all three voters boot concurrently in
    // Bootstrap mode, each seeded with the full static `controller_quorum_voters`
    // set, and elect a leader among themselves.
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().unwrap();
    let cfg0 = krabka_broker::BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().unwrap(),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: krabka_log::LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().unwrap(),
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..krabka_broker::BrokerConfig::default()
    };
    let h0 = tokio::spawn(async move {
        krabka_broker::Broker::start(cfg0)
            .await
            .expect("broker start")
    });

    // Brokers 1, 2 (Bootstrap).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = krabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: krabka_log::LogConfig::default(),
            node_id: krabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (krabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: krabka_units::millis(3_000),
            heartbeat_timeout: krabka_units::millis(9_000),
            replica_lag_time_max: krabka_units::millis(30_000),
            controller_election_timeout: krabka_units::secs(5),
            controller_heartbeat_interval: krabka_units::millis(500),
            bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
            ..krabka_broker::BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            krabka_broker::Broker::start(cfg)
                .await
                .expect("broker start")
        }));
    }

    // Static cold-boot (KIP-595): every voter is seeded with the full static
    // `controller_quorum_voters` set in Bootstrap mode, so the quorum forms by
    // electing among the concurrently-booting voters. `Broker::start` blocks
    // until its controller sees a committed leader, and a leader needs a
    // majority of the static set up and dialable — so awaiting broker 0 alone
    // would deadlock. Spawn all starts concurrently and join them. (The old
    // openraft bootstrap-then-join via add_learner/change_membership is gone
    // with the static voter set.)
    let mut cluster = Vec::with_capacity(3);
    cluster.push((h0.await.expect("spawn"), dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);

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

    // Produce 100 records with --request-required-acks=-1.
    let producer_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_1} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 10000"
            ),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn kafka-console-producer");
    eprintln!(
        "KRABKA[test] producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "kafka-console-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // intentional: wait for the produced records (acks=-1) to replicate to
    // node 3 and its high-watermark to advance before the read_committed
    // consume below. Follower high-watermark/LSO is not in the metadata image
    // and has no krabka awaiter/metric.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);
    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "100",
        "--timeout-ms",
        "20000",
    ]);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 100,
        "expected at least 100 records; got {line_count}: stdout={stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
