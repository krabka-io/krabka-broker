//! The byte-for-byte replica comparison: produce 100 records into a
//! replication-factor-three topic, then confirm `kafka-dump-log` renders every
//! broker's local segment identically.
//!
//! It is the only durability case that reads the on-disk segments back through
//! the JVM tools, so it carries the container mount handling that the other
//! cases do not need.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use crate::jvm_acceptance::{KAFKA_IMAGE, docker_run_kafka_tool};

// Replication byte-compare: stand up a 3-broker Krabka cluster, create a
// `replication-factor=3` topic, produce 100 records via the JVM
// `kafka-console-producer`, then run `kafka-dump-log` against each
// broker's local partition file and assert the three dumps are
// byte-identical.
//
// Why fixed ports + `host.docker.internal`: the JVM client opens a
// per-broker connection per partition leader, so every broker's
// advertised listener must be reachable from inside the Kafka tool
// container. The CI workflow already wires `host.docker.internal` on
// the host's `/etc/hosts` to the bridge gateway IP. Controller traffic
// uses host loopback (`127.0.0.1`) — Docker reachability is irrelevant
// for inter-broker.
//
// `kafka-dump-log` ships on the `mirror.gcr.io/confluentinc/cp-kafka:6.1.1` image
// alongside `kafka-topics` / `kafka-console-producer` — it's a standard
// Apache Kafka tool. We mount each broker's partition dir into a fresh
// container as `-v <host>:/data:ro` and dump the first segment file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_replication_byte_compare() {
    const TOPIC: &str = "krabka-replication-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    // Distinct ports from `three_node_jvm_round_trip` (which uses
    // 9192/9292/9392 + 9193/9293/9393). Linux's TIME_WAIT keeps the prior
    // test's sockets bound for ~60s after teardown; running this test
    // back-to-back on the same ports causes `Broker::start` to either fail
    // to bind or to inherit half-closed peer state, which surfaces as
    // "no leader elected within 2 min" on the openraft side.
    let client_ports = [9492u16, 9592, 9692];
    let controller_ports = [9493u16, 9593, 9693];

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

    // Static cold-boot (KIP-595): every voter is seeded with the full static
    // `controller_quorum_voters` set in Bootstrap mode, so the quorum forms by
    // electing among the concurrently-booting voters. `Broker::start` blocks
    // until its controller sees a committed leader, and a leader needs a
    // majority of the static set up and dialable — so awaiting broker 0 alone
    // would deadlock. Spawn all starts concurrently and join them. (The old
    // openraft bootstrap-then-join via add_learner/change_membership is gone
    // with the static voter set.)
    let mut tempdirs: Vec<tempfile::TempDir> = Vec::with_capacity(3);

    // Broker 0 (Bootstrap).
    let dir0 = tempfile::tempdir().expect("tempdir");
    let cfg0 = BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0])
            .parse()
            .expect("static addr"),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: krabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0])
            .parse()
            .expect("static addr"),
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
        ..BrokerConfig::default()
    };
    let h0 = tokio::spawn(async move { Broker::start(cfg0).await.expect("broker start") });

    // Brokers 1, 2 (Bootstrap).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i])
                .parse()
                .expect("static addr"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: krabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
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
            ..BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }

    // All voters boot concurrently; join their start futures to form the cluster.
    let mut cluster = Vec::with_capacity(3);
    cluster.push((h0.await.expect("spawn"), dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_all = format!(
        "host.docker.internal:{},host.docker.internal:{},host.docker.internal:{}",
        client_ports[0], client_ports[1], client_ports[2],
    );

    // 1. CreateTopics(repl=3, partitions=1).
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

    // 2. Wait for the ISR to include all three brokers (ISR == replicas here),
    //    i.e. the metadata propagated. The in-process image ISR is exactly what
    //    `kafka-topics --describe` reports, so observe it directly.
    cluster[0].0.wait_until_isr_len(TOPIC, 0, 3).await;

    // 3. Produce 100 records via kafka-console-producer with acks=all so
    //    each produce response gates on HW = LEO across the full ISR.
    //    Without this the producer returns after leader ack and we end up
    //    dumping followers before their replicators have caught up,
    //    making the byte-compare assert fail spuriously.
    let mut producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap_all,
            "--topic",
            TOPIC,
            "--producer-property",
            "acks=all",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JVM producer");
    {
        let stdin = producer_child.stdin.as_mut().expect("stdin");
        for i in 0..100 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(producer_child.stdin.take());
    let prod_out = producer_child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );

    // 4. Wait for replication lag to drain: every broker's local partition log
    //    must reach the full 100 records before we dump them. With acks=all the
    //    produce above already gated on HW=LEO across the ISR, so this resolves
    //    immediately; the awaiter reads each broker's local log end offset
    //    directly (which `kafka-topics --describe` cannot expose).
    for entry in cluster.iter().take(3) {
        entry.0.wait_until_local_log_end_offset(TOPIC, 0, 100).await;
    }

    // 5. For each broker, dump the local partition file via
    //    `kafka-dump-log`. The `-v <host>:/data:ro` mount makes the
    //    broker's on-disk partition directory visible to the tool
    //    container.
    let mut dumps = Vec::with_capacity(3);
    for (i, (_, dir)) in cluster.iter().enumerate() {
        let partition_dir = dir.path().join(format!("{TOPIC}-0"));
        let log_file = partition_dir.join("00000000000000000000.log");
        assert!(
            log_file.exists(),
            "broker {} missing log file: {log_file:?}",
            i + 1,
        );
        let mount = format!("{}:/data:ro", partition_dir.display());
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-v",
                &mount,
                KAFKA_IMAGE,
                "kafka-dump-log",
                "--files",
                "/data/00000000000000000000.log",
                "--print-data-log",
            ])
            .output()
            .expect("spawn dump-log");
        assert!(
            out.status.success(),
            "dump-log failed for broker {}: stdout={}, stderr={}",
            i + 1,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        dumps.push(String::from_utf8_lossy(&out.stdout).to_string());
    }

    // 6. All three dumps should be byte-identical.
    assert!(dumps[0] == dumps[1], "broker 1 vs broker 2 dump differ");
    assert!(dumps[1] == dumps[2], "broker 2 vs broker 3 dump differ");

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
