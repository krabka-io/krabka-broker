//! Multi-broker durability: three-node round-trips and byte-for-byte replica
//! comparison, `acks=all` across a leader crash, and transactional EOS.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_log::LogConfig;
use jvm_acceptance::*;

// Three-node quorum: produce on one node, consume on another, then kill
// the controller leader and assert the surviving brokers still answer
// Metadata. Same multi-thread runtime caveat as the other tests; we ask
// for 4 workers because we have three brokers + the test driver all
// making blocking docker calls.
//
// Fixed ports per node because docker containers must be able to reach
// the brokers via `host.docker.internal:<client-port>`. The advertised
// listener uses the same hostname so the AdminClient's post-Metadata
// reconnect resolves correctly. Controller ports use `127.0.0.1` for
// inter-broker traffic — all three Crabka brokers live on the host's
// loopback, so docker reachability is not required for the controller
// listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_jvm_round_trip() {
    const TOPIC: &str = "crabka-quorum-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [9192u16, 9292, 9392];
    let controller_ports = [9193u16, 9293, 9393];

    // Voters for inter-broker (controller) traffic: host loopback works.
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
        // bind on 0.0.0.0 so Docker-side containers can reach us.
        listen_addr: format!("0.0.0.0:{}", client_ports[0])
            .parse()
            .expect("static addr"),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0])
            .parse()
            .expect("static addr"),
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
            node_id: crabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
    let bootstrap_2 = format!("host.docker.internal:{}", client_ports[1]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create topic via node 1.
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
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // 2. Wait for the topic to propagate from node 1 (where kafka-topics
    //    created it) to node 2 (where we'll produce) by observing node 2's
    //    committed metadata image directly.
    cluster[1].0.wait_until_partition_present(TOPIC, 0).await;

    // 3. Produce via kafka-console-producer (JVM). The JVM AdminClient
    //    transparently follows the partition leader: it asks any node's
    //    Metadata for the leader of partition 0 and opens a fresh
    //    connection to that broker's advertised address. The
    //    Rust producer doesn't yet route across brokers per partition,
    //    so we use the JVM tool here; cross-broker producer routing is
    //    a follow-up that the Rust client will pick up.
    let mut producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            &bootstrap_2,
            "--topic",
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JVM producer");
    producer_child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"a\nb\nc\n")
        .expect("write stdin");
    drop(producer_child.stdin.take());
    let producer_out = producer_child.wait_with_output().expect("wait producer");
    assert!(
        producer_out.status.success(),
        "JVM producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 4. Consume via node 3.
    let out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["a", "b", "c"] {
        assert!(s.contains(needle), "missing {needle} in {s:?}");
    }

    // 5. Find the controller leader, kill it.
    let mut leader_idx = None;
    for (i, (h, _)) in cluster.iter().enumerate() {
        let want = u64::try_from(i + 1).unwrap();
        if h.controller_leader_id() == Some(crabka_broker::NodeId(want)) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("a leader exists");
    let (leader, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;
    // intentional: allow the surviving voters to run a controller re-election
    // after the leader was killed. There is no "controller leader changed"
    // awaiter, and a survivor's cached leader value can momentarily read stale,
    // so a fixed settle window is used rather than risk a stale-value wait.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 6. Survivor still answers Metadata via kafka-topics --list.
    let survivor_idx = (0..client_ports.len())
        .find(|i| *i != leader_idx)
        .expect("at least one survivor");
    let survivor_bootstrap = format!("host.docker.internal:{}", client_ports[survivor_idx]);
    let list_out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--list",
        "--bootstrap-server",
        &survivor_bootstrap,
    ]);
    let list_s = String::from_utf8_lossy(&list_out.stdout);
    assert!(
        list_s.contains(TOPIC),
        "topic missing after leader kill: {list_s:?}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// Replication byte-compare: stand up a 3-broker Crabka cluster, create a
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
    const TOPIC: &str = "crabka-replication-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0])
            .parse()
            .expect("static addr"),
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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
            node_id: crabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
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

// Transactional EOS smoke: stand up a 3-broker Crabka cluster, compile and
// run a small official JVM KafkaProducer client that commits 6 records and
// aborts 2, then verify read_committed and read_uncommitted isolation.
//
// Fixed external ports 9792/9892/9992, controller ports 9793/9893/9993,
// and loopback-only inter-broker ports 9794/9894/9994. The split listeners
// let Docker clients use host.docker.internal while host-side brokers use
// loopback, so local Linux runs do not need an /etc/hosts entry.
//
// Same multi-thread runtime caveat as the other multi-broker tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn transactional_console_producer_eos() {
    const TOPIC: &str = "crabka-txn-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [9792u16, 9892, 9992];
    let controller_ports = [9793u16, 9893, 9993];
    let inter_broker_ports = [9794u16, 9894, 9994];

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

    // Parallel spawn — sequential startup deadlocks waiting for quorum.
    let mut tempdirs = Vec::with_capacity(3);
    let mut spawns = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let listen_addr = format!("0.0.0.0:{}", client_ports[i])
            .parse()
            .expect("static addr");
        let advertised_listener = format!("host.docker.internal:{}", client_ports[i]);
        let inter_broker_addr = format!("127.0.0.1:{}", inter_broker_ports[i])
            .parse()
            .expect("static addr");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr,
            advertised_listener: advertised_listener.clone(),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static addr"),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
            listeners: vec![
                crabka_broker::config::ListenerSpec {
                    name: "EXTERNAL".to_string(),
                    bind_addr: listen_addr,
                    advertised: advertised_listener,
                    protocol: crabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                },
                crabka_broker::config::ListenerSpec {
                    name: "INTERNAL".to_string(),
                    bind_addr: inter_broker_addr,
                    advertised: inter_broker_addr.to_string(),
                    protocol: crabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                },
            ],
            inter_broker_listener_name: "INTERNAL".to_string(),
            ..BrokerConfig::default()
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (sp, dir) in spawns.into_iter().zip(tempdirs) {
        cluster.push((sp.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create the topic via node 1.
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
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // 2. Compile the small Java helper against the image's Kafka client jars.
    //    It writes one committed transaction and one aborted transaction.
    let mut producer = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/TransactionalProducer.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/TransactionalProducer.java; \
               java -cp "/tmp:$CP" TransactionalProducer "$1" "$2""#,
            "--",
            &bootstrap_1,
            TOPIC,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn transactional Java producer");
    producer
        .stdin
        .as_mut()
        .expect("producer stdin")
        .write_all(TRANSACTIONAL_PRODUCER_JAVA.as_bytes())
        .expect("write Java helper");
    drop(producer.stdin.take());
    let producer_out = producer.wait_with_output().expect("wait Java producer");
    eprintln!(
        "CRABKA[test] transactional Java producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "transactional Java producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        String::from_utf8_lossy(&producer_out.stdout).contains("TXNPROBE OK"),
        "transactional Java producer did not report success: {}",
        String::from_utf8_lossy(&producer_out.stdout),
    );

    // 3. Brief pause to let commit markers propagate through the log.
    // intentional: transactional commit-marker propagation and LSO advance are
    // not in the metadata image and have no crabka awaiter/metric.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 4. read_committed must return exactly the committed transaction.
    let committed_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "6",
        "--timeout-ms",
        "20000",
    ]);
    let committed_stdout = String::from_utf8_lossy(&committed_out.stdout);
    let committed: Vec<_> = committed_stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(
        committed
            == [
                "committed-0",
                "committed-1",
                "committed-2",
                "committed-3",
                "committed-4",
                "committed-5",
            ],
        "read_committed returned the wrong records: {committed_stdout}",
    );

    // 5. read_uncommitted must return both transactions in log order.
    let uncommitted_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_uncommitted",
        "--from-beginning",
        "--max-messages",
        "8",
        "--timeout-ms",
        "20000",
    ]);
    let uncommitted_stdout = String::from_utf8_lossy(&uncommitted_out.stdout);
    let uncommitted: Vec<_> = uncommitted_stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert!(
        uncommitted
            == [
                "committed-0",
                "committed-1",
                "committed-2",
                "committed-3",
                "committed-4",
                "committed-5",
                "aborted-0",
                "aborted-1",
            ],
        "read_uncommitted returned the wrong records: {uncommitted_stdout}",
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}

// `acks=all` durability gate: 3-broker Crabka cluster, JVM
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
    const TOPIC: &str = "crabka-acks-all-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
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
    let cfg0 = crabka_broker::BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().unwrap(),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: crabka_log::LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().unwrap(),
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..crabka_broker::BrokerConfig::default()
    };
    let h0 = tokio::spawn(async move {
        crabka_broker::Broker::start(cfg0)
            .await
            .expect("broker start")
    });

    // Brokers 1, 2 (Bootstrap).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: crabka_log::LogConfig::default(),
            node_id: crabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(3_000),
            heartbeat_timeout: crabka_units::millis(9_000),
            replica_lag_time_max: crabka_units::millis(30_000),
            controller_election_timeout: crabka_units::secs(5),
            controller_heartbeat_interval: crabka_units::millis(500),
            bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
            ..crabka_broker::BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg)
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
        "CRABKA[test] producer status={} stdout={} stderr={}",
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
    // and has no crabka awaiter/metric.
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

// `acks=all` survives a leader crash mid-produce burst: 3-broker Crabka
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
    const TOPIC: &str = "crabka-acks-all-crash-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [10392u16, 10492, 10592];
    let controller_ports = [10393u16, 10493, 10593];

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
    let cfg0 = crabka_broker::BrokerConfig {
        broker_id: 1,
        listen_addr: format!("0.0.0.0:{}", client_ports[0]).parse().unwrap(),
        advertised_listener: format!("host.docker.internal:{}", client_ports[0]),
        log_dir: dir0.path().to_path_buf(),
        log_config: crabka_log::LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: format!("0.0.0.0:{}", controller_ports[0]).parse().unwrap(),
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(200),
        heartbeat_timeout: crabka_units::millis(2_000),
        replica_lag_time_max: crabka_units::millis(2_000),
        controller_election_timeout: crabka_units::millis(500),
        controller_heartbeat_interval: crabka_units::millis(100),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..crabka_broker::BrokerConfig::default()
    };
    let h0 = tokio::spawn(async move {
        crabka_broker::Broker::start(cfg0)
            .await
            .expect("broker start")
    });

    // Brokers 1, 2 (Bootstrap).
    let mut join_spawns = Vec::with_capacity(2);
    for i in 1..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: crabka_log::LogConfig::default(),
            node_id: crabka_broker::NodeId(u64::try_from(i + 1).unwrap()),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect(),
            heartbeat_interval: crabka_units::millis(200),
            heartbeat_timeout: crabka_units::millis(2_000),
            replica_lag_time_max: crabka_units::millis(2_000),
            controller_election_timeout: crabka_units::millis(500),
            controller_heartbeat_interval: crabka_units::millis(100),
            bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
            ..crabka_broker::BrokerConfig::default()
        };
        tempdirs.push(dir);
        join_spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg)
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
    let mut cluster: Vec<(crabka_broker::BrokerHandle, tempfile::TempDir)> = Vec::with_capacity(3);
    cluster.push((h0.await.expect("spawn"), dir0));
    for (spawn, dir) in join_spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

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

    // 3. Determine partition-0 leader from Metadata via local port (not Docker).
    let leader_node_id = {
        use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
        let local_bootstrap = format!("127.0.0.1:{}", client_ports[0]);
        let probe = crabka_client_core::Client::builder()
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
    let producer_child = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
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
        ])
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
        eprintln!("CRABKA[test] killing leader node_id={leader_node_id} idx={leader_idx}");
        let (leader_handle, _dir) = cluster.remove(leader_idx);
        leader_handle.shutdown().await;
    }

    // 6. Wait for the JVM producer to complete (up to 60s for election + retry).
    let producer_out = producer_child.wait_with_output().expect("wait producer");
    eprintln!(
        "CRABKA[test] producer status={} stderr_len={}",
        producer_out.status,
        producer_out.stderr.len(),
    );
    if !producer_out.status.success() {
        eprintln!(
            "CRABKA[test] producer stderr: {}",
            String::from_utf8_lossy(&producer_out.stderr),
        );
    }

    // 7. Wait briefly for replication to settle post-election.
    // intentional: post-election follower high-watermark convergence is not in
    // the metadata image and has no crabka awaiter/metric; the JVM consumer
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
