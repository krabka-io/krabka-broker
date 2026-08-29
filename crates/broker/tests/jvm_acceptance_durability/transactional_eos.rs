//! The transactional exactly-once case: a compiled JVM `KafkaProducer` commits
//! one transaction and aborts another, and the `read_committed` and
//! `read_uncommitted` isolation levels must disagree in exactly that way.
//!
//! It is the only durability case that needs the split `EXTERNAL` and
//! `INTERNAL` listeners and the image that ships `javac`, which is why it is
//! its own file.

use std::{
    io::Write,
    process::{Command, Stdio},
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig};
use krabka_log::LogConfig;

use crate::jvm_acceptance::{KAFKA_IMAGE_TXN, TRANSACTIONAL_PRODUCER_JAVA, docker_run_kafka_tool};

// Transactional EOS smoke: stand up a 3-broker Krabka cluster, compile and
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
    const TOPIC: &str = "krabka-txn-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("krabka_broker=debug,info")),
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
            listeners: vec![
                krabka_broker::config::ListenerSpec {
                    name: "EXTERNAL".to_string(),
                    bind_addr: listen_addr,
                    advertised: advertised_listener,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                },
                krabka_broker::config::ListenerSpec {
                    name: "INTERNAL".to_string(),
                    bind_addr: inter_broker_addr,
                    advertised: inter_broker_addr.to_string(),
                    protocol: krabka_security::ListenerProtocol::Plaintext,
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
        "KRABKA[test] transactional Java producer status={} stdout={} stderr={}",
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
    // not in the metadata image and have no krabka awaiter/metric.
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
