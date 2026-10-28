//! The transactional exactly-once case: a compiled JVM `KafkaProducer` commits
//! one transaction, aborts another, and appends a later record after rolling
//! each batch into its own segment. The `read_committed` and
//! `read_uncommitted` isolation levels must disagree in exactly that way.
//!
//! It is the only durability case that needs the split `EXTERNAL` and
//! `INTERNAL` listeners and the image that ships `javac`, which is why it is
//! its own file.

use {
    crate::{
        jvm_acceptance::{
            KAFKA_IMAGE_TXN, TRANSACTIONAL_PRODUCER_JAVA, create_plain_console_topic_at,
            docker_run_kafka_tool,
        },
        support::{
            ContainerInput, controller_voters, init_jvm_tracing, jvm_broker_config,
            jvm_docker_command,
        },
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec},
    krabka_log::LogConfig,
    std::{
        io::Write,
        mem,
        net::SocketAddr,
        process::{Output, Stdio},
        time::Duration,
    },
    tempfile::TempDir,
    tokio::{time, time::Instant},
};

/// A zombie-fencing probe (KIP-360): two JVM producers share one
/// `transactional.id`, the second takes the id over, and the first must be
/// fenced the next time it tries to write.
///
/// This is the fence Kafka documents on `transactional.id` itself: the second
/// `initTransactions` bumps the epoch, and the coordinator then refuses the
/// first producer's stale `(producer_id, producer_epoch)` on its
/// `AddPartitionsToTxn`. The JVM client turns both `PRODUCER_FENCED` and
/// `INVALID_PRODUCER_EPOCH` there into a fatal `ProducerFencedException`
/// (`TransactionManager.AddPartitionsToTxnHandler`), so that is the name the
/// probe expects. A broker that admits the stale epoch instead lets the zombie
/// write, which is the failure this case is here to catch.
///
/// The first producer acquires the id and then writes nothing until after the
/// takeover, so the takeover is the only thing that can have moved its epoch
/// and the refusal cannot be read as anything else.
///
/// The probe cannot instead drive `InitProducerId` with the stale identity:
/// the JVM client sends that request's `producer_id`/`producer_epoch` fields
/// only for its own internal KIP-360 epoch bump, and a second public
/// `initTransactions()` on a producer that has committed is refused locally --
/// `TransactionManager` allows the `INITIALIZING` transition only from
/// `UNINITIALIZED`, `COMMITTING_TRANSACTION` or `ABORTING_TRANSACTION`, so it
/// throws `IllegalStateException` before any request reaches the broker. The
/// broker's `PRODUCER_FENCED` answer on that request is pinned by the
/// `init_producer_id` unit tests instead.
///
/// The probe prints `ZOMBIEPROBE fenced=<exception simple name>`, or
/// `ZOMBIEPROBE fenced=none` when nothing fenced it at all.
const ZOMBIE_PRODUCER_JAVA: &str = krabka_macros::java_string_producer_source!(
    r#"

public final class ZombieProducer {
  public static void main(String[] args) throws Exception {
"#,
    "args[0]",
    r#"    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "zombie-tid");

    // The first instance acquires the id and stops there: it writes nothing
    // before the takeover, so the only thing that can move its epoch is the
    // takeover itself.
    KafkaProducer<String, String> first = new KafkaProducer<>(config);
    first.initTransactions();

    // A second instance of the same console producer takes the id over. Its
    // InitProducerId carries no identity, so it is admitted and bumps the
    // epoch; the first instance is now a zombie holding the old one.
    KafkaProducer<String, String> second = new KafkaProducer<>(config);
    second.initTransactions();

    // The zombie writes on. Its AddPartitionsToTxn carries the epoch the
    // takeover superseded, and the coordinator must refuse it.
    String fenced = "none";
    try {
      first.beginTransaction();
      first.send(new ProducerRecord<>(args[1], "zombie-stale")).get();
      first.commitTransaction();
    } catch (Throwable error) {
      Throwable cause = error;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      fenced = cause.getClass().getSimpleName();
    }
    System.out.println("ZOMBIEPROBE fenced=" + fenced);

    for (KafkaProducer<String, String> producer : java.util.List.of(second, first)) {
      try {
        producer.close(java.time.Duration.ofSeconds(5));
      } catch (Throwable ignored) {
        // A fenced producer may fail to close; the probe has its answer.
      }
    }
  }
}
"#
);

// Transactional EOS smoke: stand up a 3-broker Krabka cluster, compile and
// run a small official JVM KafkaProducer client that commits 6 records and
// aborts 2, appends a later record, then verifies read_committed and
// read_uncommitted isolation across sealed segments.
//
// Fixed external ports 9792/9892/9992, controller ports 9793/9893/9993,
// and loopback-only inter-broker ports 9794/9894/9994. The split listeners
// let Docker clients use host.docker.internal while host-side brokers use
// loopback, so local Linux runs do not need an /etc/hosts entry.
//
// Same multi-thread runtime caveat as the other multi-broker tests.
const TOPIC: &str = "krabka-txn-itest";
struct TransactionIsolation {
    cluster: Vec<(BrokerHandle, TempDir)>,
    output: Option<Output>,
    consumed: Vec<String>,
}
#[rstest::fixture]
fn isolation() -> TransactionIsolation {
    // Start this scenario with fresh inputs and observations.

    TransactionIsolation {
        cluster: Vec::new(),
        output: None,
        consumed: Vec::new(),
    }
}
#[rstest_bdd_macros::given(
    "three brokers expose external and internal listeners with 128-byte segments and topic krabka-txn-itest"
)]
async fn segmented_cluster(isolation: &mut TransactionIsolation) {
    /// Segment size every broker in this cluster boots with, so that each
    /// batch the transactional producer writes lands in a segment of its own
    /// and the abort marker's transaction index is sealed before the reads.
    ///
    /// It is a broker default, not a topic override: Kafka's topic-level
    /// `segment.bytes` has a 1 MiB floor that krabka matches, and the only
    /// topic config below it — Kafka 4.x's `internal.segment.bytes` — is
    /// rejected client-side by the `TopicCommand` in the image this suite
    /// pins. Kafka floors its broker-level `log.segment.bytes` at 1 MiB too,
    /// but that validates a parsed broker config; the harness sets the struct
    /// field directly, as Kafka's own tests do. 128 bytes is above one batch
    /// (a one-record batch or a marker is about 75 bytes; `UnifiedLog.append`
    /// refuses a batch larger than the segment with `RECORD_LIST_TOO_LARGE`) and
    /// below two, so no batch can ever fit twice.
    const SEGMENT_SIZE: krabka_units::ByteSize = krabka_units::bytes(128);

    init_jvm_tracing("krabka_broker=debug,info");

    let client_ports = [9792u16, 9892, 9992];
    let controller_ports = [9793u16, 9893, 9993];
    let inter_broker_ports = [9794u16, 9894, 9994];

    let voters: Vec<(u64, SocketAddr)> = (0..3)
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
            // Neither topic below overrides a segment size, so both inherit
            // this one. See `SEGMENT_SIZE`.
            log_config: LogConfig {
                segment_size: SEGMENT_SIZE,
                ..LogConfig::default()
            },
            listeners: vec![
                ListenerSpec {
                    name: "EXTERNAL".to_owned(),
                    bind_addr: listen_addr,
                    advertised: advertised_listener.clone(),
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                    principal_mapper: krabka_broker::SslPrincipalMapper::default(),
                },
                ListenerSpec {
                    name: "INTERNAL".to_owned(),
                    bind_addr: inter_broker_addr,
                    advertised: inter_broker_addr.to_string(),
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_mechanisms: None,
                    principal_mapper: krabka_broker::SslPrincipalMapper::default(),
                },
            ],
            inter_broker_listener_name: "INTERNAL".to_owned(),
            ..jvm_broker_config()
                .log_dir(dir.path().to_path_buf())
                .node(u64::try_from(i + 1).unwrap())
                .listen(listen_addr)
                .controller(
                    format!("0.0.0.0:{}", controller_ports[i])
                        .parse()
                        .expect("static addr"),
                )
                .advertised(advertised_listener.clone())
                .voters(controller_voters(&voters))
                .call()
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

    // 1. Create the topic via node 1.
    create_plain_console_topic_at(&bootstrap_1, TOPIC);
    // Wait for the partition to materialize on the broker that leads it: the
    // producer's first batch must not land before the log exists with the
    // segment size the cluster booted with.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cluster.iter().any(|(broker, _)| {
        broker
            .partition_log_config_for_test(TOPIC, 0)
            .is_some_and(|config| config.segment_size == SEGMENT_SIZE)
    }) {
        assert!(
            Instant::now() < deadline,
            "the transaction-log partition never materialized with a {SEGMENT_SIZE:?} segment"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    isolation.cluster = cluster;
}
#[rstest_bdd_macros::when(
    "the JVM helper commits six records, aborts two, and appends after-abort"
)]
async fn write_transactions(isolation: &mut TransactionIsolation) {
    let bootstrap_1 = "host.docker.internal:9792";
    // 2. Compile the small Java helper against the image's Kafka client jars.
    //    It writes one committed transaction, one aborted transaction, and a
    //    later record that seals the abort marker's transaction index.
    let mut producer = jvm_docker_command().image("--entrypoint").args(&["bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/TransactionalProducer.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/TransactionalProducer.java; \
               java -cp "/tmp:$CP" TransactionalProducer "$1" "$2""#,
            "--",
            &bootstrap_1,
            TOPIC,
        ]).input(ContainerInput::Attached).call()
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
    isolation.output = Some(producer_out);
    time::sleep(Duration::from_secs(2)).await;
}
#[rstest_bdd_macros::then("the JVM exits successfully and reports {marker}")]
fn java_outcome(isolation: &TransactionIsolation, marker: String) {
    let output = isolation.output.as_ref().expect("JVM helper ran");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(&marker));
}
#[rstest_bdd_macros::when("a console consumer reads {count} records with {level} through broker 3")]
fn consume_transactions(isolation: &mut TransactionIsolation, count: usize, level: String) {
    let output = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        "host.docker.internal:9992",
        "--topic",
        TOPIC,
        "--isolation-level",
        &level,
        "--from-beginning",
        "--max-messages",
        &count.to_string(),
        "--timeout-ms",
        "20000",
    ]);
    isolation.consumed = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
}
#[rstest_bdd_macros::then("the console returns exactly these values in log order")]
fn isolated_values(isolation: &TransactionIsolation, datatable: Vec<Vec<String>>) {
    let expected: Vec<String> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value]: [String; 1] = row.try_into().expect("value");
            value
        })
        .collect();
    assert!(isolation.consumed == expected);
}
#[rstest_bdd_macros::when(
    "two JVM producers claim zombie-tid on topic {topic} and the superseded producer writes"
)]
fn fenced_producer(isolation: &mut TransactionIsolation, topic: String) {
    let bootstrap_1 = "host.docker.internal:9792";
    // 6. KIP-360: a second producer on one transactional.id fences the first,
    // and the first learns it as a fatal `ProducerFencedException` the next
    // time it writes.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        &topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        &bootstrap_1,
    ]);
    let mut zombie = jvm_docker_command().image("--entrypoint").args(&["bash",
            KAFKA_IMAGE_TXN,
            "-c",
            r#"set -e; cat >/tmp/ZombieProducer.java; \
               CP=$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':'); \
               javac -cp "$CP" -d /tmp /tmp/ZombieProducer.java; \
               java -cp "/tmp:$CP" ZombieProducer "$1" "$2""#,
            "--",
            &bootstrap_1,
            &topic,
        ]).input(ContainerInput::Attached).call()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn zombie Java producer");
    zombie
        .stdin
        .as_mut()
        .expect("zombie stdin")
        .write_all(ZOMBIE_PRODUCER_JAVA.as_bytes())
        .expect("write Java zombie helper");
    drop(zombie.stdin.take());
    let zombie_out = zombie
        .wait_with_output()
        .expect("wait Java zombie producer");
    let zombie_stdout = String::from_utf8_lossy(&zombie_out.stdout);
    eprintln!(
        "KRABKA[test] zombie Java producer status={} stdout={zombie_stdout} stderr={}",
        zombie_out.status,
        String::from_utf8_lossy(&zombie_out.stderr),
    );
    isolation.output = Some(zombie_out);
}

#[ignore = "requires Docker"]
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_durability/transactional_eos.feature",
    name = "transactional console producer eos"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactional_console_producer_eos(isolation: TransactionIsolation) {
    for (broker, _) in mem::take(&mut isolation.cluster) {
        broker.shutdown().await;
    }
    drop(isolation);
}
