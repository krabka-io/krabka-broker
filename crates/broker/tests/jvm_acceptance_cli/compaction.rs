//! `kafka-console-consumer` over a compacted topic, which must see only the
//! latest value per key.
//!
//! This suite boots its own broker with a three-second cleaner interval instead
//! of the shared `start_host_broker` harness, so it carries the `BrokerConfig`
//! that no other file in this suite needs.

use {
    crate::{
        jvm_acceptance::{
            KAFKA_IMAGE, broker0_advertised, broker0_listen, docker_run_kafka_tool,
            host_broker_config, nc_check_connectivity, wait_jvm_log_config,
        },
        support::{ContainerInput, init_jvm_tracing, jvm_docker_command, jvm_stdin_output},
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, metrics::PartitionLabel},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tempfile::TempDir,
};

const TOPIC: &str = "compacted-jvm";
const SEGMENT_SIZE: krabka_units::ByteSize = krabka_units::bytes(256);

struct CompactedTopic {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    producer: Option<Output>,
    consumer: Option<Output>,
}

#[fixture]
fn compacted() -> CompactedTopic {
    // Start this scenario with fresh inputs and observations.

    CompactedTopic {
        broker: None,
        directory: None,
        producer: None,
        consumer: None,
    }
}

#[given("a compacted topic inherits 256 byte segments and a three second cleaner interval")]
async fn compacted_log(compacted: &mut CompactedTopic) {
    init_jvm_tracing("krabka_broker=debug,info");
    let dir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig {
        // The topic below overrides no segment size, so it inherits this one.
        // See `SEGMENT_SIZE`.
        log_config: krabka_log::LogConfig {
            segment_size: SEGMENT_SIZE,
            ..krabka_log::LogConfig::default()
        },
        // 3s cleaner tick so we don't have to wait the full 30s default.
        cleaner_interval_override: Some(krabka_units::secs(3)),
        ..host_broker_config(dir.path(), "static addr")
    };
    let broker = Broker::start(config).await.expect("start broker");
    eprintln!(
        "KRABKA[test] compaction broker started listen={listen} advertised={bootstrap}",
        bootstrap = broker0_advertised(),
        listen = broker0_listen()
    );
    nc_check_connectivity();

    // 1. Create the topic with cleanup.policy=compact. The tiny segment size
    //    that seals records into a second segment before the cleaner runs is
    //    the broker default the topic inherits, not an override: see
    //    `SEGMENT_SIZE`.
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
        // Kafka's default `min.cleanable.dirty.ratio` is 0.5, so a partition
        // whose dirty region is a small share of the log earns no pass. This
        // test produces a handful of records and then waits for a pass, so it
        // asks for Kafka's own prompt-compaction setting rather than relying
        // on how the workload happens to divide into segments.
        "--config",
        "min.cleanable.dirty.ratio=0.0",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // 1b. Wait for cleanup.policy=compact + min.cleanable.dirty.ratio=0.0 to
    //     propagate from the metadata image into the partition's LogConfig via
    //     the ReplicatorSupervisor reconcile loop. Without this wait, produces
    //     can land in a Log that still carries the Delete policy → no
    //     compaction. The segment size is not waited on: it is the broker
    //     default and is in place the moment the partition exists, so the
    //     assertion after the loop is a check on the harness rather than on
    //     propagation.
    let cfg = wait_jvm_log_config(
        &broker,
        TOPIC,
        |cfg| {
            cfg.cleanup_policy == krabka_log::CleanupPolicy::Compact
                && cfg.min_cleanable_dirty_ratio == krabka_units::fraction(0.0)
        },
        |within| {
            assert!(
                within,
                "cleanup.policy/min.cleanable.dirty.ratio never propagated within 10s"
            );
        },
    )
    .await;
    assert!(
        cfg.segment_size == SEGMENT_SIZE,
        "the partition must inherit the broker's {SEGMENT_SIZE:?} segment size; saw {:?}",
        cfg.segment_size
    );

    compacted.broker = Some(broker);
    compacted.directory = Some(dir);
}

#[when("the console producer appends repeated keys and enough padding to seal their segments")]
fn write_updates(compacted: &mut CompactedTopic) {
    // 2. Produce 5 records under 3 keys — k1 has three values (v1, v2, v4);
    //    only v4 should survive compaction.
    let mut child_command = jvm_docker_command()
        .image(KAFKA_IMAGE)
        .args(&[
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
            // `SEGMENT_SIZE` actually rolls segments mid-workload.
            "--producer-property",
            "batch.size=1",
            "--producer-property",
            "linger.ms=0",
            "--producer-property",
            "max.in.flight.requests.per.connection=1",
        ])
        .input(ContainerInput::Attached)
        .call();
    // First 5 records: the actual workload. After that, a burst of "pad"
    // records under a sentinel key forces the active segment past
    // `SEGMENT_SIZE` so v5 ends up sealed (otherwise the compactor
    // can't see it; it never touches the active segment) and the test's
    // "no stale v1" assertion can actually hold for k1.
    let producer_out = jvm_stdin_output(
        &mut child_command,
        b"k1:v1\nk1:v2\nk2:v3\nk1:v4\nk3:v5\n\
              __pad__:p0\n__pad__:p1\n__pad__:p2\n__pad__:p3\n\
              __pad__:p4\n__pad__:p5\n__pad__:p6\n__pad__:p7\n",
    );
    compacted.producer = Some(producer_out);
}

#[then("all updates were acknowledged")]
fn acknowledged(compacted: &CompactedTopic) {
    let output = compacted.producer.as_ref().expect("producer completed");
    assert!(
        output.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[when("two cleaner sweeps finish and the console consumer reads from the beginning")]
async fn read_cleaned(compacted: &mut CompactedTopic) {
    let broker = compacted
        .broker
        .as_ref()
        .expect("running compaction broker");
    // 3. Wait for two whole cleaner sweeps to finish after the produce did.
    //
    //    One pass is all Kafka's cleaner owes a settled log: it rewrites every
    //    cleanable sealed segment into one, which leaves nothing dirty behind
    //    it, so the partition is not due again until something is produced.
    //    Waiting for a further pass would wait forever, and waiting for a pass
    //    *after* the produce would too whenever the sweep that ran while the
    //    producer's JVM was shutting down already did the work.
    //
    //    Two sweeps is the sound bound instead: the counter is sampled after
    //    the producer exited, so the second sweep to finish began after every
    //    record had landed and saw the whole workload. Whether the pass it
    //    owed had already run or ran there, the log is deduplicated by the
    //    time the wait returns, and step 4 is what says so.
    let sweeps_before = broker.metrics().log_cleaner_runs_total.get();
    broker
        .wait_for_metrics("two cleaner sweeps after the produce", move |m| {
            m.log_cleaner_runs_total.get() >= sweeps_before + 2
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
    compacted.consumer = Some(consumer_out);
}

#[then("only the latest value of every workload key remains and the cleaner reports a compaction")]
fn latest_values(compacted: &CompactedTopic) {
    let broker = compacted
        .broker
        .as_ref()
        .expect("running compaction broker");
    let consumer_out = compacted.consumer.as_ref().expect("consumer completed");
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
    // The stale values can only be missing because a pass removed them, and
    // this says the cleaner is what removed them rather than the produce or
    // the consumer having gone wrong.
    assert!(
        broker
            .metrics()
            .log_compactions_total
            .get_or_create(&PartitionLabel {
                topic: TOPIC.into(),
                partition: 0,
            })
            .get()
            > 0,
        "the cleaner never compacted this partition"
    );
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/compaction.feature",
    name = "Console consumers observe the latest values after compaction"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_console_consumer_sees_compacted_topic_end_to_end(compacted: CompactedTopic) {
    compacted.broker.expect("running broker").shutdown().await;
}
