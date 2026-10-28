//! The compressed legacy batches, where v0 and v1 wrap the whole batch in one
//! outer compressed message rather than compressing each record.
//!
//! Both tests drive `legacy_to_v2`, which has to decompress the outer wrapper
//! and re-emit a v2 `RecordBatch` carrying the same compression marker; gzip
//! and snappy reach different codec paths inside it.

use {
    crate::{
        jvm_acceptance::{
            KAFKA_IMAGE_LEGACY, broker0_advertised, docker_run_kafka_tool,
            start_legacy_console_broker,
        },
        support::{ContainerInput, jvm_docker_command, jvm_stdin_output},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tempfile::TempDir,
};
struct CompressedExchange {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    produced: Option<Output>,
    consumed: Option<Output>,
}
#[fixture]
fn compressed() -> CompressedExchange {
    // Start this scenario with fresh inputs and observations.

    CompressedExchange {
        broker: None,
        directory: None,
        topic: String::new(),
        produced: None,
        consumed: None,
    }
}
#[given("a legacy compatible broker hosts compression topic {topic}")]
async fn compression_topic(compressed: &mut CompressedExchange, topic: String) {
    let (broker, directory) = start_legacy_console_broker(&topic).await;
    compressed.broker = Some(broker);
    compressed.directory = Some(directory);
    compressed.topic = topic;
}
#[when("the legacy producer writes fifty numbered records compressed with {compression}")]
fn write(compressed: &mut CompressedExchange, compression: String) {
    // 50 newline-separated records to give gzip something to compress.
    let mut input = String::with_capacity(50 * 12);
    {
        use std::fmt::Write as _;
        for i in 0..50 {
            writeln!(input, "record-{i:03}").unwrap();
        }
    }

    // Produce via legacy with gzip.
    let mut child_command = jvm_docker_command()
        .image(KAFKA_IMAGE_LEGACY)
        .args(&[
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            &compressed.topic,
            "--producer-property",
            &format!("compression.type={compression}"),
            "--producer-property",
            "batch.size=131072", // 128 KiB — enough to batch all 50 records together
            "--producer-property",
            "linger.ms=100", // give the producer time to batch
        ])
        .input(ContainerInput::Attached)
        .call();
    let producer_out = jvm_stdin_output(&mut child_command, input.as_bytes());
    compressed.produced = Some(producer_out);
}
#[then("the compressed legacy producer succeeds")]
fn accepted(compressed: &CompressedExchange) {
    let output = compressed.produced.as_ref().expect("producer completed");
    assert!(
        output.status.success(),
        "legacy compressed producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[when("the modern console consumer reads fifty records from the compression topic")]
fn read(compressed: &mut CompressedExchange) {
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        &compressed.topic,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "15000",
    ]);
    compressed.consumed = Some(consumer_out);
}
#[then("all fifty numbered records survive legacy decompression and format conversion")]
fn decoded(compressed: &CompressedExchange) {
    let output = compressed.consumed.as_ref().expect("consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for i in 0..50 {
        let value = format!("record-{i:03}");
        assert!(
            stdout.contains(&value),
            "modern consumer did not emit {value} after compressed legacy produce: {stdout}"
        );
    }
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_legacy/compression.feature",
    name = "Gzip wrapped legacy records are readable by a modern consumer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_legacy_010_compressed_round_trip(compressed: CompressedExchange) {
    compressed.broker.expect("running broker").shutdown().await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_legacy/compression.feature",
    name = "Snappy wrapped legacy records are readable by a modern consumer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_legacy_010_snappy_round_trip(compressed: CompressedExchange) {
    compressed.broker.expect("running broker").shutdown().await;
}
