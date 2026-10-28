//! Cross-client produce/consume and log-append timestamp workflows.

use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE, assert_console_values, broker0_advertised, consume_console_partition,
        create_console_topic, docker_run_kafka_tool, nc_check_connectivity, produce_console,
        start_console_broker, start_host_broker,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_producer::{Acks, Compression, Producer, ProducerRecord},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tempfile::TempDir,
};

struct ConsoleExchange {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    values: Vec<String>,
    produced: Option<Output>,
    partitions: Vec<i32>,
    consumed: Option<Output>,
    printed_record: Option<String>,
    timestamp: Option<i64>,
    resolved: String,
}

#[fixture]
fn console() -> ConsoleExchange {
    // Start this scenario with fresh inputs and observations.

    ConsoleExchange {
        broker: None,
        directory: None,
        topic: String::new(),
        values: Vec::new(),
        produced: None,
        partitions: Vec::new(),
        consumed: None,
        printed_record: None,
        timestamp: None,
        resolved: String::new(),
    }
}

#[given("console topic {topic} has one partition")]
async fn console_topic(console: &mut ConsoleExchange, topic: String) {
    let (broker, directory) = start_console_broker(&topic, 1).await;
    console.broker = Some(broker);
    console.directory = Some(directory);
    console.topic = topic;
}

#[given("host topic {topic} is ready for an idempotent Rust LZ4 producer")]
async fn rust_topic(console: &mut ConsoleExchange, topic: String) {
    let (broker, directory) = start_host_broker().await;
    create_console_topic(KAFKA_IMAGE, &[], &topic, 1, 1);
    console.broker = Some(broker);
    console.directory = Some(directory);
    console.topic = topic;
}

#[given("topic {topic} stores broker log-append timestamps")]
async fn append_time_topic(console: &mut ConsoleExchange, topic: String) {
    let (broker, directory) = start_host_broker().await;
    nc_check_connectivity();
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
        "--config",
        "message.timestamp.type=LogAppendTime",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    console.broker = Some(broker);
    console.directory = Some(directory);
    console.topic = topic;
}

#[given("the producer record values are")]
fn producer_values(console: &mut ConsoleExchange, datatable: Vec<Vec<String>>) {
    console.values = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value]: [String; 1] = row.try_into().expect("one record value");
            value
        })
        .collect();
}

#[when("the stock console producer writes those values")]
fn console_produce(console: &mut ConsoleExchange) {
    let mut payload = console.values.join("\n");
    payload.push('\n');
    console.produced = Some(produce_console(
        KAFKA_IMAGE,
        &[],
        &console.topic,
        false,
        payload.as_bytes(),
    ));
}

#[then("the stock producer exits successfully")]
fn console_produced(console: &ConsoleExchange) {
    let output = console
        .produced
        .as_ref()
        .expect("the console producer completed");
    assert!(
        output.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[when("the idempotent Rust LZ4 producer writes and flushes those values")]
async fn rust_produce(console: &mut ConsoleExchange) {
    {
        let producer = Producer::builder()
            .bootstrap(broker0_advertised().to_owned())
            .enable_idempotence(true)
            .acks(Acks::All)
            .compression(Compression::Lz4)
            .build()
            .await
            .expect("producer");
        for value in &console.values {
            let metadata = producer
                .send(ProducerRecord {
                    topic: console.topic.clone(),
                    value: Some(Bytes::copy_from_slice(value.as_bytes())),
                    ..Default::default()
                })
                .await
                .expect("ack");
            console.partitions.push(metadata.partition);
        }
        producer.flush().await.expect("flush");
        producer.close().await
    }
    .expect("close");
}

#[then("every Rust producer acknowledgement names partition zero")]
fn rust_produced(console: &ConsoleExchange) {
    assert!(console.partitions.len() == console.values.len());
    assert!(console.partitions.iter().all(|&partition| partition == 0));
}

#[when("a partition console consumer reads the values within {timeout} milliseconds")]
fn partition_consume(console: &mut ConsoleExchange, timeout: u32) {
    console.consumed = Some(consume_console_partition(
        &console.topic,
        console.values.len(),
        timeout,
        &[],
    ));
}

#[then("the console consumer emits exactly the producer values in order")]
fn consumed_values(console: &ConsoleExchange) {
    let output = console
        .consumed
        .as_ref()
        .expect("the console consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = console
        .values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_console_values(&stdout, &expected, "consumer did not emit");
    assert!(
        stdout.lines().collect::<Vec<_>>() == expected,
        "console output changed record order or values: {stdout:?}"
    );
}

#[when("the console consumer prints one record with its timestamp")]
fn consume_timestamp(console: &mut ConsoleExchange) {
    let output = consume_console_partition(&console.topic, 1, 20_000, &[
        "--property",
        "print.timestamp=true",
    ]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    console.printed_record = stdout
        .lines()
        .find(|line| line.contains("stamped"))
        .map(str::to_owned);
    console.timestamp = console
        .printed_record
        .as_ref()
        .and_then(|line| line.split_once(':'))
        .and_then(|(_, value)| value.split('\t').next())
        .and_then(|value| value.trim().parse().ok());
    console.consumed = Some(output);
}

#[then("the printed record carries a numeric LogAppendTime stamp")]
fn printed_append_time(console: &ConsoleExchange) {
    let line = console
        .printed_record
        .as_ref()
        .expect("a stamped record was printed");
    assert!(
        line.starts_with("LogAppendTime:"),
        "the JVM client must read the stored batch as log-append time: {line:?}"
    );
    assert!(
        console.timestamp.is_some(),
        "the printed stamp is a millisecond clock reading"
    );
}

#[when("GetOffsetShell resolves that printed timestamp")]
fn resolve_timestamp(console: &mut ConsoleExchange) {
    let stamp = console
        .timestamp
        .expect("the consumer printed a numeric timestamp");
    let output = docker_run_kafka_tool(&[
        "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list",
        broker0_advertised(),
        "--topic",
        &console.topic,
        "--time",
        &stamp.to_string(),
    ]);
    console.resolved = String::from_utf8_lossy(&output.stdout).into_owned();
}

#[then("the printed timestamp resolves to partition zero offset zero")]
fn resolved_append_time(console: &ConsoleExchange) {
    assert!(
        console.resolved.contains(&format!("{}:0:0", console.topic)),
        "the stamp must resolve to offset zero: {:?}",
        console.resolved
    );
}

async fn stop_console(console: ConsoleExchange) {
    console
        .broker
        .expect("a host broker was started")
        .shutdown()
        .await;
}

// Blocking Docker commands require another Tokio worker to serve the broker.
#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console.feature",
    name = "Stock console tools round trip partition records without a consumer group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_producer_round_trip(console: ConsoleExchange) { stop_console(console).await; }

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console.feature",
    name = "The stock console consumer reads an idempotent Rust producer's LZ4 records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_producer_to_console_consumer(console: ConsoleExchange) {
    stop_console(console).await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_acceptance_cli/console.feature",
    name = "A log-append timestamp printed by the JVM client resolves back to its record"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn console_consumer_prints_log_append_time(console: ConsoleExchange) {
    stop_console(console).await;
}
