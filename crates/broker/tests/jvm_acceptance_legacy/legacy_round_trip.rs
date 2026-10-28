//! Legacy and modern stock clients share the same observable record exchange.
use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE, KAFKA_IMAGE_LEGACY, assert_successful_jvm_output, consume_console,
        produce_console, start_legacy_console_broker,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::process::Output,
    tempfile::TempDir,
};

pub struct RecordExchange {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    topic: String,
    produced: Option<Output>,
    consumed: Option<Output>,
}
#[fixture]
pub fn exchange() -> RecordExchange {
    // Start this scenario with fresh inputs and observations.

    RecordExchange {
        broker: None,
        directory: None,
        topic: String::new(),
        produced: None,
        consumed: None,
    }
}
#[given("a conversion capable broker hosts topic {topic}")]
async fn conversion_topic(exchange: &mut RecordExchange, topic: String) {
    let (broker, directory) = start_legacy_console_broker(&topic).await;
    exchange.broker = Some(broker);
    exchange.directory = Some(directory);
    exchange.topic = topic;
}
fn image(version: &str) -> &'static str {
    match version {
        "legacy" => KAFKA_IMAGE_LEGACY,
        "modern" => KAFKA_IMAGE,
        _ => panic!("unknown stock client version {version}"),
    }
}
#[when("the {version} console producer writes alpha bravo and charlie")]
fn write(exchange: &mut RecordExchange, version: String) {
    let selected = image(&version);
    exchange.produced = Some(produce_console(
        selected,
        &[],
        &exchange.topic,
        selected == KAFKA_IMAGE_LEGACY,
        b"alpha\nbravo\ncharlie\n",
    ));
}
#[then("the stock producer completes successfully")]
fn produced(exchange: &RecordExchange) {
    assert_successful_jvm_output(exchange.produced.as_ref().expect("producer completed"));
}
#[when("the {version} console consumer reads three records from the beginning")]
fn read(exchange: &mut RecordExchange, version: String) {
    let selected = image(&version);
    exchange.consumed = Some(consume_console(
        selected,
        &[],
        &exchange.topic,
        None,
        3,
        10000,
        selected != KAFKA_IMAGE_LEGACY,
    ));
}
#[then("the stock consumer decodes alpha bravo and charlie")]
fn decoded(exchange: &RecordExchange) {
    let output = exchange.consumed.as_ref().expect("consumer completed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for value in ["alpha", "bravo", "charlie"] {
        assert!(
            stdout.contains(value),
            "consumer did not emit {value}: status={} stdout={stdout:?} stderr={stderr:?}",
            output.status
        );
    }
}
pub async fn shutdown(exchange: RecordExchange) {
    exchange
        .broker
        .expect("running conversion broker")
        .shutdown()
        .await;
}

#[ignore = "requires Docker"]
#[scenario(
    path = "tests/features/migrated/tests/jvm_legacy_record_exchange.feature",
    name = "Legacy clients round trip through both record conversions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jvm_legacy_010_round_trip(exchange: RecordExchange) { shutdown(exchange).await; }
