//! What the broker does when the registry cannot answer, as opposed to
//! answering "not registered".
//!
//! A broker with no `[schema_registry]` section and a broker whose registry
//! answers 500 both fail closed, because admitting the record would make the
//! topic's setting a lie. `fail_open` is the deliberate opt-out, and it is the
//! only configuration here that admits an unresolvable record.

use assert2::check;
use krabka_broker::{Broker, BrokerConfig, file_config::FileConfig};
use krabka_client_core::Client;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use crate::harness::{
    INVALID_RECORD, KNOWN_ID, VALIDATED, batch_with_value, boot, create_topic, framed, produce,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_with_no_registry_rejects_a_topic_that_asks_for_validation() {
    // No `[schema_registry]` section at all.
    let dir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("schema-validation-test")
        .build()
        .await
        .expect("client build");

    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    // Fail closed. Admitting the record would make the topic's setting a lie.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;

    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(0));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_registry_fails_closed_by_default() {
    // A server that accepts the connection and then answers 500 for
    // everything: the registry failing to answer, rather than answering "not
    // registered".
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&registry)
        .await;

    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;

    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(0));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_open_admits_a_record_the_registry_could_not_answer_for() {
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&registry)
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let file: FileConfig = toml::from_str(&format!(
        r#"
        [schema_registry]
        url = "{}"
        fail_open = true
        "#,
        registry.uri()
    ))
    .expect("broker.toml parses");
    file.apply_to(&mut config)
        .expect("[schema_registry] applies");

    let broker = Broker::start(config).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("schema-validation-test")
        .build()
        .await
        .expect("client build");
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;

    check!(out.error_code == 0, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(1));

    broker.shutdown().await;
}
