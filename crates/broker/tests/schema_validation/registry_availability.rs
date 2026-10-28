//! What the broker does when the registry cannot answer, as opposed to
//! answering "not registered".
//!
//! A broker with no `[schema_registry]` section and a broker whose registry
//! answers 500 both fail closed, because admitting the record would make the
//! topic's setting a lie. `fail_open` is the deliberate opt-out, and it is the
//! only configuration here that admits an unresolvable record.

use {
    crate::harness::{
        INVALID_RECORD, KNOWN_ID, apply_registry_config, boot, boot_config, check_value_append,
        create_topic, framed,
    },
    krabka_broker::BrokerConfig,
    krabka_client_core::Client,
    wiremock::{Mock, MockServer, ResponseTemplate, matchers::method},
};

#[cfg(test)]
#[rstest_bdd_macros::then(
    "schema validation registry availability: a registry that cannot answer fails closed for {example}"
)]
async fn bdd_verify_a_registry_that_cannot_answer_fails_closed(example: String) {
    type Example = (bool,);
    let (registry_returns_500,): Example = match example.as_str() {
        "no registry section" => (false,),
        "registry returns 500" => (true,),
        _ => panic!("unknown BDD example: {example}"),
    };

    let registry = if registry_returns_500 {
        Some(unavailable_registry().await)
    } else {
        None
    };
    let (broker, client, _dir) = if let Some(registry) = &registry {
        boot(&registry.uri()).await
    } else {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let (broker, client) = boot_config(config).await;
        (broker, client, dir)
    };

    expect_fail_closed(&broker, &client).await;
    broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/schema_validation/registry_availability.feature",
    name = "a registry that cannot answer fails closed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registry_that_cannot_answer_fails_closed(example: String) { let _ = example; }

#[cfg(test)]
#[rstest_bdd_macros::then(
    "schema validation registry availability: fail open admits a record the registry could not answer for"
)]
async fn bdd_verify_fail_open_admits_a_record_the_registry_could_not_answer_for() {
    let registry = unavailable_registry().await;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let reg_url = registry.uri();
    let toml = format!("[schema_registry]\nurl = \"{reg_url}\"\nfail_open = true\n");
    apply_registry_config(&mut config, &toml);

    let (broker, client) = boot_config(config).await;
    let id = create_topic().broker(&broker).client(&client).call().await;

    check_value_append(
        &broker,
        &client,
        "validated",
        id,
        Some(framed(KNOWN_ID, b"anything")),
        (0, Some(1)),
    )
    .await;

    broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/schema_validation/registry_availability.feature",
    name = "fail open admits a record the registry could not answer for"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_open_admits_a_record_the_registry_could_not_answer_for() {}

async fn expect_fail_closed(broker: &krabka_broker::BrokerHandle, client: &Client) {
    let id = create_topic().broker(broker).client(client).call().await;

    check_value_append(
        broker,
        client,
        "validated",
        id,
        Some(framed(KNOWN_ID, b"anything")),
        (INVALID_RECORD, Some(0)),
    )
    .await;
}

async fn unavailable_registry() -> MockServer {
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&registry)
        .await;
    registry
}
