//! Registry unavailability fails closed unless `fail_open` is explicitly configured.

use {
    crate::harness::{
        SchemaScenario, apply_registry_config, boot, boot_config, create_topic, schema,
    },
    krabka_broker::BrokerConfig,
    rstest_bdd_macros::{given, scenario},
    wiremock::{Mock, MockServer, ResponseTemplate, matchers::method},
};

#[given("the schema registry is {availability} and fail_open is {fail_open}")]
async fn registry_failure_policy(
    schema: &mut SchemaScenario,
    availability: String,
    fail_open: bool,
) {
    let registry = match availability.as_str() {
        "unconfigured" => None,
        "unavailable" => Some(unavailable_registry().await),
        _ => panic!("a registry availability condition"),
    };
    let (broker, client, directory) = if fail_open {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut config = BrokerConfig::for_tests(directory.path().to_path_buf());
        let url = registry
            .as_ref()
            .expect("the unavailable registry was started")
            .uri();
        apply_registry_config(
            &mut config,
            &format!("[schema_registry]\nurl = \"{url}\"\nfail_open = true\n"),
        );
        let (broker, client) = boot_config(config).await;
        (broker, client, directory)
    } else if let Some(registry) = &registry {
        boot(&registry.uri()).await
    } else {
        let directory = tempfile::tempdir().expect("tempdir");
        let (broker, client) =
            boot_config(BrokerConfig::for_tests(directory.path().to_path_buf())).await;
        (broker, client, directory)
    };
    let id = create_topic().broker(&broker).client(&client).call().await;
    schema.install(registry, (broker, client, directory), id);
}

async fn unavailable_registry() -> MockServer {
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&registry)
        .await;
    registry
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/registry_availability.feature",
    name = "a registry that cannot answer fails closed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registry_that_cannot_answer_fails_closed(schema: SchemaScenario, availability: String) {
    let _ = availability;
    schema.shutdown().await;
    drop(schema);
}

#[scenario(
    path = "tests/features/migrated/tests/schema_validation/registry_availability.feature",
    name = "fail open admits a record the registry could not answer for"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_open_admits_a_record_the_registry_could_not_answer_for(schema: SchemaScenario) {
    schema.shutdown().await;
    drop(schema);
}
