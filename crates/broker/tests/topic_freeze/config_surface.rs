//! What the Kafka config APIs make of a freeze.
//!
//! The freeze deliberately looks like a read-only `write.freeze` topic config
//! through `DescribeConfigs`, because that key is the whole of what an operator
//! holding only the JVM tools can see. The resemblance is also the risk: whoever
//! holds `Alter` on a topic is the producing team, and a freeze has to hold
//! against exactly that team, so neither alter path may set the key or clear it.

use {
    crate::{
        control_plane::freeze_scope,
        support::{
            self, InProcess,
            configs::{ConfigMutationOutcome, ConfigOperation, alter_topic_config, topic_config},
        },
        wire::{
            CONTROL, ProduceOutcome, accepted, controlled_fixture, create_topic, produce_outcome,
            refused,
        },
    },
    assert2::check,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
        owned::{
            alter_configs_request::{
                AlterConfigsRequest, AlterConfigsResource as AlterResource,
                AlterableConfig as AlterConfig,
            },
            describe_configs_response::DescribeConfigsResourceResult,
            incremental_alter_configs_request::{
                AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
                IncrementalAlterConfigsRequest,
            },
        },
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

/// Kafka's `RESOURCE_TYPE` for a topic, which both config paths take.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// `IncrementalAlterConfigs` `config_operation` SET.
const CONFIG_OP_SET: i8 = 0;

/// The synthesised read-only topic config that reports the freeze.
const WRITE_FREEZE: &str = "write.freeze";

/// Try to write `write.freeze` through `AlterConfigs`.
async fn alter_configs(client: &Client, topic: &str, value: Option<&str>) -> ConfigMutationOutcome {
    let response = client
        .send(AlterConfigsRequest {
            resources: vec![AlterResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![AlterConfig {
                    name: WRITE_FREEZE.to_owned(),
                    value: value.map(ToOwned::to_owned),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("AlterConfigs");
    let row = &response.responses[0];
    ConfigMutationOutcome {
        error_code: row.error_code,
        error_message: row.error_message.clone(),
    }
}

/// Alter an ordinary topic config, as the control for the four refusals.
async fn client_alter_retention(client: &Client, topic: &str) -> ConfigMutationOutcome {
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![IncrementalConfig {
                    name: "retention.ms".to_owned(),
                    config_operation: CONFIG_OP_SET,
                    value: Some("60000".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            validate_only: false,
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    let row = &response.responses[0];
    ConfigMutationOutcome {
        error_code: row.error_code,
        error_message: row.error_message.clone(),
    }
}

/// Read one topic's `write.freeze` entry through `DescribeConfigs`.
async fn write_freeze_config(client: &Client, topic: &str) -> DescribeConfigsResourceResult {
    topic_config(client, topic, WRITE_FREEZE).await
}

#[derive(Default)]
struct FreezeConfigScenario {
    broker: Option<InProcess>,
    frozen: Uuid,
    control: Uuid,
    mutations: Vec<(&'static str, ConfigMutationOutcome)>,
    ordinary: Option<ConfigMutationOutcome>,
    described: Vec<(&'static str, DescribeConfigsResourceResult)>,
    frozen_write: Option<ProduceOutcome>,
    control_write: Option<ProduceOutcome>,
}

impl FreezeConfigScenario {
    fn broker(&self) -> &InProcess { self.broker.as_ref().expect("the config broker is running") }

    async fn shutdown(self) {
        self.broker
            .expect("the config broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn freeze_config() -> FreezeConfigScenario {
    // Each scenario owns fresh services and observations.
    FreezeConfigScenario {
        broker: None,
        ..Default::default()
    }
}

#[given("orders is frozen for cutover beside an unfrozen config control")]
async fn literal_config_freeze(freeze_config: &mut FreezeConfigScenario) {
    let (broker, frozen, control) = Box::pin(controlled_fixture("orders")).await;
    freeze_scope(&broker.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    freeze_config.broker = Some(broker);
    freeze_config.frozen = frozen;
    freeze_config.control = control;
}

#[given("literal orders and namespace tenant-a are frozen beside a config control")]
async fn config_scope_fixtures(freeze_config: &mut FreezeConfigScenario) {
    let broker = support::start().await;
    create_topic(&broker.broker, &broker.client, "tenant-a.orders").await;
    create_topic(&broker.broker, &broker.client, "orders").await;
    create_topic(&broker.broker, &broker.client, CONTROL).await;
    freeze_scope(&broker.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    freeze_scope(
        &broker.client,
        PATTERN_TYPE_PREFIXED,
        "tenant-a.",
        "offboarding",
    )
    .await;
    freeze_config.broker = Some(broker);
}

#[when("the administrator tries both config APIs to set and clear write.freeze")]
async fn mutate_freeze_config(freeze_config: &mut FreezeConfigScenario) {
    let client = &freeze_config.broker().client;
    let outcomes = vec![
        (
            "AlterConfigs set",
            alter_configs(client, CONTROL, Some("true")).await,
        ),
        (
            "IncrementalAlterConfigs set",
            alter_topic_config()
                .client(client)
                .topic(CONTROL)
                .name(WRITE_FREEZE)
                .maybe_value(Some("true"))
                .call()
                .await,
        ),
        (
            "AlterConfigs clear",
            alter_configs(client, "orders", Some("false")).await,
        ),
        (
            "IncrementalAlterConfigs delete",
            alter_topic_config()
                .client(client)
                .name(WRITE_FREEZE)
                .operation(ConfigOperation::Delete)
                .call()
                .await,
        ),
    ];
    freeze_config.mutations = outcomes;
}

#[then("all four config mutations refuse the controller-managed freeze key")]
fn config_mutations_refused(freeze_config: &FreezeConfigScenario) {
    let refusal = ConfigMutationOutcome {
        error_code: codes::INVALID_CONFIG,
        error_message: Some("topic config write.freeze is controller-managed and read-only; use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it".to_owned()),
    };
    for (label, result) in &freeze_config.mutations {
        check!(*result == refusal, "{label}");
    }
    check!(freeze_config.mutations.len() == 4);
}

#[when("clients attempt writes to orders and its control")]
async fn probe_config_registry(freeze_config: &mut FreezeConfigScenario) {
    freeze_config.frozen_write = Some(
        produce_outcome(
            &freeze_config.broker().broker,
            &freeze_config.broker().client,
            "orders",
            freeze_config.frozen,
        )
        .await,
    );
    freeze_config.control_write = Some(
        produce_outcome(
            &freeze_config.broker().broker,
            &freeze_config.broker().client,
            CONTROL,
            freeze_config.control,
        )
        .await,
    );
}

#[then("orders remains frozen at zero and the config control accepts its first record")]
fn config_registry_unchanged(freeze_config: &FreezeConfigScenario) {
    check!(
        freeze_config
            .frozen_write
            .as_ref()
            .expect("orders was written")
            == &refused("literal", "orders", "cutover", 0)
    );
    check!(
        freeze_config
            .control_write
            .as_ref()
            .expect("control was written")
            == &accepted(1)
    );
}

#[when("the administrator alters ordinary retention on the control topic")]
async fn alter_ordinary_retention(freeze_config: &mut FreezeConfigScenario) {
    freeze_config.ordinary =
        Some(client_alter_retention(&freeze_config.broker().client, CONTROL).await);
}

#[then("the ordinary retention alteration succeeds")]
fn ordinary_config_works(freeze_config: &FreezeConfigScenario) {
    check!(
        freeze_config
            .ordinary
            .as_ref()
            .expect("retention was altered")
            == &ConfigMutationOutcome {
                error_code: codes::NONE,
                error_message: None
            },
        "an ordinary topic config still alters"
    );
}

#[when("the administrator describes write.freeze for literal prefix and unfrozen topics")]
async fn describe_freeze_configs(freeze_config: &mut FreezeConfigScenario) {
    for (label, topic) in [
        ("a topic frozen by its own name", "orders"),
        ("a topic frozen by a namespace", "tenant-a.orders"),
        ("a topic no freeze covers", CONTROL),
    ] {
        let entry = write_freeze_config(&freeze_config.broker().client, topic).await;
        freeze_config.described.push((label, entry));
    }
}

#[then("all freeze configs are read-only and identify the matching scope")]
fn described_scope_configs(freeze_config: &FreezeConfigScenario) {
    for ((label, entry), value) in freeze_config.described.iter().zip([
        "frozen:literal:orders",
        "frozen:prefixed:tenant-a.",
        "false",
    ]) {
        check!(entry.value.as_deref() == Some(value), "{label}");
        check!(entry.read_only, "{label}");
    }
    check!(freeze_config.described.len() == 3);
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/config_surface.feature",
    name = "neither alter path can set or clear a freeze"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neither_alter_path_can_set_or_clear_a_freeze(freeze_config: FreezeConfigScenario) {
    freeze_config.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/config_surface.feature",
    name = "describe configs reports the freeze read only and names the scope"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_freeze_read_only_and_names_the_scope(
    freeze_config: FreezeConfigScenario,
) {
    freeze_config.shutdown().await;
}
