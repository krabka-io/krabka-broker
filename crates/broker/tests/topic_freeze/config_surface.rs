//! What the Kafka config APIs make of a freeze.
//!
//! The freeze deliberately looks like a read-only `write.freeze` topic config
//! through `DescribeConfigs`, because that key is the whole of what an operator
//! holding only the JVM tools can see. The resemblance is also the risk: whoever
//! holds `Alter` on a topic is the producing team, and a freeze has to hold
//! against exactly that team, so neither alter path may set the key or clear it.

use assert2::{assert, check};
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
    owned::{
        alter_configs_request::{
            AlterConfigsRequest, AlterConfigsResource as AlterResource,
            AlterableConfig as AlterConfig,
        },
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResourceResult,
        incremental_alter_configs_request::{
            AlterConfigsResource as IncrementalResource, AlterableConfig as IncrementalConfig,
            IncrementalAlterConfigsRequest,
        },
    },
};

use crate::{
    control_plane::freeze_scope,
    support,
    wire::{CONTROL, accepted, create_topic, produce_outcome, refused},
};

/// Kafka's `RESOURCE_TYPE` for a topic, which both config paths take.
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// `IncrementalAlterConfigs` `config_operation` SET.
const CONFIG_OP_SET: i8 = 0;

/// `config_operation` DELETE, which is how an operator would try to *clear* a
/// freeze through the config path.
const CONFIG_OP_DELETE: i8 = 1;

/// The synthesised read-only topic config that reports the freeze.
const WRITE_FREEZE: &str = "write.freeze";

/// The one row an alter path answers with, in the two terms an operator reads.
type AlterOutcome = (i16, Option<String>);

/// Try to write `write.freeze` through `AlterConfigs`.
async fn alter_configs(client: &Client, topic: &str, value: Option<&str>) -> AlterOutcome {
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
    (row.error_code, row.error_message.clone())
}

/// Try to write `write.freeze` through `IncrementalAlterConfigs`.
async fn incremental_alter_configs(
    client: &Client,
    topic: &str,
    operation: i8,
    value: Option<&str>,
) -> AlterOutcome {
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![IncrementalResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configs: vec![IncrementalConfig {
                    name: WRITE_FREEZE.to_owned(),
                    config_operation: operation,
                    value: value.map(ToOwned::to_owned),
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
    (row.error_code, row.error_message.clone())
}

/// Neither config path can set a freeze, and neither can clear one.
///
/// The freeze deliberately looks like a topic config through `DescribeConfigs`,
/// which is the whole reason the JVM tools can see it. That resemblance is also
/// the risk: whoever holds `Alter` on a topic is the producing team, and a
/// freeze has to hold against exactly that team. So this case comes at the key
/// from all four directions an operator could try — set and delete, on both
/// alter APIs — and asserts the freeze is still in force afterwards. The last
/// two assertions are the point: an ordinary topic config still alters, so the
/// four refusals are the key being refused by name and not the alter path being
/// broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neither_alter_path_can_set_or_clear_a_freeze() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;

    let refusal = (
        codes::INVALID_CONFIG,
        Some(
            "topic config write.freeze is controller-managed and read-only; use `krabka-guard \
             freeze set` to set it and `krabka-guard freeze clear` to clear it"
                .to_owned(),
        ),
    );

    // Setting a freeze on the topic that has none.
    check!(
        alter_configs(&p.client, CONTROL, Some("true")).await == refusal,
        "AlterConfigs set"
    );
    check!(
        incremental_alter_configs(&p.client, CONTROL, CONFIG_OP_SET, Some("true")).await == refusal,
        "IncrementalAlterConfigs set"
    );
    // Clearing the freeze on the topic that has one.
    check!(
        alter_configs(&p.client, "orders", Some("false")).await == refusal,
        "AlterConfigs clear"
    );
    check!(
        incremental_alter_configs(&p.client, "orders", CONFIG_OP_DELETE, None).await == refusal,
        "IncrementalAlterConfigs delete"
    );

    // The registry is untouched by all four, and the control topic gained none.
    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 0)
    );
    check!(produce_outcome(&p.broker, &p.client, CONTROL, control).await == accepted(1));

    // The alter path itself still works, so the four refusals above are about
    // the key and not about the API.
    let ordinary = client_alter_retention(&p.client, CONTROL).await;
    check!(
        ordinary == (codes::NONE, None),
        "an ordinary topic config still alters"
    );

    p.broker.shutdown().await;
}

/// Alter an ordinary topic config, as the control for the four refusals.
async fn client_alter_retention(client: &Client, topic: &str) -> AlterOutcome {
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
    (row.error_code, row.error_message.clone())
}

/// Read one topic's `write.freeze` entry through `DescribeConfigs`.
async fn write_freeze_config(client: &Client, topic: &str) -> DescribeConfigsResourceResult {
    let response = client
        .send(DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.to_owned(),
                configuration_keys: Some(vec![WRITE_FREEZE.to_owned()]),
                ..Default::default()
            }],
            include_synonyms: false,
            include_documentation: false,
            ..Default::default()
        })
        .await
        .expect("DescribeConfigs");
    let result = &response.results[0];
    assert!(
        result.error_code == codes::NONE,
        "DescribeConfigs({topic}): {result:?}"
    );
    result
        .configs
        .iter()
        .find(|entry| entry.name == WRITE_FREEZE)
        .cloned()
        .unwrap_or_else(|| panic!("no {WRITE_FREEZE} entry for {topic}"))
}

/// `kafka-configs --describe` shows the freeze, read-only, naming the scope.
///
/// An operator who holds only the JVM tools cannot call `DescribeTopicFreezes`,
/// so this key is the whole of what they can see. The value has to name the
/// scope rather than say `true`, because the thaw is a different command
/// depending on whether one topic or a thousand-topic namespace is frozen. The
/// unfrozen control reports `false` rather than nothing, because an absent key
/// cannot be told apart from a broker that does not have the feature.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_configs_reports_the_freeze_read_only_and_names_the_scope() {
    let p = support::start().await;
    create_topic(&p.broker, &p.client, "tenant-a.orders").await;
    create_topic(&p.broker, &p.client, "orders").await;
    create_topic(&p.broker, &p.client, CONTROL).await;

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "offboarding").await;

    for (label, topic, value, read_only) in [
        (
            "a topic frozen by its own name",
            "orders",
            "frozen:literal:orders",
            true,
        ),
        (
            "a topic frozen by a namespace",
            "tenant-a.orders",
            "frozen:prefixed:tenant-a.",
            true,
        ),
        ("a topic no freeze covers", CONTROL, "false", true),
    ] {
        let entry = write_freeze_config(&p.client, topic).await;
        check!(entry.value.as_deref() == Some(value), "{label}");
        check!(entry.read_only == read_only, "{label}");
    }

    p.broker.shutdown().await;
}
