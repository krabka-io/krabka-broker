//! The config surface the JVM tools drive: `kafka-topics --create --config
//! max.message.bytes=...`, `kafka-configs --alter --add-config`, and the
//! `DescribeConfigs` read-back both of them show.

use {
    crate::{
        support::{
            self,
            configs::{ConfigMutationOutcome, alter_topic_config, topic_config},
        },
        wire::{MAX_MESSAGE_BYTES, accepted, create_topic, produce_batch_of_wire_len, too_large},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult,
};

/// `ConfigSource::DYNAMIC_TOPIC_CONFIG`, which is what a stored topic
/// override reports.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;

/// `DescribeConfigsResponse.ConfigType::INT`, the byte the JVM `AdminClient`
/// reads out of `ConfigEntry.type()`. Kafka declares `max.message.bytes` as
/// `INT`, so the entry carries `3` rather than the untyped `UNKNOWN`.
const CONFIG_TYPE_INT: i8 = 3;

const CAP: usize = 2048;

use {
    krabka_protocol::{owned::produce_response::PartitionProduceResponse, primitives::uuid::Uuid},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};
struct MessageCap {
    process: Option<support::InProcess>,
    topic: Uuid,
    described: Option<DescribeConfigsResourceResult>,
    appended: Option<PartitionProduceResponse>,
    changed: Option<ConfigMutationOutcome>,
    alterations: Vec<(String, ConfigMutationOutcome)>,
}
#[fixture]
fn cap() -> MessageCap {
    // Start this scenario with fresh inputs and observations.

    MessageCap {
        process: None,
        topic: Uuid::ZERO,
        described: None,
        appended: None,
        changed: None,
        alterations: Vec::new(),
    }
}
#[given("orders has a maximum message size of 2048 bytes")]
async fn configured(cap: &mut MessageCap) {
    let process = support::start().await;
    cap.topic = create_topic(&process.broker, &process.client, "orders", &[(
        MAX_MESSAGE_BYTES,
        &CAP.to_string(),
    )])
    .await;
    cap.process = Some(process);
}
#[given("orders uses the broker's default maximum message size")]
async fn default_cap(cap: &mut MessageCap) {
    let process = support::start().await;
    cap.topic = create_topic(&process.broker, &process.client, "orders", &[]).await;
    cap.process = Some(process);
}
#[when("the operator describes the maximum message size")]
async fn describe(cap: &mut MessageCap) {
    let process = cap.process.as_ref().expect("orders created");
    cap.described = Some(describe_max_message_bytes(&process.client, "orders").await);
}
#[then("the complete config entry reports a mutable dynamic topic integer of 2048")]
fn entry(cap: &MessageCap) {
    check!(
        *cap.described.as_ref().expect("config described")
            == DescribeConfigsResourceResult {
                name: MAX_MESSAGE_BYTES.to_owned(),
                value: Some(CAP.to_string()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: vec![],
                config_type: CONFIG_TYPE_INT,
                documentation: None,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
    );
}
#[when("a producer submits a 2049 byte batch")]
async fn append(cap: &mut MessageCap) {
    let process = cap.process.as_ref().expect("orders created");
    cap.appended =
        Some(produce_batch_of_wire_len(&process.client, "orders", cap.topic, CAP + 1).await);
}
#[then("the complete produce response refuses the oversized batch")]
fn oversized(cap: &MessageCap) {
    check!(*cap.appended.as_ref().expect("produce completed") == too_large());
}
#[when("the operator incrementally raises the cap to 2049 bytes")]
async fn raise(cap: &mut MessageCap) {
    let process = cap.process.as_ref().expect("orders created");
    cap.changed =
        Some(set_max_message_bytes(&process.client, "orders", &(CAP + 1).to_string()).await);
}
#[then("the alteration succeeds without an error message")]
fn changed(cap: &MessageCap) {
    check!(
        *cap.changed.as_ref().expect("alter completed")
            == ConfigMutationOutcome {
                error_code: codes::NONE,
                error_message: None
            }
    );
}
#[then("the described maximum message size is 2049 bytes")]
fn raised(cap: &MessageCap) {
    check!(cap.described.as_ref().expect("config described").value == Some((CAP + 1).to_string()));
}
#[then("the same batch is accepted at offset zero")]
fn accepted_batch(cap: &MessageCap) {
    check!(*cap.appended.as_ref().expect("produce completed") == accepted(0, 0));
}
#[when("the operator tries the maximum message size values")]
async fn boundaries(cap: &mut MessageCap, datatable: Vec<Vec<String>>) {
    let process = cap.process.as_ref().expect("orders created");
    for row in datatable.into_iter().skip(1) {
        let value = row.into_iter().next().expect("config value");
        let result = set_max_message_bytes(&process.client, "orders", &value).await;
        cap.alterations.push((value, result));
    }
}
#[then("integer boundary values have the expected errors and refusal messages")]
fn validated(cap: &MessageCap, datatable: Vec<Vec<String>>) {
    let expected: Vec<(String, i16)> = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let columns: [String; 2] = row.try_into().expect("value and code");
            let [value, code] = columns;
            (value, code.parse().expect("integer error code"))
        })
        .collect();
    check!(cap.alterations.len() == expected.len());
    for ((value, result), (wanted, code)) in cap.alterations.iter().zip(expected) {
        check!(*value == wanted);
        check!(
            result.error_code == code,
            "max.message.bytes={value} answered {}",
            result.error_code
        );
        check!(
            result.error_message.is_some() == (code != codes::NONE),
            "max.message.bytes={value} answered {:?}",
            result.error_message
        );
    }
}
/// Read the topic's `max.message.bytes` entry back through `DescribeConfigs`.
async fn describe_max_message_bytes(client: &Client, topic: &str) -> DescribeConfigsResourceResult {
    topic_config(client, topic, MAX_MESSAGE_BYTES).await
}

/// Drive `kafka-configs --alter --add-config max.message.bytes=<value>`.
async fn set_max_message_bytes(client: &Client, topic: &str, value: &str) -> ConfigMutationOutcome {
    alter_topic_config()
        .client(client)
        .topic(topic)
        .name(MAX_MESSAGE_BYTES)
        .maybe_value(Some(value))
        .call()
        .await
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/config_surface.feature",
    name = "A created message cap is visible as a mutable integer topic override"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_accepts_the_key_and_describe_configs_echoes_it(cap: MessageCap) {
    cap.process
        .expect("cap broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/config_surface.feature",
    name = "Raising a live cap admits the previously refused batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incremental_alter_raises_the_cap_for_the_next_produce(cap: MessageCap) {
    cap.process
        .expect("cap broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/config_surface.feature",
    name = "Alter configuration applies Kafka integer boundary validation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_alter_path_rejects_the_values_kafka_rejects(cap: MessageCap) {
    cap.process
        .expect("cap broker started")
        .broker
        .shutdown()
        .await;
}
