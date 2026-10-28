//! The mock Schema Registry, the broker boot path that points at it, and the
//! `CreateTopics` and `Produce` drivers every case in this suite runs through.
//!
//! The registry answers the two endpoints the broker reads, so the cases can
//! be written in terms of a schema id that is bound, bound elsewhere, or not
//! registered at all. `produce` hands back the whole partition response rather
//! than an error code, because a case has to assert on `record_errors` as well.

use {
    crate::support::{
        client::start_broker_client,
        produce::{SinglePartitionProduceSetup, send_batch},
        records::value_record,
        topics::{TopicReplicationFactor, creatable_topic, topic_configs},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{BrokerConfig, BrokerHandle, file_config::FileConfig},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            create_topics_request::CreateTopicsRequest, produce_response::PartitionProduceResponse,
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd::Slot,
    rstest_bdd_macros::{given, then, when},
    std::collections::BTreeMap,
    tempfile::TempDir,
    wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    },
};

/// Kafka error 87. KIP-467 added it for "one or more records in the batch were
/// invalid", which is what a schema rejection is.
pub const INVALID_RECORD: i16 = 87;

/// A schema id the mock registry knows, bound to both validated topics.
pub const KNOWN_ID: u32 = 42;
/// A schema id the mock registry knows, bound to some *other* subject.
pub const OTHER_SUBJECT_ID: u32 = 43;
/// A schema id the mock registry answers 404 for.
pub const UNKNOWN_ID: u32 = 99;

/// The Avro schema `KNOWN_ID` resolves to, used by the `full`-mode cases.
pub const ORDER_AVRO: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;

/// Frame a body the way every Confluent serializer does:
/// `0x00 | schema_id(4 BE) | body`.
pub fn framed(id: u32, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(0x00);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(body);
    Bytes::from(out)
}

/// One Avro datum of [`ORDER_AVRO`]: `id = "a"`.
///
/// Hand-encoded rather than pulled from an Avro library, so this test does not
/// depend on one: a `string` is a zig-zag varint length then the bytes, and
/// `1` zig-zag encodes to `0x02`.
pub fn order_avro_body() -> Vec<u8> { vec![0x02, b'a'] }

/// A single-record batch carrying `value`. `None` is a tombstone.
pub fn batch_with_value(value: Option<Bytes>) -> RecordBatch { batch_with_values(vec![value]) }

/// A two-record batch: the first record is fine, the second is not.
pub fn batch_with_values(values: Vec<Option<Bytes>>) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    for (i, value) in values.into_iter().enumerate() {
        b.records
            .push(value_record(i32::try_from(i).unwrap(), value));
    }
    b
}

/// Serve the two registry endpoints the broker reads.
///
/// `KNOWN_ID` is bound to `validated-value` and `validated-full-value`, and
/// resolves to [`ORDER_AVRO`].
/// `OTHER_SUBJECT_ID` resolves, but under a subject no topic here uses.
/// `UNKNOWN_ID` answers 404, which is the registry saying "not registered"
/// rather than failing to answer.
pub async fn registry() -> MockServer {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(format!("/schemas/ids/{KNOWN_ID}/versions")))
        // Bound to BOTH validated topics' subjects. Without the second, the
        // `full`-mode cases would be rejected for the wrong subject rather
        // than for their body, and would pass while proving nothing.
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"subject": "validated-value", "version": 1},
            {"subject": "validated-full-value", "version": 1}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/schemas/ids/{KNOWN_ID}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema": ORDER_AVRO})),
        )
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/schemas/ids/{OTHER_SUBJECT_ID}/versions")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"subject": "somewhere-else-value", "version": 1}
        ])))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/schemas/ids/{UNKNOWN_ID}/versions")))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error_code": 40403, "message": "Schema not found"
        })))
        .mount(&server)
        .await;

    server
}

/// Boot a broker whose `[schema_registry]` points at `registry_url`.
///
/// The configuration goes in through `FileConfig`, the same path a real
/// `broker.toml` takes, so this covers the config wiring as well as the
/// produce path.
pub fn boot(registry_url: &str) -> impl Future<Output = (BrokerHandle, Client, tempfile::TempDir)> {
    Box::pin(async move {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        apply_registry_config(
            &mut config,
            &format!(
                r#"
        [schema_registry]
        url = "{registry_url}"
        expire_after_ms = 60000
        "#
            ),
        );

        let (broker, client) = boot_config(config).await;
        (broker, client, dir)
    })
}

/// A mock-backed topic, retaining the registry and directory in their original order.
pub fn mock_topic_fixture(
    topic: &str,
    configs: &[(&str, &str)],
) -> impl Future<
    Output = (
        MockServer,
        BrokerHandle,
        Client,
        tempfile::TempDir,
        WireUuid,
    ),
> {
    Box::pin(async move {
        let registry = registry().await;
        let (broker, client, dir) = boot(&registry.uri()).await;
        let id = create_topic()
            .broker(&broker)
            .client(&client)
            .name(topic)
            .configs(configs)
            .call()
            .await;
        (registry, broker, client, dir, id)
    })
}

/// Send one nullable value using the suite's timestamp and producer batch fixture.
pub async fn produce_value(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    value: Option<Bytes>,
) -> PartitionProduceResponse {
    produce(client, topic, topic_id, batch_with_value(value)).await
}

/// Create a configured schema-validation topic and wait for its local partition.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SchemaTopicSetup<'a> {
    #[default("validated")]
    pub name: &'a str,
    #[default(VALIDATED)]
    pub configs: &'a [(&'a str, &'a str)],
    #[default(TopicReplicationFactor(1))]
    pub replicas: TopicReplicationFactor,
}

/// The committed `CreateTopics` response supplies the identity, even when another
/// bootstrap broker's Metadata image has not caught up with the commit yet.
#[bon::builder]
pub async fn create_topic(
    broker: &BrokerHandle,
    client: &Client,
    #[builder(default = "validated")] name: &'_ str,
    #[builder(default = VALIDATED)] configs: &'_ [(&'_ str, &'_ str)],
    #[builder(default = TopicReplicationFactor(1))] replicas: TopicReplicationFactor,
) -> WireUuid {
    let setup = SchemaTopicSetup {
        name,
        configs,
        replicas,
    };
    let topic = creatable_topic()
        .name(setup.name.into())
        .replicas(setup.replicas)
        .configs(topic_configs(setup.configs.iter().copied()))
        .call();
    let name = topic.name.clone();
    let name = name.as_str();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![topic],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let created = &resp.topics[0];
    assert!(
        created.error_code == 0,
        "create {name}: {:?}",
        created.error_message
    );
    assert!(
        created.topic_id != WireUuid::ZERO,
        "create {name} answered no topic id"
    );
    broker.wait_until_partition_present(name, 0).await;
    created.topic_id
}

/// Produce one batch and return the whole partition response, so a case can
/// assert on `record_errors` as well as on the error code.
pub async fn produce(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let resp = send_batch(&client, batch, SinglePartitionProduceSetup {
        topic: (topic).into(),
        topic_id,
        ..Default::default()
    })
    .await;
    resp.responses[0].partition_responses[0].clone()
}

/// The topic configs that turn `id`-mode value validation on.
pub const VALIDATED: &[(&str, &str)] = &[("schema.validation.value", "true")];

pub fn boot_config(config: BrokerConfig) -> impl Future<Output = (BrokerHandle, Client)> {
    Box::pin(async move {
        let started = start_broker_client()
            .config(config)
            .client_id("schema-validation-test")
            .call()
            .await;
        (started.broker, started.client)
    })
}

/// Apply the registry settings through the same TOML/`FileConfig` path as the CLI.
///
/// # Panics
/// Panics if the TOML cannot be parsed or its registry section cannot be applied.
pub fn apply_registry_config(config: &mut BrokerConfig, text: &str) {
    let file: FileConfig = toml::from_str(text).expect("broker.toml parses");
    file.apply_to(config).expect("[schema_registry] applies");
}

/// Fresh broker, registry, input values, and captured produce observations.
#[derive(Default)]
pub struct SchemaScenario {
    pub(super) registry: Option<MockServer>,
    pub(super) broker: Option<BrokerHandle>,
    pub(super) client: Option<Client>,
    pub(super) directory: Option<TempDir>,
    pub(super) topics: BTreeMap<String, WireUuid>,
    pub(super) payload: Slot<Option<Bytes>>,
    pub(super) outcomes: BTreeMap<String, PartitionProduceResponse>,
    pub(super) counters: Slot<(u64, u64)>,
}

impl SchemaScenario {
    pub(super) fn broker(&self) -> &BrokerHandle {
        self.broker
            .as_ref()
            .expect("the schema-validation broker is running")
    }

    pub(super) fn client(&self) -> &Client {
        self.client
            .as_ref()
            .expect("the schema-validation client is connected")
    }

    pub(super) fn topic(&self, name: &str) -> WireUuid { self.topics[name] }

    pub(super) fn install(
        &mut self,
        registry: Option<MockServer>,
        started: (BrokerHandle, Client, TempDir),
        topic: WireUuid,
    ) {
        let (broker, client, directory) = started;
        self.registry = registry;
        self.broker = Some(broker);
        self.client = Some(client);
        self.directory = Some(directory);
        self.topics.insert("validated".to_owned(), topic);
    }

    pub(super) async fn shutdown(&mut self) {
        self.broker
            .take()
            .expect("the schema-validation broker is running")
            .shutdown()
            .await;
        drop(self.client.take());
        drop(self.registry.take());
        drop(self.directory.take());
    }

    pub(super) fn capture_cache(&self) {
        self.counters.set((
            self.broker().metrics().schema_validation_cache_misses.get(),
            self.broker().metrics().schema_validation_cache_hits.get(),
        ));
    }
}

#[fixture]
pub(super) fn schema() -> SchemaScenario {
    // Registry instances, data directories, and wire outcomes are per-scenario.
    SchemaScenario {
        broker: None,
        ..Default::default()
    }
}

#[given("a registry binds schema 42 to validated and validated-full subjects")]
async fn bound_schema_registry(schema: &mut SchemaScenario) {
    let (registry, broker, client, directory, id) =
        mock_topic_fixture("validated", VALIDATED).await;
    schema.install(Some(registry), (broker, client, directory), id);
}

#[given("the record carries schema 42 with arbitrary body bytes")]
fn known_schema_value(schema: &SchemaScenario) {
    schema.payload.set(Some(framed(KNOWN_ID, b"anything")));
}

#[given("the record value is a tombstone")]
fn tombstone_value(schema: &SchemaScenario) { schema.payload.set(None); }

#[when("the producer sends the prepared record to {topic}")]
async fn produce_prepared_value(schema: &mut SchemaScenario, topic: String) {
    let value = schema.payload.get().expect("a record value was prepared");
    let out = produce_value(schema.client(), &topic, schema.topic(&topic), value).await;
    schema.outcomes.insert(topic, out);
    schema.capture_cache();
}

#[when("an observer reads the schema-validation cache counters")]
fn observe_schema_cache(schema: &SchemaScenario) { schema.capture_cache(); }

#[then("schema cache misses are {misses} and hits are {hits}")]
fn schema_cache_counts(schema: &SchemaScenario, misses: u64, hits: u64) {
    check!(schema.counters.get() == Some((misses, hits)));
}

#[then("schema produce to {topic} returns error {code} and its log end is {offset}")]
fn schema_append_outcome(schema: &SchemaScenario, topic: String, code: i16, offset: i64) {
    let out = schema
        .outcomes
        .get(&topic)
        .expect("a schema record was produced");
    check!(out.error_code == code, "{out:?}");
    check!(schema.broker().local_log_end_offset(&topic, 0) == Some(offset));
}

#[then("the validated produce succeeds")]
fn validated_produce_succeeded(schema: &SchemaScenario) {
    check!(schema.outcomes["validated"].error_code == 0);
}
