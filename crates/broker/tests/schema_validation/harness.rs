//! The mock Schema Registry, the broker boot path that points at it, and the
//! `CreateTopics` and `Produce` drivers every case in this suite runs through.
//!
//! The registry answers the two endpoints the broker reads, so the cases can
//! be written in terms of a schema id that is bound, bound elsewhere, or not
//! registered at all. `produce` hands back the whole partition response rather
//! than an error code, because a case has to assert on `record_errors` as well.

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, file_config::FileConfig};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::PartitionProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::support;

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
pub fn order_avro_body() -> Vec<u8> {
    vec![0x02, b'a']
}

/// A single-record batch carrying `value`. `None` is a tombstone.
pub fn batch_with_value(value: Option<Bytes>) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.records.push(Record {
        offset_delta: 0,
        value,
        ..Default::default()
    });
    b
}

/// A two-record batch: the first record is fine, the second is not.
pub fn batch_with_values(values: Vec<Option<Bytes>>) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    for (i, value) in values.into_iter().enumerate() {
        b.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value,
            ..Default::default()
        });
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
pub async fn boot(registry_url: &str) -> (BrokerHandle, Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let file: FileConfig = toml::from_str(&format!(
        r#"
        [schema_registry]
        url = "{registry_url}"
        expire_after_ms = 60000
        "#
    ))
    .expect("broker.toml parses");
    file.apply_to(&mut config)
        .expect("[schema_registry] applies");

    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("schema-validation-test")
        .build()
        .await
        .expect("client build");
    (broker, client, dir)
}

/// Create `name` with the given topic configs and wait for its partition.
pub async fn create_topic(
    broker: &BrokerHandle,
    client: &Client,
    name: &str,
    configs: &[(&str, &str)],
) -> WireUuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: (*k).to_owned(),
                        value: Some((*v).to_owned()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
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
    broker.wait_until_partition_present(name, 0).await;
    support::topic_id_for(client, name).await
}

/// Produce one batch and return the whole partition response, so a case can
/// assert on `record_errors` as well as on the error code.
pub async fn produce(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    batch: RecordBatch,
) -> PartitionProduceResponse {
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(RecordsPayload::V2(vec![batch])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    resp.responses[0].partition_responses[0].clone()
}

/// The topic configs that turn `id`-mode value validation on.
pub const VALIDATED: &[(&str, &str)] = &[("schema.validation.value", "true")];
