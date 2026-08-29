//! KFC-7 broker-side schema validation, end to end against an in-process
//! broker and a faked Schema Registry.
//!
//! Every case drives the real Kafka wire path — `CreateTopics` and `Produce` —
//! and every rejection asserts two things: the error code the producer sees,
//! and that the partition's log end offset did not move. The second assertion
//! is the one that matters. A rejection that still appended the batch would be
//! the worst possible failure of this feature, and an error code alone does
//! not rule it out.
//!
//! Most cases also run against an unvalidated control topic, in the shape
//! KFC-1's suite established. The control half is what shows that a validated
//! topic's behaviour is its configuration and not a path every topic now
//! takes.
//!
//! # The registry is a mock, deliberately
//!
//! These cases are about what the broker does with an answer, not about
//! whether `krabka-schema-registry` gives the right one. `wiremock` serves the
//! two endpoints the broker reads, which is how the OPA authorizer's suite
//! already fakes its decision service. The registry's own conformance to
//! Confluent is asserted in that repository, against a real
//! `cp-schema-registry` container.

mod support;

use assert2::{assert, check};
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

/// Kafka error 87. KIP-467 added it for "one or more records in the batch were
/// invalid", which is what a schema rejection is.
const INVALID_RECORD: i16 = 87;

/// A schema id the mock registry knows, bound to both validated topics.
const KNOWN_ID: u32 = 42;
/// A schema id the mock registry knows, bound to some *other* subject.
const OTHER_SUBJECT_ID: u32 = 43;
/// A schema id the mock registry answers 404 for.
const UNKNOWN_ID: u32 = 99;

/// The Avro schema `KNOWN_ID` resolves to, used by the `full`-mode cases.
const ORDER_AVRO: &str =
    r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#;

/// Frame a body the way every Confluent serializer does:
/// `0x00 | schema_id(4 BE) | body`.
fn framed(id: u32, body: &[u8]) -> Bytes {
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
fn order_avro_body() -> Vec<u8> {
    vec![0x02, b'a']
}

/// A single-record batch carrying `value`. `None` is a tombstone.
fn batch_with_value(value: Option<Bytes>) -> RecordBatch {
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
fn batch_with_values(values: Vec<Option<Bytes>>) -> RecordBatch {
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
async fn registry() -> MockServer {
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
async fn boot(registry_url: &str) -> (BrokerHandle, Client, tempfile::TempDir) {
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
async fn create_topic(
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
async fn produce(
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
const VALIDATED: &[(&str, &str)] = &[("schema.validation.value", "true")];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_framed_with_a_bound_schema_id_is_accepted() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
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

/// The cache counters must move on a real produce.
///
/// Both were declared, registered and documented, and nothing incremented
/// them: a live broker scraped zero for the life of the process. The unit test
/// that called `record_schema_cache_hit` directly proved the counter counts,
/// not that anything counts with it, so the assertion belongs here — behind an
/// actual produce through the validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cache_counters_move_on_a_validated_produce() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    check!(broker.metrics().schema_validation_cache_misses.get() == 0);
    check!(broker.metrics().schema_validation_cache_hits.get() == 0);

    // First produce of this id: nothing cached, so one registry round trip.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;
    check!(out.error_code == 0, "{out:?}");
    check!(broker.metrics().schema_validation_cache_misses.get() == 1);
    check!(broker.metrics().schema_validation_cache_hits.get() == 0);

    // Same id inside the TTL: served from the cache, and counted as a hit.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_value(Some(framed(KNOWN_ID, b"anything"))),
    )
    .await;
    check!(out.error_code == 0, "{out:?}");
    check!(broker.metrics().schema_validation_cache_misses.get() == 1);
    check!(broker.metrics().schema_validation_cache_hits.get() == 1);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_that_fail_validation_are_rejected_and_not_appended() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    let cases: Vec<(&str, Bytes)> = vec![
        // No Confluent frame at all: what a `StringSerializer` writes.
        ("unframed", Bytes::from_static(b"plain text")),
        // A frame whose magic byte is wrong.
        ("bad magic", Bytes::from_static(&[0x01, 0, 0, 0, 42, b'x'])),
        // A frame truncated inside the schema id.
        ("truncated id", Bytes::from_static(&[0x00, 0, 0])),
        // Well framed, but the registry does not know the id.
        ("unknown id", framed(UNKNOWN_ID, b"anything")),
        // Well framed and registered, but under another subject: a producer
        // writing the right format to the wrong topic.
        ("wrong subject", framed(OTHER_SUBJECT_ID, b"anything")),
    ];

    for (name, value) in cases {
        let out = produce(&client, "validated", id, batch_with_value(Some(value))).await;
        check!(out.error_code == INVALID_RECORD, "case {name}: {out:?}");
        check!(
            !out.record_errors.is_empty(),
            "case {name}: no per-record error"
        );
        check!(
            out.record_errors[0].batch_index == 0,
            "case {name}: {:?}",
            out.record_errors
        );
        check!(
            out.record_errors[0]
                .batch_index_error_message
                .as_ref()
                .is_some_and(|m| !m.is_empty()),
            "case {name}: empty message"
        );
        // The assertion that matters: nothing was appended.
        check!(
            broker.local_log_end_offset("validated", 0) == Some(0),
            "case {name}: a rejected batch reached the log"
        );
    }

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unvalidated_topic_accepts_what_a_validated_one_rejects() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let control = create_topic(&broker, &client, "control", &[]).await;
    let validated = create_topic(&broker, &client, "validated", VALIDATED).await;

    let unframed = Bytes::from_static(b"plain text");

    let rejected = produce(
        &client,
        "validated",
        validated,
        batch_with_value(Some(unframed.clone())),
    )
    .await;
    let accepted = produce(
        &client,
        "control",
        control,
        batch_with_value(Some(unframed)),
    )
    .await;

    check!(rejected.error_code == INVALID_RECORD, "{rejected:?}");
    check!(accepted.error_code == 0, "{accepted:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(0));
    check!(broker.local_log_end_offset("control", 0) == Some(1));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tombstone_is_accepted_on_a_validated_topic() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    // A null value is a tombstone. Rejecting it would make schema validation
    // and compaction mutually exclusive.
    let out = produce(&client, "validated", id, batch_with_value(None)).await;

    check!(out.error_code == 0, "{out:?}");
    check!(broker.local_log_end_offset("validated", 0) == Some(1));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rejected_record_is_named_by_its_index_in_the_batch() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;
    let id = create_topic(&broker, &client, "validated", VALIDATED).await;

    // Record 0 is fine; record 1 is not. The batch is rejected whole — its own
    // CRC covers both — and the response says which record caused it.
    let out = produce(
        &client,
        "validated",
        id,
        batch_with_values(vec![
            Some(framed(KNOWN_ID, b"fine")),
            Some(Bytes::from_static(b"plain text")),
        ]),
    )
    .await;

    check!(out.error_code == INVALID_RECORD, "{out:?}");
    check!(out.record_errors.len() == 1, "{:?}", out.record_errors);
    check!(
        out.record_errors[0].batch_index == 1,
        "{:?}",
        out.record_errors
    );
    check!(broker.local_log_end_offset("validated", 0) == Some(0));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_mode_checks_the_body_and_id_mode_does_not() {
    let registry = registry().await;
    let (broker, client, _dir) = boot(&registry.uri()).await;

    let id_mode = create_topic(&broker, &client, "validated", VALIDATED).await;
    let full_mode = create_topic(
        &broker,
        &client,
        "validated-full",
        &[
            ("schema.validation.value", "true"),
            ("schema.validation.mode", "full"),
        ],
    )
    .await;

    // Framed with a bound id, but the body is not an Avro datum of the schema
    // that id names.
    let garbage = framed(KNOWN_ID, b"\xff\xff\xff\xff\xff\xff");

    let under_id = produce(
        &client,
        "validated",
        id_mode,
        batch_with_value(Some(garbage.clone())),
    )
    .await;
    let under_full = produce(
        &client,
        "validated-full",
        full_mode,
        batch_with_value(Some(garbage)),
    )
    .await;

    // `id` mode decides from the header alone, so it admits this.
    check!(under_id.error_code == 0, "{under_id:?}");
    // `full` mode decodes the body, so it does not.
    check!(under_full.error_code == INVALID_RECORD, "{under_full:?}");

    check!(broker.local_log_end_offset("validated", 0) == Some(1));
    check!(broker.local_log_end_offset("validated-full", 0) == Some(0));

    // And a body that IS an instance of the schema passes `full`.
    let good = produce(
        &client,
        "validated-full",
        full_mode,
        batch_with_value(Some(framed(KNOWN_ID, &order_avro_body()))),
    )
    .await;
    check!(good.error_code == 0, "{good:?}");
    check!(broker.local_log_end_offset("validated-full", 0) == Some(1));

    broker.shutdown().await;
}

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
