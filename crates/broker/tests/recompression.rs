#![cfg(test)]

//! Broker-side recompression.
//!
//! The test produces a gzip-compressed batch to a topic configured with
//! `compression.type=lz4`, fetches it back, and asserts the served
//! batch's attributes report `lz4`. This proves the broker re-encoded
//! the batch before it wrote it. The test also verifies the record
//! payload survives the round-trip intact. The broker decompresses the
//! gzip bytes and compresses them with lz4, and the client then
//! decompresses the lz4 bytes.
//!
//! The test is gated to non-Windows. This matches the multi-broker test
//! convention of the other replication and compaction tests.

mod kafka_wire;

mod support;

use {
    crate::support::{
        discovery::named_topic_metadata,
        fetch::single_partition_fetch,
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        topics,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_compression::CompressionType,
    krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
            create_topics_response::CreateTopicsResponse,
            fetch_request::FetchRequest,
            fetch_response::FetchResponse,
            metadata_response::MetadataResponse,
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
        records::{Attributes, RecordBatch},
    },
    std::net::SocketAddr,
};

const CLIENT_ID: &str = "krabka-recompression-test";

/// A running broker and the address its Kafka listener is bound to.
struct StartedBroker {
    handle: BrokerHandle,
    addr: SocketAddr,
}

async fn start_broker() -> StartedBroker {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let _ = log_dir.keep();
    StartedBroker { handle, addr }
}

async fn create_topic_with_compression(addr: SocketAddr, topic: &str, codec: &str) {
    let req = CreateTopicsRequest {
        topics: vec![
            topics::creatable_topic_with_configs()
                .name(topic.into())
                .configs(vec![CreatableTopicConfig {
                    name: "compression.type".into(),
                    value: Some(codec.into()),
                    ..Default::default()
                }])
                .call(),
        ],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let version: i16 = 7;
    let (_, r) = kafka_wire::request_once::<_, CreateTopicsResponse>(
        addr,
        &req,
        (19, version),
        CLIENT_ID,
        (1, true),
    )
    .await;
    assert!(
        r.topics[0].error_code == 0,
        "CreateTopics must succeed for compression.type={codec}: {:?}",
        r.topics[0]
    );
}

async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = named_topic_metadata(topic);
    let version: i16 = 12;
    let (_, r) = kafka_wire::request_once::<_, MetadataResponse>(
        addr,
        &req,
        (3, version),
        CLIENT_ID,
        (1, true),
    )
    .await;
    r.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response")
}

async fn produce_gzip(addr: SocketAddr, topic: &str, topic_id: Uuid, value: &[u8]) {
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Gzip),
        ..batch_from_records(vec![value_record(0, Some(Bytes::copy_from_slice(value)))])
    };
    let req = batch_request(batch, SinglePartitionProduceSetup {
        topic: (topic).into(),
        topic_id,
        ..SinglePartitionProduceSetup::replicated()
    });
    let version: i16 = 9;
    let (_, r) = kafka_wire::request_once::<_, ProduceResponse>(
        addr,
        &req,
        (0, version),
        CLIENT_ID,
        (1, true),
    )
    .await;
    let part = &r.responses[0].partition_responses[0];
    assert!(part.error_code == 0, "Produce must succeed: {part:?}");
}

async fn fetch_first_batch(addr: SocketAddr, topic: &str, topic_id: Uuid) -> RecordBatch {
    let req = FetchRequest {
        replica_id: -1,
        ..single_partition_fetch()
            .topic(topic.into())
            .topic_id(topic_id)
            .call()
    };
    let version: i16 = 12;
    let (_, r) = kafka_wire::request_once::<_, FetchResponse>(
        addr,
        &req,
        (1, version),
        CLIENT_ID,
        (1, true),
    )
    .await;
    let part = &r.responses[0].partitions[0];
    assert!(part.error_code == 0, "Fetch error: {}", part.error_code);
    part.records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(|batches| batches.first().cloned())
        .expect("Fetch returned at least one v2 batch")
}

use {
    crate::support::partitions::wait_for_compression,
    krabka_log::{Log, LogConfig},
    krabka_units::gibibytes,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct CompressionScenario {
    broker: Option<StartedBroker>,
    topic: String,
    topic_id: Option<Uuid>,
    payload: Vec<u8>,
    served: Option<RecordBatch>,
    directory: Option<TempDir>,
    log: Option<Log>,
    batch: Option<RecordBatch>,
    stored: Option<Bytes>,
    encoded: Option<Bytes>,
    level: i32,
}

#[fixture]
fn compression_case() -> CompressionScenario {
    // Broker round trips and owned log writes retain their actual encoded observations.
    CompressionScenario::default()
}

#[given("a topic {topic} whose compression policy is {policy}")]
async fn compression_topic(
    compression_case: &mut CompressionScenario,
    topic: String,
    policy: String,
) {
    let broker = start_broker().await;
    create_topic_with_compression(broker.addr, &topic, &policy).await;
    let compression = match policy.as_str() {
        "lz4" => Some(CompressionType::Lz4),
        "producer" => None,
        _ => panic!("unsupported policy {policy}"),
    };
    wait_for_compression(&broker.handle, &topic, compression).await;
    compression_case.topic_id = Some(get_topic_id(broker.addr, &topic).await);
    compression_case.broker = Some(broker);
    compression_case.topic = topic;
}

#[when("a gzip producer appends payload {payload} and a consumer fetches the first batch")]
async fn compressed_round_trip(compression_case: &mut CompressionScenario, payload: String) {
    let broker = compression_case.broker.as_ref().expect("topic broker");
    let topic_id = compression_case.topic_id.expect("topic identity");
    compression_case.payload = payload.into_bytes();
    produce_gzip(
        broker.addr,
        &compression_case.topic,
        topic_id,
        &compression_case.payload,
    )
    .await;
    compression_case.served =
        Some(fetch_first_batch(broker.addr, &compression_case.topic, topic_id).await);
}

#[then("the fetched batch uses {codec} compression and preserves the original payload")]
fn compressed_batch(compression_case: &CompressionScenario, codec: String) {
    let expected = match codec.as_str() {
        "lz4" => CompressionType::Lz4,
        "gzip" => CompressionType::Gzip,
        _ => panic!("unsupported codec {codec}"),
    };
    let served = compression_case.served.as_ref().expect("batch fetched");
    check!(served.attributes.compression() == expected);
    assert!(served.records.len() == 1);
    check!(served.records[0].value.as_deref() == Some(compression_case.payload.as_slice()));
}

#[given("an owned record batch using {codec} at compression level {level}")]
fn owned_compression(compression_case: &mut CompressionScenario, codec: String, level: i32) {
    let codec = match codec.as_str() {
        "gzip" => CompressionType::Gzip,
        "lz4" => CompressionType::Lz4,
        "zstd" => CompressionType::Zstd,
        _ => panic!("unsupported codec {codec}"),
    };
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(directory.path(), LogConfig::default()).unwrap();
    log.set_config(LogConfig {
        compression_gzip_level: if codec == CompressionType::Gzip {
            level
        } else {
            -1
        },
        compression_lz4_level: if codec == CompressionType::Lz4 {
            level
        } else {
            9
        },
        compression_zstd_level: if codec == CompressionType::Zstd {
            level
        } else {
            3
        },
        ..LogConfig::default()
    });
    let value: Vec<u8> = (0..16 * 1024u32)
        .map(|i| u8::try_from((i * 7 + i / 13) % 251).unwrap())
        .collect();
    compression_case.batch = Some(RecordBatch {
        attributes: Attributes::default().with_compression(codec),
        ..batch_from_records(vec![value_record(0, Some(Bytes::from(value)))])
    });
    compression_case.directory = Some(directory);
    compression_case.log = Some(log);
    compression_case.level = level;
}

#[when("the log appends the owned batch and reads its stored encoded bytes")]
fn append_owned_compression(compression_case: &mut CompressionScenario) {
    let log = compression_case.log.as_mut().expect("configured log");
    let batch = compression_case.batch.as_mut().expect("owned batch");
    let (base, _) = log.append(batch).unwrap();
    compression_case.encoded = Some(encoded_at_level(
        batch,
        CompressionLevel(compression_case.level),
    ));
    compression_case.stored = Some(
        log.read_raw(base, log.log_end_offset(), gibibytes(1))
            .unwrap()
            .bytes,
    );
}

#[then("the stored batch exactly matches encoding at the configured compression level")]
fn stored_compression_level(compression_case: &CompressionScenario) {
    check!(
        compression_case
            .stored
            .as_ref()
            .expect("stored encoded bytes")
            == compression_case
                .encoded
                .as_ref()
                .expect("reference encoding")
    );
}

#[scenario(
    path = "tests/features/migrated/tests/recompression.feature",
    name = "topic compression lz4 recompresses producer gzip batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_lz4_recompresses_producer_gzip_batch(
    compression_case: CompressionScenario,
) {
    compression_case
        .broker
        .take()
        .expect("broker")
        .handle
        .shutdown()
        .await;
    drop(compression_case);
}

#[scenario(
    path = "tests/features/migrated/tests/recompression.feature",
    name = "topic compression producer preserves producer gzip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_producer_preserves_producer_gzip(compression_case: CompressionScenario) {
    compression_case
        .broker
        .take()
        .expect("broker")
        .handle
        .shutdown()
        .await;
    drop(compression_case);
}

#[scenario(
    path = "tests/features/migrated/tests/recompression.feature",
    name = "stored owned batches honor every codec level"
)]
fn stored_owned_batches_honor_every_codec_level(
    compression_case: CompressionScenario,
    codec: String,
    level: i32,
) {
    let _ = (compression_case, codec, level);
}

krabka_macros::compression_bytes_fixture!(encoded_at_level, CompressionLevel);
