//! Scenarios for `tests/features/handler_fetch_zstd_topics`: a consumer that
//! sends Fetch older than v10 is refused every partition of a topic whose
//! `compression.type` is zstd. A follower broker is not.

use {
    super::test_support::{
        TopicRepresentation, expected_refused_partition, expected_single_topic, fetch_wire,
        request_partition, sessionless_request, start_with_idle_follower,
    },
    crate::{
        codes::{NONE, UNSUPPORTED_COMPRESSION_TYPE},
        config_keys::COMPRESSION_TYPE,
        handlers::test_support::{
            StartedBroker, configured_topic_request, created_topic_response, produce_records,
            record_batch, seed_partition_replicas, wait_for_bound_partition,
        },
        test_support::{KafkaErrorCode, append_partition_batch, start_broker_no_audit},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_bdd::{Given, given, then, when},
    krabka_ids::{ApiVersion, Offset, PartitionIndex},
    krabka_metadata::{MetadataRecord, NodeId, TopicConfigRecord},
    krabka_protocol::{
        owned::{
            fetch_request::{FetchRequest, FetchTopic},
            fetch_response::{FetchResponse, PartitionData},
        },
        primitives::uuid::Uuid,
        records::RecordsPayload,
    },
    std::{collections::BTreeMap, sync::atomic},
};

/// The topic every scenario creates.
const TOPIC: &str = "orders";

/// The broker under test, which leads every partition.
const THIS_NODE: NodeId = NodeId(1);

/// The follower replica of a replicated topic.
const FOLLOWER: NodeId = NodeId(2);

/// The value of the one record a scenario appends.
const RECORD_VALUE: &[u8] = b"v";

/// The `compression.type` a scenario's topic is created with.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum TopicCodec {
    Zstd,
    Gzip,
}

impl TopicCodec {
    const fn config_value(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Gzip => "gzip",
        }
    }
}

struct ZstdTopicScenario {
    broker: Given<StartedBroker>,
    response: Given<FetchResponse>,
}

#[rstest::fixture]
fn zstd_topic() -> ZstdTopicScenario {
    ZstdTopicScenario {
        broker: Given::new(
            "broker",
            "Given a topic with compression.type <codec> holds one record",
        ),
        response: Given::new(
            "response",
            "When a consumer fetches the topic with Fetch v<version>",
        ),
    }
}

#[given("a topic with compression.type {codec} holds one record")]
async fn topic_holds_one_record(zstd_topic: &mut ZstdTopicScenario, codec: TopicCodec) {
    let started = start_broker_no_audit().await;
    created_topic_response()
        .broker(&started.broker)
        .client_id("admin-client")
        .request(|| {
            configured_topic_request()
                .topic(TOPIC)
                .configs(&[(COMPRESSION_TYPE, codec.config_value())])
                .call()
        })
        .call()
        .await;
    started.broker.wait_until_partition_present(TOPIC, 0).await;
    produce_records()
        .broker(&started.broker)
        .topic(TOPIC)
        .count(1)
        .call()
        .await;
    zstd_topic.broker.set(started);
}

#[given("a replicated topic with compression.type zstd holds one record")]
async fn replicated_topic_holds_one_record(zstd_topic: &mut ZstdTopicScenario) {
    let started = start_with_idle_follower().await;
    seed_partition_replicas()
        .broker(&started.broker)
        .topic(TOPIC)
        .leader(THIS_NODE)
        .replicas(&[THIS_NODE, FOLLOWER])
        .call()
        .await;
    started
        .broker
        .submit_metadata_record_for_test(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.to_owned(),
            overrides: BTreeMap::from([(
                COMPRESSION_TYPE.to_owned(),
                TopicCodec::Zstd.config_value().to_owned(),
            )]),
        }))
        .await
        .expect("submit topic config record");
    started
        .broker
        .wait_for_image(|image| {
            image
                .topic_config(TOPIC)
                .is_some_and(|config| config.contains_key(COMPRESSION_TYPE))
        })
        .await;
    let partition = wait_for_bound_partition(
        &started.broker.broker_arc_for_test(),
        TOPIC,
        async |partition| {
            partition.current_leader.load(atomic::Ordering::Acquire) == THIS_NODE.0
                && partition
                    .replica_state
                    .lock()
                    .await
                    .isr
                    .contains(&krabka_raft::NodeId(FOLLOWER.0))
        },
        "the broker leads the partition with the follower in sync",
    )
    .await;
    append_partition_batch(&partition, &mut record_batch(1));
    zstd_topic.broker.set(started);
}

#[when("a consumer fetches the topic with Fetch v{version}")]
async fn consumer_fetches(zstd_topic: &mut ZstdTopicScenario, version: i16) {
    let request = FetchRequest {
        topics: vec![FetchTopic {
            topic: TOPIC.to_owned(),
            partitions: vec![request_partition(Offset(0))],
            ..Default::default()
        }],
        ..sessionless_request().version(version).call()
    };
    let response = fetch_wire()
        .broker(&zstd_topic.broker.get().broker)
        .request(&request)
        .version(ApiVersion(version))
        .call()
        .await;
    zstd_topic.response.set(response);
}

#[when("the follower broker fetches the topic with Fetch v{version}")]
async fn follower_fetches(zstd_topic: &mut ZstdTopicScenario, version: i16) {
    let request = FetchRequest {
        topics: vec![FetchTopic {
            topic: TOPIC.to_owned(),
            partitions: vec![request_partition(Offset(0))],
            ..Default::default()
        }],
        ..sessionless_request()
            .version(version)
            .replica(i32::try_from(FOLLOWER.0).expect("small node id"))
            .call()
    };
    let response = fetch_wire()
        .broker(&zstd_topic.broker.get().broker)
        .request(&request)
        .version(ApiVersion(version))
        .call()
        .await;
    zstd_topic.response.set(response);
}

#[then("the partition answers UNSUPPORTED_COMPRESSION_TYPE with no records")]
fn partition_refused(zstd_topic: &ZstdTopicScenario) {
    let expected = expected_single_topic(
        TopicRepresentation::Name,
        (TOPIC, Uuid::ZERO),
        expected_refused_partition(
            PartitionIndex(0),
            KafkaErrorCode(UNSUPPORTED_COMPRESSION_TYPE),
        ),
    );
    assert!(*zstd_topic.response.get() == expected);
}

/// What a consumer sees of a partition row it read.
#[derive(Debug, PartialEq, Eq)]
struct ReadRow {
    error_code: i16,
    high_watermark: i64,
    values: Vec<Option<Bytes>>,
}

impl ReadRow {
    fn of(partition: &PartitionData) -> Self {
        let batches = match partition.records.clone() {
            Some(RecordsPayload::Raw(bytes)) => match RecordsPayload::from_bytes(bytes) {
                Ok(RecordsPayload::V2(batches)) => batches,
                other => panic!("the records are not v2 batches: {other:?}"),
            },
            Some(RecordsPayload::V2(batches)) => batches,
            // A refused row carries empty records, so its error code is what
            // the comparison reports.
            Some(RecordsPayload::Legacy(bytes)) if bytes.is_empty() => Vec::new(),
            other => panic!("the records are not v2 batches: {other:?}"),
        };
        Self {
            error_code: partition.error_code,
            high_watermark: partition.high_watermark,
            values: batches
                .into_iter()
                .flat_map(|batch| batch.records)
                .map(|record| record.value)
                .collect(),
        }
    }
}

#[then("the consumer receives the record")]
fn consumer_receives_record(zstd_topic: &ZstdTopicScenario) {
    let response = zstd_topic.response.get();
    let rows: Vec<ReadRow> = response
        .responses
        .iter()
        .flat_map(|topic| topic.partitions.iter().map(ReadRow::of))
        .collect();
    assert!(
        rows == vec![ReadRow {
            error_code: NONE,
            high_watermark: 1,
            values: vec![Some(Bytes::from_static(RECORD_VALUE))],
        }]
    );
}

/// A follower reads up to the log end; the row reports the high watermark
/// from before the fetch, which is still 0.
#[then("the follower receives the record")]
fn follower_receives_record(zstd_topic: &ZstdTopicScenario) {
    let rows: Vec<ReadRow> = zstd_topic
        .response
        .get()
        .responses
        .iter()
        .flat_map(|topic| topic.partitions.iter().map(ReadRow::of))
        .collect();
    assert!(
        rows == vec![ReadRow {
            error_code: NONE,
            high_watermark: 0,
            values: vec![Some(Bytes::from_static(RECORD_VALUE))],
        }]
    );
}

krabka_bdd::scenarios!(
    "tests/features/handler_fetch_zstd_topics",
    fixtures = [zstd_topic: ZstdTopicScenario],
    runtime = "tokio-multi-thread",
);
