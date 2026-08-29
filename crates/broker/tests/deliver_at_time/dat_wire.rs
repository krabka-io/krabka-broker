//! The Kafka wire calls every KFC-1 case drives: `CreateTopics`, `Produce`,
//! `Fetch`, and `ListOffsets`, plus the readiness waits and the polling read
//! that turn them into the `Visible` snapshot a case asserts on.
//!
//! `ready_topic` is the one that matters most: it does not return until the
//! `delivery.mode` override has travelled all the way into the partition's own
//! `LogConfig`, which is what removes the window in which a scheduled topic
//! would still read as an immediate one.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::{BrokerHandle, NodeId};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::RecordBatch,
};

use crate::{
    dat_fixtures::{Mode, Visible, now_ms},
    support,
};

/// Ceiling on how long a poll waits for a record to become visible.
const VISIBILITY_DEADLINE: Duration = Duration::from_secs(30);

pub async fn create_topic(client: &Client, topic: &str, mode: Mode) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![CreatableTopicConfig {
                    name: "delivery.mode".to_owned(),
                    value: Some(mode.value.to_owned()),
                    ..CreatableTopicConfig::default()
                }],
                ..CreatableTopic::default()
            }],
            timeout_ms: 5_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    let created = response.topics.first().expect("one topic result");
    assert!(
        created.error_code == 0,
        "create {topic}: {:?}",
        created.error_message
    );
}

// Wait until `delivery.mode` has travelled from the metadata image through the
// supervisor's reconcile loop into the partition's own `LogConfig`.
//
// `CreateTopics` materializes the partition from the broker's base log config
// and the overrides land on the next reconcile, so a read taken between the two
// would see an immediate-delivery log on a scheduled topic. Waiting on the
// partition's live config is what removes that window; waiting on the metadata
// image would not, because the image is upstream of the value the fetch cap
// reads.
pub async fn wait_for_delivery_policy(broker: &BrokerHandle, topic: &str, mode: Mode) {
    broker
        .wait_for_metrics("delivery.mode reaches the partition LogConfig", |_| {
            broker
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| config.delivery_policy == mode.policy)
        })
        .await;
}

// Create `topic` in `mode` and return its id once the partition is led here and
// carries the mode.
pub async fn ready_topic(broker: &BrokerHandle, client: &Client, topic: &str, mode: Mode) -> Uuid {
    create_topic(client, topic, mode).await;
    broker
        .wait_until_local_partition_leader(topic, 0, NodeId(1))
        .await;
    wait_for_delivery_policy(broker, topic, mode).await;
    support::topic_id_for(client, topic).await
}

pub async fn produce(client: &Client, topic: &str, topic_id: Uuid, batch: RecordBatch) {
    let response = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.to_owned(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..PartitionProduceData::default()
                }],
                ..TopicProduceData::default()
            }],
            ..ProduceRequest::default()
        })
        .await
        .expect("Produce");
    let written = response
        .responses
        .first()
        .and_then(|topic| topic.partition_responses.first())
        .expect("one partition result");
    assert!(
        written.error_code == 0,
        "produce to {topic}: error {}",
        written.error_code
    );
}

// Fetch `topic` from offset 0 and return the record values it served, in order.
pub async fn fetch_values(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    max_wait_ms: i32,
) -> Vec<String> {
    let response = client
        .send(FetchRequest {
            max_wait_ms,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.to_owned(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let served = response
        .responses
        .first()
        .and_then(|topic| topic.partitions.first())
        .expect("one partition result");
    assert!(
        served.error_code == 0,
        "fetch {topic}: error {}",
        served.error_code
    );
    served
        .records
        .as_ref()
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| batch.records.iter())
                .map(|record| {
                    String::from_utf8_lossy(record.value.as_deref().unwrap_or_default())
                        .into_owned()
                })
                .collect()
        })
        .unwrap_or_default()
}

// The offset `ListOffsets` LATEST reports, which is where a seek-to-end lands.
async fn latest_offset(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1,
                    ..ListOffsetsPartition::default()
                }],
                ..ListOffsetsTopic::default()
            }],
            ..ListOffsetsRequest::default()
        })
        .await
        .expect("ListOffsets");
    let end = response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .expect("one partition result");
    assert!(
        end.error_code == 0,
        "list offsets {topic}: error {}",
        end.error_code
    );
    end.offset
}

pub async fn visible(client: &Client, topic: &str, topic_id: Uuid) -> Visible {
    Visible {
        latest: latest_offset(client, topic).await,
        // `max_wait_ms` of zero keeps this a snapshot: an empty read returns at
        // once rather than parking in the long poll.
        values: fetch_values(client, topic, topic_id, 0).await,
    }
}

// Poll until `topic` serves exactly `want`, and report the clock reading taken
// after the read that first saw it.
//
// The reading is taken *after* the read rather than before it, which makes
// "never early" a claim the broker cannot satisfy by luck: a reading below the
// delivery time means the whole round trip finished before that time and still
// came back with the record.
pub async fn wait_until_visible(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    want: &Visible,
) -> i64 {
    let deadline = Instant::now() + VISIBILITY_DEADLINE;
    loop {
        let seen = visible(client, topic, topic_id).await;
        let at_ms = now_ms();
        if seen == *want {
            return at_ms;
        }
        assert!(
            Instant::now() < deadline,
            "{topic} never served {want:?}; the last read was {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
