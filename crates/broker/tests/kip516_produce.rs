//! KIP-516: Produce by `topic_id` error semantics.
//!
//! The test client negotiates the broker's highest `Produce` version, v13,
//! which carries only the topic id. Kafka's `KafkaApis.handleProduceRequest`
//! answers `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id does
//! not resolve to a name. The zero id is such an id.
use assert2::assert;
mod support;

use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

#[tokio::test]
async fn produce_unresolved_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    p.client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "p_known".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");

    let cases = [
        (
            "non-zero id",
            WireUuid(uuid::Uuid::from_u128(0x0bad_f00d).into_bytes()),
        ),
        ("zero id", WireUuid::ZERO),
    ];
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, topic_id) in cases {
        let resp = p
            .client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: String::new(), // v13: id-only on the wire
                    topic_id,
                    partition_data: vec![
                        PartitionProduceData {
                            index: 0,
                            records: None,
                            ..Default::default()
                        },
                        PartitionProduceData {
                            index: 1,
                            records: None,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("produce");
        let refused = |index| PartitionProduceResponse {
            index,
            error_code: UNKNOWN_TOPIC_ID,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            ..Default::default()
        };
        actual.push((label, resp));
        expected.push((
            label,
            ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: String::new(),
                    topic_id,
                    partition_responses: vec![refused(0), refused(1)],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ));
    }
    assert!(actual == expected);
}
