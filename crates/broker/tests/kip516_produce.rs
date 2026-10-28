//! KIP-516: Produce by `topic_id` error semantics.
//!
//! The test client negotiates the broker's highest `Produce` version, v13,
//! which carries only the topic id. Kafka's `KafkaApis.handleProduceRequest`
//! answers `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id does
//! not resolve to a name. The zero id is such an id.
#![cfg(test)]
use {
    crate::support::topics::{creatable_topic, create_topic_request},
    assert2::assert,
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};
mod support;

use krabka_protocol::owned::{
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

struct ProduceIdentity {
    broker: Option<InProcess>,
    ids: Vec<WireUuid>,
    actual: Vec<(WireUuid, ProduceResponse)>,
}

#[fixture]
fn producing() -> ProduceIdentity {
    // Start this scenario with fresh inputs and observations.

    ProduceIdentity {
        broker: None,
        ids: Vec::new(),
        actual: Vec::new(),
    }
}

#[given("a known produce topic beside zero and unassigned topic ids")]
async fn unmatched_produce_ids(producing: &mut ProduceIdentity) {
    let broker = support::start().await;
    broker
        .client
        .send(create_topic_request(
            creatable_topic().name("p_known".into()).call(),
        ))
        .await
        .expect("create topic");
    producing.broker = Some(broker);
    producing.ids = support::topics::unresolved_topic_ids(0x0bad_f00d)
        .into_iter()
        .map(|(_, id)| id)
        .collect();
}

#[when("an empty batch is produced to partitions zero and one of each unresolved id")]
async fn produce_to_unresolved_partitions(producing: &mut ProduceIdentity) {
    let broker = producing.broker.as_ref().expect("a broker was started");
    for &topic_id in &producing.ids {
        let response = broker
            .client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: String::new(),
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
        producing.actual.push((topic_id, response));
    }
}

#[then("both produced partitions report UNKNOWN_TOPIC_ID without assigning offsets")]
fn unknown_produce_partitions(producing: &ProduceIdentity) {
    assert!(producing.actual.len() == producing.ids.len());
    for (topic_id, actual) in &producing.actual {
        let refused = |index| PartitionProduceResponse {
            index,
            error_code: UNKNOWN_TOPIC_ID,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            ..Default::default()
        };
        let expected = ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: String::new(),
                topic_id: *topic_id,
                partition_responses: vec![refused(0), refused(1)],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(*actual == expected);
    }
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "Producing to an unresolved identity refuses every requested partition"
)]
#[tokio::test]
async fn produce_unresolved_topic_id_returns_unknown_topic_id(producing: ProduceIdentity) {
    producing
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}
