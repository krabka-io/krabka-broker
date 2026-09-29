//! Handler tests for the broker-wide defaults of the topic keys `Produce`
//! enforces.
//!
//! `message.max.bytes`, `log.message.timestamp.*`, `compression.type`,
//! `log.cleanup.policy` and the other broker synonyms of a topic key are
//! dynamic broker configs. `kafka-configs --entity-type brokers` sets them for
//! the cluster (`--entity-default`) or for one node, and Kafka's
//! `DynamicLogConfig` applies the value to every log at once, so the next
//! produce validates against it. A topic that overrides the key keeps its own
//! value.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    records::{Record, RecordBatch, RecordsPayload},
};

use super::handle;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

/// The `Produce` version these tests speak.
const VERSION: i16 = 9;

async fn create_topic(broker: &BrokerHandle, name: &str) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("broker-default-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
}

/// The stored value of one dynamic broker config, on `node` (the cluster
/// default for [`DEFAULT_BROKER_CONFIG_NODE_ID`]).
async fn set_broker_config(
    broker: &BrokerHandle,
    node: krabka_metadata::NodeId,
    name: &str,
    value: &str,
) {
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: node,
            config_name: name.to_owned(),
            config_value: Some(value.to_owned()),
        })])
        .await
        .expect("submit the broker config");
}

/// The error code `Produce` answers for one record of `value_bytes` bytes and
/// no key, sent to partition 0 of `topic`.
async fn produce_error_code(broker: &BrokerHandle, topic: &str, value_bytes: usize) -> i16 {
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![RecordBatch {
                    records: vec![Record {
                        value: Some(Bytes::from(vec![0_u8; value_bytes])),
                        ..Default::default()
                    }],
                    ..Default::default()
                }])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("producer");
    let address = peer();
    let ctx = request_context(&user, &address, "producer-client");
    let request_bytes = encode_request(&request, VERSION);
    let response_bytes = handle(
        &shared,
        VERSION,
        7,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    let response: ProduceResponse = decode_response(&response_bytes, VERSION);
    response.responses[0].partition_responses[0].error_code
}

/// A cluster-wide `message.max.bytes` raises the cap `Produce` enforces on a
/// topic that sets none, and a node's own value beats the cluster's. Each
/// batch is 1.5 MiB, over the built-in 1048588-byte cap.
#[tokio::test]
async fn a_dynamic_message_max_bytes_governs_a_topic_that_sets_no_cap() {
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic(&broker, "orders").await;
    let node = krabka_metadata::NodeId(broker.broker_arc_for_test().config.node_id.0);
    let batch = 1_572_864;

    // The built-in default refuses it.
    assert!(produce_error_code(&broker, "orders", batch).await == codes::MESSAGE_TOO_LARGE);

    // A cluster-wide default of 2 MiB lets it through.
    set_broker_config(
        &broker,
        DEFAULT_BROKER_CONFIG_NODE_ID,
        "message.max.bytes",
        "2097152",
    )
    .await;
    assert!(produce_error_code(&broker, "orders", batch).await == codes::NONE);

    // This node's own value wins over the cluster's.
    set_broker_config(&broker, node, "message.max.bytes", "1100000").await;
    assert!(produce_error_code(&broker, "orders", batch).await == codes::MESSAGE_TOO_LARGE);

    broker.shutdown().await;
}
