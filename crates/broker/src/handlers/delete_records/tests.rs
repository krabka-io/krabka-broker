//! End-to-end tests for the `DeleteRecords` handler: the ACL denial rows, the
//! unknown-partition row, and the KFC-1 trim bound on a scheduled topic.
//!
//! These drive `handle` against a live broker, so they live apart from the
//! unit tests that each sibling module keeps beside its own helpers.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::owned::{
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic},
    delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
    },
};
use krabka_security::Principal;

use super::*;
use crate::{
    broker::Broker,
    codes,
    test_support::{DenyAll, peer, principal},
};

const VERSION: i16 = 2;

fn request(topic: &str, partitions: &[(i32, i64)]) -> DeleteRecordsRequest {
    DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: topic.into(),
            partitions: partitions
                .iter()
                .map(|(partition_index, offset)| DeleteRecordsPartition {
                    partition_index: *partition_index,
                    offset: *offset,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }
}

crate::test_support::wire_helpers!(
    DeleteRecordsRequest,
    DeleteRecordsResponse,
    version = VERSION,
    client_id = "admin-client"
);

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &DeleteRecordsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> DeleteRecordsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

#[tokio::test]
async fn handle_denied_topic_returns_topic_auth_rows() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request("secret", &[(0, 3), (2, -1)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteRecordsResponse {
        throttle_time_ms: 0,
        topics: vec![DeleteRecordsTopicResult {
            name: "secret".into(),
            partitions: vec![
                DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                },
                DeleteRecordsPartitionResult {
                    partition_index: 2,
                    low_watermark: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unknown_partition_preserves_requested_index() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request("missing", &[(4, 0)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteRecordsResponse {
        throttle_time_ms: 0,
        topics: vec![DeleteRecordsTopicResult {
            name: "missing".into(),
            partitions: vec![DeleteRecordsPartitionResult {
                partition_index: 4,
                low_watermark: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

// Activation time of a batch that has long since come due.
const DELIVERED_MS: i64 = 1_700_000_000_000;
// Activation time of a batch that still waits. It sits far enough ahead
// that every clock this test can read calls it pending, so the schedule
// holds without a mock timeline.
const PENDING_MS: i64 = 4_100_000_000_000;

// A two-record batch that activates at `activation_ms`, stamped with the
// epoch the partition writer expects from a leader append.
fn batch_at(activation_ms: i64, leader_epoch: i32) -> krabka_protocol::records::RecordBatch {
    krabka_protocol::records::RecordBatch {
        partition_leader_epoch: leader_epoch,
        ..crate::delivery::test_support::batch_at(activation_ms)
    }
}

// Create `topic` with the given `delivery.mode`, then append one batch
// that has come due and one that has not. Two records per batch puts the
// log end offset at 4, and a scheduled topic's delivery watermark at 2.
async fn topic_holding_a_pending_batch(
    broker_handle: &crate::broker::BrokerHandle,
    broker: &Broker,
    topic: &str,
    delivery_mode: Option<&str>,
    ctx: &crate::handlers::RequestContext<'_>,
) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::{self, CreateTopicsResponse},
    };

    let version = create_topics_response::MAX_VERSION;
    let create = crate::test_support::encode_request(
        &CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: delivery_mode
                    .map(|mode| CreatableTopicConfig {
                        name: crate::config_keys::DELIVERY_MODE.to_owned(),
                        value: Some(mode.to_owned()),
                        ..Default::default()
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        },
        version,
    );
    let bytes = crate::handlers::create_topics::handle(broker, version, 1, &create, ctx)
        .await
        .expect("CreateTopics");
    let created: CreateTopicsResponse = crate::test_support::decode_response(&bytes, version);
    assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
    broker_handle.wait_until_partition_present(topic, 0).await;

    let expected_policy = if delivery_mode == Some(crate::config_keys::DELIVERY_MODE_SCHEDULED) {
        krabka_log::DeliveryPolicy::Scheduled
    } else {
        krabka_log::DeliveryPolicy::Immediate
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker_handle
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| config.delivery_policy == expected_policy)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delivery mode reaches the partition log");

    let part = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    for activation_ms in [DELIVERED_MS, PENDING_MS] {
        part.produce_batch(batch_at(activation_ms, leader_epoch))
            .await
            .expect("append a batch");
    }
}

#[tokio::test]
async fn a_trim_stops_at_the_delivery_watermark_of_a_scheduled_topic() {
    // The `-1` sentinel resolves to the high watermark, which is 4 on both
    // topics: replication is never gated on delivery. The scheduled topic
    // keeps the batch that has not come due.
    let cases = [
        ("delete-records-immediate-delivery", None, 4),
        (
            "delete-records-scheduled-delivery",
            Some(crate::config_keys::DELIVERY_MODE_SCHEDULED),
            2,
        ),
    ];

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);

    for (topic, delivery_mode, expected_low_watermark) in cases {
        topic_holding_a_pending_batch(&broker_handle, &broker, topic, delivery_mode, &ctx).await;

        let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: topic.into(),
                partitions: vec![DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: expected_low_watermark,
                    error_code: codes::NONE,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        check!(resp == expected, "{topic}");
    }

    broker_handle.shutdown().await;
}
