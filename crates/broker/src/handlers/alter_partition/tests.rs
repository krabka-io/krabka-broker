//! End-to-end tests for the `AlterPartition` handler entry point.
//!
//! They drive a live broker so that the authorization preamble, the
//! openraft-leader check, and the `submit_change` of an accepted ISR proposal
//! are exercised together, which is why they are kept out of the module root.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::owned::{
    alter_partition_request::{PartitionData as ReqPartitionData, TopicData as ReqTopicData},
    alter_partition_response,
};
use krabka_security::{AuthMethod, Principal};

use super::{
    test_support::{request_with_topics, seed_partition, wait_for_leader, wire_topic_id},
    *,
};
use crate::test_support::{DenyAll, start_broker_with_authorizer as start_broker};

crate::test_support::wire_helpers!(
    AlterPartitionRequest,
    AlterPartitionResponse,
    client_id = "broker-client"
);

#[tokio::test]
async fn handle_denies_cluster_action_for_whole_request() {
    let version = alter_partition_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "replica".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req_bytes = encode_request(&request_with_topics(Vec::new()), version);

    let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp, version);

    let expected = AlterPartitionResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn leader_accepts_empty_alter_partition_request() {
    let version = alter_partition_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    let principal = Principal {
        name: "replica".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req_bytes = encode_request(&request_with_topics(Vec::new()), version);

    let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp, version);

    let expected = AlterPartitionResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        topics: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_topic_partition_response_and_commits_isr_change() {
    let version = 2;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    seed_partition(&broker).await;
    let principal = Principal {
        name: "replica".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = request_with_topics(vec![ReqTopicData {
        topic_id: wire_topic_id(),
        partitions: vec![ReqPartitionData {
            partition_index: 0,
            leader_epoch: 5,
            new_isr: vec![1],
            partition_epoch: 0,
            ..Default::default()
        }],
        ..Default::default()
    }]);
    let req_bytes = encode_request(&req, version);

    let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&resp, version);

    let expected = AlterPartitionResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        topics: vec![RespTopicData {
            topic_id: wire_topic_id(),
            partitions: vec![RespPartitionData {
                partition_index: 0,
                error_code: codes::NONE,
                leader_id: 1,
                leader_epoch: 5,
                isr: vec![1],
                leader_recovery_state: 0,
                partition_epoch: 1,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let image = broker.controller.current_image();
    let committed = image.partition("t", 0).expect("partition committed");
    assert!(committed.partition_epoch == 1);
    broker_handle.shutdown().await;
}

/// The topic id that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicIdKind {
    /// The id of the seeded topic "t".
    Known,
    /// A non-zero id that no topic has.
    Unknown,
    /// The zero id.
    Zero,
}

/// Kafka's `ReplicationControlManager.alterPartition` answers
/// `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id is zero or
/// names no topic. For a known topic, a partition that does not exist answers
/// `UNKNOWN_TOPIC_OR_PARTITION` and the other rows go through validation.
///
/// Each row sends partitions 0 and 1. The seeded topic has partition 0 only,
/// and every accepted ISR change raises its partition epoch by one.
#[tokio::test]
async fn topic_row_error_follows_version_and_topic_id() {
    let cases = [
        (2, TopicIdKind::Known),
        (2, TopicIdKind::Unknown),
        (2, TopicIdKind::Zero),
        (3, TopicIdKind::Known),
        (3, TopicIdKind::Unknown),
        (3, TopicIdKind::Zero),
    ];
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    seed_partition(&broker).await;
    let principal = Principal {
        name: "replica".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    let mut partition_epoch = 0;
    for (version, kind) in cases {
        let topic_id = match kind {
            TopicIdKind::Known => wire_topic_id(),
            TopicIdKind::Unknown => krabka_protocol::primitives::uuid::Uuid([0x0b; 16]),
            TopicIdKind::Zero => krabka_protocol::primitives::uuid::Uuid::ZERO,
        };
        // v2 carries `new_isr`, and v3 carries `new_isr_with_epochs`. An epoch
        // of -1 skips the KIP-903 broker epoch check.
        let row = |partition_index| ReqPartitionData {
            partition_index,
            leader_epoch: 5,
            new_isr: vec![1],
            new_isr_with_epochs: vec![super::test_support::bs(1, -1)],
            partition_epoch,
            ..Default::default()
        };
        let req = request_with_topics(vec![ReqTopicData {
            topic_id,
            partitions: vec![row(0), row(1)],
            ..Default::default()
        }]);
        let req_bytes = encode_request(&req, version);
        let resp = super::handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        actual.push((version, kind, decode_response(&resp, version)));

        let refused = |partition_index, error_code| RespPartitionData {
            partition_index,
            error_code,
            ..Default::default()
        };
        let partitions = if kind == TopicIdKind::Known {
            partition_epoch += 1;
            vec![
                RespPartitionData {
                    partition_index: 0,
                    error_code: codes::NONE,
                    leader_id: 1,
                    leader_epoch: 5,
                    isr: vec![1],
                    leader_recovery_state: 0,
                    partition_epoch,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                refused(1, codes::UNKNOWN_TOPIC_OR_PARTITION),
            ]
        } else {
            vec![
                refused(0, codes::UNKNOWN_TOPIC_ID),
                refused(1, codes::UNKNOWN_TOPIC_ID),
            ]
        };
        expected.push((
            version,
            kind,
            AlterPartitionResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                topics: vec![RespTopicData {
                    topic_id,
                    partitions,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}
