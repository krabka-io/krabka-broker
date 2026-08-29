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
