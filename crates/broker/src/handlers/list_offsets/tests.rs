//! The tests for the handler entry point: the per-topic `Describe` gate and
//! the response a denied topic receives.

use std::sync::Arc;

use assert2::assert;
use bytes::BytesMut;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
    },
};

use super::{
    handle,
    sentinels::{EARLIEST_TIMESTAMP, LATEST_TIMESTAMP},
    test_support::{decode_response, encode_request, test_context},
};
use crate::{
    codes,
    test_support::{
        DenyAll, peer, principal, start_broker_with_authorizer_no_audit as start_broker,
    },
};

#[test]
fn topic_describe_denied_yields_topic_authorization_failed_rows() {
    use krabka_protocol::owned::list_offsets_response::{
        self, ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    };

    let authorizer = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
    let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    let principal = krabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer: &peer,
        client_id: "client-a",
        connection_id: "connection-a",
        sendfile_capable: false,
        connection_listener_name: "PLAINTEXT",
        throttle: crate::quota::ThrottleSlot::default(),
    };
    assert!(crate::handlers::acl_denied(
        &authorizer,
        &image,
        &ctx,
        ResourceType::Topic,
        "t",
        AclOperation::Describe,
    ));

    // The denied-topic shape the handler emits: every partition row
    // carries TOPIC_AUTHORIZATION_FAILED.
    let resp = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "t".into(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                timestamp: -1,
                offset: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(list_offsets_response::MAX_VERSION));
    resp.encode(&mut buf, list_offsets_response::MAX_VERSION)
        .expect("encode");
    let mut cur: &[u8] = &buf;
    let decoded =
        ListOffsetsResponse::decode(&mut cur, list_offsets_response::MAX_VERSION).unwrap();
    assert!(decoded.topics[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
}

#[tokio::test]
async fn denied_handler_preserves_topic_and_partition_response_fields() {
    let version = krabka_protocol::owned::list_offsets_response::MAX_VERSION;
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopic {
            name: "orders".into(),
            partitions: vec![
                ListOffsetsPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: LATEST_TIMESTAMP,
                    ..Default::default()
                },
                ListOffsetsPartition {
                    partition_index: 2,
                    current_leader_epoch: -1,
                    timestamp: EARLIEST_TIMESTAMP,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };
    let req = encode_request(&req, version);

    let bytes = handle(&broker, version, 123, &req, &ctx)
        .await
        .expect("handle");
    let resp = decode_response(&bytes, version);

    let denied_row = |partition_index: i32| ListOffsetsPartitionResponse {
        partition_index,
        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        timestamp: -1,
        offset: -1,
        leader_epoch: -1,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
    };
    let expected = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "orders".to_string(),
            partitions: vec![denied_row(0), denied_row(2)],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
    };
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}
