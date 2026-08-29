//! End-to-end tests of the `DescribeShareGroupOffsets` handler against a
//! running broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group and
//! per-partition error rows KIP-932 asks for -- feature disabled, group
//! denied, topic unknown -- stay exactly what the JVM admin client reads.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_response::{
            self, DescribeShareGroupOffsetsResponsePartition,
            DescribeShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};
use krabka_security::Principal;

use super::{test_support::start_broker, *};
use crate::{authorizer::Authorizer, test_support::DenyAll};

type RequestTopic<'a> = (&'a str, Vec<i32>);
type RequestGroup<'a> = (&'a str, Vec<RequestTopic<'a>>);

fn request(groups: &[RequestGroup<'_>]) -> DescribeShareGroupOffsetsRequest {
    DescribeShareGroupOffsetsRequest {
        groups: groups
            .iter()
            .map(|(group_id, topics)| DescribeShareGroupOffsetsRequestGroup {
                group_id: (*group_id).into(),
                topics: Some(
                    topics
                        .iter()
                        .map(
                            |(topic_name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                                topic_name: (*topic_name).into(),
                                partitions: partitions.clone(),
                                ..Default::default()
                            },
                        )
                        .collect(),
                ),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

crate::test_support::wire_helpers!(
    DescribeShareGroupOffsetsRequest,
    DescribeShareGroupOffsetsResponse,
    version = describe_share_group_offsets_response::MAX_VERSION,
    client_id = "admin-client"
);

fn principal() -> Principal {
    crate::test_support::principal("alice")
}

#[tokio::test]
async fn handle_error_scenarios_preserve_expected_rows() {
    type Case<'a> = (
        &'a str,
        Arc<dyn Authorizer>,
        bool,
        Vec<RequestGroup<'a>>,
        DescribeShareGroupOffsetsResponse,
    );
    let version = describe_share_group_offsets_response::MAX_VERSION;
    let cases: Vec<Case<'_>> = vec![
        (
            "disabled feature preserves group error rows",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            false,
            vec![("g1", vec![("t1", vec![0])]), ("g2", vec![("t2", vec![1])])],
            DescribeShareGroupOffsetsResponse {
                throttle_time_ms: 0,
                groups: vec![
                    DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: Vec::new(),
                        error_code: codes::UNSUPPORTED_VERSION,
                        error_message: None,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g2".into(),
                        topics: Vec::new(),
                        error_code: codes::UNSUPPORTED_VERSION,
                        error_message: None,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ),
        (
            "denied group preserves group id and error code",
            Arc::new(DenyAll),
            true,
            vec![("g1", vec![("missing", vec![0])])],
            DescribeShareGroupOffsetsResponse {
                throttle_time_ms: 0,
                groups: vec![DescribeShareGroupOffsetsResponseGroup {
                    group_id: "g1".into(),
                    topics: Vec::new(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ),
        (
            "unknown topic preserves partition error rows",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            true,
            vec![("g1", vec![("missing-topic", vec![3, 5])])],
            DescribeShareGroupOffsetsResponse {
                throttle_time_ms: 0,
                groups: vec![DescribeShareGroupOffsetsResponseGroup {
                    group_id: "g1".into(),
                    topics: vec![DescribeShareGroupOffsetsResponseTopic {
                        topic_name: "missing-topic".into(),
                        topic_id: Uuid::default(),
                        partitions: vec![
                            DescribeShareGroupOffsetsResponsePartition {
                                partition_index: 3,
                                start_offset: -1,
                                leader_epoch: -1,
                                lag: -1,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: None,
                                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                            },
                            DescribeShareGroupOffsetsResponsePartition {
                                partition_index: 5,
                                start_offset: -1,
                                leader_epoch: -1,
                                lag: -1,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: None,
                                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                            },
                        ],
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    }],
                    error_code: codes::NONE,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ),
    ];
    for (case, authorizer, share_enabled, groups, expected) in cases {
        let (broker_handle, _dir) = start_broker(authorizer, share_enabled).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(&groups));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp == expected, "case: {case}");
        broker_handle.shutdown().await;
    }
}
