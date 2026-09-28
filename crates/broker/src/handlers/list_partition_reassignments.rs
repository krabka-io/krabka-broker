//! `ListPartitionReassignments` (`api_key` 46, KIP-455).

use bytes::Bytes;
use krabka_metadata::{PartitionRecord, ResourceType};
use krabka_protocol::{
    Encode,
    owned::{
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::{
            ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
            OngoingTopicReassignment,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, NONE},
};

#[tracing::instrument(
    name = "handle_list_partition_reassignments",
    level = "info",
    skip_all,
    fields(api = "ListPartitionReassignments"),
    err
)]
pub(crate) fn handle(
    broker: &Broker,
    req: ListPartitionReassignmentsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::Describe,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let topics = match &req.topics {
            None => vec![],
            Some(filter) => filter
                .iter()
                .map(|t| OngoingTopicReassignment {
                    name: t.name.clone(),
                    partitions: t
                        .partition_indexes
                        .iter()
                        .map(|&partition_index| OngoingPartitionReassignment {
                            partition_index,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
        };
        let resp = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("list-reassignment denied".into()),
            topics,
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let topics: Vec<OngoingTopicReassignment> = match &req.topics {
        // Kafka's `ReplicationControlManager.listPartitionReassignments` with
        // null topics lists every reassigning partition. Grouped by topic in
        // a `BTreeMap` for a stable alphabetical order.
        None => {
            let mut by_topic: std::collections::BTreeMap<
                String,
                Vec<OngoingPartitionReassignment>,
            > = std::collections::BTreeMap::new();
            for pr in image.reassignments_in_flight() {
                by_topic
                    .entry(pr.topic.clone())
                    .or_default()
                    .push(ongoing(pr));
            }
            by_topic
                .into_iter()
                .map(|(name, partitions)| OngoingTopicReassignment {
                    name,
                    partitions,
                    ..Default::default()
                })
                .collect()
        }
        // `listReassigningTopic`: one row per requested topic, in request
        // order, holding the requested partitions that are reassigning, in
        // request order and once per occurrence. A topic with no such
        // partition, an empty index list included, gives no row.
        Some(filter) => filter
            .iter()
            .filter_map(|t| {
                let partitions: Vec<OngoingPartitionReassignment> = t
                    .partition_indexes
                    .iter()
                    .filter_map(|&idx| image.partition(&t.name, idx))
                    .filter(|pr| !pr.adding_replicas.is_empty() || !pr.removing_replicas.is_empty())
                    .map(ongoing)
                    .collect();
                (!partitions.is_empty()).then(|| OngoingTopicReassignment {
                    name: t.name.clone(),
                    partitions,
                    ..Default::default()
                })
            })
            .collect(),
    };
    let resp = ListPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: NONE,
        error_message: None,
        topics,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn ongoing(pr: &PartitionRecord) -> OngoingPartitionReassignment {
    OngoingPartitionReassignment {
        partition_index: pr.partition,
        replicas: wire_node_ids(&pr.replicas),
        adding_replicas: wire_node_ids(&pr.adding_replicas),
        removing_replicas: wire_node_ids(&pr.removing_replicas),
        ..Default::default()
    }
}

fn wire_node_ids(nodes: &[krabka_metadata::NodeId]) -> Vec<i32> {
    nodes
        .iter()
        .map(|node| i32::try_from(node.0).unwrap_or(i32::MAX))
        .collect()
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(
        resp,
        api_version,
        "encode ListPartitionReassignments",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, NodeId, TopicRecord};
    use krabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsTopics;
    use uuid::Uuid;

    use super::*;
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = krabka_protocol::owned::list_partition_reassignments_response::MAX_VERSION;

    crate::test_support::response_helpers!(
        ListPartitionReassignmentsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    #[tokio::test]
    async fn denied_response_echoes_requested_topics() {
        struct Case {
            name: &'static str,
            topics: Option<Vec<ListPartitionReassignmentsTopics>>,
            expected_topics: Vec<OngoingTopicReassignment>,
        }

        let cases = vec![
            Case {
                name: "null topics filter yields no echoed topics",
                topics: None,
                expected_topics: vec![],
            },
            Case {
                name: "requested topic with no partition indexes is echoed with none",
                topics: Some(vec![ListPartitionReassignmentsTopics {
                    name: "orders-add".into(),
                    partition_indexes: vec![],
                    ..Default::default()
                }]),
                expected_topics: vec![OngoingTopicReassignment {
                    name: "orders-add".to_string(),
                    partitions: vec![],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
            },
            Case {
                name: "requested topic and partition indexes are echoed with empty replicas",
                topics: Some(vec![ListPartitionReassignmentsTopics {
                    name: "orders-add".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]),
                expected_topics: vec![OngoingTopicReassignment {
                    name: "orders-add".to_string(),
                    partitions: vec![
                        OngoingPartitionReassignment {
                            partition_index: 0,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        },
                        OngoingPartitionReassignment {
                            partition_index: 1,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        },
                    ],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
            },
            Case {
                name: "multiple requested topics are all echoed",
                topics: Some(vec![
                    ListPartitionReassignmentsTopics {
                        name: "orders-add".into(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    },
                    ListPartitionReassignmentsTopics {
                        name: "orders-remove".into(),
                        partition_indexes: vec![2],
                        ..Default::default()
                    },
                ]),
                expected_topics: vec![
                    OngoingTopicReassignment {
                        name: "orders-add".to_string(),
                        partitions: vec![OngoingPartitionReassignment {
                            partition_index: 0,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                    OngoingTopicReassignment {
                        name: "orders-remove".to_string(),
                        partitions: vec![OngoingPartitionReassignment {
                            partition_index: 2,
                            replicas: vec![],
                            adding_replicas: vec![],
                            removing_replicas: vec![],
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        }],
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                ],
            },
        ];

        for case in cases {
            let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
            let broker = broker_handle.broker_arc_for_test();
            let p = principal("alice");
            let peer = peer();
            let ctx = test_context(&p, &peer);

            let bytes = handle(
                &broker,
                ListPartitionReassignmentsRequest {
                    topics: case.topics,
                    ..Default::default()
                },
                &ctx,
                VERSION,
            )
            .expect("handle");
            let resp = decode_response(&bytes);

            let expected = ListPartitionReassignmentsResponse {
                throttle_time_ms: 0,
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("list-reassignment denied".to_string()),
                topics: case.expected_topics,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            };
            assert!(resp == expected, "case `{}`: {resp:?}", case.name);
            broker_handle.shutdown().await;
        }
    }

    fn partition(topic: &str, partition: i32, adding: bool) -> MetadataRecord {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(4),
            adding_replicas: if adding { vec![NodeId(3)] } else { vec![] },
            removing_replicas: vec![],
            directories: vec![Uuid::nil(), Uuid::nil(), Uuid::nil()],
            partition_epoch: 8,
        })
    }

    /// Kafka's `ReplicationControlManager.listPartitionReassignments` and
    /// `listReassigningTopic`: null topics list every reassigning partition,
    /// a named topic lists only its requested indexes that are reassigning,
    /// in request order and once per occurrence, and a topic with none (an
    /// empty index list, an idle partition, an unknown topic) gives no row.
    #[tokio::test]
    async fn success_response_lists_what_kafka_lists() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        broker_handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: Uuid::from_u128(1),
                    partitions: 3,
                    replication_factor: 3,
                }),
                partition("t", 0, true),
                partition("t", 1, true),
                partition("t", 2, false),
            ])
            .await
            .expect("seed reassignments");
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let row = |partition_index: i32| OngoingPartitionReassignment {
            partition_index,
            replicas: vec![1, 2, 3],
            adding_replicas: vec![3],
            removing_replicas: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        let topic = |name: &str, partitions: &[i32]| OngoingTopicReassignment {
            name: name.to_string(),
            partitions: partitions.iter().map(|&p| row(p)).collect(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        let filter = |topics: &[(&str, &[i32])]| {
            Some(
                topics
                    .iter()
                    .map(|&(name, indexes)| ListPartitionReassignmentsTopics {
                        name: name.into(),
                        partition_indexes: indexes.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
            )
        };
        let cases = vec![
            ("null topics", None, vec![topic("t", &[0, 1])]),
            ("empty index list", filter(&[("t", &[])]), vec![]),
            ("one index", filter(&[("t", &[1])]), vec![topic("t", &[1])]),
            (
                "repeated index",
                filter(&[("t", &[1, 1])]),
                vec![topic("t", &[1, 1])],
            ),
            (
                "request order",
                filter(&[("t", &[1, 0])]),
                vec![topic("t", &[1, 0])],
            ),
            ("idle partition", filter(&[("t", &[2])]), vec![]),
            ("unknown topic", filter(&[("u", &[0])]), vec![]),
            (
                "repeated topic",
                filter(&[("t", &[0]), ("t", &[1])]),
                vec![topic("t", &[0]), topic("t", &[1])],
            ),
        ];
        for (name, topics, expected_topics) in cases {
            let bytes = handle(
                &broker,
                ListPartitionReassignmentsRequest {
                    topics,
                    ..Default::default()
                },
                &ctx,
                VERSION,
            )
            .expect("handle");
            let resp = decode_response(&bytes);

            let expected = ListPartitionReassignmentsResponse {
                throttle_time_ms: 0,
                error_code: 0,
                error_message: None,
                topics: expected_topics,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            };
            assert2::check!(resp == expected, "case {name}");
        }
        broker_handle.shutdown().await;
    }
}
