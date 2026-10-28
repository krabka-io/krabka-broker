//! `ListPartitionReassignments` (`api_key` 46, KIP-455).

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes::{CLUSTER_AUTHORIZATION_FAILED, NONE},
        handlers::{RequestContext, cluster_describe_denied, metadata::wire_id},
    },
    krabka_metadata::{MetadataImage, PartitionRecord},
    krabka_protocol::owned::{
        list_partition_reassignments_request::ListPartitionReassignmentsRequest,
        list_partition_reassignments_response::{
            ListPartitionReassignmentsResponse, OngoingPartitionReassignment,
            OngoingTopicReassignment,
        },
    },
    std::collections::BTreeMap,
};

pub fn handle(
    broker: &Broker,
    req: &ListPartitionReassignmentsRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> ListPartitionReassignmentsResponse {
    let image = broker.controller.current_image();
    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let topics = req
            .topics
            .iter()
            .flatten()
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
            .collect();
        return ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("list-reassignment denied".into()),
            topics,
            ..Default::default()
        };
    }

    let topics: Vec<OngoingTopicReassignment> = req.topics.as_ref().map_or_else(
        || all_reassigning_topics(&image),
        |filter| {
            // `listReassigningTopic`: one row per requested topic, in request
            // order, holding the requested partitions that are reassigning,
            // in request order and once per occurrence. A topic with no such
            // partition, an empty index list included, gives no row.
            filter
                .iter()
                .filter_map(|t| {
                    let partitions: Vec<OngoingPartitionReassignment> = t
                        .partition_indexes
                        .iter()
                        .filter_map(|&idx| image.partition(&t.name, idx))
                        .filter(|pr| {
                            !pr.adding_replicas.is_empty() || !pr.removing_replicas.is_empty()
                        })
                        .map(ongoing)
                        .collect();
                    (!partitions.is_empty()).then(|| OngoingTopicReassignment {
                        name: t.name.clone(),
                        partitions,
                        ..Default::default()
                    })
                })
                .collect()
        },
    );
    ListPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: NONE,
        error_message: None,
        topics,
        ..Default::default()
    }
}

/// Kafka's `ReplicationControlManager.listPartitionReassignments` with null
/// topics: every reassigning partition, grouped by topic in a `BTreeMap` for a
/// stable alphabetical order.
fn all_reassigning_topics(image: &MetadataImage) -> Vec<OngoingTopicReassignment> {
    let mut by_topic: BTreeMap<String, Vec<OngoingPartitionReassignment>> = BTreeMap::new();
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
    nodes.iter().copied().map(wire_id).collect()
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::authorizer::AllowAllAuthorizer,
        assert2::assert,
        krabka_metadata::{MetadataRecord, NodeId, TopicRecord},
        krabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsTopics,
        std::sync::Arc,
        uuid::Uuid,
    };

    const VERSION: i16 = list_partition_reassignments_response::MAX_VERSION;

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    use {
        crate::test_support::start_broker_with_authorizer_no_audit as start_broker,
        krabka_protocol::owned::list_partition_reassignments_response,
    };

    fn expected_empty_partition(partition_index: i32) -> OngoingPartitionReassignment {
        krabka_macros::tagged_wire!(OngoingPartitionReassignment {
            partition_index,
            replicas: vec![],
            adding_replicas: vec![],
            removing_replicas: vec![],
        })
    }

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
                expected_topics: vec![krabka_macros::tagged_wire!(OngoingTopicReassignment {
                    name: "orders-add".to_owned(),
                    partitions: vec![],
                })],
            },
            Case {
                name: "requested topic and partition indexes are echoed with empty replicas",
                topics: Some(vec![ListPartitionReassignmentsTopics {
                    name: "orders-add".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]),
                expected_topics: vec![krabka_macros::tagged_wire!(OngoingTopicReassignment {
                    name: "orders-add".to_owned(),
                    partitions: vec![expected_empty_partition(0), expected_empty_partition(1),],
                })],
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
                    krabka_macros::tagged_wire!(OngoingTopicReassignment {
                        name: "orders-add".to_owned(),
                        partitions: vec![expected_empty_partition(0)],
                    }),
                    krabka_macros::tagged_wire!(OngoingTopicReassignment {
                        name: "orders-remove".to_owned(),
                        partitions: vec![expected_empty_partition(2)],
                    }),
                ],
            },
        ];

        for case in cases {
            krabka_macros::broker_test_setup! {
                bind(broker_handle, _dir, broker, ctx);
                boot [start_broker] deny_all;
                user ["alice"];
            };

            let resp = handle(
                &broker,
                &ListPartitionReassignmentsRequest {
                    topics: case.topics,
                    ..Default::default()
                },
                VERSION,
                &ctx,
            );

            let expected = krabka_macros::unthrottled_wire!(ListPartitionReassignmentsResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("list-reassignment denied".to_owned()),
                topics: case.expected_topics,
            });
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

    #[derive(Default)]
    struct ReassignmentListingScenario {
        started: Option<HandlerStartedBroker>,
        request: Option<ListPartitionReassignmentsRequest>,
        response: Option<ListPartitionReassignmentsResponse>,
    }
    #[rstest::fixture]
    fn reassignment_listing() -> ReassignmentListingScenario {
        let started = None;
        ReassignmentListingScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("topic t has reassigning partitions 0 and 1 and idle partition 2")]
    async fn active_reassignments(reassignment_listing: &mut ReassignmentListingScenario) {
        let started = start_broker(Arc::new(AllowAllAuthorizer)).await;
        started
            .broker
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
        reassignment_listing.started = Some(started);
    }
    #[rstest_bdd_macros::given("the reassignment request filters {filter}")]
    fn reassignment_filter(reassignment_listing: &mut ReassignmentListingScenario, filter: String) {
        let topics = if filter == "all topics" {
            None
        } else {
            Some(
                filter
                    .split(';')
                    .map(|entry| {
                        let (name, indexes) = entry.split_once(':').unwrap();
                        ListPartitionReassignmentsTopics {
                            name: name.into(),
                            partition_indexes: if indexes == "none" {
                                Vec::new()
                            } else {
                                indexes
                                    .split(',')
                                    .map(|index| index.parse().unwrap())
                                    .collect()
                            },
                            ..Default::default()
                        }
                    })
                    .collect(),
            )
        };
        reassignment_listing.request = Some(ListPartitionReassignmentsRequest {
            topics,
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when("the controller lists the requested reassignments")]
    fn list_requested_reassignments(reassignment_listing: &mut ReassignmentListingScenario) {
        let broker = reassignment_listing
            .started
            .as_ref()
            .unwrap()
            .broker
            .broker_arc_for_test();
        krabka_macros::request_test_identity! {bind(ctx);user ["admin"];};
        reassignment_listing.response = Some(handle(
            &broker,
            reassignment_listing.request.as_ref().unwrap(),
            VERSION,
            &ctx,
        ));
    }
    #[rstest_bdd_macros::then(
        "the complete reassignment listing contains ordered topic partitions {expected}"
    )]
    async fn check_reassignment_listing(
        reassignment_listing: &mut ReassignmentListingScenario,
        expected: String,
    ) {
        let topics = if expected == "none" {
            Vec::new()
        } else {
            expected
                .split(';')
                .map(|entry| {
                    let (name, indexes) = entry.split_once(':').unwrap();
                    krabka_macros::tagged_wire!(OngoingTopicReassignment {
                        name: name.into(),
                        partitions: indexes
                            .split(',')
                            .map(|index| krabka_macros::tagged_wire!(
                                OngoingPartitionReassignment {
                                    partition_index: index.parse().unwrap(),
                                    replicas: vec![1, 2, 3],
                                    adding_replicas: vec![3],
                                    removing_replicas: vec![]
                                }
                            ))
                            .collect()
                    })
                })
                .collect()
        };
        assert2::check!(
            reassignment_listing.response.as_ref().unwrap()
                == &krabka_macros::unthrottled_wire!(ListPartitionReassignmentsResponse {
                    error_code: 0,
                    error_message: None,
                    topics
                })
        );
        reassignment_listing
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_list_reassignments/list.feature",
        name = "Reassignment listings preserve request order occurrences and ongoing status"
    )]
    #[tokio::test]
    async fn success_response_lists_what_kafka_lists(
        reassignment_listing: ReassignmentListingScenario,
        filter: String,
        expected: String,
    ) {
        let _ = (reassignment_listing, filter, expected);
    }
}
