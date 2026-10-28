//! Handler tests for `OffsetDelete`: the group-level refusals, Kafka's check
//! order and the subscription guard, each as a whole response.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{
        handle,
        test_support::{RequestTopic, expected_row, expected_topic, req_with_topics},
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        coordinator::{
            bootstrap::OFFSETS_TOPIC,
            partitioner::local_partition_for_group,
            test_support::offset_entry,
            unified::{
                StreamsGroupSeed,
                actor::{GroupActorMessage, GroupKindTag},
                classic_state::{ClassicGroup, Member},
                consumer_state::{
                    GroupState as ConsumerGroup, ResolvedRegularExpression, test_support::member,
                },
                group::{CoordinatorGroup, GroupKind},
                streams::{
                    actor::StreamsGroupActorMessage,
                    persistence::{StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue},
                },
                test_support::seed_streams_group,
            },
        },
        handlers::test_support::create_topic as create_topic_from_test_support,
        test_support::{DenyAll, start_group_broker_no_audit},
        time_util::now_ms,
    },
    assert2::check,
    bytes::Bytes,
    krabka_protocol::owned::{
        offset_delete_request::OffsetDeleteRequest,
        offset_delete_response::{self, OffsetDeleteResponse},
    },
    std::{collections::HashMap, sync::Arc, time::Duration},
    tokio::sync::oneshot,
};

async fn create_topic(broker: &BrokerHandle, name: &str) {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("offset-delete-test")
        .name(name)
        .call()
        .await;
}

use crate::coordinator::unified::actor::test_support::subscription_blob as subscription;

/// A `Stable` classic group with one member whose selected protocol carries
/// `metadata`.
fn classic_group(group_id: &str, protocol_type: &str, metadata: Bytes) -> CoordinatorGroup {
    let mut state = ClassicGroup::new(group_id);
    state.protocol_type = Some(protocol_type.into());
    state.add_member(Member::new(
        "m1",
        "client",
        "host",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), metadata)],
    ));
    state.resolve_selected_protocol_metadata("range");
    state.complete_rebalance("range");
    state.install_assignments(HashMap::from([("m1".to_owned(), Bytes::new())]));
    CoordinatorGroup::seeded(group_id, GroupKind::Classic(state), HashMap::new())
}

/// A KIP-848 consumer group with one member that subscribes by regex.
fn consumer_group(group_id: &str, regex: &str, resolved: &[&str]) -> CoordinatorGroup {
    let mut state = ConsumerGroup::new(group_id);
    let mut m = member("m1");
    m.subscribed_topic_regex = Some(regex.into());
    state.add_or_update_member(m);
    state.set_resolved_regex(regex.into(), ResolvedRegularExpression {
        topics: resolved.iter().map(|s| (*s).to_owned()).collect(),
        version: 1,
        timestamp_ms: 1,
    });
    CoordinatorGroup::seeded(group_id, GroupKind::Consumer(state), HashMap::new())
}

fn whole(code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        ..Default::default()
    }
}

#[derive(Default)]
struct OffsetDeletionScenario {
    started: Option<HandlerStartedBroker>,
    response: Option<OffsetDeleteResponse>,
    before_ms: i64,
}
#[rstest::fixture]
fn offset_deletion() -> OffsetDeletionScenario {
    let started = None;
    OffsetDeletionScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("an offset deletion broker with group Delete allowed {allowed}")]
async fn offset_deletion_authorization(
    offset_deletion: &mut OffsetDeletionScenario,
    allowed: bool,
) {
    let authorizer: Arc<dyn Authorizer> = if allowed {
        Arc::new(AllowAllAuthorizer)
    } else {
        Arc::new(DenyAll)
    };
    offset_deletion.started = Some(start_group_broker_no_audit(authorizer).await);
}
#[rstest_bdd_macros::given(
    "topics t and u have one partition, classic-g subscribes to t and ghost and commits u-0 at offset 5, connect-g speaks connect, garbled-g has consumer metadata byte 00 and kip848-g resolves regex t.* to t"
)]
async fn offset_deletion_group_state(offset_deletion: &mut OffsetDeletionScenario) {
    let started = offset_deletion.started.as_ref().expect("started broker");
    create_topic(&started.broker, "t").await;
    create_topic(&started.broker, "u").await;
    let coordinator = Arc::clone(&started.broker.broker_arc_for_test().group_coordinator);
    for group in [
        classic_group("connect-g", "connect", Bytes::from_static(b"opaque")),
        CoordinatorGroup::seeded(
            "classic-g",
            classic_group("classic-g", "consumer", subscription(&["t", "ghost"])).kind,
            HashMap::from([(("u".to_owned(), 0), offset_entry(5))]),
        ),
        classic_group("garbled-g", "consumer", Bytes::from_static(b"\x00")),
        consumer_group("kip848-g", "t.*", &["t"]),
    ] {
        let group_id = group.group_id.clone();
        coordinator.seed_classic(&group_id, Box::new(group));
    }
}
#[rstest_bdd_macros::when("alice deletes offsets for group \"{group}\" and these topic partitions")]
async fn delete_group_offsets(
    offset_deletion: &mut OffsetDeletionScenario,
    group: String,
    datatable: Vec<Vec<String>>,
) {
    let input = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            (
                cells[0].clone(),
                cells[1]
                    .split(',')
                    .map(|p| p.parse::<i32>().expect("partition"))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let topics = input
        .iter()
        .map(|(name, partitions)| RequestTopic { name, partitions })
        .collect::<Vec<_>>();
    let request = OffsetDeleteRequest {
        group_id: if group == "<empty>" {
            String::new()
        } else {
            group
        },
        ..req_with_topics(&topics)
    };
    let broker = offset_deletion
        .started
        .as_ref()
        .expect("started broker")
        .broker
        .broker_arc_for_test();
    krabka_macros::request_test_identity! { bind(principal, peer, ctx); user ["alice"]; client ["offset-delete-client"]; };
    offset_deletion.before_ms = now_ms();
    offset_deletion.response = Some(
        handle(&broker, request, offset_delete_response::MAX_VERSION, &ctx)
            .await
            .expect("OffsetDelete"),
    );
}
#[rstest_bdd_macros::then(
    "the complete offset deletion response has top-level code {code} and these ordered topic partitions"
)]
async fn offset_deletion_response(
    offset_deletion: &mut OffsetDeletionScenario,
    code: i16,
    datatable: Vec<Vec<String>>,
) {
    let mut topics: Vec<OffsetDeleteResponseTopic> = Vec::new();
    for cells in datatable.into_iter().skip(1) {
        if topics.last().is_none_or(|topic| topic.name != cells[0]) {
            topics.push(expected_topic(&cells[0], Vec::new()));
        }
        topics
            .last_mut()
            .expect("topic row")
            .partitions
            .push(expected_row(
                cells[1].parse().expect("partition"),
                cells[2].parse().expect("partition error"),
            ));
    }
    let expected = OffsetDeleteResponse {
        topics,
        ..whole(code)
    };
    check!(offset_deletion.response.as_ref().expect("delete response") == &expected);
    if expected.topics.iter().any(|topic| {
        topic
            .partitions
            .iter()
            .any(|partition| partition.error_code == codes::NONE)
    }) {
        let broker = offset_deletion
            .started
            .as_ref()
            .expect("started broker")
            .broker
            .broker_arc_for_test();
        let partition = local_partition_for_group(
            &broker.controller.current_image(),
            broker.config.node_id,
            "classic-g",
        )
        .expect("local offsets partition");
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, krabka_ids::PartitionIndex(partition))
            .expect("offsets partition");
        let log = part.log.lock().expect("offsets log");
        let (offset, timestamp) = log
            .offset_for_timestamp(offset_deletion.before_ms)
            .expect("dated tombstone");
        check!(offset == log.log_end_offset() - 1);
        check!(timestamp <= now_ms());
        drop(log);
    }
    offset_deletion
        .started
        .take()
        .expect("started broker")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Group Delete denial returns only a top-level error"
)]
#[tokio::test]
async fn offset_deletion_case_0(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Empty group IDs are invalid"
)]
#[tokio::test]
async fn offset_deletion_case_1(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Missing groups return only a top-level error"
)]
#[tokio::test]
async fn offset_deletion_case_2(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "A non-consumer nonempty classic group cannot delete offsets"
)]
#[tokio::test]
async fn offset_deletion_case_3(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Undecodable consumer metadata protects every topic"
)]
#[tokio::test]
async fn offset_deletion_case_4(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Resolved consumer regex subscriptions protect matching topics"
)]
#[tokio::test]
async fn offset_deletion_case_5(offset_deletion: OffsetDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_deletion.feature",
    name = "Existence precedes subscription and successful deletions write clock-dated tombstones"
)]
#[tokio::test]
async fn offset_deletion_case_6(offset_deletion: OffsetDeletionScenario) {}

/// The group that a [`StreamsDeleteRow`] deletes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamsGroup {
    /// A streams group whose member reads `t`, with no offsets actor: it has
    /// never committed an offset.
    NoOffsets,
    /// The same group with an offsets actor that holds committed offsets for
    /// `t-0` and `u-0`.
    WithOffsets,
    /// A streams group with the same topology and no member.
    Empty,
}

/// One `OffsetDelete` against a streams group: the topics it names, the whole
/// expected response, and whether it writes to `__consumer_offsets`.
struct StreamsDeleteRow {
    name: &'static str,
    group: StreamsGroup,
    topics: &'static [RequestTopic<'static>],
    want: OffsetDeleteResponse,
    writes: bool,
}

/// The log end offset of the `__consumer_offsets` partition of `group_id`.
fn offsets_log_end(broker: &BrokerHandle, group_id: &str) -> krabka_ids::Offset {
    let broker = broker.broker_arc_for_test();
    let partition = local_partition_for_group(
        &broker.controller.current_image(),
        broker.config.node_id,
        group_id,
    )
    .expect("this broker coordinates the group");
    let part = broker
        .partitions
        .get(OFFSETS_TOPIC, krabka_ids::PartitionIndex(partition))
        .expect("the offsets partition");
    part.log.lock().expect("offsets log").log_end_offset()
}

/// Sends one `OffsetDelete` for `group_id` and returns the whole response
/// with the number of records it added to the group's `__consumer_offsets`
/// partition.
async fn delete_counting_writes(
    broker: &BrokerHandle,
    group_id: &str,
    topics: &[RequestTopic<'_>],
) -> (OffsetDeleteResponse, i64) {
    krabka_macros::request_test_identity! {
        bind(principal, peer, ctx);
        user ["alice"];
        client ["offset-delete-client"];
    };
    let before = offsets_log_end(broker, group_id);
    let req = OffsetDeleteRequest {
        group_id: group_id.to_owned(),
        ..req_with_topics(topics)
    };
    let resp = handle(
        &broker.broker_arc_for_test(),
        req,
        offset_delete_response::MAX_VERSION,
        &ctx,
    )
    .await
    .expect("OffsetDelete");
    (resp, offsets_log_end(broker, group_id).0 - before.0)
}

/// Kafka's `OffsetMetadataManager.deleteOffsets` on a streams group: the
/// group is found whether or not it has committed an offset,
/// `StreamsGroup.isSubscribedToTopic` refuses the source and repartition
/// source topics of a non-empty group's topology with
/// `GROUP_SUBSCRIBED_TO_TOPIC`, and every other partition answers `NONE`. A
/// group that holds no offset writes no tombstone.
#[tokio::test]
async fn offset_delete_on_a_streams_group_follows_its_topology() {
    let one = |name: &'static str, code: i16| OffsetDeleteResponse {
        topics: vec![expected_topic(name, vec![expected_row(0, code)])],
        ..whole(codes::NONE)
    };
    let rows = [
        StreamsDeleteRow {
            name: "a source topic of a group with no offsets",
            group: StreamsGroup::NoOffsets,
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: one("t", codes::GROUP_SUBSCRIBED_TO_TOPIC),
            writes: false,
        },
        StreamsDeleteRow {
            name: "a repartition source topic of a group with no offsets",
            group: StreamsGroup::NoOffsets,
            topics: &[RequestTopic {
                name: "r",
                partitions: &[0],
            }],
            want: one("r", codes::GROUP_SUBSCRIBED_TO_TOPIC),
            writes: false,
        },
        StreamsDeleteRow {
            name: "another topic of a group with no offsets",
            group: StreamsGroup::NoOffsets,
            topics: &[RequestTopic {
                name: "u",
                partitions: &[0],
            }],
            want: one("u", codes::NONE),
            writes: false,
        },
        StreamsDeleteRow {
            name: "a source topic of a group with offsets",
            group: StreamsGroup::WithOffsets,
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: one("t", codes::GROUP_SUBSCRIBED_TO_TOPIC),
            writes: false,
        },
        StreamsDeleteRow {
            name: "another topic of a group with offsets",
            group: StreamsGroup::WithOffsets,
            topics: &[RequestTopic {
                name: "u",
                partitions: &[0],
            }],
            want: one("u", codes::NONE),
            writes: true,
        },
        StreamsDeleteRow {
            name: "a source topic of an empty group",
            group: StreamsGroup::Empty,
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: one("t", codes::NONE),
            writes: false,
        },
    ];

    let HandlerStartedBroker { broker, dir: _dir } =
        start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    for topic in ["t", "u", "r"] {
        create_topic(&broker, topic).await;
    }
    let coordinator = Arc::clone(&broker.broker_arc_for_test().group_coordinator);
    let topology = StreamsGroupTopologyValue {
        epoch: 1,
        subtopologies: vec![StoredSubtopology {
            subtopology_id: "0".into(),
            source_topics: vec!["t".into()],
            repartition_source_topics: vec![StoredTopicInfo {
                name: "r".into(),
                partitions: 1,
                replication_factor: -1,
                topic_configs: vec![],
            }],
            ..StoredSubtopology::default()
        }],
    };

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (i, row) in rows.into_iter().enumerate() {
        let group_id = format!("streams-{i}");
        if row.group == StreamsGroup::Empty {
            coordinator.mark_streams(&group_id);
            coordinator
                .get_or_create_streams(&group_id)
                .tx
                .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                    topology: Some(topology.clone()),
                    ..StreamsGroupSeed::default()
                }))
                .await
                .expect("seed the empty streams group");
        } else {
            seed_streams_group()
                .coordinator(&coordinator)
                .group_id(&group_id)
                .topology(topology.clone())
                .call()
                .await;
        }
        if row.group == StreamsGroup::WithOffsets {
            let (reply, done) = oneshot::channel();
            coordinator
                .get_or_create_group(&group_id, GroupKindTag::Classic)
                .tx
                .send(GroupActorMessage::UpdateCommitted {
                    entries: vec![
                        (("t".to_owned(), 0), offset_entry(5)),
                        (("u".to_owned(), 0), offset_entry(5)),
                    ],
                    reply,
                })
                .await
                .expect("commit the streams group's offsets");
            done.await.expect("offsets committed");
        }
        let (resp, written) = delete_counting_writes(&broker, &group_id, row.topics).await;
        actual.push((row.name, resp, written > 0));
        expected.push((row.name, row.want, row.writes));
    }
    check!(actual == expected);

    broker.shutdown().await;
}

/// One `OffsetDelete` against an empty classic group that holds a committed
/// offset for `t-0` and a pending transactional offset for `t-1`: the request,
/// the whole expected response, and how many tombstones it writes.
struct HeldOffsetsRow {
    name: &'static str,
    topics: &'static [RequestTopic<'static>],
    want: OffsetDeleteResponse,
    tombstones: i64,
}

/// Kafka's `OffsetMetadataManager.deleteOffsets` answers `NONE` for every
/// partition the group does not subscribe to, but writes a tombstone only for
/// one where `hasCommittedOffset` or `hasPendingTransactionalOffsets` holds.
#[tokio::test]
async fn offset_delete_tombstones_only_offsets_the_group_holds() {
    let rows = [
        HeldOffsetsRow {
            name: "a committed offset",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("t", vec![expected_row(0, codes::NONE)])],
                ..whole(codes::NONE)
            },
            tombstones: 1,
        },
        HeldOffsetsRow {
            name: "no offset",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[2],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("t", vec![expected_row(2, codes::NONE)])],
                ..whole(codes::NONE)
            },
            tombstones: 0,
        },
        HeldOffsetsRow {
            name: "only a pending transactional offset",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[1],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("t", vec![expected_row(1, codes::NONE)])],
                ..whole(codes::NONE)
            },
            tombstones: 1,
        },
        HeldOffsetsRow {
            name: "mixed",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[2, 1, 0],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("t", vec![
                    expected_row(2, codes::NONE),
                    expected_row(1, codes::NONE),
                    expected_row(0, codes::NONE),
                ])],
                ..whole(codes::NONE)
            },
            tombstones: 2,
        },
    ];

    let HandlerStartedBroker { broker, dir: _dir } =
        start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic_from_test_support()
        .broker(&broker)
        .client_id("offset-delete-test")
        .name("t")
        .partitions(3)
        .call()
        .await;
    let coordinator = Arc::clone(&broker.broker_arc_for_test().group_coordinator);

    let want: Vec<_> = rows
        .iter()
        .map(|row| (row.name, row.want.clone(), row.tombstones))
        .collect();
    let mut got = Vec::new();
    for row in rows {
        let group_id = format!("held-{}", row.name.replace(' ', "-"));
        let mut group = CoordinatorGroup::seeded(
            group_id.as_str(),
            GroupKind::Classic(ClassicGroup::new(group_id.as_str())),
            HashMap::from([(("t".to_owned(), 0), offset_entry(5))]),
        );
        group.add_pending_txn_offsets(7, 0, [("t".to_owned(), 1)]);
        coordinator.seed_classic(&group_id, Box::new(group));

        let (resp, tombstones) = delete_counting_writes(&broker, &group_id, row.topics).await;
        got.push((row.name, resp, tombstones));
    }
    check!(got == want);

    broker.shutdown().await;
}

use {
    crate::authorizer::Authorizer,
    krabka_protocol::owned::offset_delete_response::OffsetDeleteResponseTopic,
};
