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
    state.install_assignments(HashMap::from([("m1".to_string(), Bytes::new())]));
    CoordinatorGroup::seeded(group_id, GroupKind::Classic(state), HashMap::new())
}

/// A KIP-848 consumer group with one member that subscribes by regex.
fn consumer_group(group_id: &str, regex: &str, resolved: &[&str]) -> CoordinatorGroup {
    let mut state = ConsumerGroup::new(group_id);
    let mut m = member("m1");
    m.subscribed_topic_regex = Some(regex.into());
    state.add_or_update_member(m);
    state.set_resolved_regex(regex.into(), ResolvedRegularExpression {
        topics: resolved.iter().map(|s| (*s).to_string()).collect(),
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

/// #734 and #813, row by row. Topics `t` and `u` exist with one partition
/// each; `ghost` does not.
/// An `OffsetDelete` case: name, broker, group id, request topics, and the
/// whole expected response.
struct Case<'a> {
    name: &'a str,
    broker: &'a BrokerHandle,
    group_id: &'a str,
    topics: &'a [RequestTopic<'a>],
    want: OffsetDeleteResponse,
}

#[tokio::test]
async fn offset_delete_matches_kafka_whole_responses() {
    let HandlerStartedBroker {
        broker: allowed,
        dir: _allowed_dir,
    } = start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let HandlerStartedBroker {
        broker: denied,
        dir: _denied_dir,
    } = start_group_broker_no_audit(Arc::new(DenyAll)).await;
    create_topic(&allowed, "t").await;
    create_topic(&allowed, "u").await;
    let coordinator = allowed.broker_arc_for_test().group_coordinator.clone();
    for group in [
        classic_group("connect-g", "connect", Bytes::from_static(b"opaque")),
        // `u-0` holds an offset, so its delete writes the tombstone below.
        CoordinatorGroup::seeded(
            "classic-g",
            classic_group("classic-g", "consumer", subscription(&["t", "ghost"])).kind,
            HashMap::from([(("u".to_string(), 0), offset_entry(5))]),
        ),
        classic_group("garbled-g", "consumer", Bytes::from_static(b"\x00")),
        consumer_group("kip848-g", "t.*", &["t"]),
    ] {
        let group_id = group.group_id.clone();
        coordinator.seed_classic(&group_id, Box::new(group));
    }

    let rows: Vec<Case<'_>> = vec![
        Case {
            name: "group Delete denied: top-level only",
            broker: &denied,
            group_id: "classic-g",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: whole(codes::GROUP_AUTHORIZATION_FAILED),
        },
        Case {
            name: "empty group id",
            broker: &allowed,
            group_id: "",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: whole(codes::INVALID_GROUP_ID),
        },
        Case {
            name: "missing group: top-level only",
            broker: &allowed,
            group_id: "ghost-g",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: whole(codes::GROUP_ID_NOT_FOUND),
        },
        Case {
            name: "non-empty group without the consumer protocol",
            broker: &allowed,
            group_id: "connect-g",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: whole(codes::NON_EMPTY_GROUP),
        },
        Case {
            name: "undecodable metadata subscribes to every topic",
            broker: &allowed,
            group_id: "garbled-g",
            topics: &[RequestTopic {
                name: "u",
                partitions: &[0],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("u", vec![expected_row(
                    0,
                    codes::GROUP_SUBSCRIBED_TO_TOPIC,
                )])],
                ..whole(codes::NONE)
            },
        },
        Case {
            name: "KIP-848 group subscribed by regex",
            broker: &allowed,
            group_id: "kip848-g",
            topics: &[RequestTopic {
                name: "t",
                partitions: &[0],
            }],
            want: OffsetDeleteResponse {
                topics: vec![expected_topic("t", vec![expected_row(
                    0,
                    codes::GROUP_SUBSCRIBED_TO_TOPIC,
                )])],
                ..whole(codes::NONE)
            },
        },
        Case {
            name: "existence before subscription, broker rows before the coordinator's",
            broker: &allowed,
            group_id: "classic-g",
            topics: &[
                RequestTopic {
                    name: "t",
                    partitions: &[0],
                },
                RequestTopic {
                    name: "ghost",
                    partitions: &[0],
                },
                RequestTopic {
                    name: "u",
                    partitions: &[0, 5],
                },
            ],
            want: OffsetDeleteResponse {
                topics: vec![
                    expected_topic("ghost", vec![expected_row(
                        0,
                        codes::UNKNOWN_TOPIC_OR_PARTITION,
                    )]),
                    expected_topic("u", vec![
                        expected_row(5, codes::UNKNOWN_TOPIC_OR_PARTITION),
                        expected_row(0, codes::NONE),
                    ]),
                    expected_topic("t", vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)]),
                ],
                ..whole(codes::NONE)
            },
        },
    ];

    krabka_macros::request_test_identity! {
        bind(principal, peer, ctx);
        user ["alice"];
        client ["offset-delete-client"];
    };
    let version = offset_delete_response::MAX_VERSION;
    let before_ms = now_ms();
    for Case {
        name,
        broker,
        group_id,
        topics,
        want,
    } in rows
    {
        let req = OffsetDeleteRequest {
            group_id: group_id.into(),
            ..req_with_topics(topics)
        };
        let resp = handle(&broker.broker_arc_for_test(), req, version, &ctx)
            .await
            .expect("OffsetDelete");
        check!(resp == want, "{name}");
    }

    // The successful delete writes a tombstone whose record timestamp must
    // be the broker's clock, so later commits do not age this segment from 1970.
    {
        let broker = allowed.broker_arc_for_test();
        let partition = local_partition_for_group(
            &broker.controller.current_image(),
            broker.config.node_id,
            "classic-g",
        )
        .unwrap();
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, krabka_ids::PartitionIndex(partition))
            .unwrap();
        let log = part.log.lock().unwrap();
        let (offset, timestamp) = log
            .offset_for_timestamp(before_ms)
            .expect("dated tombstone");
        check!(offset == log.log_end_offset() - 1);
        check!(timestamp <= crate::time_util::now_ms());
    }

    allowed.shutdown().await;
    denied.shutdown().await;
}

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
        group_id: group_id.to_string(),
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
    let coordinator = broker.broker_arc_for_test().group_coordinator.clone();
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
                        (("t".to_string(), 0), offset_entry(5)),
                        (("u".to_string(), 0), offset_entry(5)),
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
    let coordinator = broker.broker_arc_for_test().group_coordinator.clone();

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
            HashMap::from([(("t".to_string(), 0), offset_entry(5))]),
        );
        group.add_pending_txn_offsets(7, 0, [("t".to_string(), 1)]);
        coordinator.seed_classic(&group_id, Box::new(group));

        let (resp, tombstones) = delete_counting_writes(&broker, &group_id, row.topics).await;
        got.push((row.name, resp, tombstones));
    }
    check!(got == want);

    broker.shutdown().await;
}
