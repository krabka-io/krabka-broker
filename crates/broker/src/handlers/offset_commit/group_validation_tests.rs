//! Handler tests for how `OffsetCommit` fences a commit against the group,
//! as Kafka's `OffsetMetadataManager.validateOffsetCommit` does through
//! `ClassicGroup.validateOffsetCommit` and `ConsumerGroup.validateOffsetCommit`,
//! and how it refuses a partition whose metadata is too large.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::WireUuid,
    crate::{
        authorizer::{AllowAllAuthorizer, AuthorizationResult},
        broker::Broker,
        codes,
        coordinator::unified::{
            GroupSeed,
            actor::{GroupActorMessage, GroupKindTag, test_support::classic_member},
            classic_state::{ClassicGroup, GroupState},
            group::{CoordinatorGroup, GroupKind},
            persistence_next_gen::{
                AssignedTopicPartitions, ClassicMemberMetadata, CurrentMemberAssignmentValue,
                CurrentTopicPartitions, MemberAssignmentState, MemberMetadataValue,
                TargetAssignmentMemberValue,
            },
            test_support::{seed_streams_group, stop_streams_actor},
        },
        handlers::{RequestContext, test_support::configured_topic_request},
        test_support::{
            dispatch_context, dispatch_wire, encode_request, peer, principal, request_context,
            start_group_broker_no_audit,
        },
    },
    assert2::assert,
    krabka_metadata::ResourceType,
    krabka_protocol::owned::{
        create_topics_request::{self},
        offset_commit_request::{
            API_KEY, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
    krabka_security::Principal,
    std::{collections::HashMap, net::SocketAddr, sync::Arc},
    tokio::sync::oneshot,
};

const TOPIC: &str = "group-validation";
const MEMBER: &str = "m1";

/// Keep the shared consumer's request resources alive across each validation table.
struct ConsumerIdentity {
    address: SocketAddr,
    user: Principal,
}

fn consumer_identity() -> ConsumerIdentity {
    let user = principal("consumer");
    let address = peer();
    ConsumerIdentity { address, user }
}

impl ConsumerIdentity {
    fn context(&self) -> RequestContext<'_> {
        request_context(&self.user, &self.address, "consumer-client")
    }
}
const GENERATION: i32 = 3;

fn offset_commit_request(
    group_id: impl Into<String>,
    generation: i32,
    member_id: impl Into<String>,
    topics: Vec<OffsetCommitRequestTopic>,
) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: group_id.into(),
        generation_id_or_member_epoch: generation,
        member_id: member_id.into(),
        topics,
        ..Default::default()
    }
}

/// The group that one row commits to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    /// No group of that id exists.
    Missing,
    /// A `Stable` classic group that holds `MEMBER` at `GENERATION`.
    ClassicWithMember,
    /// A consumer (KIP-848) group with no members.
    EmptyConsumer,
    /// A streams (KIP-1071) group whose actor exited, as one does after a
    /// failed write. A respawned actor holds no member.
    StoppedStreams,
}

/// The partitions of `TOPIC` in the KIP-1251 test: partition 2 exists but is
/// assigned to no member.
const KIP_1251_TOPIC_PARTITIONS: i32 = 3;

async fn create_topic(broker: &Broker) { create_topic_with_partitions(broker, 1).await; }

async fn create_topic_with_partitions(broker: &Broker, num_partitions: i32) {
    krabka_macros::request_test_identity! {
        bind(admin, address, ctx);
        user ["admin"];
        client ["group-validation-admin"];
    };
    let request = configured_topic_request()
        .topic(TOPIC)
        .num_partitions(num_partitions)
        .call();
    dispatch_context(
        broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
}

async fn seed(broker: &Broker, group_id: &str, group: Group) {
    match group {
        Group::Missing => {}
        Group::ClassicWithMember => {
            let mut state = ClassicGroup::new(group_id);
            state.protocol_type = Some("consumer".into());
            state.add_member(classic_member(MEMBER));
            state.state = GroupState::Stable;
            state.generation_id = GENERATION;
            let seeded =
                CoordinatorGroup::seeded(group_id, GroupKind::Classic(state), HashMap::new());
            broker
                .group_coordinator
                .seed_classic(group_id, Box::new(seeded));
        }
        Group::EmptyConsumer => {
            let _ = broker
                .group_coordinator
                .get_or_create_group(group_id, GroupKindTag::Consumer);
        }
        Group::StoppedStreams => {
            seed_streams_group()
                .coordinator(&broker.group_coordinator)
                .group_id(group_id)
                .call()
                .await;
            stop_streams_actor(&broker.group_coordinator, group_id).await;
        }
    }
}

#[derive(Default)]
struct CommitGroupScenario {
    started: Option<HandlerStartedBroker>,
    version: i16,
    request: Option<OffsetCommitRequest>,
    response: Option<OffsetCommitResponse>,
    group_exists: Option<bool>,
}
#[rstest::fixture]
fn commit_group() -> CommitGroupScenario {
    let started = None;
    CommitGroupScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("a ready offset coordinator and a group that is {state}")]
async fn offset_commit_group_state(commit_group: &mut CommitGroupScenario, state: String) {
    let group = match state.as_str() {
        "missing" => Group::Missing,
        "classic stable with member m1 at generation 3" => Group::ClassicWithMember,
        "an empty consumer group" => Group::EmptyConsumer,
        "a streams group whose actor stopped" => Group::StoppedStreams,
        _ => panic!("unknown commit group state: {state}"),
    };
    let started = start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let broker = started.broker.broker_arc_for_test();
    create_topic(&broker).await;
    started.broker.wait_until_partition_present(TOPIC, 0).await;
    seed(&broker, "group-validation-0", group).await;
    commit_group.started = Some(started);
}
#[rstest_bdd_macros::given(
    "an OffsetCommit version {version} request uses member {member} and generation {generation}"
)]
fn offset_commit_group_request(
    commit_group: &mut CommitGroupScenario,
    version: i16,
    member: String,
    generation: i32,
) {
    commit_group.version = version;
    commit_group.request = Some(offset_commit_request(
        "group-validation-0",
        generation,
        if member == "empty" {
            String::new()
        } else {
            member
        },
        vec![super::test_topic_row().name(TOPIC.to_owned()).call()],
    ));
}
#[rstest_bdd_macros::when("the consumer commits its offset through the group coordinator")]
async fn handle_offset_commit_group(commit_group: &mut CommitGroupScenario) {
    let broker = commit_group
        .started
        .as_ref()
        .unwrap()
        .broker
        .broker_arc_for_test();
    let identity = consumer_identity();
    let ctx = identity.context();
    commit_group.response = Some(
        dispatch_wire(
            &broker,
            API_KEY,
            commit_group.version,
            commit_group.request.as_ref().unwrap(),
            &ctx,
        )
        .await,
    );
    commit_group.group_exists = Some(
        broker
            .group_coordinator
            .find("group-validation-0")
            .is_some(),
    );
}
#[rstest_bdd_macros::then(
    "the complete commit response has partition error {code} and group existence is {exists}"
)]
async fn check_offset_commit_group(
    commit_group: &mut CommitGroupScenario,
    code: i16,
    exists: bool,
) {
    assert!(
        commit_group.response.as_ref().unwrap()
            == &OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: TOPIC.to_owned(),
                    partitions: vec![OffsetCommitResponsePartition {
                        partition_index: 0,
                        error_code: code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
    );
    assert!(commit_group.group_exists.unwrap() == exists);
    commit_group.started.take().unwrap().broker.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=missing, version=9, member=empty (1)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=missing, version=9, member=m1 (2)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_2(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=missing, version=8, member=m1 (3)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_3(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=8, member=empty (4)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_4(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=9, member=empty (5)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_5(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=classic stable with member m1 at generation 3, version=9, member=m1 (6)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_6(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=an empty consumer group, version=9, member=empty (7)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_7(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_offset_commit_group_fencing/group.feature",
    name = "Group kind membership generation and protocol version determine commit fencing: state=a streams group whose actor stopped, version=9, member=empty (8)"
)]
#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule_case_8(commit_group: CommitGroupScenario) {
    let _ = (commit_group,);
}

/// Allows everything but `Read` on the topic `DENIED_TOPIC`.
#[derive(Debug)]
struct DenyOneTopic;

const DENIED_TOPIC: &str = "denied-topic";

krabka_macros::test_authorizer!(DenyOneTopic, req; {
    if req.resource_type == ResourceType::Topic && req.resource_name == DENIED_TOPIC {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// Kafka checks topic `Read` in `KafkaApis` before it calls the coordinator,
/// so the coordinator's error goes on the allowed topics only, and a request
/// with no allowed topic never reaches the coordinator.
#[tokio::test]
async fn topic_read_is_checked_before_the_group() {
    let HandlerStartedBroker { broker, dir: _dir } =
        start_group_broker_no_audit(Arc::new(DenyOneTopic)).await;
    let shared = broker.broker_arc_for_test();
    create_topic(&shared).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    seed(&shared, "authz-mixed", Group::ClassicWithMember).await;

    let row = |name: &str| OffsetCommitRequestTopic {
        name: name.to_owned(),
        partitions: vec![OffsetCommitRequestPartition {
            partition_index: 0,
            committed_offset: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    let answer = |name: &str, error_code| OffsetCommitResponseTopic {
        name: name.to_owned(),
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    let cases = [
        // One denied topic and one allowed topic with a wrong generation.
        ("authz-mixed", vec![row(DENIED_TOPIC), row(TOPIC)], vec![
            answer(DENIED_TOPIC, codes::TOPIC_AUTHORIZATION_FAILED),
            answer(TOPIC, codes::ILLEGAL_GENERATION),
        ]),
        // Every topic denied on a group that does not exist: the coordinator
        // is not called, so neither GROUP_ID_NOT_FOUND nor a new group.
        ("authz-all-denied", vec![row(DENIED_TOPIC)], vec![answer(
            DENIED_TOPIC,
            codes::TOPIC_AUTHORIZATION_FAILED,
        )]),
    ];
    let identity = consumer_identity();
    let ctx = identity.context();
    for (group_id, topics, answers) in cases {
        let request = offset_commit_request(group_id, GENERATION + 1, MEMBER, topics);
        let response: OffsetCommitResponse =
            dispatch_wire(&shared, API_KEY, 9, &request, &ctx).await;
        assert!(
            response
                == OffsetCommitResponse {
                    topics: answers,
                    ..Default::default()
                },
            "{group_id}"
        );
        assert!(
            shared.group_coordinator.find(group_id).is_some() == (group_id == "authz-mixed"),
            "{group_id}"
        );
    }
    broker.shutdown().await;
}

/// A partition whose metadata is longer than `offset.metadata.max.bytes`
/// answers `OFFSET_METADATA_TOO_LARGE` and leaves no committed offset.
#[tokio::test]
async fn oversized_metadata_is_refused_through_the_handler() {
    const GROUP_ID: &str = "oversized-metadata";
    let HandlerStartedBroker { broker, dir: _dir } =
        start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let shared = broker.broker_arc_for_test();
    create_topic(&shared).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let max = usize::try_from(shared.config.offset_metadata_max_bytes).unwrap();
    let partition = OffsetCommitRequestPartition {
        partition_index: 0,
        committed_offset: 42,
        committed_metadata: Some("m".repeat(max + 1)),
        ..Default::default()
    };
    let request = OffsetCommitRequest {
        group_id: GROUP_ID.to_owned(),
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.to_owned(),
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let identity = consumer_identity();
    let ctx = identity.context();
    let response: OffsetCommitResponse = dispatch_wire(&shared, API_KEY, 9, &request, &ctx).await;

    let actor = shared
        .group_coordinator
        .find(GROUP_ID)
        .expect("simple group");
    let (reply, offsets) = oneshot::channel();
    actor
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .expect("send FetchOffsets");
    let committed = offsets.await.expect("FetchOffsets reply").committed;

    let expected = OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: TOPIC.to_owned(),
            partitions: vec![OffsetCommitResponsePartition {
                partition_index: 0,
                error_code: codes::OFFSET_METADATA_TOO_LARGE,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!((response, committed.is_empty()) == (expected, true));
    broker.shutdown().await;
}

/// The consumer group that one KIP-1251 row commits to: `native` and
/// `classic` at member epoch 5, each assigned partition 0 at epoch 3 and
/// partition 1 at epoch 5, restored from records as a coordinator failover
/// restores them.
fn consumer_group_seed(topic_id: WireUuid) -> GroupSeed {
    let metadata = |classic: bool| MemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: "client".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: vec![TOPIC.into()],
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout_ms: 60_000,
        classic: classic.then(|| ClassicMemberMetadata {
            session_timeout_ms: 45_000,
            supported_protocols: vec![("range".into(), bytes::Bytes::new())],
        }),
    };
    let current = CurrentMemberAssignmentValue {
        member_epoch: 5,
        previous_member_epoch: 4,
        state: MemberAssignmentState::Stable,
        assigned_partitions: vec![CurrentTopicPartitions {
            topic_id,
            partitions: vec![0, 1],
            assignment_epochs: Some(vec![3, 5]),
        }],
        partitions_pending_revocation: vec![],
    };
    let target = TargetAssignmentMemberValue {
        topic_partitions: vec![AssignedTopicPartitions {
            topic_id,
            partitions: vec![0, 1],
        }],
    };
    GroupSeed {
        group_epoch: 5,
        target_epoch: 5,
        assignment_timestamp_ms: 0,
        members: [
            ("native".to_owned(), metadata(false)),
            ("classic".to_owned(), metadata(true)),
        ]
        .into(),
        target_per_member: [
            ("native".to_owned(), target.clone()),
            ("classic".to_owned(), target),
        ]
        .into(),
        current_per_member: [
            ("native".to_owned(), current.clone()),
            ("classic".to_owned(), current),
        ]
        .into(),
        ..GroupSeed::default()
    }
}

/// Issue #800's table through the `OffsetCommit` handler at v9: Kafka 4.3.1's
/// `ConsumerGroup.validateOffsetCommit` with the KIP-1251 per-partition
/// assignment-epoch validator. A refusal answers every committed row.
#[tokio::test]
async fn consumer_group_commit_follows_kip_1251() {
    struct Row {
        member_id: &'static str,
        epoch: i32,
        version: i16,
        partitions: &'static [i32],
        error_code: i16,
    }
    let rows = [
        // Native member.
        Row {
            member_id: "native",
            epoch: 5,
            version: 9,
            partitions: &[0, 1],
            error_code: codes::NONE,
        },
        Row {
            member_id: "native",
            epoch: 6,
            version: 9,
            partitions: &[0],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[0],
            error_code: codes::NONE,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[1],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[2],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 5,
            version: 8,
            partitions: &[0],
            error_code: codes::UNSUPPORTED_VERSION,
        },
        // Classic-protocol member.
        Row {
            member_id: "classic",
            epoch: 6,
            version: 9,
            partitions: &[0],
            error_code: codes::ILLEGAL_GENERATION,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 9,
            partitions: &[0],
            error_code: codes::NONE,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 9,
            partitions: &[1],
            error_code: codes::ILLEGAL_GENERATION,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 8,
            partitions: &[0],
            error_code: codes::NONE,
        },
    ];
    let HandlerStartedBroker { broker, dir: _dir } =
        start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let shared = broker.broker_arc_for_test();
    create_topic_with_partitions(&shared, KIP_1251_TOPIC_PARTITIONS).await;
    for partition in 0..KIP_1251_TOPIC_PARTITIONS {
        broker.wait_until_partition_present(TOPIC, partition).await;
    }
    let topic_id = WireUuid(
        shared
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic")
            .topic_id
            .into_bytes(),
    );
    let identity = consumer_identity();
    let ctx = identity.context();

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let group_id = format!("kip-1251-{i}");
        let actor = shared.group_coordinator.get_or_create_consumer(&group_id);
        actor
            .tx
            .send(GroupActorMessage::Seed(consumer_group_seed(topic_id)))
            .await
            .expect("seed");
        let request = offset_commit_request(group_id, row.epoch, row.member_id, vec![
            OffsetCommitRequestTopic {
                name: TOPIC.into(),
                partitions: row
                    .partitions
                    .iter()
                    .map(|&partition_index| OffsetCommitRequestPartition {
                        partition_index,
                        committed_offset: 42,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
        ]);
        let response: OffsetCommitResponse =
            dispatch_wire(&shared, API_KEY, row.version, &request, &ctx).await;
        actual.push((row.member_id, row.epoch, row.version, response));
        expected.push((
            row.member_id,
            row.epoch,
            row.version,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: TOPIC.into(),
                    partitions: row
                        .partitions
                        .iter()
                        .map(|&partition_index| OffsetCommitResponsePartition {
                            partition_index,
                            error_code: row.error_code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}
