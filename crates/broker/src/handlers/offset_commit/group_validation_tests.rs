//! Handler tests for how `OffsetCommit` fences a commit against the group,
//! as Kafka's `OffsetMetadataManager.validateOffsetCommit` does through
//! `ClassicGroup.validateOffsetCommit` and `ConsumerGroup.validateOffsetCommit`,
//! and how it refuses a partition whose metadata is too large.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_protocol::owned::{
    create_topics_request::{self, CreatableTopic, CreateTopicsRequest},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
};

use super::handle;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::{Broker, BrokerHandle},
    codes,
    coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag},
        classic_state::{ClassicGroup, GroupState, Member},
        group::{CoordinatorGroup, GroupKind},
    },
    test_support::{
        decode_response, dispatch_context, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

const TOPIC: &str = "group-validation";
const MEMBER: &str = "m1";
const GENERATION: i32 = 3;

/// The group that one row commits to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    /// No group of that id exists.
    Missing,
    /// A `Stable` classic group that holds `MEMBER` at `GENERATION`.
    ClassicWithMember,
    /// A consumer (KIP-848) group with no members.
    EmptyConsumer,
}

/// One row: the group, the request version, member id and generation, and
/// the error code Kafka answers on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    group: Group,
    version: i16,
    member_id: &'static str,
    generation: i32,
    error_code: i16,
}

async fn create_topic(broker: &Broker) {
    let admin = principal("admin");
    let address = peer();
    let ctx = request_context(&admin, &address, "group-validation-admin");
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: TOPIC.to_string(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    dispatch_context(
        broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
}

fn seed(broker: &Broker, group_id: &str, group: Group) {
    match group {
        Group::Missing => {}
        Group::ClassicWithMember => {
            let mut state = ClassicGroup::new(group_id);
            state.protocol_type = Some("consumer".into());
            state.add_member(Member::new(
                MEMBER,
                "client",
                "127.0.0.1",
                Duration::from_secs(30),
                Duration::from_mins(1),
                vec![("range".into(), bytes::Bytes::new())],
            ));
            state.state = GroupState::Stable;
            state.generation_id = GENERATION;
            let seeded = CoordinatorGroup::seeded(
                group_id,
                GroupKind::Classic(state),
                std::collections::HashMap::new(),
            );
            broker
                .group_coordinator
                .seed_classic(group_id, Box::new(seeded));
        }
        Group::EmptyConsumer => {
            let _ = broker
                .group_coordinator
                .get_or_create_group(group_id, GroupKindTag::Consumer);
        }
    }
}

/// Runs one row in a group of its own, and returns the response and whether
/// the group exists afterwards.
async fn drive(broker: &BrokerHandle, row: usize, case: Case) -> (OffsetCommitResponse, bool) {
    let shared = broker.broker_arc_for_test();
    let group_id = format!("group-validation-{row}");
    seed(&shared, &group_id, case.group);
    let request = OffsetCommitRequest {
        group_id: group_id.clone(),
        generation_id_or_member_epoch: case.generation,
        member_id: case.member_id.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let user = principal("consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "consumer-client");
    let bytes = handle(
        &shared,
        case.version,
        7,
        &encode_request(&request, case.version),
        &ctx,
    )
    .await
    .expect("handle offset commit");
    let response = decode_response(&bytes, case.version);
    (response, shared.group_coordinator.find(&group_id).is_some())
}

#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule() {
    let cases = [
        // A group that does not exist is created as a simple group for a
        // negative generation.
        Case {
            group: Group::Missing,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::NONE,
        },
        // Otherwise it is not found from v9 on, and an illegal generation
        // before, and it is not created.
        Case {
            group: Group::Missing,
            version: 9,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::GROUP_ID_NOT_FOUND,
        },
        Case {
            group: Group::Missing,
            version: 8,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::ILLEGAL_GENERATION,
        },
        // The admin client may not move the offsets of a classic group that
        // has members.
        Case {
            group: Group::ClassicWithMember,
            version: 8,
            member_id: "",
            generation: -1,
            error_code: codes::UNKNOWN_MEMBER_ID,
        },
        Case {
            group: Group::ClassicWithMember,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::UNKNOWN_MEMBER_ID,
        },
        Case {
            group: Group::ClassicWithMember,
            version: 9,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::NONE,
        },
        // The admin client may move the offsets of a consumer group that has
        // no members.
        Case {
            group: Group::EmptyConsumer,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::NONE,
        },
    ];
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic(&broker.broker_arc_for_test()).await;
    broker.wait_until_partition_present(TOPIC, 0).await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (row, case) in cases.into_iter().enumerate() {
        let (response, exists) = drive(&broker, row, case).await;
        actual.push((case.group, case.version, case.member_id, response, exists));
        expected.push((
            case.group,
            case.version,
            case.member_id,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: TOPIC.to_string(),
                    partitions: vec![OffsetCommitResponsePartition {
                        partition_index: 0,
                        error_code: case.error_code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            case.group != Group::Missing || case.error_code == codes::NONE,
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A partition whose metadata is longer than `offset.metadata.max.bytes`
/// answers `OFFSET_METADATA_TOO_LARGE` and leaves no committed offset.
#[tokio::test]
async fn oversized_metadata_is_refused_through_the_handler() {
    const GROUP_ID: &str = "oversized-metadata";
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let shared = broker.broker_arc_for_test();
    create_topic(&shared).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let max = usize::try_from(shared.config.offset_metadata_max_bytes).unwrap();
    let request = OffsetCommitRequest {
        group_id: GROUP_ID.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                committed_metadata: Some("m".repeat(max + 1)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let user = principal("consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "consumer-client");
    let bytes = handle(&shared, 9, 7, &encode_request(&request, 9), &ctx)
        .await
        .expect("handle offset commit");
    let response: OffsetCommitResponse = decode_response(&bytes, 9);

    let actor = shared
        .group_coordinator
        .find(GROUP_ID)
        .expect("simple group");
    let (reply, offsets) = tokio::sync::oneshot::channel();
    actor
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .expect("send FetchOffsets");
    let committed = offsets.await.expect("FetchOffsets reply").committed;

    let expected = OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: TOPIC.to_string(),
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
