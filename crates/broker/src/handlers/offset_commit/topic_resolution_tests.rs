//! Handler tests for how `OffsetCommit` answers a topic row by request version
//! and by the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleOffsetCommitRequest` resolves each `topic_id` at
//! v10 and later. A row whose name stays empty answers `UNKNOWN_TOPIC_ID` on
//! every partition row, the zero id included, and the coordinator commits
//! nothing for it. The check runs after the group authorization and before the
//! topic authorization.

use std::sync::Arc;

use assert2::assert;
use krabka_metadata::ResourceType;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::oneshot;

use super::handle;
use crate::{
    authorizer::{AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer},
    broker::BrokerHandle,
    codes,
    coordinator::unified::actor::GroupActorMessage,
    test_support::{
        DenyAll, decode_response, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The name of the topic that exists in these tests.
const KNOWN_NAME: &str = "offsets-resolution";

/// Allows every group operation and denies every topic operation.
#[derive(Debug)]
struct DenyTopics;

impl Authorizer for DenyTopics {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if req.resource_type == ResourceType::Topic {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }
}

/// The topic reference that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicRef {
    /// The name of the topic that exists (v9 and earlier).
    KnownName,
    /// A name that no topic has (v9 and earlier).
    UnknownName,
    /// The id of the topic that exists (v10).
    KnownId,
    /// A non-zero id that no topic has (v10).
    UnknownId,
    /// The zero id (v10).
    ZeroId,
}

/// One row of a table: the request version, the topic reference, and the
/// error code that Kafka puts on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    topic: TopicRef,
    error_code: i16,
}

/// The actual or the expected outcome of one [`Case`]: the response, and the
/// `(topic, partition)` keys that the group holds committed offsets for.
type Outcome = (i16, TopicRef, OffsetCommitResponse, Vec<(String, i32)>);

async fn create_known_topic(broker: &BrokerHandle) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("offset-commit-resolution-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: KNOWN_NAME.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(KNOWN_NAME, 0).await;
    let image = broker.controller_image_for_test();
    let topic = image.topic(KNOWN_NAME).expect("known topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// The committed `(topic, partition)` keys of `group`, sorted. A group that
/// has no actor holds none.
async fn committed_keys(broker: &BrokerHandle, group: &str) -> Vec<(String, i32)> {
    let shared = broker.broker_arc_for_test();
    let Some(actor) = shared.group_coordinator.find(group) else {
        return Vec::new();
    };
    let (reply, offsets) = oneshot::channel();
    actor
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .expect("send FetchOffsets");
    let mut keys: Vec<_> = offsets
        .await
        .expect("FetchOffsets reply")
        .committed
        .into_keys()
        .collect();
    keys.sort();
    keys
}

/// Send one single-row `OffsetCommit` at `case.version` for `case.topic`, in
/// a group of its own. Return the actual and the expected outcome.
///
/// The expected topic row carries the name and the id as the wire carries
/// them at `case.version`: the name before v10, the id at v10. Only a row that
/// answers `NONE` leaves a committed offset, under the topic name.
async fn drive(
    broker: &BrokerHandle,
    known_id: WireUuid,
    row: usize,
    case: Case,
) -> (Outcome, Outcome) {
    let (name, topic_id) = match case.topic {
        TopicRef::KnownName => (KNOWN_NAME, WireUuid::ZERO),
        TopicRef::UnknownName => (UNKNOWN_NAME, WireUuid::ZERO),
        TopicRef::KnownId => ("", known_id),
        TopicRef::UnknownId => ("", UNKNOWN_ID),
        TopicRef::ZeroId => ("", WireUuid::ZERO),
    };
    let group = format!("resolution-{row}");
    // An empty member id and generation -1 commit as a simple consumer, so
    // the membership check passes.
    let request = OffsetCommitRequest {
        group_id: group.clone(),
        topics: vec![OffsetCommitRequestTopic {
            name: name.to_string(),
            topic_id,
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let shared = broker.broker_arc_for_test();
    let user = principal("consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "consumer-client");
    let request_bytes = encode_request(&request, case.version);
    let response_bytes = handle(&shared, case.version, 7, &request_bytes, &ctx)
        .await
        .expect("handle offset commit");
    let actual: OffsetCommitResponse = decode_response(&response_bytes, case.version);
    let actual_keys = committed_keys(broker, &group).await;

    let id_only = case.version >= super::FIRST_TOPIC_ID_VERSION;
    let expected = OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: if id_only {
                String::new()
            } else {
                name.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partitions: vec![OffsetCommitResponsePartition {
                partition_index: 0,
                error_code: case.error_code,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected_keys = if case.error_code == codes::NONE {
        let committed_name = if case.topic == TopicRef::KnownId {
            KNOWN_NAME
        } else {
            name
        };
        vec![(committed_name.to_string(), 0)]
    } else {
        Vec::new()
    };
    (
        (case.version, case.topic, actual, actual_keys),
        (case.version, case.topic, expected, expected_keys),
    )
}

async fn run_table(authorizer: Arc<dyn Authorizer>, cases: &[Case]) {
    let (broker, _dir) = start_broker_with_authorizer_no_audit(authorizer).await;
    // A table that names no existing topic can run under an authorizer that
    // refuses `CreateTopics`.
    let needs_known = cases
        .iter()
        .any(|case| matches!(case.topic, TopicRef::KnownName | TopicRef::KnownId));
    let known_id = if needs_known {
        create_known_topic(&broker).await
    } else {
        WireUuid::ZERO
    };
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (row, case) in cases.iter().enumerate() {
        let (got, want) = drive(&broker, known_id, row, *case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    run_table(
        Arc::new(AllowAllAuthorizer),
        &[
            Case {
                version: 9,
                topic: TopicRef::KnownName,
                error_code: codes::NONE,
            },
            Case {
                version: 10,
                topic: TopicRef::KnownId,
                error_code: codes::NONE,
            },
            Case {
                version: 10,
                topic: TopicRef::UnknownId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
            Case {
                version: 10,
                topic: TopicRef::ZeroId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
        ],
    )
    .await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no topic `Read` grant sees 100 for an id that does not
/// resolve, and 29 for a name.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    run_table(
        Arc::new(DenyTopics),
        &[
            Case {
                version: 9,
                topic: TopicRef::UnknownName,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            },
            Case {
                version: 10,
                topic: TopicRef::KnownId,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            },
            Case {
                version: 10,
                topic: TopicRef::UnknownId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
            Case {
                version: 10,
                topic: TopicRef::ZeroId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
        ],
    )
    .await;
}

/// Kafka authorizes the group before it resolves any topic id, so a group
/// denial answers `GROUP_AUTHORIZATION_FAILED` on every row.
#[tokio::test]
async fn group_authorization_answers_before_topic_resolution() {
    run_table(
        Arc::new(DenyAll),
        &[
            Case {
                version: 10,
                topic: TopicRef::UnknownId,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
            },
            Case {
                version: 10,
                topic: TopicRef::ZeroId,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
            },
        ],
    )
    .await;
}

/// One v10 request that mixes a known id with the zero id and an unknown id.
/// The refused rows come first, and the committed row follows, as Kafka's
/// `OffsetCommitResponse.Builder.merge` orders them. Only the known topic
/// commits.
#[tokio::test]
async fn refused_rows_precede_the_committed_row() {
    const VERSION: i16 = 10;
    const GROUP: &str = "resolution-mixed";
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let known_id = create_known_topic(&broker).await;
    let row = |topic_id| OffsetCommitRequestTopic {
        topic_id,
        partitions: vec![OffsetCommitRequestPartition {
            partition_index: 0,
            committed_offset: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    let request = OffsetCommitRequest {
        group_id: GROUP.to_string(),
        topics: vec![row(known_id), row(WireUuid::ZERO), row(UNKNOWN_ID)],
        ..Default::default()
    };

    let shared = broker.broker_arc_for_test();
    let user = principal("consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "consumer-client");
    let request_bytes = encode_request(&request, VERSION);
    let response_bytes = handle(&shared, VERSION, 7, &request_bytes, &ctx)
        .await
        .expect("handle offset commit");
    let actual: OffsetCommitResponse = decode_response(&response_bytes, VERSION);

    let answer = |topic_id, error_code| OffsetCommitResponseTopic {
        topic_id,
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected = OffsetCommitResponse {
        topics: vec![
            answer(WireUuid::ZERO, codes::UNKNOWN_TOPIC_ID),
            answer(UNKNOWN_ID, codes::UNKNOWN_TOPIC_ID),
            answer(known_id, codes::NONE),
        ],
        ..Default::default()
    };
    assert!(
        (actual, committed_keys(&broker, GROUP).await)
            == (expected, vec![(KNOWN_NAME.to_string(), 0)])
    );
    broker.shutdown().await;
}
