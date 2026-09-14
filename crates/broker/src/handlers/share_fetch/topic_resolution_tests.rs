//! Handler tests for how `ShareFetch` answers a partition row by the kind of
//! topic id that the row carries.
//!
//! Kafka's `ErroneousAndValidPartitionData` answers `UNKNOWN_TOPIC_ID` for
//! every partition of the share session whose topic id does not resolve, the
//! zero id included. It does this before the topic `Read` authorization. When
//! the request carries acknowledgements,
//! `KafkaApis.getAcknowledgeBatchesFromShareFetchRequest` also answers
//! `UNKNOWN_TOPIC_ID` as the acknowledge error of those partitions.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        share_fetch_request::{
            AcknowledgementBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::{PartitionData, ShareFetchResponse, ShareFetchableTopicResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use super::handle;
use crate::{
    authorizer::{
        AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    },
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// The acquisition lock timeout of the test broker's share-group config.
const LOCK_TIMEOUT_MS: i32 = 30_000;

/// Denies `Read` on every topic and allows everything else, so that topic
/// creation still works and only the per-topic gate refuses.
#[derive(Debug)]
struct DenyTopicRead;

impl Authorizer for DenyTopicRead {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if request.resource_type == ResourceType::Topic && request.operation == AclOperation::Read {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }
}

/// The topic id that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicRef {
    /// The id of a topic that exists.
    Known,
    /// A non-zero id that no topic has.
    Unknown,
    /// The zero id.
    Zero,
}

/// One row of a table: the request version, the topic id, and the error code
/// that Kafka puts on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    topic: TopicRef,
    error_code: i16,
}

/// The actual or the expected outcome of one [`Case`].
type Outcome = (i16, TopicRef, ShareFetchResponse);

async fn start(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        cfg.share_group.enable = true;
    })
    .await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-fetch-resolution-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
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
    broker.wait_until_partition_present(name, 0).await;
    let image = broker.controller_image_for_test();
    let topic = image.topic(name).expect("created topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// A request for partition 0 of `topic_id`, in the share session of `member`.
/// With `acknowledge`, the row carries one acknowledgement batch.
fn request(member: &str, epoch: i32, topic_id: WireUuid, acknowledge: bool) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some("resolution-group".into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1_048_576,
        max_records: 10,
        batch_size: 10,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
                partition_max_bytes: 1_048_576,
                acknowledgement_batches: if acknowledge {
                    vec![AcknowledgementBatch {
                        first_offset: 0,
                        last_offset: 0,
                        acknowledge_types: vec![1],
                        ..Default::default()
                    }]
                } else {
                    Vec::new()
                },
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn share_fetch(
    broker: &BrokerHandle,
    version: i16,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let request_bytes = encode_request(request, version);
    let response = handle(&shared, version, 7, &request_bytes, &ctx)
        .await
        .expect("handle share fetch");
    decode_response(&response, version)
}

/// The empty record set, as a client decodes it. Kafka's
/// `ShareFetchResponse.partitionResponse` sets `MemoryRecords.EMPTY` on every
/// row.
fn no_records() -> RecordsPayload {
    RecordsPayload::Legacy(Bytes::new())
}

/// A response with one topic row for partition 0.
fn one_row(topic_id: WireUuid, partition: PartitionData) -> ShareFetchResponse {
    ShareFetchResponse {
        acquisition_lock_timeout_ms: LOCK_TIMEOUT_MS,
        responses: vec![ShareFetchableTopicResponse {
            topic_id,
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Run every case in its own share session on `broker`. `known` is the id of
/// a topic that exists.
async fn drive(
    broker: &BrokerHandle,
    known: WireUuid,
    cases: &[Case],
) -> (Vec<Outcome>, Vec<Outcome>) {
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (row, case) in cases.iter().enumerate() {
        let topic_id = match case.topic {
            TopicRef::Known => known,
            TopicRef::Unknown => UNKNOWN_ID,
            TopicRef::Zero => WireUuid::ZERO,
        };
        let member = format!("member-{row}");
        let response =
            share_fetch(broker, case.version, &request(&member, 0, topic_id, false)).await;
        actual.push((case.version, case.topic, response));
        let partition = PartitionData {
            partition_index: 0,
            error_code: case.error_code,
            records: Some(no_records()),
            ..Default::default()
        };
        expected.push((case.version, case.topic, one_row(topic_id, partition)));
    }
    (actual, expected)
}

/// The cases for every supported version: `known` is the error code for a
/// topic that exists.
fn cases(known: i16) -> Vec<Case> {
    let mut cases = Vec::new();
    for version in [1, krabka_protocol::owned::share_fetch_request::MAX_VERSION] {
        cases.extend([
            Case {
                version,
                topic: TopicRef::Known,
                error_code: known,
            },
            Case {
                version,
                topic: TopicRef::Unknown,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
            Case {
                version,
                topic: TopicRef::Zero,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
        ]);
    }
    cases
}

#[tokio::test]
async fn partition_row_error_follows_topic_id() {
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let known = create_topic(&broker, "share-resolution").await;

    let (actual, expected) = drive(&broker, known, &cases(codes::NONE)).await;

    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no topic `Read` grant sees 29 for a topic that exists and
/// 100 for an id that does not resolve.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    let (broker, _dir) = start(Arc::new(DenyTopicRead)).await;
    let known = create_topic(&broker, "share-resolution").await;

    let (actual, expected) = drive(&broker, known, &cases(codes::TOPIC_AUTHORIZATION_FAILED)).await;

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The share session keeps a partition whose id does not resolve. A later
/// fetch that acknowledges records on it gets 100 as the fetch error and as
/// the acknowledge error.
#[tokio::test]
async fn a_piggybacked_acknowledgement_on_an_unresolved_id_answers_unknown_topic_id() {
    let version = krabka_protocol::owned::share_fetch_request::MAX_VERSION;
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;

    let mut actual = Vec::new();
    for topic_id in [UNKNOWN_ID, WireUuid::ZERO] {
        let member = format!("member-{}", topic_id.0[0]);
        let opened = share_fetch(&broker, version, &request(&member, 0, topic_id, false)).await;
        let acknowledged =
            share_fetch(&broker, version, &request(&member, 1, topic_id, true)).await;
        actual.push((topic_id, opened, acknowledged));
    }

    let expected: Vec<_> = [UNKNOWN_ID, WireUuid::ZERO]
        .into_iter()
        .map(|topic_id| {
            (
                topic_id,
                one_row(
                    topic_id,
                    PartitionData {
                        partition_index: 0,
                        records: Some(no_records()),
                        error_code: codes::UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    },
                ),
                one_row(
                    topic_id,
                    PartitionData {
                        partition_index: 0,
                        records: Some(no_records()),
                        error_code: codes::UNKNOWN_TOPIC_ID,
                        acknowledge_error_code: codes::UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    },
                ),
            )
        })
        .collect();
    assert!(actual == expected);
    broker.shutdown().await;
}
