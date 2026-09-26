//! End-to-end tests of the `OffsetFetch` handler against a running broker,
//! driven over the wire encoding.
//!
//! Both request shapes are covered, because KIP-447's `require_stable` is one
//! top-level field that has to reach two different response shapes: the
//! pre-KIP-516 `topics[]` of v0–v7 and the `groups[]` of v8 and above.

use std::sync::Arc;

use assert2::assert;
use krabka_log::Offset;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
            OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::oneshot;

use super::*;
use crate::{
    codes,
    coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag},
        classic_state::OffsetEntry,
    },
    test_support::{peer, principal, start_broker_with_authorizer_no_audit as start_broker},
};

// Seed a committed offset for (group, topic, partition) directly on the
// group actor via UpdateCommitted.
//
// The commit is stamped now, not at the epoch. These groups are memberless
// and carry no protocol type, so KIP-211 retention measures each offset from
// its own commit; a 1970 stamp is already past `offsets.retention.minutes`
// and the sweep that runs at broker start would tombstone the offsets — and
// the group with them — out from under a test that is asking about
// `require_stable`.
async fn seed_committed_offset(
    broker: &Broker,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::UpdateCommitted {
        entries: vec![(
            (topic.to_string(), partition),
            OffsetEntry {
                offset: Offset(offset),
                leader_epoch: 5,
                metadata: String::new(),
                commit_timestamp_ms: crate::time_util::now_ms(),
                expire_timestamp_ms: None,
                topic_id: None,
            },
        )],
        reply: tx,
    })
    .await
    .expect("send UpdateCommitted");
    rx.await.expect("UpdateCommitted ack");
}

// A named-topic OffsetFetch (v0–v7 path) returns the group's committed
// offset for the requested partition. A non-zero committed offset pins
// the committed_offset field against the struct-field-deletion mutant,
// which would default it to 0.
#[tokio::test]
async fn named_topic_fetch_returns_committed_offset() {
    const VERSION: i16 = 7; // legacy single-group path (< 8)
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", "orders", 0, 42).await;

    let p = principal("admin");
    let peer = peer();
    let ctx = crate::test_support::request_context(&p, &peer, "consumer");
    let req = OffsetFetchRequest {
        group_id: "grp".into(),
        topics: Some(vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                name: "orders".into(),
                partition_indexes: vec![0],
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    let req_bytes = crate::test_support::encode_request(&req, VERSION);

    let bytes = handle(&broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    let resp: OffsetFetchResponse = crate::test_support::decode_response(&bytes, VERSION);

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name == "orders")
        .expect("orders topic row");
    let part = topic
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .expect("partition 0 row");
    assert!(
        part.committed_offset == 42,
        "committed_offset must echo the seeded value (42), got {}",
        part.committed_offset
    );
    broker_handle.shutdown().await;
}

// The offsets-log positions the two halves of a transaction occupy in these
// tests: its offset-commit records, and the marker that lands above them.
const TXN_RECORDS_AT: i64 = 10;
const TXN_MARKER_AT: i64 = 11;

// Mark (topic, partition) keys as written by an unresolved transaction, the
// way `TxnOffsetCommit` does once its records are durable.
async fn seed_pending_txn_offsets(
    broker: &Broker,
    group: &str,
    producer_id: i64,
    written_at: i64,
    keys: Vec<(String, i32)>,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::AddPendingTxnOffsets {
        producer_id,
        written_at,
        keys,
        reply: tx,
    })
    .await
    .expect("send AddPendingTxnOffsets");
    rx.await.expect("AddPendingTxnOffsets ack");
}

// Resolve the transaction the way its marker does: publish the offsets a
// commit carries (an abort carries none) and drop the producer's pending
// marks, stamped with the marker's own position in the offsets log.
async fn resolve_pending_txn_offsets(
    broker: &Broker,
    group: &str,
    producer_id: i64,
    resolved_through: i64,
    committed: Vec<((String, i32), OffsetEntry)>,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::ResolveTxnOffsets {
        producer_id,
        resolved_through,
        committed,
        reply: tx,
    })
    .await
    .expect("send ResolveTxnOffsets");
    rx.await.expect("ResolveTxnOffsets ack");
}

async fn fetch(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
) -> krabka_protocol::owned::offset_fetch_response::OffsetFetchResponse {
    let p = principal("admin");
    let peer = peer();
    let ctx = crate::test_support::request_context(&p, &peer, "consumer");
    let req_bytes = crate::test_support::encode_request(req, version);
    let bytes = handle(broker, version, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    crate::test_support::decode_response(&bytes, version)
}

// KIP-447 on the pre-KIP-516 shape. `orders-0` carries a stable offset that an
// open transaction is about to replace; `orders-1` is stable and untouched.
//
// `require_stable = false` keeps the pre-KIP-447 answer, so the consumer sees
// the offset the transaction is replacing. `require_stable = true` turns
// `orders-0` into the UNSTABLE_OFFSET_COMMIT row Kafka sends — the invalid
// offset sentinels with an empty, not null, metadata string — while
// `orders-1` still answers normally. Once the transaction's marker resolves,
// the same request reads the new offset.
#[tokio::test]
async fn require_stable_reports_unstable_offsets_on_the_legacy_shape() {
    const VERSION: i16 = 7; // lowest version carrying require_stable
    const PRODUCER_ID: i64 = 91;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", "orders", 0, 42).await;
    seed_committed_offset(&broker, "grp", "orders", 1, 11).await;
    seed_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_RECORDS_AT,
        vec![("orders".to_string(), 0)],
    )
    .await;

    let request = |require_stable| OffsetFetchRequest {
        group_id: "grp".into(),
        topics: Some(vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                name: "orders".into(),
                partition_indexes: vec![0, 1],
                ..Default::default()
            },
        ]),
        require_stable,
        ..Default::default()
    };
    let stable_row = |partition_index, committed_offset| OffsetFetchResponsePartition {
        partition_index,
        committed_offset,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    };
    let expect = |partitions| OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: vec![OffsetFetchResponseTopic {
            name: "orders".into(),
            partitions,
            ..Default::default()
        }],
        error_code: codes::NONE,
        groups: Vec::new(),
        ..Default::default()
    };

    let relaxed = fetch(&broker, VERSION, &request(false)).await;
    assert!(relaxed == expect(vec![stable_row(0, 42), stable_row(1, 11)]));

    let strict = fetch(&broker, VERSION, &request(true)).await;
    assert!(
        strict
            == expect(vec![
                OffsetFetchResponsePartition {
                    partition_index: 0,
                    committed_offset: -1,
                    committed_leader_epoch: -1,
                    metadata: Some(String::new()),
                    error_code: codes::UNSTABLE_OFFSET_COMMIT,
                    ..Default::default()
                },
                stable_row(1, 11),
            ])
    );

    resolve_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_MARKER_AT,
        vec![(
            ("orders".to_string(), 0),
            OffsetEntry {
                offset: Offset(77),
                leader_epoch: 5,
                metadata: String::new(),
                commit_timestamp_ms: crate::time_util::now_ms(),
                expire_timestamp_ms: None,
                topic_id: None,
            },
        )],
    )
    .await;

    let resolved = fetch(&broker, VERSION, &request(true)).await;
    assert!(resolved == expect(vec![stable_row(0, 77), stable_row(1, 11)]));
    broker_handle.shutdown().await;
}

// The same three phases on the KIP-516 `groups[]` shape. `require_stable` is a
// top-level request field there too, so it governs every group the request
// names.
#[tokio::test]
async fn require_stable_reports_unstable_offsets_on_the_groups_shape() {
    const VERSION: i16 = 9; // groups[] shape, still keyed by topic name
    const PRODUCER_ID: i64 = 91;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", "orders", 0, 42).await;
    seed_committed_offset(&broker, "grp", "orders", 1, 11).await;
    seed_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_RECORDS_AT,
        vec![("orders".to_string(), 0)],
    )
    .await;

    let request = |require_stable| OffsetFetchRequest {
        groups: vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestGroup {
                group_id: "grp".into(),
                topics: Some(vec![
                    krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopics {
                        name: "orders".into(),
                        partition_indexes: vec![0, 1],
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
        ],
        require_stable,
        ..Default::default()
    };
    let stable_row = |partition_index, committed_offset| OffsetFetchResponsePartitions {
        partition_index,
        committed_offset,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    };
    let expect = |partitions| OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: codes::NONE,
        groups: vec![OffsetFetchResponseGroup {
            group_id: "grp".into(),
            topics: vec![OffsetFetchResponseTopics {
                name: "orders".into(),
                topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
                partitions,
                ..Default::default()
            }],
            error_code: codes::NONE,
            ..Default::default()
        }],
        ..Default::default()
    };

    let relaxed = fetch(&broker, VERSION, &request(false)).await;
    assert!(relaxed == expect(vec![stable_row(0, 42), stable_row(1, 11)]));

    let strict = fetch(&broker, VERSION, &request(true)).await;
    assert!(
        strict
            == expect(vec![
                OffsetFetchResponsePartitions {
                    partition_index: 0,
                    committed_offset: -1,
                    committed_leader_epoch: -1,
                    metadata: Some(String::new()),
                    error_code: codes::UNSTABLE_OFFSET_COMMIT,
                    ..Default::default()
                },
                stable_row(1, 11),
            ])
    );

    resolve_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_MARKER_AT,
        vec![(
            ("orders".to_string(), 0),
            OffsetEntry {
                offset: Offset(77),
                leader_epoch: 5,
                metadata: String::new(),
                commit_timestamp_ms: crate::time_util::now_ms(),
                expire_timestamp_ms: None,
                topic_id: None,
            },
        )],
    )
    .await;

    let resolved = fetch(&broker, VERSION, &request(true)).await;
    assert!(resolved == expect(vec![stable_row(0, 77), stable_row(1, 11)]));
    broker_handle.shutdown().await;
}

// A `TxnOffsetCommit` records its KIP-447 mark only after its records are
// durable, so the transaction's own marker can be resolved on the group actor
// in the window in between — the idle-transaction reaper aborts without any
// client involved at all. The late mark then describes a transaction that is
// already over, and taking it would leave `orders-0` answering
// UNSTABLE_OFFSET_COMMIT with nothing left to clear it: an EOS consumer would
// retry that fetch for ever.
//
// The offsets log settles which came first. A mark for records below an
// applied marker is dropped; a mark for records above it is a new transaction
// and is honoured.
#[tokio::test]
async fn a_mark_for_records_below_an_applied_marker_does_not_strand_the_partition() {
    const VERSION: i16 = 7;
    const PRODUCER_ID: i64 = 91;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", "orders", 0, 42).await;

    let request = OffsetFetchRequest {
        group_id: "grp".into(),
        topics: Some(vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                name: "orders".into(),
                partition_indexes: vec![0],
                ..Default::default()
            },
        ]),
        require_stable: true,
        ..Default::default()
    };
    let expect = |partition| OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: vec![OffsetFetchResponseTopic {
            name: "orders".into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        error_code: codes::NONE,
        groups: Vec::new(),
        ..Default::default()
    };

    // The abort marker for the records at TXN_RECORDS_AT lands first and
    // publishes nothing; the commit's own mark arrives after it.
    resolve_pending_txn_offsets(&broker, "grp", PRODUCER_ID, TXN_MARKER_AT, Vec::new()).await;
    seed_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_RECORDS_AT,
        vec![("orders".to_string(), 0)],
    )
    .await;

    let after_late_mark = fetch(&broker, VERSION, &request).await;
    assert!(
        after_late_mark
            == expect(OffsetFetchResponsePartition {
                partition_index: 0,
                committed_offset: 42,
                committed_leader_epoch: 5,
                metadata: Some(String::new()),
                error_code: codes::NONE,
                ..Default::default()
            })
    );

    // The producer's next transaction writes above that marker, and is
    // reported unstable as usual.
    seed_pending_txn_offsets(
        &broker,
        "grp",
        PRODUCER_ID,
        TXN_MARKER_AT + 1,
        vec![("orders".to_string(), 0)],
    )
    .await;
    let next_transaction = fetch(&broker, VERSION, &request).await;
    assert!(
        next_transaction
            == expect(OffsetFetchResponsePartition {
                partition_index: 0,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: Some(String::new()),
                error_code: codes::UNSTABLE_OFFSET_COMMIT,
                ..Default::default()
            })
    );
    broker_handle.shutdown().await;
}

/// The name of the topic that exists in the topic-reference tables.
const KNOWN_NAME: &str = "orders";

/// A name that no topic in the topic-reference tables has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// An id that no topic in the topic-reference tables has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// Allows every group operation and denies every topic operation.
#[derive(Debug)]
struct DenyTopics;

impl crate::authorizer::Authorizer for DenyTopics {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        req: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        if req.resource_type == krabka_metadata::ResourceType::Topic {
            crate::authorizer::AuthorizationResult::Deny
        } else {
            crate::authorizer::AuthorizationResult::Allow
        }
    }
}

/// The topic reference that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicRef {
    /// The name of the topic that exists (v8 and v9).
    KnownName,
    /// A name that no topic has (v8 and v9).
    UnknownName,
    /// The id of the topic that exists (v10).
    KnownId,
    /// A non-zero id that no topic has (v10).
    UnknownId,
    /// The zero id (v10).
    ZeroId,
}

/// Create the known topic and return its id. Seed a committed offset for
/// `(KNOWN_NAME, 0)`, and one for the empty topic name, which a zero-id row
/// must not read.
async fn seed_topic_reference_group(broker_handle: &crate::broker::BrokerHandle) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker_handle.listen_addr().to_string())
        .client_id("offset-fetch-resolution-test")
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
    broker_handle
        .wait_until_partition_present(KNOWN_NAME, 0)
        .await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", KNOWN_NAME, 0, 42).await;
    seed_committed_offset(&broker, "grp", "", 0, 7).await;
    let image = broker_handle.controller_image_for_test();
    let topic = image.topic(KNOWN_NAME).expect("known topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// The request row for `topic`, and the topic row that the response carries
/// for it at `version` with `partition` as its only partition row.
fn topic_reference_rows(
    version: i16,
    topic: TopicRef,
    known_id: WireUuid,
    partition: OffsetFetchResponsePartitions,
) -> (OffsetFetchRequestTopics, OffsetFetchResponseTopics) {
    let (name, topic_id) = match topic {
        TopicRef::KnownName => (KNOWN_NAME, WireUuid::ZERO),
        TopicRef::UnknownName => (UNKNOWN_NAME, WireUuid::ZERO),
        TopicRef::KnownId => ("", known_id),
        TopicRef::UnknownId => ("", UNKNOWN_ID),
        TopicRef::ZeroId => ("", WireUuid::ZERO),
    };
    let request = OffsetFetchRequestTopics {
        name: name.to_string(),
        topic_id,
        partition_indexes: vec![0],
        ..Default::default()
    };
    // The wire carries the name at v8 and v9, and the id at v10.
    let id_only = version >= 10;
    let response = OffsetFetchResponseTopics {
        name: if id_only {
            String::new()
        } else {
            name.to_string()
        },
        topic_id: if id_only { topic_id } else { WireUuid::ZERO },
        partitions: vec![partition],
        ..Default::default()
    };
    (request, response)
}

fn groups_request(topics: Vec<OffsetFetchRequestTopics>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: "grp".into(),
            topics: Some(topics),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn groups_response(topics: Vec<OffsetFetchResponseTopics>) -> OffsetFetchResponse {
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: codes::NONE,
        groups: vec![OffsetFetchResponseGroup {
            group_id: "grp".into(),
            topics,
            error_code: codes::NONE,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The partition row of a partition with no committed offset, or of a
/// refused topic.
fn no_offset_row(error_code: i16) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code,
        ..Default::default()
    }
}

/// The partition row of the seeded offset of the known topic.
fn seeded_row() -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: 42,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// Send one single-topic request per case, and compare every whole response
/// with the one that the case expects.
async fn run_topic_reference_table(
    authorizer: Arc<dyn crate::authorizer::Authorizer>,
    cases: Vec<(i16, TopicRef, OffsetFetchResponsePartitions)>,
) {
    let (broker_handle, _dir) = start_broker(authorizer).await;
    let known_id = seed_topic_reference_group(&broker_handle).await;
    let broker = broker_handle.broker_arc_for_test();
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (version, topic, partition) in cases {
        let (request_row, response_row) = topic_reference_rows(version, topic, known_id, partition);
        let response = fetch(&broker, version, &groups_request(vec![request_row])).await;
        actual.push((version, topic, response));
        expected.push((version, topic, groups_response(vec![response_row])));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.fetchOffsetsForGroup` answers `UNKNOWN_TOPIC_ID` at v10
/// for every row whose name is empty after id resolution. The zero id is such
/// a row, and it must not read the offset stored under the empty name.
#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    run_topic_reference_table(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        vec![
            (9, TopicRef::KnownName, seeded_row()),
            (9, TopicRef::UnknownName, no_offset_row(codes::NONE)),
            (10, TopicRef::KnownId, seeded_row()),
            (
                10,
                TopicRef::UnknownId,
                no_offset_row(codes::UNKNOWN_TOPIC_ID),
            ),
            (10, TopicRef::ZeroId, no_offset_row(codes::UNKNOWN_TOPIC_ID)),
        ],
    )
    .await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    run_topic_reference_table(
        Arc::new(DenyTopics),
        vec![
            (
                9,
                TopicRef::KnownName,
                no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                10,
                TopicRef::KnownId,
                no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                10,
                TopicRef::UnknownId,
                no_offset_row(codes::UNKNOWN_TOPIC_ID),
            ),
            (10, TopicRef::ZeroId, no_offset_row(codes::UNKNOWN_TOPIC_ID)),
        ],
    )
    .await;
}

/// Kafka appends the refused topics after the topics that the coordinator
/// answers, so a zero-id row that comes first in the request comes last in
/// the response.
#[tokio::test]
async fn refused_topics_follow_the_answered_topics() {
    const VERSION: i16 = 10;
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let known_id = seed_topic_reference_group(&broker_handle).await;
    let broker = broker_handle.broker_arc_for_test();
    let (zero_request, zero_response) = topic_reference_rows(
        VERSION,
        TopicRef::ZeroId,
        known_id,
        no_offset_row(codes::UNKNOWN_TOPIC_ID),
    );
    let (known_request, known_response) =
        topic_reference_rows(VERSION, TopicRef::KnownId, known_id, seeded_row());

    let actual = fetch(
        &broker,
        VERSION,
        &groups_request(vec![zero_request, known_request]),
    )
    .await;

    assert!(actual == groups_response(vec![known_response, zero_response]));
    broker_handle.shutdown().await;
}

/// Allows every group operation, denies `Read` on every topic, and allows
/// `Describe` on the known topic only.
#[derive(Debug)]
struct DescribeKnownTopic;

impl crate::authorizer::Authorizer for DescribeKnownTopic {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        req: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        let allowed = req.resource_type != krabka_metadata::ResourceType::Topic
            || (req.operation == krabka_metadata::AclOperation::Describe
                && req.resource_name == KNOWN_NAME);
        if allowed {
            crate::authorizer::AuthorizationResult::Allow
        } else {
            crate::authorizer::AuthorizationResult::Deny
        }
    }
}

/// The legacy (v0 to v7) partition row of the seeded offset of the known
/// topic.
fn legacy_seeded_row() -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index: 0,
        committed_offset: 42,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// The legacy (v0 to v7) partition row of a refused topic.
fn legacy_refused_row() -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index: 0,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        ..Default::default()
    }
}

/// A legacy (v0 to v7) response that carries one partition row per topic.
fn legacy_response(topics: Vec<(&str, OffsetFetchResponsePartition)>) -> OffsetFetchResponse {
    OffsetFetchResponse {
        topics: topics
            .into_iter()
            .map(|(name, partition)| OffsetFetchResponseTopic {
                name: name.to_string(),
                partitions: vec![partition],
                ..Default::default()
            })
            .collect(),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// A legacy (v0 to v7) request for partition 0 of `topics`, or for every
/// topic when `None`.
fn legacy_request(topics: Option<&[&str]>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        group_id: "grp".into(),
        topics: topics.map(|names| {
            names
                .iter()
                .map(
                    |name| krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                        name: (*name).to_string(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    },
                )
                .collect()
        }),
        ..Default::default()
    }
}

/// A KIP-516 request for partition 0 of `topics` by name, or for every topic
/// when `None`.
fn named_groups_request(topics: Option<&[&str]>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: "grp".into(),
            topics: topics.map(|names| {
                names
                    .iter()
                    .map(|name| OffsetFetchRequestTopics {
                        name: (*name).to_string(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A KIP-516 (v8 and v9) topic row by name, with one partition row.
fn named_topic(name: &str, partition: OffsetFetchResponsePartitions) -> OffsetFetchResponseTopics {
    OffsetFetchResponseTopics {
        name: name.to_string(),
        partitions: vec![partition],
        ..Default::default()
    }
}

/// Sends each case's request and compares every whole response with the one
/// that the case expects.
async fn run_response_table(
    broker: &Broker,
    cases: Vec<(&str, i16, OffsetFetchRequest, OffsetFetchResponse)>,
) {
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (case, version, request, response) in cases {
        actual.push((case, fetch(broker, version, &request).await));
        expected.push((case, response));
    }
    assert!(actual == expected);
}

/// Kafka authorizes each topic with `Describe`, not `Read`, and appends the
/// refused topics after the answered ones on both shapes. A fetch-all leaves
/// out every topic that the principal may not describe instead of answering
/// it with `TOPIC_AUTHORIZATION_FAILED`.
#[tokio::test]
async fn topics_are_authorized_with_describe_and_fetch_all_hides_refused_topics() {
    let (broker_handle, _dir) = start_broker(Arc::new(DescribeKnownTopic)).await;
    seed_topic_reference_group(&broker_handle).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", UNKNOWN_NAME, 0, 9).await;
    let both: &[&str] = &[UNKNOWN_NAME, KNOWN_NAME];
    run_response_table(
        &broker,
        vec![
            (
                "v7 explicit list",
                7,
                legacy_request(Some(both)),
                legacy_response(vec![
                    (KNOWN_NAME, legacy_seeded_row()),
                    (UNKNOWN_NAME, legacy_refused_row()),
                ]),
            ),
            (
                "v7 fetch-all",
                7,
                legacy_request(None),
                legacy_response(vec![(KNOWN_NAME, legacy_seeded_row())]),
            ),
            (
                "v9 explicit list",
                9,
                named_groups_request(Some(both)),
                groups_response(vec![
                    named_topic(KNOWN_NAME, seeded_row()),
                    named_topic(
                        UNKNOWN_NAME,
                        no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
                    ),
                ]),
            ),
            (
                "v9 fetch-all",
                9,
                named_groups_request(None),
                groups_response(vec![named_topic(KNOWN_NAME, seeded_row())]),
            ),
        ],
    )
    .await;
    broker_handle.shutdown().await;
}

/// A fetch-all names each topic by id at v10, so Kafka leaves out a topic that
/// the metadata image does not hold. Before v10 the name carries the row. The
/// topics come in name order.
#[tokio::test]
async fn fetch_all_leaves_out_topics_without_an_id_at_v10() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let known_id = seed_topic_reference_group(&broker_handle).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset(&broker, "grp", UNKNOWN_NAME, 0, 9).await;
    let offset_row = |committed_offset| OffsetFetchResponsePartitions {
        committed_offset,
        ..seeded_row()
    };
    run_response_table(
        &broker,
        vec![
            (
                "v9 fetch-all",
                9,
                named_groups_request(None),
                groups_response(vec![
                    named_topic("", offset_row(7)),
                    named_topic(UNKNOWN_NAME, offset_row(9)),
                    named_topic(KNOWN_NAME, seeded_row()),
                ]),
            ),
            (
                "v10 fetch-all",
                10,
                named_groups_request(None),
                groups_response(vec![OffsetFetchResponseTopics {
                    topic_id: known_id,
                    partitions: vec![seeded_row()],
                    ..Default::default()
                }]),
            ),
        ],
    )
    .await;
    broker_handle.shutdown().await;
}

/// Joins `member_id` to the KIP-848 consumer group `group` and returns its
/// member epoch.
async fn join_consumer_group(broker: &Broker, group: &str, member_id: &str) -> i32 {
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    let handle = broker.group_coordinator.get_or_create_consumer(group);
    let (reply, joined) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["orders".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client".into(),
            client_host: "host".into(),
            reply,
            regex_authorized_topics: std::collections::HashSet::new(),
        })
        .await
        .expect("send Heartbeat");
    let joined = joined.await.expect("Heartbeat reply");
    assert!(joined.error_code == codes::NONE);
    joined.member_epoch
}

/// Kafka's `OffsetMetadataManager.fetchOffsets` reads offsets without creating
/// a group, and `ConsumerGroup.validateOffsetFetch` checks the v9 member id
/// and epoch of a consumer group. A refused fetch is the group row with the
/// error code and no topics (`OffsetFetchResponse.groupError`).
#[tokio::test]
async fn offset_fetch_creates_no_group_and_checks_the_member_epoch() {
    struct Row {
        name: &'static str,
        version: i16,
        group_id: &'static str,
        member_id: Option<&'static str>,
        /// The epoch relative to the member's epoch, or an absolute -1.
        member_epoch: EpochRef,
        expected: OffsetFetchResponseGroup,
        group_exists_after: bool,
    }
    #[derive(Clone, Copy)]
    enum EpochRef {
        Current,
        Older,
        NoEpoch,
    }

    let offsets_row = |partition: OffsetFetchResponsePartitions| {
        vec![OffsetFetchResponseTopics {
            name: "orders".into(),
            partitions: vec![partition],
            ..Default::default()
        }]
    };
    let group_row = |group_id: &str, topics, error_code| OffsetFetchResponseGroup {
        group_id: group_id.into(),
        topics,
        error_code,
        ..Default::default()
    };
    let rows = [
        Row {
            name: "unknown group at v8",
            version: 8,
            group_id: "typo",
            member_id: None,
            member_epoch: EpochRef::NoEpoch,
            expected: group_row("typo", offsets_row(no_offset_row(codes::NONE)), codes::NONE),
            group_exists_after: false,
        },
        Row {
            name: "unknown group at v9 with a member",
            version: 9,
            group_id: "typo",
            member_id: Some("m1"),
            member_epoch: EpochRef::Current,
            expected: group_row("typo", offsets_row(no_offset_row(codes::NONE)), codes::NONE),
            group_exists_after: false,
        },
        Row {
            name: "consumer member with its epoch",
            version: 9,
            group_id: "grp",
            member_id: Some("m1"),
            member_epoch: EpochRef::Current,
            expected: group_row("grp", offsets_row(seeded_row()), codes::NONE),
            group_exists_after: true,
        },
        Row {
            name: "consumer member with an older epoch",
            version: 9,
            group_id: "grp",
            member_id: Some("m1"),
            member_epoch: EpochRef::Older,
            expected: group_row("grp", vec![], codes::STALE_MEMBER_EPOCH),
            group_exists_after: true,
        },
        Row {
            name: "unknown member of a consumer group",
            version: 9,
            group_id: "grp",
            member_id: Some("ghost"),
            member_epoch: EpochRef::Current,
            expected: group_row("grp", vec![], codes::UNKNOWN_MEMBER_ID),
            group_exists_after: true,
        },
        Row {
            name: "no member id and epoch -1 (admin client)",
            version: 9,
            group_id: "grp",
            member_id: None,
            member_epoch: EpochRef::NoEpoch,
            expected: group_row("grp", offsets_row(seeded_row()), codes::NONE),
            group_exists_after: true,
        },
        Row {
            name: "no member id with an epoch",
            version: 9,
            group_id: "grp",
            member_id: None,
            member_epoch: EpochRef::Current,
            expected: group_row("grp", vec![], codes::UNKNOWN_MEMBER_ID),
            group_exists_after: true,
        },
    ];

    for row in rows {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let epoch = join_consumer_group(&broker, "grp", "m1").await;
        seed_committed_offset(&broker, "grp", "orders", 0, 42).await;

        let request = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: row.group_id.into(),
                member_id: row.member_id.map(str::to_string),
                member_epoch: match row.member_epoch {
                    EpochRef::Current => epoch,
                    EpochRef::Older => epoch - 1,
                    EpochRef::NoEpoch => -1,
                },
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "orders".into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = fetch(&broker, row.version, &request).await;

        let expected = OffsetFetchResponse {
            groups: vec![row.expected],
            ..Default::default()
        };
        assert2::check!(response == expected, "{}", row.name);
        assert2::check!(
            broker.group_coordinator.find(row.group_id).is_some() == row.group_exists_after,
            "{}",
            row.name
        );
        broker_handle.shutdown().await;
    }
}

/// The legacy single-group shape (v0-v7) creates no group for an unknown id.
#[tokio::test]
async fn legacy_offset_fetch_creates_no_group() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let request = OffsetFetchRequest {
        group_id: "typo".into(),
        topics: Some(vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                name: "orders".into(),
                partition_indexes: vec![0],
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    let response = fetch(&broker, 7, &request).await;
    assert!(response.topics[0].partitions[0].committed_offset == -1);
    assert!(broker.group_coordinator.find("typo").is_none());
    broker_handle.shutdown().await;
}
