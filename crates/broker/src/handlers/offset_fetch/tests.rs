//! End-to-end tests of the `OffsetFetch` handler against a running broker,
//! driven over the wire encoding.
//!
//! Both request shapes are covered, because KIP-447's `require_stable` is one
//! top-level field that has to reach two different response shapes: the
//! pre-KIP-516 `topics[]` of v0–v7 and the `groups[]` of v8 and above.

use std::sync::Arc;

use assert2::assert;
use krabka_log::Offset;
use krabka_protocol::owned::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
    OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
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
                commit_timestamp_ms: 0,
                expire_timestamp_ms: None,
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
                commit_timestamp_ms: 0,
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
                commit_timestamp_ms: 0,
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
