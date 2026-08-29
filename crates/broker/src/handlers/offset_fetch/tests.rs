//! End-to-end test of the `OffsetFetch` handler against a running broker,
//! driven over the wire encoding on the legacy single-group path.

use std::sync::Arc;

use assert2::assert;
use krabka_log::Offset;
use krabka_protocol::owned::offset_fetch_response::OffsetFetchResponse;
use tokio::sync::oneshot;

use super::*;
use crate::{
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
