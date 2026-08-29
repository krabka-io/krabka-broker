//! The `ShareFetch` and `ShareAcknowledge` calls every consume test in this
//! binary drives, in one place. It builds a single-partition fetch request at
//! a given share-session epoch, sends a per-offset acknowledgement, sends a
//! renew-ack that extends a lock without changing record state, and retries
//! the first fetch until the acquire pass returns records.

use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    share_acknowledge_request::{
        AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
        ShareAcknowledgeRequest,
    },
    share_acknowledge_response::ShareAcknowledgeResponse,
    share_fetch_request::{
        AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
    },
    share_fetch_response::ShareFetchResponse,
};

use crate::{NONE, ONE_MB, harness::wire};

/// Build a `ShareFetchRequest` for a single `(topic_id, partition)` at the given
/// share-session epoch. The request can also carry acknowledgement batches.
pub fn share_fetch_req(
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
    acks: Vec<FetchAckBatch>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: ONE_MB,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition,
                partition_max_bytes: ONE_MB,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}

/// `ShareFetch`. This helper retries while the share-state leadership and
/// acquisition are still settling. The first acquire pass after topic creation
/// can briefly find the `__share_group_state` partition still materializing, so
/// this helper mirrors the retry-on-not-ready loop in `share_state.rs`. Returns
/// the (single) partition row.
pub async fn share_fetch(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let req = share_fetch_req(group, member, tid, partition, epoch, max_wait_ms, vec![]);
    let resp: ShareFetchResponse = client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == NONE,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// A `ShareAcknowledge` carrying one batch of per-offset ack types over
/// `[first, last]`. Returns the partition row.
pub async fn share_ack(
    client: &Client,
    member: &str,
    tid: uuid::Uuid,
    epoch: i32,
    first: i64,
    last: i64,
    ack_type: i8,
) -> krabka_protocol::owned::share_acknowledge_response::PartitionData {
    let count = usize::try_from(last - first + 1).unwrap();
    let req = ShareAcknowledgeRequest {
        group_id: Some("g1".into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        is_renew_ack: false,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(tid),
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: first,
                    last_offset: last,
                    acknowledge_types: vec![ack_type; count],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// A renew-ack `ShareAcknowledge` (`is_renew_ack = true`) over `[first, last]`
/// with *empty* ack types. The broker renew path extends each batch's lock
/// without changing record state. Returns the partition row.
pub async fn share_renew(
    client: &Client,
    member: &str,
    tid: uuid::Uuid,
    epoch: i32,
    first: i64,
    last: i64,
) -> krabka_protocol::owned::share_acknowledge_response::PartitionData {
    let req = ShareAcknowledgeRequest {
        group_id: Some("g1".into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        is_renew_ack: true,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(tid),
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: first,
                    last_offset: last,
                    acknowledge_types: vec![],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge renew");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge(renew) top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// Total number of offsets covered by the acquired ranges on a fetch row.
pub fn acquired_count(p: &krabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
}

/// Do the very first `ShareFetch` for a freshly-created topic. This helper
/// retries until the acquire pass actually returns records. Leadership and
/// materialization of both the data partition and `__share_group_state` may
/// still be settling. Asserts the supplied invariant on the resulting row.
pub async fn fetch_until_acquired(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = share_fetch(client, group, member, tid, partition, epoch, 0).await;
        if row.error_code == NONE && acquired_count(&row) > 0 {
            return row;
        }
        // intentional: bounded RPC poll — the acquire happens only via this
        // ShareFetch as share-state leadership/acquisition settles; no
        // metadata-image or metric signal reflects "the next fetch will acquire".
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records for {group}:{tid}:{partition}");
}
