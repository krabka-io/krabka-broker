//! The `ShareFetch` and `ShareAcknowledge` calls every consume test in this
//! binary drives, in one place. It builds a single-partition fetch request at
//! a given share-session epoch, sends a per-offset acknowledgement, sends a
//! renew-ack that extends a lock without changing record state, and retries
//! the first fetch until the acquire pass returns records.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    share_acknowledge_request::{
        AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
        ShareAcknowledgeRequest,
    },
    share_acknowledge_response::ShareAcknowledgeResponse,
};

pub use crate::support::share::{acquired_count, share_fetch_req};
use crate::{NONE, RENEW, harness::wire};

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
    crate::support::share::fetch_row(
        client,
        share_fetch_req(group, member, tid, partition, epoch, max_wait_ms, vec![]),
        true,
    )
    .await
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
    crate::support::share::share_ack(
        client,
        crate::support::share::ShareAck {
            group: "g1",
            member,
            topic_id: tid,
            partition: 0,
            epoch,
            first,
            last,
            ack_type,
        },
    )
    .await
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
                    acknowledge_types: vec![RENEW],
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
    crate::support::share::fetch_until_acquired(
        client,
        share_fetch_req(group, member, tid, partition, epoch, 0, vec![]),
        true,
    )
    .await
}
