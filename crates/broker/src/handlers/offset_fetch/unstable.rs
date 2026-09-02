//! The KIP-447 `UNSTABLE_OFFSET_COMMIT` partition row.
//!
//! A `read_committed` EOS consumer sets `require_stable = true` on
//! `OffsetFetch` precisely so that it is told to wait rather than handed the
//! offset a still-open transaction is about to replace. Answering with the
//! older stable offset instead makes the consumer rewind and reprocess records
//! the transaction already committed.
//!
//! Kafka's row carries the invalid-offset sentinels and an empty — not null —
//! metadata string alongside error code 88, and the two response shapes
//! (pre-KIP-516 `Partition`, KIP-516 `Partitions`) must agree on it, so both
//! are built here.

use krabka_protocol::owned::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
};

use crate::codes;

/// The `committed_offset` an unstable row reports: `OffsetFetchResponse`'s
/// invalid-offset sentinel.
const INVALID_OFFSET: i64 = -1;

/// The `committed_leader_epoch` an unstable row reports: Kafka writes an
/// absent `OptionalInt` out as `-1`.
const INVALID_LEADER_EPOCH: i32 = -1;

/// The unstable row in the pre-KIP-516 (v0–v7) response shape.
pub(super) fn legacy_row(partition_index: i32) -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index,
        committed_offset: INVALID_OFFSET,
        committed_leader_epoch: INVALID_LEADER_EPOCH,
        metadata: Some(String::new()),
        error_code: codes::UNSTABLE_OFFSET_COMMIT,
        ..Default::default()
    }
}

/// The unstable row in the KIP-516 (v8+) `groups[]` response shape.
pub(super) fn group_row(partition_index: i32) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index,
        committed_offset: INVALID_OFFSET,
        committed_leader_epoch: INVALID_LEADER_EPOCH,
        metadata: Some(String::new()),
        error_code: codes::UNSTABLE_OFFSET_COMMIT,
        ..Default::default()
    }
}
