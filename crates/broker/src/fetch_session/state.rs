//! The per-partition state that a fetch session caches, and the key that
//! identifies a partition across Fetch versions.
//!
//! `FetchSessionKey` keeps both halves of a topic's identity, because a Fetch
//! request carries only one of them depending on its version.
//! `CachedPartitionState` holds what the client asked for next to what the
//! broker last sent, which is the comparison KIP-227 needs to decide whether a
//! partition belongs in the next response.

use krabka_protocol::primitives::uuid::Uuid as WireUuid;

/// (`topic_name`, `topic_id`, partition). The cache keeps both the name and
/// the id, because Fetch v 12 and below sends only the name and v 13 and above
/// sends only the id. The cache must resolve the key for either version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FetchSessionKey {
    pub topic_name: String,
    pub topic_id: WireUuid,
    pub partition: i32,
}

/// Per-partition cached state. The first block (`fetch_offset` and the fields
/// after it) records what the client wants on the next read. The `last_*`
/// block records what the broker sent in the previous response. The broker
/// compares the two blocks to decide whether the next response includes this
/// partition, because KIP-227 omits a partition when nothing has changed since
/// the previous response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedPartitionState {
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub current_leader_epoch: i32,
    pub max_bytes: i32,
    pub log_start_offset: i64,
    pub last_high_watermark: i64,
    pub last_last_stable_offset: i64,
    pub last_log_start_offset: i64,
    pub last_preferred_read_replica: i32,
    pub last_aborted_txns_hash: u64,
    pub last_error_code: i16,
}
