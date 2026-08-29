//! KIP-405 remote read path.
//!
//! This module wraps the broker's shared [`RemoteStorageManager`] and
//! [`RemoteLogMetadataManager`] pair. It serves `Fetch` and `ListOffsets`
//! requests for offsets that have no local copy any more.
//!
//! The RSM and RLMM SPIs are synchronous and blocking. This module therefore
//! wraps byte-range reads, index reads, and `ListOffsets` metadata scans in
//! `tokio::task::spawn_blocking`, so those remote-tier operations do not stall
//! the broker's reactor. It decodes the fetched bytes with
//! [`krabka_remote_storage::index`], whose lookups mirror
//! `krabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the Kafka-format
//! index bytes that the copy path wrote verbatim.

use std::sync::Arc;

use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{LogOffset, RemoteLogMetadataManager, RemoteStorageManager};

mod aborted_txns;
mod blocking;
mod fetch;
#[cfg(test)]
mod test_support;
mod tiered_offsets;
mod timestamp_lookup;

/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: LogOffset,
    pub(crate) last_offset: LogOffset,
    pub(crate) producer_id: i64,
}

/// Holds the broker's shared `RSM` and `RLMM`, and serves remote reads.
pub(crate) struct RemoteReader {
    pub(crate) rsm: Arc<dyn RemoteStorageManager>,
    pub(crate) rlmm: Arc<dyn RemoteLogMetadataManager>,
}

/// The last offset durably copied to the remote tier and the leader epoch
/// that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TieredOffset {
    pub(crate) offset: LogOffset,
    pub(crate) leader_epoch: LeaderEpoch,
}

impl RemoteReader {
    pub(crate) fn new(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
    ) -> Self {
        Self { rsm, rlmm }
    }
}
