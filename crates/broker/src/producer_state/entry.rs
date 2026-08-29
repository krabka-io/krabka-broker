//! The per-producer sequence record and the per-partition map that holds it.
//!
//! `ProducerEntry` is one idempotent producer's last-accepted-batch state on a
//! partition, and `PartitionProducerState` is the map of those records that a
//! per-partition mutex guards. They sit in their own file because every other
//! part of the tracker reads or writes them.

use std::collections::HashMap;

use krabka_log::ProducerId;

use crate::partition::LogOffset;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Last absolute offset of the last accepted batch for this producer
    /// (`base_offset + last_offset_delta`).
    /// [`ProducerState::truncate`](super::ProducerState::truncate) reads it to
    /// drop entries whose batch was truncated off the log.
    pub last_offset: LogOffset,
    pub base_offset: LogOffset,
    /// Timestamp of the last accepted batch for this producer.
    pub last_timestamp: i64,
    /// Wall-clock millis of the last `commit` that touched this entry.
    /// [`ProducerState::expire_older_than`](super::ProducerState::expire_older_than)
    /// uses it to evict idle idempotent-producer state. This matches Kafka's
    /// `producer.id.expiration.ms`, which expires by inactivity.
    pub last_activity_ms: i64,
}

#[derive(Debug, Default)]
pub struct PartitionProducerState {
    pub entries: HashMap<ProducerId, ProducerEntry>,
}
