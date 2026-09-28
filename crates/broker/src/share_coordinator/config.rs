//! KIP-932 share-coordinator (persister) configuration.

use std::time::Duration;

use krabka_compression::CompressionType;
use krabka_units::{ByteSize, bytes, mebibytes};

/// Kafka's `share.coordinator.*` keys of `ShareCoordinatorConfig`.
///
/// This struct is not `Eq`. The recovery read budget is a quantity, and its
/// `f64` storage is only `PartialEq`.
#[derive(Debug, Clone, PartialEq)]
pub struct ShareCoordinatorConfig {
    pub state_topic_num_partitions: i32,
    pub state_topic_replication_factor: i16,
    pub state_topic_min_isr: i32,
    pub state_topic_segment_bytes: ByteSize,
    /// Kafka's `share.coordinator.snapshot.update.records.per.snapshot`.
    pub snapshot_update_records_per_snapshot: u32,
    /// Kafka's `share.coordinator.load.buffer.size`: the most bytes one
    /// recovery read of a state partition asks for.
    pub load_buffer_size: ByteSize,
    /// Kafka's `share.coordinator.write.timeout.ms`: how long an append to
    /// `__share_group_state` may take.
    pub write_timeout: Duration,
    /// Kafka's `share.coordinator.state.topic.prune.interval.ms`: how often
    /// the coordinator trims the redundant prefix of each led state
    /// partition.
    pub state_topic_prune_interval: Duration,
    /// Kafka's `share.coordinator.cold.partition.snapshot.interval.ms`: how
    /// old the latest snapshot of a key may get before the coordinator writes
    /// a new one.
    pub cold_partition_snapshot_interval: Duration,
    /// Kafka's `share.coordinator.state.topic.compression.codec`: the codec
    /// of every batch the coordinator appends to `__share_group_state`.
    pub state_topic_compression_codec: CompressionType,
    /// Kafka's `share.coordinator.threads`. Accepted and not applied: the
    /// coordinator runs as tasks on the broker's shared async runtime, so it
    /// has no thread pool to size.
    pub threads: u32,
    /// Kafka's `share.coordinator.append.linger.ms`, with `None` for its `-1`
    /// adaptive linger. Accepted and not applied: each share-state write is
    /// its own append, and the partition writer groups concurrent appends
    /// without waiting for more.
    pub append_linger: Option<Duration>,
    /// Kafka's `share.coordinator.cached.buffer.max.bytes`. Accepted and not
    /// applied: the coordinator encodes each record into a new buffer and
    /// keeps no buffer for reuse.
    pub cached_buffer_max_bytes: ByteSize,
}

/// Kafka's `share.coordinator.cached.buffer.max.bytes` default: 1 MiB plus
/// `Records.LOG_OVERHEAD`, the 12-byte offset and size prefix of a batch.
pub const CACHED_BUFFER_MAX_BYTES_DEFAULT: u32 = 1024 * 1024 + 12;

impl Default for ShareCoordinatorConfig {
    fn default() -> Self {
        Self {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 2,
            state_topic_segment_bytes: mebibytes(100),
            snapshot_update_records_per_snapshot: 500,
            load_buffer_size: mebibytes(5),
            write_timeout: Duration::from_secs(5),
            state_topic_prune_interval: Duration::from_secs(300),
            cold_partition_snapshot_interval: Duration::from_secs(300),
            state_topic_compression_codec: CompressionType::None,
            threads: 1,
            append_linger: None,
            cached_buffer_max_bytes: bytes(CACHED_BUFFER_MAX_BYTES_DEFAULT),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_match_kafka() {
        let expected = ShareCoordinatorConfig {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 2,
            state_topic_segment_bytes: mebibytes(100),
            snapshot_update_records_per_snapshot: 500,
            load_buffer_size: mebibytes(5),
            write_timeout: Duration::from_secs(5),
            state_topic_prune_interval: Duration::from_mins(5),
            cold_partition_snapshot_interval: Duration::from_mins(5),
            state_topic_compression_codec: CompressionType::None,
            threads: 1,
            append_linger: None,
            cached_buffer_max_bytes: bytes(1_048_588),
        };
        assert!(ShareCoordinatorConfig::default() == expected);
    }
}
