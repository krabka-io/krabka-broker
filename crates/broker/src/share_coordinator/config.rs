//! KIP-932 share-coordinator (persister) configuration.

use std::time::Duration;

use krabka_units::{ByteSize, mebibytes};

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
}

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
            write_timeout: Duration::from_millis(5_000),
            state_topic_prune_interval: Duration::from_millis(300_000),
            cold_partition_snapshot_interval: Duration::from_millis(300_000),
        };
        assert!(ShareCoordinatorConfig::default() == expected);
    }
}
