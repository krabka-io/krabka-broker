//! Tunables of the barrier coordinator.
//!
//! The broker configuration owns these values. The coordinator takes them as
//! one struct, so a test can build a coordinator without a `BrokerConfig`.

use krabka_units::{ByteSize, Time, mebibytes, millis, minutes, secs};

/// How the coordinator sizes its internal topic, its retries, and its
/// scheduler.
///
/// The struct is [`PartialEq`] but not [`Eq`], because [`Time`] and
/// [`ByteSize`] are backed by a float.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BarrierConfig {
    /// Partition count of `__barrier_state`. It fixes the group-to-partition
    /// map, so a change moves every group.
    pub(crate) state_topic_num_partitions: i32,
    /// Replication factor of `__barrier_state`. The broker count caps it.
    pub(crate) state_topic_replication_factor: i16,
    /// How much of a state partition the coordinator reads per call during
    /// recovery.
    pub(crate) recovery_read_max: ByteSize,
    /// How long one injection retries the partitions that carry no marker
    /// yet. The coordinator publishes a partial cut when this time runs out.
    pub(crate) injection_timeout: Time,
    /// The first wait between two fan-out attempts.
    pub(crate) retry_backoff: Time,
    /// The largest wait between two fan-out attempts.
    pub(crate) retry_backoff_max: Time,
    /// How often the scheduler looks for a group that is due.
    pub(crate) scheduler_tick: Time,
    /// How many cuts a group keeps when the caller names no value.
    pub(crate) default_retained_cuts: i32,
    /// Maximum number of cuts a barrier group may retain.
    pub(crate) max_retained_cuts: i32,
    /// Maximum number of barrier groups the coordinator accepts.
    pub(crate) max_groups: usize,
    /// Maximum number of topics in one barrier group.
    pub(crate) max_topics_per_group: usize,
    /// Shortest periodic injection interval a barrier group may ask for.
    pub(crate) min_injection_interval: Time,
}

impl Default for BarrierConfig {
    fn default() -> Self {
        Self {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            recovery_read_max: mebibytes(1),
            injection_timeout: minutes(1),
            retry_backoff: millis(100),
            retry_backoff_max: secs(5),
            scheduler_tick: secs(1),
            default_retained_cuts: 32,
            max_retained_cuts: 100,
            max_groups: 100,
            max_topics_per_group: 100,
            min_injection_interval: secs(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::convert::TimeExt as _;

    use super::*;

    #[test]
    fn the_defaults_are_the_documented_values() {
        let expected = BarrierConfig {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            recovery_read_max: mebibytes(1),
            injection_timeout: minutes(1),
            retry_backoff: millis(100),
            retry_backoff_max: secs(5),
            scheduler_tick: secs(1),
            default_retained_cuts: 32,
            max_retained_cuts: 100,
            max_groups: 100,
            max_topics_per_group: 100,
            min_injection_interval: secs(1),
        };
        assert!(BarrierConfig::default() == expected);
    }


    #[test]
    fn the_first_backoff_is_below_the_largest_one() {
        let config = BarrierConfig::default();
        assert!(config.retry_backoff.millis_i64() < config.retry_backoff_max.millis_i64());
        assert!(config.retry_backoff_max.millis_i64() < config.injection_timeout.millis_i64());
    }
}
