//! KIP-932 share-group membership configuration.
use std::time::Duration;

/// The broker share-group settings: the share keys of Kafka's
/// `GroupCoordinatorConfig` and of its `ShareGroupConfig`.
///
/// Kafka has no broker share isolation key. A group reads with its own
/// `share.isolation.level`, which defaults to `read_uncommitted`.
///
/// Each `min_*` and `max_*` pair bounds the broker value beside it and the
/// matching per-group override. The `[runtime]` applier checks Kafka's
/// ranges and the order within each triple at startup.
#[derive(Debug, Clone, PartialEq)]
pub struct ShareGroupConfig {
    /// Kafka's `group.share.enable`.
    pub enable: bool,
    /// Kafka's `group.share.session.timeout.ms`.
    pub session_timeout: Duration,
    /// Kafka's `group.share.heartbeat.interval.ms`.
    pub heartbeat_interval: Duration,
    /// Kafka's `group.share.min.session.timeout.ms`.
    pub min_session_timeout: Duration,
    /// Kafka's `group.share.max.session.timeout.ms`.
    pub max_session_timeout: Duration,
    /// Kafka's `group.share.min.heartbeat.interval.ms`.
    pub min_heartbeat_interval: Duration,
    /// Kafka's `group.share.max.heartbeat.interval.ms`.
    pub max_heartbeat_interval: Duration,
    /// Kafka's `group.share.max.size`.
    pub max_size: usize,
    /// Kafka's `group.share.record.lock.duration.ms`.
    pub record_lock_duration: Duration,
    /// Kafka's `group.share.min.record.lock.duration.ms`.
    pub min_record_lock_duration: Duration,
    /// Kafka's `group.share.max.record.lock.duration.ms`.
    pub max_record_lock_duration: Duration,
    /// Kafka's `group.share.delivery.count.limit`: the delivery count at
    /// which a record is archived.
    pub max_delivery_attempts: i16,
    /// Kafka's `group.share.min.delivery.count.limit`.
    pub min_delivery_count_limit: i16,
    /// Kafka's `group.share.max.delivery.count.limit`.
    pub max_delivery_count_limit: i16,
    /// Kafka's `group.share.partition.max.record.locks`: the most records a
    /// share partition holds in flight.
    pub max_inflight_records: i32,
    /// Kafka's `group.share.min.partition.max.record.locks`.
    pub min_partition_max_record_locks: i32,
    /// Kafka's `group.share.max.partition.max.record.locks`.
    pub max_partition_max_record_locks: i32,
    pub backlog_poll_interval: Duration,
    pub actor_mailbox_capacity: usize,
    /// Kafka's internal `group.share.initialize.retry.interval.ms`: how long a
    /// partition may stay initializing before the group asks the persister to
    /// initialize it again.
    pub initialize_retry_interval: Duration,
}

impl Default for ShareGroupConfig {
    fn default() -> Self {
        Self {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_size: 200,
            record_lock_duration: Duration::from_secs(30),
            min_record_lock_duration: Duration::from_secs(15),
            max_record_lock_duration: Duration::from_mins(1),
            max_delivery_attempts: 5,
            min_delivery_count_limit: 2,
            max_delivery_count_limit: 10,
            max_inflight_records: 2000,
            min_partition_max_record_locks: 100,
            max_partition_max_record_locks: 4000,
            backlog_poll_interval: Duration::from_secs(15),
            actor_mailbox_capacity: 64,
            initialize_retry_interval: Duration::from_secs(30),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The defaults of the share keys of Kafka 4.3.1's
    /// `GroupCoordinatorConfig` and of its `ShareGroupConfig`.
    #[test]
    fn defaults_are_kafkas() {
        let expected = ShareGroupConfig {
            enable: true,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_size: 200,
            record_lock_duration: Duration::from_secs(30),
            min_record_lock_duration: Duration::from_secs(15),
            max_record_lock_duration: Duration::from_mins(1),
            max_delivery_attempts: 5,
            min_delivery_count_limit: 2,
            max_delivery_count_limit: 10,
            max_inflight_records: 2000,
            min_partition_max_record_locks: 100,
            max_partition_max_record_locks: 4000,
            backlog_poll_interval: Duration::from_secs(15),
            actor_mailbox_capacity: 64,
            initialize_retry_interval: Duration::from_secs(30),
        };
        assert!(ShareGroupConfig::default() == expected);
    }
}
