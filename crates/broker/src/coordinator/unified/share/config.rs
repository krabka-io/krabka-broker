//! KIP-932 share-group membership configuration.
use std::{borrow::Cow, collections::BTreeMap, time::Duration};

use crate::coordinator::unified::config::{DEFAULT_ASSIGNMENT_INTERVAL, group_millis};

/// Kafka's `GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG`.
const KEY_SHARE_SESSION_TIMEOUT_MS: &str = "share.session.timeout.ms";
/// Kafka's `GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG`.
const KEY_SHARE_HEARTBEAT_INTERVAL_MS: &str = "share.heartbeat.interval.ms";
/// Kafka's `GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG`.
const KEY_SHARE_ASSIGNMENT_INTERVAL_MS: &str = "share.assignment.interval.ms";

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
    /// Kafka's `group.share.session.timeout.ms`.
    pub session_timeout: Duration,
    /// Kafka's `group.share.heartbeat.interval.ms`.
    pub heartbeat_interval: Duration,
    /// Kafka's `group.share.assignment.interval.ms`: the least time between
    /// two target assignments of a group. Zero does not wait.
    pub assignment_interval: Duration,
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
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            assignment_interval: DEFAULT_ASSIGNMENT_INTERVAL,
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

impl ShareGroupConfig {
    /// The defaults with no assignment interval, for the tests that expect
    /// each membership change to be assigned at once.
    #[cfg(test)]
    pub(crate) fn assigning_at_once() -> Self {
        Self {
            assignment_interval: Duration::ZERO,
            ..Self::default()
        }
    }

    /// The membership settings a share group runs with: each `share.*`
    /// override in the group's stored config over the broker value.
    ///
    /// This is Kafka's `GroupMetadataManager.shareGroupSessionTimeoutMs`,
    /// `shareGroupHeartbeatIntervalMs` and `shareGroupAssignmentIntervalMs`:
    /// `GroupConfigManager.groupConfig` over `GroupCoordinatorConfig`. A group
    /// with no override borrows the broker value.
    #[must_use]
    pub(crate) fn for_group(&self, overrides: Option<&BTreeMap<String, String>>) -> Cow<'_, Self> {
        let session = group_millis(overrides, KEY_SHARE_SESSION_TIMEOUT_MS);
        let heartbeat = group_millis(overrides, KEY_SHARE_HEARTBEAT_INTERVAL_MS);
        let assignment = group_millis(overrides, KEY_SHARE_ASSIGNMENT_INTERVAL_MS);
        if session.is_none() && heartbeat.is_none() && assignment.is_none() {
            return Cow::Borrowed(self);
        }
        let mut config = self.clone();
        config.session_timeout = session.unwrap_or(config.session_timeout);
        config.heartbeat_interval = heartbeat.unwrap_or(config.heartbeat_interval);
        config.assignment_interval = assignment.unwrap_or(config.assignment_interval);
        Cow::Owned(config)
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
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            assignment_interval: Duration::from_secs(1),
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

    /// Kafka's `shareGroupSessionTimeoutMs`, `shareGroupHeartbeatIntervalMs`
    /// and `shareGroupAssignmentIntervalMs`: a `share.*` override replaces the
    /// broker value, another coordinator's key and a value that does not parse
    /// leave it.
    #[test]
    fn for_group_applies_each_share_override() {
        let broker = ShareGroupConfig::default();
        let with = |entries: &[(&str, &str)]| {
            let overrides: BTreeMap<String, String> = entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            broker.for_group(Some(&overrides)).into_owned()
        };
        let rows: Vec<(&[(&str, &str)], ShareGroupConfig)> = vec![
            (&[], broker.clone()),
            (
                &[("share.session.timeout.ms", "50000")],
                ShareGroupConfig {
                    session_timeout: Duration::from_secs(50),
                    ..broker.clone()
                },
            ),
            (
                &[("share.heartbeat.interval.ms", "7000")],
                ShareGroupConfig {
                    heartbeat_interval: Duration::from_secs(7),
                    ..broker.clone()
                },
            ),
            (
                &[("share.assignment.interval.ms", "0")],
                ShareGroupConfig {
                    assignment_interval: Duration::ZERO,
                    ..broker.clone()
                },
            ),
            (
                &[
                    ("consumer.session.timeout.ms", "50000"),
                    ("share.heartbeat.interval.ms", "many"),
                ],
                broker.clone(),
            ),
        ];
        let (actual, expected): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .map(|(entries, config)| (with(entries), config))
            .unzip();
        assert!(actual == expected);
        assert!(matches!(broker.for_group(None), Cow::Borrowed(_)));
    }
}
