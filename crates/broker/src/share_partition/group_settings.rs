//! The share settings of one group on the share-partition leader: each
//! per-group `share.*` override from the group config in the metadata image,
//! with the broker setting as the default.
//!
//! This is Kafka's `ShareGroupConfigProvider`. `SharePartition` asks it for
//! the record lock duration, the delivery count limit, the record lock limit
//! and whether `Renew` is allowed, and `KafkaApis` asks it for the isolation
//! level and for the lock duration that `AcquisitionLockTimeoutMs` reports.
//! An override that does not parse falls back to the default; the config
//! RPCs validate a value before they store it.

use std::time::Duration;

use krabka_metadata::MetadataImage;

use crate::coordinator::unified::share::config::{ShareGroupConfig, ShareIsolationLevel};

/// Kafka's `GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG`.
const KEY_RECORD_LOCK_DURATION_MS: &str = "share.record.lock.duration.ms";
/// Kafka's `GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG`.
const KEY_DELIVERY_COUNT_LIMIT: &str = "share.delivery.count.limit";
/// Kafka's `GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG`.
const KEY_PARTITION_MAX_RECORD_LOCKS: &str = "share.partition.max.record.locks";
/// Kafka's `GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG`.
const KEY_ISOLATION_LEVEL: &str = "share.isolation.level";
/// Kafka's `GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG`.
const KEY_RENEW_ACKNOWLEDGE_ENABLE: &str = "share.renew.acknowledge.enable";

/// The resolved share settings of one group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupShareSettings {
    /// How long an acquired record stays locked to its member.
    pub(crate) record_lock_duration: Duration,
    /// The delivery count at which a record is archived.
    pub(crate) delivery_count_limit: i16,
    /// The most records a share partition holds in flight.
    pub(crate) max_record_locks: i32,
    /// The group reads only committed transactional data.
    pub(crate) read_committed: bool,
    /// The group allows `Renew` acknowledgements.
    pub(crate) renew_acknowledge_enabled: bool,
}

impl GroupShareSettings {
    /// Resolves the settings of `group`: each override in the group config
    /// of `image`, or the broker setting of `defaults`.
    ///
    /// Kafka has no broker-level share isolation key, and its group default is
    /// `read_uncommitted`. The broker setting stands in as that default here,
    /// because it is what a deployment configures today.
    #[must_use]
    pub(crate) fn resolve(image: &MetadataImage, group: &str, defaults: &ShareGroupConfig) -> Self {
        let overrides = image.group_config(group);
        let value = |key: &str| overrides.and_then(|configs| configs.get(key));
        Self {
            record_lock_duration: value(KEY_RECORD_LOCK_DURATION_MS)
                .and_then(|ms| ms.trim().parse::<u64>().ok())
                .map_or(defaults.record_lock_duration, Duration::from_millis),
            delivery_count_limit: value(KEY_DELIVERY_COUNT_LIMIT)
                .and_then(|limit| limit.trim().parse().ok())
                .unwrap_or(defaults.max_delivery_attempts),
            max_record_locks: value(KEY_PARTITION_MAX_RECORD_LOCKS)
                .and_then(|locks| locks.trim().parse().ok())
                .unwrap_or(defaults.max_inflight_records),
            read_committed: value(KEY_ISOLATION_LEVEL).map_or(
                defaults.isolation_level == ShareIsolationLevel::ReadCommitted,
                |level| level.trim().eq_ignore_ascii_case("read_committed"),
            ),
            renew_acknowledge_enabled: value(KEY_RENEW_ACKNOWLEDGE_ENABLE)
                .is_none_or(|enabled| !enabled.trim().eq_ignore_ascii_case("false")),
        }
    }

    /// The lock duration in milliseconds, as `AcquisitionLockTimeoutMs`
    /// carries it.
    #[must_use]
    pub(crate) fn record_lock_duration_ms(&self) -> i32 {
        i32::try_from(self.record_lock_duration.as_millis()).unwrap_or(i32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{GroupConfigRecord, MetadataRecord};

    use super::*;

    fn image(configs: &[(&str, &str)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "g".into(),
            configs: configs
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        }));
        image
    }

    #[test]
    fn each_override_replaces_its_broker_default() {
        let defaults = ShareGroupConfig::default();
        let broker = GroupShareSettings {
            record_lock_duration: defaults.record_lock_duration,
            delivery_count_limit: defaults.max_delivery_attempts,
            max_record_locks: defaults.max_inflight_records,
            read_committed: false,
            renew_acknowledge_enabled: true,
        };
        let rows: Vec<(&[(&str, &str)], GroupShareSettings)> = vec![
            (&[], broker),
            (
                &[(KEY_RECORD_LOCK_DURATION_MS, "60000")],
                GroupShareSettings {
                    record_lock_duration: Duration::from_mins(1),
                    ..broker
                },
            ),
            (
                &[(KEY_DELIVERY_COUNT_LIMIT, "2")],
                GroupShareSettings {
                    delivery_count_limit: 2,
                    ..broker
                },
            ),
            (
                &[(KEY_PARTITION_MAX_RECORD_LOCKS, "10")],
                GroupShareSettings {
                    max_record_locks: 10,
                    ..broker
                },
            ),
            (
                &[(KEY_ISOLATION_LEVEL, "read_committed")],
                GroupShareSettings {
                    read_committed: true,
                    ..broker
                },
            ),
            (
                &[(KEY_RENEW_ACKNOWLEDGE_ENABLE, "FALSE")],
                GroupShareSettings {
                    renew_acknowledge_enabled: false,
                    ..broker
                },
            ),
            (&[(KEY_DELIVERY_COUNT_LIMIT, "many")], broker),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (configs, settings) in rows {
            actual.push(GroupShareSettings::resolve(&image(configs), "g", &defaults));
            expected.push(settings);
        }
        assert!(actual == expected);
    }
}
