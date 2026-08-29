//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use krabka_compression::CompressionType;
use krabka_units::prelude::{ByteSize, Time, days, gibibytes, hours, kibibytes, mebibytes, millis};

/// Kafka's `segment.bytes` default: roll the active segment at 1 GiB.
const DEFAULT_SEGMENT_SIZE: ByteSize = gibibytes(1);

/// Kafka's `segment.ms` default: roll the active segment once its first
/// record is a week old.
const DEFAULT_SEGMENT_ROLL_INTERVAL: Time = days(7);

/// Kafka's `retention.ms` default: delete sealed segments a week after their
/// newest record.
const DEFAULT_RETENTION: Time = days(7);

/// Kafka's `index.interval.bytes` default: one sparse `.index`/`.timeindex`
/// entry per 4 KiB of `.log`.
const DEFAULT_INDEX_INTERVAL: ByteSize = kibibytes(4);

/// Kafka's `delete.retention.ms` default: a tombstone or transaction marker
/// stays readable for a day after it first becomes compaction-eligible.
const DEFAULT_DELETE_RETENTION: Time = hours(24);

/// Default clock-confidence bound for scheduled delivery: the broker treats
/// its own clock as accurate to within a quarter of a second.
const DEFAULT_DELIVERY_CLOCK_UNCERTAINTY: Time = millis(250);

/// Default upper bound on a single read's initial allocation.
pub const DEFAULT_READ_BUFFER_CAP: ByteSize = mebibytes(4);

/// Default byte window for timestamp scans between sparse index entries.
pub const DEFAULT_TIMESTAMP_SCAN_WINDOW: ByteSize = kibibytes(64);

/// Per-topic policy for what to do with old log segments.
///
/// `Delete` is the default. It deletes segments by age or by size in
/// `crate::retention`. `Compact` does newest-wins dedup by key. `crate::compact`
/// implements it, and [`crate::Log::compact`] invokes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    #[default]
    Delete,
    Compact,
}

/// Per-topic policy for when a durable record becomes visible to consumers.
///
/// `Immediate` is the default and is every ordinary topic. `Scheduled` gates
/// visibility on each batch's activation time, so a producer can write a
/// record now and have it delivered later. `crate::delivery` implements it,
/// and [`Log::advance_delivery_watermark`](crate::Log::advance_delivery_watermark)
/// computes the offset that separates the visible prefix from the scheduled
/// tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryPolicy {
    /// A batch is visible as soon as it is durable.
    #[default]
    Immediate,
    /// A batch is visible once its activation time has passed. The activation
    /// time is the batch's `max_timestamp`, the v2 header field, so the
    /// schedule travels with the records and needs no sidecar.
    Scheduled,
}

/// Tunables for [`Log`](crate::Log) behavior.
///
/// Defaults match Apache Kafka 4.2 for `segment.bytes`, `segment.ms`,
/// `retention.ms`, `index.interval.bytes`, and the other tunables. Start from
/// the [`Default`](Self::default) impl. Most production deployments override
/// only [`Self::retention`] and [`Self::retention_size`].
#[derive(Debug, Clone, PartialEq)]
pub struct LogConfig {
    /// Cap the initial allocation used by decoded and raw segment reads.
    pub read_buffer_cap: ByteSize,

    /// Read timestamp searches in windows of this size.
    pub timestamp_scan_window: ByteSize,

    /// Roll the active segment once it grows past this. Kafka's
    /// `segment.bytes`; default 1 GiB.
    pub segment_size: ByteSize,

    /// Roll the active segment when its first record is older than this.
    /// Kafka's `segment.ms`; default 7 days.
    pub segment_roll_interval: Time,

    /// Delete sealed segments older than this. `None` = unlimited. Kafka's
    /// `retention.ms`; default 7 days.
    pub retention: Option<Time>,

    /// Delete oldest sealed segments until the total `.log` size fits.
    /// `None` = unlimited. Kafka's `retention.bytes`.
    pub retention_size: Option<ByteSize>,

    /// Write one `.index`/`.timeindex` entry per this much `.log`. Kafka's
    /// `index.interval.bytes`; default 4 KiB.
    pub index_interval: ByteSize,

    /// fsync after every `append`. Default off. The broker manages fsync
    /// separately.
    pub flush_on_append: bool,

    /// On open, CRC every batch in the active segment from the last index entry to EOF.
    pub validate_on_open: bool,

    /// Cleanup policy. Defaults to `Delete`. See [`CleanupPolicy`].
    pub cleanup_policy: CleanupPolicy,

    /// Broker-side recompression target. `None` is Kafka's
    /// `compression.type=producer`, which is pass-through: the broker stores
    /// the batch exactly as the producer sent it. `Some(c)` re-encodes every
    /// batch the broker accepts on this partition to `c` before the write.
    /// This matches Kafka's per-topic `compression.type` config. `gzip`,
    /// `snappy`, `lz4`, `zstd`, and `uncompressed` map to `Some(_)`.
    /// `producer`, the default, maps to `None`.
    pub compression_type: Option<CompressionType>,

    /// When `true`, the broker's `RemoteLogManager` may copy this
    /// partition's sealed segments (KIP-405) to the remote tier. This maps to
    /// Kafka's per-topic `remote.storage.enable`. Default `false`, which is
    /// also Kafka's default, because tiered storage is opt-in per topic.
    pub remote_storage_enable: bool,

    /// Local-disk time-retention window for tiered partitions (KIP-405).
    /// `None` inherits [`Self::retention`]. Default `None`.
    pub local_retention: Option<Time>,

    /// Local-disk size budget for tiered partitions (KIP-405).
    /// `None` inherits [`Self::retention_size`]. Default `None`.
    pub local_retention_size: Option<ByteSize>,

    /// KIP-534. After a tombstone or transaction marker first becomes
    /// compaction-eligible, the log retains it for at least this long before
    /// deletion. This is the delete-horizon grace window. Default 24h.
    pub delete_retention: Time,

    /// When a durable record becomes visible. Defaults to
    /// [`DeliveryPolicy::Immediate`]. See [`DeliveryPolicy`].
    pub delivery_policy: DeliveryPolicy,

    /// Declared bound on how far this broker's clock can be from true time.
    /// Default 250 ms. It has an effect only under
    /// [`DeliveryPolicy::Scheduled`].
    ///
    /// A batch is visible once
    /// `max_timestamp + delivery_clock_uncertainty <= now`. If the clock
    /// reads `c` while true time is somewhere in `[c - e, c + e]`, then
    /// `c >= activation + e` proves true time has reached the activation
    /// instant. Delivery is therefore never early, and it is late by at most
    /// `2 * delivery_clock_uncertainty`.
    pub delivery_clock_uncertainty: Time,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            read_buffer_cap: DEFAULT_READ_BUFFER_CAP,
            timestamp_scan_window: DEFAULT_TIMESTAMP_SCAN_WINDOW,
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment_roll_interval: DEFAULT_SEGMENT_ROLL_INTERVAL,
            retention: Some(DEFAULT_RETENTION),
            retention_size: None,
            index_interval: DEFAULT_INDEX_INTERVAL,
            flush_on_append: false,
            validate_on_open: true,
            cleanup_policy: CleanupPolicy::Delete,
            // Pass-through: producers' compression choice wins. Kafka's
            // default. Operators flip this to a specific codec on
            // topics where they want broker-side enforcement.
            compression_type: None,
            // Tiered storage is opt-in per topic (Kafka default false).
            remote_storage_enable: false,
            local_retention: None,
            local_retention_size: None,
            delete_retention: DEFAULT_DELETE_RETENTION,
            // Scheduled delivery is opt-in per topic; an ordinary topic pays
            // nothing for it.
            delivery_policy: DeliveryPolicy::Immediate,
            delivery_clock_uncertainty: DEFAULT_DELIVERY_CLOCK_UNCERTAINTY,
        }
    }
}

#[cfg(test)]
mod tests {

    use krabka_units::prelude::{ByteSizeExt as _, TimeExt, bytes, secs};

    use super::*;

    #[test]
    fn defaults_match_kafka_4x() {
        assert2::assert!(
            LogConfig::default()
                == LogConfig {
                    read_buffer_cap: mebibytes(4),
                    timestamp_scan_window: kibibytes(64),
                    segment_size: bytes(1 << 30),
                    segment_roll_interval: days(7),
                    retention: Some(days(7)),
                    retention_size: None,
                    index_interval: bytes(4096),
                    flush_on_append: false,
                    validate_on_open: true,
                    cleanup_policy: CleanupPolicy::Delete,
                    compression_type: None,
                    remote_storage_enable: false,
                    local_retention: None,
                    local_retention_size: None,
                    delete_retention: secs(24 * 60 * 60),
                    delivery_policy: DeliveryPolicy::Immediate,
                    delivery_clock_uncertainty: krabka_units::prelude::millis(250),
                }
        );
    }

    #[test]
    fn defaults_cross_the_raw_seams_as_kafkas_documented_numbers() {
        // The quantities exist to be handed to `.index` sizing, retention
        // arithmetic, and Kafka config reporting as plain integers; a
        // scale slip in a constructor would show up here.
        let c = LogConfig::default();
        assert2::check!(c.segment_size.bytes_u64() == 1_073_741_824);
        assert2::check!(c.index_interval.bytes_u64() == 4_096);
        assert2::check!(c.segment_roll_interval.millis_i64() == 604_800_000);
        assert2::check!(c.retention.map(TimeExt::millis_i64) == Some(604_800_000));
        assert2::check!(c.delete_retention.millis_i64() == 86_400_000);
    }

    #[test]
    fn default_cleanup_policy_is_delete() {
        let c = LogConfig::default();
        assert2::assert!(c.cleanup_policy == CleanupPolicy::Delete);
    }

    #[test]
    fn default_compression_is_producer_passthrough() {
        let c = LogConfig::default();
        assert2::assert!(c.compression_type == None);
    }

    #[test]
    fn delivery_is_immediate_with_a_quarter_second_clock_bound() {
        let c = LogConfig::default();
        assert2::check!(c.delivery_policy == DeliveryPolicy::Immediate);
        assert2::check!(c.delivery_clock_uncertainty.millis_i64() == 250);
    }

    #[test]
    fn default_local_retention_is_none() {
        let c = LogConfig::default();
        assert2::assert!(c.local_retention == None);
        assert2::assert!(c.local_retention_size == None);
    }
}
