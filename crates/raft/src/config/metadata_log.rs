//! Kafka's `MetadataLogConfig`: where the metadata log lives, how its
//! segments roll, how long its prefix stays once a snapshot covers it, and how
//! often an idle leader writes to it.
//!
//! The metadata log is the single partition `__cluster_metadata-0`. Kafka keeps
//! it in a directory of that name under `metadata.log.dir`, beside its
//! segments, its KIP-630 snapshots and its `quorum-state` file, and krabka
//! keeps the same layout, so the JVM tools and Kafka's system tests find the
//! files where they look for them.

use {
    krabka_units::{
        fmt::Human as _,
        prelude::{ByteSize, Time, days, gibibytes, mebibytes, millis},
    },
    std::path::{Path, PathBuf},
};

/// The name of the metadata partition's directory under the metadata log
/// directory: Kafka's `UnifiedLog.logDirName` of `__cluster_metadata-0`.
pub const METADATA_PARTITION_DIR: &str = "__cluster_metadata-0";

/// `metadata.log.segment.bytes` default: 1 GiB.
pub const DEFAULT_METADATA_LOG_SEGMENT_SIZE: ByteSize = gibibytes(1);

/// The least `metadata.log.segment.bytes` Kafka accepts: 8 MiB. Kafka's
/// `ConfigDef` refuses a smaller value, and only its internal
/// `internal.metadata.log.segment.bytes` goes below it, for tests.
pub const MIN_METADATA_LOG_SEGMENT_SIZE: ByteSize = mebibytes(8);

/// `metadata.log.segment.ms` default: seven days.
pub const DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL: Time = days(7);

/// `metadata.max.retention.bytes` default: 100 MiB.
pub const DEFAULT_METADATA_MAX_RETENTION_SIZE: ByteSize = mebibytes(100);

/// `metadata.max.retention.ms` default: seven days.
pub const DEFAULT_METADATA_MAX_RETENTION: Time = days(7);

/// `metadata.max.idle.interval.ms` default: 500 ms.
pub const DEFAULT_METADATA_MAX_IDLE_INTERVAL: Time = millis(500);

/// The metadata partition directory under `metadata_log_dir`, Kafka's
/// `metadata.log.dir`. It holds the metadata log segments, the KIP-630
/// `<end_offset>-<epoch>.checkpoint` snapshots and the `quorum-state` file.
#[must_use]
pub fn metadata_partition_dir(metadata_log_dir: &Path) -> PathBuf {
    metadata_log_dir.join(METADATA_PARTITION_DIR)
}

/// How the metadata log rolls its segments and when it gives up the prefix
/// that a snapshot covers. The defaults are Kafka's.
///
/// Quantities render in the operator form (`1GiB`, `7d`), as the rest of
/// `ControllerConfig`'s `Debug` does.
#[derive(Clone, Copy, PartialEq, derive_more::Debug, krabka_macros::FieldDefaults)]
pub struct MetadataLogConfig {
    /// `metadata.log.segment.bytes`: the active segment rolls before an
    /// append that would take it past this size.
    #[debug("{:?}", segment_size.human().to_string())]
    #[default(DEFAULT_METADATA_LOG_SEGMENT_SIZE)]
    pub segment_size: ByteSize,
    /// `metadata.log.segment.ms`: the active segment rolls before an append
    /// that comes this long after the segment's first record.
    #[debug("{:?}", segment_roll_interval.human().to_string())]
    #[default(DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL)]
    pub segment_roll_interval: Time,
    /// `metadata.max.retention.bytes`: once the log and its snapshots together
    /// are larger than this, the oldest snapshot goes and the log start moves
    /// up to the next one. `None` is Kafka's negative value, which keeps every
    /// snapshot whatever the size.
    #[debug("{:?}", max_retention_size.map(|size| size.human().to_string()))]
    #[default(Some(DEFAULT_METADATA_MAX_RETENTION_SIZE))]
    pub max_retention_size: Option<ByteSize>,
    /// `metadata.max.retention.ms`: a snapshot whose last record is older than
    /// this goes, and the log start moves up to the next one. `None` is
    /// Kafka's negative value, which keeps every snapshot whatever its age.
    #[debug("{:?}", max_retention.map(|time| time.human().to_string()))]
    #[default(Some(DEFAULT_METADATA_MAX_RETENTION))]
    pub max_retention: Option<Time>,
    /// `metadata.max.idle.interval.ms` (KIP-835): how often the leader appends
    /// a `NoOpRecord`. Zero appends none.
    #[debug("{:?}", max_idle_interval.human().to_string())]
    #[default(DEFAULT_METADATA_MAX_IDLE_INTERVAL)]
    pub max_idle_interval: Time,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{ByteSizeExt, TimeExt},
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
        std::collections::BTreeMap,
    };

    struct MetadataStorage {
        root: Option<PathBuf>,
        config: Option<MetadataLogConfig>,
        partition: Option<PathBuf>,
        minimum_segment_bytes: Option<u64>,
    }

    #[fixture]
    fn storage() -> MetadataStorage {
        // Start this scenario with fresh inputs and observations.

        MetadataStorage {
            root: None,
            config: None,
            partition: None,
            minimum_segment_bytes: None,
        }
    }

    #[given("the metadata log root directory is {root}")]
    fn metadata_root(storage: &mut MetadataStorage, root: String) {
        storage.root = Some(PathBuf::from(root));
    }

    #[when("default metadata log storage is resolved")]
    fn resolve_metadata_storage(storage: &mut MetadataStorage) {
        let root = storage
            .root
            .as_ref()
            .expect("a metadata root directory was given");
        storage.config = Some(MetadataLogConfig::default());
        storage.partition = Some(metadata_partition_dir(root));
        storage.minimum_segment_bytes = Some(MIN_METADATA_LOG_SEGMENT_SIZE.bytes_u64());
    }

    #[derive(DataTableRow)]
    struct StorageLimit {
        setting: String,
        value: i128,
    }

    #[then("metadata storage uses these Kafka limits:")]
    fn default_limits(storage: &MetadataStorage, #[datatable] rows: Rows<StorageLimit>) {
        let config = storage
            .config
            .as_ref()
            .expect("default metadata storage was resolved");
        let actual: BTreeMap<String, Option<i128>> = [
            (
                "segment_bytes",
                Some(i128::from(config.segment_size.bytes_u64())),
            ),
            (
                "segment_roll_ms",
                Some(i128::from(config.segment_roll_interval.millis_i64())),
            ),
            (
                "retention_bytes",
                config
                    .max_retention_size
                    .map(ByteSizeExt::bytes_u64)
                    .map(i128::from),
            ),
            (
                "retention_ms",
                config
                    .max_retention
                    .map(TimeExt::millis_i64)
                    .map(i128::from),
            ),
            (
                "idle_ms",
                Some(i128::from(config.max_idle_interval.millis_i64())),
            ),
            (
                "minimum_segment_bytes",
                storage.minimum_segment_bytes.map(i128::from),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
        let expected: BTreeMap<_, _> = rows
            .into_iter()
            .map(|row| (row.setting, Some(row.value)))
            .collect();
        check!(actual == expected);
    }

    #[then("the metadata partition directory is {partition}")]
    fn partition_directory(storage: &MetadataStorage, partition: String) {
        check!(storage.partition == Some(PathBuf::from(partition)));
    }

    scenarios!("tests/features/migrated/src/config/metadata_log.feature", fixtures = [storage: MetadataStorage]);
}
