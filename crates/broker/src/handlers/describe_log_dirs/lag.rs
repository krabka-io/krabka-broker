//! The two `offset_lag` values that a `DescribeLogDirs` partition entry
//! carries.
//!
//! Kafka's `ReplicaManager.getLogEndOffsetLag`: a current log reports
//! `max(HW − LEO, 0)`, which is 0 because the high watermark never passes the
//! log end offset, while a KIP-113 future log reports
//! `current_log.LEO − future_log.LEO` so that an operator can watch an
//! intra-broker move drain. Both report [`INVALID_OFFSET_LAG`] when the broker
//! has no local current log for the partition. Both readings reach into the
//! partition registry and the future-log registry, which is why they live
//! together and away from the directory scan.

use {
    crate::{future_log::FutureLogState, partition_registry::PartitionRegistry},
    std::sync::{Arc, PoisonError},
};

/// `DescribeLogDirsResponse.INVALID_OFFSET_LAG`: the lag is not available
/// because the replica is not created or is offline.
pub(super) const INVALID_OFFSET_LAG: i64 = -1;

/// `max(HW − LEO, 0)` for a loaded current log.
///
/// Returns [`INVALID_OFFSET_LAG`] when the partition is not materialized on
/// this broker.
pub(super) async fn offset_lag_for(
    partitions: &PartitionRegistry,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, krabka_ids::PartitionIndex(partition)) else {
        return INVALID_OFFSET_LAG;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Lag is a record-count delta between two offsets, not an offset.
    (hw.0 - leo.0).max(0)
}

/// `current_log.LEO − future_log.LEO` for an in-progress KIP-113 move, with no
/// clamp.
///
/// Returns [`INVALID_OFFSET_LAG`] if the partition is not materialized
/// locally. The future LEO counts as 0 if the future-log registry has no
/// entry. The registry has no entry when the broker has just started and the
/// resume task has not opened the future log yet.
pub(super) fn future_offset_lag(
    partitions: &PartitionRegistry,
    future_logs: &dashmap::DashMap<(String, krabka_ids::PartitionIndex), Arc<FutureLogState>>,
    topic: &str,
    partition: krabka_ids::PartitionIndex,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return INVALID_OFFSET_LAG;
    };
    let current_leo = part.log_end_offset();
    let future_leo =
        future_logs
            .get(&(topic.to_owned(), partition))
            .map_or(krabka_log::Offset(0), |e| {
                e.value()
                    .future_log
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .log_end_offset()
            });
    // Lag is a record-count delta between two offsets, not an offset.
    current_leo.0 - future_leo.0
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            future_log::FutureLogState,
            partition::Partition,
            test_support::{RecordCount, open_partition, repeated_records_batch},
        },
        assert2::assert,
        std::{
            fs,
            path::Path,
            sync::{Arc, Mutex, PoisonError},
        },
        tokio::task::JoinHandle,
        tokio_util::sync::CancellationToken,
    };

    /// Builds a `Partition` rooted at `<log_dir>/<topic>-<partition>`.
    ///
    /// The function uses the real `spawn_partition` path and mirrors the
    /// `future_log` and registry test fixtures. It appends `count` records, so
    /// the LEO of the partition advances to `count`.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct PartitionLogSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        partition: krabka_ids::PartitionIndex,
        #[default(RecordCount(0))]
        count: RecordCount,
    }

    #[bon::builder]
    fn partition_with_leo(
        log_dir: &Path,
        #[builder(default = "orders")] topic: &'_ str,
        #[builder(into, default)] partition: krabka_ids::PartitionIndex,
        #[builder(into, default = RecordCount (0))] count: RecordCount,
    ) -> Arc<Partition> {
        let setup = PartitionLogSetup {
            topic,
            partition,
            count,
        };
        let PartitionLogSetup {
            topic,
            partition,
            count,
        } = setup;
        let part = open_partition()
            .log_dir(log_dir)
            .topic(topic)
            .partition(partition.get())
            .call();
        if count.0 > 0 {
            append_n(&part.log, count);
        }
        part
    }

    /// Appends one batch of `count` records to a `Log` behind a mutex.
    ///
    /// The LEO of the log advances by `count`.
    fn append_n(log: &Mutex<krabka_log::Log>, count: RecordCount) {
        let mut batch = repeated_records_batch()
            .count(count)
            .timestamp(1_700_000_000)
            .call();
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .append(&mut batch)
            .expect("append records");
    }

    struct LogDirectoryLagScenario {
        dirs: Vec<tempfile::TempDir>,
        registry: PartitionRegistry,
        current: Option<Arc<Partition>>,
        future_logs: dashmap::DashMap<(String, krabka_ids::PartitionIndex), Arc<FutureLogState>>,
        lag: Option<i64>,
    }
    #[rstest::fixture]
    fn directory_lag() -> LogDirectoryLagScenario {
        let registry = PartitionRegistry::new();
        LogDirectoryLagScenario {
            dirs: Vec::new(),
            registry,
            current: None,
            future_logs: dashmap::DashMap::new(),
            lag: None,
        }
    }
    #[rstest_bdd_macros::given(
        "the current local log for topic t partition {partition} has five records"
    )]
    fn current_log_leo(directory_lag: &mut LogDirectoryLagScenario, partition: i32) {
        let dir = tempfile::tempdir().unwrap();
        let part = partition_with_leo()
            .log_dir(dir.path())
            .topic("t")
            .partition(partition)
            .count(5)
            .call();
        directory_lag.registry.insert(
            "t".into(),
            krabka_ids::PartitionIndex(partition),
            Arc::clone(&part),
        );
        directory_lag.current = Some(part);
        directory_lag.dirs.push(dir);
    }
    #[rstest_bdd_macros::given(
        "the future log for topic {topic} partition 3 has {records} records"
    )]
    fn future_log_leo(directory_lag: &mut LogDirectoryLagScenario, topic: String, records: i32) {
        let dir = tempfile::tempdir().unwrap();
        directory_lag.future_logs.insert(
            (topic, krabka_ids::PartitionIndex(3)),
            future_state_with_leo(dir.path(), RecordCount(records)),
        );
        directory_lag.dirs.push(dir);
    }
    #[rstest_bdd_macros::when("the current log lag is requested for topic {topic} partition 0")]
    async fn current_log_lag(directory_lag: &mut LogDirectoryLagScenario, topic: String) {
        directory_lag.lag = Some(offset_lag_for(&directory_lag.registry, &topic, 0).await);
    }
    #[rstest_bdd_macros::when("the future log lag is requested for topic {topic} partition 3")]
    fn future_log_lag(directory_lag: &mut LogDirectoryLagScenario, topic: String) {
        directory_lag.lag = Some(future_offset_lag(
            &directory_lag.registry,
            &directory_lag.future_logs,
            &topic,
            krabka_ids::PartitionIndex(3),
        ));
    }
    #[rstest_bdd_macros::then("the log lag is {expected} and the current log end remains 5")]
    fn check_log_lag(directory_lag: &LogDirectoryLagScenario, expected: i64) {
        assert!(directory_lag.current.as_ref().unwrap().log_end_offset() == krabka_log::Offset(5));
        assert2::check!(directory_lag.lag.unwrap() == expected);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_log_directory_lag/lag.feature",
        name = "Current lag is unknown without a local log and otherwise clamps at zero"
    )]
    #[tokio::test]
    async fn offset_lag_matches_kafka(
        directory_lag: LogDirectoryLagScenario,
        topic: String,
        expected: i64,
    ) {
        let _ = (directory_lag, topic, expected);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_log_directory_lag/lag.feature",
        name = "Future lag compares current and future ends without clamping"
    )]
    #[tokio::test]
    async fn future_offset_lag_matches_kafka(
        directory_lag: LogDirectoryLagScenario,
        topic: String,
        records: i32,
        expected: i64,
    ) {
        let _ = (directory_lag, topic, records, expected);
    }

    /// Builds a `FutureLogState` whose future log has LEO `future_count`.
    fn future_state_with_leo(dir: &Path, future_count: RecordCount) -> Arc<FutureLogState> {
        let future_path = dir.join("future");
        fs::create_dir_all(&future_path).unwrap();
        let flog = krabka_log::Log::open(&future_path, krabka_log::LogConfig::default()).unwrap();
        let future_log = Arc::new(Mutex::new(flog));
        if future_count.0 > 0 {
            append_n(&future_log, future_count);
        }
        Arc::new(FutureLogState {
            target_log_dir: dir.to_path_buf(),
            future_path,
            future_log,
            cancel: CancellationToken::new(),
            task: Mutex::new(None::<JoinHandle<()>>),
        })
    }
}
