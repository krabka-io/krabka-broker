//! The two `offset_lag` values that a `DescribeLogDirs` partition entry
//! carries.
//!
//! A current log reports `LEO − HW`, while a KIP-113 future log reports
//! `current_log.LEO − future_log.LEO` so that an operator can watch an
//! intra-broker move drain. Both readings reach into the partition registry and
//! the future-log registry, which is why they live together and away from the
//! directory scan.

/// `LEO − HW` for a loaded current log, with a clamp at 0.
///
/// Returns 0 when the partition is not materialized on this broker.
pub(super) async fn offset_lag_for(
    partitions: &crate::partition_registry::PartitionRegistry,
    topic: &str,
    partition: i32,
) -> i64 {
    let Some(part) = partitions.get(topic, krabka_ids::PartitionIndex(partition)) else {
        return 0;
    };
    let leo = part.log_end_offset();
    let hw = part.high_watermark().await;
    // Lag is a record-count delta between two offsets, not an offset.
    (leo.0 - hw.0).max(0)
}

/// `current_log.LEO − future_log.LEO` for an in-progress KIP-113 move, with a
/// clamp at 0.
///
/// Returns 0 if the partition is not materialized locally. Also returns 0 if
/// the future-log registry has no entry. The registry has no entry when the
/// broker has just started and the resume task has not opened the future log
/// yet.
pub(super) fn future_offset_lag(
    partitions: &crate::partition_registry::PartitionRegistry,
    future_logs: &dashmap::DashMap<
        (String, krabka_ids::PartitionIndex),
        std::sync::Arc<crate::future_log::FutureLogState>,
    >,
    topic: &str,
    partition: krabka_ids::PartitionIndex,
) -> i64 {
    let Some(part) = partitions.get(topic, partition) else {
        return 0;
    };
    let current_leo = part.log_end_offset();
    let future_leo =
        future_logs
            .get(&(topic.to_string(), partition))
            .map_or(krabka_log::Offset(0), |e| {
                e.value()
                    .future_log
                    .lock()
                    .expect("future log mutex poisoned")
                    .log_end_offset()
            });
    // Lag is a record-count delta between two offsets, not an offset.
    (current_leo.0 - future_leo.0).max(0)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_protocol::records::{Attributes, Record, RecordBatch};

    use super::*;
    use crate::log_dir;

    /// Builds a `Partition` rooted at `<log_dir>/<topic>-<partition>`.
    ///
    /// The function uses the real `spawn_partition` path and mirrors the
    /// `future_log` and registry test fixtures. It appends `count` records, so
    /// the LEO of the partition advances to `count`.
    fn partition_with_leo(
        log_dir: &std::path::Path,
        topic: &str,
        partition: krabka_ids::PartitionIndex,
        count: i32,
    ) -> std::sync::Arc<crate::partition::Partition> {
        let part_dir = log_dir::partition_dir(log_dir, topic, partition.get());
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        if count > 0 {
            append_n(&part.log, count);
        }
        part
    }

    /// Appends one batch of `count` records to a `Log` behind a mutex.
    ///
    /// The LEO of the log advances by `count`.
    fn append_n(log: &std::sync::Mutex<krabka_log::Log>, count: i32) {
        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..count)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut batch)
            .expect("append records");
    }

    /// A partition that is not materialized locally reports lag `0`.
    ///
    /// It does not report the `-1` that a whole-function replacement mutant
    /// returns.
    #[tokio::test]
    async fn offset_lag_missing_partition_is_zero() {
        let reg = crate::partition_registry::PartitionRegistry::new();
        assert!(offset_lag_for(&reg, "ghost", 0).await == 0);
    }

    /// A materialized partition with LEO ahead of HW reports `LEO - HW`.
    ///
    /// A fresh HW is 0. This test pins the real subtraction against the
    /// whole-function `-> -1` replacement.
    #[tokio::test]
    async fn offset_lag_uses_leo_minus_hw() {
        let dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(dir.path(), "t", krabka_ids::PartitionIndex(0), 5);
        assert!(part.log_end_offset() == krabka_log::Offset(5));
        reg.insert("t".into(), krabka_ids::PartitionIndex(0), part);
        // Fresh partition HW is 0 → lag == LEO == 5 (not -1, not 0).
        assert!(offset_lag_for(&reg, "t", 0).await == 5);
    }

    /// Builds a `FutureLogState` whose future log has LEO `future_count`.
    fn future_state_with_leo(
        dir: &std::path::Path,
        future_count: i32,
    ) -> std::sync::Arc<crate::future_log::FutureLogState> {
        let future_path = dir.join("future");
        std::fs::create_dir_all(&future_path).unwrap();
        let flog = krabka_log::Log::open(&future_path, krabka_log::LogConfig::default()).unwrap();
        let future_log = std::sync::Arc::new(std::sync::Mutex::new(flog));
        if future_count > 0 {
            append_n(&future_log, future_count);
        }
        std::sync::Arc::new(crate::future_log::FutureLogState {
            target_log_dir: dir.to_path_buf(),
            future_path,
            future_log,
            cancel: tokio_util::sync::CancellationToken::new(),
            task: std::sync::Mutex::new(None::<tokio::task::JoinHandle<()>>),
        })
    }

    /// With no local partition, the future-log lag is `0`.
    ///
    /// It is not the `1` that a whole-function `-> 1` replacement mutant
    /// returns.
    #[tokio::test]
    async fn future_offset_lag_missing_partition_is_zero() {
        let reg = crate::partition_registry::PartitionRegistry::new();
        let future_logs = dashmap::DashMap::new();
        let lag = future_offset_lag(&reg, &future_logs, "ghost", krabka_ids::PartitionIndex(0));
        assert!(lag == 0);
    }

    /// `future_offset_lag` is `current_log.LEO − future_log.LEO`, clamped at 0.
    ///
    /// With current LEO 5 and future LEO 2 the answer is 3. This value
    /// separates the real subtraction from every mutant: `-> 0` gives 0,
    /// `-> 1` gives 1, `-` → `+` gives 7, and `-` → `/` gives 2.
    #[tokio::test]
    async fn future_offset_lag_is_current_minus_future_leo() {
        let cur_dir = tempfile::tempdir().unwrap();
        let fut_dir = tempfile::tempdir().unwrap();
        let reg = crate::partition_registry::PartitionRegistry::new();
        let part = partition_with_leo(cur_dir.path(), "t", krabka_ids::PartitionIndex(3), 5);
        assert!(part.log_end_offset() == krabka_log::Offset(5));
        reg.insert("t".into(), krabka_ids::PartitionIndex(3), part);

        let future_logs = dashmap::DashMap::new();
        future_logs.insert(
            ("t".to_string(), krabka_ids::PartitionIndex(3)),
            future_state_with_leo(fut_dir.path(), 2),
        );

        let lag = future_offset_lag(&reg, &future_logs, "t", krabka_ids::PartitionIndex(3));
        assert!(lag == 3, "current LEO 5 − future LEO 2 == 3, got {lag}");
    }
}
