//! Local-retention eviction: which copied sealed segments a leader may drop
//! from its own disk once the remote tier holds them.
//!
//! The pure walk that picks the deletion target sits beside the pass that
//! applies it, because the two share one contiguous-prefix rule.

use std::{collections::HashSet, sync::Arc};

use krabka_log::{LogConfig, Offset, SegmentExport};
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tracing::{debug, warn};

use super::NO_BYTES;
use crate::partition::Partition;

/// Compute the highest `target` to pass to
/// [`krabka_log::Log::delete_local_segments_through`] given the
/// partition's local sealed-segment exports and the per-topic
/// local-retention settings. Returns `None` when nothing is deletable.
///
/// A segment is eligible if and only if its `base_offset` is in
/// `finished_bases`, that is, `CopySegmentFinished` in the RLMM, AND it meets
/// either time-based eviction (`now_ms - seg.max_timestamp > effective_local`)
/// or size-based eviction (oldest-first until the sealed total fits
/// `effective_local_size`). The walk stops at the first non-finished
/// segment, so the local prefix stays contiguous. This matches Kafka.
///
/// Size-based eviction ignores the active segment. Operators set
/// local.retention.bytes in MB or GB ranges, where the active segment,
/// bounded by `segment.bytes`, is negligible.
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    finished_bases: &HashSet<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    now_ms: i64,
) -> Option<i64> {
    let sealed_total: ByteSize = exports
        .iter()
        .map(|e| e.size)
        .fold(NO_BYTES, |acc, size| acc + size);
    let deletable_size_remaining =
        effective_local_size.map_or(NO_BYTES, |budget| (sealed_total - budget).max(NO_BYTES));
    let finished: Vec<bool> = exports
        .iter()
        .map(|ex| finished_bases.contains(&ex.base_offset.0))
        .collect();
    let time_expired: Vec<bool> = exports
        .iter()
        .map(|ex| {
            let age = Time::from_millis(now_ms.saturating_sub(ex.max_timestamp));
            ex.max_timestamp != -1 && matches!(effective_local, Some(retention) if age > retention)
        })
        .collect();
    let sizes: Vec<u64> = exports.iter().map(|ex| ex.size.bytes_u64()).collect();
    let prefix = krabka_verified::retention::retention_prefix(
        true,
        &finished,
        &time_expired,
        &sizes,
        deletable_size_remaining.bytes_u64(),
    );
    let last_offset = prefix
        .len
        .checked_sub(1)
        .map(|index| exports[index].last_offset.0);
    krabka_verified::retention::retention_delete_target(last_offset)
}

/// After the copy pass, drop local sealed segments whose
/// remote copy is `CopySegmentFinished` and that fall outside the
/// per-topic local-retention window. Returns the count of segments
/// that this pass physically removed from disk.
pub(crate) fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    log_config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    let effective_local = log_config.local_retention.or(log_config.retention);
    let effective_local_size = log_config
        .local_retention_size
        .or(log_config.retention_size);

    let finished_bases: HashSet<i64> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for local retention");
            return 0;
        }
    };

    let Some(target) = local_retention_target(
        exports,
        &finished_bases,
        effective_local,
        effective_local_size,
        now_ms,
    ) else {
        return 0;
    };

    let result = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        log.delete_local_segments_through(Offset(target))
    };
    match result {
        Ok(n) => {
            debug!(topic = %tp.topic, partition = tp.partition, target, removed = n,
                   "remote-log-manager: local-retention deletion pass completed");
            n
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, target, error = %e,
                  "remote-log-manager: failed to delete local segments");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_log::Log;
    use krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteStorageManager,
    };
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::remote_log_manager::{
        ArchiveMode, copy_eligible, now_ms,
        test_support::{
            FakeWormArchive, batch, rolled_tiered_partition_with_config, synth_export, tp,
        },
    };

    #[test]
    fn local_retention_target_returns_none_when_no_finished_segments() {
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = HashSet::new();
        // Big enough time-pressure to delete everything, but nothing is finished.
        assert!(local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000) == None);
    }

    #[test]
    fn unknown_timestamp_needs_size_pressure_for_local_eviction() {
        let exports = vec![synth_export(0, 9, -1, 100)];
        let finished = maplit::hashset! {0};

        check!(local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000) == None);
        check!(
            local_retention_target(&exports, &finished, Some(millis(1)), Some(bytes(0)), 10_000,)
                == Some(10)
        );
    }

    #[test]
    fn maximum_retention_window_keeps_the_host_time_comparison() {
        let exports = vec![synth_export(0, 9, 0, 100)];
        let finished = maplit::hashset! {0};

        check!(
            local_retention_target(
                &exports,
                &finished,
                Some(Time::from_millis(i64::MAX)),
                None,
                i64::MAX,
            ) == None
        );
    }

    #[test]
    fn local_retention_target_time_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 5_000, 64),
        ];
        let finished: HashSet<i64> = maplit::hashset! {0, 10, 20};
        // now=1000, retention=500ms → segs with max_ts<500 are deletable.
        // Only seg0 (max_ts=100) and seg1 (max_ts=200) qualify; seg2 stops it.
        let target = local_retention_target(&exports, &finished, Some(millis(500)), None, 1_000);
        assert!(target == Some(20));
    }

    #[test]
    fn local_retention_target_size_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 100),
            synth_export(10, 19, 200, 100),
            synth_export(20, 29, 300, 100),
        ];
        let finished: HashSet<i64> = maplit::hashset! {0, 10, 20};
        let cases = [
            // Total = 300; budget = 150 → must evict 150 bytes → oldest two go.
            (Some(bytes(150)), Some(20)),
            // Budget tighter than one segment: still only the oldest, because
            // after evicting 100B the remaining is 100 (>budget? no, 200>150,
            // wait: total=300, budget=150 → need to evict 150; after dropping
            // first 100B we still need 50 more → second segment also drops.
            // Test with budget = 50: need to evict 250 → all three? but the
            // walk stops since segments 0..=2 all become deletable.
            (Some(bytes(50)), Some(30)),
            // Budget larger than total → nothing deletable.
            (Some(bytes(10_000)), None),
        ];
        for (budget, expected) in cases {
            let target = local_retention_target(&exports, &finished, None, budget, 1_000);
            assert!(target == expected, "budget: {budget:?}");
        }
    }

    #[test]
    fn local_retention_target_equal_size_budget_keeps_all_segments() {
        let exports = vec![synth_export(0, 9, 100, 100), synth_export(10, 19, 200, 100)];
        let finished: HashSet<i64> = maplit::hashset! {0, 10};
        let target = local_retention_target(&exports, &finished, None, Some(bytes(200)), 1_000);
        assert!(target == None);
    }

    #[test]
    fn local_retention_target_skips_unfinished_segments_and_stops() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];
        // Segment at base=10 has NOT been copy-finished. Walk stops there.
        let finished: HashSet<i64> = maplit::hashset! {0, 20};
        let target = local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000);
        assert!(
            target == Some(10),
            "only seg0 deletable; walk stops at seg1"
        );
    }

    #[test]
    fn local_retention_target_rejects_exhausted_offset() {
        let exports = vec![synth_export(0, i64::MAX, 100, 64)];
        let finished = maplit::hashset! {0};

        assert!(local_retention_target(&exports, &finished, Some(millis(1)), None, 10_000) == None);
    }

    #[test]
    fn local_retention_target_uses_already_resolved_effective_ms() {
        // The pure helper takes already-resolved effective_* args. This test
        // pins that contract: when caller passes effective_local_ms equal to
        // the topic's `retention` (the fallback), the helper deletes the
        // same set as if `local_retention` had been set directly.
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = maplit::hashset! {0, 10};
        // Caller resolved effective_local = retention = 250ms; now=1000.
        let target = local_retention_target(&exports, &finished, Some(millis(250)), None, 1_000);
        assert!(target == Some(20));
    }

    /// Test-only drive helper. It mirrors the body of `local_retention_pass`
    /// without the `Partition` wrapper, so the test can exercise the
    /// integration against a real `Log` and no broker fixtures.
    fn local_retention_drive(
        log: &mut Log,
        finished_bases: &HashSet<i64>,
        log_config: &LogConfig,
        now_ms: i64,
    ) -> usize {
        let effective_local = log_config.local_retention.or(log_config.retention);
        let effective_local_size = log_config
            .local_retention_size
            .or(log_config.retention_size);
        let exports = log.tierable_segments();
        let Some(target) = local_retention_target(
            &exports,
            finished_bases,
            effective_local,
            effective_local_size,
            now_ms,
        ) else {
            return 0;
        };
        log.delete_local_segments_through(Offset(target)).unwrap()
    }

    #[tokio::test]
    async fn local_retention_drive_deletes_copied_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");
        let log_config = log.config_snapshot();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        // Gather finished bases the same way `local_retention_pass` would.
        let finished_bases: HashSet<i64> = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect();
        assert!(finished_bases.len() == exports.len());

        // Drive retention with `now_ms` far in the future so every sealed
        // segment satisfies the 1ms time-based eviction.
        let future = now_ms() + 1_000_000;
        let removed = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed == exports.len());

        // local_log_start_offset advanced; sealed log files are gone.
        let last = exports.last().unwrap().last_offset;
        assert!(log.local_log_start_offset() == last + 1);
        for ex in &exports {
            assert!(
                !ex.log_path.exists(),
                "sealed segment {:?} should be deleted",
                ex.log_path
            );
        }
        // Re-running is a no-op.
        let removed_again = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed_again == 0);
    }

    #[tokio::test]
    async fn local_retention_pass_deletes_finished_segments_and_returns_count() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = {
            let log = partition.log.lock().expect("partition log mutex poisoned");
            (log.tierable_segments(), log.config_snapshot())
        };
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::Mutable,
            &rsm,
            &rlmm,
        )
        .await;
        assert!(copied == exports.len());

        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            now_ms() + 1_000_000,
        );

        assert!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        assert!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        assert!(log.tierable_segments().is_empty());
    }

    #[tokio::test]
    async fn local_retention_still_evicts_under_a_write_once_archive() {
        let log_dir = tempfile::tempdir().unwrap();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = {
            let log = partition.log.lock().expect("partition log mutex poisoned");
            (log.tierable_segments(), log.config_snapshot())
        };
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
            ArchiveMode::WriteOnce,
            &rsm,
            &rlmm,
        )
        .await;
        check!(copied == exports.len());

        // Archiving a segment is exactly what makes its local copy droppable.
        // A write-once remote tier does not change that: local retention
        // deletes local files and never touches the archive.
        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            now_ms() + 1_000_000,
        );

        check!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        check!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        check!(log.tierable_segments().is_empty());
    }
}
