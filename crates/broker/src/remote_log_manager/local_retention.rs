//! Local-retention eviction: which copied sealed segments a replica may drop
//! from its own disk once the remote tier holds them.
//!
//! Leader and follower alike run this pass. What makes a sealed segment
//! droppable is the RLMM saying the leader finished copying the offsets it
//! holds, and the RLMM is shared, so a follower reaches the same answer over
//! its own disk that the leader reaches over the leader's.
//!
//! The question is asked in offsets, never in segment boundaries. A replica
//! rolls its own segments, so a follower's segment need not line up with the
//! leader's; [`remote_covered_through`] turns the RLMM listing into the one
//! offset the tier holds an unbroken copy through, and nothing past it is
//! droppable on any replica.
//!
//! The pure walk that picks the deletion target sits beside the pass that
//! applies it, because the two share one contiguous-prefix rule.

use std::sync::Arc;

use krabka_log::{LogConfig, Offset, SegmentExport};
use krabka_remote_storage::{RemoteLogMetadataManager, RemoteLogSegmentState, TopicIdPartition};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use krabka_verified::retention::{
    LocalRetentionSegment, local_retention_prefix, retention_delete_target,
};
use tracing::{debug, warn};

use crate::{api_catalog::UnstableApiVersions, partition::Partition};

/// The offset through which the remote tier holds an unbroken copy of this
/// partition, given the `(start, end)` range of every `CopySegmentFinished`
/// segment and the base offset of the replica's oldest local segment.
/// Returns `None` when the remote tier does not reach `local_start` at all.
///
/// This is Kafka's `UnifiedLog.highestOffsetInRemoteStorage()`, the bound
/// `RLMFollowerTask` keeps current on a follower by reading the RLMM. Local
/// retention needs it because **a replica's segment boundaries are its own**:
/// a follower rolls on its own `segment.bytes` as it appends what it fetched,
/// so a leader segment copied as 0..=99 says nothing about a follower segment
/// spanning 0..=199. Matching a local segment's base offset against a remote
/// start offset would call that follower segment copied and delete 100..=199
/// with no remote copy anywhere; a failover in that window would lose
/// acknowledged records. An offset-range bound holds whatever the boundaries.
///
/// The walk stops at the first gap rather than taking the maximum end, because
/// a copy that failed between two that succeeded leaves a hole, and the
/// segments past it cover none of it.
pub(crate) fn remote_covered_through(finished: &[(i64, i64)], local_start: i64) -> Option<i64> {
    let mut ranges: Vec<(i64, i64)> = finished.to_vec();
    ranges.sort_unstable();
    krabka_verified::retention::remote_covered_through(&ranges, local_start)
}

/// Compute the highest `target` to pass to
/// [`krabka_log::Log::delete_local_segments_through`] given the
/// partition's local sealed-segment exports, the size of its whole local log
/// (active segment included), and the per-topic local-retention settings.
/// Returns `None` when nothing is deletable.
///
/// The rule is Kafka's `UnifiedLog.deleteOldSegments` on a tiered log,
/// proved in [`krabka_verified::retention::local_retention_prefix`]. A
/// segment can go only when the remote tier covers it whole, that is, its
/// `last_offset` is at or below `covered_through` (see
/// [`remote_covered_through`]); the walk stops at the first segment the tier
/// does not cover, so the local prefix stays contiguous. Kafka runs
/// `local.retention.bytes` first: it deletes an oldest segment only while
/// the local log's bytes over the budget still cover the whole segment, and
/// only when the local log is at least its budget. Then
/// `local.retention.ms` deletes the prefix with
/// `now_ms - anchor > effective_local`, where the anchor is the segment's
/// `largestTimestamp()` ([`SegmentExport::max_timestamp`]). A segment whose
/// records claim a timestamp in the future has a negative age and is never
/// time-expired, which is what Kafka 4.3.1's
/// `UnifiedLog.deleteRetentionMsBreachedSegments` does (it only logs that the
/// segment is "ineligible to be deleted"). Kafka trunk (KAFKA-20609) ages such
/// a segment of a tiered topic by its file's `lastModified()` instead, so
/// producer clock skew cannot pin local disk forever; that anchor applies only
/// under `unstable.api.versions.enable`, which is what `unstable` carries.
///
/// Kafka's walk ends at the active segment, which is never in the remote
/// tier and so never eligible (`isSegmentEligibleForDeletion`); the walk here
/// ends there too. Kafka also rolls that active segment when it breaches the
/// time or size predicate, so the next copy can upload it. This host does not:
/// an active segment waits for `segment.bytes` or `segment.ms` to roll it.
pub(crate) fn local_retention_target_under(
    unstable: UnstableApiVersions,
    exports: &[SegmentExport],
    covered_through: Option<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    local_log_size: ByteSize,
    now_ms: i64,
) -> Option<i64> {
    let size_debt = effective_local_size
        .and_then(|budget| local_log_size.bytes_u64().checked_sub(budget.bytes_u64()));
    let mut facts: Vec<LocalRetentionSegment> = exports
        .iter()
        .map(|ex| {
            let anchor = match unstable {
                UnstableApiVersions::Enabled if now_ms < ex.max_timestamp => ex.last_modified_ms,
                _ => ex.max_timestamp,
            };
            let age = Time::from_millis(now_ms.saturating_sub(anchor));
            LocalRetentionSegment {
                blocked: !matches!(covered_through, Some(through) if ex.last_offset.0 <= through),
                expired: matches!(effective_local, Some(retention) if age > retention),
                size: ex.size.bytes_u64(),
            }
        })
        .collect();
    // The active segment ends the walk: the remote tier never holds it, and
    // `delete_local_segments_through` never removes it.
    let sealed_size = exports
        .iter()
        .fold(ByteSize::from_bytes(0), |total, ex| total + ex.size);
    facts.push(LocalRetentionSegment {
        blocked: true,
        expired: false,
        size: local_log_size
            .bytes_u64()
            .saturating_sub(sealed_size.bytes_u64()),
    });
    let len = local_retention_prefix(&facts, size_debt);
    let last_offset = len.checked_sub(1).map(|index| exports[index].last_offset.0);
    retention_delete_target(last_offset)
}

/// [`local_retention_target_under`] for Kafka 4.3.1's behavior, which is what
/// a broker runs unless `unstable.api.versions.enable` is set.
#[cfg(test)]
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    covered_through: Option<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    local_log_size: ByteSize,
    now_ms: i64,
) -> Option<i64> {
    local_retention_target_under(
        UnstableApiVersions::Disabled,
        exports,
        covered_through,
        effective_local,
        effective_local_size,
        local_log_size,
        now_ms,
    )
}

/// After the copy pass, drop local sealed segments whose
/// remote copy is `CopySegmentFinished` and that fall outside the
/// per-topic local-retention window. Returns the count of segments
/// that this pass physically removed from disk.
///
/// This runs on every replica of a tiered partition. On a follower the copy
/// pass belongs to another broker, so `rlmm` is the only thing that says a
/// segment is safe to drop -- which is exactly what it says on the leader too.
/// It says it in offsets: see [`remote_covered_through`] for why a follower
/// cannot read a remote segment's boundaries as its own.
pub(crate) fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    log_config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
    unstable: UnstableApiVersions,
) -> usize {
    // Trunk ages a future-timestamped segment by its file only while the tier
    // still takes copies (`remoteLogEnabledAndRemoteCopyEnabled`).
    let unstable = if log_config.remote_tier.copy_disable {
        UnstableApiVersions::Disabled
    } else {
        unstable
    };
    let effective_local = log_config.local_retention.or(log_config.retention);
    let effective_local_size = log_config
        .local_retention_size
        .or(log_config.retention_size);

    let Some(local_start) = exports.first().map(|ex| ex.base_offset.0) else {
        return 0;
    };
    let finished: Vec<(i64, i64)> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(|md| (md.start_offset(), md.end_offset()))
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for local retention");
            return 0;
        }
    };
    let covered_through = remote_covered_through(&finished, local_start);

    let (target, result) = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        let Some(target) = local_retention_target_under(
            unstable,
            exports,
            covered_through,
            effective_local,
            effective_local_size,
            log.size(),
            now_ms,
        ) else {
            return 0;
        };
        (target, log.delete_local_segments_through(Offset(target)))
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
    use krabka_ids::{LeaderEpoch, PartitionIndex};
    use krabka_log::Log;
    use krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteStorageManager,
    };
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::remote_log_manager::{
        ArchiveMode, copy_eligible, now_ms,
        test_support::{
            FakeWormArchive, batch, leading_partition_over, rolled_tiered_partition_with_config,
            synth_export, tier, tp,
        },
    };

    #[test]
    fn local_retention_target_returns_none_when_no_finished_segments() {
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        // Big enough time-pressure to delete everything, but the remote tier
        // covers nothing.
        assert!(
            local_retention_target(&exports, None, Some(millis(1)), None, bytes(128), 10_000)
                == None
        );
    }

    /// Kafka 4.3.1's `UnifiedLog.deleteRetentionMsBreachedSegments` ages a
    /// segment by its `largestTimestamp()` alone: one whose records claim a
    /// timestamp in the future is never time-expired, and the log only says it
    /// is "ineligible to be deleted". Kafka trunk (KAFKA-20609) ages such a
    /// segment of a tiered topic by its file's `lastModified()` instead, and
    /// only under `unstable.api.versions.enable` does krabka follow it. `now`
    /// is 10 000 ms and the window 1 ms; the segment is fully copied.
    ///
    /// Each case is `(name, newest timestamp, file last modified, target in
    /// the default mode, target under the unstable flag)`.
    #[test]
    fn a_future_timestamp_is_aged_by_the_file_only_under_the_unstable_flag() {
        for (name, max_timestamp, last_modified_ms, default_target, unstable_target) in [
            (
                "a past timestamp ages the segment either way",
                100,
                100,
                Some(10),
                Some(10),
            ),
            (
                "a future timestamp with an old file",
                20_000,
                100,
                None,
                Some(10),
            ),
            (
                "a future timestamp with a young file",
                20_000,
                9_999,
                None,
                None,
            ),
        ] {
            let exports = vec![SegmentExport {
                last_modified_ms,
                ..synth_export(0, 9, max_timestamp, 100)
            }];
            for (unstable, expected) in [
                (UnstableApiVersions::Disabled, default_target),
                (UnstableApiVersions::Enabled, unstable_target),
            ] {
                check!(
                    local_retention_target_under(
                        unstable,
                        &exports,
                        Some(9),
                        Some(millis(1)),
                        None,
                        bytes(100),
                        10_000
                    ) == expected,
                    "{name}: {unstable:?}"
                );
            }
        }
    }

    #[test]
    fn maximum_retention_window_keeps_the_host_time_comparison() {
        let exports = vec![synth_export(0, 9, 0, 100)];

        check!(
            local_retention_target(
                &exports,
                Some(9),
                Some(Time::from_millis(i64::MAX)),
                None,
                bytes(100),
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
        // now=1000, retention=500ms → segs with max_ts<500 are deletable.
        // Only seg0 (max_ts=100) and seg1 (max_ts=200) qualify; seg2 stops it.
        let target = local_retention_target(
            &exports,
            Some(29),
            Some(millis(500)),
            None,
            bytes(192),
            1_000,
        );
        assert!(target == Some(20));
    }

    /// Kafka's `deleteRetentionSizeBreachedSegments` on a tiered log: an oldest
    /// segment goes only while the local log's bytes over
    /// `local.retention.bytes` still cover the whole segment, and the active
    /// segment counts toward the local log's size. Three 100-byte sealed
    /// segments and a 50-byte active segment make a 350-byte local log.
    #[test]
    fn local_retention_target_size_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 100),
            synth_export(10, 19, 200, 100),
            synth_export(20, 29, 300, 100),
        ];
        let cases = [
            ("200 over deletes two", bytes(150), Some(20)),
            ("150 over deletes one, not two", bytes(200), Some(10)),
            ("the active segment's bytes count", bytes(250), Some(10)),
            ("50 over deletes nothing", bytes(300), None),
            ("exactly at budget deletes nothing", bytes(350), None),
            ("300 over deletes all three", bytes(50), Some(30)),
            ("under budget deletes nothing", bytes(10_000), None),
        ];
        for (name, budget, expected) in cases {
            let target =
                local_retention_target(&exports, Some(29), None, Some(budget), bytes(350), 1_000);
            check!(target == expected, "{name}");
        }
    }

    #[test]
    fn local_retention_target_equal_size_budget_keeps_all_segments() {
        let exports = vec![synth_export(0, 9, 100, 100), synth_export(10, 19, 200, 100)];
        let target = local_retention_target(
            &exports,
            Some(19),
            None,
            Some(bytes(200)),
            bytes(200),
            1_000,
        );
        assert!(target == None);
    }

    #[test]
    fn local_retention_target_skips_unfinished_segments_and_stops() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];
        // The tier holds 0..=9 and 20..=29 but not 10..=19, so its unbroken
        // cover ends at 9 and the walk stops at seg1.
        let covered = remote_covered_through(&[(0, 9), (20, 29)], 0);
        assert!(covered == Some(9));
        let target =
            local_retention_target(&exports, covered, Some(millis(1)), None, bytes(192), 10_000);
        assert!(
            target == Some(10),
            "only seg0 deletable; walk stops at seg1"
        );
    }

    /// One [`remote_covered_through`] case: an RLMM listing, the base offset
    /// of the replica's oldest local segment, and the bound they imply.
    struct CoverCase {
        label: &'static str,
        finished: &'static [(i64, i64)],
        local_start: i64,
        expected: Option<i64>,
    }

    #[test]
    fn remote_covered_through_walks_an_unbroken_prefix() {
        let cases = [
            CoverCase {
                label: "obsolete disconnected prefix cannot suppress the local anchor",
                finished: &[(0, 9), (20, 29)],
                local_start: 20,
                expected: Some(29),
            },
            CoverCase {
                label: "obsolete ranges alone do not cover the local anchor",
                finished: &[(0, 9)],
                local_start: 20,
                expected: None,
            },
            CoverCase {
                label: "nothing copied",
                finished: &[],
                local_start: 0,
                expected: None,
            },
            CoverCase {
                label: "one segment from the local start",
                finished: &[(0, 9)],
                local_start: 0,
                expected: Some(9),
            },
            CoverCase {
                label: "abutting segments join, in any listed order",
                finished: &[(20, 29), (0, 9), (10, 19)],
                local_start: 0,
                expected: Some(29),
            },
            CoverCase {
                label: "a gap stops the walk, and a later segment cannot bridge it",
                finished: &[(0, 9), (20, 29)],
                local_start: 0,
                expected: Some(9),
            },
            CoverCase {
                label: "the tier starts past the oldest local segment",
                finished: &[(10, 19)],
                local_start: 0,
                expected: None,
            },
            CoverCase {
                label: "remote retention dropped the head, but the local log moved too",
                finished: &[(10, 19), (20, 29)],
                local_start: 10,
                expected: Some(29),
            },
        ];
        for case in cases {
            check!(
                remote_covered_through(case.finished, case.local_start) == case.expected,
                "{}",
                case.label
            );
        }
    }

    #[test]
    fn a_local_segment_the_tier_covers_only_in_part_is_not_droppable() {
        // The follower rolled one 0..=199 segment where the leader rolled two,
        // and the leader has copied only the first of them. Reading the remote
        // start offset 0 as "this local segment is copied" would delete
        // 100..=199, which no remote segment holds; a failover in that window
        // would lose acknowledged records.
        let exports = vec![
            synth_export(0, 199, 100, 64),
            synth_export(200, 399, 200, 64),
        ];
        let covered = remote_covered_through(&[(0, 99)], 0);
        check!(covered == Some(99));
        check!(
            local_retention_target(&exports, covered, Some(millis(1)), None, bytes(128), 10_000)
                == None
        );

        // Once the leader copies 100..=199 too, the follower's first segment is
        // covered whole and goes.
        let covered = remote_covered_through(&[(0, 99), (100, 199)], 0);
        check!(covered == Some(199));
        check!(
            local_retention_target(&exports, covered, Some(millis(1)), None, bytes(128), 10_000)
                == Some(200)
        );
    }

    #[test]
    fn local_retention_target_rejects_exhausted_offset() {
        let exports = vec![synth_export(0, i64::MAX, 100, 64)];

        assert!(
            local_retention_target(
                &exports,
                Some(i64::MAX),
                Some(millis(1)),
                None,
                bytes(64),
                10_000
            ) == None
        );
    }

    #[test]
    fn local_retention_target_uses_already_resolved_effective_ms() {
        // The pure helper takes already-resolved effective_* args. This test
        // pins that contract: when caller passes effective_local_ms equal to
        // the topic's `retention` (the fallback), the helper deletes the
        // same set as if `local_retention` had been set directly.
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        // Caller resolved effective_local = retention = 250ms; now=1000.
        let target = local_retention_target(
            &exports,
            Some(19),
            Some(millis(250)),
            None,
            bytes(128),
            1_000,
        );
        assert!(target == Some(20));
    }

    /// Test-only drive helper. It mirrors the body of `local_retention_pass`
    /// without the `Partition` wrapper, so the test can exercise the
    /// integration against a real `Log` and no broker fixtures.
    fn local_retention_drive(
        log: &mut Log,
        finished: &[(i64, i64)],
        log_config: &LogConfig,
        now_ms: i64,
    ) -> usize {
        let effective_local = log_config.local_retention.or(log_config.retention);
        let effective_local_size = log_config
            .local_retention_size
            .or(log_config.retention_size);
        let exports = log.tierable_segments();
        let Some(local_start) = exports.first().map(|ex| ex.base_offset.0) else {
            return 0;
        };
        let Some(target) = local_retention_target(
            &exports,
            remote_covered_through(finished, local_start),
            effective_local,
            effective_local_size,
            log.size(),
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
        log.sync().expect("flush sealed segments before archiving");
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");
        let log_config = log.config_snapshot();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
        )
        .await;
        assert!(copied == exports.len());

        // Gather finished ranges the same way `local_retention_pass` would.
        let finished: Vec<(i64, i64)> = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(|md| (md.start_offset(), md.end_offset()))
            .collect();
        assert!(finished.len() == exports.len());

        // Drive retention with `now_ms` far in the future so every sealed
        // segment satisfies the 1ms time-based eviction.
        let future = now_ms() + 1_000_000;
        let removed = local_retention_drive(&mut log, &finished, &log_config, future);
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
        let removed_again = local_retention_drive(&mut log, &finished, &log_config, future);
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
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
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
            crate::api_catalog::UnstableApiVersions::Disabled,
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
            &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
            &tp(),
            1,
            LeaderEpoch(0),
            exports.clone(),
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
            crate::api_catalog::UnstableApiVersions::Disabled,
        );

        check!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        check!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        check!(log.tierable_segments().is_empty());
    }

    /// A tiered topic whose producers stamp records far in the future keeps
    /// its copied segments on local disk under Kafka 4.3.1, which never
    /// time-expires them. Kafka trunk (KAFKA-20609) ages them by their file's
    /// `lastModified()`, and only under `unstable.api.versions.enable`, and
    /// only while the tier still takes copies (`remoteLogEnabledAndRemoteCopyEnabled`).
    ///
    /// Each case is `(name, unstable flag, remote.log.copy.disable, whether the
    /// copied segments are evicted)`.
    #[tokio::test]
    async fn future_stamped_segments_leave_the_disk_only_under_trunks_rule() {
        for (name, unstable, copy_disable, evicted) in [
            (
                "4.3.1 keeps them",
                UnstableApiVersions::Disabled,
                false,
                false,
            ),
            (
                "trunk ages them by their file",
                UnstableApiVersions::Enabled,
                false,
                true,
            ),
            (
                "trunk keeps them when the tier takes no copies",
                UnstableApiVersions::Enabled,
                true,
                false,
            ),
        ] {
            let log_dir = tempfile::tempdir().unwrap();
            let remote_dir = tempfile::tempdir().unwrap();
            let part_dir = crate::log_dir::partition_dir(log_dir.path(), "orders", 0);
            std::fs::create_dir_all(&part_dir).unwrap();
            let mut log = Log::open(
                &part_dir,
                LogConfig {
                    segment_size: bytes(256),
                    remote_storage_enable: true,
                    local_retention: Some(millis(1)),
                    remote_tier: krabka_log::RemoteTierFlags {
                        copy_disable,
                        ..krabka_log::RemoteTierFlags::DEFAULT
                    },
                    ..LogConfig::default()
                },
            )
            .unwrap();
            // Every record claims a timestamp a million seconds ahead.
            let future = now_ms() + 1_000_000_000;
            for _ in 0..12 {
                let mut future_batch = batch(2);
                future_batch.base_timestamp = future;
                future_batch.max_timestamp = future;
                log.append(&mut future_batch).unwrap();
            }
            log.sync().unwrap();
            let partition = leading_partition_over(PartitionIndex(0), log_dir.path(), log);
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
                &tier(ArchiveMode::Mutable, &rsm, &rlmm),
                &tp(),
                1,
                LeaderEpoch(0),
                exports.clone(),
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
                unstable,
            );

            check!(
                removed == if evicted { exports.len() } else { 0 },
                "{name}: removed {removed} of {}",
                exports.len()
            );
        }
    }
}
