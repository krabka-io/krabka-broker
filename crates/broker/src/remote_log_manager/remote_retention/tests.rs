//! Unit tests for the remote-retention eviction set and its delete pass.

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
    LogSegmentData, RemoteLogSegmentId, RemoteLogSegmentMetadataUpdate, RemoteStorageError,
};
use krabka_units::{bytes, hours, millis};
use uuid::Uuid;

use super::*;
use crate::remote_log_manager::{
    copy_eligible, now_ms,
    test_support::{FakeWormArchive, rolled_log, seed_finished_segments, tier, tp},
};

/// A partition whose `DeleteRecords` floor has never moved: offset 0, so no
/// segment breaches it and the case under test is the only axis in play.
const NO_FLOOR: Offset = Offset(0);

/// An RSM that refuses every delete the way a WORM backend does, and
/// counts how many times it was asked. Modelled on [`AlwaysFailRsm`],
/// with the failure moved from the copy to the delete.
#[derive(Default)]
struct RefusesDeleteRsm {
    deletes_attempted: std::sync::atomic::AtomicUsize,
}

impl RemoteStorageManager for RefusesDeleteRsm {
    fn copy_log_segment_data(
        &self,
        _metadata: &RemoteLogSegmentMetadata,
        _data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        Ok(None)
    }
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _start: u32,
        _end: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        Err(RemoteStorageError::SegmentNotFound(
            metadata.remote_log_segment_id().clone(),
        ))
    }
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.deletes_attempted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(RemoteStorageError::Worm(
            krabka_remote_storage::WormError::DeleteRefused {
                key: format!("{}.log", metadata.remote_log_segment_id().id),
            },
        ))
    }
}

fn synth_remote_md(
    id: u128,
    start: i64,
    end: i64,
    max_ts: i64,
    size: i32,
) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        start,
        end,
        max_ts,
        1,
        max_ts,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => start},
        ),
    )
    .unwrap()
    .with_update(&RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        event_timestamp_ms: max_ts,
        custom_metadata: None,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 1,
    })
    .unwrap()
}

#[test]
fn remote_retention_eviction_set_returns_empty_when_no_segments() {
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &[],
        Some(millis(1)),
        Some(bytes(1)),
        None,
        10_000,
    );
    assert!(out.is_empty());
}

#[test]
fn unknown_timestamp_needs_size_pressure_for_remote_eviction() {
    let segments = vec![synth_remote_md(10, 0, 9, -1, 100)];

    check!(
        remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            Some(millis(1)),
            None,
            None,
            10_000,
        )
        .is_empty()
    );
    check!(
        remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            Some(millis(1)),
            Some(bytes(0)),
            None,
            10_000,
        )
        .len()
            == 1
    );
}

#[test]
fn maximum_retention_window_keeps_the_host_time_comparison() {
    let segments = vec![synth_remote_md(10, 0, 9, 0, 100)];

    check!(
        remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            Some(Time::from_millis(i64::MAX)),
            None,
            None,
            i64::MAX,
        )
        .is_empty()
    );
}

#[test]
fn remote_retention_eviction_set_time_based_picks_oldest_until_first_in_window() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),
        synth_remote_md(11, 10, 19, 200, 100),
        synth_remote_md(12, 20, 29, 9_500, 100),
    ];
    // now=10_000, retention=500ms → seg with max_ts < 9_500 is deletable.
    // seg0 (100) + seg1 (200) qualify; seg2 (9_500) stops the walk.
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        Some(millis(500)),
        None,
        None,
        10_000,
    );
    assert!(out.len() == 2);
    check!(out[0].start_offset() == 0);
    check!(out[1].start_offset() == 10);
}

#[test]
fn remote_retention_eviction_set_size_based_evicts_oldest_first() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),
        synth_remote_md(11, 10, 19, 200, 100),
        synth_remote_md(12, 20, 29, 300, 100),
    ];
    let cases = [
        // Total=300, budget=150 → reclaim 150 → oldest two go.
        (Some(bytes(150)), 2),
        // Budget tighter than one segment → all three.
        (Some(bytes(50)), 3),
        // Budget larger than total → none.
        (Some(bytes(10_000)), 0),
    ];
    for (budget, expected_len) in cases {
        let out =
            remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, budget, None, 1_000);
        assert!(out.len() == expected_len, "budget: {budget:?}");
    }
}

#[test]
fn remote_retention_eviction_set_equal_size_budget_keeps_all_segments() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        None,
        Some(bytes(100)),
        None,
        1_000,
    );
    assert!(out.is_empty());
}

#[test]
fn remote_retention_eviction_set_time_and_size_take_union_of_either() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),
        synth_remote_md(11, 10, 19, 200, 100),
        synth_remote_md(12, 20, 29, 5_000, 100),
    ];
    // Time-window: seg0+seg1 qualify (max_ts<500). Budget very generous
    // so size-based evicts nothing. Result is the time-window prefix.
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        Some(millis(500)),
        Some(bytes(10_000)),
        None,
        1_000,
    );
    assert!(out.len() == 2);
}

#[test]
fn remote_retention_eviction_set_none_settings_disable_axis() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    // No time or size → no eviction.
    assert!(
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, None, None, 10_000)
            .is_empty()
    );
}

#[test]
fn remote_retention_eviction_set_walk_stops_at_first_non_deletable() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),     // deletable by time
        synth_remote_md(11, 10, 19, 9_500, 100), // in window → stops walk
        synth_remote_md(12, 20, 29, 200, 100),   // also deletable by time, but
                                                 // walk stopped at seg1 already.
    ];
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        Some(millis(500)),
        None,
        None,
        10_000,
    );
    assert!(out.len() == 1);
    assert!(out[0].start_offset() == 0);
}

#[tokio::test]
async fn remote_retention_pass_evicts_old_segments_through_lifecycle() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let copied = copy_eligible(
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
    )
    .await;
    assert!(copied == exports.len());
    let pre = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert!(!pre.is_empty());

    let cfg = LogConfig {
        retention: Some(millis(1)),
        ..LogConfig::default()
    };
    // far-future `now_ms` → every finished segment is past the window.
    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: NO_FLOOR,
            deleted_below: None,
            now_ms: now_ms() + 1_000_000,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;
    assert!(outcome.deleted == exports.len());
    // Every remote copy is gone, so the global floor moves past the last of
    // them and `ListOffsets(earliest)` follows it.
    assert!(
        outcome.log_start
            == Some(Offset(
                pre.iter()
                    .map(RemoteLogSegmentMetadata::end_offset)
                    .max()
                    .unwrap()
                    + 1
            ))
    );

    // DeleteSegmentFinished drops the entries entirely from the cache.
    let post = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert!(
        post.is_empty(),
        "every segment should be gone, got {} left",
        post.len()
    );
    // RSM data is gone too.
    for md in &pre {
        assert!(rsm.fetch_log_segment(md, 0, None).is_err());
    }
}

#[tokio::test]
async fn remote_retention_pass_noop_when_nothing_qualifies() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    copy_eligible(
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
    )
    .await;

    let cfg = LogConfig {
        // Long retention; nothing is past the window.
        retention: Some(hours(8_760)),
        retention_size: None,
        ..LogConfig::default()
    };
    // Use a `now_ms` close to the segments' max_timestamp so the test
    // is independent of wall-clock. `rolled_log` builds batches with
    // default base_timestamp=0, so picking now=1 keeps every segment
    // inside the year-long retention window.
    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: NO_FLOOR,
            deleted_below: None,
            now_ms: 1,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;
    assert!(outcome == RemoteRetentionOutcome::default());
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
}

/// With no retention settings and an unmoved floor there is nothing to
/// evict, but the pass no longer returns before it lists: the log-start
/// breach is an axis of its own and a `DeleteRecords` has to be able to free
/// remote bytes on a topic that keeps its records forever.
#[tokio::test]
async fn remote_retention_pass_no_settings_and_an_unmoved_floor_evict_nothing() {
    let remote_dir = tempfile::tempdir().unwrap();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    seed_finished_segments(&rlmm, 3);
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: NO_FLOOR,
            deleted_below: None,
            now_ms: now_ms(),
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;
    assert!(outcome == RemoteRetentionOutcome::default());
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == 3);
}

#[test]
fn remote_retention_eviction_set_is_empty_for_a_write_once_archive() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),
        synth_remote_md(11, 10, 19, 200, 100),
        synth_remote_md(12, 20, 29, 300, 100),
    ];
    // The `mutable_len` column keeps the fixture honest: an empty result
    // under `WriteOnce` only means something if the very same inputs do
    // evict on a mutable tier.
    let cases = [
        ("time window past for all", Some(millis(1)), None, 10_000, 3),
        (
            "time window past for a prefix",
            Some(millis(9_750)),
            None,
            10_000,
            2,
        ),
        (
            "size budget below one segment",
            None,
            Some(bytes(50)),
            1_000,
            3,
        ),
        (
            "size budget of half the total",
            None,
            Some(bytes(150)),
            1_000,
            2,
        ),
        (
            "time and size together",
            Some(millis(1)),
            Some(bytes(150)),
            10_000,
            3,
        ),
    ];
    for (name, retention, retention_size, now, mutable_len) in cases {
        check!(
            remote_retention_eviction_set(
                ArchiveMode::Mutable,
                &segs,
                retention,
                retention_size,
                None,
                now
            )
            .len()
                == mutable_len,
            "case {name}"
        );
        check!(
            remote_retention_eviction_set(
                ArchiveMode::WriteOnce,
                &segs,
                retention,
                retention_size,
                None,
                now
            )
            .is_empty(),
            "case {name}"
        );
    }
}

#[tokio::test]
async fn remote_retention_pass_never_reaches_the_rsm_for_a_write_once_archive() {
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    seed_finished_segments(&rlmm, 3);
    // `FakeWormArchive::delete_log_segment_data` panics.
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
    let cfg = LogConfig {
        retention: Some(millis(1)),
        retention_size: Some(bytes(1)),
        ..LogConfig::default()
    };

    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::WriteOnce,
            log_start_offset: NO_FLOOR,
            deleted_below: None,
            now_ms: now_ms() + 1_000_000,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;

    check!(outcome == RemoteRetentionOutcome::default());
    // Not even the metadata lifecycle moved: the pass returns before it
    // lists, so a 30-second tick over a WORM partition costs nothing.
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(listed.len() == 3);
    check!(
        listed
            .iter()
            .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
    );
}

#[tokio::test]
async fn remote_retention_pass_reaches_a_refusing_rsm_only_on_a_mutable_tier() {
    let cases = [
        ("mutable tier asks and is refused", ArchiveMode::Mutable, 1),
        ("write-once archive never asks", ArchiveMode::WriteOnce, 0),
    ];
    for (name, archive, expected_attempts) in cases {
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        seed_finished_segments(&rlmm, 3);
        let rsm_impl = Arc::new(RefusesDeleteRsm::default());
        let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
        let cfg = LogConfig {
            retention: Some(millis(1)),
            ..LogConfig::default()
        };

        let outcome = remote_retention_pass(
            &tp(),
            1,
            RemoteRetentionBounds {
                log_config: &cfg,
                archive,
                log_start_offset: NO_FLOOR,
                deleted_below: None,
                now_ms: now_ms() + 1_000_000,
            },
            &rsm,
            &rlmm,
            &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
        )
        .await;

        check!(outcome == RemoteRetentionOutcome::default(), "case {name}");
        check!(
            rsm_impl
                .deletes_attempted
                .load(std::sync::atomic::Ordering::Relaxed)
                == expected_attempts,
            "case {name}"
        );
    }
}

/// The log-start breach is its own eviction axis (Kafka's
/// `deleteLogStartOffsetBreachedSegments`): a finished segment whose whole
/// offset range fell below the partition's `log_start_offset` goes, whatever
/// `retention.ms` and `retention.bytes` say -- including when both are unset,
/// which is exactly the topic where a `DeleteRecords` used to leave the
/// remote copies listed, fetchable and billed forever.
///
/// The floor is compared against `end_offset`, so a segment the floor lands
/// inside keeps its remaining records and stops the walk.
#[test]
fn a_segment_below_the_log_start_is_evicted_whatever_retention_says() {
    // Three ten-record segments: [0, 9], [10, 19], [20, 29].
    let segs = vec![
        synth_remote_md(10, 0, 9, 9_500, 100),
        synth_remote_md(11, 10, 19, 9_500, 100),
        synth_remote_md(12, 20, 29, 9_500, 100),
    ];
    // now=10_000 against max_ts=9_500 keeps every segment inside a 500ms
    // window, so the time axis never fires and a non-empty result is the
    // breach axis alone.
    let cases = [
        ("no floor at all, no settings", None, None, None, 0),
        (
            "no floor at all, time window open",
            None,
            Some(millis(500)),
            None,
            0,
        ),
        (
            "a floor at zero breaches nothing",
            Some(NO_FLOOR),
            None,
            None,
            0,
        ),
        (
            "breach only, no settings at all",
            Some(Offset(20)),
            None,
            None,
            2,
        ),
        (
            "breach only, generous settings",
            Some(Offset(20)),
            Some(millis(500)),
            Some(bytes(10_000)),
            2,
        ),
        (
            "the floor inside a segment keeps it",
            Some(Offset(15)),
            None,
            None,
            1,
        ),
        (
            "the floor at a segment's end offset keeps it",
            Some(Offset(9)),
            None,
            None,
            0,
        ),
        (
            "a floor past every segment takes them all",
            Some(Offset(30)),
            None,
            None,
            3,
        ),
    ];
    for (name, floor, retention, retention_size, expected) in cases {
        check!(
            remote_retention_eviction_set(
                ArchiveMode::Mutable,
                &segs,
                retention,
                retention_size,
                floor,
                10_000,
            )
            .len()
                == expected,
            "case {name}"
        );
        check!(
            remote_retention_eviction_set(
                ArchiveMode::WriteOnce,
                &segs,
                retention,
                retention_size,
                floor,
                10_000,
            )
            .is_empty(),
            "case {name}: a write-once archive has no delete to give"
        );
    }
}

/// Breach and time window together take the union, and the union is still a
/// contiguous prefix: a segment the time axis would have skipped over is
/// covered by the breach, so the walk does not stop short of it.
#[test]
fn the_breach_and_the_time_window_take_the_union_of_either() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 9_500, 100), // in the time window; breached
        synth_remote_md(11, 10, 19, 100, 100), // past the time window
        synth_remote_md(12, 20, 29, 9_500, 100), // in the window, not breached
    ];
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        Some(millis(500)),
        None,
        Some(Offset(10)),
        10_000,
    );
    assert!(out.len() == 2);
    check!(out[0].start_offset() == 0, "breached");
    check!(out[1].start_offset() == 10, "past the time window");
}

/// A breach eviction frees the archive and leaves the floor where the
/// operator put it.
///
/// The reported floor is an *advance*, and a breach can never produce one: it
/// removes only what is already below the floor. A time or size eviction is
/// the case that moves it, which
/// [`remote_retention_pass_evicts_old_segments_through_lifecycle`] covers.
#[tokio::test]
async fn a_breach_eviction_frees_the_archive_without_moving_the_floor() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "the test needs a prefix to evict");
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let copied = copy_eligible(
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
    )
    .await;
    assert!(copied == exports.len());

    // A `DeleteRecords` floor one past the oldest copied segment, and a topic
    // that keeps its records forever: the breach is the only axis that can
    // evict anything here.
    let floor = exports[0].last_offset + 1;
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };

    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: floor,
            deleted_below: Some(floor),
            now_ms: 1,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;

    check!(outcome.deleted == 1);
    check!(
        outcome.log_start == None,
        "everything it removed was already under the floor"
    );
    let left = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(left.len() == exports.len() - 1);
    check!(
        left.iter().all(|md| md.end_offset() >= floor.0),
        "only the breached prefix goes"
    );
}

/// The floor may only cross a run of segments the pass removed without a gap.
///
/// `copy_eligible` skips a segment whose copy failed and carries on with the
/// next one, so the finished list is not always a contiguous offset prefix.
/// Publishing the last delete's end offset over such a gap would put the floor
/// above a segment that is still on local disk and still readable.
#[tokio::test]
async fn the_reported_floor_stops_at_a_gap_in_the_finished_segments() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    assert!(exports.len() >= 3, "the test needs a segment to skip over");
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    // Copy the first and the third segment and not the second, which is what
    // a failed copy in the middle of a tick leaves behind.
    let gapped = vec![exports[0].clone(), exports[2].clone()];
    let copied = copy_eligible(
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        &tp(),
        1,
        LeaderEpoch(0),
        gapped,
    )
    .await;
    assert!(copied == 2);

    // Time retention past every segment, so both finished copies are
    // deletable and the walk reaches the far side of the gap.
    let cfg = LogConfig {
        retention: Some(millis(1)),
        ..LogConfig::default()
    };
    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: NO_FLOOR,
            deleted_below: None,
            now_ms: now_ms() + 1_000_000,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;

    check!(outcome.deleted == 2, "both copies are past the window");
    check!(
        outcome.log_start == Some(exports[0].last_offset + 1),
        "the floor stops below the segment that was never copied"
    );
}

/// A partition reopened after its local segments were evicted keeps its
/// archive.
///
/// `Log::open` infers a `log_start_offset` from the segments that survived on
/// disk, and on a tiered partition that inference sits above the whole
/// archive. If the breach axis measured against it, the first tick after every
/// restart would delete every remote segment the partition has. The axis
/// measures against the floor someone actually deleted up to, which a reopened
/// log reports as `None`.
#[tokio::test]
async fn a_floor_nobody_moved_leaves_the_archive_alone() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "the test needs a prefix to evict");
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let copied = copy_eligible(
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
    )
    .await;
    assert!(copied == exports.len());

    // What a restart leaves behind: a `log_start_offset` past every copied
    // segment, and nothing saying anyone deleted up to it.
    let inferred = exports[exports.len() - 1].last_offset + 1;
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let outcome = remote_retention_pass(
        &tp(),
        1,
        RemoteRetentionBounds {
            log_config: &cfg,
            archive: ArchiveMode::Mutable,
            log_start_offset: inferred,
            deleted_below: None,
            now_ms: 1,
        },
        &rsm,
        &rlmm,
        &Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()),
    )
    .await;

    check!(outcome.deleted == 0, "an inferred floor deletes nothing");
    check!(outcome.log_start == None);
    check!(
        rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len(),
        "the archive is intact"
    );
}
