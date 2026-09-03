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
    test_support::{FakeWormArchive, rolled_log, seed_finished_segments, tp},
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
        NO_FLOOR,
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
            NO_FLOOR,
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
            NO_FLOOR,
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
            NO_FLOOR,
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
        NO_FLOOR,
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
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            None,
            budget,
            NO_FLOOR,
            1_000,
        );
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
        NO_FLOOR,
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
        NO_FLOOR,
        1_000,
    );
    assert!(out.len() == 2);
}

#[test]
fn remote_retention_eviction_set_none_settings_disable_axis() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    // No time or size → no eviction.
    assert!(
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, None, NO_FLOOR, 10_000)
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
        NO_FLOOR,
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
            now_ms: now_ms() + 1_000_000,
        },
        &rsm,
        &rlmm,
    )
    .await;
    assert!(outcome.deleted == exports.len());
    // Every remote copy is gone, so the global floor moves past the last of
    // them and `ListOffsets(earliest)` follows it.
    assert!(
        outcome.log_start
            == Some(Offset(
                pre.iter().map(|md| md.end_offset()).max().unwrap() + 1
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
        &tp(),
        1,
        LeaderEpoch(0),
        exports.clone(),
        ArchiveMode::Mutable,
        &rsm,
        &rlmm,
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
            now_ms: 1,
        },
        &rsm,
        &rlmm,
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
            now_ms: now_ms(),
        },
        &rsm,
        &rlmm,
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
                NO_FLOOR,
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
                NO_FLOOR,
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
            now_ms: now_ms() + 1_000_000,
        },
        &rsm,
        &rlmm,
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
                now_ms: now_ms() + 1_000_000,
            },
            &rsm,
            &rlmm,
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
        ("no floor, no settings", Offset(0), None, None, 0),
        (
            "no floor, time window open",
            Offset(0),
            Some(millis(500)),
            None,
            0,
        ),
        ("breach only, no settings at all", Offset(20), None, None, 2),
        (
            "breach only, generous settings",
            Offset(20),
            Some(millis(500)),
            Some(bytes(10_000)),
            2,
        ),
        (
            "the floor inside a segment keeps it",
            Offset(15),
            None,
            None,
            1,
        ),
        (
            "the floor at a segment's end offset keeps it",
            Offset(9),
            None,
            None,
            0,
        ),
        (
            "a floor past every segment takes them all",
            Offset(30),
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
        Offset(10),
        10_000,
    );
    assert!(out.len() == 2);
    check!(out[0].start_offset() == 0, "breached");
    check!(out[1].start_offset() == 10, "past the time window");
}

/// The pass reports the floor the caller must raise `log_start_offset` to.
/// Without it, a `DeleteRecords` frees the remote bytes but
/// `ListOffsets(earliest)` keeps naming an offset no tier can serve.
#[tokio::test]
async fn a_breach_eviction_reports_the_floor_the_log_start_must_follow() {
    let log_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let log = rolled_log(log_dir.path());
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "the test needs a prefix to evict");
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
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
            now_ms: 1,
        },
        &rsm,
        &rlmm,
    )
    .await;

    check!(outcome.deleted == 1);
    check!(outcome.log_start == Some(floor));
    let left = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(left.len() == exports.len() - 1);
    check!(
        left.iter().all(|md| md.end_offset() >= floor.0),
        "only the breached prefix goes"
    );
}
