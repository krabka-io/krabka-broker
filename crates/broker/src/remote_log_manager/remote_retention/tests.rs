//! Unit tests for the remote-retention eviction set and its delete pass.

use std::collections::BTreeMap;

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
        10_000,
    );
    assert!(out.is_empty());
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
    let out =
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, Some(millis(500)), None, 10_000);
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
        let out = remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, budget, 1_000);
        assert!(out.len() == expected_len, "budget: {budget:?}");
    }
}

#[test]
fn remote_retention_eviction_set_equal_size_budget_keeps_all_segments() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    let out =
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, Some(bytes(100)), 1_000);
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
        1_000,
    );
    assert!(out.len() == 2);
}

#[test]
fn remote_retention_eviction_set_none_settings_disable_axis() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    // No time or size → no eviction.
    assert!(
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, None, None, 10_000).is_empty()
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
    let out =
        remote_retention_eviction_set(ArchiveMode::Mutable, &segs, Some(millis(500)), None, 10_000);
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
    let deleted = remote_retention_pass(
        &tp(),
        1,
        &cfg,
        ArchiveMode::Mutable,
        &rsm,
        &rlmm,
        now_ms() + 1_000_000,
    )
    .await;
    assert!(deleted == exports.len());

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
    let deleted = remote_retention_pass(&tp(), 1, &cfg, ArchiveMode::Mutable, &rsm, &rlmm, 1).await;
    assert!(deleted == 0);
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
}

#[tokio::test]
async fn remote_retention_pass_no_settings_no_op() {
    // Neither retention.ms nor retention.bytes — early return, no list.
    let remote_dir = tempfile::tempdir().unwrap();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let deleted =
        remote_retention_pass(&tp(), 1, &cfg, ArchiveMode::Mutable, &rsm, &rlmm, now_ms()).await;
    assert!(deleted == 0);
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

    let deleted = remote_retention_pass(
        &tp(),
        1,
        &cfg,
        ArchiveMode::WriteOnce,
        &rsm,
        &rlmm,
        now_ms() + 1_000_000,
    )
    .await;

    check!(deleted == 0);
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

        let deleted =
            remote_retention_pass(&tp(), 1, &cfg, archive, &rsm, &rlmm, now_ms() + 1_000_000).await;

        check!(deleted == 0, "case {name}");
        check!(
            rsm_impl
                .deletes_attempted
                .load(std::sync::atomic::Ordering::Relaxed)
                == expected_attempts,
            "case {name}"
        );
    }
}
