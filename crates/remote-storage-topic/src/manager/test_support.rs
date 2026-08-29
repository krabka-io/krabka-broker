//! Fixtures shared by the manager submodules' unit tests.
//!
//! Each submodule keeps the tests for the code it holds, and the segment
//! builders, the manager starters, and the flaky-HWM test double here are the
//! parts several of them need. One module for the harness keeps a change to a
//! fixture in one place.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    CustomMetadata, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::TopicBasedRemoteLogMetadataManager;
use crate::{
    error::MetadataLogError,
    log::{
        AssignmentHandle, InProcessMetadataEventLog, MetadataEventLog, MetadataEventStream,
        PartitionStart,
    },
};

/// Test double that delegates to an inner [`InProcessMetadataEventLog`]
/// but can fail `high_water_marks()` on demand. The in-process fixture's
/// HWM RPC always succeeds, which is why the rest of the suite cannot
/// exercise the C1 fail-closed path.
pub struct HwmFlakyLog {
    inner: Arc<InProcessMetadataEventLog>,
    fail_hwm: std::sync::atomic::AtomicBool,
}

impl HwmFlakyLog {
    pub fn new(partition_count: i32) -> Arc<Self> {
        Arc::new(Self {
            inner: InProcessMetadataEventLog::new(partition_count),
            fail_hwm: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub fn set_fail_hwm(&self, fail: bool) {
        self.fail_hwm
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl MetadataEventLog for HwmFlakyLog {
    fn partition_count(&self) -> i32 {
        self.inner.partition_count()
    }
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.inner.publish(partition, event).await
    }
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        self.inner.subscribe(assignment)
    }
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        if self.fail_hwm.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(MetadataLogError::Other("injected HWM failure".into()));
        }
        self.inner.high_water_marks().await
    }
}

static SNAP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn snapshot_test_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "krabka-rlmm-{label}-{}-{}",
        std::process::id(),
        SNAP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

pub fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
}

pub fn started(id: u128, start: i64, end: i64) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        start,
        end,
        end + 1,
        1,
        100,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            2048,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => start},
        ),
    )
    .unwrap()
}

pub fn finish(id: u128) -> RemoteLogSegmentMetadataUpdate {
    RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        event_timestamp_ms: 200,
        custom_metadata: Some(CustomMetadata(vec![7])),
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 1,
    }
}

/// Run the sync RLMM trait method on the blocking pool, exactly
/// like the broker does.
pub async fn on_blocking<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

/// Poll until `tp` reads `Ok(Some)`, which means assigned and caught up,
/// or panic.
pub async fn wait_ready(m: &Arc<TopicBasedRemoteLogMetadataManager>, tp: &TopicIdPartition) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if matches!(
            m.remote_log_segment_metadata(tp, LeaderEpoch(0), 42),
            Ok(Some(_))
        ) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partition never became ready"
        );
        // Yield-poll the pump's progress rather than sleeping a fixed
        // cadence; the deadline above stays as the hang-guard.
        tokio::task::yield_now().await;
    }
}

/// Start a manager that consumes NOTHING until the caller drives
/// `reconcile_assignment`. The assignment and readiness tests use this,
/// and they assert that pre-assignment reads are a genuine miss.
pub fn start_manager(log: Arc<dyn MetadataEventLog>) -> Arc<TopicBasedRemoteLogMetadataManager> {
    TopicBasedRemoteLogMetadataManager::start(
        log,
        Handle::current(),
        snapshot_test_dir("test"),
        std::time::Duration::from_hours(1),
    )
    .unwrap()
}

/// Start a manager and assign EVERY metadata partition, which is the
/// eager "consume all" behavior. Tests that publish through the manager
/// and read the result back use this, and so do the multi-broker pre-seed
/// writers. It blocks until each non-empty partition has caught up to its
/// assignment-time HWM, so a subsequent read does not race the pump.
pub async fn start_manager_all(
    log: Arc<dyn MetadataEventLog>,
) -> Arc<TopicBasedRemoteLogMetadataManager> {
    let n = log.partition_count();
    let m = start_manager(log);
    let all: Vec<i32> = (0..n).collect();
    m.reconcile_assignment(&all).await;
    // Wait for the pump to catch up to every assigned partition's HWM so
    // the manager is "ready" for all partitions, mirroring the old
    // bootstrap contract.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !all.iter().all(|&mp| m.metadata_partition_ready(mp)) {
        assert!(
            std::time::Instant::now() < deadline,
            "manager did not catch up on all partitions within 5s"
        );
        tokio::task::yield_now().await;
    }
    m
}
