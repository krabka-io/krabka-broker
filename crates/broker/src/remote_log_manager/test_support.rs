//! Fixtures the remote-log-manager unit tests share: stand-in remote-storage
//! and metadata backends, and builders for rolled logs, tiered partitions and
//! synthetic segment exports.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::Ordering},
};

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_log::{Log, LogConfig, Offset, SegmentExport};
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_remote_storage::{
    CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
    LogSegmentData, ObjectEntry, RemoteLogMetadataManager, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
    RemoteStorageError, RemoteStorageManager, Sha256Digest, TopicIdPartition, WormArchiver,
};
use krabka_units::bytes;
use uuid::Uuid;

use crate::{
    metrics::BrokerMetrics,
    partition::Partition,
    remote_log_manager::{ArchiveMode, RemoteTier},
    test_support::FakeMetadataSource,
};

/// A `BrokerMetrics` shared by every tier a unit test builds.
///
/// One registry serves them all because these tests assert on the archive,
/// not on the counters; a test that reads a counter builds its own metrics
/// and its own [`RemoteTier`] so nothing else can move it.
pub(crate) fn shared_test_metrics() -> &'static BrokerMetrics {
    static METRICS: std::sync::OnceLock<BrokerMetrics> = std::sync::OnceLock::new();
    METRICS.get_or_init(BrokerMetrics::new)
}

/// The index cache every unit-test tier shares. It is disabled: these tests
/// sweep the archive, and a cache that stores nothing cannot make one of them
/// pass by holding bytes a later assertion expects to be gone.
fn shared_test_index_cache() -> &'static Arc<krabka_remote_storage::RemoteIndexCache> {
    static CACHE: std::sync::OnceLock<Arc<krabka_remote_storage::RemoteIndexCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Arc::new(krabka_remote_storage::RemoteIndexCache::disabled()))
}

/// The copy deadline a unit test sweeps under when it is not the thing being
/// tested. It is long enough that no in-process fake reaches it.
pub(crate) const TEST_COPY_TIMEOUT: krabka_units::Time = krabka_units::secs(60);

/// The tier a unit test sweeps, wired to the shared metrics.
pub(crate) fn tier<'a>(
    archive: ArchiveMode,
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
) -> RemoteTier<'a> {
    tier_with_copy_timeout(archive, rsm, rlmm, TEST_COPY_TIMEOUT)
}

/// The same tier under a chosen copy deadline, for the suites that drive a
/// store slow enough to reach it.
pub(crate) fn tier_with_copy_timeout<'a>(
    archive: ArchiveMode,
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
    copy_timeout: krabka_units::Time,
) -> RemoteTier<'a> {
    RemoteTier {
        archive,
        rsm,
        rlmm,
        metrics: shared_test_metrics(),
        index_cache: shared_test_index_cache(),
        unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
        copy_timeout,
    }
}

/// Fresh mutable storage and metadata backends for one test's temporary directory.
pub fn local_backends(
    remote_dir: &std::path::Path,
) -> (
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
) {
    (
        Arc::new(LocalTieredStorage::new(remote_dir)),
        Arc::new(InmemoryRemoteLogMetadataManager::new()),
    )
}

/// Stand-in backends that never retain readable segment or index bytes.
/// Copy and deletion behavior remain explicit in each implementation.
macro_rules! missing_remote_reads {
    () => {
        fn fetch_log_segment(
            &self,
            metadata: &krabka_remote_storage::RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, krabka_remote_storage::RemoteStorageError> {
            Err(krabka_remote_storage::RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &krabka_remote_storage::RemoteLogSegmentMetadata,
            _index_type: krabka_remote_storage::IndexType,
        ) -> Result<Vec<u8>, krabka_remote_storage::RemoteStorageError> {
            Err(krabka_remote_storage::RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
    };
}
pub(crate) use missing_remote_reads;

/// A mutable tier with counters owned by the test that asserts on them.
pub(crate) fn tier_with_metrics<'a>(
    rsm: &'a Arc<dyn RemoteStorageManager>,
    rlmm: &'a Arc<dyn RemoteLogMetadataManager>,
    metrics: &'a BrokerMetrics,
    unstable_api_versions: crate::api_catalog::UnstableApiVersions,
) -> RemoteTier<'a> {
    RemoteTier {
        metrics,
        unstable_api_versions,
        ..tier(ArchiveMode::Mutable, rsm, rlmm)
    }
}

/// Copy every supplied fixture segment as broker 1 in leader epoch 0.
/// The assertion stays at the copy checkpoint, before a retention pass runs.
pub async fn copy_all_exports(tier: &RemoteTier<'_>, exports: &[SegmentExport]) {
    let copied = super::copy_eligible(tier, &tp(), 1, LeaderEpoch(0), exports.to_vec()).await;
    assert2::assert!(copied == exports.len());
}

/// One default-concurrency sweep by the fixture broker (node and broker ID 1).
pub async fn sweep_once(
    partitions: &crate::partition_registry::PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    tier: &RemoteTier<'_>,
) {
    super::tick_all(
        partitions,
        controller,
        tier,
        NodeId(1),
        1,
        super::SweepConcurrency::default(),
    )
    .await;
}

/// The sealed exports and their config from one hold of the partition's log lock.
pub fn partition_snapshot(partition: &Partition) -> (Vec<SegmentExport>, LogConfig) {
    let log = partition.log.lock().expect("partition log mutex poisoned");
    (log.tierable_segments(), log.config_snapshot())
}

/// Remote and local counts after a sweep; the local count waits for rollover flush.
pub fn sweep_counts(
    partition: &Partition,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> (usize, usize) {
    let remote_finished = rlmm
        .list_remote_log_segments(&tp())
        .unwrap()
        .iter()
        .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        .count();
    let mut log = partition.log.lock().expect("partition log mutex poisoned");
    log.sync().expect("flush rolled segments");
    (remote_finished, log.tierable_segments().len())
}

/// Fresh mutable backends with every supplied fixture export successfully archived.
pub async fn archived_backends(
    remote_dir: &std::path::Path,
    exports: &[SegmentExport],
) -> (
    Arc<dyn RemoteStorageManager>,
    Arc<dyn RemoteLogMetadataManager>,
) {
    let (rsm, rlmm) = local_backends(remote_dir);
    copy_all_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports).await;
    (rsm, rlmm)
}

/// A stand-in write-once archive. Every copy seals a real (unsigned) WORM
/// manifest over the segment's leader-epoch bytes, keeps that manifest in
/// memory, and returns the chain receipt the backend would.
///
/// Its delete **panics**. A write-once backend refuses every delete, so a
/// broker that reaches one has already lost: the panic turns that into a
/// test failure instead of a warning nobody reads.
pub struct FakeWormArchive {
    archiver: WormArchiver,
    manifests: Mutex<BTreeMap<Uuid, Vec<u8>>>,
}

impl FakeWormArchive {
    pub fn new() -> Self {
        Self {
            archiver: WormArchiver::new(None),
            manifests: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn archived_segments(&self) -> usize {
        self.manifests
            .lock()
            .expect("archived-manifest mutex poisoned")
            .len()
    }
}

impl RemoteStorageManager for FakeWormArchive {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let body = data.leader_epoch_index.clone();
        let entry = ObjectEntry {
            suffix: IndexType::LeaderEpoch.suffix().to_string(),
            key: format!("{}.leader-epoch", metadata.remote_log_segment_id().id),
            size_bytes: u64::try_from(body.len()).expect("test object fits in u64"),
            sha256: Sha256Digest::of(&body),
            e_tag: None,
            version_id: None,
            create_precondition: true,
        };
        let sealed = self.archiver.seal(metadata, vec![entry])?;
        self.manifests
            .lock()
            .expect("archived-manifest mutex poisoned")
            .insert(metadata.remote_log_segment_id().id, sealed.bytes.to_vec());
        Ok(Some(sealed.receipt.to_custom_metadata()))
    }
    missing_remote_reads!();
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        panic!(
            "a write-once archive must never reach an RSM delete (segment {})",
            metadata.remote_log_segment_id().id
        );
    }
}

/// A metadata source over `image`, with node 1 reported as the controller
/// leader so that the sweep treats this broker as the partition leader.
pub fn fixed_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(Some(NodeId(1)))
        .build()
}

pub fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
}

pub fn batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(vec![b'x'; 64])),
            ..Default::default()
        });
    }
    b
}

/// Build a log rolled into several sealed segments under `dir`.
pub fn rolled_log(dir: &std::path::Path) -> Log {
    let mut log = Log::open(
        dir,
        LogConfig {
            segment_size: bytes(256), // tiny so we roll fast
            ..LogConfig::default()
        },
    )
    .unwrap();
    for _ in 0..12 {
        let mut b = batch(2);
        log.append(&mut b).unwrap();
    }
    log.sync().unwrap();
    log
}

pub fn rolled_tiered_partition_with_config(
    log_dir: &std::path::Path,
    config: LogConfig,
) -> Arc<Partition> {
    rolled_tiered_partition_at(PartitionIndex(0), log_dir, config)
}

/// The same fixture at a chosen partition index, for the suites that sweep
/// more than one partition of `orders` in a tick. The index is the
/// partition's identity everywhere the sweep looks -- the directory it opens,
/// the `TopicIdPartition` it copies under -- so it cannot be patched in
/// afterwards.
pub fn rolled_tiered_partition_at(
    index: PartitionIndex,
    log_dir: &std::path::Path,
    config: LogConfig,
) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(log_dir, "orders", index.get());
    std::fs::create_dir_all(&part_dir).unwrap();
    let mut log = Log::open(&part_dir, config).unwrap();
    for _ in 0..12 {
        let mut b = batch(2);
        log.append(&mut b).unwrap();
    }
    log.sync().unwrap();
    leading_partition_over(index, log_dir, log)
}

/// A partition of `orders` at `index` that this broker (node 1, epoch 0)
/// leads, over a log the caller has already written. The log must live in the
/// partition directory under `log_dir`.
pub fn leading_partition_over(
    index: PartitionIndex,
    log_dir: &std::path::Path,
    log: Log,
) -> Arc<Partition> {
    let log_end = log.log_end_offset();
    let partition = crate::broker::spawn_partition(
        "orders".to_string(),
        index,
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    partition.current_leader.store(1, Ordering::Relaxed);
    partition.current_leader_epoch.store(0, Ordering::Release);
    // A sole replica has replicated everything it wrote: the high watermark
    // is the log end, so nothing the fixture wrote is withheld from the tier
    // as uncommitted.
    partition
        .replica_state
        .try_lock()
        .expect("nothing else holds a partition that was just spawned")
        .hw = log_end;
    partition
}

/// A sealed-segment export with no files behind it, whose file was last
/// modified at its newest record's timestamp.
pub fn synth_export(base: i64, last: i64, max_ts: i64, size: u32) -> SegmentExport {
    SegmentExport {
        base_offset: Offset(base),
        last_offset: Offset(last),
        max_timestamp: max_ts,
        last_modified_ms: max_ts,
        size: bytes(size),
        log_path: std::path::PathBuf::new(),
        offset_index_path: std::path::PathBuf::new(),
        time_index_path: std::path::PathBuf::new(),
        transaction_index_path: None,
        producer_snapshot_path: std::path::PathBuf::new(),
        leader_epochs: Vec::new(),
    }
}

/// Add a `CopySegmentStarted` record and leave it there, the way a copy
/// that died after the metadata write but before the backend answered
/// does. Returns the segment's UUID.
pub fn stuck_started_segment(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    id: u128,
    base: i64,
) -> Uuid {
    let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(id));
    let md = RemoteLogSegmentMetadata::new(
        segment_id.clone(),
        base,
        base + 9,
        100,
        1,
        100,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            100,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => base},
        ),
    )
    .unwrap();
    rlmm.add_remote_log_segment_metadata(md).unwrap();
    segment_id.id
}

/// Put `count` `CopySegmentFinished` segments into `rlmm`, ten offsets
/// apart, without going near an RSM.
pub fn seed_finished_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, count: usize) {
    for i in 0..count {
        let index = u128::try_from(i).expect("test segment count fits in u128");
        let base = i64::try_from(i).expect("test segment count fits in i64") * 10;
        let id = 0x5000 + index;
        stuck_started_segment(rlmm, id, base);
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 100,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
    }
}
