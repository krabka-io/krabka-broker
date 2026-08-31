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
    CustomMetadata, IndexType, LogSegmentData, ObjectEntry, RemoteLogMetadataManager,
    RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager, Sha256Digest,
    TopicIdPartition, WormArchiver,
};
use krabka_units::bytes;
use uuid::Uuid;

use crate::{partition::Partition, test_support::FakeMetadataSource};

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
    log
}

pub fn rolled_tiered_partition_with_config(
    log_dir: &std::path::Path,
    config: LogConfig,
) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(log_dir, "orders", 0);
    std::fs::create_dir_all(&part_dir).unwrap();
    let mut log = Log::open(&part_dir, config).unwrap();
    for _ in 0..12 {
        let mut b = batch(2);
        log.append(&mut b).unwrap();
    }
    let partition = crate::broker::spawn_partition(
        "orders".to_string(),
        PartitionIndex(0),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    partition.current_leader.store(1, Ordering::Relaxed);
    partition.current_leader_epoch.store(0, Ordering::Release);
    partition
}

pub fn synth_export(base: i64, last: i64, max_ts: i64, size: u32) -> SegmentExport {
    SegmentExport {
        base_offset: Offset(base),
        last_offset: Offset(last),
        max_timestamp: max_ts,
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
