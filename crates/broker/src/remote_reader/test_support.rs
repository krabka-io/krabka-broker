//! Fixtures the remote-reader unit tests share.
//!
//! The builders here populate a `LocalTieredStorage` and an
//! `InmemoryRemoteLogMetadataManager` the way the copy path does, either from
//! a real rolled `Log` or from hand-written segment and index bytes, and hand
//! back a [`RemoteReader`] over that tier. The stand-in metadata manager lets
//! a test drive the error paths that a healthy backend never produces.

// These exercise the full RSM/RLMM plumbing through `RemoteReader` against
// `LocalTieredStorage` and `InmemoryRemoteLogMetadataManager`, using the
// copy path's `copy_eligible` to populate the tier from a real `Log`.

use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use assert2::assert;
use krabka_ids::LeaderEpoch;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::Record;
use krabka_remote_storage::{
    InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager,
    TopicIdPartition,
};
use krabka_units::convert::ByteSizeExt as _;
use uuid::Uuid;

use super::RemoteReader;

pub fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
}

fn batch_of(n: i32, value_size: usize) -> krabka_protocol::records::RecordBatch {
    use bytes::Bytes;
    let mut b = krabka_protocol::records::RecordBatch {
        last_offset_delta: n - 1,
        ..krabka_protocol::records::RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(vec![b'x'; value_size])),
            ..Default::default()
        });
    }
    b
}

fn timestamped_batch_at(
    base_offset: i64,
    timestamps: &[i64],
    value_byte: u8,
) -> krabka_protocol::records::RecordBatch {
    use bytes::Bytes;

    let base_timestamp = timestamps.first().copied().unwrap_or_default();
    krabka_protocol::records::RecordBatch {
        base_offset,
        last_offset_delta: i32::try_from(timestamps.len().saturating_sub(1)).unwrap(),
        base_timestamp,
        max_timestamp: timestamps.iter().copied().max().unwrap_or_default(),
        records: timestamps
            .iter()
            .enumerate()
            .map(|(offset_delta, timestamp)| Record {
                timestamp_delta: timestamp - base_timestamp,
                offset_delta: i32::try_from(offset_delta).unwrap(),
                value: Some(Bytes::from(vec![value_byte; 4])),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn offset_index_bytes(entries: &[(u32, u32)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (relative_offset, position) in entries {
        buf.extend_from_slice(&relative_offset.to_be_bytes());
        buf.extend_from_slice(&position.to_be_bytes());
    }
    buf
}

fn time_index_bytes(entries: &[(i64, u32)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (timestamp, relative_offset) in entries {
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&relative_offset.to_be_bytes());
    }
    buf
}

fn write_test_file(dir: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

/// A [`RemoteStorageManager`] that counts the index objects it downloads and
/// otherwise delegates.
///
/// It is how the tests tell a cache hit from a cache miss: the reader's own
/// counters say what it thinks happened, and this says what actually reached
/// the tier.
pub struct CountingRsm {
    inner: Arc<dyn RemoteStorageManager>,
    index_fetches: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingRsm {
    pub fn new(
        inner: Arc<dyn RemoteStorageManager>,
    ) -> (Arc<Self>, Arc<std::sync::atomic::AtomicUsize>) {
        let index_fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Arc::new(Self {
                inner,
                index_fetches: Arc::clone(&index_fetches),
            }),
            index_fetches,
        )
    }
}

impl RemoteStorageManager for CountingRsm {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &krabka_remote_storage::LogSegmentData,
    ) -> Result<Option<krabka_remote_storage::CustomMetadata>, RemoteStorageError> {
        self.inner.copy_log_segment_data(metadata, data)
    }

    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.inner
            .fetch_log_segment(metadata, start_position, end_position)
    }

    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: krabka_remote_storage::IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.index_fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.fetch_index(metadata, index_type)
    }

    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.inner.delete_log_segment_data(metadata)
    }
}

/// The tempdirs a caching reader keeps alive: the remote tier and the index
/// cache's log directory. Dropping either deletes what the reader reads.
pub struct ReaderDirs {
    pub _remote: tempfile::TempDir,
    pub _log: tempfile::TempDir,
}

/// The `sparse_remote_segment_reader` fixture, with the production index cache
/// turned on and a counting RSM underneath.
pub fn caching_sparse_remote_segment_reader() -> (
    RemoteReader,
    ReaderDirs,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let (reader, remote_dir) = sparse_remote_segment_reader();
    let (counting, index_fetches) = CountingRsm::new(Arc::clone(&reader.rsm));
    let log_dir = tempfile::tempdir().unwrap();
    let cache = krabka_remote_storage::RemoteIndexCache::new(
        log_dir.path(),
        krabka_remote_storage::DEFAULT_INDEX_CACHE_TOTAL_SIZE_BYTES,
    )
    .unwrap();
    let cached = RemoteReader::with_limits(
        counting,
        Arc::clone(&reader.rlmm),
        crate::remote_reader::RemoteReaderLimits {
            index_cache: Arc::new(cache),
            pool: crate::remote_reader::ReaderPool::new(10, 100),
        },
    );
    (
        cached,
        ReaderDirs {
            _remote: remote_dir,
            _log: log_dir,
        },
        index_fetches,
    )
}

pub fn sparse_remote_segment_reader_with_max_timestamp(
    max_timestamp_ms: i64,
) -> (RemoteReader, tempfile::TempDir) {
    let source_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();

    let first = timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a');
    let second = timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b');
    let mut log_bytes = bytes::BytesMut::new();
    first.encode(&mut log_bytes).unwrap();
    let second_position = u32::try_from(log_bytes.len()).unwrap();
    second.encode(&mut log_bytes).unwrap();
    let log_bytes = log_bytes.freeze();

    let log_path = write_test_file(source_dir.path(), "00000000000000000010.log", &log_bytes);
    let offset_index_path = write_test_file(
        source_dir.path(),
        "00000000000000000010.index",
        &offset_index_bytes(&[(0, 0), (4, second_position)]),
    );
    let time_index_path = write_test_file(
        source_dir.path(),
        "00000000000000000010.timeindex",
        &time_index_bytes(&[(1_700, 0), (2_400, 4)]),
    );

    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
    let md = RemoteLogSegmentMetadata::new(
        id.clone(),
        10,
        16,
        max_timestamp_ms,
        1,
        2_400,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            i32::try_from(log_bytes.len()).unwrap_or(i32::MAX),
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0_i32) => 10_i64},
        ),
    )
    .unwrap();

    rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
    let data = krabka_remote_storage::LogSegmentData {
        log_segment: log_path,
        offset_index: offset_index_path,
        time_index: time_index_path,
        transaction_index: None,
        producer_snapshot_index: None,
        leader_epoch_index: bytes::Bytes::from_static(b"0\n1\n0 10\n"),
    };
    rsm.copy_log_segment_data(&md, &data).unwrap();
    rlmm.update_remote_log_segment_metadata(
        krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id,
            event_timestamp_ms: 2_400,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        },
    )
    .unwrap();

    (RemoteReader::new(rsm, rlmm), remote_dir)
}

pub fn sparse_remote_segment_reader() -> (RemoteReader, tempfile::TempDir) {
    sparse_remote_segment_reader_with_max_timestamp(2_400)
}

/// Builds a log rolled into several sealed segments under `dir`, then
/// copies every sealed segment into a fresh `LocalTieredStorage` and
/// `InmemoryRemoteLogMetadataManager`. It returns the constructed reader
/// and the log. The caller keeps the log alive so that the on-disk files
/// outlive the call.
pub fn populated_reader(
    log_dir: &std::path::Path,
    remote_dir: &std::path::Path,
) -> (RemoteReader, Log) {
    let mut log = Log::open(
        log_dir,
        LogConfig {
            segment_size: krabka_units::bytes(256),
            ..LogConfig::default()
        },
    )
    .unwrap();
    for _ in 0..12 {
        let mut b = batch_of(2, 64);
        log.append(&mut b).unwrap();
    }
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "test needs multiple sealed segments");

    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    // Manually copy each segment as `CopySegmentStarted` →
    // `CopySegmentFinished` (mirrors the copy path's copy_eligible
    // without the broker-side dependencies).
    for ex in &exports {
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        // Unwrap the log-layer `Offset`s into the remote-storage metadata's
        // `i64` world at the seam.
        let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
            maplit::btreemap! {LeaderEpoch(0) => ex.base_offset.0}
        } else {
            ex.leader_epochs
                .iter()
                .map(|&(epoch, off)| (epoch, off.0))
                .collect()
        };
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            ex.base_offset.0,
            ex.last_offset.0,
            ex.max_timestamp,
            1,
            ex.max_timestamp,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                ex.size.bytes_i32(),
                RemoteLogSegmentState::CopySegmentStarted,
                epochs.clone(),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        // Render the leader-epoch checkpoint the same way the copy path
        // does so `fetch_index(LeaderEpoch)` returns real bytes.
        let mut s = String::from("0\n");
        let _ = writeln!(s, "{}", epochs.len());
        for (e, st) in &epochs {
            let _ = writeln!(s, "{e} {st}");
        }
        let data = krabka_remote_storage::LogSegmentData {
            log_segment: ex.log_path.clone(),
            offset_index: ex.offset_index_path.clone(),
            time_index: ex.time_index_path.clone(),
            transaction_index: ex.transaction_index_path.clone(),
            producer_snapshot_index: None,
            leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
        };
        rsm.copy_log_segment_data(&md, &data).unwrap();
        rlmm.update_remote_log_segment_metadata(
            krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id,
                event_timestamp_ms: ex.max_timestamp,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            },
        )
        .unwrap();
    }

    (RemoteReader::new(rsm, rlmm), log)
}

/// Works like `populated_reader`, but before the copy it writes one
/// aborted-txn entry into the first sealed segment's `.txnindex`. The
/// entry is 24 BE bytes: `start_offset`, `last_offset`, and
/// `producer_id`. The copy path then carries it to the remote tier. It
/// returns the reader, the log, and the written
/// `(start_offset, last_offset, producer_id)`.
pub fn populated_reader_with_abort(
    log_dir: &std::path::Path,
    remote_dir: &std::path::Path,
) -> (RemoteReader, Log, (i64, i64, i64)) {
    let mut log = Log::open(
        log_dir,
        LogConfig {
            segment_size: krabka_units::bytes(256),
            ..LogConfig::default()
        },
    )
    .unwrap();
    for _ in 0..12 {
        let mut b = batch_of(2, 64);
        log.append(&mut b).unwrap();
    }
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "test needs multiple sealed segments");

    // Write a `.txnindex` next to the first sealed segment's `.log` so the
    // export below picks it up. The abort covers the whole first segment.
    let first = &exports[0];
    // Unwrap the log-layer `Offset`s into this helper's `i64` tuple at the seam.
    let abort = (first.base_offset.0, first.last_offset.0, 7777_i64);
    let mut txn_bytes = Vec::new();
    txn_bytes.extend_from_slice(&abort.0.to_be_bytes());
    txn_bytes.extend_from_slice(&abort.1.to_be_bytes());
    txn_bytes.extend_from_slice(&abort.2.to_be_bytes());
    let txn_path = first.log_path.with_extension("txnindex");
    std::fs::write(&txn_path, &txn_bytes).unwrap();

    // Re-derive exports so the first one now carries the txnindex path.
    let exports = log.tierable_segments();
    assert!(
        exports[0].transaction_index_path.is_some(),
        "first segment must now carry a .txnindex"
    );

    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    for ex in &exports {
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        // Unwrap the log-layer `Offset`s into the remote-storage metadata's
        // `i64` world at the seam.
        let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
            maplit::btreemap! {LeaderEpoch(0) => ex.base_offset.0}
        } else {
            ex.leader_epochs
                .iter()
                .map(|&(epoch, off)| (epoch, off.0))
                .collect()
        };
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            ex.base_offset.0,
            ex.last_offset.0,
            ex.max_timestamp,
            1,
            ex.max_timestamp,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                ex.size.bytes_i32(),
                RemoteLogSegmentState::CopySegmentStarted,
                epochs.clone(),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        let mut s = String::from("0\n");
        let _ = writeln!(s, "{}", epochs.len());
        for (e, st) in &epochs {
            let _ = writeln!(s, "{e} {st}");
        }
        let data = krabka_remote_storage::LogSegmentData {
            log_segment: ex.log_path.clone(),
            offset_index: ex.offset_index_path.clone(),
            time_index: ex.time_index_path.clone(),
            transaction_index: ex.transaction_index_path.clone(),
            producer_snapshot_index: None,
            leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
        };
        rsm.copy_log_segment_data(&md, &data).unwrap();
        rlmm.update_remote_log_segment_metadata(
            krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id,
                event_timestamp_ms: ex.max_timestamp,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            },
        )
        .unwrap();
    }

    (RemoteReader::new(rsm, rlmm), log, abort)
}

// `NotReady` from the RLMM must propagate out of the reader
// ── (not be swallowed as a miss), so the handlers can keep
// ── OFFSET_OUT_OF_RANGE / answer conservatively.

pub struct NotReadyRlmm;
impl RemoteLogMetadataManager for NotReadyRlmm {
    fn add_remote_log_segment_metadata(
        &self,
        _m: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        Ok(())
    }
    fn update_remote_log_segment_metadata(
        &self,
        _u: krabka_remote_storage::RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        Ok(())
    }
    fn remote_log_segment_metadata(
        &self,
        _tp: &TopicIdPartition,
        _epoch: LeaderEpoch,
        _offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady { partition: 3 })
    }
    fn highest_offset_for_epoch(
        &self,
        _tp: &TopicIdPartition,
        _epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        Ok(None)
    }
    fn list_remote_log_segments(
        &self,
        _tp: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady { partition: 3 })
    }
    fn list_remote_log_segments_by_epoch(
        &self,
        _tp: &TopicIdPartition,
        _epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Ok(Vec::new())
    }
    fn put_remote_partition_delete_metadata(
        &self,
        _m: krabka_remote_storage::RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        Ok(())
    }
}
