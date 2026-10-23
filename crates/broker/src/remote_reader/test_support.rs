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

use {
    super::RemoteReader,
    assert2::assert,
    krabka_ids::LeaderEpoch,
    krabka_log::{Log, LogConfig},
    krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
        RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager,
        TopicIdPartition,
    },
    krabka_units::convert::ByteSizeExt as _,
    std::{
        fmt::Write as _,
        sync::{Arc, Mutex},
    },
    uuid::Uuid,
};

pub fn tp() -> TopicIdPartition { TopicIdPartition::new(Uuid::from_u128(1), "orders", 0) }

use crate::{
    remote_log_manager::{ProducerSnapshotExport, export_epoch_map, export_segment_data},
    remote_reader::{ReaderPool, RemoteReaderLimits},
    test_support::{KeyedBatchSetup, KeyedRecordCount, keyed_records_batch as batch_of},
};

krabka_macros::timestamped_batch!(timestamped_batch_at);

/// One entry of an offset index: a batch's last offset, relative to the
/// segment's base, and where the batch starts in the segment.
struct OffsetIndexEntry {
    relative_offset: u32,
    position: u32,
}

/// One entry of a time index: a running maximum timestamp and the relative
/// offset of the batch that set it.
struct TimeIndexEntry {
    timestamp: i64,
    relative_offset: u32,
}

fn offset_index_bytes(entries: &[OffsetIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    for OffsetIndexEntry {
        relative_offset,
        position,
    } in entries
    {
        buf.extend_from_slice(&relative_offset.to_be_bytes());
        buf.extend_from_slice(&position.to_be_bytes());
    }
    buf
}

fn time_index_bytes(entries: &[TimeIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    for TimeIndexEntry {
        timestamp,
        relative_offset,
    } in entries
    {
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&relative_offset.to_be_bytes());
    }
    buf
}

/// A reader and the directory that holds the tier it reads.
pub struct ReaderWithDir {
    pub reader: RemoteReader,
    pub dir: tempfile::TempDir,
}

/// The reader built on a populated log, and that log.
pub struct PopulatedReader {
    pub reader: RemoteReader,
    pub log: Log,
}

fn write_test_file(dir: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

/// The requested bounds and returned byte count of each log-object read.
pub type LogFetches = Arc<Mutex<Vec<(u32, Option<u32>, usize)>>>;

/// A [`CountingRsm`] and the counter of the index objects it downloaded.
pub struct CountedRsm {
    pub rsm: Arc<CountingRsm>,
    pub index_fetches: Arc<std::sync::atomic::AtomicUsize>,
    pub log_fetches: LogFetches,
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
    log_fetches: LogFetches,
}

impl CountingRsm {
    pub fn counting(inner: Arc<dyn RemoteStorageManager>) -> CountedRsm {
        let index_fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let log_fetches = Arc::new(std::sync::Mutex::new(Vec::new()));
        CountedRsm {
            rsm: Arc::new(Self {
                inner,
                index_fetches: Arc::clone(&index_fetches),
                log_fetches: Arc::clone(&log_fetches),
            }),
            index_fetches,
            log_fetches,
        }
    }
}

/// Rebuilds `reader` over a counting RSM, retaining its metadata view.
pub fn counted_reader(
    reader: &RemoteReader,
) -> (RemoteReader, Arc<std::sync::atomic::AtomicUsize>) {
    let counted = CountingRsm::counting(Arc::clone(&reader.rsm));
    (
        RemoteReader::new(counted.rsm, Arc::clone(&reader.rlmm)),
        counted.index_fetches,
    )
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
        let bytes = self
            .inner
            .fetch_log_segment(metadata, start_position, end_position)?;
        self.log_fetches
            .lock()
            .unwrap()
            .push((start_position, end_position, bytes.len()));
        Ok(bytes)
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

/// A reader with the production index cache, the directories it reads from
/// and the counter of the index objects that reached the tier.
pub struct CachingReader {
    pub reader: RemoteReader,
    pub dirs: ReaderDirs,
    pub index_fetches: Arc<std::sync::atomic::AtomicUsize>,
}

/// The `sparse_remote_segment_reader` fixture, with the production index cache
/// turned on and a counting RSM underneath.
pub fn caching_sparse_remote_segment_reader() -> CachingReader {
    let ReaderWithDir {
        reader,
        dir: remote_dir,
    } = sparse_remote_segment_reader();
    let CountedRsm {
        rsm: counting,
        index_fetches,
        log_fetches: _,
    } = CountingRsm::counting(Arc::clone(&reader.rsm));
    let log_dir = tempfile::tempdir().unwrap();
    let cache = krabka_remote_storage::RemoteIndexCache::new(
        log_dir.path(),
        krabka_remote_storage::DEFAULT_INDEX_CACHE_TOTAL_SIZE_BYTES,
    )
    .unwrap();
    let cached =
        RemoteReader::with_limits(counting, Arc::clone(&reader.rlmm), RemoteReaderLimits {
            index_cache: Arc::new(cache),
            pool: ReaderPool::new(10, 100),
        });
    CachingReader {
        reader: cached,
        dirs: ReaderDirs {
            _remote: remote_dir,
            _log: log_dir,
        },
        index_fetches,
    }
}

pub fn sparse_remote_segment_reader_with_max_timestamp(
    max_timestamp_ms: i64,
) -> (RemoteReader, tempfile::TempDir) {
    let ReaderWithDir { reader, dir } = sparse_reader_with_max_timestamp(max_timestamp_ms);
    (reader, dir)
}

fn sparse_reader_with_max_timestamp(max_timestamp_ms: i64) -> ReaderWithDir {
    remote_segment_reader(
        &timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a'),
        &timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b'),
        max_timestamp_ms,
    )
}

/// The sparse fixture as stored on a `LogAppendTime` topic: producer time
/// remains in record deltas, while the batch maximum is the reader's time.
pub fn append_time_remote_segment_reader() -> ReaderWithDir {
    let stamped = |mut batch: krabka_protocol::records::RecordBatch| {
        batch.attributes = batch
            .attributes
            .with_timestamp_type(krabka_protocol::records::TimestampType::LogAppendTime);
        batch
    };
    remote_segment_reader(
        &stamped(timestamped_batch_at(
            10,
            &[1_000, 1_100, 1_600, 1_700],
            b'a',
        )),
        &stamped(timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b')),
        2_400,
    )
}

/// The encoded length of the second batch of the sparse fixture, which is the
/// smallest byte budget that still holds a batch read from its own start.
pub fn sparse_fixture_second_batch_len() -> usize { sparse_fixture_batch_bytes()[1].len() }

/// The two batches of the sparse fixture, as its segment stores them: offsets
/// 10 to 13, then 14 to 16.
pub fn sparse_fixture_batch_bytes() -> [bytes::Bytes; 2] {
    let encode = |batch: krabka_protocol::records::RecordBatch| {
        let mut buf = bytes::BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    };
    [
        encode(timestamped_batch_at(
            10,
            &[1_000, 1_100, 1_600, 1_700],
            b'a',
        )),
        encode(timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b')),
    ]
}

/// A remote segment whose second batch carries its newest record first, so the
/// record the time index's entry points at is not the last one of the batch.
pub fn unordered_timestamps_remote_segment_reader() -> ReaderWithDir {
    remote_segment_reader(
        &timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a'),
        &timestamped_batch_at(14, &[2_400, 2_000, 2_200], b'b'),
        2_400,
    )
}

/// The sparse fixture with both batches gzip-compressed and every record of the
/// second one carrying a `second_value_len`-byte value, so a limit between the
/// two record sizes tells the batches apart.
pub fn compressed_remote_segment_reader(second_value_len: usize) -> ReaderWithDir {
    use krabka_compression::CompressionType;

    let compressed = |mut batch: krabka_protocol::records::RecordBatch| {
        batch.attributes = batch.attributes.with_compression(CompressionType::Gzip);
        batch
    };
    let mut second = timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b');
    for record in &mut second.records {
        record.value = Some(bytes::Bytes::from(vec![b'b'; second_value_len]));
    }
    remote_segment_reader(
        &compressed(timestamped_batch_at(
            10,
            &[1_000, 1_100, 1_600, 1_700],
            b'a',
        )),
        &compressed(second),
        2_400,
    )
}

/// Copies a two-batch segment into a fresh tier, with the indexes Kafka would
/// write for it at an index interval of one batch: the offset index holds each
/// batch's last offset and start position, and the time index holds each
/// running maximum with the last offset of the batch that set it.
fn remote_segment_reader(
    first: &krabka_protocol::records::RecordBatch,
    second: &krabka_protocol::records::RecordBatch,
    max_timestamp_ms: i64,
) -> ReaderWithDir {
    let source_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();

    let mut log_bytes = bytes::BytesMut::new();
    first.encode(&mut log_bytes).unwrap();
    let second_position = u32::try_from(log_bytes.len()).unwrap();
    second.encode(&mut log_bytes).unwrap();
    let log_bytes = log_bytes.freeze();

    let log_path = write_test_file(source_dir.path(), "00000000000000000010.log", &log_bytes);
    let offset_index_path = write_test_file(
        source_dir.path(),
        "00000000000000000010.index",
        &offset_index_bytes(&[
            OffsetIndexEntry {
                relative_offset: 3,
                position: 0,
            },
            OffsetIndexEntry {
                relative_offset: 6,
                position: second_position,
            },
        ]),
    );
    let time_index_path = write_test_file(
        source_dir.path(),
        "00000000000000000010.timeindex",
        &time_index_bytes(&[
            TimeIndexEntry {
                timestamp: 1_700,
                relative_offset: 3,
            },
            TimeIndexEntry {
                timestamp: 2_400,
                relative_offset: 6,
            },
        ]),
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

    ReaderWithDir {
        reader: RemoteReader::new(rsm, rlmm),
        dir: remote_dir,
    }
}

pub fn sparse_remote_segment_reader() -> ReaderWithDir { sparse_reader_with_max_timestamp(2_400) }

/// One finished remote segment of [`tier_reader`]: its batches, in offset
/// order, the leader epochs its metadata records, and whether its metadata
/// carries the unknown max timestamp `-1` instead of its largest record
/// timestamp.
pub struct TierSegment {
    pub batches: Vec<krabka_protocol::records::RecordBatch>,
    pub epochs: std::collections::BTreeMap<LeaderEpoch, i64>,
    pub unknown_max_timestamp: bool,
}

/// A [`TierSegment`] of one batch at `base` with `timestamps`, in `epoch` from
/// `base`, whose metadata carries the unknown max timestamp when
/// `unknown_max` is set.
pub fn single_batch_tier_segment(
    base: i64,
    timestamps: &[i64],
    epoch: i32,
    unknown_max: bool,
) -> TierSegment {
    TierSegment {
        batches: vec![timestamped_batch_at(base, timestamps, b'v')],
        epochs: maplit::btreemap! {LeaderEpoch(epoch) => base},
        unknown_max_timestamp: unknown_max,
    }
}

/// A tier holding `segments`, each copied with the indexes Kafka writes at an
/// index interval of one batch, as `remote_segment_reader` does for one.
/// A segment spans its first batch's base offset to its last batch's last
/// offset, and its max timestamp is its largest record timestamp unless it
/// asks for the unknown one.
pub fn tier_reader(segments: &[TierSegment]) -> ReaderWithDir {
    let remote_dir = tempfile::tempdir().unwrap();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    for segment in segments {
        let source_dir = tempfile::tempdir().unwrap();
        let first = segment.batches.first().expect("a segment holds a batch");
        let base = first.base_offset;
        let mut log_bytes = bytes::BytesMut::new();
        let mut offsets = Vec::new();
        let mut times = Vec::new();
        let mut max_timestamp = i64::MIN;
        let mut end = base;
        for batch in &segment.batches {
            let position = u32::try_from(log_bytes.len()).unwrap();
            batch.encode(&mut log_bytes).unwrap();
            end = batch.base_offset + i64::from(batch.last_offset_delta);
            let relative_offset = u32::try_from(end - base).unwrap();
            offsets.push(OffsetIndexEntry {
                relative_offset,
                position,
            });
            let batch_max = batch
                .records
                .iter()
                .map(|record| batch.base_timestamp + record.timestamp_delta)
                .max()
                .unwrap_or(-1);
            if batch_max > max_timestamp {
                max_timestamp = batch_max;
                times.push(TimeIndexEntry {
                    timestamp: batch_max,
                    relative_offset,
                });
            }
        }
        let name = format!("{base:020}");
        let data = krabka_remote_storage::LogSegmentData {
            log_segment: write_test_file(source_dir.path(), &format!("{name}.log"), &log_bytes),
            offset_index: write_test_file(
                source_dir.path(),
                &format!("{name}.index"),
                &offset_index_bytes(&offsets),
            ),
            time_index: write_test_file(
                source_dir.path(),
                &format!("{name}.timeindex"),
                &time_index_bytes(&times),
            ),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: bytes::Bytes::from_static(b"0\n0\n"),
        };
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            base,
            end,
            if segment.unknown_max_timestamp {
                -1
            } else {
                max_timestamp
            },
            1,
            max_timestamp,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                i32::try_from(log_bytes.len()).unwrap(),
                RemoteLogSegmentState::CopySegmentStarted,
                segment.epochs.clone(),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        rsm.copy_log_segment_data(&md, &data).unwrap();
        rlmm.update_remote_log_segment_metadata(
            krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id,
                event_timestamp_ms: max_timestamp,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            },
        )
        .unwrap();
    }
    ReaderWithDir {
        reader: RemoteReader::new(rsm, rlmm),
        dir: remote_dir,
    }
}

/// A tier an unclean leader election left behind. The current lineage is
/// epoch 0 from offset 0 and epoch 1 from offset 5, with the log end at 10:
/// - offsets 0 to 4 in epoch 0, timestamps 100 to 140;
/// - a deposed leader's copy of offsets 3 to 9, all in epoch 0, which the
///   lineage gives to epoch 1 from offset 5 on: its offset 5 has timestamp
///   1 000;
/// - offsets 5 to 9 in epoch 1, timestamps 900, 950, then 1 100 at offset 7.
pub fn divergent_tier_reader() -> ReaderWithDir {
    tier_reader(&[
        TierSegment {
            batches: vec![timestamped_batch_at(0, &[100, 110, 120, 130, 140], b'a')],
            epochs: maplit::btreemap! {LeaderEpoch(0) => 0},
            unknown_max_timestamp: false,
        },
        TierSegment {
            batches: vec![timestamped_batch_at(
                3,
                &[130, 140, 1_000, 1_010, 1_020, 1_030, 1_040],
                b'x',
            )],
            epochs: maplit::btreemap! {LeaderEpoch(0) => 3},
            unknown_max_timestamp: false,
        },
        TierSegment {
            batches: vec![timestamped_batch_at(
                5,
                &[900, 950, 1_100, 1_110, 1_120],
                b'b',
            )],
            epochs: maplit::btreemap! {LeaderEpoch(1) => 5},
            unknown_max_timestamp: false,
        },
    ])
}

/// Builds a log rolled into several sealed segments under `dir`, then
/// copies every sealed segment into a fresh `LocalTieredStorage` and
/// `InmemoryRemoteLogMetadataManager`. It returns the constructed reader
/// and the log. The caller keeps the log alive so that the on-disk files
/// outlive the call.
pub fn populated_reader(
    log_dir: &std::path::Path,
    remote_dir: &std::path::Path,
) -> PopulatedReader {
    let log = populated_log(log_dir, SEGMENT_BYTES);
    PopulatedReader {
        reader: copy_log_to_reader(&log, remote_dir),
        log,
    }
}

/// The segment size of [`populated_reader`]'s log, which seals one two-record
/// batch per segment.
const SEGMENT_BYTES: u32 = 256;

fn populated_log(log_dir: &std::path::Path, segment_bytes: u32) -> Log {
    let mut log = Log::open(log_dir, LogConfig {
        segment_size: krabka_units::bytes(segment_bytes),
        ..LogConfig::default()
    })
    .unwrap();
    for _ in 0..12 {
        let mut b = batch_of(KeyedBatchSetup {
            records: KeyedRecordCount(2),
            ..Default::default()
        });
        log.append(&mut b).unwrap();
    }
    log.sync().unwrap();
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "test needs multiple sealed segments");

    log
}

fn copy_log_to_reader(log: &Log, remote_dir: &std::path::Path) -> RemoteReader {
    let exports = log.tierable_segments();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
    let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(InmemoryRemoteLogMetadataManager::new());
    // Manually copy each segment as `CopySegmentStarted` →
    // `CopySegmentFinished` (mirrors the copy path's copy_eligible
    // without the broker-side dependencies).
    for ex in &exports {
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        // Unwrap the log-layer `Offset`s into the remote-storage metadata's
        // `i64` world at the seam.
        let epochs = export_epoch_map(ex, LeaderEpoch(0));
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
        .unwrap()
        .with_txn_index_empty(ex.transaction_index_path.is_none());
        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        // Render the leader-epoch checkpoint the same way the copy path
        // does so `fetch_index(LeaderEpoch)` returns real bytes.
        let mut s = String::from("0\n");
        let _ = writeln!(s, "{}", epochs.len());
        for (e, st) in &epochs {
            let _ = writeln!(s, "{e} {st}");
        }
        let data = export_segment_data(ex, ProducerSnapshotExport::Omit, || {
            bytes::Bytes::from(s.into_bytes())
        });
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

    RemoteReader::new(rsm, rlmm)
}

/// Works like `populated_reader`, but before the copy it writes one
/// aborted-txn entry into the first sealed segment's `.txnindex`. The
/// entry is 34 BE bytes: a 2-byte version (0), then `producer_id`,
/// `start_offset`, `last_offset`, and `last_stable_offset`. The copy path
/// then carries it to the remote tier. It returns the reader, the log, and
/// the written `(start_offset, last_offset, producer_id)`.
pub fn populated_reader_with_abort(
    log_dir: &std::path::Path,
    remote_dir: &std::path::Path,
) -> (RemoteReader, Log, (i64, i64, i64)) {
    reader_with_abort_in_segments_of(log_dir, remote_dir, SEGMENT_BYTES)
}

/// [`populated_reader_with_abort`] over a log whose segments are
/// `segment_bytes` long, so a test can ask for segments that hold several
/// batches.
pub fn reader_with_abort_in_segments_of(
    log_dir: &std::path::Path,
    remote_dir: &std::path::Path,
    segment_bytes: u32,
) -> (RemoteReader, Log, (i64, i64, i64)) {
    let log = populated_log(log_dir, segment_bytes);
    let exports = log.tierable_segments();

    // Write a `.txnindex` next to the first sealed segment's `.log` so the
    // export below picks it up. The abort covers the whole first segment.
    let first = &exports[0];
    // Unwrap the log-layer `Offset`s into this helper's `i64` tuple at the seam.
    let abort = (first.base_offset.0, first.last_offset.0, 7777_i64);
    let mut txn_bytes = Vec::new();
    txn_bytes.extend_from_slice(&0_i16.to_be_bytes()); // version
    txn_bytes.extend_from_slice(&abort.2.to_be_bytes()); // producer_id
    txn_bytes.extend_from_slice(&abort.0.to_be_bytes()); // start_offset
    txn_bytes.extend_from_slice(&abort.1.to_be_bytes()); // last_offset
    txn_bytes.extend_from_slice(&(abort.1 + 1).to_be_bytes()); // last_stable_offset
    let txn_path = first.log_path.with_extension("txnindex");
    std::fs::write(&txn_path, &txn_bytes).unwrap();

    // Re-derive exports so the first one now carries the txnindex path.
    let exports = log.tierable_segments();
    assert!(
        exports[0].transaction_index_path.is_some(),
        "first segment must now carry a .txnindex"
    );

    (copy_log_to_reader(&log, remote_dir), log, abort)
}

/// Registers a finished epoch-0 segment over `start..=end` in the reader's
/// metadata and uploads nothing for it, so its index objects are missing.
///
/// Placed inside a segment that is already there, it is the nested segment
/// that shadows its host in the RLMM's floor-by-start lookup for every offset
/// of the host above `end`.
pub fn nest_finished_segment(reader: &RemoteReader, start: i64, end: i64) {
    nest_finished_segment_with_empty_flag(reader, start, end, false);
}

/// Registers a finished segment which authoritatively has no transaction index.
pub fn nest_finished_segment_with_txn_index_empty(reader: &RemoteReader, start: i64, end: i64) {
    nest_finished_segment_with_empty_flag(reader, start, end, true);
}

fn nest_finished_segment_with_empty_flag(
    reader: &RemoteReader,
    start: i64,
    end: i64,
    txn_index_empty: bool,
) {
    let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
    let md = RemoteLogSegmentMetadata::new(
        id.clone(),
        start,
        end,
        0,
        1,
        0,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            1,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0_i32) => start},
        ),
    )
    .unwrap()
    .with_txn_index_empty(txn_index_empty);
    reader.rlmm.add_remote_log_segment_metadata(md).unwrap();
    reader
        .rlmm
        .update_remote_log_segment_metadata(krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id,
            event_timestamp_ms: 0,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
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

    fn caught_up(&self, _tp: &TopicIdPartition) -> Result<bool, RemoteStorageError> {
        Err(RemoteStorageError::NotReady { partition: 3 })
    }
}

/// A metadata manager whose cache lags the store: it serves `inner`'s
/// answers, but [`RemoteLogMetadataManager::caught_up`] says `false`, so a
/// caller that needs a current view must not trust them.
pub struct LaggingRlmm(pub Arc<dyn RemoteLogMetadataManager>);
impl RemoteLogMetadataManager for LaggingRlmm {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.0.add_remote_log_segment_metadata(metadata)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: krabka_remote_storage::RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        self.0.update_remote_log_segment_metadata(update)
    }

    fn remote_log_segment_metadata(
        &self,
        tp: &TopicIdPartition,
        epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.0.remote_log_segment_metadata(tp, epoch, offset)
    }

    fn highest_offset_for_epoch(
        &self,
        tp: &TopicIdPartition,
        epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        self.0.highest_offset_for_epoch(tp, epoch)
    }

    fn list_remote_log_segments(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.0.list_remote_log_segments(tp)
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        tp: &TopicIdPartition,
        epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.0.list_remote_log_segments_by_epoch(tp, epoch)
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: krabka_remote_storage::RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.0.put_remote_partition_delete_metadata(metadata)
    }

    fn caught_up(&self, _tp: &TopicIdPartition) -> Result<bool, RemoteStorageError> { Ok(false) }
}

pub fn reader_with_metadata(
    make_metadata: impl FnOnce() -> Arc<dyn RemoteLogMetadataManager>,
) -> ReaderWithDir {
    let remote_dir = tempfile::tempdir().unwrap();
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir.path()));
    let rlmm = make_metadata();
    let reader = RemoteReader::new(rsm, rlmm);
    ReaderWithDir {
        reader,
        dir: remote_dir,
    }
}

pub fn empty_reader() -> (tempfile::TempDir, RemoteReader) {
    let ReaderWithDir { reader, dir } =
        reader_with_metadata(|| Arc::new(InmemoryRemoteLogMetadataManager::new()));
    (dir, reader)
}

pub fn not_ready_reader() -> ReaderWithDir { reader_with_metadata(|| Arc::new(NotReadyRlmm)) }

/// A populated reader and source log with both directory guards retained by its caller.
pub struct ReaderFixture {
    pub local: tempfile::TempDir,
    pub remote: tempfile::TempDir,
    pub reader: RemoteReader,
    pub log: Log,
}
/// Build the reader while retaining its directory guards and source log in the caller.
pub fn reader_fixture() -> ReaderFixture {
    let local = tempfile::tempdir().unwrap();
    let remote = tempfile::tempdir().unwrap();
    let PopulatedReader { reader, log } = populated_reader(local.path(), remote.path());
    ReaderFixture {
        local,
        remote,
        reader,
        log,
    }
}
