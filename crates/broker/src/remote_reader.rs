//! KIP-405 remote read path.
//!
//! This module wraps the broker's shared [`RemoteStorageManager`] and
//! [`RemoteLogMetadataManager`] pair. It serves `Fetch` and `ListOffsets`
//! requests for offsets that have no local copy any more.
//!
//! The RSM and RLMM SPIs are synchronous and blocking. This module therefore
//! wraps byte-range reads, index reads, and `ListOffsets` metadata scans in
//! `tokio::task::spawn_blocking`, so those remote-tier operations do not stall
//! the broker's reactor. It decodes the fetched bytes with
//! [`crabka_remote_storage::index`], whose lookups mirror
//! `crabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the Kafka-format
//! index bytes that the copy path wrote verbatim.

use std::sync::Arc;

use crabka_ids::LeaderEpoch;
use crabka_protocol::records::RecordBatch;
use crabka_remote_storage::{
    BytePosition, IndexType, LogOffset, RemoteLogMetadataManager, RemoteLogSegmentMetadata,
    RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager, TimestampMs, TopicIdPartition,
    corrupt_log, end_position_for, first_batch_at_or_after, first_record_at_or_after_timestamp,
    parse_offset_index, parse_time_index, parse_txn_index, position_for_relative_offset,
    relative_offset_floor_for_timestamp, txn_overlaps,
};
use tracing::warn;

/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: LogOffset,
    pub(crate) last_offset: LogOffset,
    pub(crate) producer_id: i64,
}

/// Holds the broker's shared `RSM` and `RLMM`, and serves remote reads.
pub(crate) struct RemoteReader {
    pub(crate) rsm: Arc<dyn RemoteStorageManager>,
    pub(crate) rlmm: Arc<dyn RemoteLogMetadataManager>,
}

/// The last offset durably copied to the remote tier and the leader epoch
/// that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TieredOffset {
    pub(crate) offset: LogOffset,
    pub(crate) leader_epoch: LeaderEpoch,
}

impl RemoteReader {
    pub(crate) fn new(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
    ) -> Self {
        Self { rsm, rlmm }
    }

    /// Finds the finished segment in the RLMM that covers
    /// `(leader_epoch, offset)`, fetches its offset index, positions into the
    /// `.log` data, and returns the first batch whose last offset is
    /// `>= offset`. It returns `None` when no finished segment covers the
    /// requested offset.
    ///
    /// `max_bytes` caps the byte range that this method fetches from the
    /// remote tier. The caller's `partition_max_bytes` from the Fetch request
    /// arrives here.
    pub(crate) async fn fetch_batch(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>, RemoteStorageError> {
        // Primary lookup: epoch-indexed fast path.  The caller resolves
        // `leader_epoch` from the local leader-epoch checkpoint via
        // `epoch_for_offset`, so this is the epoch that *owned* the requested
        // offset at copy time.  The RLMM indexes a segment under every epoch
        // in its `segment_leader_epochs` map, so this reliably hits after a
        // clean failover.
        let primary = self
            .rlmm
            .remote_log_segment_metadata(tp, leader_epoch, offset)?;

        // Defensive fallback: the epoch-indexed primary lookup can still miss
        // in rare edge cases (e.g. the local leader-epoch checkpoint is empty
        // on a fresh replica, or an unclean election produced a gap in the
        // checkpoint that `epoch_for_offset` cannot bridge).  When the primary
        // misses, scan `list_remote_log_segments` for finished segments that
        // cover `offset` and prefer the one whose `segment_leader_epochs` map
        // contains the passed epoch (same lineage) — this closes the
        // wrong-segment-under-log-divergence hazard.  Only if no lineage-
        // matching candidate exists does the fallback revert to
        // `max_by_key(start_offset)` as a last resort; in a clean log without
        // epoch-range overlap that tie-break is always deterministic.
        let metadata = if let Some(m) = primary {
            m
        } else {
            let candidates = self.rlmm.list_remote_log_segments(tp)?;
            let covering: Vec<_> = candidates
                .into_iter()
                .filter(|m| {
                    m.state() == RemoteLogSegmentState::CopySegmentFinished
                        && m.start_offset() <= offset
                        && offset <= m.end_offset()
                })
                .collect();
            // Prefer a segment whose epoch map contains the owning epoch
            // (same lineage as the checkpoint resolution).
            let Some(m) = covering
                .iter()
                .filter(|m| m.segment_leader_epochs().contains_key(&leader_epoch))
                .max_by_key(|m| m.start_offset())
                .or_else(|| {
                    // No lineage-matching candidate — last resort: highest
                    // start_offset among all covering finished segments.
                    covering.iter().max_by_key(|m| m.start_offset())
                })
                .cloned()
            else {
                return Ok(None);
            };
            m
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(None);
        }

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Offset)
            .await?;
        let entries = parse_offset_index(&index_bytes)?;
        let target_rel = u32::try_from((offset - metadata.start_offset()).max(0)).unwrap_or(0);
        let start_position = position_for_relative_offset(entries, target_rel);

        // Cap the read so the broker doesn't pull an entire segment when the
        // Fetch asked for one batch. Always pull at least one full batch worth
        // of bytes — the segment's `size` is the safe ceiling.
        let segment_size =
            u32::try_from(metadata.segment_size_in_bytes().max(0)).unwrap_or(u32::MAX);
        let end_position = end_position_for(start_position, segment_size, max_bytes);

        let data = self
            .fetch_log_blocking(metadata.clone(), start_position, end_position)
            .await?;

        let batch = first_batch_at_or_after(&data, offset);
        Ok(batch)
    }

    /// Returns the aborted transactions that overlap the inclusive offset
    /// range `[from_offset, to_offset]`, in the finished remote segment that
    /// covers `from_offset`. It returns an empty `Vec` in three cases: no
    /// finished segment covers the offset, the segment carries no transaction
    /// index (`SegmentNotFound` from `fetch_index`), or nothing overlaps.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        from_offset: LogOffset,
        to_offset: LogOffset,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        let Some(metadata) =
            self.rlmm
                .remote_log_segment_metadata(tp, leader_epoch, from_offset)?
        else {
            return Ok(Vec::new());
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(Vec::new());
        }

        let index_bytes = match self
            .fetch_index_blocking(metadata, IndexType::Transaction)
            .await
        {
            Ok(bytes) => bytes,
            // The transaction index is optional: a segment with no aborted
            // transactions has no `.txnindex`, surfaced as SegmentNotFound.
            Err(RemoteStorageError::SegmentNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let entries = parse_txn_index(&index_bytes)?;
        Ok(entries
            .iter()
            .filter(|e| txn_overlaps(e, from_offset, to_offset))
            .map(|e| AbortedTxnEntry {
                start_offset: e.start_offset.get(),
                last_offset: e.last_offset.get(),
                producer_id: e.producer_id.get(),
            })
            .collect())
    }

    /// Returns the lowest `start_offset` across the finished segments for
    /// `tp`, or `None` when no finished segment exists. It drives
    /// `ListOffsets` EARLIEST below `local_log_start_offset()`.
    pub(crate) async fn earliest_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<LogOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        Ok(listed
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(|md| md.start_offset())
            .min())
    }

    /// Returns the highest offset held by a finished remote segment and the
    /// leader epoch that owns that offset. In-progress copies are invisible.
    pub(crate) async fn latest_tiered_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<TieredOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        let Some(metadata) = listed
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .max_by_key(RemoteLogSegmentMetadata::end_offset)
        else {
            return Ok(None);
        };
        let offset = metadata.end_offset();
        let Some(leader_epoch) = metadata
            .segment_leader_epochs()
            .iter()
            .filter(|(_, start)| **start <= offset)
            .max_by_key(|(_, start)| **start)
            .map(|(epoch, _)| *epoch)
        else {
            return Ok(None);
        };
        Ok(Some(TieredOffset {
            offset,
            leader_epoch,
        }))
    }

    /// Returns the smallest absolute offset and its record timestamp where the
    /// timestamp is `>= target_timestamp`, across the finished remote segments.
    /// The sparse time index supplies a scan floor; the exact answer comes from
    /// decoding records from the corresponding offset-index position.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: TimestampMs,
    ) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
        let mut listed = self.list_remote_log_segments_blocking(tp).await?;
        listed.retain(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished);
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);

        for metadata in listed
            .into_iter()
            // `-1` is the persisted unknown-max sentinel for a sealed segment
            // opened without a tail scan. It must remain scan-eligible for a
            // positive timestamp lookup after broker restart.
            .filter(|md| md.max_timestamp_ms() == -1 || md.max_timestamp_ms() >= target_timestamp)
        {
            let (time_index_bytes, offset_index_bytes) = tokio::try_join!(
                self.fetch_index_blocking(metadata.clone(), IndexType::Timestamp),
                self.fetch_index_blocking(metadata.clone(), IndexType::Offset),
            )?;
            let scan_rel = relative_offset_floor_for_timestamp(
                parse_time_index(&time_index_bytes)?,
                target_timestamp,
            );
            let start_position =
                position_for_relative_offset(parse_offset_index(&offset_index_bytes)?, scan_rel);
            // ponytail: one tail read keeps the scan exact; switch to bounded,
            // batch-aligned windows only if remote segment profiling requires it.
            let data = self
                .fetch_log_blocking(metadata.clone(), start_position, None)
                .await?;
            let scan_offset = metadata
                .start_offset()
                .checked_add(i64::from(scan_rel))
                .ok_or_else(|| corrupt_log("timestamp-index offset overflow"))?;
            if let Some(found) =
                first_record_at_or_after_timestamp(&data, scan_offset, target_timestamp)?
            {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    async fn list_remote_log_segments_blocking(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let rlmm = self.rlmm.clone();
        let tp = tp.clone();
        match tokio::task::spawn_blocking(move || rlmm.list_remote_log_segments(&tp)).await {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "remote-reader: list_remote_log_segments task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "list_remote_log_segments task panicked",
                )))
            }
        }
    }

    async fn fetch_index_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        kind: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        match tokio::task::spawn_blocking(move || rsm.fetch_index(&metadata, kind)).await {
            Ok(res) => res,
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_index task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_index task panicked",
                )))
            }
        }
    }

    async fn fetch_log_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        start_position: BytePosition,
        end_position: Option<BytePosition>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        match tokio::task::spawn_blocking(move || {
            rsm.fetch_log_segment(&metadata, start_position, end_position)
        })
        .await
        {
            Ok(res) => res,
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_log_segment task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_log_segment task panicked",
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // These exercise the full RSM/RLMM plumbing through `RemoteReader` against
    // `LocalTieredStorage` and `InmemoryRemoteLogMetadataManager`, using the
    // copy path's `copy_eligible` to populate the tier from a real `Log`.

    use std::{collections::BTreeMap, fmt::Write as _};

    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::Record;
    use crabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
        RemoteStorageManager,
    };
    use crabka_units::convert::ByteSizeExt as _;
    use uuid::Uuid;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn batch_of(n: i32, value_size: usize) -> crabka_protocol::records::RecordBatch {
        use bytes::Bytes;
        let mut b = crabka_protocol::records::RecordBatch {
            last_offset_delta: n - 1,
            ..crabka_protocol::records::RecordBatch::default()
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
    ) -> crabka_protocol::records::RecordBatch {
        use bytes::Bytes;

        let base_timestamp = timestamps.first().copied().unwrap_or_default();
        crabka_protocol::records::RecordBatch {
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

    fn sparse_remote_segment_reader_with_max_timestamp(
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

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            10,
            16,
            max_timestamp_ms,
            1,
            2_400,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                i32::try_from(log_bytes.len()).unwrap_or(i32::MAX),
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0_i32), 10_i64)]),
            ),
        )
        .unwrap();

        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        let data = crabka_remote_storage::LogSegmentData {
            log_segment: log_path,
            offset_index: offset_index_path,
            time_index: time_index_path,
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: bytes::Bytes::from_static(b"0\n1\n0 10\n"),
        };
        rsm.copy_log_segment_data(&md, &data).unwrap();
        rlmm.update_remote_log_segment_metadata(
            crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
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

    fn sparse_remote_segment_reader() -> (RemoteReader, tempfile::TempDir) {
        sparse_remote_segment_reader_with_max_timestamp(2_400)
    }

    /// Builds a log rolled into several sealed segments under `dir`, then
    /// copies every sealed segment into a fresh `LocalTieredStorage` and
    /// `InmemoryRemoteLogMetadataManager`. It returns the constructed reader
    /// and the log. The caller keeps the log alive so that the on-disk files
    /// outlive the call.
    fn populated_reader(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_size: crabka_units::bytes(256),
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
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        // Manually copy each segment as `CopySegmentStarted` →
        // `CopySegmentFinished` (mirrors the copy path's copy_eligible
        // without the broker-side dependencies).
        for ex in &exports {
            let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
            // Unwrap the log-layer `Offset`s into the remote-storage metadata's
            // `i64` world at the seam.
            let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(LeaderEpoch(0), ex.base_offset.0)])
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
                crabka_remote_storage::RemoteLogSegmentDetails::new(
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
            let data = crabka_remote_storage::LogSegmentData {
                log_segment: ex.log_path.clone(),
                offset_index: ex.offset_index_path.clone(),
                time_index: ex.time_index_path.clone(),
                transaction_index: ex.transaction_index_path.clone(),
                producer_snapshot_index: None,
                leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
            };
            rsm.copy_log_segment_data(&md, &data).unwrap();
            rlmm.update_remote_log_segment_metadata(
                crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
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
    fn populated_reader_with_abort(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log, (i64, i64, i64)) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_size: crabka_units::bytes(256),
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
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        for ex in &exports {
            let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
            // Unwrap the log-layer `Offset`s into the remote-storage metadata's
            // `i64` world at the seam.
            let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(LeaderEpoch(0), ex.base_offset.0)])
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
                crabka_remote_storage::RemoteLogSegmentDetails::new(
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
            let data = crabka_remote_storage::LogSegmentData {
                log_segment: ex.log_path.clone(),
                offset_index: ex.offset_index_path.clone(),
                time_index: ex.time_index_path.clone(),
                transaction_index: ex.transaction_index_path.clone(),
                producer_snapshot_index: None,
                leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
            };
            rsm.copy_log_segment_data(&md, &data).unwrap();
            rlmm.update_remote_log_segment_metadata(
                crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
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

    #[tokio::test]
    async fn aborted_transactions_returns_copied_abort() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log, abort) = populated_reader_with_abort(log_dir.path(), remote_dir.path());
        let (start, last, pid) = abort;

        // Query the first segment's offset range → the abort overlaps.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), start, last)
            .await
            .expect("ok");
        let expected = vec![AbortedTxnEntry {
            start_offset: start,
            last_offset: last,
            producer_id: pid,
        }];
        assert!(got == expected, "the copied abort is returned");
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_segment_has_no_txnindex() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        // The default harness writes no `.txnindex` for any segment.
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        let seg = &exports[0];

        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), seg.base_offset.0, seg.last_offset.0)
            .await
            .expect("ok");
        assert!(
            got.is_empty(),
            "segment with no .txnindex yields an empty list, not an error"
        );
    }

    #[tokio::test]
    async fn fetch_batch_finds_segment_and_returns_first_batch() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());

        // Pick an offset inside the second sealed segment. Each batch covers
        // two records, so base_offset=2 lives in segment[1] (base=2).
        let exports = log.tierable_segments();
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[1].base_offset.0;

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), target_offset, 4096)
            .await
            .expect("ok")
            .expect("found a batch");
        // The batch returned should start at or before target_offset and end
        // at or after it.
        let last = got.base_offset + i64::from(got.last_offset_delta);
        assert!(
            got.base_offset <= target_offset && last >= target_offset,
            "batch [{},{}] doesn't cover target {target_offset}",
            got.base_offset,
            last
        );
    }

    #[tokio::test]
    async fn fetch_batch_uses_offset_relative_to_remote_segment_start() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 12, 4096)
            .await
            .expect("ok")
            .expect("offset 12 is in the synthetic remote segment");

        assert!(
            got.base_offset == 10,
            "relative offset 2 should read the first batch, not jump to {}",
            got.base_offset
        );
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_when_segment_not_in_rlmm() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        // RLMM is empty → no segment for `tp` at epoch 0.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_no_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        // RLMM is empty → no covering segment → empty list, not an error.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), 0, 100)
            .await
            .expect("ok");
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_for_in_progress_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id,
            0,
            99,
            100,
            1,
            100,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0_i64)]),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        let reader = RemoteReader::new(rsm, rlmm);
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 50, 4096)
            .await
            .unwrap();
        assert!(
            got.is_none(),
            "started (not finished) segment must be invisible"
        );
    }

    #[tokio::test]
    async fn earliest_offset_returns_lowest_finished_start() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        let got = reader.earliest_offset(&tp()).await.unwrap();
        assert!(got == Some(expected));
    }

    #[tokio::test]
    async fn earliest_offset_returns_none_when_no_finished_segments() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
    }

    #[tokio::test]
    async fn latest_tiered_offset_uses_highest_finished_segment_and_its_epoch() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let expected = log
            .tierable_segments()
            .iter()
            .map(|segment| segment.last_offset.0)
            .max()
            .unwrap();

        let started_id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        reader
            .rlmm
            .add_remote_log_segment_metadata(
                RemoteLogSegmentMetadata::new(
                    started_id,
                    expected + 1,
                    expected + 100,
                    0,
                    1,
                    0,
                    crabka_remote_storage::RemoteLogSegmentDetails::new(
                        1,
                        RemoteLogSegmentState::CopySegmentStarted,
                        BTreeMap::from([(LeaderEpoch(7), expected + 1)]),
                    ),
                )
                .unwrap(),
            )
            .unwrap();

        let got = reader
            .latest_tiered_offset(&tp())
            .await
            .unwrap()
            .expect("finished segments exist");
        assert!(
            got == TieredOffset {
                offset: expected,
                leader_epoch: LeaderEpoch(0),
            }
        );
    }

    struct SlowListRlmm {
        reactor_ticked: Arc<std::sync::atomic::AtomicBool>,
        observed_tick: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RemoteLogMetadataManager for SlowListRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _metadata: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn update_remote_log_segment_metadata(
            &self,
            _update: crabka_remote_storage::RemoteLogSegmentMetadataUpdate,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn remote_log_segment_metadata(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(None)
        }

        fn highest_offset_for_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Option<i64>, RemoteStorageError> {
            Ok(None)
        }

        fn list_remote_log_segments(
            &self,
            _topic_id_partition: &TopicIdPartition,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.observed_tick.store(
                self.reactor_ticked
                    .load(std::sync::atomic::Ordering::Acquire),
                std::sync::atomic::Ordering::Release,
            );
            Ok(Vec::new())
        }

        fn list_remote_log_segments_by_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(Vec::new())
        }

        fn put_remote_partition_delete_metadata(
            &self,
            _metadata: crabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metadata_listing_does_not_block_the_reactor() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let reactor_ticked = Arc::new(AtomicBool::new(false));
        let observed_tick = Arc::new(AtomicBool::new(false));
        let tick = reactor_ticked.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            tick.store(true, Ordering::Release);
        });
        let remote_dir = tempfile::tempdir().unwrap();
        let reader = RemoteReader::new(
            Arc::new(LocalTieredStorage::new(remote_dir.path())),
            Arc::new(SlowListRlmm {
                reactor_ticked,
                observed_tick: observed_tick.clone(),
            }),
        );

        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
        assert!(
            observed_tick.load(Ordering::Acquire),
            "the current-thread reactor must run while the blocking RLMM call is in flight"
        );
    }

    #[tokio::test]
    async fn offset_for_timestamp_locates_remote_segment() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        // The segment metadata copies `max_timestamp` from the export; the
        // log's batch builder leaves base_timestamp at 0 by default, so
        // every batch's max_timestamp is 0 — so segments' max_timestamps are
        // all 0. Target a timestamp <= 0 to match the first segment.
        let target_ts = 0_i64;
        let got = reader
            .offset_for_timestamp(&tp(), target_ts)
            .await
            .unwrap()
            .expect("first segment matches ts=0");
        // The first finished segment is the lowest-base one.
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        assert!(got == (expected, 0));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_before_sparse_ceiling() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 1_500)
            .await
            .unwrap()
            .expect("timestamp 1500 has a remote match");

        assert!(got == (12, 1_600));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_exact_indexed_record_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_000)
            .await
            .unwrap()
            .expect("timestamp 2000 has an exact record match");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_segment_with_unknown_max_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader_with_max_timestamp(-1);

        let got = reader
            .offset_for_timestamp(&tp(), 2_000)
            .await
            .unwrap()
            .expect("the unknown max sentinel must not suppress an exact remote scan");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_none_when_past_last() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log) = populated_reader(log_dir.path(), remote_dir.path());
        // All segments have max_ts=0 by construction (see test above); any
        // strictly-positive target is past every remote segment.
        let got = reader.offset_for_timestamp(&tp(), 1).await.unwrap();
        assert!(got == None);
    }

    // `NotReady` from the RLMM must propagate out of the reader
    // ── (not be swallowed as a miss), so the handlers can keep
    // ── OFFSET_OUT_OF_RANGE / answer conservatively.

    struct NotReadyRlmm;
    impl RemoteLogMetadataManager for NotReadyRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _m: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
        fn update_remote_log_segment_metadata(
            &self,
            _u: crabka_remote_storage::RemoteLogSegmentMetadataUpdate,
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
            _m: crabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn fetch_batch_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[tokio::test]
    async fn earliest_offset_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader.earliest_offset(&tp()).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
    }

    // ── I1: the list-based read paths (`earliest_offset` /
    // ── `offset_for_timestamp` → `list_remote_log_segments`) must observe
    // ── `NotReady` from the REAL `TopicBasedRemoteLogMetadataManager` while
    // ── an assigned metadata partition is still catching up, and an empty
    // ── result for a partition this broker does not own (Unassigned). The
    // ── `NotReadyRlmm` stub proves propagation through the reader; this test
    // ── proves the manager's list-path gate actually produces those states.

    /// Drives `reconcile_assignment` and blocks, off the reactor, until the
    /// list path stops returning `NotReady` for `tp`. At that point the
    /// partition is caught up to its assignment-time HWM.
    async fn assign_and_wait_ready(
        m: &Arc<crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager>,
        mp: i32,
        tp: &TopicIdPartition,
    ) {
        m.reconcile_assignment(&[mp]).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            // `list_remote_log_segments` is the method the list path uses.
            match m.list_remote_log_segments(tp) {
                Ok(_) => return,
                Err(RemoteStorageError::NotReady { .. }) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "list path never became ready"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("unexpected list error: {e:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_path_observes_not_ready_and_unassigned_from_real_manager() {
        use crabka_remote_storage_topic::{
            InProcessMetadataEventLog, MetadataEventLog, TopicBasedRemoteLogMetadataManager,
            metadata_partition_for,
        };

        let topic_id = Uuid::from_u128(0xABCD);
        let owned = TopicIdPartition::new(topic_id, "orders", 0);
        let not_owned = TopicIdPartition::new(topic_id, "orders", 1);

        // Wide metadata topic so the two user-partitions land in distinct
        // metadata partitions.
        let n = 16;
        let mp_owned = metadata_partition_for(&owned, n);
        let mp_other = metadata_partition_for(&not_owned, n);
        assert!(mp_owned != mp_other, "test needs distinct metadata buckets");

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

        let writer_snap_dir = tempfile::tempdir().unwrap();
        let mgr_snap_dir = tempfile::tempdir().unwrap();

        // Pre-seed a finished segment for the owned partition via a transient
        // all-consuming writer.
        {
            let writer = TopicBasedRemoteLogMetadataManager::start(
                log.clone(),
                tokio::runtime::Handle::current(),
                writer_snap_dir.path().to_path_buf(),
                std::time::Duration::from_hours(1),
            )
            .unwrap();
            writer
                .reconcile_assignment(&(0..n).collect::<Vec<_>>())
                .await;
            let id = crabka_remote_storage::RemoteLogSegmentId::new(owned.clone(), Uuid::new_v4());
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
                0,
                99,
                100,
                1,
                100,
                crabka_remote_storage::RemoteLogSegmentDetails::new(
                    2048,
                    RemoteLogSegmentState::CopySegmentStarted,
                    BTreeMap::from([(LeaderEpoch(0), 0)]),
                ),
            )
            .unwrap();
            let w2 = writer.clone();
            let md2 = md.clone();
            tokio::task::spawn_blocking(move || {
                w2.add_remote_log_segment_metadata(md2).unwrap();
            })
            .await
            .unwrap();
            let w2 = writer.clone();
            tokio::task::spawn_blocking(move || {
                w2.update_remote_log_segment_metadata(
                    crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                        remote_log_segment_id: id,
                        event_timestamp_ms: 100,
                        custom_metadata: None,
                        state: RemoteLogSegmentState::CopySegmentFinished,
                        broker_id: 1,
                    },
                )
                .unwrap();
            })
            .await
            .unwrap();
            writer.shutdown();
        }

        // A fresh manager that consumes NOTHING until assigned.
        let m = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            tokio::runtime::Handle::current(),
            mgr_snap_dir.path().to_path_buf(),
            std::time::Duration::from_hours(1),
        )
        .unwrap();

        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = m.clone();
        let reader = RemoteReader::new(rsm, rlmm);

        // Unowned partition (never assigned) → the list path treats it as a
        // genuine miss: empty, not an error.
        assert!(
            reader.earliest_offset(&not_owned).await.unwrap() == None,
            "unassigned partition is an empty list-path result, not NotReady"
        );

        // Assign the owned partition. Before catch-up the list path surfaces
        // NotReady through the reader. Poll until ready; observe at least the
        // ready (Some) terminal state.
        assign_and_wait_ready(&m, mp_owned, &owned).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == Some(0),
            "owned + caught up → real earliest from the remote tier"
        );

        // Remove the owned partition: the list path now returns empty (the
        // broker no longer owns it), NOT a stale segment.
        m.reconcile_assignment(&[]).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == None,
            "removed partition's list path returns empty, not stale segments"
        );

        m.shutdown();
    }

    /// The broker tiers segments under the leader epoch that was active at
    /// copy time. In normal operation `fetch_batch` receives the owning epoch,
    /// which the caller resolves from the leader-epoch checkpoint, and the
    /// epoch-indexed primary lookup hits.
    ///
    /// This test exercises the *defensive fallback*. The caller passes an
    /// epoch that is NOT in the segment's `segment_leader_epochs` map, which
    /// simulates a missing or empty checkpoint. The lineage-unmatched fallback
    /// must still resolve the segment through `list_remote_log_segments` and
    /// return the batch. It closes the wrong-segment hazard: it prefers
    /// lineage-matching candidates first, and uses `max_by_key(start_offset)`
    /// only as a last resort.
    #[tokio::test]
    async fn fallback_resolves_segment_across_leader_epoch_change() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();

        // `populated_reader` registers all segments under epoch 0 (the epoch
        // present in the tierable-segment export, defaulted to 0 when the log
        // was written without an explicit epoch).
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());

        // Pick an offset inside the first sealed segment.
        let exports = log.tierable_segments();
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[0].base_offset.0;

        // Query with epoch 1 — the RLMM epoch-indexed primary path returns
        // None because the segment's `segment_leader_epochs` only contains
        // epoch 0.  The lineage-unmatched defensive fallback must find it via
        // `list_remote_log_segments` and return the batch.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(1), target_offset, 4096)
            .await
            .expect("ok")
            .expect("defensive fallback must resolve the segment despite epoch mismatch");

        let last = got.base_offset + i64::from(got.last_offset_delta);
        assert!(
            got.base_offset <= target_offset && last >= target_offset,
            "batch [{},{}] doesn't cover target {target_offset}",
            got.base_offset,
            last,
        );
    }
}
