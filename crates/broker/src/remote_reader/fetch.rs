//! Assembly of one `Fetch` answer from the remote tier.
//!
//! This module resolves the finished remote segment that covers a requested
//! offset, turns that offset into a byte position through the segment's
//! offset index, and reads back the capped byte range that holds the batch.
//! The segment resolution carries the defensive fallback that keeps a read
//! answerable when the epoch-indexed lookup in the `RLMM` misses.

use krabka_ids::LeaderEpoch;
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::{
    IndexType, LogOffset, RemoteLogSegmentState, RemoteStorageError, TopicIdPartition,
    end_position_for, first_batch_at_or_after, parse_offset_index, position_for_relative_offset,
};

use super::RemoteReader;

impl RemoteReader {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
        RemoteLogSegmentMetadata, RemoteStorageManager,
    };
    use uuid::Uuid;

    use super::*;
    use crate::remote_reader::test_support::{
        NotReadyRlmm, populated_reader, sparse_remote_segment_reader, tp,
    };

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
    async fn fetch_batch_returns_none_for_in_progress_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id,
            0,
            99,
            100,
            1,
            100,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0_i64},
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
