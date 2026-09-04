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
    IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
    TopicIdPartition, end_position_for, first_batch_at_or_after, parse_offset_index,
    position_for_relative_offset,
};
use krabka_verified::remote_read_relative_offset;

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
        // contains the passed epoch (same lineage). No lineage-unmatched
        // segment is a safe fallback under log divergence, so that case is a
        // miss rather than a read from a different history.
        let primary = primary.and_then(|metadata| {
            relative_offset(&metadata, leader_epoch, offset).map(|relative| (metadata, relative))
        });
        let (metadata, target_rel) = if let Some(selected) = primary {
            selected
        } else {
            let candidates = self.rlmm.list_remote_log_segments(tp)?;
            let Some(selected) = candidates
                .into_iter()
                .filter_map(|metadata| {
                    relative_offset(&metadata, leader_epoch, offset)
                        .map(|relative| (metadata, relative))
                })
                .max_by_key(|(metadata, _)| metadata.start_offset())
            else {
                return Ok(None);
            };
            selected
        };

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Offset)
            .await?;
        let entries = parse_offset_index(&index_bytes)?;
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

fn relative_offset(
    metadata: &RemoteLogSegmentMetadata,
    leader_epoch: LeaderEpoch,
    requested_offset: LogOffset,
) -> Option<u32> {
    let epochs = metadata.segment_leader_epochs();
    let epoch_start = epochs.get(&leader_epoch).copied();
    let next_epoch_start = epochs
        .iter()
        .filter(|(epoch, _)| **epoch > leader_epoch)
        .map(|(_, start)| *start)
        .min();
    remote_read_relative_offset(
        metadata.start_offset(),
        metadata.end_offset(),
        requested_offset,
        metadata.state() == RemoteLogSegmentState::CopySegmentFinished,
        epoch_start,
        next_epoch_start,
    )
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
        NotReadyRlmm, caching_sparse_remote_segment_reader, populated_reader,
        sparse_remote_segment_reader, tp,
    };

    /// KIP-405's `RemoteIndexCache`: a consumer walking one cold segment
    /// downloads its `.index` once, not once per `Fetch`. Before the cache,
    /// every batch a consumer read from the tier pulled the whole offset index
    /// again, which is two or three object GETs per batch on a topic whose
    /// segments are all remote.
    #[tokio::test]
    async fn two_fetches_of_one_segment_download_its_offset_index_once() {
        let (reader, _dirs, index_fetches) = caching_sparse_remote_segment_reader();

        for offset in [10, 12] {
            reader
                .fetch_batch(&tp(), LeaderEpoch(0), offset, 4096)
                .await
                .expect("ok")
                .expect("both offsets are in the synthetic remote segment");
        }

        assert!(
            index_fetches.load(std::sync::atomic::Ordering::Relaxed) == 1,
            "the second fetch must read the cached index, not download it again"
        );
        let stats = reader.index_cache.stats();
        assert!(stats.hits == 1 && stats.misses == 1, "{stats:?}");
    }

    /// The same segment's index is fetched again once the cache is told the
    /// segment is going away, which is what keeps a deleted segment's bytes
    /// from holding the budget against live ones.
    #[tokio::test]
    async fn dropping_a_segment_from_the_cache_makes_the_next_read_download_again() {
        let (reader, _dirs, index_fetches) = caching_sparse_remote_segment_reader();
        let segment_id = reader
            .rlmm
            .list_remote_log_segments(&tp())
            .expect("list")
            .first()
            .expect("one segment")
            .remote_log_segment_id()
            .id;

        reader
            .fetch_batch(&tp(), LeaderEpoch(0), 10, 4096)
            .await
            .expect("ok")
            .expect("a batch");
        reader.index_cache.remove_segment(segment_id);
        reader
            .fetch_batch(&tp(), LeaderEpoch(0), 12, 4096)
            .await
            .expect("ok")
            .expect("a batch");

        assert!(index_fetches.load(std::sync::atomic::Ordering::Relaxed) == 2);
        assert!(reader.index_cache.stats().hits == 0);
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

    #[test]
    fn relative_offset_respects_epoch_subrange_boundary() {
        let metadata = RemoteLogSegmentMetadata::new(
            krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4()),
            0,
            99,
            0,
            1,
            0,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentFinished,
                maplit::btreemap! {
                    LeaderEpoch(0) => 0,
                    LeaderEpoch(1) => 50,
                },
            ),
        )
        .unwrap();

        for (epoch, offset, expected) in [
            (LeaderEpoch(0), 49, Some(49)),
            (LeaderEpoch(0), 50, None),
            (LeaderEpoch(1), 49, None),
            (LeaderEpoch(1), 50, Some(50)),
        ] {
            assert!(
                relative_offset(&metadata, epoch, offset) == expected,
                "epoch={epoch:?} offset={offset}"
            );
        }
    }

    /// The broker tiers segments under the leader epoch that was active at
    /// copy time. In normal operation `fetch_batch` receives the owning epoch,
    /// which the caller resolves from the leader-epoch checkpoint, and the
    /// epoch-indexed primary lookup hits.
    ///
    /// This test exercises the defensive fallback with an epoch that is not in
    /// the segment's lineage, as an empty or stale checkpoint could supply.
    /// Serving that segment could cross divergent histories, so the fallback
    /// must fail closed.
    #[tokio::test]
    async fn fallback_rejects_segment_from_the_wrong_leader_epoch() {
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
        // epoch 0. The fallback must not cross that lineage boundary.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(1), target_offset, 4096)
            .await
            .expect("ok");

        assert!(got.is_none(), "wrong-epoch remote data must fail closed");
    }
}
