//! Reading a remote segment's transaction index.
//!
//! A `Fetch` under `read_committed` needs the aborted transactions that
//! overlap the offsets it returns. This module fetches the covering segment's
//! `.txnindex` from the remote tier, decodes it, and keeps the entries that
//! overlap the requested range. A segment with no aborted transactions has no
//! index object at all, so the missing-object error maps onto an empty list.

use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    IndexType, LogOffset, RemoteLogMetadataManager, RemoteLogSegmentState, RemoteStorageError,
    TopicIdPartition, parse_txn_index, txn_overlaps,
};

use super::{AbortedTxnEntry, RemoteReader};

impl RemoteReader {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteStorageManager,
    };

    use super::*;
    use crate::remote_reader::test_support::{populated_reader, populated_reader_with_abort, tp};

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
}
