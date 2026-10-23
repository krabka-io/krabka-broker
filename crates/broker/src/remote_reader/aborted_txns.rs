//! Reading remote transaction indexes for a Fetch window.
//!
//! A `Fetch` under `read_committed` needs the aborted transactions that
//! overlap the offsets it returns. Abort markers can live in later segments,
//! so this module scans the finished remote tail's `.txnindex` objects and
//! keeps entries overlapping the requested range. A segment owning no abort
//! marker has no index object; metadata which identifies those segments is
//! used to avoid requesting the missing optional objects.

use {
    super::{AbortedTxnEntry, RemoteReader},
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
        TopicIdPartition, parse_txn_index, txn_overlaps,
    },
};

impl RemoteReader {
    /// Returns the aborted transactions that overlap the inclusive offset
    /// range `[from_offset, to_offset]`. An abort marker can be in a later
    /// finished segment than its data, so all later indexes are considered.
    /// Returns an empty `Vec` if no finished segment covers the offset or no
    /// available transaction index contains an overlapping entry. Missing
    /// optional indexes (`SegmentNotFound` from `fetch_index`) are skipped.
    ///
    /// The covering segment is resolved exactly as the data read
    /// ([`RemoteReader::fetch_raw`]) resolves it, fallback included, so a
    /// `read_committed` `Fetch` never gets batches the tier served without
    /// the aborts that cover them.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        from_offset: LogOffset,
        to_offset: LogOffset,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        if self
            .segment_covering(tp, leader_epoch, from_offset)?
            .is_none()
        {
            return Ok(Vec::new());
        }

        let mut segments = self.list_remote_log_segments_blocking(tp).await?;
        segments.retain(|segment| {
            segment.state() == RemoteLogSegmentState::CopySegmentFinished
                && segment.end_offset() >= from_offset
        });
        segments.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        let mut aborts = Vec::new();
        // ponytail: scan the finished tail for later abort markers; use trusted
        // LSO hints to stop earlier if cold-Fetch profiling requires it.
        for segment in segments {
            // `true` is authoritative. Older metadata defaults to `false`, so
            // it retains the compatibility fetch and error behaviour below.
            if segment.txn_index_empty() {
                continue;
            }
            let bytes = match self
                .fetch_index_blocking(segment, IndexType::Transaction)
                .await
            {
                Ok(bytes) => bytes,
                // The index is optional when this segment owns no abort marker.
                Err(RemoteStorageError::SegmentNotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            aborts.extend(
                parse_txn_index(&bytes)?
                    .iter()
                    .filter(|entry| txn_overlaps(entry, from_offset, to_offset))
                    .map(|entry| AbortedTxnEntry {
                        start_offset: entry.start_offset.get(),
                        last_offset: entry.last_offset.get(),
                        producer_id: entry.producer_id.get(),
                    }),
            );
        }
        Ok(aborts)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_reader::test_support::{
            ReaderFixture, counted_reader, empty_reader, nest_finished_segment,
            nest_finished_segment_with_txn_index_empty, populated_reader_with_abort,
            reader_fixture, reader_with_abort_in_segments_of, tp,
        },
        assert2::assert,
        std::{sync::atomic::Ordering, time::Instant},
    };

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

    /// #1388: the abort lookup resolves the covering segment as the data read
    /// does. A finished segment nested inside the first one, above its base,
    /// shadows that segment in the RLMM's floor-by-start lookup for every
    /// later offset. The data read then finds the first segment through its
    /// list-scan fallback; the abort lookup took only the epoch-indexed lookup
    /// and answered no aborts, so a `read_committed` consumer was handed the
    /// aborted batches as committed.
    #[tokio::test]
    async fn the_abort_lookup_covers_every_offset_the_data_read_serves() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        // Segments of several batches each, so one can hold a nested segment.
        let (reader, _log, (start, last, producer_id)) =
            reader_with_abort_in_segments_of(log_dir.path(), remote_dir.path(), 768);
        assert!(
            last > start + 1,
            "the first segment must outlast the nested one: {start}..={last}"
        );
        nest_finished_segment(&reader, start + 1, start + 1);

        let abort = vec![AbortedTxnEntry {
            start_offset: start,
            last_offset: last,
            producer_id,
        }];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (offset, label) in [
            (start, "the epoch-indexed lookup finds the first segment"),
            (
                last,
                "the nested segment shadows it; only the fallback finds it",
            ),
        ] {
            let served = reader
                .fetch_raw(&tp(), LeaderEpoch(0), offset, 0)
                .await
                .expect("the tier answers")
                .is_some();
            let aborts = reader
                .aborted_transactions(&tp(), LeaderEpoch(0), offset, last)
                .await
                .expect("the tier answers");
            actual.push((label, served, aborts));
            expected.push((label, true, abort.clone()));
        }
        assert!(actual == expected);
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_segment_has_no_txnindex() {
        // The default harness writes no `.txnindex` for any segment.
        let ReaderFixture {
            local: _log_dir,
            remote: _remote_dir,
            reader,
            log,
        } = reader_fixture();
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
    async fn marked_empty_segments_never_fetch_an_index_on_repeated_reads() {
        for segment_count in [1_i64, 10, 100, 1_000] {
            let (_remote_dir, reader) = empty_reader();
            for offset in 0..segment_count {
                nest_finished_segment_with_txn_index_empty(&reader, offset, offset);
            }
            let (reader, index_fetches) = counted_reader(&reader);
            let started = Instant::now();
            for _ in 0..2 {
                let got = reader
                    .aborted_transactions(&tp(), LeaderEpoch(0), 0, segment_count - 1)
                    .await
                    .expect("marked-empty metadata is sufficient");
                assert!(got.is_empty());
            }
            let elapsed = started.elapsed();
            assert!(index_fetches.load(Ordering::Relaxed) == 0);
            eprintln!(
                "marked-empty scan: segments={segment_count}, reads=2, index_requests=0, elapsed={elapsed:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_later_non_empty_segment_still_supplies_its_abort() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log, (start, last, producer_id)) =
            populated_reader_with_abort(log_dir.path(), remote_dir.path());
        nest_finished_segment_with_txn_index_empty(&reader, start - 1, start - 1);
        let (reader, index_fetches) = counted_reader(&reader);

        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), start - 1, last)
            .await
            .expect("the later transaction index is read");

        assert!(got.iter().any(|entry| entry.producer_id == producer_id));
        // The marked-empty prefix and empty suffixes make no requests; only
        // the real transaction index is fetched.
        assert!(index_fetches.load(Ordering::Relaxed) == 1);
    }

    #[tokio::test]
    async fn legacy_false_metadata_keeps_fetching_a_missing_optional_index() {
        let (_remote_dir, reader) = empty_reader();
        nest_finished_segment(&reader, 0, 0); // the metadata default is false
        let (reader, index_fetches) = counted_reader(&reader);

        for expected_fetches in 1..=2 {
            let got = reader
                .aborted_transactions(&tp(), LeaderEpoch(0), 0, 0)
                .await
                .expect("a missing optional index remains a successful empty result");
            assert!(got.is_empty());
            assert!(index_fetches.load(Ordering::Relaxed) == expected_fetches);
        }
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_no_segment() {
        let (_remote_dir, reader) = empty_reader();
        // RLMM is empty → no covering segment → empty list, not an error.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), 0, 100)
            .await
            .expect("ok");
        assert!(got.is_empty());
    }
}
