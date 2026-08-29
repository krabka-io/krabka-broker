//! The `ListOffsets`-by-timestamp scan over the remote tier.
//!
//! A remote segment carries a sparse time index, so the index alone answers
//! only which relative offset the scan may start from. This module walks the
//! candidate segments in offset order, converts that floor into a byte
//! position through the offset index, and decodes records from there until it
//! finds the first one at or after the requested timestamp.

use krabka_remote_storage::{
    IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
    TimestampMs, TopicIdPartition, corrupt_log, first_record_at_or_after_timestamp,
    parse_offset_index, parse_time_index, position_for_relative_offset,
    relative_offset_floor_for_timestamp,
};

use super::RemoteReader;

impl RemoteReader {
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
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::remote_reader::test_support::{
        populated_reader, sparse_remote_segment_reader,
        sparse_remote_segment_reader_with_max_timestamp, tp,
    };

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
}
