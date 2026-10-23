//! The `ListOffsets`-by-timestamp scan over the remote tier.
//!
//! A remote segment carries a sparse time index, so the index alone answers
//! only which relative offset the scan may start from. This module walks the
//! candidate segments in offset order, converts that floor into a byte
//! position through the offset index, and decodes records from there until it
//! finds the first one at or after the requested timestamp.

use {
    super::{RemoteReader, lineage::EpochLineage},
    krabka_protocol::records::HEADER_LEN,
    krabka_remote_storage::{
        IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
        TimestampMs, TopicIdPartition, first_record_at_or_after_timestamp, parse_offset_index,
        parse_time_index, position_for_relative_offset, relative_offset_floor_for_timestamp,
    },
};

/// The normal object-store read made while refining a sparse timestamp index.
/// A batch which crosses this boundary is completed with one adjacent range;
/// consequently live scan storage is bounded by this window or one encoded
/// batch, rather than by the remainder of the segment.
const TIMESTAMP_SCAN_WINDOW: u32 = 64 * 1024;

impl RemoteReader {
    /// Answers a by-timestamp seek from the first candidate remote segment,
    /// as Kafka 4.3.1's `RemoteLogManager.findOffsetByTimestamp` does: the
    /// first finished segment, in start-offset order, whose max timestamp
    /// reaches `target_timestamp`, whose end offset reaches `minimum_offset`,
    /// and which the leader-epoch `lineage` admits. The answer is the smallest
    /// offset `>= minimum_offset` in that segment whose record timestamp is
    /// `>= target_timestamp`, or `None` when that segment holds none. Later
    /// segments are never read:
    ///
    /// ```java
    /// if (rlsMetadata.maxTimestampMs() >= timestamp
    ///     && rlsMetadata.endOffset() >= startingOffset
    ///     && isRemoteSegmentWithinLeaderEpochs(rlsMetadata, unifiedLog.logEndOffset(), epochWithOffsets)
    ///     && rlsMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED)) {
    ///     List<LogSegment> segmentsCopy = unifiedLog.logSegments();
    ///     if (segmentsCopy.isEmpty() || rlsMetadata.startOffset() < segmentsCopy.get(0).baseOffset()) {
    ///         return lookupTimestamp(rlsMetadata, timestamp, startingOffset);
    ///     } else {
    ///         // search in local-log
    ///     }
    /// }
    /// ```
    ///
    /// Kafka walks the leader epochs from the one holding `startingOffset`
    /// and each epoch's segments in start-offset order; over the segments the
    /// lineage admits, that is start-offset order.
    ///
    /// When the first candidate starts at or above `local_base`, the base
    /// offset of the log's first local segment, Kafka searches the local log
    /// instead, and this returns `None` so the caller's local search answers.
    /// `local_base` is `None` for a log with no local segment, where Kafka
    /// reads the tier.
    ///
    /// The sparse time index supplies a scan floor; the exact answer comes
    /// from decoding records from the corresponding offset-index position.
    ///
    /// `max_record_body` is the topic's Kafka trunk `max.decompressed.message.bytes`,
    /// or `None` for no limit. A compressed record the scan has to read that is
    /// above it fails the lookup with [`RemoteStorageError::RecordTooLarge`],
    /// as `RemoteLogManager.findOffsetByTimestamp` fails with
    /// `InvalidRecordException`.
    ///
    /// A segment outside the partition's leader-epoch `lineage` is never read,
    /// as `findOffsetByTimestamp` skips any segment that fails
    /// `isRemoteSegmentWithinLeaderEpochs`: after an unclean election the tier
    /// can still hold a deposed leader's copy of offsets the current lineage
    /// assigns to other records. See [`EpochLineage::admits`].
    ///
    /// A segment whose metadata carries the unknown max timestamp (`-1`, which
    /// Kafka's metadata never does) is the first candidate only when its
    /// records reach `target_timestamp`, so the lookup reads it to find out.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: TimestampMs,
        minimum_offset: LogOffset,
        max_record_body: Option<usize>,
        lineage: &EpochLineage,
        local_base: Option<LogOffset>,
    ) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
        let mut listed = self.list_remote_log_segments_blocking(tp).await?;
        listed.retain(|md| {
            md.state() == RemoteLogSegmentState::CopySegmentFinished && lineage.admits(md)
        });
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);

        for metadata in listed.into_iter().filter(|md| {
            md.end_offset() >= minimum_offset
                && (md.max_timestamp_ms() == UNKNOWN_MAX_TIMESTAMP
                    || md.max_timestamp_ms() >= target_timestamp)
        }) {
            if local_base.is_some_and(|base| metadata.start_offset() >= base) {
                // Kafka's "search in local-log" branch: the caller's local
                // search is the answer.
                return Ok(None);
            }
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
            let (found, reaches_target) = self
                .scan_timestamp_ranges(
                    &metadata,
                    start_position,
                    metadata.start_offset().max(minimum_offset),
                    target_timestamp,
                    max_record_body,
                )
                .await?;
            if found.is_some()
                || metadata.max_timestamp_ms() != UNKNOWN_MAX_TIMESTAMP
                || reaches_target
            {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Scans complete batches from bounded, adjacent ranges. The sparse index
    /// position is batch-aligned. If the regular window cuts through a batch,
    /// only that batch's declared remainder is fetched before decoding it.
    async fn scan_timestamp_ranges(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        floor_offset: LogOffset,
        target_timestamp: TimestampMs,
        max_record_body: Option<usize>,
    ) -> Result<(Option<(LogOffset, TimestampMs)>, bool), RemoteStorageError> {
        let segment_size = u32::try_from(metadata.segment_size_in_bytes())
            .map_err(|_| corrupt_timestamp_log("negative remote segment size"))?;
        let header_len = u32::try_from(HEADER_LEN).expect("record header length fits u32");
        let mut position = start_position;
        let mut buffered = Vec::new();
        let mut cursor = 0_usize;
        let mut reaches_target = false;

        while position < segment_size {
            if cursor == buffered.len() {
                buffered.clear();
                cursor = 0;
                let end = position
                    .saturating_add(TIMESTAMP_SCAN_WINDOW)
                    .min(segment_size);
                buffered = self
                    .fetch_log_blocking(metadata.clone(), position, Some(end - 1))
                    .await?;
                if buffered.is_empty() {
                    return Err(corrupt_timestamp_log("empty remote range"));
                }
            }

            let available = u32::try_from(buffered.len() - cursor)
                .map_err(|_| corrupt_timestamp_log("remote range is too large"))?;
            if available < header_len {
                let missing = header_len - available;
                append_range(self, metadata, &mut buffered, position + available, missing).await?;
            }
            let header = &buffered[cursor..cursor + HEADER_LEN];
            let declared = i32::from_be_bytes(header[8..12].try_into().expect("four bytes"));
            let batch_len = u32::try_from(declared)
                .ok()
                .and_then(|length| length.checked_add(12))
                .filter(|length| *length >= header_len)
                .ok_or_else(|| corrupt_timestamp_log("invalid record batch length"))?;
            if position
                .checked_add(batch_len)
                .is_none_or(|end| end > segment_size)
            {
                return Err(corrupt_timestamp_log("record batch exceeds remote segment"));
            }
            let available = u32::try_from(buffered.len() - cursor)
                .map_err(|_| corrupt_timestamp_log("remote range is too large"))?;
            if available < batch_len {
                append_range(
                    self,
                    metadata,
                    &mut buffered,
                    position + available,
                    batch_len - available,
                )
                .await?;
            }
            let end = cursor + usize::try_from(batch_len).expect("u32 fits usize");
            let batch = &buffered[cursor..end];
            let found = first_record_at_or_after_timestamp(
                batch,
                floor_offset,
                target_timestamp,
                max_record_body,
            )?;
            if let Some(found) = found {
                return Ok((Some(found), true));
            }
            reaches_target |= first_record_at_or_after_timestamp(
                batch,
                metadata.start_offset(),
                target_timestamp,
                None,
            )?
            .is_some();
            position += batch_len;
            cursor = end;
        }
        Ok((None, reaches_target))
    }
}

async fn append_range(
    reader: &RemoteReader,
    metadata: &RemoteLogSegmentMetadata,
    buffer: &mut Vec<u8>,
    start: u32,
    length: u32,
) -> Result<(), RemoteStorageError> {
    let end = start
        .checked_add(length)
        .and_then(|end| end.checked_sub(1))
        .ok_or_else(|| corrupt_timestamp_log("remote range overflow"))?;
    let bytes = reader
        .fetch_log_blocking(metadata.clone(), start, Some(end))
        .await?;
    if bytes.len() != usize::try_from(length).expect("u32 fits usize") {
        return Err(corrupt_timestamp_log("short remote range"));
    }
    buffer.extend_from_slice(&bytes);
    Ok(())
}

fn corrupt_timestamp_log(message: &'static str) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

/// The persisted unknown-max sentinel for a sealed segment opened without a
/// tail scan.
const UNKNOWN_MAX_TIMESTAMP: TimestampMs = -1;

#[cfg(test)]
mod tests {
    use {
        super::{EpochLineage, RemoteReader},
        crate::remote_reader::test_support::{
            CountedRsm, CountingRsm, ReaderFixture, ReaderWithDir, TierSegment,
            append_time_remote_segment_reader, compressed_remote_segment_reader,
            divergent_tier_reader, reader_fixture, single_batch_tier_segment as segment,
            sparse_remote_segment_reader, sparse_remote_segment_reader_with_max_timestamp,
            tier_reader, tp, unordered_timestamps_remote_segment_reader,
        },
        assert2::assert,
        bytes::Bytes,
        krabka_protocol::records::RecordBatch,
        krabka_remote_storage::RemoteStorageManager,
        std::sync::Arc,
    };

    /// One seek of [`a_seek_answers_from_the_first_candidate_segment_only`].
    struct Seek {
        name: &'static str,
        lineage: EpochLineage,
        target: i64,
        minimum: i64,
        local_base: Option<i64>,
        want: Option<(i64, i64)>,
    }

    /// Runs each seek against the reader, and pairs every answer with its
    /// name, as the expected answers are.
    async fn seek_all(
        reader: &RemoteReader,
        seeks: Vec<Seek>,
    ) -> (
        Vec<(&'static str, Option<(i64, i64)>)>,
        Vec<(&'static str, Option<(i64, i64)>)>,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for Seek {
            name,
            lineage,
            target,
            minimum,
            local_base,
            want,
        } in seeks
        {
            let found = reader
                .offset_for_timestamp(&tp(), target, minimum, None, &lineage, local_base)
                .await
                .unwrap();
            actual.push((name, found));
            expected.push((name, want));
        }
        (actual, expected)
    }

    /// #1388: Kafka 4.3.1's `RemoteLogManager.findOffsetByTimestamp` answers
    /// from its first candidate segment, the first finished one the
    /// leader-epoch lineage admits whose max timestamp reaches the target and
    /// whose end offset reaches the floor, even when that segment holds no
    /// match at or above the floor. When that candidate starts at or above the
    /// first local segment's base, Kafka searches the local log instead.
    ///
    /// The lineage is epoch 0 from offset 0 and epoch 1 from offset 5, with the
    /// log end at 12. The tier holds:
    /// - offsets 0 to 4 in epoch 0, timestamps 100, 500, 150, 160, 170;
    /// - a deposed leader's copy of offsets 3 to 9, all in epoch 0, which the
    ///   lineage does not admit: its offset 5 has timestamp 1 000;
    /// - offsets 5 to 9 in epoch 1, timestamps 900, 1 200, 950, 960, 970;
    /// - offsets 10 and 11 in epoch 1, timestamps 1 300, 1 400.
    #[tokio::test]
    async fn a_seek_answers_from_the_first_candidate_segment_only() {
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = tier_reader(&[
            segment(0, &[100, 500, 150, 160, 170], 0, false),
            segment(3, &[130, 140, 1_000, 1_010, 1_020, 1_030, 1_040], 0, false),
            segment(5, &[900, 1_200, 950, 960, 970], 1, false),
            segment(10, &[1_300, 1_400], 1, false),
        ]);
        let lineage = || EpochLineage::from_entries(&[(0, 0), (1, 5)], 12);
        let seek = |name, target, minimum, local_base, want| Seek {
            name,
            lineage: lineage(),
            target,
            minimum,
            local_base,
            want,
        };
        let (actual, expected) = seek_all(&reader, vec![
            seek(
                "the first candidate holds the match",
                400,
                0,
                None,
                Some((1, 500)),
            ),
            seek("its only match is below the floor", 400, 2, None, None),
            seek(
                "a segment ending below the floor is no candidate",
                400,
                5,
                None,
                Some((5, 900)),
            ),
            seek(
                "a segment below the target is no candidate",
                1_300,
                0,
                None,
                Some((10, 1_300)),
            ),
            seek(
                "the lineage skips the deposed leader's copy",
                1_000,
                0,
                None,
                Some((6, 1_200)),
            ),
            seek(
                "the first admitted candidate holds no match above the floor",
                1_000,
                7,
                None,
                None,
            ),
            seek(
                "a candidate below the local base is read from the tier",
                400,
                0,
                Some(5),
                Some((1, 500)),
            ),
            seek(
                "a candidate at the local base is the local log's to answer",
                1_000,
                0,
                Some(5),
                None,
            ),
            Seek {
                name: "an empty checkpoint admits the deposed leader's copy",
                lineage: EpochLineage::unknown(),
                target: 1_000,
                minimum: 0,
                local_base: None,
                want: Some((5, 1_000)),
            },
        ])
        .await;
        assert!(actual == expected);
    }

    /// A segment whose metadata carries the unknown max timestamp is the first
    /// candidate exactly when its records reach the target, below the floor
    /// too, as its largest timestamp would.
    #[tokio::test]
    async fn an_unknown_max_timestamp_is_measured_from_the_segments_records() {
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = tier_reader(&[
            segment(0, &[100, 500, 150], 0, true),
            segment(3, &[200, 300], 0, true),
            segment(5, &[600, 700], 0, false),
        ]);
        let seek = |name, target, minimum, want| Seek {
            name,
            lineage: EpochLineage::unknown(),
            target,
            minimum,
            local_base: None,
            want,
        };
        let (actual, expected) = seek_all(&reader, vec![
            seek("its records hold the match", 400, 0, Some((1, 500))),
            seek(
                "its records reach the target only below the floor",
                400,
                2,
                None,
            ),
            seek("its records never reach the target", 550, 0, Some((5, 600))),
        ])
        .await;
        assert!(actual == expected);
    }

    #[yare::parameterized(
        start = { 0 },
        second_record = { 1 },
        later_record = { 3 },
        before_end = { 23 },
        at_end = { 24 },
        beyond_end = { i64::MAX },
    )]
    #[test_macro(tokio::test)]
    async fn timestamp_lookup_excludes_records_before_the_logical_floor(minimum: i64) {
        let ReaderFixture {
            local: _log_dir,
            remote: _remote_dir,
            reader,
            log,
        } = reader_fixture();
        let end = log.tierable_segments().last().unwrap().last_offset.0 + 1;
        let expected = (minimum < end).then_some((minimum, 0));
        let actual = reader
            .offset_for_timestamp(&tp(), 0, minimum, None, &EpochLineage::unknown(), None)
            .await
            .unwrap();
        assert2::assert!(actual == expected, "minimum {minimum}, end {end}");
    }

    #[yare::parameterized(
        earlier_record = { 0, 1_500, Some((10, 1_700)) },
        exact_floor = { 11, 1_500, Some((11, 1_700)) },
        later_timestamp = { 0, 1_800, Some((14, 2_400)) },
        floor_at_match = { 15, 2_400, Some((15, 2_400)) },
        after_last_timestamp = { 0, 2_401, None },
    )]
    #[test_macro(tokio::test)]
    async fn timestamp_lookup_answers_in_append_time(
        floor: i64,
        target: i64,
        expected: Option<(i64, i64)>,
    ) {
        let ReaderWithDir { reader, dir: _dir } = append_time_remote_segment_reader();
        let actual = reader
            .offset_for_timestamp(&tp(), target, floor, None, &EpochLineage::unknown(), None)
            .await
            .unwrap();
        assert2::assert!(actual == expected, "floor {floor} target {target}");
    }

    /// #1388: Kafka's `findOffsetByTimestamp` reads only segments that pass
    /// `isRemoteSegmentWithinLeaderEpochs`. The deposed leader's copy of
    /// offsets 3 to 9 claims epoch 0 past offset 4, where the lineage starts
    /// epoch 1, so the answer for 1 000 is the current lineage's offset 7, not
    /// the divergent copy's offset 5. An empty checkpoint says nothing about
    /// lineage, so it reads every segment, as a remote `Fetch` does then.
    #[tokio::test]
    async fn timestamp_lookup_skips_a_segment_outside_the_leader_epoch_lineage() {
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = divergent_tier_reader();
        let lineage = EpochLineage::from_entries(&[(0, 0), (1, 5)], 10);
        let cases = [
            ("before the divergence", &lineage, 100, Some((0, 100))),
            (
                "past it, the lineage's record",
                &lineage,
                1_000,
                Some((7, 1_100)),
            ),
            (
                "an empty checkpoint judges nothing",
                &EpochLineage::unknown(),
                1_000,
                Some((5, 1_000)),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, lineage, target, want) in cases {
            let found = reader
                .offset_for_timestamp(&tp(), target, 0, None, lineage, None)
                .await
                .unwrap();
            actual.push((label, found));
            expected.push((label, want));
        }
        assert!(actual == expected);
    }

    #[tokio::test]
    async fn offset_for_timestamp_locates_remote_segment() {
        let ReaderFixture {
            local: _log_dir,
            remote: _remote_dir,
            reader,
            log,
        } = reader_fixture();
        let exports = log.tierable_segments();
        // The segment metadata copies `max_timestamp` from the export; the
        // log's batch builder leaves base_timestamp at 0 by default, so
        // every batch's max_timestamp is 0 — so segments' max_timestamps are
        // all 0. Target a timestamp <= 0 to match the first segment.
        let target_ts = 0_i64;
        let got = reader
            .offset_for_timestamp(&tp(), target_ts, 0, None, &EpochLineage::unknown(), None)
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
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 1_500, 0, None, &EpochLineage::unknown(), None)
            .await
            .unwrap()
            .expect("timestamp 1500 has a remote match");

        assert!(got == (12, 1_600));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_exact_indexed_record_timestamp() {
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_000, 0, None, &EpochLineage::unknown(), None)
            .await
            .unwrap()
            .expect("timestamp 2000 has an exact record match");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_segment_with_unknown_max_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader_with_max_timestamp(-1);

        let got = reader
            .offset_for_timestamp(&tp(), 2_000, 0, None, &EpochLineage::unknown(), None)
            .await
            .unwrap()
            .expect("the unknown max sentinel must not suppress an exact remote scan");

        assert!(got == (14, 2_000));
    }

    /// The time index names the last offset of the batch that set a running
    /// maximum. The newest record can sit earlier in that batch, and the scan
    /// must still find it: Kafka's `RemoteLogManager.lookupTimestamp` filters
    /// on the log start offset, never on the index entry's offset.
    #[tokio::test]
    async fn offset_for_timestamp_finds_the_newest_record_before_its_batchs_last_offset() {
        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = unordered_timestamps_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_400, 0, None, &EpochLineage::unknown(), None)
            .await
            .unwrap()
            .expect("the newest record is the first of the second batch");

        assert!(got == (14, 2_400));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_none_when_past_last() {
        let ReaderFixture {
            local: _log_dir,
            remote: _remote_dir,
            reader,
            log: _log,
        } = reader_fixture();
        // All segments have max_ts=0 by construction (see test above); any
        // strictly-positive target is past every remote segment.
        let got = reader
            .offset_for_timestamp(&tp(), 1, 0, None, &EpochLineage::unknown(), None)
            .await
            .unwrap();
        assert!(got == None);
    }

    /// Kafka trunk's `max.decompressed.message.bytes` on the remote scan:
    /// `RemoteLogManager.lookupTimestamp` decompresses a batch only when its max
    /// timestamp reaches the target, and fails on a record above the limit
    /// rather than moving on to another segment. A record of the second batch
    /// is a `1_007`-byte body, and one of the first is a few dozen.
    #[yare::parameterized(
        found_before_oversized_batch = { 1_500, Some(100), Ok(Some((12, 1_600))) },
        matching_record_is_oversized = { 1_800, Some(100), Err(1_007) },
        no_limit = { 1_800, None, Ok(Some((14, 2_000))) },
        limit_above_record = { 1_800, Some(1_007), Ok(Some((14, 2_000))) },
    )]
    #[test_macro(tokio::test)]
    async fn offset_for_timestamp_refuses_a_record_above_the_limit_that_it_reads(
        target: i64,
        limit: Option<usize>,
        expected: Result<Option<(i64, i64)>, usize>,
    ) {
        use krabka_remote_storage::RemoteStorageError;

        let ReaderWithDir {
            reader,
            dir: _remote_dir,
        } = compressed_remote_segment_reader(1_000);
        let actual = match reader
            .offset_for_timestamp(&tp(), target, 0, limit, &EpochLineage::unknown(), None)
            .await
        {
            Ok(found) => Ok(found),
            Err(RemoteStorageError::RecordTooLarge { size, limit: seen }) => {
                assert2::assert!(Some(seen) == limit);
                Err(size)
            }
            Err(other) => panic!("unexpected error: {other}"),
        };
        assert2::assert!(actual == expected);
    }

    fn padded_batch(base: i64, timestamp: i64, value_len: usize) -> RecordBatch {
        let mut batch = segment(base, &[timestamp], 0, false).batches.remove(0);
        batch.records[0].value = Some(Bytes::from(vec![b'x'; value_len]));
        batch
    }

    fn counted_timestamp_reader(batches: Vec<RecordBatch>) -> (ReaderWithDir, CountedRsm) {
        let ReaderWithDir { reader, dir } = tier_reader(&[TierSegment {
            batches,
            epochs: maplit::btreemap! {krabka_ids::LeaderEpoch(0) => 0},
            unknown_max_timestamp: false,
        }]);
        let counted = CountingRsm::counting(Arc::clone(&reader.rsm));
        let rsm: Arc<dyn RemoteStorageManager> = counted.rsm.clone();
        let reader = RemoteReader::new(rsm, Arc::clone(&reader.rlmm));
        (ReaderWithDir { reader, dir }, counted)
    }

    /// #1662: an answer in the first range costs one fixed window regardless
    /// of the sealed segment's tail. These are bytes actually returned by the
    /// RSM, rather than an estimate from the requested bounds.
    #[tokio::test]
    async fn an_early_timestamp_answer_does_not_download_the_segment_tail() {
        let mut transferred = Vec::new();
        for tail_batches in [16, 64] {
            let mut batches = vec![padded_batch(0, 100, 32)];
            batches.extend(
                (1..=tail_batches).map(|offset| {
                    padded_batch(i64::from(offset), 200 + i64::from(offset), 64 * 1024)
                }),
            );
            let (ReaderWithDir { reader, dir }, counted) = counted_timestamp_reader(batches);
            let found = reader
                .offset_for_timestamp(&tp(), 100, 0, None, &EpochLineage::unknown(), None)
                .await
                .unwrap();
            assert!(found == Some((0, 100)));
            let ranges = counted.log_fetches.lock().unwrap();
            assert!(ranges.len() == 1);
            assert!(ranges[0].1.is_some(), "the log GET must be bounded");
            transferred.push((tail_batches, ranges[0].2));
            drop(dir);
        }
        assert!(transferred == vec![(16, 64 * 1024), (64, 64 * 1024)]);
    }

    /// A normal window may end in a batch, and a valid batch may itself be
    /// larger than the window. Both make progress by fetching only the
    /// declared adjacent remainder and preserve the exact timestamp answer.
    #[tokio::test]
    async fn timestamp_ranges_complete_crossing_and_oversized_batches() {
        for (name, first_value_len) in [("crossing", 40 * 1024), ("oversized", 96 * 1024)] {
            let (ReaderWithDir { reader, dir }, counted) = counted_timestamp_reader(vec![
                padded_batch(0, 100, first_value_len),
                padded_batch(1, 200, 40 * 1024),
            ]);
            let found = reader
                .offset_for_timestamp(&tp(), 150, 0, None, &EpochLineage::unknown(), None)
                .await
                .unwrap();
            assert2::assert!(found == Some((1, 200)), "{name}");
            let ranges = counted.log_fetches.lock().unwrap();
            assert!(ranges.len() >= 2, "{name}: {ranges:?}");
            assert!(ranges.iter().all(|(_, end, _)| end.is_some()));
            drop(dir);
        }
    }
}
