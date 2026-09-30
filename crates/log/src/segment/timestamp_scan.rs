//! Timestamp lookups over a segment: the sparse time-index floor, the forward
//! scan that refines it, and the restore of `max_timestamp` after a no-scan
//! open.
//!
//! Kafka's `LogSegment.findOffsetByTimestamp` needs a record scan after the
//! index lookup because the index is sparse, and all of the windowing that the
//! scan needs lives here.

use std::ops::ControlFlow;

use krabka_ids::Offset;
use krabka_protocol::records::{RecordBatch, TimestampType};
use krabka_units::prelude::{ByteSize, ByteSizeExt, bytes};

use super::Segment;
use crate::{
    config::DEFAULT_TIMESTAMP_SCAN_WINDOW, error::LogError, record_limit::check_records_read,
};

impl Segment {
    /// Absolute offset and record timestamp of the first record in this
    /// segment whose timestamp is `>= target_ts`.
    ///
    /// This method takes a floor position from the sparse time index, then
    /// scans `.log` batches forward. The index is sparse, so an exact answer
    /// needs that scan after the index lookup. This matches Kafka's
    /// `LogSegment.findOffsetByTimestamp`. The result is `None` when no
    /// record in this segment qualifies.
    ///
    /// The scan applies no `max.decompressed.message.bytes`: this is the lookup
    /// the log's own bookkeeping uses, to read a segment's first timestamp. A
    /// `ListOffsets` answer goes through
    /// [`Segment::offset_for_timestamp_with_window`], which takes the limit.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        // With no limit the scan has nothing to refuse.
        self.offset_for_timestamp_with_window(target_ts, DEFAULT_TIMESTAMP_SCAN_WINDOW, None)
            .ok()
            .flatten()
    }

    /// [`Segment::offset_for_timestamp`] over `scan_window`-sized reads, with
    /// `limit` as Kafka trunk's `max.decompressed.message.bytes`.
    ///
    /// Kafka's `FileRecords.searchForTimestamp` decompresses a batch only when
    /// its max timestamp reaches `target_ts`, and reads it only as far as the
    /// record it returns, so a `limit` fails the lookup only for a record that
    /// Kafka's iterator would have read.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for such a record of a compressed
    /// batch. A scan with no `limit` never fails; an unreadable segment
    /// answers `None`.
    pub(crate) fn offset_for_timestamp_with_window(
        &self,
        target_ts: i64,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let floor_rel = self.time_index.lookup(target_ts);
        let Some(scan_from) = self
            .base_offset
            .0
            .checked_add(i64::from(floor_rel))
            .map(Offset)
        else {
            return Ok(None);
        };
        self.scan_from_floor_windowed(scan_from, scan_window, target_ts, limit)
    }

    /// [`Segment::offset_of_max_timestamp_with_window`] over the default window
    /// with no limit, for the tests that only care where the answer is.
    #[cfg(test)]
    pub(crate) fn offset_of_max_timestamp(&self) -> Option<(Offset, i64)> {
        self.offset_of_max_timestamp_with_window(DEFAULT_TIMESTAMP_SCAN_WINDOW, None)
            .expect("a lookup with no limit refuses nothing")
    }

    /// Absolute offset and timestamp of the record that carries this
    /// segment's `max_timestamp`, read in `scan_window`-sized pieces, with
    /// `limit` as Kafka trunk's `max.decompressed.message.bytes`: Kafka's
    /// `RecordBatch.offsetOfMaxTimestamp` decompresses the batch that holds the
    /// maximum, up to the record it returns.
    ///
    /// Ties resolve to the earliest offset, as in Kafka. The result is `None`
    /// for an empty segment. This method starts the scan at the time index's
    /// floor for the maximum, then scans forward for the first record whose
    /// timestamp equals the segment maximum.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for a record of that batch that the
    /// `limit` refuses. A scan with no `limit` never fails.
    pub(crate) fn offset_of_max_timestamp_with_window(
        &self,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        if self.max_timestamp == i64::MIN {
            let found = self.scan_max_timestamp_windowed(scan_window);
            // The unknown-maximum scan reads every record, so the limit is
            // checked on the batch that holds the maximum the way the
            // known-maximum path checks it: by looking the maximum up again.
            if let Some((_, timestamp)) = found
                && limit.is_some()
            {
                self.scan_from_floor_windowed(self.base_offset, scan_window, timestamp, limit)?;
            }
            return Ok(found);
        }
        let floor_rel = self.time_index.lookup(self.max_timestamp);
        let Some(scan_from) = self
            .base_offset
            .0
            .checked_add(i64::from(floor_rel))
            .map(Offset)
        else {
            return Ok(None);
        };
        // Equality against `max_timestamp` is safe because Kafka's batch
        // `max_timestamp` is always a real record timestamp (the largest
        // among the batch's records), so some record's timestamp equals
        // the segment max exactly.
        self.scan_from_floor_windowed(scan_from, scan_window, self.max_timestamp, limit)
    }

    /// Recover the maximum timestamp for a sealed segment opened through the
    /// no-scan path. Those segments intentionally keep `max_timestamp` at its
    /// unknown sentinel, so KIP-734 `MAX_TIMESTAMP` must derive the answer
    /// from records instead of treating the segment as empty.
    fn scan_max_timestamp_windowed(&self, window_size: ByteSize) -> Option<(Offset, i64)> {
        let mut cursor = self.base_offset;
        let mut window = window_size.max(bytes(1));
        let mut best: Option<(Offset, i64)> = None;
        loop {
            if cursor > self.last_offset {
                return best;
            }
            let batches = self.read(cursor, window).ok()?;
            if batches.is_empty() {
                let current = u32::try_from(window.bytes_u64()).ok()?;
                window = bytes(krabka_verified::timestamp_scan_window(current)?);
                continue;
            }
            for batch in &batches {
                let records = Self::timestamp_records(batch)?;
                let timestamps: Vec<_> = records.iter().map(|(_, timestamp)| *timestamp).collect();
                if let Some(index) = krabka_verified::earliest_max_timestamp_index(&timestamps) {
                    let candidate = records[index];
                    if best.is_none_or(|(_, best_timestamp)| candidate.1 > best_timestamp) {
                        best = Some(candidate);
                    }
                }
            }
            let last = batches.last().expect("non-empty checked above");
            cursor = Offset(krabka_verified::timestamp_scan_next(
                cursor.0,
                last.base_offset,
                last.last_offset_delta,
            )?);
        }
    }

    /// Window-size-parameterized core of [`Segment::scan_from_floor`]. It is
    /// a separate function so that tests can force multi-window scans with a
    /// tiny window.
    ///
    /// Termination: each iteration does one of three things. It returns a
    /// match. It returns `None` because `cursor > last_offset`. Or it decodes
    /// at least one full batch and advances `cursor` strictly past that batch.
    /// `read` caps reads at `max_bytes` and, unlike `read_raw`, gives no
    /// anti-stall guarantee. A single batch larger than the window therefore
    /// decodes to an empty `Vec`. This function detects that case, an empty
    /// result while `cursor` is still within the segment, and doubles the
    /// window before it tries again. The window is therefore bounded by the
    /// largest batch, not by the whole tail.
    ///
    /// `limit` is Kafka trunk's `max.decompressed.message.bytes`. A batch whose
    /// max timestamp reaches `target_ts` is the one Kafka's scan decompresses,
    /// and it reads that batch up to the record it returns, or to its end when
    /// none qualifies, so the limit is checked over exactly those records. A
    /// scan that cannot proceed (an unreadable segment, an offset that
    /// overflows) answers `None`, as it did before the limit existed.
    fn scan_from_floor_windowed(
        &self,
        floor_offset: Offset,
        window_size: ByteSize,
        target_ts: i64,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let mut cursor = floor_offset;
        let mut window = window_size.max(bytes(1));
        loop {
            if cursor > self.last_offset {
                return Ok(None);
            }
            let Ok(batches) = self.read(cursor, window) else {
                return Ok(None);
            };
            if batches.is_empty() {
                // The batch at `cursor` is larger than the window, so it
                // could not be fully decoded. Grow the window and retry
                // the same cursor; bounded by the largest batch size.
                let Some(grown) = u32::try_from(window.bytes_u64())
                    .ok()
                    .and_then(krabka_verified::timestamp_scan_window)
                else {
                    return Ok(None);
                };
                window = bytes(grown);
                continue;
            }
            for batch in &batches {
                let Some(records) = Self::timestamp_records(batch) else {
                    return Ok(None);
                };
                let timestamps: Vec<_> = records.iter().map(|(_, timestamp)| *timestamp).collect();
                let found = krabka_verified::first_timestamp_index(&timestamps, target_ts);
                if batch.max_timestamp >= target_ts {
                    let read = found.map_or(batch.records.len(), |index| index + 1);
                    check_records_read(batch, read, limit)?;
                }
                if let Some(index) = found {
                    return Ok(Some(records[index]));
                }
            }
            // No match in this window; resume just past the last batch
            // read. `read` includes the batch covering `cursor`, so
            // `last_read` >= cursor and the cursor strictly advances.
            let last = batches.last().expect("non-empty checked above");
            let Some(next) = krabka_verified::timestamp_scan_next(
                cursor.0,
                last.base_offset,
                last.last_offset_delta,
            ) else {
                return Ok(None);
            };
            cursor = Offset(next);
        }
    }

    /// The `(offset, timestamp)` pair of every record in `batch`, as a reader
    /// sees them.
    ///
    /// Under `CreateTime` a record's timestamp is the batch's `base_timestamp`
    /// plus its own delta, which is how the v2 format stores it. Under
    /// `LogAppendTime` every record carries the batch's `max_timestamp`
    /// instead: the broker stamped that one field and left `base_timestamp`
    /// and the deltas as the producer wrote them, and Kafka's
    /// `DefaultRecord.timestamp()` substitutes `maxTimestamp` for the whole
    /// batch whenever the timestamp-type bit is set. A scan that read the
    /// deltas on such a batch would answer `ListOffsets` in producer time on a
    /// topic whose whole point is that it answers in append time.
    fn timestamp_records(batch: &RecordBatch) -> Option<Vec<(Offset, i64)>> {
        let log_append_time = batch.attributes.timestamp_type() == TimestampType::LogAppendTime;
        batch
            .records
            .iter()
            .map(|record| {
                krabka_verified::timestamp_record_coordinates(
                    batch.base_offset,
                    record.offset_delta,
                    batch.base_timestamp,
                    record.timestamp_delta,
                )
                .map(|(offset, timestamp)| {
                    (
                        Offset(offset),
                        if log_append_time {
                            batch.max_timestamp
                        } else {
                            timestamp
                        },
                    )
                })
            })
            .collect()
    }

    /// Restore `max_timestamp` for a segment loaded through the no-scan
    /// [`Segment::open`] path.
    ///
    /// `open` leaves the field at its unknown sentinel. Retention compares
    /// that sentinel against the age cutoff, so without this call every
    /// reopened segment looks older than any window and the first
    /// [`Log::tick`](crate::Log::tick) after a restart deletes all of them.
    ///
    /// Kafka's `LogSegment` reads `maxTimestampSoFar` back from the time
    /// index, and this method starts there. The sparse index lags the writes,
    /// though: its newest entry holds the running maximum as of the last
    /// *indexed* batch, and every batch appended after it is unaccounted for.
    /// The method therefore also walks the batch headers from the newest
    /// offset-index entry to the end of the file. That walk is bounded by
    /// `index_interval` plus one batch, and it reads no record body.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::Io`] when the `.log` file cannot be read.
    pub fn restore_max_timestamp(&mut self) -> Result<(), LogError> {
        // The time index's newest entry pairs the running maximum with the
        // last offset of the batch that set it, which is where the restored
        // maximum's batch starts too.
        let (mut max_timestamp, mut max_timestamp_offset) = self
            .time_index
            .last_entry()
            .map_or((i64::MIN, self.base_offset - 1), |(timestamp, relative)| {
                (timestamp, self.base_offset + i64::from(relative))
            });
        let scan_from = self
            .offset_index
            .last_entry()
            .map_or(0, |(_, position)| u64::from(position));
        self.walk_batch_headers(scan_from, |view| {
            if view.max_timestamp > max_timestamp {
                max_timestamp = view.max_timestamp;
                max_timestamp_offset = view.last_offset;
            }
            ControlFlow::Continue(())
        })?;
        if max_timestamp > self.max_timestamp {
            self.max_timestamp = max_timestamp;
            self.max_timestamp_offset = max_timestamp_offset;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_protocol::records::{Record, RecordBatch};
    use krabka_units::prelude::kibibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, sample_batch};

    /// The scan reports the offset of the maximum timestamp as
    /// `batch.base_offset + record.offset_delta`, and keeps the first record
    /// holding that timestamp when several share it.
    #[test]
    fn the_windowed_scan_reports_the_first_offset_at_the_maximum() {
        let dir = tempdir().unwrap();
        // A non-zero base offset and a non-zero delta, so a sum is telling
        // apart from a product and from either operand alone.
        let mut seg = Segment::create(dir.path(), Offset(10)).unwrap();
        let mut batch = RecordBatch {
            base_offset: 10,
            base_timestamp: 500,
            max_timestamp: 502,
            last_offset_delta: 2,
            ..RecordBatch::default()
        };
        // Records at offsets 10, 11, 12 with timestamps 500, 502, 502: the
        // maximum is shared, and the first to hold it is offset 11.
        for (delta, ts_delta) in [(0i32, 0i64), (1, 2), (2, 2)] {
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: ts_delta,
                key: Some(Bytes::from(format!("k{delta}"))),
                value: Some(Bytes::from(format!("v{delta}"))),
                ..Default::default()
            });
        }
        seg.append(&batch, DENSE_INDEX).unwrap();

        let found = seg.scan_max_timestamp_windowed(kibibytes(64));
        check!(found == Some((Offset(11), 502)), "got {found:?}");

        // Multiple batches: max timestamp is at last_offset
        seg.append(&sample_batch(13, 1, 600), DENSE_INDEX).unwrap();
        seg.max_timestamp = i64::MIN;
        let found_last = seg.scan_max_timestamp_windowed(bytes(1));
        check!(found_last == Some((Offset(13), 600)));

        // offset_of_max_timestamp routes through scan_max_timestamp_windowed when max_timestamp is MIN
        let found_via_api = seg.offset_of_max_timestamp();
        check!(found_via_api == Some((Offset(13), 600)));
    }

    #[test]
    fn malformed_record_coordinates_fail_the_scan_closed() {
        let offset_overflow = RecordBatch {
            base_offset: i64::MAX,
            base_timestamp: 0,
            records: vec![Record {
                offset_delta: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let timestamp_overflow = RecordBatch {
            base_offset: 0,
            base_timestamp: i64::MAX,
            records: vec![Record {
                timestamp_delta: 1,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(Segment::timestamp_records(&offset_overflow).is_none());
        assert2::assert!(Segment::timestamp_records(&timestamp_overflow).is_none());
    }

    #[test]
    fn malformed_or_stale_time_index_floor_fails_closed() {
        let dir = tempdir().unwrap();
        let mut stale = Segment::create(dir.path(), Offset(0)).unwrap();
        stale.append(&sample_batch(0, 1, 100), DENSE_INDEX).unwrap();
        stale.time_index.append(200, u32::MAX).unwrap();
        assert2::assert!(stale.offset_for_timestamp(200).is_none());

        let dir2 = tempdir().unwrap();
        let mut overflowing = Segment::create(dir2.path(), Offset(i64::MAX)).unwrap();
        overflowing.last_offset = Offset(i64::MAX);
        overflowing.time_index.append(0, 1).unwrap();
        assert2::assert!(overflowing.offset_for_timestamp(0).is_none());
    }

    #[test]
    fn truncated_log_failure_exhausts_the_retry_window() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 100), DENSE_INDEX).unwrap();
        seg.log_file.set_len(1).unwrap();

        assert2::assert!(
            seg.offset_for_timestamp_with_window(100, bytes(u32::MAX), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn offset_for_timestamp_finds_first_ge() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Two batches: offsets 0..=2 ts 100..=102, offsets 3..=4 ts 200..=201.
        seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap();
        seg.append(&sample_batch(3, 2, 200), DENSE_INDEX).unwrap();
        // sample_batch sets per-record timestamp_delta = i, base_timestamp = ts_base.
        // Batch 1 records: (off0,ts100),(off1,ts101),(off2,ts102).
        // Batch 2 records: (off3,ts200),(off4,ts201).
        for (name, ts, want) in [
            ("first exact", 100, Some((Offset(0), 100))),
            ("within first batch", 101, Some((Offset(1), 101))),
            ("between batches", 150, Some((Offset(3), 200))),
            ("last exact", 201, Some((Offset(4), 201))),
            ("past end", 202, None),
        ] {
            check!(seg.offset_for_timestamp(ts) == want, "case {name}: ts={ts}");
        }
        drop(dir);
    }

    #[test]
    fn scan_from_floor_finds_match_beyond_first_window() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Many single-record batches with increasing timestamps. With a
        // tiny scan window each batch lands in its own window, so a match
        // at the tail forces the windowed loop to advance many times.
        let n = 50i64;
        for off in 0..n {
            let mut b = RecordBatch {
                base_offset: off,
                base_timestamp: 1_000 + off,
                max_timestamp: 1_000 + off,
                last_offset_delta: 0,
                ..RecordBatch::default()
            };
            b.records.push(Record {
                offset_delta: 0,
                timestamp_delta: 0,
                value: Some(Bytes::from(format!("v{off}"))),
                ..Default::default()
            });
            seg.append(&b, DENSE_INDEX).unwrap();
        }
        // A window of 1 byte forces one batch per read (anti-stall rule).
        // Target ts is the very last record's, so the loop must advance
        // through every window before matching.
        let target = 1_000 + (n - 1);
        for (_name, threshold, expected) in [
            (
                "match at final record",
                target,
                Some((Offset(n - 1), target)),
            ),
            ("no matching record", 10_001, None),
        ] {
            assert2::assert!(
                seg.scan_from_floor_windowed(Offset(0), bytes(1), threshold, None)
                    .unwrap()
                    == expected
            );
        }
        drop(dir);
    }

    #[test]
    fn scan_returns_absolute_offset_of_matching_record() {
        // A full-size window keeps the match in the first read so the
        // cursor-advance path isn't involved.
        const WINDOW: ByteSize = kibibytes(64);
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // A leading single-record batch at offset 0, then a 3-record batch
        // based at offset 1 (abs offsets 1,2,3; timestamps 200,201,202). The
        // match is the *third* record, whose absolute offset is
        // `base_offset + offset_delta = 1 + 2 = 3` — a value that only a
        // correct `+` reproduces (`1 - 2` or `1 * 2` both differ), so this
        // pins the returned offset arithmetic.
        seg.append(&sample_batch(0, 1, 100), DENSE_INDEX).unwrap();
        seg.append(&sample_batch(1, 3, 200), DENSE_INDEX).unwrap();
        let got = seg
            .scan_from_floor_windowed(Offset(0), WINDOW, 202, None)
            .unwrap();
        assert2::assert!(got == Some((Offset(3), 202)));
        drop(dir);
    }

    #[test]
    fn offset_of_max_timestamp_earliest_on_tie() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Batch records ts: 100,101,102 (max in batch = 102 at offset 2).
        seg.append(&sample_batch(0, 3, 100), DENSE_INDEX).unwrap();
        // Second batch: offsets 3,4 ts 200,201 — segment max becomes 201 @4.
        seg.append(&sample_batch(3, 2, 200), DENSE_INDEX).unwrap();
        assert2::assert!(seg.offset_of_max_timestamp() == Some((Offset(4), 201)));

        // Empty segment → None.
        let dir2 = tempdir().unwrap();
        let empty = Segment::create(dir2.path(), Offset(0)).unwrap();
        assert2::assert!(empty.offset_of_max_timestamp() == None);
        drop(dir);
        drop(dir2);
    }

    #[test]
    fn offset_of_max_timestamp_tie_picks_earliest() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // All three records share timestamp 500; earliest offset is 0.
        let mut b = RecordBatch {
            base_offset: 0,
            base_timestamp: 500,
            max_timestamp: 500,
            last_offset_delta: 2,
            ..RecordBatch::default()
        };
        for i in 0..3 {
            b.records.push(Record {
                offset_delta: i,
                timestamp_delta: 0,
                value: Some(Bytes::from("v")),
                ..Default::default()
            });
        }
        seg.append(&b, DENSE_INDEX).unwrap();
        assert2::assert!(seg.offset_of_max_timestamp() == Some((Offset(0), 500)));
        drop(dir);
    }
}

/// Kafka trunk's `max.decompressed.message.bytes` on the by-timestamp lookups:
/// `FileRecords.searchForTimestamp` and `RecordBatch.offsetOfMaxTimestamp`.
#[cfg(test)]
mod record_limit_tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_compression::CompressionType;
    use krabka_protocol::records::{Attributes, Record, RecordBatch};
    use krabka_units::prelude::{ByteSize, bytes, kibibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::DENSE_INDEX;

    /// The body size of a record with a `1000`-byte value and nothing else:
    /// its attributes byte, a one-byte timestamp and offset delta, a null key,
    /// the value behind its two-byte length, and no headers.
    const LARGE: usize = 1_007;
    const LIMIT: ByteSize = bytes(100);
    const WINDOW: ByteSize = kibibytes(64);

    /// A batch at `base_offset` holding one record per `(timestamp, value
    /// length)` pair, compressed with `codec`.
    fn batch(base_offset: i64, records: &[(i64, usize)], codec: CompressionType) -> RecordBatch {
        let base_timestamp = records[0].0;
        RecordBatch {
            base_offset,
            base_timestamp,
            max_timestamp: records.iter().map(|(ts, _)| *ts).max().unwrap(),
            last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
            attributes: Attributes::default().with_compression(codec),
            records: records
                .iter()
                .enumerate()
                .map(|(delta, (timestamp, value_len))| Record {
                    offset_delta: i32::try_from(delta).unwrap(),
                    timestamp_delta: timestamp - base_timestamp,
                    value: Some(Bytes::from(vec![7_u8; *value_len])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn segment(batches: &[RecordBatch]) -> (tempfile::TempDir, Segment) {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        for batch in batches {
            seg.append(batch, DENSE_INDEX).unwrap();
        }
        (dir, seg)
    }

    /// What a lookup answers: the record it found, or the size of the record
    /// it refused.
    type Lookup = Result<Option<(Offset, i64)>, usize>;

    fn answer(result: Result<Option<(Offset, i64)>, LogError>) -> Lookup {
        match result {
            Ok(found) => Ok(found),
            Err(LogError::RecordTooLarge { size, limit }) => {
                check!(limit == LIMIT.bytes_usize());
                Err(size)
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    /// A batch whose max timestamp is below the target is skipped without being
    /// decompressed, and a batch is read only up to the record the lookup
    /// returns, so the limit fails a lookup only for a record Kafka reads. An
    /// uncompressed batch is never held to it.
    #[test]
    fn a_lookup_by_timestamp_refuses_only_the_records_kafka_reads() {
        for (name, codec, limit, target, expected) in [
            (
                "found before the oversized batch",
                CompressionType::Gzip,
                Some(LIMIT),
                1_000,
                Ok(Some((Offset(0), 1_000))),
            ),
            (
                "the oversized record is the match",
                CompressionType::Gzip,
                Some(LIMIT),
                1_001,
                Err(LARGE),
            ),
            (
                "no limit",
                CompressionType::Gzip,
                None,
                1_001,
                Ok(Some((Offset(1), 1_001))),
            ),
            (
                "an uncompressed batch is never held to it",
                CompressionType::None,
                Some(LIMIT),
                1_001,
                Ok(Some((Offset(1), 1_001))),
            ),
        ] {
            let (_dir, seg) = segment(&[
                batch(0, &[(1_000, 10)], codec),
                batch(1, &[(1_001, 1_000)], codec),
            ]);
            let found = seg.offset_for_timestamp_with_window(target, WINDOW, limit);
            check!(answer(found) == expected, "{name}");
        }
    }

    /// Kafka's iterator decodes lazily, so a record after the one it returns is
    /// never read, and one before it always is.
    #[test]
    fn a_lookup_stops_reading_a_batch_at_the_record_it_returns() {
        let (_dir, seg) = segment(&[batch(
            0,
            &[(1_000, 10), (1_001, 1_000), (1_002, 10)],
            CompressionType::Gzip,
        )]);
        for (target, expected) in [
            (1_000, Ok(Some((Offset(0), 1_000)))),
            (1_001, Err(LARGE)),
            // The scan walks past the oversized record to reach the last one.
            (1_002, Err(LARGE)),
            // The batch's max timestamp is below the target, so Kafka skips it
            // without decompressing it.
            (1_003, Ok(None)),
        ] {
            let found = seg.offset_for_timestamp_with_window(target, WINDOW, Some(LIMIT));
            check!(answer(found) == expected, "target {target}");
        }
    }

    /// `RecordBatch.offsetOfMaxTimestamp` decompresses the batch that holds the
    /// segment's maximum, and no other.
    #[test]
    fn a_max_timestamp_lookup_reads_only_the_batch_holding_the_maximum() {
        let gzip = CompressionType::Gzip;
        let (_dir, mut seg) = segment(&[
            batch(0, &[(1_000, 1_000)], gzip),
            batch(1, &[(1_001, 10)], gzip),
        ]);
        let lookup =
            |seg: &Segment| answer(seg.offset_of_max_timestamp_with_window(WINDOW, Some(LIMIT)));
        // The oversized record is in an older batch, which Kafka never reads.
        check!(lookup(&seg) == Ok(Some((Offset(1), 1_001))));

        // Once the oversized batch holds the maximum, the lookup refuses it.
        seg.append(&batch(2, &[(1_002, 1_000)], gzip), DENSE_INDEX)
            .unwrap();
        check!(lookup(&seg) == Err(LARGE));

        // A segment that never learned its maximum finds it by reading
        // every record, and the limit holds there too.
        seg.max_timestamp = i64::MIN;
        check!(lookup(&seg) == Err(LARGE));
        check!(
            seg.offset_of_max_timestamp_with_window(WINDOW, None)
                .unwrap()
                == Some((Offset(2), 1_002))
        );
    }
}
