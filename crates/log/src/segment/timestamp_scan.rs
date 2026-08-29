//! Timestamp lookups over a segment: the sparse time-index floor, the forward
//! scan that refines it, and the restore of `max_timestamp` after a no-scan
//! open.
//!
//! Kafka's `LogSegment.findOffsetByTimestamp` needs a record scan after the
//! index lookup because the index is sparse, and all of the windowing that the
//! scan needs lives here.

use std::ops::ControlFlow;

use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, bytes};

use super::Segment;
use crate::{config::DEFAULT_TIMESTAMP_SCAN_WINDOW, error::LogError};

impl Segment {
    /// Absolute offset and record timestamp of the first record in this
    /// segment whose timestamp is `>= target_ts`.
    ///
    /// This method takes a floor position from the sparse time index, then
    /// scans `.log` batches forward. The index is sparse, so an exact answer
    /// needs that scan after the index lookup. This matches Kafka's
    /// `LogSegment.findOffsetByTimestamp`. The result is `None` when no
    /// record in this segment qualifies.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        self.offset_for_timestamp_with_window(target_ts, DEFAULT_TIMESTAMP_SCAN_WINDOW)
    }

    pub(crate) fn offset_for_timestamp_with_window(
        &self,
        target_ts: i64,
        scan_window: ByteSize,
    ) -> Option<(Offset, i64)> {
        let floor_rel = self.time_index.lookup(target_ts);
        let scan_from = self.base_offset + i64::from(floor_rel);
        self.scan_from_floor_windowed(scan_from, scan_window, |ts| ts >= target_ts)
    }

    /// Absolute offset and timestamp of the record that carries this
    /// segment's `max_timestamp`.
    ///
    /// Ties resolve to the earliest offset, as in Kafka. The result is `None`
    /// for an empty segment. This method starts the scan at the time index's
    /// floor for the maximum, then scans forward for the first record whose
    /// timestamp equals the segment maximum.
    #[must_use]
    pub fn offset_of_max_timestamp(&self) -> Option<(Offset, i64)> {
        self.offset_of_max_timestamp_with_window(DEFAULT_TIMESTAMP_SCAN_WINDOW)
    }

    pub(crate) fn offset_of_max_timestamp_with_window(
        &self,
        scan_window: ByteSize,
    ) -> Option<(Offset, i64)> {
        if self.max_timestamp == i64::MIN {
            return self.scan_max_timestamp_windowed(scan_window);
        }
        let floor_rel = self.time_index.lookup(self.max_timestamp);
        let scan_from = self.base_offset + i64::from(floor_rel);
        // Equality against `max_timestamp` is safe because Kafka's batch
        // `max_timestamp` is always a real record timestamp (the largest
        // among the batch's records), so some record's timestamp equals
        // the segment max exactly.
        self.scan_from_floor_windowed(scan_from, scan_window, |ts| ts == self.max_timestamp)
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
                window *= 2.0;
                continue;
            }
            for batch in &batches {
                for record in &batch.records {
                    let timestamp = batch.base_timestamp + record.timestamp_delta;
                    if best.is_none_or(|(_, best_timestamp)| timestamp > best_timestamp) {
                        best = Some((
                            Offset(batch.base_offset + i64::from(record.offset_delta)),
                            timestamp,
                        ));
                    }
                }
            }
            let last = batches.last().expect("non-empty checked above");
            cursor = Offset(last.base_offset + i64::from(last.last_offset_delta)) + 1;
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
    fn scan_from_floor_windowed(
        &self,
        floor_offset: Offset,
        window_size: ByteSize,
        pred: impl Fn(i64) -> bool,
    ) -> Option<(Offset, i64)> {
        let mut cursor = floor_offset;
        let mut window = window_size.max(bytes(1));
        loop {
            if cursor > self.last_offset {
                return None;
            }
            let batches = self.read(cursor, window).ok()?;
            if batches.is_empty() {
                // The batch at `cursor` is larger than the window, so it
                // could not be fully decoded. Grow the window and retry
                // the same cursor; bounded by the largest batch size.
                window *= 2.0;
                continue;
            }
            for batch in &batches {
                for rec in &batch.records {
                    let ts = batch.base_timestamp + rec.timestamp_delta;
                    if pred(ts) {
                        return Some((Offset(batch.base_offset + i64::from(rec.offset_delta)), ts));
                    }
                }
            }
            // No match in this window; resume just past the last batch
            // read. `read` includes the batch covering `cursor`, so
            // `last_read` >= cursor and the cursor strictly advances.
            let last = batches.last().expect("non-empty checked above");
            let last_read = Offset(last.base_offset + i64::from(last.last_offset_delta));
            cursor = last_read + 1;
        }
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
        let mut max_timestamp = self
            .time_index
            .last_entry()
            .map_or(i64::MIN, |(timestamp, _)| timestamp);
        let scan_from = self
            .offset_index
            .last_entry()
            .map_or(0, |(_, position)| u64::from(position));
        self.walk_batch_headers(scan_from, |view| {
            max_timestamp = max_timestamp.max(view.max_timestamp);
            ControlFlow::Continue(())
        })?;
        self.max_timestamp = self.max_timestamp.max(max_timestamp);
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
                seg.scan_from_floor_windowed(Offset(0), bytes(1), |ts| ts >= threshold) == expected
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
        let got = seg.scan_from_floor_windowed(Offset(0), WINDOW, |ts| ts >= 202);
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
