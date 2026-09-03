//! Timestamp-to-offset lookups: the `ListOffsets` queries that search by
//! record time rather than by offset.
//!
//! Each search reads sealed segments oldest-first and then the active
//! segment, and ties resolve to the earliest offset as KIP-734 requires.

use std::time::SystemTime;

use krabka_ids::Offset;
use krabka_units::prelude::ByteSizeExt as _;

use super::Log;
use crate::{error::LogError, name, retention, segment::Segment};

impl Log {
    /// Legacy `ListOffsets` v0 segment boundaries at or before `timestamp`,
    /// newest first and capped by `max_num_offsets`.
    ///
    /// Version 0 predates record timestamps. Its timestamp lookup uses each
    /// segment file's modification time and returns segment base offsets (plus
    /// the log end for a non-empty active segment), matching Kafka's
    /// `legacyFetchOffsetsBefore` behavior.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when a segment's modification time cannot be read.
    pub fn legacy_offsets_before(
        &self,
        timestamp: i64,
        max_num_offsets: usize,
    ) -> Result<Vec<Offset>, LogError> {
        let segments: Vec<&Segment> = self.segments.iter().chain(self.active.as_ref()).collect();
        let log_start = self.log_start_offset();
        let mut offset_times = Vec::with_capacity(segments.len() + 1);
        for segment in &segments {
            let modified = std::fs::metadata(name::log_path(&self.dir, segment.base_offset().0))?
                .modified()?;
            offset_times.push((
                segment.base_offset().max(log_start),
                retention::now_ms(modified),
            ));
        }
        if segments
            .last()
            .is_some_and(|segment| segment.size() > krabka_units::ByteSize::ZERO)
        {
            offset_times.push((self.log_end_offset(), retention::now_ms(SystemTime::now())));
        }

        let start = match timestamp {
            -1 => offset_times.len().checked_sub(1),
            -2 => (!offset_times.is_empty()).then_some(0),
            _ => offset_times
                .iter()
                .rposition(|(_, modified)| *modified <= timestamp),
        };
        let Some(start) = start else {
            return Ok(Vec::new());
        };
        Ok(offset_times[..=start]
            .iter()
            .rev()
            .take(max_num_offsets)
            .map(|(offset, _)| *offset)
            .collect())
    }

    /// Earliest local `(offset, record_timestamp)` whose record timestamp is
    /// `>= target_ts`.
    ///
    /// The search reads sealed segments oldest-first and then the active
    /// segment. The first segment whose `max_timestamp >= target_ts` holds
    /// the answer. The per-segment helper does the index lookup and the
    /// forward scan. The result is `None` when no local record qualifies,
    /// including the case of an empty log.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        for seg in &self.segments {
            // A sealed segment restores its maximum on open, so the unknown
            // sentinel survives only where the segment holds no readable
            // batch. There is nothing in such a segment to find.
            if seg.max_timestamp() >= target_ts
                && let Some(hit) = seg.offset_for_timestamp_with_window(target_ts, scan_window)
            {
                return Some(hit);
            }
        }
        if let Some(active) = &self.active
            && active.max_timestamp() >= target_ts
        {
            return active.offset_for_timestamp_with_window(target_ts, scan_window);
        }
        None
    }

    /// Offset and timestamp of the record that carries the partition's
    /// largest timestamp.
    ///
    /// The scan reads sealed segments and then the active segment. Ties
    /// resolve to the earliest offset: the first segment wins, and the first
    /// record within it wins. The result is `None` when the log holds no
    /// records.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn max_timestamp_offset_and_ts(&self) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        let mut best: Option<(i64, Offset)> = None; // (timestamp, offset)
        let candidates = self.segments.iter().chain(self.active.as_ref());
        for seg in candidates {
            if let Some((offset, ts)) = seg.offset_of_max_timestamp_with_window(scan_window)
                && best.is_none_or(|(best_ts, _)| ts > best_ts)
            {
                best = Some((ts, offset));
            }
        }
        best.map(|(ts, offset)| (offset, ts))
    }

    /// Offset of the record carrying the partition's largest timestamp,
    /// or `log_start_offset()` when the log holds no records (KIP-734
    /// `MAX_TIMESTAMP`).
    #[must_use]
    pub fn offset_of_max_timestamp(&self) -> Offset {
        self.max_timestamp_offset_and_ts()
            .map_or_else(|| self.log_start_offset(), |(offset, _)| offset)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, kibibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{sample_batch, ts_batch},
        segment::Segment,
    };

    /// The largest timestamp in the log wins, and the first segment holding it
    /// keeps the answer when several do.
    ///
    /// KIP-734 asks for the offset of the maximum timestamp, so a tie has to
    /// resolve to one offset -- and taking the later one hands back a record
    /// that is not the first with that timestamp.
    #[test]
    fn the_max_timestamp_offset_comes_from_the_first_segment_holding_it() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: kibibytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // Every batch carries the same timestamps, so several segments share
        // the maximum and only the ordering separates them.
        for _ in 0..40 {
            let mut batch = sample_batch(4);
            log.append(&mut batch).expect("append");
        }
        check!(!log.segments.is_empty(), "the appends should have rolled");

        let (offset, ts) = log
            .max_timestamp_offset_and_ts()
            .expect("a log with records has a maximum");
        // sample_batch stamps every record at the same timestamp, so the
        // maximum is shared and the earliest offset carrying it is the answer.
        let earliest = log
            .segments
            .iter()
            .chain(log.active.as_ref())
            .filter_map(Segment::offset_of_max_timestamp)
            .filter(|(_, seg_ts)| *seg_ts == ts)
            .map(|(seg_offset, _)| seg_offset)
            .min()
            .expect("some segment holds the maximum");
        check!(
            offset == earliest,
            "got {offset:?}, earliest is {earliest:?}"
        );
    }

    #[test]
    fn log_offset_for_timestamp_across_segments() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1), // roll after every batch → each record its own segment
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // offsets 0..=4 with timestamps 100,200,300,400,500.
        for (_name, i, ts) in [
            ("first", 0, 100),
            ("second", 1, 200),
            ("third", 2, 300),
            ("fourth", 3, 400),
            ("fifth", 4, 500),
        ] {
            let mut b = ts_batch(ts);
            assert2::assert!(log.append(&mut b).unwrap().0 == Offset(i));
        }
        for (name, ts, want) in [
            // before-first → offset 0.
            ("before first", 50, Some((Offset(0), 100))),
            // exact match on a sealed segment.
            ("exact sealed", 300, Some((Offset(2), 300))),
            // between records → next record up.
            ("between records", 350, Some((Offset(3), 400))),
            // landing on the active segment's record.
            ("active record", 500, Some((Offset(4), 500))),
            // after-last → None.
            ("after last", 600, None),
        ] {
            check!(log.offset_for_timestamp(ts) == want, "case {name}: ts={ts}");
        }
        log.close();
        drop(dir);
    }

    #[test]
    fn reopened_log_scans_sealed_segments_with_unknown_max_timestamp() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        {
            let mut log = Log::open(dir.path(), config.clone()).unwrap();
            for timestamp in [100, 200, 300] {
                log.append(&mut ts_batch(timestamp)).unwrap();
            }
            log.close();
        }

        let log = Log::open(dir.path(), config).unwrap();
        assert2::assert!(log.offset_for_timestamp(150) == Some((Offset(1), 200)));
        assert2::assert!(log.max_timestamp_offset_and_ts() == Some((Offset(2), 300)));
        log.close();
    }

    #[test]
    fn configured_io_policy_reaches_reads_and_timestamp_scans() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                read_buffer_cap: bytes(1),
                timestamp_scan_window: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        let mut batch = ts_batch(100);
        log.append(&mut batch).unwrap();

        assert2::assert!(
            !log.read_raw(Offset(0), Offset(1), kibibytes(1))
                .unwrap()
                .bytes
                .is_empty()
        );
        assert2::assert!(log.offset_for_timestamp(100) == Some((Offset(0), 100)));
    }

    #[test]
    fn log_offset_for_timestamp_empty_log_is_none() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.offset_for_timestamp(0) == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_offset_of_max_timestamp_in_active() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1), // each record its own segment
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // timestamps 100,300,200 at offsets 0,1,2. Max is 300 @ offset 1.
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        assert2::assert!(log.offset_of_max_timestamp() == 1);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_offset_of_max_timestamp_empty_is_log_start() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.offset_of_max_timestamp() == log.log_start_offset());
        assert2::assert!(log.max_timestamp_offset_and_ts() == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_max_timestamp_offset_and_ts_returns_pair() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        // Max timestamp 300 lives at offset 1.
        assert2::assert!(log.max_timestamp_offset_and_ts() == Some((Offset(1), 300)));
        log.close();
        drop(dir);
    }
}
