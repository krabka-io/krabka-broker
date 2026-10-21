//! Timestamp-to-offset lookups: the `ListOffsets` queries that search by
//! record time rather than by offset.
//!
//! Each search reads sealed segments oldest-first and then the active
//! segment, and ties resolve to the earliest offset as KIP-734 requires.
//!
//! A `ListOffsets` seek answers from the first segment whose largest timestamp
//! reaches the target, even when none of that segment's retained records
//! does, as Kafka 4.3.1's `UnifiedLog.searchOffsetInLocalLog` does:
//!
//! ```java
//! for (LogSegment segment : segments) {
//!     if (segment.largestTimestamp() >= targetTimestamp) {
//!         return segment.findOffsetByTimestamp(targetTimestamp, startOffset);
//!     }
//! }
//! ```

use {
    super::Log,
    crate::{error::LogError, name, retention, segment::Segment},
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
    krabka_verified::timestamp::first_timestamp_index,
    std::time::SystemTime,
};

/// How far a by-timestamp search goes.
#[derive(Clone, Copy)]
enum Seek {
    /// Answer from the first segment whose largest timestamp reaches the
    /// target, as Kafka's `ListOffsets` does.
    FirstCandidate,
    /// Go on past segments with no retained match, to the first retained
    /// record at or after the target anywhere in the log.
    FirstRetained,
}

/// Kafka 4.3.1's `LogSegment.largestTimestamp`: the segment's largest record
/// timestamp, or the `.log` file's modification time when the segment has
/// none (`maxTimestampSoFar < 0`, an empty segment or one whose batches carry
/// no timestamp). Java's `File.lastModified` reads 0 when the filesystem
/// cannot report it.
fn largest_timestamp(segment: &Segment) -> i64 {
    let max = segment.max_timestamp();
    if max >= 0 {
        return max;
    }
    segment.last_modified().map_or(0, retention::now_ms)
}

/// The scan window and the record limit of a log's configuration.
struct TimestampScanSettings {
    scan_window: ByteSize,
    limit: Option<ByteSize>,
}

/// The segment that holds the partition's largest timestamp so far, with that
/// timestamp and the offset of its record.
#[derive(Clone, Copy)]
struct BestSegment<'a> {
    timestamp: i64,
    offset: Offset,
    segment: &'a Segment,
}

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
    /// `>= target_ts`, excluding records below the logical log start.
    ///
    /// The search reads sealed segments oldest-first and then the active
    /// segment, and goes past a segment whose `max_timestamp >= target_ts`
    /// comes only from records below the log start. The per-segment helper
    /// does the index lookup and the forward scan. The result is `None` when
    /// no retained local record qualifies, including the case of an empty log.
    ///
    /// This is the lookup the log's own bookkeeping uses, and it applies no
    /// `max.decompressed.message.bytes`. A `ListOffsets` answer goes through
    /// [`Self::offset_for_timestamp_checked`], which does, and which stops at
    /// the first candidate segment as Kafka does.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        // With no limit the scan has nothing to refuse.
        self.scan_offset_for_timestamp(
            target_ts,
            scan_window,
            None,
            self.log_start_offset(),
            Seek::FirstRetained,
        )
        .ok()
        .flatten()
    }

    /// [`Self::offset_for_timestamp`] under the log's
    /// [`max_decompressed_record`](crate::LogConfig::max_decompressed_record),
    /// as Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp` answers it.
    ///
    /// Kafka's scan decompresses a compressed batch whose max timestamp reaches
    /// the target, up to the record it returns, and fails on a record above the
    /// limit. A lookup that never reads the oversized record is unaffected.
    ///
    /// The answer comes from the first segment whose largest timestamp
    /// reaches `target_ts` (Kafka's `LogSegment.largestTimestamp`, which falls
    /// back to the file's modification time for a segment with no record
    /// timestamp). When that segment holds no retained match the answer is
    /// `None`, even if a later segment holds one: Kafka 4.3.1's
    /// `UnifiedLog.searchOffsetInLocalLog` returns
    /// `segment.findOffsetByTimestamp(target, startOffset)` from that first
    /// segment and reads no other.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for such a record.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn offset_for_timestamp_checked(
        &self,
        target_ts: i64,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        self.offset_for_timestamp_checked_from(target_ts, self.log_start_offset())
    }

    /// [`Self::offset_for_timestamp_checked`] restricted to offsets at or above
    /// `minimum`, also respecting this log's own start offset.
    ///
    /// A caller with an authoritative deletion floor from another storage
    /// projection can search the remaining records exactly without changing
    /// this log's state or clamping an earlier hit to a different record.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] when a compressed record the scan
    /// reads exceeds the configured decompressed-record limit.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn offset_for_timestamp_checked_from(
        &self,
        target_ts: i64,
        minimum: Offset,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let TimestampScanSettings { scan_window, limit } = self.timestamp_scan_settings();
        self.scan_offset_for_timestamp(target_ts, scan_window, limit, minimum, Seek::FirstCandidate)
    }

    /// The scan window and the record limit of the log's current configuration.
    fn timestamp_scan_settings(&self) -> TimestampScanSettings {
        let config = self.config.read().unwrap();
        TimestampScanSettings {
            scan_window: config.timestamp_scan_window,
            limit: config.max_decompressed_record,
        }
    }

    /// The by-timestamp search. [`Seek::FirstCandidate`] answers from the first
    /// segment whose largest timestamp reaches `target_ts`, as `ListOffsets`
    /// does; [`Seek::FirstRetained`] goes on until it finds a retained record.
    fn scan_offset_for_timestamp(
        &self,
        target_ts: i64,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
        minimum: Offset,
        seek: Seek,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let minimum = self.log_start_offset().max(minimum);
        match seek {
            Seek::FirstCandidate => {
                let segments: Vec<&Segment> =
                    self.segments.iter().chain(self.active.as_ref()).collect();
                let largest: Vec<i64> = segments.iter().map(|seg| largest_timestamp(seg)).collect();
                let Some(candidate) = first_timestamp_index(&largest, target_ts) else {
                    return Ok(None);
                };
                segments[candidate].offset_for_timestamp_with_window_from(
                    target_ts,
                    scan_window,
                    limit,
                    minimum,
                )
            }
            Seek::FirstRetained => {
                for seg in &self.segments {
                    // A sealed segment restores its maximum on open, so the
                    // unknown sentinel survives only where the segment holds
                    // no readable batch. There is nothing in such a segment
                    // to find.
                    if seg.last_offset() >= minimum
                        && seg.max_timestamp() >= target_ts
                        && let Some(hit) = seg.offset_for_timestamp_with_window_from(
                            target_ts,
                            scan_window,
                            limit,
                            minimum,
                        )?
                    {
                        return Ok(Some(hit));
                    }
                }
                if let Some(active) = &self.active
                    && active.max_timestamp() >= target_ts
                {
                    return active.offset_for_timestamp_with_window_from(
                        target_ts,
                        scan_window,
                        limit,
                        minimum,
                    );
                }
                Ok(None)
            }
        }
    }

    /// Offset and timestamp of the record that carries the partition's
    /// largest timestamp (KIP-734 `MAX_TIMESTAMP`), under the log's
    /// [`max_decompressed_record`](crate::LogConfig::max_decompressed_record),
    /// as Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp(MAX_TIMESTAMP)`
    /// answers it: Kafka decompresses only the batch that holds the log's
    /// maximum, up to the record it returns.
    ///
    /// The scan reads sealed segments and then the active segment. Ties
    /// resolve to the earliest offset: the first segment wins, and the first
    /// record within it wins. The result is `None` when the log holds no
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for a record of that batch above the
    /// limit.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn max_timestamp_offset_and_ts(&self) -> Result<Option<(Offset, i64)>, LogError> {
        let TimestampScanSettings { scan_window, limit } = self.timestamp_scan_settings();
        self.scan_max_timestamp(scan_window, limit)
    }

    fn scan_max_timestamp(
        &self,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let mut best: Option<BestSegment<'_>> = None;
        let candidates = self.segments.iter().chain(self.active.as_ref());
        for seg in candidates {
            // Every segment is measured without the limit: Kafka picks the
            // winner from the segments' max timestamps alone, and reads only
            // the winner's batch.
            if let Some((offset, ts)) =
                seg.offset_of_max_timestamp_with_window(scan_window, None)?
                && best.is_none_or(|best| ts > best.timestamp)
            {
                best = Some(BestSegment {
                    timestamp: ts,
                    offset,
                    segment: seg,
                });
            }
        }
        if let Some(BestSegment {
            segment: winner, ..
        }) = best
            && limit.is_some()
        {
            winner.offset_of_max_timestamp_with_window(scan_window, limit)?;
        }
        Ok(best.map(|best| (best.offset, best.timestamp)))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::LogConfig,
            log::test_support::{
                TempLog, configured_test_log, gzip_value, rolled_sample_log, test_log,
                tiny_segments, ts_batch,
            },
            segment::Segment,
        },
        assert2::check,
        krabka_units::prelude::{bytes, kibibytes},
        tempfile::tempdir,
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
        let log = rolled_sample_log(dir.path());

        let (offset, ts) = log
            .max_timestamp_offset_and_ts()
            .expect("no limit is set")
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

    #[yare::parameterized(
        before_first = { "before first", 50, Some((Offset(0), 100)) },
        exact_sealed = { "exact sealed", 300, Some((Offset(2), 300)) },
        between_records = { "between records", 350, Some((Offset(3), 400)) },
        active_record = { "active record", 500, Some((Offset(4), 500)) },
        after_last = { "after last", 600, None },
    )]
    fn log_offset_for_timestamp_across_segments(
        name: &str,
        target: i64,
        want: Option<(Offset, i64)>,
    ) {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
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
        check!(
            log.offset_for_timestamp(target) == want,
            "case {name}: ts={target}"
        );
        log.close();
        drop(dir);
    }

    /// The two answers a by-timestamp search gives: the `ListOffsets` lookup's
    /// and the log's own bookkeeping lookup's.
    #[derive(Debug, PartialEq, Eq)]
    struct Answers {
        list_offsets: Option<(Offset, i64)>,
        bookkeeping: Option<(Offset, i64)>,
    }

    /// #1388: a `ListOffsets` seek answers from the first segment whose largest
    /// timestamp reaches the target, even when none of that segment's retained
    /// records does, as Kafka 4.3.1's `UnifiedLog.searchOffsetInLocalLog`
    /// returns `segment.findOffsetByTimestamp(target, startOffset)` from that
    /// segment. Kafka reads every segment the log still holds, so one wholly
    /// below the log start that retention has not yet removed can be that
    /// segment. A segment with no record timestamp has the file's
    /// modification time as its `largestTimestamp`, which reaches any target
    /// below the present. The log's own bookkeeping lookup goes on to the
    /// first retained record.
    #[test]
    fn a_list_offsets_seek_answers_from_the_first_candidate_segment_only() {
        struct Case {
            name: &'static str,
            /// Each segment's record timestamps, one record per batch.
            segments: &'static [&'static [i64]],
            log_start: i64,
            target: i64,
            want: Answers,
        }
        let cases = [
            Case {
                name: "the first candidate holds the match",
                segments: &[&[100, 500, 150, 160, 170], &[600]],
                log_start: 0,
                target: 400,
                want: Answers {
                    list_offsets: Some((Offset(1), 500)),
                    bookkeeping: Some((Offset(1), 500)),
                },
            },
            Case {
                name: "a segment below the target is no candidate",
                segments: &[&[100, 200], &[300, 600], &[700]],
                log_start: 0,
                target: 400,
                want: Answers {
                    list_offsets: Some((Offset(3), 600)),
                    bookkeeping: Some((Offset(3), 600)),
                },
            },
            Case {
                name: "the first candidate's only match is below the log start",
                segments: &[&[100, 500, 150, 160, 170], &[600]],
                log_start: 2,
                target: 400,
                want: Answers {
                    list_offsets: None,
                    bookkeeping: Some((Offset(5), 600)),
                },
            },
            Case {
                name: "the first candidate is wholly below the log start",
                segments: &[&[100, 500], &[150, 160], &[600]],
                log_start: 2,
                target: 400,
                want: Answers {
                    list_offsets: None,
                    bookkeeping: Some((Offset(4), 600)),
                },
            },
            Case {
                name: "a segment with no record timestamp is a candidate by its file time",
                segments: &[&[-1], &[600]],
                log_start: 0,
                target: 400,
                want: Answers {
                    list_offsets: None,
                    bookkeeping: Some((Offset(1), 600)),
                },
            },
            Case {
                name: "the active segment is the first candidate",
                segments: &[&[100, 200], &[150, 600]],
                log_start: 0,
                target: 400,
                want: Answers {
                    list_offsets: Some((Offset(3), 600)),
                    bookkeeping: Some((Offset(3), 600)),
                },
            },
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for Case {
            name,
            segments,
            log_start,
            target,
            want,
        } in cases
        {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            for (index, timestamps) in segments.iter().enumerate() {
                if index > 0 {
                    assert2::assert!(log.roll().unwrap());
                }
                for &ts in *timestamps {
                    log.append(&mut ts_batch(ts)).unwrap();
                }
            }
            log.set_log_start_offset(Offset(log_start)).unwrap();
            actual.push((name, Answers {
                list_offsets: log.offset_for_timestamp_checked(target).unwrap(),
                bookkeeping: log.offset_for_timestamp(target),
            }));
            expected.push((name, want));
            log.close();
        }
        check!(actual == expected);
    }

    #[test]
    fn reopened_log_scans_sealed_segments_with_unknown_max_timestamp() {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        {
            let mut log = Log::open(dir.path(), config.clone()).unwrap();
            for timestamp in [100, 200, 300] {
                log.append(&mut ts_batch(timestamp)).unwrap();
            }
            log.close();
        }

        let log = Log::open(dir.path(), config).unwrap();
        assert2::assert!(log.offset_for_timestamp(150) == Some((Offset(1), 200)));
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == Some((Offset(2), 300)));
        log.close();
    }

    #[test]
    fn configured_io_policy_reaches_reads_and_timestamp_scans() {
        let TempLog { dir: _dir, mut log } = configured_test_log(LogConfig {
            read_buffer_cap: bytes(1),
            timestamp_scan_window: bytes(1),
            ..LogConfig::default()
        });
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
        let TempLog { dir, log } = test_log();
        assert2::assert!(log.offset_for_timestamp(0) == None);
        log.close();
        drop(dir);
    }

    /// An empty log has no maximum: Kafka's `UnifiedLog.fetchOffsetByTimestamp`
    /// answers `MAX_TIMESTAMP` with an empty result rather than the log start.
    #[test]
    fn log_max_timestamp_of_an_empty_log_is_none() {
        let TempLog { dir, log } = test_log();
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_max_timestamp_offset_and_ts_returns_pair() {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        let mut log = Log::open(dir.path(), config).unwrap();
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        // Max timestamp 300 lives at offset 1.
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == Some((Offset(1), 300)));
        log.close();
        drop(dir);
    }

    #[test]
    fn legacy_offsets_before_semantics() {
        let TempLog { dir: _dir, mut log } = test_log();
        assert2::assert!(log.legacy_offsets_before(-1, 10).unwrap() == vec![Offset(0)]);
        assert2::assert!(log.legacy_offsets_before(-2, 10).unwrap() == vec![Offset(0)]);
        assert2::assert!(log.legacy_offsets_before(0, 10).unwrap().is_empty());

        let mut b = ts_batch(100);
        log.append(&mut b).unwrap();

        // -1 (latest): log end offset then segment base
        let latest = log.legacy_offsets_before(-1, 10).unwrap();
        assert2::assert!(latest == vec![Offset(1), Offset(0)]);

        // -2 (earliest): segment base only
        let earliest = log.legacy_offsets_before(-2, 10).unwrap();
        assert2::assert!(earliest == vec![Offset(0)]);

        // 0 timestamp: earlier than file mtime, empty
        assert2::assert!(log.legacy_offsets_before(0, 10).unwrap().is_empty());

        // Future timestamp: both offsets returned, capped by max_num_offsets
        let all = log.legacy_offsets_before(i64::MAX, 10).unwrap();
        assert2::assert!(all == vec![Offset(1), Offset(0)]);
        let capped = log.legacy_offsets_before(i64::MAX, 1).unwrap();
        assert2::assert!(capped == vec![Offset(1)]);
    }

    /// A gzip batch of one record stamped `ts` with a `value_len`-byte value.
    fn gzip_batch(ts: i64, value_len: usize) -> krabka_protocol::records::RecordBatch {
        let mut batch = ts_batch(ts);
        gzip_value(&mut batch, value_len);
        batch
    }

    /// What a checked lookup answers: the record it found, or `Err` when it
    /// refused one as too large.
    fn answer(
        result: Result<Option<(Offset, i64)>, LogError>,
    ) -> Result<Option<(Offset, i64)>, ()> {
        match result {
            Ok(found) => Ok(found),
            Err(LogError::RecordTooLarge { limit: 100, .. }) => Err(()),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    /// Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp` holds a compressed
    /// batch it decompresses to `max.decompressed.message.bytes`. Three
    /// one-record segments, the outer two oversized: timestamps 100 (oversized),
    /// 300 (small), 200 (oversized).
    #[test]
    fn the_checked_lookups_hold_a_decompressed_batch_to_the_record_limit() {
        let TempLog { dir: _dir, mut log } = configured_test_log(LogConfig {
            max_decompressed_record: Some(bytes(100)),
            ..tiny_segments()
        });
        for (ts, value_len) in [(100, 1_000), (300, 10), (200, 1_000)] {
            log.append(&mut gzip_batch(ts, value_len)).unwrap();
        }
        check!(log.segments.len() == 2, "the third batch is the active one");

        for (name, target, expected) in [
            // The first segment holding a timestamp that reaches the target
            // is the one Kafka reads, and its match is oversized.
            ("the match is oversized", 50, Err(())),
            // Segment 0 tops out at 100, so Kafka skips it undecompressed.
            (
                "an older oversized segment is skipped",
                150,
                Ok(Some((Offset(1), 300))),
            ),
            (
                "a match in the small batch",
                300,
                Ok(Some((Offset(1), 300))),
            ),
            ("nothing that late", 301, Ok(None)),
        ] {
            check!(
                answer(log.offset_for_timestamp_checked(target)) == expected,
                "{name}: {target}"
            );
        }
        // The maximum is in the small segment, and Kafka reads only that one,
        // whatever the other segments hold.
        check!(answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(1), 300))));

        // The log's own bookkeeping lookups take no limit.
        check!(log.offset_for_timestamp(50) == Some((Offset(0), 100)));

        // Once the oversized batch holds the maximum, the lookup refuses it.
        log.append(&mut gzip_batch(400, 1_000)).unwrap();
        check!(answer(log.max_timestamp_offset_and_ts()) == Err(()));

        // Raising the limit, as the operator does, answers it again; so does
        // no limit at all.
        for limit in [Some(bytes(1_008)), None] {
            let mut config = log.config_snapshot();
            config.max_decompressed_record = limit;
            log.set_config(config);
            check!(
                answer(log.offset_for_timestamp_checked(50)) == Ok(Some((Offset(0), 100))),
                "{limit:?}"
            );
            check!(
                answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(3), 400))),
                "{limit:?}"
            );
        }
        log.close();
    }

    /// A limit changes nothing for a partition that keeps its records
    /// uncompressed: Kafka reads them in place.
    #[test]
    fn the_checked_lookups_never_hold_an_uncompressed_batch_to_the_limit() {
        let TempLog { dir: _dir, mut log } = configured_test_log(LogConfig {
            max_decompressed_record: Some(bytes(100)),
            ..LogConfig::default()
        });
        let mut batch = gzip_batch(100, 1_000);
        batch.attributes = batch
            .attributes
            .with_compression(krabka_compression::CompressionType::None);
        log.append(&mut batch).unwrap();

        check!(answer(log.offset_for_timestamp_checked(100)) == Ok(Some((Offset(0), 100))));
        check!(answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(0), 100))));
        log.close();
    }
}
