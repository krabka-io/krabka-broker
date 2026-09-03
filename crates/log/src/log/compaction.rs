//! One compaction pass over the sealed segment list, and the broker-side
//! inputs that pass depends on.
//!
//! The pass rewrites every sealed segment into a single new one and never
//! touches the active segment, so the log-end offset a producer sees does
//! not move.

use krabka_ids::{Offset, ProducerId};
use krabka_units::prelude::{
    ByteSize, ByteSizeExt as _, Ratio, RatioExt as _, Time, TimeExt as _, fraction,
};
use tracing::instrument;

use super::Log;
use crate::{error::LogError, retention, segment::Segment, txn_index::TxnIndex};

/// Inputs to one [`Log::compact`] pass that depend on broker-side state.
///
/// These inputs are the wall clock that computes the KIP-534 delete horizons,
/// and the set of producers that count as active.
///
/// `active_producers` maps `producer_id` to the `base_offset` of that
/// producer's last batch. When compaction removes every record of that batch,
/// the cleaner writes a bare batch header (`RETAIN_EMPTY`) again, so the
/// producer's sequence and epoch state and the log-end offset survive.
#[derive(Debug, Clone)]
pub struct CompactionContext {
    /// Wall clock for this pass. It drives delete-horizon stamps and expiry.
    pub now: std::time::SystemTime,
    /// `producer_id` → last batch `base_offset` for currently-active
    /// producers.
    pub active_producers: std::collections::HashMap<ProducerId, Offset>,
}

/// What a partition looks like to the broker's cleaner before it decides
/// whether a compaction pass is worth running.
///
/// Kafka's `LogCleanerManager.cleanableOffsets` splits a log into three parts
/// and this mirrors it: the clean prefix below the first dirty offset, the
/// cleanable range a pass would rewrite, and an uncleanable tail that is
/// neither. The ratio Kafka tests is the cleanable range over the clean
/// prefix plus that range, so the uncleanable tail is left out of both halves.
///
/// krabka keeps no checkpoint file, because [`Log::compact`] rewrites the
/// sealed segments it is given into one: the first sealed segment is therefore
/// the previous pass's output and counts as clean, and every sealed segment
/// after it arrived since. A log that has never been compacted reports its
/// first segment as clean, which is the conservative direction: the ratio it
/// reports is never larger than the true one.
///
/// The uncleanable tail is Kafka's, too. The active segment is always in it,
/// and so is every sealed segment from the first one whose largest timestamp
/// is younger than `min.compaction.lag.ms` onwards. That is what the min lag
/// means in Kafka: it withholds the young tail from the pass, it does not
/// postpone the pass. A topic taking writes more often than its min lag still
/// has its older segments cleaned on schedule.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CompactionCandidacy {
    /// Bytes the previous compaction pass already deduplicated.
    clean_bytes: ByteSize,
    /// Bytes in the cleanable range, which one pass would deduplicate.
    cleanable_bytes: ByteSize,
    /// Timestamp of the oldest record in the dirty region, which is what
    /// `max.compaction.lag.ms` bounds. `None` when nothing is dirty.
    ///
    /// Kafka reads this over the whole dirty region rather than over the
    /// cleanable range, so a segment held back by the min lag does not hide
    /// an older one from the max lag.
    oldest_dirty_timestamp_ms: Option<i64>,
}

impl CompactionCandidacy {
    /// Kafka's `LogToClean.cleanableRatio`: cleanable bytes over the clean
    /// prefix plus them. Zero for an empty log, which no cleaner should spend
    /// a pass on.
    fn cleanable_ratio(self) -> Ratio {
        let total = self.clean_bytes + self.cleanable_bytes;
        if total.bytes_f64() <= 0.0 {
            return Ratio::ZERO;
        }
        fraction(self.cleanable_bytes.bytes_f64() / total.bytes_f64())
    }
}

/// The three `LogConfig` values the decision reads, so it is a pure function
/// of the log's shape and the topic's configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CompactionSchedule {
    min_lag: Time,
    max_lag: Option<Time>,
    min_ratio: Ratio,
}

/// How many of the dirty sealed segments a pass may clean, given each one's
/// largest timestamp in offset order.
///
/// This is Kafka's `findFirstUncleanableSegment`: with a positive
/// `min.compaction.lag.ms`, the first segment whose largest timestamp is
/// within the lag ends the cleanable range, and every segment from there on is
/// uncleanable. A zero lag withholds nothing. A segment carrying no batch
/// reports [`i64::MIN`], which no lag can hold back.
fn cleanable_prefix(largest_timestamps_ms: &[i64], now_ms: i64, min_lag_ms: i64) -> usize {
    if min_lag_ms <= 0 {
        return largest_timestamps_ms.len();
    }
    let youngest_cleanable_ms = now_ms.saturating_sub(min_lag_ms);
    largest_timestamps_ms
        .iter()
        .position(|timestamp| *timestamp > youngest_cleanable_ms)
        .unwrap_or(largest_timestamps_ms.len())
}

/// Kafka's cleanable test, over one partition's [`CompactionCandidacy`].
///
/// This is `grabFilthiestCompactedLog`'s filter: a log with nothing cleanable
/// is never due, one whose dirty region has outlived `max.compaction.lag.ms`
/// is due regardless of its ratio, and otherwise it is due once the cleanable
/// range is a larger share of the log than `min.cleanable.dirty.ratio`.
fn compaction_is_due(
    candidacy: CompactionCandidacy,
    schedule: CompactionSchedule,
    now_ms: i64,
) -> bool {
    if candidacy.cleanable_bytes.bytes_f64() <= 0.0 {
        return false;
    }
    if let (Some(max_lag), Some(oldest)) = (schedule.max_lag, candidacy.oldest_dirty_timestamp_ms)
        && now_ms.saturating_sub(oldest) > max_lag.millis_i64_trunc()
    {
        return true;
    }
    candidacy.cleanable_ratio().as_f64() > schedule.min_ratio.as_f64()
}

impl Log {
    /// Whether a compaction pass over this partition is due, as Kafka's
    /// `LogCleanerManager` decides it: the cleanable range has to be a large
    /// enough share of the log (`min.cleanable.dirty.ratio`), unless the dirty
    /// region has been dirty so long that `max.compaction.lag.ms` forces a
    /// pass regardless. `min.compaction.lag.ms` withholds the young tail from
    /// that range rather than holding the pass back.
    ///
    /// The cleanup policy itself is not read here. That is the caller's test,
    /// because a partition the policy excludes is one the broker's cleaner
    /// skips before it opens the log at all.
    ///
    /// # Panics
    /// Panics if the configuration lock is poisoned.
    #[must_use]
    pub fn compaction_due(&self, now: std::time::SystemTime) -> bool {
        let (min_lag, max_lag, min_ratio) = {
            let cfg = self.config.read().unwrap();
            (
                cfg.min_compaction_lag,
                cfg.max_compaction_lag,
                cfg.min_cleanable_dirty_ratio,
            )
        };
        let now_ms = retention::now_ms(now);
        compaction_is_due(
            self.compaction_candidacy(min_lag.millis_i64_trunc(), now_ms),
            CompactionSchedule {
                min_lag,
                max_lag,
                min_ratio,
            },
            now_ms,
        )
    }

    /// The clean/cleanable split and dirty-region age [`Self::compaction_due`]
    /// reads. See [`CompactionCandidacy`] for what counts as which and why.
    fn compaction_candidacy(&self, min_lag_ms: i64, now_ms: i64) -> CompactionCandidacy {
        let mut sealed = self.segments.iter();
        let clean_bytes = sealed.next().map_or(ByteSize::ZERO, Segment::size);
        let dirty: Vec<&Segment> = sealed.collect();
        let cleanable = self
            .cleanable_sealed_count(min_lag_ms, now_ms)
            .saturating_sub(1);
        let cleanable_bytes = dirty[..cleanable]
            .iter()
            .fold(ByteSize::ZERO, |total, segment| total + segment.size());
        let oldest_dirty_timestamp_ms = dirty
            .iter()
            .filter_map(|segment| {
                segment
                    .offset_for_timestamp(i64::MIN)
                    .map(|(_, timestamp)| timestamp)
            })
            .min();
        CompactionCandidacy {
            clean_bytes,
            cleanable_bytes,
            oldest_dirty_timestamp_ms,
        }
    }

    /// How many sealed segments one pass may consume, counting the clean
    /// first one: the clean prefix plus the dirty segments Kafka's
    /// `min.compaction.lag.ms` does not withhold.
    ///
    /// Zero for a log with no sealed segment at all.
    fn cleanable_sealed_count(&self, min_lag_ms: i64, now_ms: i64) -> usize {
        if self.segments.is_empty() {
            return 0;
        }
        let largest_timestamps: Vec<i64> = self.segments[1..]
            .iter()
            .map(Segment::max_timestamp)
            .collect();
        1 + cleanable_prefix(&largest_timestamps, now_ms, min_lag_ms)
    }

    /// Run one compaction pass over the cleanable sealed segments.
    ///
    /// The pass never touches the active segment, and it stops short of the
    /// sealed segments `min.compaction.lag.ms` withholds -- the same range
    /// [`Self::compaction_due`] measures, so the decision and the pass agree
    /// on what one pass would do. The output is a single new sealed segment at
    /// the lowest input base offset, and it replaces the sealed segments it
    /// consumed; any withheld sealed segments stay where they are and become
    /// cleanable once their records outlive the lag.
    ///
    /// `ctx` carries the wall clock, which drives the KIP-534 delete-horizon
    /// computation, and the set of currently-active producers. The cleaner
    /// keeps the last batch of each active producer with `RETAIN_EMPTY`, even
    /// when compaction removes all of its records.
    #[instrument(
        level = "info",
        skip_all,
        fields(sealed_segments = self.segments.len()),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn compact(&mut self, ctx: &CompactionContext) -> Result<(), LogError> {
        if self.segments.is_empty() {
            return Ok(());
        }

        let (index_interval, delete_retention, min_lag) = {
            let cfg_guard = self.config.read().unwrap();
            if !cfg_guard.cleanup_policy.contains_compact() {
                return Ok(());
            }
            (
                cfg_guard.index_interval,
                cfg_guard.delete_retention,
                cfg_guard.min_compaction_lag,
            )
        };

        let now_ms = retention::now_ms(ctx.now);
        let consumed = self.cleanable_sealed_count(min_lag.millis_i64_trunc(), now_ms);
        // Compaction rewrites sealed segments, so any record the last
        // activation walk read may not survive it.
        let compacted_from = self.log_start_offset();
        self.invalidate_delivery_schedule(compacted_from);
        let consumed_bases: Vec<Offset> = self.segments[..consumed]
            .iter()
            .map(Segment::base_offset)
            .collect();

        // Borrow sealed segments to run map + rewrite (which open
        // additional file handles internally for reading). Then drop the
        // borrows and clear self.segments so the original segments'
        // file handles close before atomic_swap deletes/renames
        // (Windows requires no open handle on a file before remove/rename).
        let rewrite = {
            let sealed_refs: Vec<&Segment> = self.segments[..consumed].iter().collect();
            let offset_map = crate::compact::build_offset_map(&sealed_refs)?;
            let txn_meta =
                crate::compact::CleanedTransactionMetadata::build(&sealed_refs, &offset_map)?;
            crate::compact::rewrite_segments(
                &self.dir,
                &sealed_refs,
                &offset_map,
                &txn_meta,
                crate::compact::RewriteRetention {
                    now_ms,
                    delete_retention,
                },
                &ctx.active_producers,
                index_interval,
            )?
        };

        self.segments.drain(..consumed);
        crate::compact::atomic_swap(&self.dir, &consumed_bases, &rewrite)?;

        // Validation scans the new log from byte zero, rebuilds both sparse
        // indexes, and derives exact offset and timestamp frontiers before the
        // segment is sealed.
        let mut new_seg = Segment::open_active_with_index_interval(
            &self.dir,
            rewrite.new_base_offset,
            true,
            index_interval,
        )?;
        new_seg.set_io(self.io.clone());
        new_seg.seal();
        let txn_index = TxnIndex::open(new_seg.txn_index_path())?;
        for base in &consumed_bases {
            self.sealed_txn_indexes.remove(base);
        }
        self.sealed_txn_indexes
            .insert(rewrite.new_base_offset, txn_index);
        self.segments.insert(0, new_seg);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use bytes::BytesMut;
    use krabka_units::prelude::{bytes, mebibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{compaction_ctx, keyed_batch},
        name,
    };

    /// Kafka's cleanable test, over the shapes a partition can be in. The
    /// clock is fixed at `10_000` ms so a "dirty since" timestamp reads as an
    /// age directly.
    #[test]
    fn the_cleanable_test_reads_the_ratio_and_the_max_lag() {
        const NOW_MS: i64 = 10_000;
        let candidacy = |clean: u32, cleanable: u32, oldest: i64| CompactionCandidacy {
            clean_bytes: bytes(clean),
            cleanable_bytes: bytes(cleanable),
            oldest_dirty_timestamp_ms: Some(oldest),
        };
        let schedule = |min_lag_ms: i64, max_lag_ms: Option<i64>, ratio: f64| CompactionSchedule {
            min_lag: Time::from_millis(min_lag_ms),
            max_lag: max_lag_ms.map(Time::from_millis),
            min_ratio: fraction(ratio),
        };
        let cases = [
            (
                "nothing cleanable",
                candidacy(100, 0, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "cleanable two thirds of the log",
                candidacy(100, 200, 0),
                schedule(0, None, 0.5),
                true,
            ),
            (
                // Kafka's filter is `cleanableRatio > minCleanableRatio`, so
                // a log exactly at the threshold is not yet worth a pass.
                "cleanable exactly half the log",
                candidacy(100, 100, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "cleanable a quarter of the log is below the ratio",
                candidacy(300, 100, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "below the ratio but past the max lag",
                candidacy(300, 100, 0),
                schedule(0, Some(5_000), 0.5),
                true,
            ),
            (
                "below the ratio and inside the max lag",
                candidacy(300, 100, 9_000),
                schedule(0, Some(5_000), 0.5),
                false,
            ),
            (
                // The min lag withholds segments from the cleanable range; it
                // never holds back a pass over what is left, so a log with a
                // cleanable range above the ratio is due whatever the lag is.
                "above the ratio under a long min lag",
                candidacy(100, 200, 9_900),
                schedule(60_000, None, 0.5),
                true,
            ),
            (
                // Everything dirty is inside the min lag, so the cleanable
                // range is empty and even the max lag has nothing to clean.
                "the whole dirty region is withheld by the min lag",
                candidacy(100, 0, 9_900),
                schedule(60_000, Some(1), 0.5),
                false,
            ),
        ];
        for (label, candidacy, schedule, expected) in cases {
            assert2::check!(
                compaction_is_due(candidacy, schedule, NOW_MS) == expected,
                "{label}"
            );
        }
    }

    /// The min lag's own rule: it ends the cleanable range at the first
    /// segment whose largest timestamp is younger than the lag, and withholds
    /// nothing when it is zero.
    #[test]
    fn the_min_lag_ends_the_cleanable_range_at_the_first_young_segment() {
        const NOW_MS: i64 = 10_000;
        // Four dirty segments, the last two written inside a 1s lag.
        let timestamps = [1_000, 5_000, 9_500, 9_800];
        let cases = [
            ("no lag withholds nothing", 0, 4),
            ("a lag shorter than the whole log", 1_000, 2),
            ("a lag that covers every segment", 60_000, 0),
            // The boundary is Kafka's `largestTimestamp > now - minLag`, so a
            // segment exactly at the edge is still cleanable.
            ("a segment exactly at the edge", 5_000, 2),
            ("an empty log has no cleanable segment", 1_000, 0),
        ];
        for (label, min_lag_ms, expected) in cases {
            let stamps: &[i64] = if label.starts_with("an empty log") {
                &[]
            } else {
                &timestamps
            };
            assert2::check!(
                cleanable_prefix(stamps, NOW_MS, min_lag_ms) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_pass_leaves_a_log_the_cleaner_no_longer_owes_one() {
        // Twelve distinct keys survive the pass, so the segment it produces is
        // the bulk of the log and the ratio falls under Kafka's 0.5 default.
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..12 {
            let key = format!("k{i}");
            let mut batch = keyed_batch(i, &[(0, key.as_bytes(), b"v")]);
            log.append(&mut batch).unwrap();
        }
        assert2::check!(log.compaction_due(std::time::SystemTime::now()));

        log.compact(&compaction_ctx()).unwrap();
        assert2::check!(!log.compaction_due(std::time::SystemTime::now()));
    }

    /// One sealed segment per batch, each stamped `1_000 * i` milliseconds
    /// after the epoch, all under one key. The last append lands in the active
    /// segment.
    fn log_stamped_a_second_apart(dir: &std::path::Path, cfg: LogConfig, batches: i64) -> Log {
        let mut log = Log::open(dir, cfg).unwrap();
        for i in 0..batches {
            let value = format!("v{i}");
            let mut batch = keyed_batch(i, &[(0, b"key", value.as_bytes())]);
            batch.base_timestamp = i * 1_000;
            batch.max_timestamp = i * 1_000;
            log.append(&mut batch).unwrap();
        }
        log
    }

    /// A `SystemTime` `millis` after the epoch, which is the clock the
    /// stamped-segment tests reason in.
    fn at_epoch_millis(millis: u64) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(millis)
    }

    /// Kafka's `min.compaction.lag.ms` withholds the young tail from the
    /// cleanable range; it does not postpone the pass. A topic that takes a
    /// write more often than its lag would otherwise never be compacted at
    /// all, because its newest record is always too young.
    #[test]
    fn a_topic_written_to_faster_than_its_min_lag_is_still_due() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1),
            // Longer than the second that separates two appends, and short
            // enough to leave the older segments cleanable.
            min_compaction_lag: Time::from_millis(1_500),
            ..Default::default()
        };
        let log = log_stamped_a_second_apart(dir.path(), cfg, 6);

        assert2::check!(log.compaction_due(at_epoch_millis(6_000)));
    }

    /// The withheld tail is withheld from the pass as well, so a record inside
    /// the min lag is not deduplicated early: the pass rewrites the segments
    /// below the lag and leaves the rest where they are.
    #[test]
    fn a_pass_leaves_the_segments_the_min_lag_withholds() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1),
            min_compaction_lag: Time::from_millis(2_500),
            ..Default::default()
        };
        // Sealed segments carry timestamps 0..=4_000 and the active one 5_000.
        // At 6_000 the lag withholds everything stamped after 3_500, so the
        // pass consumes the four segments stamped 0..=3_000.
        let mut log = log_stamped_a_second_apart(dir.path(), cfg, 6);
        log.compact(&CompactionContext {
            now: at_epoch_millis(6_000),
            active_producers: std::collections::HashMap::new(),
        })
        .unwrap();

        assert2::check!(log.segments.len() == 2);
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let values: Vec<Vec<u8>> = out
            .batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .map(|record| record.value.as_deref().unwrap().to_vec())
            .collect();
        assert2::check!(values == vec![b"v3".to_vec(), b"v4".to_vec(), b"v5".to_vec()]);
    }

    #[test]
    fn a_ratio_of_one_holds_a_partly_dirty_log_back_until_the_max_lag() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1),
            min_cleanable_dirty_ratio: fraction(1.0),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..3 {
            let value = format!("v{i}");
            let mut batch = keyed_batch(i, &[(0, b"key", value.as_bytes())]);
            log.append(&mut batch).unwrap();
        }
        assert2::check!(!log.compaction_due(std::time::SystemTime::now()));

        // The batches carry timestamp 0, so any max lag has long elapsed and
        // the pass is owed however clean the ratio says the log is.
        log.set_config(LogConfig {
            max_compaction_lag: Some(Time::from_millis(1)),
            ..cfg
        });
        assert2::check!(log.compaction_due(std::time::SystemTime::now()));
    }

    fn assert_corrupt_suffix_preserves_originals(suffix: &[u8]) {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..3 {
            let value = format!("v{i}");
            let mut batch = keyed_batch(0, &[(0, b"key", value.as_bytes())]);
            log.append(&mut batch).unwrap();
        }
        assert2::assert!(!log.segments.is_empty());

        let corrupt_path = name::log_path(dir.path(), log.segments[0].base_offset().0);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&corrupt_path)
            .unwrap()
            .write_all(suffix)
            .unwrap();

        let originals: Vec<_> = log
            .segments
            .iter()
            .map(|segment| {
                let path = name::log_path(dir.path(), segment.base_offset().0);
                (path.clone(), std::fs::read(path).unwrap())
            })
            .collect();
        let bases_before: Vec<_> = log.segments.iter().map(Segment::base_offset).collect();

        let error = log.compact(&compaction_ctx()).unwrap_err();
        assert2::assert!(matches!(error, LogError::Records(_) | LogError::Corrupt(_)));
        assert2::assert!(
            log.segments
                .iter()
                .map(Segment::base_offset)
                .collect::<Vec<_>>()
                == bases_before
        );
        for (path, bytes) in originals {
            assert2::assert!(std::fs::read(path).unwrap() == bytes);
        }
    }

    #[test]
    fn compact_rejects_truncated_suffix_without_replacing_originals() {
        assert_corrupt_suffix_preserves_originals(&[0; 16]);
    }

    #[test]
    fn compact_rejects_crc_corrupt_suffix_without_replacing_originals() {
        let batch = keyed_batch(0, &[(0, b"corrupt", b"suffix")]);
        let mut encoded = BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut encoded).unwrap();
        // The stored CRC occupies bytes 17..21. Changing it preserves framing
        // while making the complete suffix fail integrity validation.
        encoded[17] ^= 1;
        assert_corrupt_suffix_preserves_originals(&encoded);
    }

    #[test]
    fn compact_no_op_when_only_one_segment() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        let mut b = keyed_batch(0, &[(0, b"k1", b"v1")]);
        log.append(&mut b).unwrap();
        // Only the active segment exists; sealed list is empty.
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == 1);
    }

    #[test]
    fn compact_dedupes_sealed_segments_keeps_active_intact() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256), // force rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Write 3 sealed segments, each with one record under "k1".
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
            // Roll the active segment by forcing a tick or a large pad batch.
            // Easiest: call set_segment_size or rely on the small segment_size.
        }
        // Add one more append to ensure the last write is in a fresh active
        // segment (not part of what compaction touches).
        let mut b = keyed_batch(0, &[(0, b"active-key", b"active-value")]);
        log.append(&mut b).unwrap();

        let active_leo_before = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == active_leo_before);

        // After compaction: read everything, assert only the newest k1 plus
        // the active "active-key" survive.
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let all_records: Vec<_> = out.batches.iter().flat_map(|b| b.records.iter()).collect();
        let keys: Vec<&[u8]> = all_records
            .iter()
            .map(|r| r.key.as_ref().unwrap().as_ref())
            .collect();
        assert2::assert!(keys.contains(&b"k1".as_ref()));
        assert2::assert!(keys.contains(&b"active-key".as_ref()));
    }

    /// Compaction must run and must not do nothing. Three sealed segments
    /// each carry a record under the SAME key "k1". After `compact`, exactly
    /// ONE k1 record must remain, the newest one, "v2". The sealed segment
    /// list must collapse to a single rewritten segment. A compaction that
    /// returned `Ok(())` at once would leave all three k1 records and three
    /// sealed segments.
    #[test]
    fn compact_actually_dedupes_reducing_record_count() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1), // one batch per segment: every append exceeds this and rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Three sealed segments, each one record under "k1" (v0, v1, v2).
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        // A final append lands in a fresh active segment (untouched by compact).
        let mut tail = keyed_batch(0, &[(0, b"tail", b"t")]);
        log.append(&mut tail).unwrap();

        // Sanity: before compaction there are >= 2 sealed segments holding the
        // three k1 versions.
        assert2::assert!(log.segments.len() >= 2);

        log.compact(&compaction_ctx()).unwrap();

        // Sealed segments collapse to exactly one rewritten segment.
        assert2::assert!(log.segments.len() == 1);

        // Exactly one surviving k1 record, and it is the newest value "v2".
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let k1_values: Vec<&[u8]> = out
            .batches
            .iter()
            .flat_map(|b| b.records.iter())
            .filter(|r| r.key.as_deref() == Some(b"k1".as_ref()))
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert2::assert!(k1_values == vec![b"v2".as_ref()]);
    }

    #[test]
    fn compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        let mut b = keyed_batch(0, &[(0, b"active", b"x")]);
        log.append(&mut b).unwrap();
        log.compact(&compaction_ctx()).unwrap();
        let leo1 = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        let leo2 = log.log_end_offset();
        assert2::assert!(leo1 == leo2);
    }
}
