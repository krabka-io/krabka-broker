//! Periodic maintenance: the time- and size-based retention sweep over
//! sealed segments.
//!
//! The segment roll itself is not here. Kafka rolls inside the append, in
//! [`Log::should_roll_for_incoming`], against the incoming batch's own size
//! and timestamp; a wall-clock roll on this sweep would roll an idle
//! partition's active segment, which Kafka never does, and the next sweep
//! would then delete the tail records that segment held.
//!
//! Retention never deletes the active segment, never leaves the log with
//! no segment at all, and never evicts a segment that still holds a record
//! whose delivery time has not arrived.

use std::{collections::HashSet, time::SystemTime};

use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::Log;
use crate::{error::LogError, retention, segment::Segment};

impl Log {
    /// Periodic maintenance: apply time- and size-based retention to sealed
    /// segments when `cleanup.policy` holds `delete`, and delete sealed
    /// segments below the log start offset under every policy. The active
    /// segment is never deleted, and if every segment would otherwise be
    /// evicted we retain at least one.
    ///
    /// `high_watermark` bounds every reason a segment can be evicted, time,
    /// size and log-start-offset breach alike: Kafka's `UnifiedLog`
    /// `deletableSegments` only ever considers a segment whose upper bound
    /// offset (the next segment's base offset, or the log end offset for the
    /// last one) is at or below the high watermark. A replica's log start
    /// offset can be pushed past its own high watermark by a lagging
    /// follower relationship, and without this bound retention would delete
    /// records a fetch at the high watermark still needs to see.
    #[instrument(
        level = "debug",
        skip_all,
        fields(evicted = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn tick(&mut self, now: SystemTime, high_watermark: Offset) -> Result<(), LogError> {
        // Tiered topics' segment lifecycle is owned by the RemoteLogManager.
        if self.config.read().unwrap().remote_storage_enable {
            return Ok(());
        }
        // Refresh the visibility watermark before retention reads it, so a
        // partition that no consumer has fetched from still gets an accurate
        // floor. On a topic that delivers immediately this returns the log
        // end and does no I/O.
        let visible_floor = self
            .advance_delivery_watermark(retention::now_ms(now))
            .watermark;

        let sealed_refs: Vec<&Segment> = self.segments.iter().collect();
        let active_size = self.active.as_ref().map_or(ByteSize::ZERO, Segment::size);

        let cfg_guard = self.config.read().unwrap();
        // Kafka's `UnifiedLog.deleteOldSegments`: time and size retention run
        // only when `cleanup.policy` holds `delete`. A compact-only log keeps
        // every key however old it is, and loses a segment only when the whole
        // segment is below the log start offset.
        let retention_applies = cfg_guard.cleanup_policy.contains_delete();
        let time_evict = if retention_applies {
            retention::time_based_evict(&sealed_refs, &cfg_guard, now)
        } else {
            Vec::new()
        };
        let total_size: ByteSize = sealed_refs
            .iter()
            .fold(active_size, |total, segment| total + segment.size());
        let size_debt = cfg_guard
            .retention_size
            .filter(|_| retention_applies)
            .map_or(0, |budget| {
                if total_size > budget {
                    (total_size - budget).bytes_u64()
                } else {
                    0
                }
            });
        drop(cfg_guard);

        // Kafka's `deleteLogStartOffsetBreachedSegments`: a sealed segment
        // whose next segment starts at or below the log start offset holds no
        // record at or above it. Every policy deletes it.
        let log_start = self.log_start_offset();
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        let upper_bounds: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .take(self.segments.len())
            .collect();
        let start_breached: Vec<bool> = upper_bounds
            .iter()
            .map(|next_base| *next_base <= log_start)
            .collect();
        let time_expired: Vec<bool> = start_breached
            .iter()
            .enumerate()
            .map(|(index, breached)| *breached || index < time_evict.len())
            .collect();
        // Kafka's `deletableSegments`: every eviction reason, time, size and
        // log-start-offset breach alike, is additionally gated by
        // `highWatermark >= upperBoundOffset`. A segment above the watermark
        // can still be truncated away by a leader election, so retention
        // never removes it out from under a fetch sitting at the watermark.
        //
        // On an immediate topic the floor is the log end, so every entry is
        // false. On a scheduled topic the first waiting segment stops the
        // prefix; later segments are never skipped around it.
        let scheduled: Vec<bool> = self
            .segments
            .iter()
            .zip(&upper_bounds)
            .map(|(segment, upper_bound)| {
                segment.last_offset() >= visible_floor || *upper_bound > high_watermark
            })
            .collect();
        let sizes: Vec<u64> = self
            .segments
            .iter()
            .map(|segment| segment.size().bytes_u64())
            .collect();
        let selection = krabka_verified::local_retention_prefix(
            &time_expired,
            &scheduled,
            &sizes,
            size_debt,
            self.active.is_some(),
        );
        let to_evict: Vec<Offset> = self
            .segments
            .iter()
            .take(selection.len)
            .map(Segment::base_offset)
            .collect();

        // Unlink first, and forget only what actually left the disk. A failed
        // unlink otherwise drops the segment from `self.segments` -- and from
        // `Log::size`, and from the partition's disk gauge -- while its bytes
        // stay on the filesystem with nothing left to retry them. Eviction is
        // a prefix, so stopping at the first failure keeps it one.
        let mut deleted: HashSet<Offset> = HashSet::with_capacity(to_evict.len());
        let mut failure: Option<LogError> = None;
        for base in to_evict {
            match retention::delete_segment_files(&*self.io, &self.dir, base) {
                Ok(()) => {
                    deleted.insert(base);
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        tracing::Span::current().record("evicted", deleted.len());
        self.segments
            .retain(|s| !deleted.contains(&s.base_offset()));
        self.sealed_txn_indexes
            .retain(|base, _| !deleted.contains(base));
        self.stamp_indexes.retain(|base, _| !deleted.contains(base));
        if let Some(error) = failure {
            // The floor still follows whatever did come off disk, so the log
            // start is refreshed before the failure is reported.
            self.set_log_start_offset(self.first_local_offset())?;
            return Err(error);
        }
        // Ordinary retention deletes the records outright: nothing holds them
        // any more, so the global floor follows the files off disk (Kafka's
        // `deleteSegments` → `maybeIncrementLogStartOffset`). This is the
        // opposite of the tiered eviction in `delete_local_segments_through`,
        // which leaves the floor behind because the remote tier still answers
        // for those offsets. The early return above keeps a tiered topic out
        // of this path entirely.
        self.set_log_start_offset(self.first_local_offset())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, kibibytes, millis, secs};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{rolled_log, sample_batch},
    };

    /// A high watermark that never gates retention, for tests that are not
    /// about the watermark bound itself.
    const UNBOUNDED_HW: Offset = Offset(i64::MAX);

    /// Retention never evicts the last segment, however far past the budget
    /// the log is.
    ///
    /// A log with no segments has nowhere to append and no offset to report;
    /// the guard is what keeps an aggressive retention setting from leaving
    /// one. The cap is on the count, so a budget of nothing still leaves one
    /// behind.
    #[test]
    fn retention_never_evicts_the_last_segment() {
        let dir = tempdir().unwrap();
        // Roll often, and keep nothing: everything is evictable.
        let config = LogConfig {
            segment_size: kibibytes(1),
            retention_size: Some(ByteSize::ZERO),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..40 {
            let mut batch = sample_batch(4);
            log.append(&mut batch).expect("append");
        }
        check!(!log.segments.is_empty(), "the appends should have rolled");

        log.tick(SystemTime::now(), UNBOUNDED_HW).expect("tick");
        let remaining = log.segments.len() + usize::from(log.active.is_some());
        check!(
            remaining >= 1,
            "a log must keep a segment to append to, got {remaining}"
        );
        // And it is still usable afterwards.
        let mut batch = sample_batch(1);
        check!(
            log.append(&mut batch).is_ok(),
            "the log still accepts appends"
        );
    }

    /// Kafka's `UnifiedLog.deleteOldSegments`: `cleanup.policy` decides which
    /// passes run. Each log has three one-record segments. Offset 0 is older
    /// than `retention.ms`, and offsets 1 and 2 are fresh. The base offsets
    /// that survive the tick are compared as a whole.
    #[test]
    fn tick_applies_time_and_size_retention_only_when_the_policy_deletes() {
        use crate::config::CleanupPolicy;

        /// The policy, the retention byte budget, the log start offset set
        /// before the tick, and the base offsets that survive it.
        type Case = (CleanupPolicy, Option<ByteSize>, Offset, Vec<Offset>);

        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let all = vec![Offset(0), Offset(1), Offset(2)];
        let cases: [Case; 8] = [
            (
                CleanupPolicy::Delete,
                None,
                Offset(0),
                vec![Offset(1), Offset(2)],
            ),
            (CleanupPolicy::Compact, None, Offset(0), all.clone()),
            (
                CleanupPolicy::CompactAndDelete,
                None,
                Offset(0),
                vec![Offset(1), Offset(2)],
            ),
            // Size retention with a budget of nothing deletes every sealed
            // segment, but only under a policy that deletes.
            (
                CleanupPolicy::Delete,
                Some(ByteSize::ZERO),
                Offset(0),
                vec![Offset(2)],
            ),
            (
                CleanupPolicy::Compact,
                Some(ByteSize::ZERO),
                Offset(0),
                all.clone(),
            ),
            (
                CleanupPolicy::CompactAndDelete,
                Some(ByteSize::ZERO),
                Offset(0),
                vec![Offset(2)],
            ),
            // A segment wholly below the log start offset goes under every
            // policy, and a segment the start offset only reaches stays.
            (
                CleanupPolicy::Compact,
                None,
                Offset(1),
                vec![Offset(1), Offset(2)],
            ),
            (CleanupPolicy::Compact, None, Offset(2), vec![Offset(2)]),
        ];

        for (cleanup_policy, retention_size, log_start, expected) in cases {
            let dir = tempdir().unwrap();
            let mut log = Log::open(
                dir.path(),
                LogConfig {
                    segment_size: bytes(1),
                    retention: Some(secs(10)),
                    retention_size,
                    cleanup_policy,
                    ..LogConfig::default()
                },
            )
            .unwrap();
            for timestamp in [1_000, 95_000, 95_000] {
                let mut batch = sample_batch(1);
                batch.base_timestamp = timestamp;
                batch.max_timestamp = timestamp;
                log.append(&mut batch).unwrap();
            }
            log.set_log_start_offset(log_start).unwrap();

            log.tick(now, UNBOUNDED_HW).unwrap();

            let surviving: Vec<Offset> = log
                .segments
                .iter()
                .chain(log.active.as_ref())
                .filter(|segment| segment.size() > ByteSize::ZERO)
                .map(Segment::base_offset)
                .collect();
            check!(
                surviving == expected,
                "{cleanup_policy:?} {retention_size:?} start={log_start:?}"
            );
        }
    }

    #[test]
    fn tick_with_no_retention_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(2);
        let mut b2 = sample_batch(3);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now(), UNBOUNDED_HW).unwrap();
        assert2::assert!(log.log_end_offset() == before);
    }

    #[test]
    fn tick_never_deletes_only_segment() {
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention: Some(secs(1)),
            retention_size: Some(ByteSize::ZERO),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut b1 = sample_batch(2);
        log.append(&mut b1).unwrap();
        // Advance "now" 30 days into the future.
        let now = SystemTime::now() + Duration::from_hours(30 * 24);
        log.tick(now, UNBOUNDED_HW).unwrap();
        assert2::assert!(log.log_end_offset() == 2);
    }

    // The time-driven segment roll moved to the append path (see
    // `crate::log::append::tests`), against the incoming batch's own
    // timestamp rather than the wall clock, so `tick` no longer rolls at
    // all: an idle partition's active segment never rolls, and the roll a
    // busy one gets matches Kafka's `LogSegment.shouldRoll`.

    #[test]
    fn tick_removes_only_retained_away_segment_stamp_indexes() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        for _ in 0..3 {
            log.append(&mut sample_batch(1)).unwrap();
        }

        log.tick(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1),
            UNBOUNDED_HW,
        )
        .unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
        check!(!log.sealed_txn_indexes.contains_key(&Offset(0)));
        check!(!log.sealed_txn_indexes.contains_key(&Offset(1)));
    }

    #[test]
    fn tick_retention_size_debt_calculation() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            retention_size: Some(bytes(100_000)),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..5 {
            log.append(&mut sample_batch(1)).unwrap();
        }
        let total = log.size();
        assert2::assert!(total < bytes(100_000));
        // Under budget: nothing is evicted by size
        log.tick(SystemTime::UNIX_EPOCH, UNBOUNDED_HW).unwrap();
        assert2::assert!(log.segments.len() == 4);

        // Budget smaller than total size: only excess is evicted
        let target_budget = total - bytes(100);
        let mut new_config = log.config_snapshot();
        new_config.retention_size = Some(target_budget);
        log.set_config(new_config);
        log.tick(SystemTime::UNIX_EPOCH, UNBOUNDED_HW).unwrap();
        // Evicts at least one segment
        assert2::assert!(log.segments.len() < 4);
    }

    #[test]
    fn tick_skips_retention_when_remote_storage_enable_is_true() {
        use std::time::Duration;
        let far_future = SystemTime::now() + Duration::from_hours(365 * 24);

        // Tiered topic: tick must not delete any segment, however old.
        // The remote-log manager owns local eviction.
        let dir_tiered = tempdir().unwrap();
        let mut tiered = rolled_log(
            dir_tiered.path(),
            &LogConfig {
                remote_storage_enable: true,
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let sealed_before = tiered.tierable_segments().len();
        assert2::assert!(sealed_before > 0);
        tiered.tick(far_future, UNBOUNDED_HW).unwrap();
        assert2::assert!(tiered.tierable_segments().len() == sealed_before);

        // Non-tiered baseline: tick should still evict aggressively.
        let dir_plain = tempdir().unwrap();
        let mut plain = rolled_log(
            dir_plain.path(),
            &LogConfig {
                remote_storage_enable: false,
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        assert2::assert!(!plain.tierable_segments().is_empty());
        plain.tick(far_future, UNBOUNDED_HW).unwrap();
        // Non-tiered path keeps at least one segment (the active one); every
        // sealed segment is evicted.
        assert2::assert!(plain.tierable_segments().len() == 0);
    }
}
