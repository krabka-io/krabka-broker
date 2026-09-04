//! Periodic maintenance: the time-driven segment roll and the time- and
//! size-based retention sweep over sealed segments.
//!
//! Retention never deletes the active segment, never leaves the log with
//! no segment at all, and never evicts a segment that still holds a record
//! whose delivery time has not arrived.

use std::{collections::HashSet, time::SystemTime};

use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, ByteSizeExt, TimeExt as _};
use tracing::instrument;

use super::Log;
use crate::{error::LogError, retention, segment::Segment};

impl Log {
    /// Periodic maintenance: roll an old active segment, then apply time- and
    /// size-based retention to sealed segments. The active segment is never
    /// deleted, and if every segment would otherwise be evicted we retain at
    /// least one.
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
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        let segment_roll_interval = self.config.read().unwrap().segment_roll_interval;
        let roll_cutoff =
            retention::now_ms(now).saturating_sub(segment_roll_interval.millis_i64_trunc());
        let should_roll = self.active.as_ref().is_some_and(|segment| {
            segment
                .offset_for_timestamp(i64::MIN)
                .is_some_and(|(_, first_timestamp)| first_timestamp < roll_cutoff)
        });
        if should_roll {
            self.roll_active_segment()?;
        }

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
        let time_evict = retention::time_based_evict(&sealed_refs, &cfg_guard, now);
        let total_size: ByteSize = sealed_refs
            .iter()
            .fold(active_size, |total, segment| total + segment.size());
        let size_debt = cfg_guard.retention_size.map_or(0, |budget| {
            if total_size > budget {
                (total_size - budget).bytes_u64()
            } else {
                0
            }
        });
        drop(cfg_guard);

        let time_expired: Vec<bool> = (0..self.segments.len())
            .map(|index| index < time_evict.len())
            .collect();
        // On an immediate topic the floor is the log end, so every entry is
        // false. On a scheduled topic the first waiting segment stops the
        // prefix; later segments are never skipped around it.
        let scheduled: Vec<bool> = self
            .segments
            .iter()
            .map(|segment| segment.last_offset() >= visible_floor)
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

        log.tick(SystemTime::now()).expect("tick");
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

    #[test]
    fn tick_with_no_retention_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(2);
        let mut b2 = sample_batch(3);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now()).unwrap();
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
        log.tick(now).unwrap();
        assert2::assert!(log.log_end_offset() == 2);
    }

    #[test]
    fn tick_rolls_active_segment_when_first_record_is_old() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(10),
            retention: None,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(2);
        batch.base_timestamp = 1_000;
        batch.max_timestamp = 1_000;
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(12))
            .unwrap();

        assert2::assert!(log.segments.len() == 1);
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(2));
    }

    #[test]
    fn tick_keeps_active_segment_at_roll_boundary() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(10),
            retention: None,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(1);
        batch.base_timestamp = 1_000;
        batch.max_timestamp = 1_000;
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(11))
            .unwrap();

        assert2::assert!(log.segments.is_empty());
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(0));
    }

    #[test]
    fn tick_rolls_tiered_segment_without_local_eviction() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(1),
            retention: Some(secs(1)),
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(1);
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10))
            .unwrap();

        assert2::assert!(log.segments.len() == 1);
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(1));
    }

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

        log.tick(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
    }

    #[test]
    fn tick_rolls_but_skips_retention_when_remote_storage_enable_is_true() {
        use std::time::Duration;
        let far_future = SystemTime::now() + Duration::from_hours(365 * 24);

        // Tiered topic: tick rolls the old active segment but must not delete
        // any segment. The remote-log manager owns local eviction.
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
        tiered.tick(far_future).unwrap();
        assert2::assert!(tiered.tierable_segments().len() == sealed_before + 1);

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
        plain.tick(far_future).unwrap();
        // Non-tiered path keeps at least one segment (the active one); every
        // sealed segment is evicted.
        assert2::assert!(plain.tierable_segments().len() == 0);
    }
}
