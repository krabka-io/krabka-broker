//! Flush thresholds, per-segment roll jitter, and deferred file reclamation.

use {
    super::{Log, PendingDelete, RollJitter},
    crate::{error::LogError, io::LogIo, retention},
    krabka_ids::Offset,
    krabka_units::{Time, prelude::TimeExt as _},
    std::{
        collections::hash_map::RandomState,
        hash::BuildHasher,
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
};

/// Retired segment files whose deletion delay has passed, taken out of the
/// log by [`Log::maintain_deferring_deletions`].
///
/// Unlinking a large segment file drops its page cache, which takes tens of
/// milliseconds a file on a loaded disk. Holding the files here lets the
/// caller delete them without holding the log, so the appends and reads of
/// the partition do not wait for it.
#[derive(Debug, Default)]
#[must_use = "the retired files stay on disk until `delete` runs"]
pub struct DueDeletions {
    io: Option<Arc<dyn LogIo>>,
    pending: Vec<PendingDelete>,
}

/// A [`DueDeletions::delete`] that stopped at a failure.
#[derive(Debug)]
pub struct FailedDeletion {
    /// The failure that stopped the deletion.
    pub error: LogError,
    /// The files not yet deleted. [`Log::restore_deletions`] hands them back
    /// to the log for a later pass.
    pub remaining: DueDeletions,
}

impl DueDeletions {
    /// Whether there is nothing to delete.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.pending.is_empty() }

    /// Delete the files, newest name first within each retired segment, as
    /// [`Log::maintain`] does.
    ///
    /// # Errors
    /// Stops at the first file that fails to delete and returns the error with
    /// every file not yet deleted.
    pub fn delete(mut self) -> Result<(), FailedDeletion> {
        let Some(io) = self.io.clone() else {
            return Ok(());
        };
        while let Some(pending) = self.pending.first_mut() {
            while let Some(path) = pending.paths.last() {
                if let Err(error) = retention::remove_optional(&*io, path) {
                    return Err(FailedDeletion {
                        error,
                        remaining: self,
                    });
                }
                pending.paths.pop();
            }
            self.pending.remove(0);
        }
        Ok(())
    }
}

impl Log {
    /// Flush dirty data and reclaim retired files whose deadlines have passed.
    ///
    /// # Errors
    /// Returns the first flush or file deletion failure.
    ///
    /// # Panics
    /// Panics when the configuration lock is poisoned.
    pub fn maintain(&mut self, now: SystemTime) -> Result<(), LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(Instant::now())?;
        self.reap_deleted_files(now)
    }

    /// [`Self::maintain`], except that the retired files whose deadlines have
    /// passed are handed back rather than deleted under `&mut self`.
    ///
    /// # Errors
    /// Returns the first flush failure. The retired files then stay with the
    /// log.
    ///
    /// # Panics
    /// Panics when the configuration lock is poisoned.
    pub fn maintain_deferring_deletions(
        &mut self,
        now: SystemTime,
    ) -> Result<DueDeletions, LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(Instant::now())?;
        Ok(self.take_due_deletions(now))
    }

    /// Take the retired files whose deadlines have passed, leaving the rest.
    pub fn take_due_deletions(&mut self, now: SystemTime) -> DueDeletions {
        let (due, waiting) = std::mem::take(&mut self.pending_deletes)
            .into_iter()
            .partition(|pending| pending.deadline <= now);
        self.pending_deletes = waiting;
        DueDeletions {
            io: Some(self.io.clone()),
            pending: due,
        }
    }

    /// Hand back the files a [`DueDeletions::delete`] did not delete. They
    /// become due again at `not_before`, so a caller retrying a failing disk
    /// can space its attempts.
    pub fn restore_deletions(&mut self, remaining: DueDeletions, not_before: SystemTime) {
        self.pending_deletes
            .extend(remaining.pending.into_iter().map(|pending| PendingDelete {
                deadline: pending.deadline.max(not_before),
                paths: pending.paths,
            }));
    }

    /// Time until the next flush or retired-file deadline, or `None` when idle.
    ///
    /// # Panics
    /// Panics when the configuration lock is poisoned.
    #[must_use]
    pub fn maintenance_delay(&self, now: SystemTime) -> Option<std::time::Duration> {
        self.maintenance_delay_at(now, Instant::now())
    }

    fn maintenance_delay_at(&self, now: SystemTime, elapsed: Instant) -> Option<Duration> {
        let config = self.config.read().unwrap();
        let flush = if self.unflushed_messages == 0 {
            None
        } else if config
            .flush_messages
            .is_some_and(|limit| self.unflushed_messages >= limit)
        {
            Some(Duration::ZERO)
        } else {
            config.flush_interval.map(|interval| {
                Duration::from_millis(u64::try_from(interval.millis_i64_trunc()).unwrap_or(0))
                    .saturating_sub(elapsed.saturating_duration_since(self.last_flush))
            })
        };
        flush
            .into_iter()
            .chain(
                self.pending_deletes
                    .iter()
                    .map(|pending| pending.deadline.duration_since(now).unwrap_or_default()),
            )
            .min()
    }

    pub(super) fn jittered_roll_interval(&mut self, interval: Time) -> Time {
        let bound = self
            .config
            .read()
            .unwrap()
            .segment_jitter
            .millis_i64_trunc()
            .min(interval.millis_i64_trunc());
        if bound <= 0 {
            return interval;
        }
        let base = self
            .active
            .as_ref()
            .expect("an open log has an active segment")
            .base_offset();
        let sample = match self.roll_jitter {
            Some(RollJitter {
                segment_base,
                sample,
            }) if segment_base == base => sample,
            _ => {
                let sample = RandomState::new().hash_one(base);
                self.roll_jitter = Some(RollJitter {
                    segment_base: base,
                    sample,
                });
                sample
            }
        };
        let bound = u64::try_from(bound).expect("the jitter bound is positive");
        let jitter = i64::try_from(sample % bound).expect("jitter is smaller than an i64 bound");
        Time::from_millis(interval.millis_i64_trunc().saturating_sub(jitter))
    }

    pub(super) fn flush_after_append(&mut self, delta: i32, force: bool) -> Result<(), LogError> {
        self.unflushed_messages = self
            .unflushed_messages
            .saturating_add(u64::try_from(delta).unwrap_or(0) + 1);
        let threshold = self.config.read().unwrap().flush_messages;
        if force {
            // Durable sidecars must not precede records in pending rollover jobs.
            self.rollover_flusher.finish()?;
            self.active_segment_flush()?;
            self.unflushed_messages = 0;
            self.last_flush = Instant::now();
        } else if threshold.is_some_and(|limit| self.unflushed_messages >= limit) {
            self.sync()?;
        } else {
            self.flush_if_due(Instant::now())?;
        }
        Ok(())
    }

    pub(super) fn flush_if_due(&mut self, now: Instant) -> Result<(), LogError> {
        let due = {
            let config = self.config.read().unwrap();
            self.unflushed_messages > 0
                && (config
                    .flush_messages
                    .is_some_and(|limit| self.unflushed_messages >= limit)
                    || config.flush_interval.is_some_and(|interval| {
                        now.saturating_duration_since(self.last_flush).as_millis()
                            >= u128::try_from(interval.millis_i64_trunc()).unwrap_or(0)
                    }))
        };
        if due {
            self.sync()?;
            self.last_flush = now;
        }
        Ok(())
    }

    pub(super) fn retire_segment(&mut self, base: Offset, now: SystemTime) -> Result<(), LogError> {
        if self.config.read().unwrap().file_delete_delay <= Time::ZERO {
            return retention::delete_segment_files(&*self.io, &self.dir, base);
        }
        self.retire_files(base, now, &[
            "log",
            "index",
            "timeindex",
            "txnindex",
            "stampindex",
            "snapshot",
        ])
    }

    pub(super) fn retire_files(
        &mut self,
        base: Offset,
        now: SystemTime,
        extensions: &[&str],
    ) -> Result<(), LogError> {
        let delay = self.config.read().unwrap().file_delete_delay;
        let delay =
            std::time::Duration::from_millis(u64::try_from(delay.millis_i64_trunc()).unwrap_or(0));
        let deadline = now
            .checked_add(delay)
            .ok_or_else(|| LogError::InvalidArgument("file deletion deadline overflow".into()))?;
        let mut paths = Vec::new();
        let result = retention::retire_files(&*self.io, &self.dir, base, extensions, &mut paths);
        if !paths.is_empty() {
            self.pending_deletes.push(PendingDelete { deadline, paths });
        }
        result
    }

    pub(super) fn reap_deleted_files(&mut self, now: SystemTime) -> Result<(), LogError> {
        self.take_due_deletions(now)
            .delete()
            .map_err(|FailedDeletion { error, remaining }| {
                self.restore_deletions(remaining, now);
                error
            })
    }
}

#[cfg(test)]
mod tests;
