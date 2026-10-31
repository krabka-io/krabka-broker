//! Flush thresholds, per-segment roll jitter, and deferred file reclamation.

// Only `pace_writeback` reads a byte count, and `wasm32-wasip1` has no
// `pace_writeback`.
#[cfg(not(target_os = "wasi"))]
use {super::page_cache, krabka_units::prelude::ByteSizeExt};
use {
    super::{Log, PendingDelete, RollJitter},
    crate::{
        error::LogError,
        io::LogIo,
        retention::{self, DirSync},
        segment::Segment,
    },
    krabka_ids::Offset,
    krabka_units::{Time, prelude::TimeExt as _},
    std::{
        collections::hash_map::RandomState,
        hash::BuildHasher,
        mem,
        sync::{Arc, PoisonError},
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
    /// Flush dirty data, write a log start retention moved once its
    /// checkpoint is due, and reclaim retired files whose deadlines have
    /// passed.
    ///
    /// # Errors
    /// Returns the first flush or file deletion failure.
    pub fn maintain(&mut self, now: SystemTime) -> Result<(), LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(Instant::now())?;
        self.checkpoint_log_start_if_due()?;
        self.reap_deleted_files(now)
    }

    /// [`Self::maintain`], except that the retired files whose deadlines have
    /// passed are handed back rather than deleted under `&mut self`.
    ///
    /// # Errors
    /// Returns the first flush or checkpoint failure. The retired files then
    /// stay with the log.
    pub fn maintain_deferring_deletions(
        &mut self,
        now: SystemTime,
    ) -> Result<DueDeletions, LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(Instant::now())?;
        self.checkpoint_log_start_if_due()?;
        Ok(self.take_due_deletions(now))
    }

    /// Take the retired files whose deadlines have passed, leaving the rest.
    pub fn take_due_deletions(&mut self, now: SystemTime) -> DueDeletions {
        let (due, waiting) = mem::take(&mut self.pending_deletes)
            .into_iter()
            .partition(|pending| pending.deadline <= now);
        self.pending_deletes = waiting;
        DueDeletions {
            io: Some(Arc::clone(&self.io)),
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

    /// Time until the next flush, log-start checkpoint or retired-file
    /// deadline, or `None` when idle.
    #[must_use]
    pub fn maintenance_delay(&self, now: SystemTime) -> Option<Duration> {
        self.maintenance_delay_at(now, Instant::now())
    }

    pub(super) fn maintenance_delay_at(
        &self,
        now: SystemTime,
        elapsed: Instant,
    ) -> Option<Duration> {
        let config = self.config.read().unwrap_or_else(PoisonError::into_inner);
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
            .chain(self.start_checkpoint_delay(elapsed))
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
            .unwrap_or_else(PoisonError::into_inner)
            .segment_jitter
            .millis_i64_trunc()
            .min(interval.millis_i64_trunc());
        let Some(bound) = u64::try_from(bound).ok().filter(|&bound| bound > 0) else {
            return interval;
        };
        // An open log always has an active segment; without one there is no
        // roll to jitter.
        let Some(base) = self.active.as_ref().map(Segment::base_offset) else {
            return interval;
        };
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
        // The remainder is below `bound`, which came from an `i64`.
        let jitter = i64::try_from(sample % bound).unwrap_or(i64::MAX);
        Time::from_millis(interval.millis_i64_trunc().saturating_sub(jitter))
    }

    pub(super) fn flush_after_append(&mut self, delta: i32, force: bool) -> Result<(), LogError> {
        self.unflushed_messages = self
            .unflushed_messages
            .saturating_add(u64::try_from(delta).unwrap_or(0) + 1);
        let threshold = self
            .config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .flush_messages;
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

    /// Start a background sync of the active segment after every
    /// `writeback_window` bytes appended through the page cache, and wait for
    /// the oldest one while two are still running.
    ///
    /// This is what bounds the dirty data the kernel holds for the segment,
    /// to about two windows: see [`crate::LogConfig::writeback_window`]. The
    /// wait is the only time an append stops for it, and it ends when a sync
    /// of one window's bytes ends. Each sync also hands the segment's whole
    /// pages written since the last one to the page-cache ledger, which
    /// drops the oldest pages of the process's logs beyond
    /// [`crate::LogConfig::page_cache_budget`]. A segment written through
    /// `O_DIRECT` has no dirty pages, and `flush_on_append` syncs every
    /// append, so neither counts bytes here. `wasm32-wasip1` has no flush
    /// pool and paces nothing.
    #[cfg(not(target_os = "wasi"))]
    pub(super) fn pace_writeback(&mut self, appended: usize) -> Result<(), LogError> {
        let (window, budget) = {
            let config = self.config.read().unwrap_or_else(PoisonError::into_inner);
            let window = if config.flush_on_append {
                0
            } else {
                config.writeback_window.bytes_u64()
            };
            (window, config.page_cache_budget.map(ByteSizeExt::bytes_u64))
        };
        let active = self.active.as_ref().ok_or_else(super::no_active_segment)?;
        if active.writes_direct() {
            self.writeback_unsynced = 0;
            self.release_covered_sealed_tail();
            return Ok(());
        }
        if window == 0 {
            self.writeback_unsynced = 0;
            return Ok(());
        }
        self.writeback_unsynced = self
            .writeback_unsynced
            .saturating_add(u64::try_from(appended).unwrap_or(u64::MAX));
        if self.writeback_unsynced < window {
            return Ok(());
        }
        self.writeback_unsynced = 0;
        self.hand_over_pages(budget)
    }

    /// Hand the whole pages of the active segment written since the last
    /// hand-over to a write-back job, after waiting for the oldest job while
    /// two are still running. This is the end of a window of
    /// [`Self::pace_writeback`], and what [`Log::sync`] does with the pages a
    /// window had not reached, so no page written through the page cache
    /// stays outside the ledger's accounting. Nothing happens for a segment
    /// that writes directly, or when no whole page is new.
    ///
    /// Whole pages only: the page the next append writes into stays out of
    /// the write-out, so the append never finds it busy and the ledger never
    /// drops it from under it, and no page is handed over twice.
    #[cfg(not(target_os = "wasi"))]
    pub(super) fn hand_over_pages(&mut self, budget: Option<u64>) -> Result<(), LogError> {
        let active = self.active.as_ref().ok_or_else(super::no_active_segment)?;
        if active.writes_direct() {
            return Ok(());
        }
        let end = active.size().bytes_u64() & !(page_cache::PAGE - 1);
        let offset = self.writeback_handed;
        if end <= offset {
            return Ok(());
        }
        while self.writeback_jobs.len() >= 2
            && let Some(oldest) = self.writeback_jobs.pop_front()
        {
            self.rollover_flusher.finish_through(oldest)?;
        }
        self.writeback_handed = end;
        let file = active.log_file_handle();
        self.rollover_flusher.submit_writeback(
            file,
            Arc::clone(&self.io),
            offset,
            end - offset,
            budget,
        )?;
        self.writeback_jobs
            .push_back(self.rollover_flusher.ticket());
        Ok(())
    }

    /// The page-cache budget in bytes, as configured.
    #[cfg(not(target_os = "wasi"))]
    pub(super) fn page_cache_budget(&self) -> Option<u64> {
        self.config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .page_cache_budget
            .map(ByteSizeExt::bytes_u64)
    }

    /// Start the write-back pacing over for the segment that is active now,
    /// with its pages handed over up to `handed`: a fresh segment at 0, a
    /// segment cut in place at the cut, a sealed segment promoted by a cut at
    /// its size, all on a page boundary. A roll, a truncation and a reset
    /// each call this when they replace or cut the active segment; the
    /// frontier of the segment before would otherwise hold back every window
    /// until the new one grew past it.
    #[cfg(not(target_os = "wasi"))]
    pub(super) fn restart_writeback_pacing(&mut self, handed: u64) {
        self.writeback_unsynced = 0;
        self.writeback_jobs.clear();
        self.writeback_handed = handed & !(page_cache::PAGE - 1);
    }

    pub(super) fn flush_if_due(&mut self, now: Instant) -> Result<(), LogError> {
        let due = {
            let config = self.config.read().unwrap_or_else(PoisonError::into_inner);
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
        if self
            .config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .file_delete_delay
            <= Time::ZERO
        {
            return retention::delete_segment_files(&*self.io, &self.dir, base);
        }
        self.retire_files(
            base,
            now,
            &[
                "log",
                "index",
                "timeindex",
                "txnindex",
                "stampindex",
                "snapshot",
            ],
            DirSync::Now,
        )
    }

    pub(super) fn retire_files(
        &mut self,
        base: Offset,
        now: SystemTime,
        extensions: &[&str],
        sync: DirSync,
    ) -> Result<(), LogError> {
        let delay = self
            .config
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .file_delete_delay;
        let delay = Duration::from_millis(u64::try_from(delay.millis_i64_trunc()).unwrap_or(0));
        let deadline = now
            .checked_add(delay)
            .ok_or_else(|| LogError::InvalidArgument("file deletion deadline overflow".into()))?;
        let mut paths = Vec::new();
        let result =
            retention::retire_files(&*self.io, &self.dir, base, extensions, &mut paths, sync);
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
