//! When the log start reaches `log-start-offset-checkpoint`.
//!
//! A start that no segment boundary witnesses is written before the call that
//! moved it returns: a `DeleteRecords` or a trim that lands inside a segment, a
//! tiered floor below every local segment, a truncation that lowers the start.
//! Without the file, a reopened log serves those records again.
//!
//! A start that retention moved is different. Retention deletes whole
//! segments, so the base offset of the first surviving segment already records
//! the new start. Kafka writes that value on a timer,
//! `log.flush.start.offset.checkpoint.interval.ms`
//! (`LogManager.checkpointLogStartOffsets`), rather than on each
//! `deleteOldSegments`. Here the log owes the write for at most
//! [`LogConfig::start_offset_checkpoint_interval`](crate::LogConfig::start_offset_checkpoint_interval),
//! and the broker's writer pays it from its maintenance pass, which
//! [`Log::maintenance_delay`] wakes when the write falls due.
//!
//! A crash before the write leaves an older, lower start on disk. [`Log::open`]
//! reads it as Kafka's `LogLoader` does: on a log without remote storage the
//! start is never below the base offset of the first segment.

use {
    super::Log,
    crate::{error::LogError, log_start_offset_checkpoint},
    krabka_ids::Offset,
    krabka_units::prelude::TimeExt as _,
    std::time::{Duration, Instant},
};

impl Log {
    /// Write `start` to the checkpoint durably, and record that the file holds
    /// it, which pays any write retention owed.
    pub(super) fn write_start_checkpoint(&mut self, start: Offset) -> Result<(), LogError> {
        log_start_offset_checkpoint::write(&*self.io, &self.dir, start)?;
        self.checkpointed_start = Some(start);
        self.start_checkpoint_due = None;
        Ok(())
    }

    /// Owe the checkpoint `start`, the start retention just moved the log to.
    ///
    /// The deadline is set by the first change the file has not seen, so a
    /// log whose retention keeps moving the start still writes once an
    /// interval rather than never.
    pub(super) fn defer_start_checkpoint(&mut self, start: Offset, now: Instant) {
        if self.checkpointed_start == Some(start) {
            self.start_checkpoint_due = None;
        } else if self.start_checkpoint_due.is_none() {
            // Kafka's setting is an `Int` of milliseconds, so no larger
            // interval can overflow the deadline.
            let interval = self
                .config
                .read()
                .unwrap()
                .start_offset_checkpoint_interval
                .millis_i64_trunc()
                .clamp(0, i64::from(i32::MAX));
            let interval = Duration::from_millis(interval.unsigned_abs());
            self.start_checkpoint_due = Some(now + interval);
        }
    }

    /// Record that the checkpoint file is gone and nothing is owed to it.
    pub(super) fn forget_start_checkpoint(&mut self) {
        self.checkpointed_start = None;
        self.start_checkpoint_due = None;
    }

    /// Write the log start retention moved, once it has waited
    /// [`LogConfig::start_offset_checkpoint_interval`](crate::LogConfig::start_offset_checkpoint_interval).
    /// [`Log::maintain`] and [`Log::maintain_deferring_deletions`] call this.
    ///
    /// # Errors
    /// Returns [`LogError::Io`] when the checkpoint cannot be written. The
    /// write stays owed.
    pub fn checkpoint_log_start_if_due(&mut self) -> Result<(), LogError> {
        self.checkpoint_log_start_at(Instant::now())
    }

    pub(super) fn checkpoint_log_start_at(&mut self, now: Instant) -> Result<(), LogError> {
        if self.start_checkpoint_due.is_some_and(|due| due <= now) {
            self.write_start_checkpoint(self.start_offset)?;
        }
        Ok(())
    }

    /// Write an owed log start now, whatever its deadline. [`Log::sync`] calls
    /// this, and the broker calls it when a partition's writer stops, as
    /// Kafka's `LogManager.shutdown` checkpoints every log start. A fenced log
    /// is being removed, so it writes nothing.
    ///
    /// # Errors
    /// Returns [`LogError::Io`] when the checkpoint cannot be written.
    pub fn flush_log_start_checkpoint(&mut self) -> Result<(), LogError> {
        if self.start_checkpoint_due.is_none() || self.fenced {
            return Ok(());
        }
        self.write_start_checkpoint(self.start_offset)
    }

    /// Time until an owed log start falls due, or `None` when nothing is owed.
    pub(super) fn start_checkpoint_delay(&self, now: Instant) -> Option<Duration> {
        self.start_checkpoint_due
            .map(|due| due.saturating_duration_since(now))
    }
}

#[cfg(test)]
mod tests;
