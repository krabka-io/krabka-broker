//! The three mutations a live checkpoint accepts -- `append`,
//! `truncate_from_end` and `clear` -- each of which persists the file only when
//! it actually changed the entry list. They wrap the pure cores in [`super`]
//! and mirror Kafka's `LeaderEpochFileCache`.

use krabka_ids::{LeaderEpoch, Offset};
use tracing::instrument;

use super::{EpochEntry, LeaderEpochCheckpoint, append_to, is_strict_successor, truncate_to};
use crate::error::LogError;

impl LeaderEpochCheckpoint {
    /// Append `(epoch, start_offset)`. This method is idempotent. A second
    /// append of an entry with the same epoch does nothing and keeps the
    /// earliest recorded `start_offset`. The method rewrites the file
    /// atomically.
    #[instrument(level = "debug", skip(self), fields(epoch = epoch.0, start_offset = start_offset.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, epoch: LeaderEpoch, start_offset: Offset) -> Result<(), LogError> {
        if self.entries.iter().any(|entry| entry.epoch == epoch) {
            return Ok(());
        }
        let entry = EpochEntry {
            epoch,
            start_offset,
        };
        if self
            .entries
            .last()
            .is_some_and(|previous| !is_strict_successor(previous, &entry))
        {
            return Err(LogError::InvalidArgument(format!(
                "leader epoch checkpoint entry ({}, {}) is not strictly after the previous entry",
                epoch.0, start_offset.0
            )));
        }
        if append_to(&mut self.entries, epoch, start_offset) {
            self.flush()?;
        }
        Ok(())
    }

    /// Remove epoch entries that begin at or after `end_offset`. This mirrors
    /// Kafka's LeaderEpochFileCache.truncateFromEnd. The method persists the
    /// file if anything changed.
    #[instrument(level = "debug", skip(self), fields(end_offset = end_offset.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn truncate_from_end(&mut self, end_offset: Offset) -> Result<(), LogError> {
        let before = self.entries.len();
        truncate_to(&mut self.entries, end_offset);
        if self.entries.len() != before {
            self.flush()?;
        }
        Ok(())
    }

    /// Drop every recorded epoch. This mirrors Kafka's
    /// `LeaderEpochFileCache.clearAndFlush`, which
    /// `LocalLog.truncateFullyAndStartAt` invokes.
    ///
    /// [`crate::Log::reset_to`] uses this method. Once the log is empty, no
    /// offset has a backing record, so the broker may advertise no epoch. The
    /// method persists the now-empty file only when it removed something.
    #[instrument(level = "debug", skip(self), fields(cleared = self.entries.len()), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn clear(&mut self) -> Result<(), LogError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        self.entries.clear();
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader_epoch_checkpoint::test_support::fresh;

    #[test]
    fn append_preserves_existing_rows() {
        let (_d, path) = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(LeaderEpoch(0), Offset(0)).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(LeaderEpoch(1), Offset(50)).unwrap();
        assert2::assert!(
            c2.entries()
                == &[
                    EpochEntry {
                        epoch: LeaderEpoch(0),
                        start_offset: Offset(0),
                    },
                    EpochEntry {
                        epoch: LeaderEpoch(1),
                        start_offset: Offset(50),
                    },
                ]
        );
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(0), Offset(999)).unwrap(); // ignored; epoch 0 already recorded
        assert2::assert!(
            c.entries()
                == &[EpochEntry {
                    epoch: LeaderEpoch(0),
                    start_offset: Offset(0)
                }]
        );
    }

    #[test]
    fn append_rejects_novel_out_of_order_entries_without_mutating() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(5), Offset(100)).unwrap();
        let before = std::fs::read(&path).unwrap();

        for (epoch, offset) in [(2, 150), (6, 100)] {
            let error = c.append(LeaderEpoch(epoch), Offset(offset)).unwrap_err();
            assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        }

        assert2::assert!(std::fs::read(&path).unwrap() == before);
        assert2::assert!(
            c.entries()
                == &[
                    EpochEntry {
                        epoch: LeaderEpoch(0),
                        start_offset: Offset(0),
                    },
                    EpochEntry {
                        epoch: LeaderEpoch(5),
                        start_offset: Offset(100),
                    },
                ]
        );
    }

    #[test]
    fn append_after_truncation_restores_a_strict_history() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(5), Offset(100)).unwrap();
        c.truncate_from_end(Offset(100)).unwrap();
        c.append(LeaderEpoch(2), Offset(50)).unwrap();
        c.append(LeaderEpoch(4), Offset(80)).unwrap();

        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(3), Offset(120)) == (LeaderEpoch(2), Offset(80))
        );
    }

    #[test]
    fn truncate_from_end_removes_entries_at_or_after_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(1), Offset(0)).unwrap();
        c.append(LeaderEpoch(7), Offset(4)).unwrap();
        c.truncate_from_end(Offset(4)).unwrap();
        assert2::assert!(c.latest_epoch() == Some(LeaderEpoch(1)));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(7), Offset(4)) == Offset(-1));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(4)) == Offset(4));
    }

    #[test]
    fn clear_removes_all_entries_and_persists_empty() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(LeaderEpoch(1), Offset(0)).unwrap();
        c.append(LeaderEpoch(2), Offset(50)).unwrap();
        c.clear().unwrap();
        assert2::assert!(c.entries() == &[][..]);
        assert2::assert!(c.latest_epoch() == None);
        // Persisted: a reopen sees no entries.
        let reopened = LeaderEpochCheckpoint::open(path).unwrap();
        assert2::assert!(reopened.entries().is_empty());
    }

    #[test]
    fn clear_on_empty_cache_skips_flush_and_writes_no_file() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.clear().unwrap();
        assert2::assert!(c.entries().is_empty());
        // The early-return skips the flush for an already-empty cache, so no
        // checkpoint file is written. A forced-`false` empty-guard would flush
        // an empty file here instead.
        assert2::assert!(!path.exists());
    }
}
