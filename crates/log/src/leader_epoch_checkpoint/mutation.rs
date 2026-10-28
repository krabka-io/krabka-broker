//! The mutations a live checkpoint accepts -- `append`, `assign`,
//! `truncate_from_end`, `truncate_from_start` and `clear` -- each of which
//! persists the file only when it actually changed the entry list. They wrap the pure cores in [`super`]
//! and mirror Kafka's `LeaderEpochFileCache`.

use {
    super::{EpochEntry, LeaderEpochCheckpoint, append_to, is_strict_successor, truncate_to},
    crate::error::LogError,
    krabka_ids::{LeaderEpoch, Offset},
    tracing::instrument,
};

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

    /// Kafka's `LeaderEpochFileCache.assign(epoch, startOffset)`, which
    /// `UnifiedLog.assignEpochStartOffset` calls when `Partition.makeLeader`
    /// records a new leader epoch at the log end before anything is written.
    ///
    /// It is a no-op when `epoch` is already the latest recorded epoch and
    /// `start_offset` is at or after that entry's start
    /// (`isUpdateNeeded`). Otherwise it first drops every trailing entry whose
    /// epoch is `>= epoch` or whose start is `>= start_offset`
    /// (`maybeTruncateNonMonotonicEntries`) and then records the new entry, so
    /// the history stays strictly increasing in both columns. The file is
    /// rewritten only when the entry list changed, and a failed rewrite
    /// leaves the entries as they were.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidArgument`] for a negative epoch or start
    /// offset, as Kafka throws `IllegalArgumentException`, and an I/O error
    /// when the checkpoint cannot be persisted.
    #[instrument(level = "debug", skip(self), fields(epoch = epoch.0, start_offset = start_offset.0), err)]
    pub fn assign(&mut self, epoch: LeaderEpoch, start_offset: Offset) -> Result<(), LogError> {
        if epoch.0 < 0 || start_offset.0 < 0 {
            return Err(LogError::InvalidArgument(format!(
                "invalid leader epoch checkpoint entry ({}, {})",
                epoch.0, start_offset.0
            )));
        }
        // `isUpdateNeeded` first, so the per-batch call on every append costs
        // one comparison and no copy of the history.
        if !assign_needed(&self.entries, epoch, start_offset) {
            return Ok(());
        }
        let previous = self.entries.clone();
        assign_to(&mut self.entries, epoch, start_offset);
        self.flush_or_restore(previous)
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

    /// Kafka's `LeaderEpochFileCache.truncateFromStart(startOffset)`, which
    /// `UnifiedLog.maybeIncrementLogStartOffset` calls when the log start
    /// moves up and `LogLoader.load` calls with the checkpointed log start.
    ///
    /// Every entry that starts at or below `start_offset` is removed, and the
    /// newest of them comes back with its start raised to `start_offset`, so
    /// the epoch that covers the new log start still answers for it and no
    /// entry points below the log. The call is exclusive: an entry that
    /// starts exactly at `start_offset` keeps its place. The file is
    /// rewritten only when an entry was removed, and a failed rewrite leaves
    /// the entries as they were.
    ///
    /// # Errors
    /// Returns an I/O error when the checkpoint cannot be persisted.
    #[instrument(level = "debug", skip(self), fields(start_offset = start_offset.0), err)]
    pub fn truncate_from_start(&mut self, start_offset: Offset) -> Result<(), LogError> {
        let previous = self.entries.clone();
        if truncate_start_of(&mut self.entries, start_offset) {
            self.flush_or_restore(previous)?;
        }
        Ok(())
    }

    fn flush_or_restore(&mut self, previous: Vec<EpochEntry>) -> Result<(), LogError> {
        self.flush().inspect_err(|_| {
            // The file still holds `previous`; so must memory.
            self.entries = previous;
        })
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

/// Kafka's `LeaderEpochFileCache.isUpdateNeeded`: whether `assign` would
/// change `entries`. Only the latest epoch at or after its own start is a
/// no-op.
fn assign_needed(entries: &[EpochEntry], epoch: LeaderEpoch, start_offset: Offset) -> bool {
    entries
        .last()
        .is_none_or(|latest| latest.epoch != epoch || start_offset < latest.start_offset)
}

/// Pure core of [`LeaderEpochCheckpoint::assign`], for an entry that
/// [`assign_needed`] admitted: Kafka's `maybeTruncateNonMonotonicEntries`
/// followed by the put.
fn assign_to(entries: &mut Vec<EpochEntry>, epoch: LeaderEpoch, start_offset: Offset) {
    while entries
        .last()
        .is_some_and(|last| last.epoch >= epoch || last.start_offset >= start_offset)
    {
        entries.pop();
    }
    entries.push(EpochEntry {
        epoch,
        start_offset,
    });
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_start`]. Returns
/// `true` when it removed an entry, so the caller knows that it must flush.
fn truncate_start_of(entries: &mut Vec<EpochEntry>, start_offset: Offset) -> bool {
    let removed = entries
        .iter()
        .take_while(|entry| entry.start_offset <= start_offset)
        .count();
    let Some(newest_removed) = removed
        .checked_sub(1)
        .and_then(|index| entries.get(index))
        .map(|entry| entry.epoch)
    else {
        return false;
    };
    if removed == 1
        && entries
            .first()
            .is_some_and(|entry| entry.start_offset == start_offset)
    {
        // The one entry at the new start is put back unchanged.
        return false;
    }
    entries.splice(..removed, [EpochEntry {
        epoch: newest_removed,
        start_offset,
    }]);
    true
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            leader_epoch_checkpoint::test_support::{
                TempCheckpoint, check_persisted, checkpoint, entry, fresh, history_entries,
            },
            test_support::TempPath,
        },
        std::fs,
    };

    #[test]
    fn append_preserves_existing_rows() {
        let TempPath { dir: _d, path } = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(LeaderEpoch(0), Offset(0)).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(LeaderEpoch(1), Offset(50)).unwrap();
        assert2::assert!(c2.entries() == &[entry(0, 0), entry(1, 50)]);
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0)]);
        c.append(LeaderEpoch(0), Offset(999)).unwrap(); // ignored; epoch 0 already recorded
        assert2::assert!(c.entries() == &[entry(0, 0)]);
    }

    #[yare::parameterized(
        epoch_out_of_order = { 2, 150 },
        offset_not_increasing = { 6, 100 },
    )]
    fn append_rejects_novel_out_of_order_entries_without_mutating(epoch: i32, offset: i64) {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0), entry(5, 100)]);
        let before = fs::read(&c.path).unwrap();

        let error = c.append(LeaderEpoch(epoch), Offset(offset)).unwrap_err();
        assert2::assert!(matches!(error, LogError::InvalidArgument(_)));

        assert2::assert!(fs::read(&c.path).unwrap() == before);
        assert2::assert!(c.entries() == &[entry(0, 0), entry(5, 100)]);
    }

    #[test]
    fn append_after_truncation_restores_a_strict_history() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0), entry(5, 100)]);
        c.truncate_from_end(Offset(100)).unwrap();
        c.append(LeaderEpoch(2), Offset(50)).unwrap();
        c.append(LeaderEpoch(4), Offset(80)).unwrap();

        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(3), Offset(120)) == (LeaderEpoch(2), Offset(80))
        );
    }

    #[derive(Default)]
    struct EpochMutationScenario {
        checkpoint: Option<TempCheckpoint>,
        outcome: Option<Result<(), LogError>>,
    }
    #[rstest::fixture]
    fn epoch_mutation() -> EpochMutationScenario {
        let checkpoint = None;
        EpochMutationScenario {
            checkpoint,
            ..EpochMutationScenario::default()
        }
    }
    #[rstest_bdd_macros::given("a persisted leader epoch history {history}")]
    fn given_epoch_mutation_history(epoch_mutation: &mut EpochMutationScenario, history: String) {
        epoch_mutation.checkpoint = Some(checkpoint(&history_entries(&history)));
    }
    #[rstest_bdd_macros::when("leader epoch {epoch} is assigned to start offset {offset}")]
    fn assign_epoch_start(epoch_mutation: &mut EpochMutationScenario, epoch: i32, offset: i64) {
        epoch_mutation.outcome = Some(
            epoch_mutation
                .checkpoint
                .as_mut()
                .expect("checkpoint history given")
                .checkpoint
                .assign(LeaderEpoch(epoch), Offset(offset)),
        );
    }
    #[rstest_bdd_macros::when("the leader epoch history is truncated from start offset {offset}")]
    fn truncate_epoch_start(epoch_mutation: &mut EpochMutationScenario, offset: i64) {
        epoch_mutation.outcome = Some(
            epoch_mutation
                .checkpoint
                .as_mut()
                .expect("checkpoint history given")
                .checkpoint
                .truncate_from_start(Offset(offset)),
        );
    }
    #[rstest_bdd_macros::then("the leader epoch entries in memory and on disk are {history}")]
    fn persisted_epoch_history(epoch_mutation: &EpochMutationScenario, history: String) {
        epoch_mutation
            .outcome
            .as_ref()
            .expect("checkpoint mutation performed")
            .as_ref()
            .expect("checkpoint mutation accepted");
        check_persisted(
            &epoch_mutation
                .checkpoint
                .as_ref()
                .expect("checkpoint history given")
                .checkpoint,
            &history_entries(&history),
            &history,
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/leader_epoch_mutation", fixtures = [epoch_mutation: EpochMutationScenario]);

    #[yare::parameterized(
        negative_epoch = { -1, 5 },
        negative_start = { 2, -1 },
    )]
    fn assign_rejects_negative_entries_without_mutating(epoch: i32, start: i64) {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0)]);
        let error = c.assign(LeaderEpoch(epoch), Offset(start)).unwrap_err();
        assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        assert2::assert!(c.entries() == &[entry(1, 0)]);
    }

    #[test]
    fn truncate_from_end_removes_entries_at_or_after_end_offset() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0), entry(7, 4)]);
        c.truncate_from_end(Offset(4)).unwrap();
        assert2::assert!(c.latest_epoch() == Some(LeaderEpoch(1)));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(7), Offset(4)) == Offset(-1));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(4)) == Offset(4));
        // Persisted: a reopen sees only epoch 1.
        let reopened = LeaderEpochCheckpoint::open(c.path.clone()).unwrap();
        assert2::assert!(reopened.latest_epoch() == Some(LeaderEpoch(1)));
    }

    #[test]
    fn clear_removes_all_entries_and_persists_empty() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0), entry(2, 50)]);
        c.clear().unwrap();
        assert2::assert!(c.entries() == &[][..]);
        assert2::assert!(c.latest_epoch() == None);
        // Persisted: a reopen sees no entries.
        let reopened = LeaderEpochCheckpoint::open(c.path.clone()).unwrap();
        assert2::assert!(reopened.entries().is_empty());
    }

    #[test]
    fn clear_on_empty_cache_skips_flush_and_writes_no_file() {
        let TempPath { dir: _d, path } = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.clear().unwrap();
        assert2::assert!(c.entries().is_empty());
        // The early-return skips the flush for an already-empty cache, so no
        // checkpoint file is written. A forced-`false` empty-guard would flush
        // an empty file here instead.
        assert2::assert!(!path.exists());
    }
}
