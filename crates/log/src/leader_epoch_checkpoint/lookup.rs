//! The read-side queries over a recorded epoch history: where an epoch ends,
//! which epoch owned an offset, and the KIP-320 `(found_epoch, end_offset)`
//! pair a follower or consumer truncates to. The pure core over a raw slice
//! sits here with the methods that delegate to it, so that the divergence
//! contract is stated once.

use {
    super::{LeaderEpochCheckpoint, UNDEFINED_OFFSET, epoch_and_offset_for_entries},
    krabka_ids::{LeaderEpoch, Offset},
};

impl LeaderEpochCheckpoint {
    /// End offset of a *recorded* `epoch`. It is the `start_offset` of the
    /// next-larger recorded epoch, or `log_end_offset` if `epoch` is the
    /// current epoch. The method returns -1, which is `UNDEFINED_OFFSET`, if
    /// `epoch` is not recorded.
    ///
    /// This is an exact-match lookup for the log's own bookkeeping, not
    /// Kafka's `endOffsetFor`: a gap epoch or one below the first entry is
    /// unknown here. Wire answers use [`Self::epoch_and_offset_for`].
    #[must_use]
    pub fn end_offset_for_epoch(&self, epoch: LeaderEpoch, log_end_offset: Offset) -> Offset {
        if !self.entries.iter().any(|e| e.epoch == epoch) {
            return UNDEFINED_OFFSET;
        }
        // End of `epoch` is the start of the next-larger epoch. Higher
        // epochs always carry higher start offsets, so the minimum start
        // among epochs `> epoch` is that next epoch's start; if `epoch` is
        // the latest, there is none and the end is the log end. No clone or
        // sort — a single linear pass.
        self.entries
            .iter()
            .filter(|e| e.epoch > epoch)
            .map(|e| e.start_offset)
            .min()
            .unwrap_or(log_end_offset)
    }

    /// Floor lookup: return the epoch of the entry whose `start_offset` is
    /// the greatest value `<= offset`, that is, the leader epoch that owned
    /// `offset`.
    ///
    /// The method returns `None` when the checkpoint has no entries, and when
    /// `offset` comes before the first entry's `start_offset`. In the second
    /// case the offset predates every recorded epoch boundary.
    ///
    /// Entries are stored in increasing `start_offset` order by construction,
    /// because `append` always writes the current epoch, whose `start_offset`
    /// is `>=` that of every prior entry. This lookup is therefore a single
    /// linear scan from the back. It is equivalent to a search for the last
    /// entry with `start_offset <= offset`.
    #[must_use]
    pub fn epoch_for_offset(&self, offset: Offset) -> Option<LeaderEpoch> {
        self.entries
            .iter()
            .filter(|e| e.start_offset <= offset)
            .max_by_key(|e| e.start_offset)
            .map(|e| e.epoch)
    }

    /// Kafka `LeaderEpochFileCache.endOffsetFor`. It returns
    /// `(found_epoch, end_offset)`: the epoch that the requested offset range
    /// really belongs to on this log, and the first offset *after* that epoch.
    /// The broker uses it to detect follower and consumer log divergence
    /// (KIP-101/320). The Creusot contract of
    /// [`epoch_and_offset_for_entries`] pins Kafka's whole case table:
    ///
    ///  - `requested == UNDEFINED_EPOCH`       → `(UNDEFINED_EPOCH, UNDEFINED_OFFSET)`
    ///  - `requested == latest recorded epoch` → `(requested, log_end_offset)`
    ///  - no recorded epoch above `requested`  → `(UNDEFINED_EPOCH, UNDEFINED_OFFSET)`
    ///    (this covers an empty checkpoint and a future epoch)
    ///  - `requested` below every recorded epoch → `(requested, first_recorded_start)`
    ///  - otherwise (gap or exact older match)  → `(floor_epoch, next_epoch_start)`
    ///
    /// where `floor_epoch` is the largest recorded epoch `<= requested` and
    /// `next_epoch_start` is the start of the least recorded epoch
    /// `> requested`. `(-1, -1)` means "cannot place this epoch": an
    /// `OffsetForLeaderEpoch` row carries it as is, and a `Fetch` answers it
    /// with `OFFSET_OUT_OF_RANGE` (`Partition.readRecords`).
    #[must_use]
    pub fn epoch_and_offset_for(
        &self,
        requested_epoch: LeaderEpoch,
        log_end_offset: Offset,
    ) -> (LeaderEpoch, Offset) {
        epoch_and_offset_for_entries(&self.entries, requested_epoch, log_end_offset)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            leader_epoch_checkpoint::{
                UNDEFINED_EPOCH,
                test_support::{TempCheckpoint, checkpoint, entry, fresh, history_entries},
            },
            test_support::TempPath,
        },
        assert2::check,
    };

    #[test]
    fn end_offset_for_current_epoch_returns_log_end_offset() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0), entry(1, 50)]);
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(100)) == 100);
    }

    #[test]
    fn end_offset_for_older_epoch_returns_next_start() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0), entry(1, 50), entry(2, 100)]);
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(0), Offset(200)) == Offset(50));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(200)) == Offset(100));
    }

    #[test]
    fn end_offset_for_unknown_epoch_returns_undefined() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0)]);
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(7), Offset(200)) == -1);
    }

    // ── epoch_for_offset ──────────────────────────────────────────────────────

    #[yare::parameterized(
        zero = { "zero", 0 },
        positive = { "positive", 100 },
    )]
    fn epoch_for_offset_empty_returns_none(_name: &str, offset: i64) {
        let TempPath { dir: _d, path } = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert2::assert!(c.epoch_for_offset(Offset(offset)) == None);
    }

    #[test]
    fn epoch_for_offset_before_first_entry_returns_none() {
        // Epoch 0 starts at offset 10 (first entry does not start at 0).
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 10), entry(1, 50)]);
        assert2::assert!(c.epoch_for_offset(Offset(9)) == None);
    }

    #[yare::parameterized(
        start_epoch_0 = { 0, Some(LeaderEpoch(0)), "start of epoch 0" },
        middle_epoch_0 = { 25, Some(LeaderEpoch(0)), "middle of epoch 0" },
        last_before_epoch_1 = { 49, Some(LeaderEpoch(0)), "last offset before epoch 1" },
        start_epoch_1 = { 50, Some(LeaderEpoch(1)), "start of epoch 1" },
        middle_epoch_1 = { 75, Some(LeaderEpoch(1)), "middle of epoch 1" },
        last_before_epoch_2 = { 99, Some(LeaderEpoch(1)), "last offset before epoch 2" },
    )]
    fn epoch_for_offset_within_epoch_range(offset: i64, want: Option<LeaderEpoch>, why: &str) {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0), entry(1, 50), entry(2, 100)]);
        check!(
            c.epoch_for_offset(Offset(offset)) == want,
            "{why} (offset={offset})"
        );
    }

    /// Epoch 1 owns its first offset and all following offsets once it is the
    /// last recorded epoch.
    #[yare::parameterized(
        epoch_boundary = { "epoch 1 start", 50 },
        past_last = { "past last entry", 100 },
        far_past_last = { "far past last entry", 999 },
    )]
    fn epoch_for_offset_at_or_past_last_entry(_name: &str, offset: i64) {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0), entry(1, 50)]);
        assert2::assert!(c.epoch_for_offset(Offset(offset)) == Some(LeaderEpoch(1)));
    }

    #[test]
    fn epoch_for_offset_single_entry_at_zero() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: c,
        } = checkpoint(&[entry(0, 0)]);
        assert2::assert!(c.epoch_for_offset(Offset(0)) == Some(LeaderEpoch(0)));
        assert2::assert!(c.epoch_for_offset(Offset(1000)) == Some(LeaderEpoch(0)));
    }

    // ── epoch_and_offset_for (KIP-320) ────────────────────────────────────────

    #[derive(Default)]
    struct EpochLookupScenario {
        checkpoint: Option<TempCheckpoint>,
        answer: Option<(LeaderEpoch, Offset)>,
    }
    #[rstest::fixture]
    fn epoch_lookup() -> EpochLookupScenario {
        let checkpoint = None;
        EpochLookupScenario {
            checkpoint,
            ..EpochLookupScenario::default()
        }
    }
    #[rstest_bdd_macros::given("leader epoch lookup history {history}")]
    fn given_epoch_lookup_history(epoch_lookup: &mut EpochLookupScenario, history: String) {
        epoch_lookup.checkpoint = Some(checkpoint(&history_entries(&history)));
    }
    #[rstest_bdd_macros::when("epoch {requested} is looked up with log end {log_end}")]
    fn find_epoch_end(epoch_lookup: &mut EpochLookupScenario, requested: i32, log_end: i64) {
        epoch_lookup.answer = Some(
            epoch_lookup
                .checkpoint
                .as_ref()
                .expect("checkpoint history given")
                .checkpoint
                .epoch_and_offset_for(LeaderEpoch(requested), Offset(log_end)),
        );
    }
    #[rstest_bdd_macros::then("the epoch lookup returns epoch {epoch} ending at offset {offset}")]
    fn expected_epoch_end(epoch_lookup: &EpochLookupScenario, epoch: i32, offset: i64) {
        let expected_epoch = if epoch == -1 {
            UNDEFINED_EPOCH
        } else {
            LeaderEpoch(epoch)
        };
        check!(epoch_lookup.answer == Some((expected_epoch, Offset(offset))));
    }
    rstest_bdd_macros::scenarios!("tests/features/leader_epoch_lookup", fixtures = [epoch_lookup: EpochLookupScenario]);
}
