//! The read-side queries over a recorded epoch history: where an epoch ends,
//! which epoch owned an offset, and the KIP-320 `(found_epoch, end_offset)`
//! pair a follower or consumer truncates to. The pure core over a raw slice
//! sits here with the methods that delegate to it, so that the divergence
//! contract is stated once.

use krabka_ids::{LeaderEpoch, Offset};

#[cfg(test)]
use super::UNDEFINED_EPOCH;
use super::{LeaderEpochCheckpoint, UNDEFINED_OFFSET, epoch_and_offset_for_entries};

impl LeaderEpochCheckpoint {
    /// End offset of `epoch`. It is the `start_offset` of the next-larger
    /// recorded epoch, or `log_end_offset` if `epoch` is the current epoch.
    /// The method returns -1, which is `UNDEFINED_OFFSET`, if `epoch` is
    /// unknown.
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
    /// (KIP-320):
    ///
    ///  - `requested == UNDEFINED_EPOCH`            → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested == latest recorded epoch`      → `(requested, log_end_offset)`
    ///  - `requested` above all recorded epochs     → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested` below all recorded epochs     → `(requested, first_recorded_start)`
    ///  - otherwise (gap or exact older match)      → `(floor_epoch, next_epoch_start)`
    ///
    /// where `floor_epoch` is the largest recorded epoch `<= requested`.
    /// `end_offset` is always a valid truncation target (`>= 0`).
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
    use assert2::check;

    use super::*;
    use crate::leader_epoch_checkpoint::test_support::fresh;

    #[test]
    fn end_offset_for_current_epoch_returns_log_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(100)) == 100);
    }

    #[test]
    fn end_offset_for_older_epoch_returns_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        c.append(LeaderEpoch(2), Offset(100)).unwrap();
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(0), Offset(200)) == Offset(50));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(200)) == Offset(100));
    }

    #[test]
    fn end_offset_for_unknown_epoch_returns_undefined() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(7), Offset(200)) == -1);
    }

    // ── epoch_for_offset ──────────────────────────────────────────────────────

    #[test]
    fn epoch_for_offset_empty_returns_none() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        for (_name, offset) in [("zero", 0), ("positive", 100)] {
            assert2::assert!(c.epoch_for_offset(Offset(offset)) == None);
        }
    }

    #[test]
    fn epoch_for_offset_before_first_entry_returns_none() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        // Epoch 0 starts at offset 10 (first entry does not start at 0).
        c.append(LeaderEpoch(0), Offset(10)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        assert2::assert!(c.epoch_for_offset(Offset(9)) == None);
    }

    #[test]
    fn epoch_for_offset_within_epoch_range() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        c.append(LeaderEpoch(2), Offset(100)).unwrap();
        for (offset, want, why) in [
            // Offsets 0–49 belong to epoch 0.
            (0, Some(LeaderEpoch(0)), "start of epoch 0"),
            (25, Some(LeaderEpoch(0)), "middle of epoch 0"),
            (49, Some(LeaderEpoch(0)), "last offset before epoch 1"),
            // Offsets 50–99 belong to epoch 1.
            (50, Some(LeaderEpoch(1)), "start of epoch 1"),
            (75, Some(LeaderEpoch(1)), "middle of epoch 1"),
            (99, Some(LeaderEpoch(1)), "last offset before epoch 2"),
        ] {
            check!(
                c.epoch_for_offset(Offset(offset)) == want,
                "{why} (offset={offset})"
            );
        }
    }

    #[test]
    fn epoch_for_offset_at_epoch_boundary() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        // Offset exactly at epoch 1's start_offset → belongs to epoch 1.
        assert2::assert!(c.epoch_for_offset(Offset(50)) == Some(LeaderEpoch(1)));
    }

    #[test]
    fn epoch_for_offset_past_last_entry() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        // Any offset >= 50 that extends beyond the last known epoch → epoch 1
        // (the current / latest epoch owns all subsequent offsets).
        for (_name, offset) in [("past last entry", 100), ("far past last entry", 999)] {
            assert2::assert!(c.epoch_for_offset(Offset(offset)) == Some(LeaderEpoch(1)));
        }
    }

    #[test]
    fn epoch_for_offset_single_entry_at_zero() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        assert2::assert!(c.epoch_for_offset(Offset(0)) == Some(LeaderEpoch(0)));
        assert2::assert!(c.epoch_for_offset(Offset(1000)) == Some(LeaderEpoch(0)));
    }

    // ── epoch_and_offset_for (KIP-320) ────────────────────────────────────────

    #[test]
    fn epoch_and_offset_latest_returns_pair_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        // Requested == latest recorded epoch → (epoch, log_end_offset).
        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(1), Offset(100)) == (LeaderEpoch(1), Offset(100))
        );
    }

    #[test]
    fn epoch_and_offset_older_returns_floor_epoch_and_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        c.append(LeaderEpoch(2), Offset(100)).unwrap();
        // Recorded older epoch → (epoch, start of next epoch).
        for (_name, requested, expected) in [
            ("oldest epoch", LeaderEpoch(0), (LeaderEpoch(0), Offset(50))),
            (
                "middle epoch",
                LeaderEpoch(1),
                (LeaderEpoch(1), Offset(100)),
            ),
        ] {
            assert2::assert!(c.epoch_and_offset_for(requested, Offset(200)) == expected);
        }
    }

    #[test]
    fn epoch_and_offset_gap_uses_floor_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(5), Offset(100)).unwrap();
        // Requested epoch 3 is not recorded; floor is epoch 0, next start 100.
        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(3), Offset(200)) == (LeaderEpoch(0), Offset(100))
        );
    }

    #[test]
    fn epoch_and_offset_future_epoch_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        // Requested epoch above everything recorded → (UNDEFINED, log_end).
        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(7), Offset(100)) == (UNDEFINED_EPOCH, Offset(100))
        );
    }

    #[test]
    fn epoch_and_offset_below_all_returns_requested_and_first_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(LeaderEpoch(3), Offset(30)).unwrap();
        c.append(LeaderEpoch(4), Offset(40)).unwrap();
        // Requested epoch below the first recorded epoch.
        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(1), Offset(100)) == (LeaderEpoch(1), Offset(30))
        );
    }

    #[test]
    fn epoch_and_offset_empty_cache_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(0), Offset(9)) == (UNDEFINED_EPOCH, Offset(9))
        );
    }
}
