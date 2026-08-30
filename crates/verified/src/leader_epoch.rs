//! KIP-101 leader-epoch divergence lookup kernel.
//!
//! The log crate keeps the checkpoint entries in strictly increasing epoch and
//! start-offset order. This kernel returns the epoch and offset a follower uses
//! to truncate a divergent suffix.

use creusot_std::prelude::*;
use krabka_ids::{LeaderEpoch, Offset};

const UNDEFINED_EPOCH: LeaderEpoch = LeaderEpoch(-1);

/// One leader epoch and the offset where it begins.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq, Hash))]
pub struct EpochEntry {
    pub epoch: LeaderEpoch,
    pub start_offset: Offset,
}

/// Resolve a requested leader epoch to `(found_epoch, end_offset)`.
///
/// Entries must be strictly increasing in both fields.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].epoch.0@ < entries@[j].epoch.0@
        && entries@[i].start_offset.0@ < entries@[j].start_offset.0@)]
#[ensures(requested_epoch.0@ == -1
    ==> result.0.0@ == -1 && result.1.0@ == log_end_offset.0@)]
#[ensures(entries@.len() > 0
        && requested_epoch.0@ == entries@[entries@.len() - 1].epoch.0@
    ==> result.0.0@ == requested_epoch.0@ && result.1.0@ == log_end_offset.0@)]
#[ensures(requested_epoch.0@ != -1
        && (forall<i: Int> 0 <= i && i < entries@.len()
            ==> entries@[i].epoch.0@ < requested_epoch.0@)
    ==> result.0.0@ == -1 && result.1.0@ == log_end_offset.0@)]
#[ensures(forall<i: Int> requested_epoch.0@ != -1
        && 0 <= i && i < entries@.len()
        && entries@[i].epoch.0@ > requested_epoch.0@
        && (forall<j: Int> 0 <= j && j < i
            ==> entries@[j].epoch.0@ <= requested_epoch.0@)
    ==> result.1.0@ == entries@[i].start_offset.0@
        && result.0.0@ == if i == 0 {
            requested_epoch.0@
        } else {
            entries@[i - 1].epoch.0@
        })]
#[must_use]
pub fn epoch_and_offset_for_entries(
    entries: &[EpochEntry],
    requested_epoch: LeaderEpoch,
    log_end_offset: Offset,
) -> (LeaderEpoch, Offset) {
    if requested_epoch.0 == UNDEFINED_EPOCH.0 {
        return (UNDEFINED_EPOCH, log_end_offset);
    }

    let mut i = 0usize;
    #[invariant(i@ <= entries@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> entries@[j].epoch.0@ <= requested_epoch.0@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() && entries[i].epoch.0 <= requested_epoch.0 {
        i += 1;
    }

    if i == entries.len() {
        if i > 0 && entries[i - 1].epoch.0 == requested_epoch.0 {
            (requested_epoch, log_end_offset)
        } else {
            (UNDEFINED_EPOCH, log_end_offset)
        }
    } else if i == 0 {
        (requested_epoch, entries[i].start_offset)
    } else {
        (entries[i - 1].epoch, entries[i].start_offset)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn iterator_oracle(
        entries: &[EpochEntry],
        requested_epoch: LeaderEpoch,
        log_end_offset: Offset,
    ) -> (LeaderEpoch, Offset) {
        if requested_epoch == UNDEFINED_EPOCH {
            return (UNDEFINED_EPOCH, log_end_offset);
        }
        if entries.iter().map(|entry| entry.epoch).max() == Some(requested_epoch) {
            return (requested_epoch, log_end_offset);
        }
        match entries
            .iter()
            .filter(|entry| entry.epoch > requested_epoch)
            .min_by_key(|entry| entry.epoch)
        {
            None => (UNDEFINED_EPOCH, log_end_offset),
            Some(next) => (
                entries
                    .iter()
                    .filter(|entry| entry.epoch <= requested_epoch)
                    .map(|entry| entry.epoch)
                    .max()
                    .unwrap_or(requested_epoch),
                next.start_offset,
            ),
        }
    }

    #[test]
    fn lookup_covers_kip_101_boundaries() {
        const ENTRIES: &[EpochEntry] = &[
            EpochEntry {
                epoch: LeaderEpoch(0),
                start_offset: Offset(0),
            },
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(50),
            },
            EpochEntry {
                epoch: LeaderEpoch(5),
                start_offset: Offset(100),
            },
        ];
        const LATE_START: &[EpochEntry] = &[
            EpochEntry {
                epoch: LeaderEpoch(3),
                start_offset: Offset(30),
            },
            EpochEntry {
                epoch: LeaderEpoch(4),
                start_offset: Offset(40),
            },
        ];

        for (name, entries, requested, log_end, expected) in [
            (
                "empty",
                &[][..],
                LeaderEpoch(0),
                Offset(9),
                (LeaderEpoch(-1), Offset(9)),
            ),
            (
                "undefined",
                ENTRIES,
                LeaderEpoch(-1),
                Offset(200),
                (LeaderEpoch(-1), Offset(200)),
            ),
            (
                "latest",
                ENTRIES,
                LeaderEpoch(5),
                Offset(200),
                (LeaderEpoch(5), Offset(200)),
            ),
            (
                "future",
                ENTRIES,
                LeaderEpoch(7),
                Offset(200),
                (LeaderEpoch(-1), Offset(200)),
            ),
            (
                "below first",
                LATE_START,
                LeaderEpoch(1),
                Offset(200),
                (LeaderEpoch(1), Offset(30)),
            ),
            (
                "exact older",
                ENTRIES,
                LeaderEpoch(2),
                Offset(200),
                (LeaderEpoch(2), Offset(100)),
            ),
            (
                "gap",
                ENTRIES,
                LeaderEpoch(3),
                Offset(200),
                (LeaderEpoch(2), Offset(100)),
            ),
        ] {
            assert2::check!(
                epoch_and_offset_for_entries(entries, requested, log_end) == expected,
                "case {name}"
            );
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_iterator_oracle(
            epochs in proptest::collection::btree_set(0i32..100, 0..32),
            requested in -1i32..110,
            log_end in 0i64..10_000,
        ) {
            let entries = epochs
                .into_iter()
                .enumerate()
                .map(|(i, epoch)| {
                    EpochEntry {
                        epoch: LeaderEpoch(epoch),
                        start_offset: Offset(
                            i64::try_from(i).expect("epoch set length is bounded to 32") * 10,
                        ),
                    }
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(
                epoch_and_offset_for_entries(
                    &entries,
                    LeaderEpoch(requested),
                    Offset(log_end),
                ),
                iterator_oracle(&entries, LeaderEpoch(requested), Offset(log_end))
            );
        }
    }
}
