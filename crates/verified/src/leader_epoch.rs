//! KIP-101 leader-epoch divergence lookup kernel.
//!
//! The log crate keeps the checkpoint entries in strictly increasing epoch and
//! start-offset order. This kernel returns the epoch and offset a follower uses
//! to truncate a divergent suffix.

use creusot_std::prelude::*;

const UNDEFINED_EPOCH: i32 = -1;

/// Resolve a requested leader epoch to `(found_epoch, end_offset)`.
///
/// Each entry is `(leader_epoch, start_offset)`. Entries must be strictly
/// increasing in both fields.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@ && entries@[i].1@ < entries@[j].1@)]
#[ensures(requested_epoch@ == -1
    ==> result.0@ == -1 && result.1@ == log_end_offset@)]
#[ensures(entries@.len() > 0
        && requested_epoch@ == entries@[entries@.len() - 1].0@
    ==> result.0@ == requested_epoch@ && result.1@ == log_end_offset@)]
#[ensures(requested_epoch@ != -1
        && (forall<i: Int> 0 <= i && i < entries@.len()
            ==> entries@[i].0@ < requested_epoch@)
    ==> result.0@ == -1 && result.1@ == log_end_offset@)]
#[ensures(forall<i: Int> requested_epoch@ != -1
        && 0 <= i && i < entries@.len()
        && entries@[i].0@ > requested_epoch@
        && (forall<j: Int> 0 <= j && j < i ==> entries@[j].0@ <= requested_epoch@)
    ==> result.1@ == entries@[i].1@
        && result.0@ == if i == 0 { requested_epoch@ } else { entries@[i - 1].0@ })]
#[must_use]
pub fn epoch_and_offset_for_entries(
    entries: &[(i32, i64)],
    requested_epoch: i32,
    log_end_offset: i64,
) -> (i32, i64) {
    if requested_epoch == UNDEFINED_EPOCH {
        return (UNDEFINED_EPOCH, log_end_offset);
    }

    let mut i = 0usize;
    #[invariant(i@ <= entries@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> entries@[j].0@ <= requested_epoch@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() && entries[i].0 <= requested_epoch {
        i += 1;
    }

    if i == entries.len() {
        if i > 0 && entries[i - 1].0 == requested_epoch {
            (requested_epoch, log_end_offset)
        } else {
            (UNDEFINED_EPOCH, log_end_offset)
        }
    } else if i == 0 {
        (requested_epoch, entries[i].1)
    } else {
        (entries[i - 1].0, entries[i].1)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn iterator_oracle(
        entries: &[(i32, i64)],
        requested_epoch: i32,
        log_end_offset: i64,
    ) -> (i32, i64) {
        if requested_epoch == UNDEFINED_EPOCH {
            return (UNDEFINED_EPOCH, log_end_offset);
        }
        if entries.iter().map(|entry| entry.0).max() == Some(requested_epoch) {
            return (requested_epoch, log_end_offset);
        }
        match entries
            .iter()
            .filter(|entry| entry.0 > requested_epoch)
            .min_by_key(|entry| entry.0)
        {
            None => (UNDEFINED_EPOCH, log_end_offset),
            Some(next) => (
                entries
                    .iter()
                    .filter(|entry| entry.0 <= requested_epoch)
                    .map(|entry| entry.0)
                    .max()
                    .unwrap_or(requested_epoch),
                next.1,
            ),
        }
    }

    #[test]
    fn lookup_covers_kip_101_boundaries() {
        const ENTRIES: &[(i32, i64)] = &[(0, 0), (2, 50), (5, 100)];
        const LATE_START: &[(i32, i64)] = &[(3, 30), (4, 40)];

        for (name, entries, requested, log_end, expected) in [
            ("empty", &[][..], 0, 9, (-1, 9)),
            ("undefined", ENTRIES, -1, 200, (-1, 200)),
            ("latest", ENTRIES, 5, 200, (5, 200)),
            ("future", ENTRIES, 7, 200, (-1, 200)),
            ("below first", LATE_START, 1, 200, (1, 30)),
            ("exact older", ENTRIES, 2, 200, (2, 100)),
            ("gap", ENTRIES, 3, 200, (2, 100)),
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
                    (
                        epoch,
                        i64::try_from(i).expect("epoch set length is bounded to 32") * 10,
                    )
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(
                epoch_and_offset_for_entries(&entries, requested, log_end),
                iterator_oracle(&entries, requested, log_end)
            );
        }
    }
}
