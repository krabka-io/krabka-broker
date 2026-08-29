//! Property-based coverage of the KIP-101/320 truncation contract at a scale
//! the exhaustive `leader_epoch_model` cannot reach.

use krabka_ids::{LeaderEpoch, Offset};
use proptest::prelude::*;

use super::{EpochEntry, UNDEFINED_EPOCH, append_to, epoch_and_offset_for_entries};

/// Fold random `(epoch_gap, offset_jump)` steps into a strictly-increasing
/// leader epoch-history. Gaps are allowed, as they are when `append` builds
/// one.
fn leader_history(steps: &[(i32, i64)]) -> Vec<EpochEntry> {
    let mut v: Vec<EpochEntry> = vec![];
    let (mut le, mut lo) = (-1i32, -1i64);
    for &(de, doff) in steps {
        let e = le + 1 + de.rem_euclid(3); // epoch gap 1..=3
        let o = lo + 1 + doff.rem_euclid(1000); // offset jump 1..=1000
        append_to(&mut v, LeaderEpoch(e), Offset(o));
        le = e;
        lo = o;
    }
    v
}

proptest! {
    /// Large-N randomized leader epoch-histories, requested epoch, and
    /// follower log-end. The test asserts the same KIP-101/320 truncation
    /// contract that the exhaustive `leader_epoch_model` checks, at a scale
    /// the BFS cannot reach: histories up to 20 entries, epochs to about
    /// 60, and offsets to about 20000.
    #[test]
    fn truncation_contract_holds(
        steps in proptest::collection::vec((0i32..10, 0i64..1000), 0..20usize),
        requested in -1i32..70,
        dleo in 0i64..2000,
    ) {
        // `requested` is generated as a raw `i32` (the KIP-320 wire type);
        // wrap it into the domain newtype for the call and every comparison.
        let requested = LeaderEpoch(requested);
        let leader = leader_history(&steps);
        let last_off: i64 = leader.last().map_or(0, |e| e.start_offset.0);
        // Follower log end is at or past the last epoch boundary.
        let leo = last_off + 1 + dleo;
        let (found, trunc) = epoch_and_offset_for_entries(&leader, requested, Offset(leo));
        let latest = leader.iter().map(|e| e.epoch).max();

        // Always a valid truncation target.
        prop_assert!(trunc >= 0, "truncation target {} < 0", trunc);
        // The resolved epoch never exceeds the requested epoch.
        prop_assert!(
            found <= requested,
            "found_epoch {} > requested {}",
            found,
            requested
        );

        if let Some(entry) = leader.iter().find(|e| e.epoch == requested) {
            // Committed-prefix-preserved: never truncate below the start of
            // an epoch the leader and follower agree on.
            prop_assert!(
                trunc >= entry.start_offset,
                "truncation {} dropped agreed epoch {} (starts at {})",
                trunc,
                requested,
                entry.start_offset
            );
            if latest == Some(requested) {
                // Current epoch → keep up to the follower's log end.
                prop_assert_eq!(found, requested);
                prop_assert_eq!(trunc, Offset(leo), "latest epoch keeps up to log end");
            } else {
                // Older agreed epoch → truncate to the next leader epoch's
                // start, dropping the divergent higher-epoch suffix.
                let next_start = leader
                    .iter()
                    .filter(|e| e.epoch > requested)
                    .map(|e| e.start_offset)
                    .min()
                    .expect("a non-latest recorded epoch has a higher epoch");
                prop_assert_eq!(found, requested);
                prop_assert_eq!(
                    trunc,
                    next_start,
                    "older epoch truncates to next epoch start"
                );
                prop_assert!(trunc <= leo, "truncation {} above log end {}", trunc, leo);
            }
        } else if requested == UNDEFINED_EPOCH {
            // No last epoch → no truncation this round.
            prop_assert_eq!(found, UNDEFINED_EPOCH);
            prop_assert_eq!(trunc, Offset(leo));
        }
    }
}
