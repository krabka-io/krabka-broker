use super::*;

/// The leader's own log end counts toward a candidate's majority only when
/// it actually reaches that candidate.
///
/// `count < majority` guards the increment so the tally saturates, which
/// makes it invisible on its own -- the answer is `count >= majority`
/// either way. What is visible is joining the two conditions with `||`
/// instead of `&&`: an empty tally satisfies `count < majority` for any
/// majority, so the leader would be counted for a candidate its log has
/// not reached, and one follower would look like two.
#[test]
fn leader_counts_toward_a_candidate_only_when_its_log_reaches_it() {
    // One follower at 5 and a leader at 0, needing two of the three.
    check!(!candidate_has_majority(0, &[5], 5, 2, true));
    // The same shape with the leader's log at the candidate: now it counts.
    check!(candidate_has_majority(5, &[5], 5, 2, true));
}

proptest! {
    #[test]
    fn hwm_matches_sort_oracle(
        log_end in 0i64..1_000,
        followers in proptest::collection::vec(0i64..1_000, 1..7),
        majority_seed in 0usize..8,
        epoch_start_offset in 0i64..1_000,
        current_hwm in 0i64..1_000,
        leader_counts in any::<bool>(),
    ) {
        let majority = 1 + majority_seed % (followers.len() + usize::from(leader_counts));
        // Kernel precondition domain: clamp like the kraft-core call site does.
        let followers: Vec<i64> = followers.iter().map(|o| (*o).min(log_end)).collect();
        let current_hwm = current_hwm.min(log_end);
        prop_assert_eq!(
            recompute_high_watermark(
                log_end,
                &followers,
                majority,
                epoch_start_offset,
                current_hwm,
                leader_counts,
            ),
            hwm_sort_oracle(
                log_end,
                &followers,
                majority,
                epoch_start_offset,
                current_hwm,
                leader_counts,
            )
        );
    }

    #[test]
    fn jitter_in_range(me in any::<u64>(), epoch in any::<u32>(), base in 1u64..10_000) {
        prop_assert!(election_jitter_ms(me, epoch, base) < base);
    }
}

#[test]
fn jitter_zero_base_is_zero() {
    assert2::assert!(election_jitter_ms(7, 3, 0) == 0);
}

#[test]
fn jitter_uses_node_and_epoch_hash_inputs() {
    for (_name, node, epoch, expected) in [
        ("node one epoch zero", 1, 0, 485),
        ("node two epoch zero", 2, 0, 354),
        ("node one epoch one", 1, 1, 446),
    ] {
        assert2::assert!(election_jitter_ms(node, epoch, 1000) == expected);
    }
}

#[test]
fn up_to_date_is_the_kip595_rule() {
    // higher epoch wins regardless of offset
    for (name, ours_epoch, ours_offset, candidate_epoch, candidate_offset, expected) in [
        ("higher epoch", 5, 100, 6, 0, true),
        ("same epoch equal offset", 5, 100, 5, 100, true),
        ("same epoch older offset", 5, 100, 5, 99, false),
        ("lower epoch", 5, 0, 4, i64::MAX, false),
    ] {
        check!(
            log_is_up_to_date(ours_epoch, ours_offset, candidate_epoch, candidate_offset)
                == expected,
            "case {name}"
        );
    }
}

#[test]
fn election_quorum_is_a_strict_majority() {
    for (voters, grants, expected) in [
        (1, 1, true),
        (2, 1, false),
        (2, 2, true),
        (3, 1, false),
        (3, 2, true),
        (4, 2, false),
        (4, 3, true),
    ] {
        check!(election_has_quorum(voters, grants) == expected);
    }
}

#[test]
fn hwm_never_regresses_and_gates_on_epoch_start() {
    // majority offset (2 of {10, 3, 9} with majority=2 -> 9) is <= epoch_start 9: hold.
    for (name, followers, epoch_start, current, expected) in [
        ("gated at epoch start", &[3, 9][..], 9, 5, 5),
        ("advances past epoch start", &[3, 9][..], 8, 5, 9),
        ("never regresses", &[1, 1][..], 0, 7, 7),
    ] {
        check!(
            recompute_high_watermark(10, followers, 2, epoch_start, current, true) == expected,
            "case {name}"
        );
    }
}

#[test]
fn hwm_counts_leader_and_followers_until_majority() {
    for (name, followers, majority, expected) in [
        ("two of three", &[9, 8][..], 2, 9),
        ("all three at leader", &[10, 10][..], 3, 10),
        ("all three below leader", &[4, 4][..], 3, 4),
    ] {
        check!(
            recompute_high_watermark(10, followers, majority, 0, 0, true) == expected,
            "case {name}"
        );
    }
}

/// A three-voter WAL quorum: the leader's log end is one vote and a
/// majority is two, so the watermark is the higher follower ack once it
/// passes the current watermark, and the current watermark otherwise.
#[test]
fn majority_watermark_follows_the_second_highest_ack() {
    for (name, log_end, followers, current, expected) in [
        ("no follower has acked", 10, &[0, 0][..], 0, 0),
        ("one follower ack makes a majority", 10, &[7, 0][..], 0, 7),
        ("the higher of two acks wins", 10, &[4, 9][..], 0, 9),
        ("an ack at the leader end", 10, &[10, 3][..], 5, 10),
        ("a stale ack never lowers it", 10, &[3, 2][..], 5, 5),
        ("an empty log", 0, &[0, 0][..], 0, 0),
    ] {
        check!(
            majority_watermark(log_end, followers, 2, current) == expected,
            "case {name}"
        );
    }
}

#[test]
fn hwm_can_exclude_a_removed_leader() {
    check!(recompute_high_watermark(10, &[9, 4], 2, 0, 0, true) == 9);
    check!(recompute_high_watermark(10, &[9, 4], 2, 0, 0, false) == 4);
}
