use super::*;
#[cfg(test)]
use std::vec;

/// `Partition.isReplicaIsrEligible` and `isBrokerEpochIsrEligible`.
#[yare::parameterized(
    alive_fetch_epoch = { ELIGIBLE, true },
    fetch_without_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), ..ELIGIBLE },
        true,
    },
    previous_broker_incarnation = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(6), ..ELIGIBLE },
        false,
    },
    no_fetch_for_leader = { IsrEligibilityFacts { fetch_broker_epoch: None, ..ELIGIBLE }, false },
    no_alive_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), alive_broker_epoch: None, ..ELIGIBLE },
        false,
    },
    fenced_broker = { IsrEligibilityFacts { fenced: true, ..ELIGIBLE }, false },
    controlled_shutdown = { IsrEligibilityFacts { shutting_down: true, ..ELIGIBLE }, false },
)]
fn replica_isr_eligibility_follows_kip_841(facts: IsrEligibilityFacts, expected: bool) {
    assert2::check!(replica_isr_eligible(facts) == expected);
}

/// One candidate at a leader whose log ends at 120, whose high watermark is
/// 100, and whose current epoch began at offset 90.
fn candidate_at(
    role: IsrMemberRole,
    follower_log_end: i64,
    caught_up_within_lag: bool,
) -> IsrCandidateFacts {
    IsrCandidateFacts {
        role,
        follower_log_end,
        leader_log_end: 120,
        leader_high_watermark: 100,
        leader_epoch_start: Some(90),
        caught_up_within_lag,
        eligibility: ELIGIBLE,
    }
}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "isr tests replica isr eligibility follows kip 841: isr candidates follow kafkas shrink and expand rules for {example}"
)]
fn bdd_verify_isr_candidates_follow_kafkas_shrink_and_expand_rules(example: String) {
    type Example<'a> = (&'a str, IsrCandidateFacts, bool);
    let (label, facts, expected): Example<'_> = match example.as_str() {
        "leader" => (
            "the leader stays",
            candidate_at(IsrMemberRole::Leader, -1, false),
            true,
        ),
        "reassigned away" => (
            "a reassigned-away member leaves",
            candidate_at(IsrMemberRole::Unassigned, 120, true),
            false,
        ),
        "idle at leader end" => (
            "an idle follower at the leader's log end stays",
            candidate_at(IsrMemberRole::InSyncFollower, 120, false),
            true,
        ),
        "recently caught up" => (
            "a follower that caught up recently stays",
            candidate_at(IsrMemberRole::InSyncFollower, 50, true),
            true,
        ),
        "behind bound" => (
            "a follower behind for longer than the bound leaves",
            candidate_at(IsrMemberRole::InSyncFollower, 119, false),
            false,
        ),
        "never fetched" => (
            "a follower that never fetched from this leader leaves after the bound",
            candidate_at(IsrMemberRole::InSyncFollower, -1, false),
            false,
        ),
        "at high watermark" => (
            "a follower at the high watermark joins",
            candidate_at(IsrMemberRole::OutOfSyncFollower, 100, false),
            true,
        ),
        "short of high watermark" => (
            "a follower one short of the high watermark stays out",
            candidate_at(IsrMemberRole::OutOfSyncFollower, 99, true),
            false,
        ),
        "before epoch start" => (
            "a follower past the high watermark but before the epoch start stays out",
            IsrCandidateFacts {
                leader_epoch_start: Some(110),
                ..candidate_at(IsrMemberRole::OutOfSyncFollower, 105, true)
            },
            false,
        ),
        "unknown epoch start" => (
            "a leader that does not know its epoch start admits nobody",
            IsrCandidateFacts {
                leader_epoch_start: None,
                ..candidate_at(IsrMemberRole::OutOfSyncFollower, 120, true)
            },
            false,
        ),
        "fenced follower" => (
            "a fenced follower at the log end stays out",
            IsrCandidateFacts {
                eligibility: IsrEligibilityFacts {
                    fenced: true,
                    ..ELIGIBLE
                },
                ..candidate_at(IsrMemberRole::OutOfSyncFollower, 120, true)
            },
            false,
        ),
        "out of sync never fetched" => (
            "a follower that never fetched from this leader stays out",
            candidate_at(IsrMemberRole::OutOfSyncFollower, -1, true),
            false,
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    assert2::check!(isr_candidate_selected(facts) == expected, "{label}");
}

/// In-sync rows follow `Partition.getOutOfSyncReplicas`; out-of-sync rows
/// follow `Partition.needsExpandIsr`.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/isr/tests/replica_isr_eligibility_follows_kip_841.feature",
    name = "isr candidates follow kafkas shrink and expand rules"
)]
fn isr_candidates_follow_kafkas_shrink_and_expand_rules(example: String) { let _ = example; }
