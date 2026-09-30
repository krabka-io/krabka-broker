use super::*;

/// `Partition.isReplicaIsrEligible` and `isBrokerEpochIsrEligible`.
#[test]
fn replica_isr_eligibility_follows_kip_841() {
    for (label, facts, expected) in [
        (
            "an alive broker whose fetch carried its epoch",
            ELIGIBLE,
            true,
        ),
        (
            "a fetch that carried no epoch skips the comparison",
            IsrEligibilityFacts {
                fetch_broker_epoch: Some(-1),
                ..ELIGIBLE
            },
            true,
        ),
        (
            "a fetch from the broker's previous incarnation",
            IsrEligibilityFacts {
                fetch_broker_epoch: Some(6),
                ..ELIGIBLE
            },
            false,
        ),
        (
            "no fetch reached this leader yet",
            IsrEligibilityFacts {
                fetch_broker_epoch: None,
                ..ELIGIBLE
            },
            false,
        ),
        (
            "a broker the metadata cache holds no alive epoch for",
            IsrEligibilityFacts {
                fetch_broker_epoch: Some(-1),
                alive_broker_epoch: None,
                ..ELIGIBLE
            },
            false,
        ),
        (
            "a fenced broker",
            IsrEligibilityFacts {
                fenced: true,
                ..ELIGIBLE
            },
            false,
        ),
        (
            "a broker in controlled shutdown",
            IsrEligibilityFacts {
                shutting_down: true,
                ..ELIGIBLE
            },
            false,
        ),
    ] {
        assert2::check!(replica_isr_eligible(facts) == expected, "{label}");
    }
}

/// One scan of a leader whose log ends at 120, whose high watermark is
/// 100, and whose current epoch began at offset 90. In-sync rows follow
/// `Partition.getOutOfSyncReplicas`, out-of-sync rows
/// `Partition.needsExpandIsr`.
#[test]
fn isr_candidates_follow_kafkas_shrink_and_expand_rules() {
    use IsrMemberRole::{InSyncFollower, Leader, OutOfSyncFollower, Unassigned};
    let at = |role, follower_log_end, caught_up_within_lag| IsrCandidateFacts {
        role,
        follower_log_end,
        leader_log_end: 120,
        leader_high_watermark: 100,
        leader_epoch_start: Some(90),
        caught_up_within_lag,
        eligibility: ELIGIBLE,
    };
    for (label, facts, expected) in [
        ("the leader stays", at(Leader, -1, false), true),
        (
            "a reassigned-away member leaves",
            at(Unassigned, 120, true),
            false,
        ),
        (
            "an idle follower at the leader's log end stays",
            at(InSyncFollower, 120, false),
            true,
        ),
        (
            "a follower that caught up recently stays",
            at(InSyncFollower, 50, true),
            true,
        ),
        (
            "a follower behind for longer than the bound leaves",
            at(InSyncFollower, 119, false),
            false,
        ),
        (
            "a follower that never fetched from this leader leaves after the bound",
            at(InSyncFollower, -1, false),
            false,
        ),
        (
            "a follower at the high watermark joins",
            at(OutOfSyncFollower, 100, false),
            true,
        ),
        (
            "a follower one short of the high watermark stays out",
            at(OutOfSyncFollower, 99, true),
            false,
        ),
        (
            "a follower past the high watermark but before the epoch start stays out",
            IsrCandidateFacts {
                leader_epoch_start: Some(110),
                ..at(OutOfSyncFollower, 105, true)
            },
            false,
        ),
        (
            "a leader that does not know its epoch start admits nobody",
            IsrCandidateFacts {
                leader_epoch_start: None,
                ..at(OutOfSyncFollower, 120, true)
            },
            false,
        ),
        (
            "a fenced follower at the log end stays out",
            IsrCandidateFacts {
                eligibility: IsrEligibilityFacts {
                    fenced: true,
                    ..ELIGIBLE
                },
                ..at(OutOfSyncFollower, 120, true)
            },
            false,
        ),
        (
            "a follower that never fetched from this leader stays out",
            at(OutOfSyncFollower, -1, true),
            false,
        ),
    ] {
        assert2::check!(isr_candidate_selected(facts) == expected, "{label}");
    }
    assert2::check!(!isr_proposal_changed(0, 0));
    assert2::check!(isr_proposal_changed(1, 0));
    assert2::check!(isr_proposal_changed(0, 1));
    assert2::check!(isr_proposal_changed(usize::MAX, usize::MAX));
}
