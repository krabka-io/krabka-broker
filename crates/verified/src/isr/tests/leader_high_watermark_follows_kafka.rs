use super::*;

/// `Partition.maybeIncrementLeaderHW` for a leader at log end 100 and
/// high watermark 40, with three assigned replicas and
/// `min.insync.replicas` 2 unless a row says otherwise.
#[test]
fn leader_high_watermark_follows_kafka() {
    let facts = |isr_size, effective_min_isr| HighWatermarkFacts {
        current: 40,
        leader_log_end: 100,
        isr_size,
        effective_min_isr,
    };
    let in_isr = |log_end| HwmReplica {
        log_end,
        in_isr: true,
        caught_up_within_lag: false,
        eligibility: ELIGIBLE,
    };
    let outside = |log_end, caught_up_within_lag, eligibility| HwmReplica {
        log_end,
        in_isr: false,
        caught_up_within_lag,
        eligibility,
    };
    let fenced = IsrEligibilityFacts {
        fenced: true,
        ..ELIGIBLE
    };
    let cases: [(&str, HighWatermarkFacts, &[HwmReplica], i64); 10] = [
        (
            "the slowest in-sync follower sets the watermark",
            facts(3, 2),
            &[in_isr(80), in_isr(60)],
            60,
        ),
        (
            "a leader alone in its ISR advances to its log end",
            facts(1, 1),
            &[outside(10, false, ELIGIBLE), outside(20, false, ELIGIBLE)],
            100,
        ),
        (
            "under min ISR the watermark does not move",
            facts(1, 2),
            &[outside(100, true, ELIGIBLE), outside(100, true, ELIGIBLE)],
            40,
        ),
        (
            "exactly at min ISR it moves",
            facts(2, 2),
            &[in_isr(70), outside(10, false, ELIGIBLE)],
            70,
        ),
        (
            "a caught-up eligible replica outside the ISR holds it back",
            facts(2, 2),
            &[in_isr(90), outside(55, true, ELIGIBLE)],
            55,
        ),
        (
            "a replica outside the ISR that has not caught up does not",
            facts(2, 2),
            &[in_isr(90), outside(55, false, ELIGIBLE)],
            90,
        ),
        (
            "a caught-up fenced replica outside the ISR does not",
            facts(2, 2),
            &[in_isr(90), outside(55, true, fenced)],
            90,
        ),
        (
            "an ISR member that has not fetched from this leader holds it",
            facts(3, 2),
            &[in_isr(90), in_isr(-1)],
            40,
        ),
        (
            "the watermark never falls",
            facts(3, 2),
            &[in_isr(90), in_isr(30)],
            40,
        ),
        (
            "an ISR member at the log end with nothing else caught up",
            facts(2, 2),
            &[in_isr(100), outside(0, false, ELIGIBLE)],
            100,
        ),
    ];
    for (label, facts, replicas, expected) in cases {
        assert2::check!(
            leader_high_watermark(facts, replicas) == expected,
            "{label}"
        );
    }
}

/// The controller holds leader epoch 5 and partition epoch 10 for a
/// partition led by the requester. Each row is one `AlterPartition` a
/// Kafka controller answers as `expected`, per
/// `ReplicationControlManager.validateAlterPartitionData`.
#[test]
fn isr_admission_follows_kafkas_alter_partition_validation() {
    use IsrAdmission::{
        Admit, FencedLeaderEpoch, IneligibleReplica, InvalidRequest, InvalidUpdateVersion,
        NotController,
    };
    let current = AlterPartitionFacts {
        request_leader_epoch: 5,
        current_leader_epoch: 5,
        request_partition_epoch: 10,
        current_partition_epoch: 10,
        requester_is_leader: true,
        proposed_isr: ProposedIsr::Valid,
        recovery_state_valid: true,
        replicas_eligible: true,
    };
    let cases = [
        ("an up-to-date shrink", current, Admit),
        (
            "a newer leader epoch than the controller has seen",
            AlterPartitionFacts {
                request_leader_epoch: 6,
                ..current
            },
            NotController,
        ),
        (
            "a newer partition epoch than the controller has seen",
            AlterPartitionFacts {
                request_partition_epoch: 11,
                ..current
            },
            NotController,
        ),
        (
            "a newer partition epoch outranks a fenced leader epoch",
            AlterPartitionFacts {
                request_leader_epoch: 4,
                request_partition_epoch: 11,
                ..current
            },
            NotController,
        ),
        (
            "a leader from an older epoch",
            AlterPartitionFacts {
                request_leader_epoch: 4,
                request_partition_epoch: 9,
                requester_is_leader: false,
                ..current
            },
            FencedLeaderEpoch,
        ),
        (
            "a request from a broker that is not the leader",
            AlterPartitionFacts {
                requester_is_leader: false,
                request_partition_epoch: 9,
                ..current
            },
            InvalidRequest,
        ),
        (
            "a proposal built on an ISR the controller has since replaced",
            AlterPartitionFacts {
                request_partition_epoch: 9,
                proposed_isr: ProposedIsr::Invalid,
                replicas_eligible: false,
                ..current
            },
            InvalidUpdateVersion,
        ),
        (
            "an ISR naming an unassigned replica",
            AlterPartitionFacts {
                proposed_isr: ProposedIsr::Invalid,
                replicas_eligible: false,
                ..current
            },
            InvalidRequest,
        ),
        (
            "an ISR that drops the leader",
            AlterPartitionFacts {
                proposed_isr: ProposedIsr::WithoutLeader,
                ..current
            },
            InvalidRequest,
        ),
        (
            "a recovering leader proposing two members, one ineligible",
            AlterPartitionFacts {
                recovery_state_valid: false,
                replicas_eligible: false,
                ..current
            },
            InvalidRequest,
        ),
        (
            "a fenced broker in the proposed ISR",
            AlterPartitionFacts {
                replicas_eligible: false,
                ..current
            },
            IneligibleReplica,
        ),
    ];
    for (label, facts, expected) in cases {
        assert2::check!(isr_admission(facts) == expected, "{label}");
    }
}
