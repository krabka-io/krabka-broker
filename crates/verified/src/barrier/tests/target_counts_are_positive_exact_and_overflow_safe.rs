use super::*;

#[test]
fn target_counts_are_positive_exact_and_overflow_safe() {
    assert2::check!(
        barrier_target_count_decision(4, 3) == BarrierTargetCountDecision::Expand { next: 7 }
    );
    assert2::check!(barrier_target_count_decision(4, 0) == BarrierTargetCountDecision::Malformed);
    assert2::check!(
        barrier_target_count_decision(i64::MAX, 1) == BarrierTargetCountDecision::Overflow
    );
}

#[test]
fn marker_fencing_ranks_malformed_then_leader_then_epoch() {
    use BarrierMarkerFenceDecision::{Append, FencedEpoch, Malformed, NotLeader};

    let admitted = BarrierMarkerFenceFacts {
        image_present: true,
        expected_leader: 2,
        expected_epoch: 7,
        image_leader: 2,
        image_epoch: 7,
        current_leader: 2,
        current_epoch: 7,
    };
    for (what, facts, expected) in [
        ("one exact generation", admitted, Append),
        (
            "epoch zero is a generation",
            BarrierMarkerFenceFacts {
                expected_epoch: 0,
                image_epoch: 0,
                current_epoch: 0,
                ..admitted
            },
            Append,
        ),
        (
            "no image entry",
            BarrierMarkerFenceFacts {
                image_present: false,
                ..admitted
            },
            Malformed,
        ),
        (
            "a negative expected epoch",
            BarrierMarkerFenceFacts {
                expected_epoch: -1,
                ..admitted
            },
            Malformed,
        ),
        (
            "a negative image epoch",
            BarrierMarkerFenceFacts {
                image_epoch: -1,
                ..admitted
            },
            Malformed,
        ),
        (
            "a negative installed epoch",
            BarrierMarkerFenceFacts {
                current_epoch: -1,
                ..admitted
            },
            Malformed,
        ),
        (
            "a malformed epoch outranks another leader",
            BarrierMarkerFenceFacts {
                current_epoch: -1,
                image_leader: 3,
                ..admitted
            },
            Malformed,
        ),
        (
            "the image names another leader",
            BarrierMarkerFenceFacts {
                image_leader: 3,
                ..admitted
            },
            NotLeader,
        ),
        (
            "another leader is installed",
            BarrierMarkerFenceFacts {
                current_leader: 3,
                ..admitted
            },
            NotLeader,
        ),
        (
            "another leader outranks another epoch",
            BarrierMarkerFenceFacts {
                current_leader: 3,
                current_epoch: 8,
                ..admitted
            },
            NotLeader,
        ),
        (
            "the image is at another epoch",
            BarrierMarkerFenceFacts {
                image_epoch: 8,
                ..admitted
            },
            FencedEpoch,
        ),
        (
            "the installed partition is at another epoch",
            BarrierMarkerFenceFacts {
                current_epoch: 6,
                ..admitted
            },
            FencedEpoch,
        ),
    ] {
        assert2::check!(barrier_marker_fence_decision(facts) == expected, "{what}");
    }
}

#[test]
fn only_requested_nonnegative_placements_enter_a_cut() {
    assert2::check!(barrier_placement_decision(true, 0) == BarrierPlacementDecision::Accept);
    assert2::check!(barrier_placement_decision(false, 4) == BarrierPlacementDecision::Reject);
    assert2::check!(barrier_placement_decision(true, -1) == BarrierPlacementDecision::Reject);
}

#[test]
fn a_cut_is_complete_exactly_when_nothing_is_missing() {
    assert2::check!(barrier_cut_classification(false) == BarrierCutClassification::Complete);
    assert2::check!(barrier_cut_classification(true) == BarrierCutClassification::Partial);
}
