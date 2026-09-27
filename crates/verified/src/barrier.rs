//! Barrier target, marker-fence, and cut-classification decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

/// The result of adding one topic's partitions to a frozen target count.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierTargetCountDecision {
    Malformed,
    Overflow,
    Expand { next: i64 },
}

/// Add a topic's exact positive partition count without wrapping.
#[ensures(match result {
    BarrierTargetCountDecision::Malformed => partition_count@ <= 0,
    BarrierTargetCountDecision::Overflow => partition_count@ > 0
        && total@ > i64::MAX@ - partition_count@,
    BarrierTargetCountDecision::Expand { next } => partition_count@ > 0
        && total@ <= i64::MAX@ - partition_count@
        && next@ == total@ + partition_count@,
})]
#[must_use]
pub fn barrier_target_count_decision(
    total: i64,
    partition_count: i32,
) -> BarrierTargetCountDecision {
    if partition_count <= 0 {
        return BarrierTargetCountDecision::Malformed;
    }
    let count = i64::from(partition_count);
    match total.checked_add(count) {
        Some(next) => BarrierTargetCountDecision::Expand { next },
        None => BarrierTargetCountDecision::Overflow,
    }
}

/// Facts that bind a marker append to one installed leadership generation.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct BarrierMarkerFenceFacts {
    pub image_present: bool,
    pub expected_leader: u64,
    pub expected_epoch: i32,
    pub image_leader: u64,
    pub image_epoch: i32,
    pub current_leader: u64,
    pub current_epoch: i32,
}

/// Whether one marker append is admitted by the leader and epoch fence.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierMarkerFenceDecision {
    Malformed,
    NotLeader,
    FencedEpoch,
    Append,
}

/// The marker fence, in the order its refusals rank: facts that name no
/// leadership generation at all (no image entry, or a negative epoch) are
/// `Malformed`; a leader other than the expected one in the image or in the
/// installed partition is `NotLeader`; a matching leader at another epoch is
/// `FencedEpoch`; and only an exact match of all three is `Append`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn barrier_marker_fence_model(facts: BarrierMarkerFenceFacts) -> BarrierMarkerFenceDecision {
    pearlite! {
        if !facts.image_present
            || facts.expected_epoch@ < 0
            || facts.image_epoch@ < 0
            || facts.current_epoch@ < 0
        {
            BarrierMarkerFenceDecision::Malformed
        } else if facts.image_leader != facts.expected_leader
            || facts.current_leader != facts.expected_leader
        {
            BarrierMarkerFenceDecision::NotLeader
        } else if facts.image_epoch != facts.expected_epoch
            || facts.current_epoch != facts.expected_epoch
        {
            BarrierMarkerFenceDecision::FencedEpoch
        } else {
            BarrierMarkerFenceDecision::Append
        }
    }
}

/// Require the metadata image and installed partition to name exactly the
/// expected leader generation; see `barrier_marker_fence_model` for which
/// refusal each mismatch gets. The host maps the refusals to different wire
/// codes, so every variant is pinned.
#[ensures(result == barrier_marker_fence_model(facts))]
#[must_use]
pub fn barrier_marker_fence_decision(facts: BarrierMarkerFenceFacts) -> BarrierMarkerFenceDecision {
    if !facts.image_present
        || facts.expected_epoch < 0
        || facts.image_epoch < 0
        || facts.current_epoch < 0
    {
        return BarrierMarkerFenceDecision::Malformed;
    }
    if facts.image_leader != facts.expected_leader || facts.current_leader != facts.expected_leader
    {
        return BarrierMarkerFenceDecision::NotLeader;
    }
    if facts.image_epoch != facts.expected_epoch || facts.current_epoch != facts.expected_epoch {
        return BarrierMarkerFenceDecision::FencedEpoch;
    }
    BarrierMarkerFenceDecision::Append
}

/// Whether one successful marker response may enter the cut.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierPlacementDecision {
    Reject,
    Accept,
}

#[ensures((result == BarrierPlacementDecision::Accept) == (requested && offset@ >= 0))]
#[must_use]
pub fn barrier_placement_decision(requested: bool, offset: i64) -> BarrierPlacementDecision {
    if requested && offset >= 0 {
        BarrierPlacementDecision::Accept
    } else {
        BarrierPlacementDecision::Reject
    }
}

/// The status derived from the exact missing-target set.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum BarrierCutClassification {
    Complete,
    Partial,
}

#[ensures((result == BarrierCutClassification::Complete) == !has_missing)]
#[must_use]
pub fn barrier_cut_classification(has_missing: bool) -> BarrierCutClassification {
    if has_missing {
        BarrierCutClassification::Partial
    } else {
        BarrierCutClassification::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BarrierCutClassification, BarrierMarkerFenceDecision, BarrierMarkerFenceFacts,
        BarrierPlacementDecision, BarrierTargetCountDecision, barrier_cut_classification,
        barrier_marker_fence_decision, barrier_placement_decision, barrier_target_count_decision,
    };

    #[test]
    fn target_counts_are_positive_exact_and_overflow_safe() {
        assert2::check!(
            barrier_target_count_decision(4, 3) == BarrierTargetCountDecision::Expand { next: 7 }
        );
        assert2::check!(
            barrier_target_count_decision(4, 0) == BarrierTargetCountDecision::Malformed
        );
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
}
