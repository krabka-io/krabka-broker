//! Barrier target, marker-fence, and cut-classification decisions.

#[cfg(creusot)]
use std::clone::Clone;

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
use creusot_std::prelude::ensures;

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

/// Require the metadata image and installed partition to name exactly the
/// expected leader generation.
#[ensures((result == BarrierMarkerFenceDecision::Append) == (
    facts.image_present
        && facts.expected_epoch@ >= 0
        && facts.image_epoch@ >= 0
        && facts.current_epoch@ >= 0
        && facts.image_leader@ == facts.expected_leader@
        && facts.current_leader@ == facts.expected_leader@
        && facts.image_epoch@ == facts.expected_epoch@
        && facts.current_epoch@ == facts.expected_epoch@
))]
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
    fn marker_fencing_requires_one_exact_leadership_generation() {
        let admitted = BarrierMarkerFenceFacts {
            image_present: true,
            expected_leader: 2,
            expected_epoch: 7,
            image_leader: 2,
            image_epoch: 7,
            current_leader: 2,
            current_epoch: 7,
        };
        assert2::check!(
            barrier_marker_fence_decision(admitted) == BarrierMarkerFenceDecision::Append
        );
        for rejected in [
            BarrierMarkerFenceFacts {
                expected_epoch: -1,
                ..admitted
            },
            BarrierMarkerFenceFacts {
                image_leader: 3,
                ..admitted
            },
            BarrierMarkerFenceFacts {
                current_epoch: 8,
                ..admitted
            },
        ] {
            assert2::check!(
                barrier_marker_fence_decision(rejected) != BarrierMarkerFenceDecision::Append
            );
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
