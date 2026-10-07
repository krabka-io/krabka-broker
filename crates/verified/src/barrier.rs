//! Barrier target, marker-fence, and cut-classification decisions.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// The result of adding one topic's partitions to a frozen target count.
    pub enum BarrierTargetCountDecision {
        Malformed,
        Overflow,
        Expand { next: i64 },
    }

    /// Facts that bind a marker append to one installed leadership generation.
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
    pub enum BarrierMarkerFenceDecision {
        Malformed,
        NotLeader,
        FencedEpoch,
        Append,
    }

    /// Whether one successful marker response may enter the cut.
    pub enum BarrierPlacementDecision {
        Reject,
        Accept,
    }

    /// The status derived from the exact missing-target set.
    pub enum BarrierCutClassification {
        Complete,
        Partial,
    }
}

mod cut_classification;
pub use cut_classification::{
    barrier_cut_classification, barrier_marker_fence_decision, barrier_placement_decision,
    barrier_target_count_decision,
};

#[cfg(test)]
mod tests;
