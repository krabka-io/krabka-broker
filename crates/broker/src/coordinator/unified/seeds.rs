//! Hydration seeds that carry a group's replayed state from bootstrap into a
//! freshly-spawned actor.
//!
//! One seed type exists per group protocol, and each is the exact projection
//! of that protocol's `__consumer_offsets` records. They live together here
//! because both the replay paths and the registry that spawns the actors
//! depend on them.

use super::{persistence_next_gen, share, streams};

/// Hydration seed that the bootstrap replayer passes into a freshly-spawned
/// [`actor::GroupActorHandle`].
///
/// All fields come directly from records decoded out of `__consumer_offsets`.
///
/// [`actor::GroupActorHandle`]: super::actor::GroupActorHandle
#[derive(Debug, Default, Clone, PartialEq)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence_next_gen::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence_next_gen::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence_next_gen::CurrentMemberAssignmentValue>,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`].
///
/// All fields come from share-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ShareGroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members:
        std::collections::HashMap<String, share::persistence::ShareGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupCurrentMemberAssignmentValue,
    >,
    /// KIP-932 `ShareGroupStatePartitionMetadata`, key v14. It holds the
    /// `(topic_id, partition)` share-states this group has already
    /// initialized, and the topic ids whose share-state the broker deletes.
    /// The lifecycle hook can then skip a re-initialization of those
    /// partitions on restart.
    pub state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue,
}

/// Hydration seed for a [`streams::actor::StreamsGroupActorHandle`], per
/// KIP-1071.
///
/// All fields come from streams-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StreamsGroupSeed {
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology: Option<streams::persistence::StreamsGroupTopologyValue>,
    pub partition_metadata: Option<streams::persistence::StreamsGroupPartitionMetadataValue>,
    pub members:
        std::collections::HashMap<String, streams::persistence::StreamsGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    >,
}
