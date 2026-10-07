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
    /// The `ConsumerGroupRegularExpression` records: what each regular
    /// expression that the members subscribe to resolved to, by regular
    /// expression.
    pub resolved_regexes:
        std::collections::HashMap<String, persistence_next_gen::RegularExpressionValue>,
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
    /// KIP-932 `ShareGroupStatePartitionMetadata`, key v15. It holds the
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
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    /// The KIP-1331 description epochs of the last group metadata record.
    pub description_epochs: streams::persistence::DescriptionEpochs,
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

/// Snapshot member metadata, current assignments, then the protocol's target map.
macro_rules! snapshot_member_maps {
    ($state:ident; $members:ident, $current:ident, $targets:ident;
        $metadata:path, $assignment:path; |$id:ident, $member:ident| $target:block
    ) => {
        let mut $members = ::std::collections::HashMap::new();
        let mut $targets = ::std::collections::HashMap::new();
        let mut $current = ::std::collections::HashMap::new();
        for ($id, $member) in &$state.members {
            $members.insert($id.clone(), $metadata($member));
            $current.insert($id.clone(), $assignment($member));
            $target
        }
    };
}
pub(super) use snapshot_member_maps;

/// Restore epochs only for members whose metadata survived replay, then apply
/// the protocol's original assignment fields in the same iteration.
macro_rules! hydrate_member_epochs {
    ($state:ident, $seed:ident; $member:ident, $current:ident { $($assignment:tt)* }) => {
        for (mid, $current) in $seed.current_per_member {
            if let Some($member) = $state.members.get_mut(&mid) {
                $member.member_epoch = $current.member_epoch;
                $member.previous_member_epoch = $current.previous_member_epoch;
                $($assignment)*
            }
        }
    };
}
pub(super) use hydrate_member_epochs;

/// Apply a record to the bootstrap seed, then the cache, releasing each map
/// guard separately. Clone only an admissible seed write; move the cache write.
macro_rules! update_replayed_seeds {
    ($self:ident, $pending:ident, $cached:ident, $group:ident; ($first:expr, $last:expr);
        |$seed:ident| $admissible:expr => |$record:ident| $apply:block
    ) => {{
        {
            if let Some(mut $seed) = $self.$pending.get_mut($group)
                && $admissible
            {
                let $record = $first;
                $apply
            }
        }
        if let Some(mut $seed) = $self.$cached.get_mut($group)
            && $admissible
        {
            let $record = $last;
            $apply
        }
    }};
}
pub(super) use update_replayed_seeds;

/// Replay one member record with the shared parentage and epoch policy.
macro_rules! update_replayed_member {
    ($arguments:tt; metadata) => {
        $crate::coordinator::unified::seeds::update_replayed_member!(
            @apply $arguments;
            MemberMetadata, members;
        );
    };
    ($arguments:tt; target) => {
        $crate::coordinator::unified::seeds::update_replayed_member!(
            @apply $arguments;
            TargetAssignmentMember, target_per_member;
        );
    };
    ($arguments:tt; current) => {
        $crate::coordinator::unified::seeds::update_replayed_member!(
            @apply $arguments;
            CurrentMemberAssignment, current_per_member; current
        );
    };
    (@apply ($self:ident, $pending:ident, $cached:ident, $group:ident, $member:ident, $value:ident);
        $kind:ident, $field:ident; $($mode:ident)?
    ) => {
        $crate::coordinator::unified::seeds::update_replayed_seeds!(
            $self, $pending, $cached, $group; ($value.clone(), $value);
            |seed| $crate::coordinator::unified::replay_policy::replay_write_is_admissible(
                $crate::coordinator::unified::replay_policy::ReplayRecordKind::$kind,
                true, seed.members.contains_key($member),
            ) $( && $crate::coordinator::unified::seeds::update_replayed_member!(
                @epoch seed, $member, $value; $mode
            ))? => |record| {
                seed.$field.insert($member.into(), record);
            }
        );
    };
    (@epoch $seed:ident, $member:ident, $value:ident; current) => {
        $seed.current_per_member.get($member).is_none_or(|current| {
            $crate::coordinator::unified::replay_policy::replay_epoch_is_admissible(
                current.member_epoch, $value.member_epoch,
            )
        })
    };
}
pub(super) use update_replayed_member;

/// Remove a group's replay projections and protocol lock in log order.
pub(super) fn remove_replayed_group<S>(
    pending: &dashmap::DashMap<String, S>,
    cached: &dashmap::DashMap<String, S>,
    group_types: &dashmap::DashMap<String, super::group_coordinator::GroupType>,
    group_id: &str,
) {
    use super::replay_policy::{ReplayMutation, ReplayRecordKind, replay_mutation};
    assert2::debug_assert!(
        replay_mutation(ReplayRecordKind::GroupMetadata, None, true, false)
            == ReplayMutation::RemoveGroup
    );
    pending.remove(group_id);
    cached.remove(group_id);
    group_types.remove(group_id);
}

/// Scrub each projection separately, releasing the seed guard before the cache.
pub(super) fn scrub_replayed_seeds<S>(
    pending: &dashmap::DashMap<String, S>,
    cached: &dashmap::DashMap<String, S>,
    group_id: &str,
    mut scrub: impl FnMut(&mut S),
) {
    for seeds in [pending, cached] {
        if let Some(mut seed) = seeds.get_mut(group_id) {
            scrub(seed.value_mut());
        }
    }
}

/// The shared member/assignment tombstone mutations for all three seed types.
/// Each replay supplies its protocol-specific records as additional match arms.
macro_rules! scrub_seed_assignments {
    ($seed:ident, $key:expr, $kind:ident, $epoch:ident; $($pattern:pat => $value:expr),* $(,)?) => {
        match $key {
            $kind::MemberMetadata { member_id, .. } => {
                $seed.members.remove(member_id);
                $seed.target_per_member.remove(member_id);
                $seed.current_per_member.remove(member_id);
            }
            $kind::TargetAssignmentMetadata { .. } => {
                $seed.$epoch = 0;
                $seed.target_per_member.clear();
            }
            $kind::TargetAssignmentMember { member_id, .. } => {
                $seed.target_per_member.remove(member_id);
            }
            $kind::CurrentMemberAssignment { member_id, .. } => {
                $seed.current_per_member.remove(member_id);
            }
            $($pattern => $value),*
        }
    };
}
pub(super) use scrub_seed_assignments;
