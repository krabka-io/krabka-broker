//! Application of one decoded `__consumer_offsets` record to coordinator state.
//!
//! Each group protocol keeps its own record schema, so the module holds one
//! applier per protocol: the KIP-848 next-gen records, the KIP-932 share-group
//! records, the KIP-1071 streams-group records, and the classic
//! `GroupMetadata` value that rebuilds a classic group in place.

use std::sync::Arc;

use crate::{
    coordinator::{
        GroupCoordinator,
        persistence::{GroupMetadataValue, Key, OffsetCommitValue},
        unified::classic_state::{
            ClassicGroup as ClassicState, GroupState as ClassicGroupState, Member,
        },
    },
    error::BrokerError,
};

/// Decodes `value` as the value of `key`'s record type and drops the result.
///
/// Kafka's `CoordinatorRecordSerde.deserialize` decodes every value as the
/// loader reads it, before `CoordinatorLoaderImpl` hands the record to the
/// shard, so a value that does not decode fails the load even when it belongs
/// to a transaction that later aborts or never ends. Replay defers the values
/// of transactional records until their marker, so it calls this when it
/// defers one.
///
/// # Errors
///
/// Returns the decode error of the value: an unsupported value version, or
/// bytes that do not decode at that version.
pub(super) fn check_value(key: &Key, value: &[u8]) -> Result<(), BrokerError> {
    use crate::coordinator::unified::{
        persistence_next_gen::{self as ng, NextGenKey},
        share::persistence::{self as sp, ShareGroupKey},
        streams::persistence::{self as st, StreamsGroupKey},
    };
    match key {
        Key::OffsetCommit { .. } => OffsetCommitValue::decode_value(value).map(|_| ()),
        Key::GroupMetadata { .. } => GroupMetadataValue::decode_value(value).map(|_| ()),
        Key::NextGen(key) => match key {
            NextGenKey::GroupMetadata { .. } => ng::GroupMetadataValue::decode(value).map(|_| ()),
            NextGenKey::PartitionMetadata { .. } => {
                ng::PartitionMetadataValue::decode(value).map(|_| ())
            }
            NextGenKey::MemberMetadata { .. } => ng::MemberMetadataValue::decode(value).map(|_| ()),
            NextGenKey::TargetAssignmentMetadata { .. } => {
                ng::TargetAssignmentMetadataValue::decode(value).map(|_| ())
            }
            NextGenKey::TargetAssignmentMember { .. } => {
                ng::TargetAssignmentMemberValue::decode(value).map(|_| ())
            }
            NextGenKey::CurrentMemberAssignment { .. } => {
                ng::CurrentMemberAssignmentValue::decode(value).map(|_| ())
            }
            NextGenKey::RegularExpression { .. } => {
                ng::RegularExpressionValue::decode(value).map(|_| ())
            }
        },
        Key::Share(key) => match key {
            ShareGroupKey::GroupMetadata { .. } => {
                sp::ShareGroupMetadataValue::decode(value).map(|_| ())
            }
            ShareGroupKey::MemberMetadata { .. } => {
                sp::ShareGroupMemberMetadataValue::decode(value).map(|_| ())
            }
            ShareGroupKey::TargetAssignmentMetadata { .. } => {
                sp::ShareGroupTargetAssignmentMetadataValue::decode(value).map(|_| ())
            }
            ShareGroupKey::TargetAssignmentMember { .. } => {
                sp::ShareGroupTargetAssignmentMemberValue::decode(value).map(|_| ())
            }
            ShareGroupKey::CurrentMemberAssignment { .. } => {
                sp::ShareGroupCurrentMemberAssignmentValue::decode(value).map(|_| ())
            }
            ShareGroupKey::StatePartitionMetadata { .. } => {
                sp::ShareGroupStatePartitionMetadataValue::decode(value).map(|_| ())
            }
        },
        Key::Streams(key) => match key {
            StreamsGroupKey::GroupMetadata { .. } => {
                st::StreamsGroupMetadataValue::decode(value).map(|_| ())
            }
            StreamsGroupKey::MemberMetadata { .. } => {
                st::StreamsGroupMemberMetadataValue::decode(value).map(|_| ())
            }
            StreamsGroupKey::Topology { .. } => {
                st::StreamsGroupTopologyValue::decode(value).map(|_| ())
            }
            StreamsGroupKey::TargetAssignmentMetadata { .. } => {
                st::StreamsGroupTargetAssignmentMetadataValue::decode(value).map(|_| ())
            }
            StreamsGroupKey::TargetAssignmentMember { .. } => {
                st::StreamsGroupTargetAssignmentMemberValue::decode(value).map(|_| ())
            }
            StreamsGroupKey::CurrentMemberAssignment { .. } => {
                st::StreamsGroupCurrentMemberAssignmentValue::decode(value).map(|_| ())
            }
        },
    }
}

pub(super) fn apply_next_gen_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::persistence_next_gen::NextGenKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::persistence_next_gen as ng;
    match key {
        ng::NextGenKey::GroupMetadata { group_id } => {
            coordinator
                .replay_group_metadata(&group_id, ng::GroupMetadataValue::decode(value_bytes)?);
        }
        ng::NextGenKey::PartitionMetadata { group_id } => {
            ng::PartitionMetadataValue::decode(value_bytes)?;
            coordinator.replay_partition_metadata(&group_id);
        }
        ng::NextGenKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.replay_member_metadata(
                &group_id,
                &member_id,
                ng::MemberMetadataValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::TargetAssignmentMetadata { group_id } => {
            coordinator.replay_target_assignment_metadata(
                &group_id,
                ng::TargetAssignmentMetadataValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.replay_target_assignment_member(
                &group_id,
                &member_id,
                ng::TargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.replay_current_member_assignment(
                &group_id,
                &member_id,
                ng::CurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
        ng::NextGenKey::RegularExpression { group_id, regex } => {
            coordinator.replay_regular_expression(
                &group_id,
                &regex,
                ng::RegularExpressionValue::decode(value_bytes)?,
            );
        }
    }
    Ok(())
}

pub(super) fn apply_share_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::share::persistence::ShareGroupKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::share::persistence as sp;
    match key {
        sp::ShareGroupKey::GroupMetadata { group_id } => {
            let value = sp::ShareGroupMetadataValue::decode(value_bytes)?;
            coordinator.replay_share_group_metadata(&group_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
        sp::ShareGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            let value = sp::ShareGroupMemberMetadataValue::decode(value_bytes)?;
            coordinator.replay_share_member_metadata(&group_id, &member_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
        sp::ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            let value = sp::ShareGroupTargetAssignmentMetadataValue::decode(value_bytes)?;
            coordinator.replay_share_target_assignment_metadata(&group_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
        sp::ShareGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            let value = sp::ShareGroupTargetAssignmentMemberValue::decode(value_bytes)?;
            coordinator.replay_share_target_assignment_member(&group_id, &member_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
        sp::ShareGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            let value = sp::ShareGroupCurrentMemberAssignmentValue::decode(value_bytes)?;
            coordinator.replay_share_current_member_assignment(&group_id, &member_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
        sp::ShareGroupKey::StatePartitionMetadata { group_id } => {
            let value = sp::ShareGroupStatePartitionMetadataValue::decode(value_bytes)?;
            coordinator.replay_share_state_partition_metadata(&group_id, value);
            if coordinator.cached_share_seed(&group_id).is_some() {
                coordinator.mark_share(&group_id);
            }
        }
    }
    Ok(())
}

pub(super) fn apply_streams_record(
    coordinator: &Arc<GroupCoordinator>,
    key: crate::coordinator::unified::streams::persistence::StreamsGroupKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::unified::streams::persistence as sp;
    match key {
        sp::StreamsGroupKey::GroupMetadata { group_id } => {
            let v = sp::StreamsGroupMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_group_metadata(&group_id, v)?;
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
        sp::StreamsGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            let value = sp::StreamsGroupMemberMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_member_metadata(&group_id, &member_id, value);
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
        sp::StreamsGroupKey::Topology { group_id } => {
            let value = sp::StreamsGroupTopologyValue::decode(value_bytes)?;
            coordinator.replay_streams_topology(&group_id, value);
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
        sp::StreamsGroupKey::TargetAssignmentMetadata { group_id } => {
            let v = sp::StreamsGroupTargetAssignmentMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_target_assignment_metadata(&group_id, v);
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
        sp::StreamsGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            let value = sp::StreamsGroupTargetAssignmentMemberValue::decode(value_bytes)?;
            coordinator.replay_streams_target_assignment_member(&group_id, &member_id, value);
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
        sp::StreamsGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            let value = sp::StreamsGroupCurrentMemberAssignmentValue::decode(value_bytes)?;
            coordinator.replay_streams_current_member_assignment(&group_id, &member_id, value);
            if coordinator.cached_streams_seed(&group_id).is_some() {
                coordinator.mark_streams(&group_id);
            }
        }
    }
    Ok(())
}

/// Rebuilds a classic group from its persisted `GroupMetadata` value, as
/// Kafka's `GroupMetadataManager.replay(GroupMetadataKey, GroupMetadataValue)`
/// does.
///
/// Each loaded member supports one protocol: the protocol that the group
/// selected, with the stored subscription of the member as its metadata. A new
/// member that proposes that protocol can then join the group. Kafka gives the
/// members of a value with no selected protocol a protocol with a null name,
/// and no `JoinGroup` can propose that name, so here they get no protocol. A
/// stored rebalance timeout of -1, the schema default of a version 0 value,
/// becomes the session timeout of the member. An empty protocol type becomes
/// no protocol type.
pub(super) fn apply_group_metadata(
    g: &mut ClassicState,
    v: GroupMetadataValue,
    replay_timestamp_ms: i64,
) {
    g.protocol_type = Some(v.protocol_type).filter(|protocol_type| !protocol_type.is_empty());
    g.generation_id = v.generation;
    g.leader_id = v.leader;
    // Repopulate members. `last_heartbeat` defaults to `now` inside
    // `Member::new` so they don't immediately time out; the client will
    // re-join anyway after a coordinator restart.
    g.members.clear();
    g.static_members.clear();
    for m in v.members {
        let rebalance_timeout_ms = if m.rebalance_timeout_ms == -1 {
            m.session_timeout_ms
        } else {
            m.rebalance_timeout_ms
        };
        let session_timeout = std::time::Duration::from_millis(
            u64::try_from(m.session_timeout_ms.max(0)).unwrap_or(30_000),
        );
        let rebalance_timeout = std::time::Duration::from_millis(
            u64::try_from(rebalance_timeout_ms.max(0)).unwrap_or(60_000),
        );
        let protocols = v
            .protocol_name
            .iter()
            .map(|name| (name.clone(), m.subscription.clone()))
            .collect();
        let mut member = Member::new(
            m.member_id.clone(),
            m.client_id,
            m.client_host,
            session_timeout,
            rebalance_timeout,
            protocols,
        )
        .with_instance_id(m.group_instance_id.clone());
        member.protocol_metadata = m.subscription;
        member.assignment = Some(m.assignment);
        if let Some(iid) = m.group_instance_id {
            g.static_members.insert(iid, m.member_id.clone());
        }
        g.members.insert(m.member_id, member);
    }
    g.protocol_name = v.protocol_name;
    g.state = if g.members.is_empty() {
        ClassicGroupState::Empty
    } else {
        ClassicGroupState::Stable
    };
    let _ = replay_timestamp_ms; // currently unused; logged for debug
}
