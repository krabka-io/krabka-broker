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
        persistence::GroupMetadataValue,
        unified::classic_state::{
            ClassicGroup as ClassicState, GroupState as ClassicGroupState, Member,
        },
    },
    error::BrokerError,
};

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
            coordinator.mark_share(&group_id);
            coordinator.replay_share_group_metadata(
                &group_id,
                sp::ShareGroupMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_member_metadata(
                &group_id,
                &member_id,
                sp::ShareGroupMemberMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_target_assignment_metadata(
                &group_id,
                sp::ShareGroupTargetAssignmentMetadataValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_target_assignment_member(
                &group_id,
                &member_id,
                sp::ShareGroupTargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_current_member_assignment(
                &group_id,
                &member_id,
                sp::ShareGroupCurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
        sp::ShareGroupKey::StatePartitionMetadata { group_id } => {
            coordinator.mark_share(&group_id);
            coordinator.replay_share_state_partition_metadata(
                &group_id,
                sp::ShareGroupStatePartitionMetadataValue::decode(value_bytes)?,
            );
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
            coordinator.mark_streams(&group_id);
            let v = sp::StreamsGroupMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_group_metadata(&group_id, v.epoch);
        }
        sp::StreamsGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_member_metadata(
                &group_id,
                &member_id,
                sp::StreamsGroupMemberMetadataValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::Topology { group_id } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_topology(
                &group_id,
                sp::StreamsGroupTopologyValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::PartitionMetadata { group_id } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_partition_metadata(
                &group_id,
                sp::StreamsGroupPartitionMetadataValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::TargetAssignmentMetadata { group_id } => {
            coordinator.mark_streams(&group_id);
            let v = sp::StreamsGroupTargetAssignmentMetadataValue::decode(value_bytes)?;
            coordinator.replay_streams_target_assignment_metadata(&group_id, v.assignment_epoch);
        }
        sp::StreamsGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_target_assignment_member(
                &group_id,
                &member_id,
                sp::StreamsGroupTargetAssignmentMemberValue::decode(value_bytes)?,
            );
        }
        sp::StreamsGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            coordinator.mark_streams(&group_id);
            coordinator.replay_streams_current_member_assignment(
                &group_id,
                &member_id,
                sp::StreamsGroupCurrentMemberAssignmentValue::decode(value_bytes)?,
            );
        }
    }
    Ok(())
}

pub(super) fn apply_group_metadata(
    g: &mut ClassicState,
    v: GroupMetadataValue,
    replay_timestamp_ms: i64,
) {
    g.protocol_type = Some(v.protocol_type);
    g.generation_id = v.generation;
    g.leader_id = v.leader;
    g.protocol_name = v.protocol_name;
    // Repopulate members. `last_heartbeat` defaults to `now` inside
    // `Member::new` so they don't immediately time out; the client will
    // re-join anyway after a coordinator restart.
    g.members.clear();
    g.static_members.clear();
    for m in v.members {
        let session_timeout = std::time::Duration::from_millis(
            u64::try_from(m.session_timeout_ms.max(0)).unwrap_or(30_000),
        );
        let rebalance_timeout = std::time::Duration::from_millis(
            u64::try_from(m.rebalance_timeout_ms.max(0)).unwrap_or(60_000),
        );
        let mut member = Member::new(
            m.member_id.clone(),
            m.client_id,
            m.client_host,
            session_timeout,
            rebalance_timeout,
            Vec::new(),
        )
        .with_instance_id(m.group_instance_id.clone());
        member.protocol_metadata = m.subscription;
        member.assignment = Some(m.assignment);
        if let Some(iid) = m.group_instance_id {
            g.static_members.insert(iid, m.member_id.clone());
        }
        g.members.insert(m.member_id, member);
    }
    g.state = if g.members.is_empty() {
        ClassicGroupState::Empty
    } else {
        ClassicGroupState::Stable
    };
    let _ = replay_timestamp_ms; // currently unused; logged for debug
}
