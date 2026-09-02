//! Building the durable records for a group-state transition and appending
//! them.
//!
//! These functions project live group state into the persisted next-gen
//! values, assemble the [`PendingRecords`] delta for one transition, and flush
//! it — the classic k2 snapshot for a classic group, the next-gen record set
//! plus the respawn-cache update for a consumer group.

use std::collections::HashMap;

use krabka_protocol::primitives::uuid::Uuid;

use super::{
    FALLBACK_REBALANCE_TIMEOUT_MS_I32, FALLBACK_SESSION_TIMEOUT_MS_I32, chrono_now_ms,
    pending_records::PendingRecords,
};
use crate::coordinator::unified::{
    GroupCoordinator,
    classic_state::ClassicGroup,
    consumer_state::{GroupState, MemberState},
    offsets_log::OffsetsLog,
    persistence_next_gen::{
        ClassicMemberMetadata, CurrentMemberAssignmentValue, GroupMetadataValue,
        MemberMetadataValue, TargetAssignmentMemberValue, TargetAssignmentMetadataValue,
    },
};

/// Maps a member's in-memory classic facade, if there is one, into the
/// persisted k5 `ClassicMemberMetadata` sub-block. It is the single source of
/// truth for the log-write path and its incremental cache update.
fn classic_member_metadata(m: &MemberState) -> Option<ClassicMemberMetadata> {
    m.classic.as_ref().map(|f| ClassicMemberMetadata {
        session_timeout_ms: i32::try_from(f.session_timeout.as_millis())
            .unwrap_or(FALLBACK_SESSION_TIMEOUT_MS_I32),
        supported_protocols: f.supported_protocols.clone(),
        last_synced_assignment: f.last_synced_assignment.clone(),
    })
}

fn member_metadata_value(member: &MemberState) -> MemberMetadataValue {
    MemberMetadataValue {
        instance_id: member.instance_id.clone(),
        rack_id: member.rack_id.clone(),
        client_id: member.client_id.clone(),
        client_host: member.client_host.clone(),
        subscribed_topic_names: member.subscribed_topic_names.iter().cloned().collect(),
        subscribed_topic_regex: member.subscribed_topic_regex.clone(),
        server_assignor: member.server_assignor.clone(),
        rebalance_timeout_ms: i32::try_from(member.rebalance_timeout.as_millis())
            .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS_I32),
        classic: classic_member_metadata(member),
    }
}

fn current_assignment_value(member: &MemberState) -> CurrentMemberAssignmentValue {
    use crate::coordinator::unified::persistence_next_gen::AssignedTopicPartitions;
    CurrentMemberAssignmentValue {
        member_epoch: member.member_epoch,
        previous_member_epoch: member.previous_member_epoch,
        state: member.assignment_state,
        assigned_partitions: member
            .assigned_partitions
            .iter()
            .map(|(topic_id, partitions)| AssignedTopicPartitions {
                topic_id: *topic_id,
                partitions: partitions.clone(),
            })
            .collect(),
        partitions_pending_revocation: member
            .partitions_pending_revocation
            .iter()
            .map(|(topic_id, partitions)| AssignedTopicPartitions {
                topic_id: *topic_id,
                partitions: partitions.clone(),
            })
            .collect(),
    }
}

fn target_assignment_value(target: &HashMap<Uuid, Vec<i32>>) -> TargetAssignmentMemberValue {
    use crate::coordinator::unified::persistence_next_gen::AssignedTopicPartitions;
    TargetAssignmentMemberValue {
        topic_partitions: target
            .iter()
            .map(|(topic_id, partitions)| AssignedTopicPartitions {
                topic_id: *topic_id,
                partitions: partitions.clone(),
            })
            .collect(),
    }
}

/// Builds the durable records for one group-state transition.
///
/// Member metadata is limited to `affected_members`. When reconciliation made
/// a new target, every target and current assignment is included because the
/// reconciler updates the whole group at once.
pub(super) fn snapshot_pending_after_change(
    state: &GroupState,
    affected_members: &[String],
    target_changed: bool,
) -> PendingRecords {
    let mut pending = PendingRecords {
        group_metadata: Some(GroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending
                .member_metadata
                .push((mid.clone(), Some(member_metadata_value(m))));
            pending
                .current_per_member
                .push((mid.clone(), Some(current_assignment_value(m))));
        }
    }
    if target_changed {
        pending.target_metadata = Some(TargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
        for (mid, member) in &state.members {
            if !affected_members.iter().any(|affected| affected == mid) {
                pending
                    .current_per_member
                    .push((mid.clone(), Some(current_assignment_value(member))));
            }
            pending.target_per_member.push((
                mid.clone(),
                state
                    .target
                    .per_member
                    .get(mid)
                    .map(target_assignment_value),
            ));
        }
    }
    pending
}

/// Builds a `PendingRecords` set that describes the WHOLE consumer group: the
/// group epoch, the target epoch when non-zero, and every member's k5
/// member-metadata (facade included), k8 current-assignment, and k7 target
/// when present. The upgrade flip uses it to write the full converted group
/// atomically in one batch.
pub(crate) fn full_pending_records(state: &GroupState) -> PendingRecords {
    let all_member_ids: Vec<String> = state.members.keys().cloned().collect();
    snapshot_pending_after_change(state, &all_member_ids, true)
}

/// Builds a wire-faithful classic k2 `GroupMetadataValue` from a downgraded
/// [`ClassicGroup`].
///
/// It persists every classic member with its `subscription` (the selected
/// `protocol_metadata`) and its `assignment` (the seed the downgrade computed
/// from the next-gen target). Bootstrap replay therefore reconstructs the
/// classic group with its members and their assignments intact. See
/// `apply_group_metadata` in `coordinator::bootstrap`. The downgrade flip uses
/// this function.
pub(crate) fn classic_group_metadata_record(
    state: &ClassicGroup,
    now_ms: i64,
) -> crate::coordinator::unified::persistence::GroupMetadataValue {
    use crate::coordinator::unified::persistence::{GroupMetadataValue, MemberMetadata};
    let members = state
        .members
        .values()
        .map(|m| MemberMetadata {
            member_id: m.id.clone(),
            group_instance_id: m.group_instance_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis())
                .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS_I32),
            session_timeout_ms: i32::try_from(m.session_timeout.as_millis())
                .unwrap_or(FALLBACK_SESSION_TIMEOUT_MS_I32),
            subscription: m.protocol_metadata.clone(),
            assignment: m.assignment.clone().unwrap_or_default(),
        })
        .collect();
    GroupMetadataValue {
        protocol_type: state
            .protocol_type
            .clone()
            .unwrap_or_else(|| "consumer".into()),
        generation: state.generation_id,
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone(),
        current_state_timestamp_ms: now_ms,
        members,
    }
}

/// Append the complete classic k2 snapshot for one durable group transition.
pub(super) async fn flush_classic_metadata(
    state: &ClassicGroup,
    offsets_log: &dyn OffsetsLog,
) -> Result<(), crate::error::BrokerError> {
    let now_ms = chrono_now_ms();
    let pending = PendingRecords {
        classic_group_metadata: Some(classic_group_metadata_record(state, now_ms)),
        ..PendingRecords::default()
    };
    offsets_log
        .append(&state.group_id, pending.to_batch(&state.group_id, now_ms))
        .await
}

pub(super) async fn flush_pending(
    state: &GroupState,
    pending: PendingRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.to_batch(&state.group_id, now_ms);
    offsets_log.append(&state.group_id, batch).await?;
    pending.apply_to_cache(coordinator, &state.group_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use assert2::{assert, check};
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    use super::*;
    use crate::coordinator::unified::actor::member_state::build_member;

    #[test]
    fn reconciled_snapshot_persists_every_members_assignments() {
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            state.add_or_update_member(build_member(
                member_id,
                &ConsumerGroupHeartbeatRequest {
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                crate::coordinator::unified::ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            ));
        }
        let topic_id = Uuid([9; 16]);
        state.group_epoch = 2;
        state.target.epoch = 2;
        state
            .target
            .per_member
            .insert("m1".into(), maplit::hashmap! {topic_id => vec![0]});
        state
            .target
            .per_member
            .insert("m2".into(), maplit::hashmap! {topic_id => vec![1]});

        let pending = snapshot_pending_after_change(&state, &["m2".into()], true);
        let member_ids: Vec<&str> = pending
            .member_metadata
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        let mut target_ids: Vec<&str> = pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        let mut current_ids: Vec<&str> = pending
            .current_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        target_ids.sort_unstable();
        current_ids.sort_unstable();

        check!(member_ids == vec!["m2"]);
        check!(target_ids == vec!["m1", "m2"]);
        check!(current_ids == vec!["m1", "m2"]);
        assert!(pending.target_metadata.is_some());
    }

    #[test]
    fn full_pending_records_contains_every_member_record() {
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            state.add_or_update_member(build_member(
                member_id,
                &ConsumerGroupHeartbeatRequest::default(),
                crate::coordinator::unified::ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            ));
            state
                .target
                .per_member
                .insert(member_id.into(), HashMap::new());
        }
        state.group_epoch = 4;
        state.target.epoch = 4;

        let pending = full_pending_records(&state);

        check!(pending.group_metadata == Some(GroupMetadataValue { epoch: 4 }));
        check!(
            pending.target_metadata
                == Some(TargetAssignmentMetadataValue {
                    assignment_epoch: 4,
                })
        );
        check!(pending.member_metadata.len() == 2);
        check!(pending.target_per_member.len() == 2);
        assert!(pending.current_per_member.len() == 2);
    }

    #[test]
    fn member_only_snapshot_does_not_rewrite_group_target() {
        let mut state = GroupState::new("g");
        state.add_or_update_member(build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            crate::coordinator::unified::ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        ));

        let pending = snapshot_pending_after_change(&state, &["m1".into()], false);

        check!(pending.member_metadata.len() == 1);
        check!(pending.current_per_member.len() == 1);
        check!(pending.target_metadata.is_none());
        assert!(pending.target_per_member.is_empty());
    }
}
