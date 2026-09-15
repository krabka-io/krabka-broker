//! The classic `LeaveGroup` and `DeleteGroups` paths.
//!
//! Both remove state from a group and both must persist the result before they
//! answer: a leave that empties a classic group bumps and rewrites its
//! generation, a leave against an upgraded group tombstones the departed
//! member's next-gen records, and a delete appends the tombstones of the
//! group's offsets and its classic k2 record, which stops the actor.

use std::collections::{BTreeSet, HashSet};

use krabka_protocol::{
    owned::{leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse},
    records::RecordBatch,
};
use tokio::sync::oneshot;

use super::{
    ActorServices, ErrorCode, ParkedWaiters, chrono_now_ms,
    downgrade::maybe_downgrade,
    member_state::run_reconcile,
    persistence::{flush_classic_metadata, flush_pending, snapshot_pending_after_change},
    retention::tombstone_batch,
    waiters::{drain_removed_classic_waiters, maybe_complete_classic},
};
use crate::{
    codes,
    coordinator::{
        DeleteGroupError,
        unified::{
            classic_ops, consumer_state::GroupState, group::CoordinatorGroup,
            offsets_log::OffsetsLog,
        },
    },
};

#[cfg(test)]
mod tests;

pub(super) async fn handle_classic_leave_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: &LeaveGroupRequest,
    version: i16,
) -> Result<Vec<MemberResponse>, ErrorCode> {
    if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let before_members: Vec<String> = state.members.keys().cloned().collect();
        let responses = classic_ops::handle_leave(state, request, version);
        let removed: Vec<String> = before_members
            .into_iter()
            .filter(|member_id| !state.members.contains_key(member_id))
            .collect();
        if !removed.is_empty() && state.members.is_empty() {
            let Some(generation_id) = crate::metadata_epoch::next_i32(state.generation_id) else {
                *state = previous;
                tracing::warn!(group_id = %state.group_id,
                    "classic LeaveGroup refused because the generation is exhausted");
                return Err(codes::INVALID_REQUEST);
            };
            state.generation_id = generation_id;
            if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                *state = previous;
                tracing::warn!(group_id = %state.group_id, %error,
                    "classic LeaveGroup log write failed");
                return Err(codes::COORDINATOR_LOAD_IN_PROGRESS);
            }
        }
        drain_removed_classic_waiters(&removed, &mut parked.joiners, &mut parked.followers);
        maybe_complete_classic(state, &mut parked.joiners, &mut parked.followers);
        return Ok(responses);
    }

    let Some(state) = group.as_consumer_mut() else {
        return Err(codes::UNKNOWN_MEMBER_ID);
    };
    let (responses, removed) = resolve_consumer_classic_leave(state, request, version);
    if removed.is_empty() {
        return Ok(responses);
    }
    for member_id in &removed {
        state.remove_member(member_id);
    }
    run_reconcile(state, services.config, services.metadata);
    let mut pending = snapshot_pending_after_change(state, &[], true);
    for member_id in &removed {
        pending.member_metadata.push((member_id.clone(), None));
        pending.target_per_member.push((member_id.clone(), None));
        pending.current_per_member.push((member_id.clone(), None));
    }
    flush_pending(
        state,
        pending,
        services.offsets_log,
        services.coordinator,
        chrono_now_ms(),
    )
    .await
    .map_err(|error| {
        tracing::warn!(group_id = %state.group_id, %error,
            "hosted classic LeaveGroup log write failed");
        codes::COORDINATOR_LOAD_IN_PROGRESS
    })?;
    maybe_downgrade(
        group,
        services.config,
        services.metadata,
        services.offsets_log,
        services.coordinator,
    )
    .await
    .map_err(|error| {
        tracing::warn!(group_id = %group.group_id, %error,
            "hosted classic LeaveGroup downgrade log write failed");
        codes::COORDINATOR_LOAD_IN_PROGRESS
    })?;
    Ok(responses)
}

/// Resolves a classic `LeaveGroup` request against a consumer-kind group.
/// Resolution happens before any removal, matching Kafka's batch semantics:
/// duplicate valid identities all succeed but produce one set of tombstones.
fn resolve_consumer_classic_leave(
    state: &GroupState,
    request: &LeaveGroupRequest,
    version: i16,
) -> (Vec<MemberResponse>, Vec<String>) {
    let identities: Vec<(&str, Option<&str>)> = if version >= 3 {
        request
            .members
            .iter()
            .map(|member| {
                (
                    member.member_id.as_str(),
                    member.group_instance_id.as_deref(),
                )
            })
            .collect()
    } else {
        vec![(request.member_id.as_str(), None)]
    };
    let mut seen = HashSet::new();
    let mut removed = Vec::new();
    let responses = identities
        .into_iter()
        .map(|(member_id, instance_id)| {
            let (resolved, error_code) = match instance_id {
                Some(instance_id) => match state.current_member_for_instance(instance_id) {
                    Some(current) if member_id.is_empty() || current == member_id => {
                        (Some(current), codes::NONE)
                    }
                    Some(_) => (None, codes::FENCED_INSTANCE_ID),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                None if state.members.contains_key(member_id) => (Some(member_id), codes::NONE),
                None => (None, codes::UNKNOWN_MEMBER_ID),
            };
            if let Some(resolved) = resolved
                && seen.insert(resolved.to_string())
            {
                removed.push(resolved.to_string());
            }
            MemberResponse {
                member_id: member_id.to_string(),
                group_instance_id: instance_id.map(str::to_string),
                error_code,
                ..Default::default()
            }
        })
        .collect();
    (responses, removed)
}

/// The `ClassicDelete` mailbox arm: delete an empty classic group and every
/// offset it holds, in one batch.
///
/// The batch follows Kafka's `GroupCoordinatorShard.deleteGroups`. It starts
/// with an `OffsetCommit` tombstone for each committed offset
/// (`OffsetMetadataManager.deleteAllOffsets`), then one for each key that an
/// open transaction wrote and that has no committed offset, and ends with the
/// k2 `GroupMetadata` tombstone. The open transaction keys include the ones a
/// `TxnOffsetCommit` has reserved but not marked yet. Without the offset tombstones the commits stay
/// live in `__consumer_offsets`, and a replay seeds the deleted group again
/// from them.
///
/// Returns the actor's keep-running flag: a deleted group stops its actor.
pub(super) async fn handle_classic_delete_message(
    group: &CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    reply: oneshot::Sender<Result<(), DeleteGroupError>>,
) -> bool {
    let Some(state) = group.as_classic() else {
        let _ = reply.send(Err(DeleteGroupError::NotFound));
        return true;
    };
    if !state.members.is_empty() {
        let _ = reply.send(Err(DeleteGroupError::NonEmpty));
        return true;
    }
    let group_id = state.group_id.clone();
    let batch = delete_group_batch(group, chrono_now_ms());
    match offsets_log.append(&group_id, batch).await {
        Ok(()) => {
            let _ = reply.send(Ok(()));
            false
        }
        Err(error) => {
            tracing::warn!(%group_id, %error, "classic DeleteGroups tombstone write failed");
            let _ = reply.send(Err(DeleteGroupError::Internal));
            true
        }
    }
}

/// The records that delete `group`: its offset tombstones, then its group
/// tombstone. The offset keys are sorted, so the batch does not depend on map
/// order.
fn delete_group_batch(group: &CoordinatorGroup, now_ms: i64) -> RecordBatch {
    let keys: Vec<(String, i32)> = group
        .committed_offsets
        .keys()
        .cloned()
        .chain(group.unresolved_txn_keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    tombstone_batch(&group.group_id, &keys, Some(&group.kind), now_ms)
}
