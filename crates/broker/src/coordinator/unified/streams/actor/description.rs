//! KIP-1331 inside the streams-group actor: asking a member for the topology
//! description, storing what it pushes, and serving it to a describe.
//!
//! Kafka splits this between the shard, which keeps the group's
//! `StoredDescriptionTopologyEpoch` and `FailedDescriptionTopologyEpoch`, and
//! the broker-wide `StreamsGroupTopologyDescriptionManager`, which owns the
//! plugin and the back-off. The coordinator owns shared runtime state, so each
//! step below runs in the one turn of the actor that handles the message.
//!
//! As in released Kafka 4.4, persist UNCERTAIN before backend mutation and
//! the final epoch only after success. Retention sweeps retry empty groups
//! only after all offsets expire; permanent store rejection suppresses solicitation for
//! that epoch while transient failure retains exponential heartbeat backoff.

use {
    super::{
        super::migration::streams_records_tombstone_batch, ActorState, StreamsTurn, chrono_now_ms,
        records::group_metadata_value,
    },
    crate::{
        codes,
        coordinator::{
            DeleteGroupError,
            unified::{
                GroupCoordinator, GroupType,
                actor::{GroupActorMessage, ReapOutcome},
                streams::{
                    config::StreamsGroupConfig,
                    description::{DescriptionEpochs, Jitter, StorageFailure, TopologyDescription},
                    persistence::PendingStreamsRecords,
                    topology::status,
                },
            },
        },
        error::BrokerError,
        metrics::TopologyStorageOperation,
        task_util::ask,
    },
    dashmap::mapref::entry::Entry,
    krabka_protocol::owned::{
        common::streams_group_topology_description_update_request::topology_description::TopologyDescription as WireDescription,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    krabka_verified::streams_cleanup::{finish_store, may_reclaim},
    std::{future::Future, sync::Arc, time::Instant},
};

/// A `StreamsGroupTopologyDescriptionUpdate` that the handler passed to the
/// group.
#[derive(Debug)]
pub struct DescriptionPush {
    pub member_id: String,
    pub topology_epoch: i32,
    pub description: WireDescription,
}

/// The error code and message of a push's response.
pub type PushAnswer = (i16, Option<String>);

/// Kafka's `StreamsGroup.currentTopologyEpoch`: the epoch of the group's
/// topology, or -1 before a member sent one.
fn current_topology_epoch(actor: &ActorState) -> i32 {
    actor
        .state
        .topology
        .as_ref()
        .map_or(-1, |topology| topology.epoch)
}

/// Kafka's `StreamsGroupTopologyDescriptionManager.maybeSetTopologyDescriptionRequired`:
/// asks the member of an accepted heartbeat for the topology description.
///
/// It asks when a plugin is configured, the request is at version 1, which
/// carries the flag, the member is not leaving, the group has a topology whose
/// epoch the plugin neither holds nor rejected, the response does not tell the
/// member its topology is stale, and no other member of the group was asked
/// for the same epoch within the back-off window.
pub(super) fn maybe_request_description(
    actor: &ActorState,
    config: &StreamsGroupConfig,
    request: &StreamsGroupHeartbeatRequest,
    version: i16,
    response: &mut StreamsGroupHeartbeatResponse,
    now: Instant,
) {
    if version < 1
        || !config.description_plugin_configured()
        || request.member_epoch < 0
        || response.error_code != codes::NONE
    {
        return;
    }
    let topology_epoch = current_topology_epoch(actor);
    let stale = response
        .status
        .iter()
        .flatten()
        .any(|entry| entry.status_code == status::STALE_TOPOLOGY);
    if topology_epoch < 0
        || actor.description_epochs.stored == topology_epoch
        || actor.description_epochs.failed == topology_epoch
        || stale
    {
        return;
    }
    if actor
        .description_runtime
        .arm_if_not_active(topology_epoch, now, Jitter::draw())
    {
        response.topology_description_required = true;
        // Kafka's wording: the Apache Kafka system tests read the broker log
        // for it.
        tracing::info!(
            group_id = %actor.state.group_id,
            topology_epoch,
            "Requested topology description push at topology epoch {topology_epoch}."
        );
    }
}

/// Kafka's `StreamsGroupTopologyDescriptionManager.pushTopology` with the
/// in-memory plugin: checks the push against the group, stores the
/// description, and records its epoch, in the group metadata record when the
/// group writes the trunk tags.
///
/// # Errors
///
/// Returns the error of the log write that records the epoch. The actor then
/// stops, as it does when a heartbeat cannot write.
pub(super) async fn handle_push(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
    push: &DescriptionPush,
) -> Result<PushAnswer, BrokerError> {
    let group_id = actor.state.group_id.clone();
    // `GroupMetadataManager.validateStreamsGroupTopologyDescriptionUpdate`.
    if actor.holds_nothing() {
        return Ok((
            codes::GROUP_ID_NOT_FOUND,
            Some(format!("Group {group_id} not found.")),
        ));
    }
    if !actor.state.members.contains_key(&push.member_id) {
        return Ok((
            codes::UNKNOWN_MEMBER_ID,
            Some(format!(
                "Member {} is not a member of group {group_id}.",
                push.member_id
            )),
        ));
    }
    let topology_epoch = current_topology_epoch(actor);
    if push.topology_epoch != topology_epoch {
        return Ok((
            codes::INVALID_REQUEST,
            Some(format!(
                "Topology epoch {} does not match the group's current topology epoch \
                 {topology_epoch}.",
                push.topology_epoch
            )),
        ));
    }
    let description = match TopologyDescription::from_push(&push.description) {
        Ok(description) => description,
        Err(message) => return Ok((codes::INVALID_REQUEST, Some(message))),
    };
    // Persist intent before an external side effect: replay must never mistake
    // a possibly-written backend value for a reliably stored epoch.
    actor.description_epochs.stored = DescriptionEpochs::UNCERTAIN;
    persist_epochs(actor, turn).await?;
    let result = if let Some(backend) = &turn.config.topology_description_backend {
        observe_native(
            turn.coordinator,
            TopologyStorageOperation::Set,
            backend.set(&group_id, topology_epoch, &description),
        )
        .await
    } else {
        actor.description_runtime.store(topology_epoch, description);
        Ok(())
    };
    let (stored, failed) = finish_store(
        topology_epoch,
        actor.description_epochs.failed,
        result.is_ok(),
        result == Err(StorageFailure::Permanent),
    );
    actor.description_epochs.stored = stored;
    actor.description_epochs.failed = failed;
    match result {
        Ok(()) => {}
        Err(failure) => {
            if failure == StorageFailure::Permanent {
                persist_epochs(actor, turn).await?;
            }
            if failure == StorageFailure::Permanent {
                actor.description_runtime.clear(topology_epoch);
            } else {
                actor.description_runtime.arm_or_extend(
                    topology_epoch,
                    Instant::now(),
                    Jitter::draw(),
                );
            }
            return Ok((
                codes::STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED,
                Some("Topology description plugin failed.".to_owned()),
            ));
        }
    }
    persist_epochs(actor, turn).await?;
    actor.description_runtime.clear(topology_epoch);
    Ok((codes::NONE, None))
}

/// What `StreamsGroupTopologyDescriptionManager.attachTopologyDescriptions`
/// finds for the group: the stored description, when the group's recorded
/// epoch says the plugin holds the current topology epoch and the in-memory
/// plugin still holds a description of that epoch, as its `getTopology`
/// answers.
pub(super) async fn stored_description(
    actor: &ActorState,
    turn: StreamsTurn<'_>,
) -> Result<Option<TopologyDescription>, StorageFailure> {
    let config = turn.config;
    if !config.description_plugin_configured() {
        return Ok(None);
    }
    let Some(topology) = actor.state.topology.as_ref() else {
        return Ok(None);
    };
    let topology_epoch = topology.epoch;
    if !actor.description_epochs.holds(topology_epoch) {
        return Ok(None);
    }
    if let Some(backend) = &config.topology_description_backend {
        return observe_native(
            turn.coordinator,
            TopologyStorageOperation::Get,
            backend.get(&actor.state.group_id, topology_epoch),
        )
        .await;
    }
    Ok(actor.description_runtime.get(topology_epoch))
}

/// Persist a backend operation's epoch boundary.
async fn persist_epochs(actor: &ActorState, turn: StreamsTurn<'_>) -> Result<(), BrokerError> {
    if actor.trunk_records {
        let pending = PendingStreamsRecords {
            group_metadata: Some(group_metadata_value(actor)),
            ..Default::default()
        };
        turn.flush(actor, pending, chrono_now_ms()).await?;
    }
    Ok(())
}

/// Retry cleanup of uncertain/stale storage. A failed delete keeps UNCERTAIN
/// durable and is retried on a later session check, never in a tight loop.
pub(super) async fn cleanup(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
    force: bool,
) -> Result<bool, BrokerError> {
    if !turn.config.description_plugin_configured() {
        return Ok(true);
    }
    // Recheck membership in the actor after the caller's read: a joining
    // member may have revived the group before this message arrived.
    if force && !actor.state.members.is_empty() {
        return Ok(false);
    }
    let stored = actor.description_epochs.stored;
    if stored == DescriptionEpochs::NONE {
        if force {
            actor.description_runtime.delete();
            actor.description_runtime.reset_backoff();
        }
        return Ok(true);
    }
    if !force && (stored == current_topology_epoch(actor) || !actor.state.members.is_empty()) {
        return Ok(true);
    }
    actor.description_epochs.stored = DescriptionEpochs::UNCERTAIN;
    persist_epochs(actor, turn).await?;
    if let Some(backend) = &turn.config.topology_description_backend
        && observe_native(
            turn.coordinator,
            TopologyStorageOperation::Delete,
            backend.delete(&actor.state.group_id),
        )
        .await
        .is_err()
    {
        return Ok(false);
    }
    actor.description_runtime.delete();
    actor.description_epochs.stored = DescriptionEpochs::NONE;
    actor.description_runtime.reset_backoff();
    persist_epochs(actor, turn).await?;
    Ok(true)
}

/// A classic join must not convert over orphaned plugin state. Failed
/// conversion deletes are throttled at the UNCERTAIN epoch as in Kafka 4.4.
pub(super) async fn prepare_conversion(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
) -> Result<bool, BrokerError> {
    if !actor.state.members.is_empty() {
        return Ok(false);
    }
    if actor.description_epochs.stored == DescriptionEpochs::NONE {
        actor.description_runtime.delete();
        actor.description_runtime.reset_backoff();
        return Ok(true);
    }
    if !actor.description_runtime.arm_if_not_active(
        DescriptionEpochs::UNCERTAIN,
        Instant::now(),
        Jitter::draw(),
    ) {
        return Ok(false);
    }
    let result = cleanup(actor, turn, true).await?;
    if result {
        actor.description_runtime.reset_backoff();
    }
    Ok(result)
}

/// Reclaim empty Streams groups only after every offset has expired. Holding
/// this actor turn prevents a joining member from racing plugin deletion.
pub(super) async fn reap(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
    now_ms: i64,
    retention_ms: i64,
) -> Result<ReapOutcome, BrokerError> {
    let has_members = !actor.state.members.is_empty();
    let initialized = !actor.holds_nothing();
    if !may_reclaim(has_members, true, initialized) {
        return Ok(ReapOutcome::default());
    }
    let group_id = &actor.state.group_id;
    let mut outcome = if let Some(home) = turn.coordinator.find(group_id) {
        let Ok(outcome) = ask(&home.tx, |reply| GroupActorMessage::ReapStreamsOffsets {
            now_ms,
            retention_ms,
            reply,
        })
        .await
        else {
            return Ok(ReapOutcome::default());
        };
        outcome
    } else {
        ReapOutcome {
            group_deleted: true,
            ..Default::default()
        }
    };
    if !may_reclaim(has_members, outcome.group_deleted, initialized) {
        return Ok(outcome);
    }
    outcome.group_deleted = false;
    if !cleanup(actor, turn, true).await? {
        return Ok(outcome);
    }
    let Ok(batch) = streams_records_tombstone_batch(&actor.state.group_id, &[], now_ms) else {
        return Ok(outcome);
    };
    turn.coordinator
        .offsets_log
        .append(&actor.state.group_id, batch)
        .await?;
    retire_runtime(actor, turn.coordinator);
    outcome.group_deleted = true;
    Ok(outcome)
}

#[cfg(test)]
mod tests;

/// Explicit lifecycle operations stay inside the membership turn through the
/// durable append and registry retirement. Queued joins cannot write afterward.
pub(super) async fn delete(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
) -> Result<(), DeleteGroupError> {
    if turn.coordinator.group_type(&actor.state.group_id) != Some(GroupType::Streams) {
        return Err(DeleteGroupError::NotFound);
    }
    if !actor.state.members.is_empty() {
        return Err(DeleteGroupError::NonEmpty);
    }
    if !cleanup(actor, turn, true)
        .await
        .map_err(|_| DeleteGroupError::Internal)?
    {
        return Err(DeleteGroupError::TopologyDescription);
    }
    let id = &actor.state.group_id;
    let batch = streams_records_tombstone_batch(id, &[], chrono_now_ms())
        .map_err(|_| DeleteGroupError::Internal)?;
    if let Some(home) = turn.coordinator.find(id) {
        ask(&home.tx, |reply| GroupActorMessage::StreamsDelete {
            batch,
            reply,
        })
        .await
        .map_err(|_| DeleteGroupError::Internal)?
        .map_err(|_| DeleteGroupError::Internal)?;
    } else {
        turn.coordinator
            .offsets_log
            .append(id, batch)
            .await
            .map_err(|_| DeleteGroupError::Internal)?;
    }
    retire_runtime(actor, turn.coordinator);
    turn.coordinator.groups.remove(id);
    turn.coordinator.seeds.remove(id);
    turn.coordinator.remove_cached_seed(id);
    turn.coordinator.streams_seeds.remove(id);
    turn.coordinator.streams_seeds_cache.remove(id);
    turn.coordinator.forget_group_metrics(id);
    retire_registry_owner(turn.coordinator, id);

    Ok(())
}

pub(super) async fn convert(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
    member_id: &str,
    now_ms: i64,
) -> Result<super::super::migration::DowngradeOutcome, BrokerError> {
    use super::super::migration::{DowngradeOutcome, streams_to_classic_batch};
    if turn.coordinator.group_type(&actor.state.group_id) != Some(GroupType::Streams) {
        return Ok(DowngradeOutcome::NotStreams);
    }
    if !actor.state.members.is_empty() {
        return Ok(DowngradeOutcome::RejectLiveMembers);
    }
    if !prepare_conversion(actor, turn).await? {
        return Ok(DowngradeOutcome::RetryCleanup);
    }
    let id = &actor.state.group_id;
    if !member_id.is_empty() {
        turn.coordinator.streams_seeds.remove(id);
        turn.coordinator.streams_seeds_cache.remove(id);
        retire_runtime(actor, turn.coordinator);
        retire_registry_owner(turn.coordinator, id);
        return Ok(DowngradeOutcome::Removed);
    }
    turn.coordinator
        .offsets_log
        .append(id, streams_to_classic_batch(id, now_ms)?)
        .await?;
    retire_runtime(actor, turn.coordinator);
    turn.coordinator.mark_classic_after_streams_downgrade(id);
    turn.coordinator.streams_groups.remove(id);
    Ok(DowngradeOutcome::Converted)
}

/// Keep creation blocked until the old actor's type lock is gone.
fn retire_registry_owner(coordinator: &GroupCoordinator, id: &str) {
    if let Entry::Occupied(entry) = coordinator.streams_groups.entry(id.to_owned()) {
        coordinator.group_types.remove(id);
        entry.remove();
    }
}

/// Retire only the runtime held by this actor, never a replacement's entry.
pub(super) fn retire_runtime(actor: &ActorState, coordinator: &GroupCoordinator) {
    coordinator
        .streams_description_runtime
        .remove_if(&actor.state.group_id, |_, runtime| {
            Arc::ptr_eq(runtime, &actor.description_runtime)
        });
}

/// Observe completed callbacks only. Dropped/panicking futures are not reported
/// as successes; log persistence and in-memory reference storage are excluded.
async fn observe_native<T>(
    coordinator: &GroupCoordinator,
    operation: TopologyStorageOperation,
    future: impl Future<Output = Result<T, StorageFailure>>,
) -> Result<T, StorageFailure> {
    let started = Instant::now();
    let result = future.await;
    if let Some(metrics) = coordinator.metrics.get() {
        metrics.record_topology_storage(operation, result.is_ok(), started.elapsed());
    }
    result
}
