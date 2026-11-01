//! KIP-1331 inside the streams-group actor: asking a member for the topology
//! description, storing what it pushes, and serving it to a describe.
//!
//! Kafka splits this between the shard, which keeps the group's
//! `StoredDescriptionTopologyEpoch` and `FailedDescriptionTopologyEpoch`, and
//! the broker-wide `StreamsGroupTopologyDescriptionManager`, which owns the
//! plugin and the back-off. The actor keeps all three for its group, so each
//! step below runs in the one turn of the actor that handles the message.
//!
//! As in released Kafka 4.4, persist UNCERTAIN before backend mutation and
//! the final epoch only after success. Session checks retry uncertain and
//! stale-epoch cleanup; permanent store rejection suppresses solicitation for
//! that epoch while transient failure retains exponential heartbeat backoff.

use {
    super::{ActorState, StreamsTurn, chrono_now_ms, records::group_metadata_value},
    crate::{
        codes,
        coordinator::unified::streams::{
            config::StreamsGroupConfig,
            description::{
                DescriptionEpochs, Jitter, StorageFailure, StoredDescription, TopologyDescription,
            },
            persistence::PendingStreamsRecords,
            topology::status,
        },
        error::BrokerError,
    },
    krabka_protocol::owned::{
        common::streams_group_topology_description_update_request::topology_description::TopologyDescription as WireDescription,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::time::Instant,
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
    actor: &mut ActorState,
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
        .description_backoff
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
        backend.set(&group_id, topology_epoch, &description).await
    } else {
        actor.description = Some(StoredDescription {
            topology_epoch,
            description,
        });
        Ok(())
    };
    match result {
        Ok(()) => actor.description_epochs.stored = topology_epoch,
        Err(failure) => {
            if failure == StorageFailure::Permanent {
                actor.description_epochs.failed = topology_epoch;
                persist_epochs(actor, turn).await?;
            }
            if failure == StorageFailure::Permanent {
                actor.description_backoff.clear(topology_epoch);
            } else {
                actor.description_backoff.arm_or_extend(
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
    actor.description_backoff.clear(topology_epoch);
    Ok((codes::NONE, None))
}

/// What `StreamsGroupTopologyDescriptionManager.attachTopologyDescriptions`
/// finds for the group: the stored description, when the group's recorded
/// epoch says the plugin holds the current topology epoch and the in-memory
/// plugin still holds a description of that epoch, as its `getTopology`
/// answers.
pub(super) async fn stored_description(
    actor: &ActorState,
    config: &StreamsGroupConfig,
) -> Result<Option<TopologyDescription>, StorageFailure> {
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
        return backend.get(&actor.state.group_id, topology_epoch).await;
    }
    Ok(actor
        .description
        .as_ref()
        .filter(|stored| stored.topology_epoch == topology_epoch)
        .map(|stored| stored.description.clone()))
}

/// Persist a backend operation's epoch boundary.
async fn persist_epochs(actor: &mut ActorState, turn: StreamsTurn<'_>) -> Result<(), BrokerError> {
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
    if stored == DescriptionEpochs::NONE
        || (!force && (stored == current_topology_epoch(actor) || !actor.state.members.is_empty()))
    {
        return Ok(true);
    }
    actor.description_epochs.stored = DescriptionEpochs::UNCERTAIN;
    persist_epochs(actor, turn).await?;
    if let Some(backend) = &turn.config.topology_description_backend {
        if backend.delete(&actor.state.group_id).await.is_err() {
            return Ok(false);
        }
    }
    actor.description = None;
    actor.description_epochs.stored = DescriptionEpochs::NONE;
    actor.description_backoff = Default::default();
    persist_epochs(actor, turn).await?;
    Ok(true)
}

/// A classic join must not convert over orphaned plugin state. Failed
/// conversion deletes are throttled at the UNCERTAIN epoch as in Kafka 4.4.
pub(super) async fn prepare_conversion(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
) -> Result<bool, BrokerError> {
    if actor.description_epochs.stored == DescriptionEpochs::NONE {
        return Ok(true);
    }
    if !actor.description_backoff.arm_if_not_active(
        DescriptionEpochs::UNCERTAIN,
        Instant::now(),
        Jitter::draw(),
    ) {
        return Ok(false);
    }
    let result = cleanup(actor, turn, true).await?;
    if result {
        actor.description_backoff = Default::default();
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
