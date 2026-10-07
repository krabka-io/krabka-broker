//! KIP-1331 inside the streams-group actor: asking a member for the topology
//! description, storing what it pushes, and serving it to a describe.
//!
//! Kafka splits this between the shard, which keeps the group's
//! `StoredDescriptionTopologyEpoch` and `FailedDescriptionTopologyEpoch`, and
//! the broker-wide `StreamsGroupTopologyDescriptionManager`, which owns the
//! plugin and the back-off. The actor keeps all three for its group, so each
//! step below runs in the one turn of the actor that handles the message.
//!
//! Kafka trunk writes the group metadata record twice per push, with the
//! epochs in KIP-1331's tags 2 and 3. Kafka 4.3.1, whose records the broker
//! writes, has neither KIP-1331 nor those tags, so a push writes nothing: the
//! actor keeps the epochs in memory beside the in-memory plugin's
//! description, and both go when the actor does.

use std::time::Instant;

use krabka_protocol::owned::{
    common::streams_group_topology_description_update_request::topology_description::TopologyDescription as WireDescription,
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use super::ActorState;
use crate::{
    codes,
    coordinator::unified::streams::{
        config::StreamsGroupConfig,
        description::{Jitter, StoredDescription, TopologyDescription},
        topology::status,
    },
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
        || !config.topology_description_plugin.is_configured()
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
/// description, and records its epoch in memory.
pub(super) fn handle_push(actor: &mut ActorState, push: &DescriptionPush) -> PushAnswer {
    let group_id = actor.state.group_id.clone();
    // `GroupMetadataManager.validateStreamsGroupTopologyDescriptionUpdate`.
    if actor.holds_nothing() {
        return (
            codes::GROUP_ID_NOT_FOUND,
            Some(format!("Group {group_id} not found.")),
        );
    }
    if !actor.state.members.contains_key(&push.member_id) {
        return (
            codes::UNKNOWN_MEMBER_ID,
            Some(format!(
                "Member {} is not a member of group {group_id}.",
                push.member_id
            )),
        );
    }
    let topology_epoch = current_topology_epoch(actor);
    if push.topology_epoch != topology_epoch {
        return (
            codes::INVALID_REQUEST,
            Some(format!(
                "Topology epoch {} does not match the group's current topology epoch \
                 {topology_epoch}.",
                push.topology_epoch
            )),
        );
    }
    let description = match TopologyDescription::from_push(&push.description) {
        Ok(description) => description,
        Err(message) => return (codes::INVALID_REQUEST, Some(message)),
    };
    // `InMemoryTopologyDescriptionPlugin.setTopology` replaces what the group
    // held, and `setStoredDescriptionTopologyEpoch` records the epoch.
    actor.description = Some(StoredDescription {
        topology_epoch,
        description,
    });
    actor.description_epochs.stored = topology_epoch;
    actor.description_backoff.clear(topology_epoch);
    (codes::NONE, None)
}

/// What `StreamsGroupTopologyDescriptionManager.attachTopologyDescriptions`
/// finds for the group: the stored description, when the group's recorded
/// epoch says the plugin holds the current topology epoch and the in-memory
/// plugin still holds a description of that epoch, as its `getTopology`
/// answers.
pub(super) fn stored_description(actor: &ActorState) -> Option<TopologyDescription> {
    let topology_epoch = actor.state.topology.as_ref()?.epoch;
    if !actor.description_epochs.holds(topology_epoch) {
        return None;
    }
    actor
        .description
        .as_ref()
        .filter(|stored| stored.topology_epoch == topology_epoch)
        .map(|stored| stored.description.clone())
}

#[cfg(test)]
mod tests;
