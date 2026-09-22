//! The `StreamsGroupHeartbeat` exchange and the session-expiry tick.
//!
//! Every membership change a streams group makes arrives here: a first join
//! that mints a member id, a steady-state heartbeat that reports owned tasks
//! and changelog offsets, a leave at `member_epoch == -1`, and the eviction of
//! members that went silent past the session timeout. Each path reconciles
//! when the group is dirty and then writes the resulting records as one batch,
//! so a failed log write ends the actor.

use std::{sync::Arc, time::Instant};

use krabka_protocol::owned::{
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use super::{
    ActorState, chrono_now_ms,
    reconciliation::{configure_after_load, reconcile},
    records::{flush_pending, snapshot_pending_after_change},
    request::{build_member, task_ids_to_map, task_offsets_to_map},
    response::{ResponseDelta, build_assignment_resp, endpoint_to_partitions, error_resp},
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, GroupCoordinator, first_join_member_id,
        offsets_log::OffsetsLog,
        streams::{
            config::StreamsGroupConfig,
            state::{OwnedTasks, RoleTasks, StoredTopologyHandle},
            topology,
        },
    },
    metadata_source::MetadataSource,
};

/// Evict members silent past the session timeout, fence members past their
/// rebalance timeout, reconcile, and persist the resulting tombstones. Returns `Err` if the log write fails (the actor exits).
pub(super) async fn handle_session_tick(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let now = Instant::now();
    let mut evicted = actor.state.evict_expired(now, config.session_timeout);
    // A member that did not revoke its tasks within its rebalance timeout is
    // fenced like a member whose session expired
    // (`scheduleStreamsGroupRebalanceTimeout`).
    evicted.extend(actor.state.fence_rebalance_timeouts(now));
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` set `dirty`; reconcile owns the single `bump_epoch`.
    reconcile(actor, config, metadata_source).await;
    let mut pending = snapshot_pending_after_change(actor, &[]);
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await
}

pub(super) async fn handle_heartbeat(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<StreamsGroupHeartbeatResponse, crate::error::BrokerError> {
    let ClientIdentity {
        id: client_id,
        host: client_host,
    } = client;
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    // Kafka's `groups.containsKey`: a group that this heartbeat creates
    // reports endpoint information epoch 0.
    let group_existed = actor.state.group_epoch > 0;

    if let Some(resp) = changelog_partition_count_error(req) {
        return Ok(resp);
    }

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(
            actor,
            config,
            offsets_log,
            metadata_source,
            coordinator,
            req,
            now_ms,
        )
        .await;
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-1071 mirrors KIP-848: epoch 0 from an unknown member is a first
    // join. The client may supply its own id; an empty id mints a server UUID.
    // Epoch 0 from a known member is a rejoin and takes the existing-member
    // path below.
    if req.member_epoch == 0 && !actor.state.members.contains_key(&req.member_id) {
        if actor.state.members.len() >= config.max_size {
            return Ok(error_resp(
                codes::GROUP_MAX_SIZE_REACHED,
                Some(format!(
                    "The streams group has reached its maximum capacity of {} members.",
                    config.max_size
                )),
            ));
        }
        if let Some(resp) = topology_error(actor, req, metadata_source) {
            return Ok(resp);
        }
        let new_member_id = first_join_member_id(&req.member_id);
        let m = build_member(&new_member_id, req, client_id, client_host, now);
        actor.state.add_or_update_member(m);
        // A topology on a join initializes the group topology, or replaces an
        // older one. A join with an older topology keeps the group topology,
        // and its responses carry `STALE_TOPOLOGY`.
        if let Some(topo) = &req.topology
            && actor
                .topology
                .as_ref()
                .is_none_or(|group| topo.epoch > group.epoch)
        {
            accept_topology(actor, topo);
        }
        reconcile(actor, config, metadata_source).await;
        if req.shutdown_application {
            actor.state.request_shutdown(&new_member_id);
        }
        if actor
            .state
            .reconcile_member(&new_member_id, owned_role_tasks(req).as_ref())
        {
            actor.state.track_rebalance_timeout(&new_member_id, now);
        }
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&new_member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
        return Ok(accepted_response(
            actor,
            config,
            metadata_source,
            req,
            &new_member_id,
            &MemberBefore::default(),
            group_existed,
        ));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    // The owned-task maps matter only for a heartbeat at the previous member
    // epoch, so a heartbeat at epoch 0 or at the member epoch builds none.
    let needs_owned = actor.state.members.get(&req.member_id).is_some_and(|m| {
        req.member_epoch != 0
            && req.member_epoch != m.member_epoch
            && req.member_epoch == m.previous_member_epoch
    });
    let owned_maps = needs_owned.then(|| {
        (
            req.active_tasks.as_deref().map(task_ids_to_map),
            req.standby_tasks.as_deref().map(task_ids_to_map),
            req.warmup_tasks.as_deref().map(task_ids_to_map),
        )
    });
    let owned =
        owned_maps
            .as_ref()
            .map_or_else(OwnedTasks::default, |(active, standby, warmup)| {
                OwnedTasks {
                    active: active.as_ref(),
                    standby: standby.as_ref(),
                    warmup: warmup.as_ref(),
                }
            });
    if let Err(error_code) =
        actor
            .state
            .validate_heartbeat_epoch(&req.member_id, req.member_epoch, owned)
    {
        return Ok(error_resp(
            error_code,
            epoch_error_message(actor, req, error_code),
        ));
    }
    let before = MemberBefore::of(&actor.state.members[&req.member_id]);
    if let Some(resp) = topology_error(actor, req, metadata_source) {
        return Ok(resp);
    }

    // ─── Steady state ────────────────────────────────────────────
    let mut changed = update_member_steady_state(actor, req, client_id, client_host, now);
    // A newer topology replaces the group topology. A member with an older
    // one gets `STALE_TOPOLOGY` in its own responses.
    if let Some(topo) = &req.topology
        && topo.epoch > actor.state.topology_epoch
    {
        accept_topology(actor, topo);
        changed = true;
    }
    refresh_topic_metadata(actor, config, metadata_source).await;

    if actor.state.dirty {
        reconcile(actor, config, metadata_source).await;
        changed = true;
    }
    // Kafka's `maybeReconcile`: move the member toward the target, and arm
    // or cancel its rebalance timeout when its assignment changed.
    if actor
        .state
        .reconcile_member(&req.member_id, owned_role_tasks(req).as_ref())
    {
        actor.state.track_rebalance_timeout(&req.member_id, now);
        changed = true;
    }
    if req.shutdown_application {
        actor.state.request_shutdown(&req.member_id);
    }

    if changed {
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&req.member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    }
    Ok(accepted_response(
        actor,
        config,
        metadata_source,
        req,
        &req.member_id,
        &before,
        group_existed,
    ))
}

/// The owned tasks of a heartbeat, when it reports all three roles, as
/// Kafka's `TasksTuple.fromHeartbeatRequest` builds them.
fn owned_role_tasks(req: &StreamsGroupHeartbeatRequest) -> Option<RoleTasks> {
    match (&req.active_tasks, &req.standby_tasks, &req.warmup_tasks) {
        (Some(active), Some(standby), Some(warmup)) => Some(RoleTasks {
            active: task_ids_to_map(active),
            standby: task_ids_to_map(standby),
            warmup: task_ids_to_map(warmup),
        }),
        _ => None,
    }
}

/// The user endpoint of a member before a heartbeat changed it. A joining
/// member starts from none.
#[derive(Default)]
struct MemberBefore {
    user_endpoint: Option<(String, u16)>,
}

impl MemberBefore {
    fn of(member: &crate::coordinator::unified::streams::state::StreamsMemberState) -> Self {
        Self {
            user_endpoint: member.user_endpoint.clone(),
        }
    }
}

/// Builds the response of an accepted heartbeat, as the end of Kafka's
/// `streamsGroupHeartbeat` does.
///
/// The task lists go out when the member joins or its tasks changed. The
/// group's endpoint information epoch goes up when the member's endpoint
/// changed, or its tasks changed and it has an endpoint. Kafka compares the
/// tasks before and after the heartbeat, because only the member's own
/// heartbeat changes them. Here a new target also trims the tasks of the
/// other members, so the comparison is with the tasks that the last response
/// sent. A member whose last
/// seen epoch differs from the group's gets the endpoint information of the
/// whole group. A group that this heartbeat creates keeps epoch 0.
fn accepted_response(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    req: &StreamsGroupHeartbeatRequest,
    member_id: &str,
    before: &MemberBefore,
    group_existed: bool,
) -> StreamsGroupHeartbeatResponse {
    let member = &actor.state.members[member_id];
    let tasks = [
        member.active.clone(),
        member.standby.clone(),
        member.warmup.clone(),
    ];
    let tasks_changed = member.sent_tasks != tasks;
    let endpoint_changed = before.user_endpoint != member.user_endpoint;
    let mut endpoint_epoch = actor.state.endpoint_information_epoch;
    if endpoint_changed || (tasks_changed && member.user_endpoint.is_some()) {
        endpoint_epoch = endpoint_epoch.saturating_add(1);
    }
    let partitions_by_user_endpoint =
        (endpoint_epoch != req.endpoint_information_epoch).then(|| {
            let image = metadata_source.map(|source| source.current_image());
            let configured = actor
                .topology
                .as_ref()
                .zip(image.as_ref())
                .and_then(|(topology, image)| topology::configure_topics(topology, image).ok());
            endpoint_to_partitions(
                &actor.state,
                member_id,
                configured.as_ref().and_then(|c| c.subtopologies.as_ref()),
                image.as_deref(),
            )
        });
    if group_existed {
        actor.state.endpoint_information_epoch = endpoint_epoch;
    }
    if let Some(member) = actor.state.members.get_mut(member_id) {
        member.sent_tasks = tasks;
    }
    build_assignment_resp(
        &actor.state,
        member_id,
        config,
        ResponseDelta {
            send_tasks: req.member_epoch == 0 || tasks_changed,
            endpoint_information_epoch: actor.state.endpoint_information_epoch,
            partitions_by_user_endpoint,
        },
    )
}

/// The `error_message` of a heartbeat that the member epoch check refused, in
/// the words of Kafka's `getMemberOrThrow` and
/// `throwIfStreamsGroupMemberEpochIsInvalid`.
fn epoch_error_message(
    actor: &ActorState,
    req: &StreamsGroupHeartbeatRequest,
    error_code: i16,
) -> Option<String> {
    let Some(member) = actor.state.members.get(&req.member_id) else {
        return Some(format!(
            "Member {} is not a member of group {}.",
            req.member_id, actor.state.group_id
        ));
    };
    (error_code == codes::FENCED_MEMBER_EPOCH).then(|| {
        let relation = if req.member_epoch > member.member_epoch {
            "greater"
        } else {
            "smaller"
        };
        format!(
            "The streams group member has a {relation} member epoch ({}) than the one known by \
             the group coordinator ({}). The member must abandon all its partitions and rejoin.",
            req.member_epoch, member.member_epoch
        )
    })
}

/// Kafka's `GroupCoordinatorService.throwIfInvalidTopology`, which runs on a
/// joining heartbeat before the coordinator: a changelog topic must leave its
/// partition count undefined, because the coordinator decides it.
fn changelog_partition_count_error(
    req: &StreamsGroupHeartbeatRequest,
) -> Option<StreamsGroupHeartbeatResponse> {
    if req.member_epoch != 0 {
        return None;
    }
    let topic = req
        .topology
        .iter()
        .flat_map(|topology| topology.subtopologies.iter())
        .flat_map(|subtopology| subtopology.state_changelog_topics.iter())
        .find(|topic| topic.partitions != 0)?;
    Some(error_resp(
        codes::STREAMS_INVALID_TOPOLOGY,
        Some(format!(
            "Changelog topic {} must have an undefined partition count, but it is set to {}.",
            topic.name, topic.partitions
        )),
    ))
}

/// The error response for a topology that Kafka's `configureTopics` refuses
/// against the current metadata image, or `None` when the topology can be
/// configured.
///
/// The topology is the one that this heartbeat leaves the group with: the
/// topology of the request when the group has none or an older one, else the
/// topology of the group. Kafka configures the topology inside the heartbeat
/// and answers the exception with its code and message, and it writes nothing.
fn topology_error(
    actor: &ActorState,
    req: &StreamsGroupHeartbeatRequest,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) -> Option<StreamsGroupHeartbeatResponse> {
    let source = metadata_source?;
    let from_request = req
        .topology
        .as_ref()
        .filter(|wire| {
            actor
                .topology
                .as_ref()
                .is_none_or(|group| wire.epoch > group.epoch)
        })
        .map(topology::to_stored_topology);
    let topology = from_request.as_ref().or(actor.topology.as_ref())?;
    let error = topology::configure_topics(topology, &source.current_image()).err()?;
    Some(error_resp(error.error_code(), error.error_message()))
}

/// Marks the group for a reconcile when a topic that the topology needs
/// changed since the last reconcile.
///
/// Kafka's `onMetadataUpdate` requests a metadata refresh for every streams
/// group that uses a created, changed or deleted topic, and the next heartbeat
/// computes the metadata hash again. A new hash configures the topology again
/// and bumps the group epoch. This function first tries again to create the
/// internal topics that the last reconcile could not create, as Kafka creates
/// the missing internal topics on every heartbeat.
async fn refresh_topic_metadata(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    let Some(source) = metadata_source else {
        return;
    };
    if !actor.state.dirty {
        configure_after_load(actor, source);
    }
    let Some(topology) = actor.topology.as_ref() else {
        return;
    };
    if !actor.state.dirty
        && !actor.missing_internal_topics.is_empty()
        && let Err(error) = topology::ensure_internal_topics(
            source,
            &actor.missing_internal_topics,
            config.internal_topic_replication_factor,
        )
        .await
    {
        tracing::warn!(
            group_id = %actor.state.group_id,
            %error,
            "streams internal topic creation failed again",
        );
    }
    if topology::metadata_hash(topology, &source.current_image()) != actor.metadata_hash {
        actor.state.dirty = true;
    }
}

/// Updates a steady-state member's reported ownership, catch-up offsets, and
/// `last_seen`. Returns `true` if anything that needs persistence changed.
fn update_member_steady_state(
    actor: &mut ActorState,
    req: &StreamsGroupHeartbeatRequest,
    client_id: &str,
    client_host: &str,
    now: Instant,
) -> bool {
    let Some(m) = actor.state.members.get_mut(&req.member_id) else {
        return false;
    };
    m.last_seen = now;
    let mut changed = false;

    if m.client_id != client_id {
        m.client_id = client_id.to_string();
        changed = true;
    }
    if m.client_host != client_host {
        m.client_host = client_host.to_string();
        changed = true;
    }
    if update_member_metadata(m, req) {
        // Kafka's `hasStreamsMemberMetadataChanged`: a changed member bumps
        // the group epoch, so the assignor sees the new process, rack, tags
        // and endpoint.
        actor.state.dirty = true;
        changed = true;
    }

    if let Some(offsets) = &req.task_offsets {
        let map = task_offsets_to_map(offsets);
        if map != m.task_offsets {
            m.task_offsets = map;
            changed = true;
        }
    }
    if let Some(end_offsets) = &req.task_end_offsets {
        let map = task_offsets_to_map(end_offsets);
        if map != m.task_end_offsets {
            m.task_end_offsets = map;
            changed = true;
        }
    }
    changed
}

/// Applies the member fields of a heartbeat to a known member, as Kafka's
/// `StreamsGroupMember.Builder.maybeUpdate*` calls do: a field that the
/// request carries replaces the stored value, and an absent field (or a
/// rebalance timeout of -1) keeps it. A rejoin at epoch 0 sets the user
/// endpoint also when the request has none. Returns `true` if a field changed.
fn update_member_metadata(
    m: &mut crate::coordinator::unified::streams::state::StreamsMemberState,
    req: &StreamsGroupHeartbeatRequest,
) -> bool {
    let before = (
        m.instance_id.clone(),
        m.rack_id.clone(),
        m.rebalance_timeout_ms,
        m.topology_epoch,
        m.process_id.clone(),
        m.user_endpoint.clone(),
        m.client_tags.clone(),
    );
    if req.instance_id.is_some() {
        m.instance_id.clone_from(&req.instance_id);
    }
    if req.rack_id.is_some() {
        m.rack_id.clone_from(&req.rack_id);
    }
    if req.rebalance_timeout_ms != -1 {
        m.rebalance_timeout_ms = req.rebalance_timeout_ms;
    }
    if let Some(topology) = &req.topology {
        m.topology_epoch = topology.epoch;
    }
    if let Some(process_id) = &req.process_id {
        m.process_id.clone_from(process_id);
    }
    let endpoint = req
        .user_endpoint
        .as_ref()
        .map(|endpoint| (endpoint.host.clone(), endpoint.port));
    if req.member_epoch == 0 || endpoint.is_some() {
        m.user_endpoint = endpoint;
    }
    if let Some(tags) = &req.client_tags {
        m.client_tags = tags
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
    }
    before
        != (
            m.instance_id.clone(),
            m.rack_id.clone(),
            m.rebalance_timeout_ms,
            m.topology_epoch,
            m.process_id.clone(),
            m.user_endpoint.clone(),
            m.client_tags.clone(),
        )
}

/// Handles a leave-group heartbeat, where `member_epoch == -1`.
async fn handle_leave(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<StreamsGroupHeartbeatResponse, crate::error::BrokerError> {
    // Kafka's `streamsGroupLeave` records the shutdown request before it looks
    // the member up with `getMemberOrThrow`: an unknown member gets
    // `UNKNOWN_MEMBER_ID`, and nothing is written.
    if req.shutdown_application {
        actor.state.request_shutdown(&req.member_id);
    }
    if actor.state.remove_member(&req.member_id).is_none() {
        return Ok(error_resp(
            codes::UNKNOWN_MEMBER_ID,
            Some(format!(
                "Member {} is not a member of group {}.",
                req.member_id, actor.state.group_id
            )),
        ));
    }
    // `remove_member` set `dirty`; reconcile owns the single `bump_epoch`.
    reconcile(actor, config, metadata_source).await;
    let mut pending = snapshot_pending_after_change(actor, &[]);
    pending.member_metadata.push((req.member_id.clone(), None));
    pending
        .target_per_member
        .push((req.member_id.clone(), None));
    pending
        .current_per_member
        .push((req.member_id.clone(), None));
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    // Kafka's leave response echoes the member id and epoch, and sends an
    // empty status list and no group configuration.
    Ok(StreamsGroupHeartbeatResponse {
        member_id: req.member_id.clone(),
        member_epoch: req.member_epoch,
        status: Some(Vec::new()),
        ..Default::default()
    })
}

/// Accepts a client-supplied topology. It stores the resolved value for
/// persistence and reconcile, stamps the epoch on the state handle, and marks
/// the group dirty.
fn accept_topology(
    actor: &mut ActorState,
    wire_topology: &krabka_protocol::owned::streams_group_heartbeat_request::Topology,
) {
    let stored = topology::to_stored_topology(wire_topology);
    actor.state.topology = Some(StoredTopologyHandle {
        epoch: stored.epoch,
    });
    actor.state.topology_epoch = stored.epoch;
    actor.topology = Some(stored);
    actor.state.dirty = true;
}
