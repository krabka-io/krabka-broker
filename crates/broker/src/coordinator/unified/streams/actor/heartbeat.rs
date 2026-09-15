//! The `StreamsGroupHeartbeat` exchange and the session-expiry tick.
//!
//! Every membership change a streams group makes arrives here: a first join
//! that mints a member id, a steady-state heartbeat that reports owned tasks
//! and changelog offsets, a leave at `member_epoch == -1`, and the eviction of
//! members that went silent past the session timeout. Each path reconciles
//! when the group is dirty and then writes the resulting records as one batch,
//! so a failed log write ends the actor.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use krabka_protocol::owned::{
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use super::{
    ActorState, chrono_now_ms,
    reconciliation::reconcile,
    records::{flush_pending, snapshot_pending_after_change},
    request::{build_member, task_ids_to_map, task_offsets_to_map},
    response::{base_resp, build_assignment_resp, error_resp},
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, GroupCoordinator, first_join_member_id,
        offsets_log::OffsetsLog,
        streams::{
            config::StreamsGroupConfig,
            state::{OwnedTasks, StoredTopologyHandle},
            topology::{self, status as topo_status},
        },
    },
    metadata_source::MetadataSource,
};

/// Evict members silent past the session timeout, reconcile, and persist the
/// resulting tombstones. Returns `Err` if the log write fails (the actor exits).
pub(super) async fn handle_session_tick(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let evicted = actor
        .state
        .evict_expired(Instant::now(), config.session_timeout);
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
            return Ok(error_resp(codes::GROUP_MAX_SIZE_REACHED, config));
        }
        let new_member_id = first_join_member_id(&req.member_id);
        let m = build_member(&new_member_id, req, client_id, client_host, now);
        actor.state.add_or_update_member(m);
        // Topology supplied on first join is accepted before reconcile.
        if let Some(topo) = &req.topology {
            accept_topology(actor, topo);
        }
        apply_shutdown_application(actor, req);
        reconcile(actor, config, metadata_source).await;
        actor.state.advance_member_epoch(&new_member_id);
        let reported = req
            .active_tasks
            .as_ref()
            .map(|active| task_ids_to_map(active))
            .unwrap_or_default();
        actor.state.reconcile_member(&new_member_id, &reported);
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&new_member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
        return Ok(build_assignment_resp(&actor.state, &new_member_id, config));
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
    let cur_epoch =
        match actor
            .state
            .validate_heartbeat_epoch(&req.member_id, req.member_epoch, owned)
        {
            Ok(epoch) => epoch,
            Err(error_code) => return Ok(error_resp(error_code, config)),
        };

    // ─── Steady state ────────────────────────────────────────────
    let mut changed = update_member_steady_state(actor, req, client_id, client_host, now);
    // Topology handling: newer epoch is accepted, older is flagged STALE.
    if let Some(topo) = &req.topology {
        let cur_topo_epoch = actor.state.topology_epoch;
        if topo.epoch > cur_topo_epoch {
            accept_topology(actor, topo);
            changed = true;
        } else if topo.epoch < cur_topo_epoch {
            set_status(
                actor,
                topo_status::STALE_TOPOLOGY,
                "member reported a stale topology",
            );
        }
    }
    if apply_shutdown_application(actor, req) {
        changed = true;
    }

    if actor.state.dirty {
        reconcile(actor, config, metadata_source).await;
        changed = true;
    }
    // If the member's target advanced past its current epoch, hand it over.
    if actor.state.target.epoch > cur_epoch {
        actor.state.advance_member_epoch(&req.member_id);
        changed = true;
    }
    let reported = req.active_tasks.as_ref().map_or_else(
        || {
            let Some(member) = actor.state.members.get(&req.member_id) else {
                return BTreeMap::new();
            };
            let mut reported = member.active.clone();
            for (subtopology, partitions) in &member.active_pending_revocation {
                reported
                    .entry(subtopology.clone())
                    .or_default()
                    .extend(partitions.iter().copied());
            }
            reported
        },
        |active| task_ids_to_map(active),
    );
    if actor.state.reconcile_member(&req.member_id, &reported) {
        changed = true;
    }

    if changed {
        let pending = snapshot_pending_after_change(actor, std::slice::from_ref(&req.member_id));
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    }
    Ok(build_assignment_resp(&actor.state, &req.member_id, config))
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
    // Kafka's `streamsGroupLeave` looks the member up with `getMemberOrThrow`:
    // an unknown member gets `UNKNOWN_MEMBER_ID`, and nothing is written.
    if actor.state.remove_member(&req.member_id).is_none() {
        return Ok(error_resp(codes::UNKNOWN_MEMBER_ID, config));
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
    Ok(base_resp(codes::NONE, -1, config))
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

/// KIP-1071 shutdown-application: any member can signal the whole group to
/// shut down. This function records the signal as a group status, so later
/// responses carry it. It returns `true` if it added the status.
fn apply_shutdown_application(actor: &mut ActorState, req: &StreamsGroupHeartbeatRequest) -> bool {
    if !req.shutdown_application {
        return false;
    }
    set_status(
        actor,
        topo_status::SHUTDOWN_APPLICATION,
        "a member requested application shutdown",
    )
}

/// Adds a `(code, detail)` pair to the group status if no entry with that code
/// is present. Returns `true` if the function added the pair.
fn set_status(actor: &mut ActorState, code: i8, detail: &str) -> bool {
    if actor.state.status.iter().any(|(c, _)| *c == code) {
        return false;
    }
    actor.state.status.push((code, detail.to_string()));
    true
}
