//! The KIP-848 `ConsumerGroupHeartbeat` path.
//!
//! [`step_heartbeat`] is the pure decision core — epoch validation, member
//! upsert or leave, reconciliation, and the response build — with no `.await`
//! and no I/O, so the reconciliation policy is model-checkable on its own. The
//! async wrappers around it flush the records it produces and drive the
//! in-place upgrade a heartbeat against a classic group triggers.

use std::time::Instant;

use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{
        Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use super::{
    ActorServices, ErrorCode, FALLBACK_HEARTBEAT_INTERVAL_MS, MetadataProvider, chrono_now_ms,
    downgrade::maybe_downgrade,
    member_state::{reported_owned, run_reconcile, try_build_member, update_member_state},
    pending_records::PendingRecords,
    persistence::{flush_pending, snapshot_pending_after_change},
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, GroupCoordinator,
        config::NextGenConfig,
        consumer_state::GroupState,
        first_join_member_id,
        group::{CoordinatorGroup, GroupKind},
        migration,
        offsets_log::OffsetsLog,
        validate_member_epoch,
    },
};

#[cfg(test)]
mod tests;

pub(super) async fn handle_actor_heartbeat(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    request: ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
) -> bool {
    if group.is_classic() {
        let convertible = group
            .as_classic()
            .is_some_and(migration::classic_is_convertible);
        if !services.config.migration_policy.allows_upgrade() || !convertible {
            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            return true;
        }
        let classic = group.as_classic().expect("classic kind");
        let new_state = migration::convert_classic_to_consumer(classic);
        let pending = migration::upgrade_pending_records(&new_state);
        if flush_pending(
            &new_state,
            pending,
            services.offsets_log,
            services.coordinator,
            chrono_now_ms(),
        )
        .await
        .is_err()
        {
            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                ..Default::default()
            });
            return false;
        }
        *group.kind_mut() = GroupKind::Consumer(new_state);
    }

    let Some(state) = group.as_consumer_mut() else {
        let _ = reply.send(ConsumerGroupHeartbeatResponse {
            error_code: codes::GROUP_ID_NOT_FOUND,
            ..Default::default()
        });
        return true;
    };
    match handle_heartbeat(
        state,
        services.config,
        services.metadata,
        services.offsets_log,
        services.coordinator,
        &request,
        client,
    )
    .await
    {
        Ok(response) => {
            let _ = reply.send(response);
        }
        Err(error) => {
            tracing::warn!(group_id = %group.group_id, %error,
                "next-gen actor exiting after log-write failure");
            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                ..Default::default()
            });
            return false;
        }
    }
    if let Err(error) = maybe_downgrade(
        group,
        services.config,
        services.metadata,
        services.offsets_log,
        services.coordinator,
    )
    .await
    {
        tracing::warn!(group_id = %group.group_id, %error,
            "next-gen actor exiting after downgrade log-write failure");
        return false;
    }
    true
}

/// Outcome of the pure heartbeat decision phase: the response to return to the
/// client and the records the async caller must append to the offsets log.
pub(crate) struct HeartbeatStep {
    pub response: ConsumerGroupHeartbeatResponse,
    pub pending: PendingRecords,
}

/// The pure, synchronous heartbeat decision core: assignor selection and epoch
/// validation, member upsert or leave, `update_member_state`, `run_reconcile`,
/// `advance_member_epoch`, and the response build.
///
/// This function holds no `.await` and does no I/O. `handle_heartbeat` calls
/// it, then flushes `pending` to the log. It is a separate function so that
/// the reconciliation policy is model-checkable on its own.
pub(crate) fn step_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> HeartbeatStep {
    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return leave_step(state, config, metadata, req);
    }

    // ─── Validate assignor selection ─────────────────────────────
    if req
        .server_assignor
        .as_deref()
        .is_some_and(|name| !config.assignor_enabled(name))
    {
        return HeartbeatStep {
            response: error_resp(codes::UNSUPPORTED_ASSIGNOR, config),
            pending: PendingRecords::default(),
        };
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-848 (finalized): the consumer generates its own member UUID and
    // sends it with `member_epoch == 0` on first join. Treat epoch 0 from a
    // member we don't yet know as a first-join, adopting the client-supplied
    // id. An empty `member_id` is tolerated as a fallback (raw-RPC / older
    // callers) by minting a server-side UUID.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        let new_member_id = first_join_member_id(&req.member_id);
        if let Some(iid) = req.instance_id.as_deref()
            && state
                .current_member_for_instance(iid)
                .and_then(|existing| state.members.get(existing))
                .is_some_and(|m| m.member_epoch != 0)
        {
            return HeartbeatStep {
                response: error_resp(codes::UNRELEASED_INSTANCE_ID, config),
                pending: PendingRecords::default(),
            };
        }
        if state.members.len() >= config.max_size {
            return HeartbeatStep {
                response: error_resp(codes::GROUP_MAX_SIZE_REACHED, config),
                pending: PendingRecords::default(),
            };
        }
        let m = match try_build_member(&new_member_id, req, client, now) {
            Ok(m) => m,
            Err(message) => {
                return HeartbeatStep {
                    response: invalid_regex_resp(message, config),
                    pending: PendingRecords::default(),
                };
            }
        };
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        // Compute the new member's current assignment (grants free target
        // partitions, withholds those still held by others) before responding.
        let owned = reported_owned(req);
        state.reconcile_member(&new_member_id, &owned);
        let pending =
            snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id), true);
        let response = build_assignment_resp(state, &new_member_id, config);
        return HeartbeatStep { response, pending };
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = match validate_member_epoch(
        state.members.get(&req.member_id).map(|m| m.member_epoch),
        req.member_epoch,
    ) {
        Ok(epoch) => epoch,
        Err(error_code) => {
            return HeartbeatStep {
                response: error_resp(error_code, config),
                pending: PendingRecords::default(),
            };
        }
    };

    // ─── Steady-state: update last_seen / subscription / owned ───
    let previous_target_epoch = state.target.epoch;
    let any_change = match update_member_state(state, config, metadata, req, client, now, cur_epoch)
    {
        Ok(changed) => changed,
        Err(message) => {
            return HeartbeatStep {
                response: invalid_regex_resp(message, config),
                pending: PendingRecords::default(),
            };
        }
    };
    let pending = if any_change {
        snapshot_pending_after_change(
            state,
            std::slice::from_ref(&req.member_id),
            state.target.epoch != previous_target_epoch,
        )
    } else {
        PendingRecords::default()
    };
    let response = build_assignment_resp(state, &req.member_id, config);
    HeartbeatStep { response, pending }
}

/// Pure form of the leave path (`member_epoch == -1`). It removes the member,
/// reconciles the survivors, and builds their replacement records plus the
/// departed member's tombstones.
/// The async caller flushes the returned `pending`.
fn leave_step(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    let mut pending = if state.remove_member(&req.member_id).is_some() {
        run_reconcile(state, config, metadata);
        snapshot_pending_after_change(state, &[], true)
    } else {
        PendingRecords::default()
    };
    if !pending.is_empty() {
        pending.member_metadata.push((req.member_id.clone(), None));
        pending
            .target_per_member
            .push((req.member_id.clone(), None));
        pending
            .current_per_member
            .push((req.member_id.clone(), None));
    }
    HeartbeatStep {
        response: base_resp(0, req.member_epoch, config),
        pending,
    }
}

async fn handle_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<ConsumerGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    let step = step_heartbeat(state, config, metadata, req, client, now);
    flush_pending(state, step.pending, offsets_log, coordinator, now_ms).await?;
    Ok(step.response)
}

fn base_resp(
    error_code: ErrorCode,
    member_epoch: i32,
    config: &NextGenConfig,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(FALLBACK_HEARTBEAT_INTERVAL_MS),
        ..Default::default()
    }
}

fn error_resp(error_code: ErrorCode, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

/// The `INVALID_REGULAR_EXPRESSION` (128) rejection Kafka answers to a
/// heartbeat whose `SubscribedTopicRegex` does not compile. Kafka carries the
/// exception's message in `error_message`, so do the same.
fn invalid_regex_resp(message: String, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_message: Some(message),
        ..error_resp(codes::INVALID_REGULAR_EXPRESSION, config)
    }
}

fn build_assignment_resp(
    state: &GroupState,
    member_id: &str,
    config: &NextGenConfig,
) -> ConsumerGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    // KIP-848: the `assignment` field carries the member's *current* assignment —
    // the partitions it may own right now — NOT the raw target. `reconcile_member`
    // computes this each heartbeat, withholding any target partition still held by
    // another member until that member revokes it. Returning the current
    // assignment (`assigned_partitions`) rather than the target is what prevents
    // two members from owning the same partition during a handoff.
    let target_partitions = m.assigned_partitions.clone();
    let assignment = Some(RespAssignment {
        topic_partitions: target_partitions
            .iter()
            .map(
                |(tid, parts)| krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    });
    ConsumerGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}
