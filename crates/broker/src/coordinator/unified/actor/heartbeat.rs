//! The KIP-848 `ConsumerGroupHeartbeat` path.
//!
//! [`step_heartbeat`] is the pure decision core — epoch validation, member
//! upsert or leave, reconciliation, and the response build — with no `.await`
//! and no I/O, so the reconciliation policy is model-checkable on its own. The
//! async wrappers around it flush the records it produces and drive the
//! in-place upgrade a heartbeat against a classic group triggers.

use std::{collections::HashSet, time::Instant};

use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{
        Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use self::identity::{
    HeartbeatError, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH, Resolved,
    resolve_leaving_member, resolve_member,
};
use super::{
    ActorServices, ErrorCode, FALLBACK_HEARTBEAT_INTERVAL_MS, MetadataProvider, chrono_now_ms,
    downgrade::maybe_downgrade,
    member_state::{
        check_subscribed_topic_regex, reported_owned, run_reconcile, try_build_member,
        update_member_state,
    },
    pending_records::PendingRecords,
    persistence::{
        current_assignment_value, flush_pending, snapshot_pending_after_change,
        target_assignment_value,
    },
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity,
        config::NextGenConfig,
        consumer_state::GroupState,
        first_join_member_id,
        group::{CoordinatorGroup, GroupKind},
        migration,
    },
};

mod identity;
#[cfg(test)]
mod tests;

pub(super) async fn handle_actor_heartbeat(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    request: ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    regex_authorized_topics: &HashSet<String>,
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
    match handle_heartbeat(state, services, &request, client, regex_authorized_topics).await {
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
    regex_authorized_topics: &HashSet<String>,
) -> HeartbeatStep {
    // ─── Leave path ──────────────────────────────────────────────
    // Kafka's `consumerGroupHeartbeat`: -1 leaves the group, and -2 is a
    // static member that leaves for a while.
    if req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH
        || req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH
    {
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

    // Kafka's `throwIfConsumerGroupIsFull`: only a member id the group does
    // not hold is refused.
    if state.members.len() >= config.max_size
        && (req.member_id.is_empty() || !state.members.contains_key(&req.member_id))
    {
        return rejected(
            HeartbeatError {
                code: codes::GROUP_MAX_SIZE_REACHED,
                message: format!(
                    "The consumer group has reached its maximum capacity of {} members.",
                    config.max_size
                ),
            },
            config,
        );
    }

    // KIP-848 (finalized): the consumer generates its own member UUID and
    // sends it with `member_epoch == 0` on first join. An empty `member_id` is
    // tolerated as a fallback (raw-RPC / older callers) by minting a
    // server-side UUID.
    let member_id = first_join_member_id(&req.member_id);
    let resolved = match resolve_member(state, req, &member_id) {
        Ok(resolved) => resolved,
        Err(error) => return rejected(error, config),
    };

    // ─── First-join path ─────────────────────────────────────────
    if resolved == Resolved::New {
        let m = match try_build_member(&member_id, req, client, now, regex_authorized_topics) {
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
        state.advance_member_epoch(&member_id);
        // Compute the new member's current assignment (grants free target
        // partitions, withholds those still held by others) before responding.
        let owned = reported_owned(req);
        state.reconcile_member(&member_id, &owned);
        state.track_rebalance_timeout(&member_id, now);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&member_id), true);
        let response = build_assignment_resp(state, &member_id, config);
        return HeartbeatStep { response, pending };
    }

    // ─── Static replacement ──────────────────────────────────────
    // Kafka's `getOrMaybeSubscribeStaticConsumerGroupMember`: the new member
    // takes the released member's subscription, target and assignment under
    // its own id, at epoch 0, and the released member goes.
    let replaced = match &resolved {
        Resolved::Replaces { previous } => {
            if let Some(check) = req
                .subscribed_topic_regex
                .as_deref()
                .and_then(|pattern| check_subscribed_topic_regex(pattern).err())
            {
                return HeartbeatStep {
                    response: invalid_regex_resp(check, config),
                    pending: PendingRecords::default(),
                };
            }
            state.replace_static_member(previous, &member_id);
            Some(previous.clone())
        }
        Resolved::New | Resolved::Existing => None,
    };

    // ─── Steady-state: update last_seen / subscription / owned ───
    let request_for_member;
    let req = if req.member_id == member_id {
        req
    } else {
        request_for_member = ConsumerGroupHeartbeatRequest {
            member_id: member_id.clone(),
            ..req.clone()
        };
        &request_for_member
    };
    let previous_target_epoch = state.target.epoch;
    let any_change = match update_member_state(
        state,
        config,
        metadata,
        req,
        client,
        now,
        regex_authorized_topics,
    ) {
        Ok(changed) => changed,
        Err(message) => {
            return HeartbeatStep {
                response: invalid_regex_resp(message, config),
                pending: PendingRecords::default(),
            };
        }
    };
    state.track_rebalance_timeout(&member_id, now);
    let mut pending = if any_change || replaced.is_some() {
        snapshot_pending_after_change(
            state,
            std::slice::from_ref(&member_id),
            state.target.epoch != previous_target_epoch,
        )
    } else {
        PendingRecords::default()
    };
    if let Some(previous) = replaced {
        replacement_records(state, &mut pending, &previous, &member_id);
    }
    let response = build_assignment_resp(state, &member_id, config);
    HeartbeatStep { response, pending }
}

/// Adds the records of a static replacement that the snapshot does not hold:
/// the new member's target, and the tombstones of the released member, as
/// Kafka's `replaceMember` writes them.
fn replacement_records(
    state: &GroupState,
    pending: &mut PendingRecords,
    previous: &str,
    member_id: &str,
) {
    if !pending
        .target_per_member
        .iter()
        .any(|(id, _)| id == member_id)
    {
        let target = state
            .target
            .per_member
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        pending.target_per_member.push((
            member_id.to_string(),
            Some(target_assignment_value(&target)),
        ));
    }
    if previous != member_id {
        pending.member_metadata.push((previous.to_string(), None));
        pending.target_per_member.push((previous.to_string(), None));
        pending
            .current_per_member
            .push((previous.to_string(), None));
    }
}

/// Pure form of the leave path (`member_epoch` -1 or -2), after Kafka's
/// `consumerGroupLeave`.
///
/// A dynamic member, or a static member that sends -1, is removed: the group
/// reconciles the survivors and writes their records plus the departed
/// member's tombstones. A static member that sends -2 stays in the group at
/// epoch -2 with its assignment, so a new member with the same instance id can
/// take its place; only its current assignment record is written.
///
/// The response echoes the request's member id and epoch. A member the group
/// does not hold, or an instance id that another member owns, gets Kafka's
/// error.
/// The async caller flushes the returned `pending`.
fn leave_step(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    let member_id = match resolve_leaving_member(state, req) {
        Ok(member) => member.member_id.clone(),
        Err(error) => return rejected(error, config),
    };
    if req.instance_id.is_some() && req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        state.release_static_member(&member_id);
        let pending = PendingRecords {
            current_per_member: state
                .members
                .get(&member_id)
                .map(|member| (member_id.clone(), Some(current_assignment_value(member))))
                .into_iter()
                .collect(),
            ..PendingRecords::default()
        };
        return HeartbeatStep {
            response: ConsumerGroupHeartbeatResponse {
                member_id: Some(member_id),
                ..base_resp(codes::NONE, LEAVE_GROUP_STATIC_MEMBER_EPOCH, config)
            },
            pending,
        };
    }
    state.remove_member(&member_id);
    run_reconcile(state, config, metadata);
    let mut pending = snapshot_pending_after_change(state, &[], true);
    pending.member_metadata.push((member_id.clone(), None));
    pending.target_per_member.push((member_id.clone(), None));
    pending.current_per_member.push((member_id, None));
    HeartbeatStep {
        response: ConsumerGroupHeartbeatResponse {
            member_id: Some(req.member_id.clone()),
            ..base_resp(codes::NONE, req.member_epoch, config)
        },
        pending,
    }
}

/// The error response of a refused heartbeat, with Kafka's message.
fn rejected(error: HeartbeatError, config: &NextGenConfig) -> HeartbeatStep {
    HeartbeatStep {
        response: ConsumerGroupHeartbeatResponse {
            error_message: Some(error.message),
            ..error_resp(error.code, config)
        },
        pending: PendingRecords::default(),
    }
}

async fn handle_heartbeat(
    state: &mut GroupState,
    services: ActorServices<'_>,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    regex_authorized_topics: &HashSet<String>,
) -> Result<ConsumerGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    let step = step_heartbeat(
        state,
        services.config,
        services.metadata,
        req,
        client,
        now,
        regex_authorized_topics,
    );
    flush_pending(
        state,
        step.pending,
        services.offsets_log,
        services.coordinator,
        now_ms,
    )
    .await?;
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
