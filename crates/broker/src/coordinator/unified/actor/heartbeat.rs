//! The KIP-848 `ConsumerGroupHeartbeat` path.
//!
//! [`step_heartbeat`] is the pure decision core — epoch validation, member
//! upsert or leave, reconciliation, and the response build — with no `.await`
//! and no I/O, so the reconciliation policy is model-checkable on its own. The
//! async wrappers around it flush the records it produces and drive the
//! in-place upgrade a heartbeat against a classic group triggers.

use {
    self::identity::{
        HeartbeatError, LEAVE_GROUP_MEMBER_EPOCH, LEAVE_GROUP_STATIC_MEMBER_EPOCH, Resolved,
        resolve_leaving_member, resolve_member,
    },
    super::{
        ActorServices, ErrorCode, FALLBACK_HEARTBEAT_INTERVAL_MS, MetadataProvider, chrono_now_ms,
        downgrade::{downgrade_fencing, downgrades_without},
        member_state::{
            MemberChange, MemberUpdate, after_member_update, check_subscribed_topic_regex,
            reported_owned, try_build_member, update_member_state,
        },
        pending_records::PendingRecords,
        persistence::{
            Recorder, current_assignment_value, leave_written_consumer_group,
            target_assignment_value,
        },
        regex_resolution::{RegexResolution, Resolutions, delete_unsubscribed_regexes},
    },
    crate::{
        codes,
        coordinator::unified::{
            ClientIdentity,
            config::NextGenConfig,
            consumer_state::GroupState,
            first_join_member_id,
            group::{CoordinatorGroup, GroupKind},
            migration,
            offsets_log::write_failure_code,
            reconciler,
            regex_resolver::TopicRegexResolver,
            registry::consumer_group_not_found,
        },
        error::BrokerError,
    },
    krabka_protocol::owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::{
            Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
        },
    },
    std::{mem, time::Instant},
    tokio::sync::oneshot,
};

mod identity;
#[cfg(test)]
mod regex_tests;
#[cfg(test)]
mod tests;

pub(super) async fn handle_actor_heartbeat(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    request: ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    regex_resolver: &dyn TopicRegexResolver,
    reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
) -> bool {
    if let Some(message) = refuse_over_classic(group, services.config, request.member_epoch) {
        let _ = reply.send(group_id_not_found(Some(message)));
        return true;
    }
    // Kafka's `getOrMaybeCreateConsumerGroup` writes the records that replace a
    // classic group in the heartbeat's own batch, and the whole batch fails
    // or commits together. The classic group comes back when the heartbeat
    // is refused.
    let (prefix, classic_before) = match take_over_classic(group, services.metadata) {
        Ok(takeover) => takeover
            .map(|takeover| (takeover.prefix, takeover.classic_before))
            .unzip(),
        Err(message) => {
            let _ = reply.send(group_id_not_found(Some(message)));
            return true;
        }
    };
    let Some(state) = group.as_consumer_mut() else {
        let _ = reply.send(group_id_not_found(None));
        return true;
    };
    // A consumer group exists once the log holds it, and it stays after its
    // members left. An actor whose group no record holds, such as one left by
    // a join that Kafka refused before it wrote anything, holds no group, and
    // Kafka answers any other epoch with GROUP_ID_NOT_FOUND.
    if request.member_epoch != 0 && !state.is_persisted() && state.members.is_empty() {
        let message = consumer_group_not_found(&state.group_id, request.member_epoch);
        let _ = reply.send(group_id_not_found(Some(message)));
        return true;
    }
    if let Some(fenced) = downgrading_leave(state, services.config, &request) {
        return downgrade_on_leave(group, &fenced, services, &request, reply).await;
    }
    match handle_heartbeat(state, services, &request, client, regex_resolver, prefix).await {
        Ok(response) => {
            if response.error_code != codes::NONE
                && let Some(kind) = classic_before
            {
                *group.kind_mut() = kind;
            }
            let _ = reply.send(response);
        }
        Err(error) => {
            tracing::warn!(group_id = %group.group_id, %error,
                "next-gen actor exiting after log-write failure");
            let _ = reply.send(ConsumerGroupHeartbeatResponse {
                error_code: write_failure_code(&error),
                ..Default::default()
            });
            if let Some(kind) = classic_before {
                // The takeover records failed with the heartbeat's batch, so
                // the log holds the classic group unless that batch commits
                // after all, and then the member's next heartbeat repeats it.
                services
                    .coordinator
                    .leave_respawn_seed(group.respawn_seed(kind));
                return false;
            }
            return leave_written_consumer_group(group, services.coordinator);
        }
    }
    true
}

/// Kafka's `getOrMaybeCreateConsumerGroup` over a classic group: the message
/// of its refusal, or `None` when the heartbeat may replace the group. Only a
/// joining heartbeat (epoch 0) may create a consumer group over a classic
/// one, and only when the classic group is empty or can upgrade online.
fn refuse_over_classic(
    group: &CoordinatorGroup,
    config: &NextGenConfig,
    member_epoch: i32,
) -> Option<String> {
    let classic = group.as_classic()?;
    if member_epoch != 0 {
        return Some(format!("Group {} is not a consumer group.", group.group_id));
    }
    if classic.members.is_empty() {
        return None;
    }
    migration::validate_online_upgrade(
        classic,
        config.migration_policy.allows_upgrade(),
        config.max_size,
    )
    .err()
}

/// Kafka's `consumerGroupFenceMembers` downgrades the group, in place of the
/// fence, when the leaving member is its last native one. The member to fence
/// when this leave downgrades the group, or `None`.
fn downgrading_leave(
    state: &GroupState,
    config: &NextGenConfig,
    request: &ConsumerGroupHeartbeatRequest,
) -> Option<[String; 1]> {
    if request.member_epoch != LEAVE_GROUP_MEMBER_EPOCH {
        return None;
    }
    let member = resolve_leaving_member(state, request).ok()?;
    let fenced = [member.member_id.clone()];
    downgrades_without(state, config, &fenced).then_some(fenced)
}

/// Downgrades the group in place of fencing the leaving member and answers
/// the leave. It returns `false` when the downgrade's log write fails, and
/// the actor then exits.
async fn downgrade_on_leave(
    group: &mut CoordinatorGroup,
    fenced: &[String],
    services: ActorServices<'_>,
    request: &ConsumerGroupHeartbeatRequest,
    reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
) -> bool {
    if let Err(error) = downgrade_fencing(group, fenced, services).await {
        tracing::warn!(group_id = %group.group_id, %error,
            "next-gen actor exiting after downgrade log-write failure");
        let _ = reply.send(ConsumerGroupHeartbeatResponse {
            error_code: write_failure_code(&error),
            ..Default::default()
        });
        return leave_written_consumer_group(group, services.coordinator);
    }
    let _ = reply.send(ConsumerGroupHeartbeatResponse {
        member_id: Some(request.member_id.clone()),
        ..base_resp(codes::NONE, request.member_epoch)
    });
    true
}

/// A classic group that a heartbeat replaced with a consumer group.
struct ClassicTakeover {
    /// The records that replace the classic group, written ahead of the
    /// heartbeat's own in one batch.
    prefix: PendingRecords,
    /// The classic group, restored when the heartbeat is refused.
    classic_before: GroupKind,
}

/// Replaces a classic group with a consumer group in place, or returns
/// `Ok(None)` when the group is not classic. It returns Kafka's message when
/// the classic group cannot be converted.
fn take_over_classic(
    group: &mut CoordinatorGroup,
    metadata: &dyn MetadataProvider,
) -> Result<Option<ClassicTakeover>, String> {
    let Some(classic) = group.as_classic() else {
        return Ok(None);
    };
    let (prefix, state) = if classic.members.is_empty() {
        // `maybeDeleteEmptyClassicGroup`: an empty classic group, such as one
        // that only holds committed offsets, is deleted whatever the policy
        // and protocol type, and a new consumer group takes its id. The
        // committed offsets stay with the group id.
        let prefix = PendingRecords {
            classic_group_metadata_tombstone: true,
            ..PendingRecords::default()
        };
        (prefix, GroupState::new(group.group_id.clone()))
    } else {
        // `convertToConsumerGroup`, which refuses a member that
        // `ConsumerGroup.fromClassicGroup` cannot carry over.
        let state = migration::convert_classic_to_consumer(classic, &metadata.snapshot())?;
        (migration::upgrade_pending_records(&state), state)
    };
    let classic_before = mem::replace(group.kind_mut(), GroupKind::Consumer(state));
    Ok(Some(ClassicTakeover {
        prefix,
        classic_before,
    }))
}

/// The `GROUP_ID_NOT_FOUND` answer, with Kafka's message when it has one.
fn group_id_not_found(message: Option<String>) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: codes::GROUP_ID_NOT_FOUND,
        error_message: message,
        ..Default::default()
    }
}

/// Outcome of the pure heartbeat decision phase: the response to return to the
/// client and the records the async caller must append to the offsets log.
pub struct HeartbeatStep {
    pub response: ConsumerGroupHeartbeatResponse,
    pub pending: PendingRecords,
    /// What the heartbeat's regex resolution found. The caller applies it
    /// after the heartbeat's batch, as Kafka's `handleRegularExpressionsResult`
    /// does, and writes its records in a batch of their own.
    pub resolutions: Option<Resolutions>,
}

impl HeartbeatStep {
    fn answer(response: ConsumerGroupHeartbeatResponse) -> Self {
        Self {
            response,
            pending: PendingRecords::default(),
            resolutions: None,
        }
    }
}

/// The pure, synchronous heartbeat decision core: Kafka's
/// `consumerGroupHeartbeat`, from the member lookup to the response.
///
/// This function holds no `.await` and does no I/O. `handle_heartbeat` calls
/// it, then flushes `pending` to the log. It is a separate function so that
/// the reconciliation policy is model-checkable on its own.
pub fn step_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
    regexes: &RegexResolution<'_>,
) -> HeartbeatStep {
    // ─── Leave path ──────────────────────────────────────────────
    // Kafka's `consumerGroupHeartbeat`: -1 leaves the group, and -2 is a
    // static member that leaves for a while.
    if req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH
        || req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH
    {
        return leave_step(state, metadata, req);
    }

    // ─── Validate assignor selection ─────────────────────────────
    if req
        .server_assignor
        .as_deref()
        .is_some_and(|name| !config.assignor_enabled(name))
    {
        return HeartbeatStep::answer(error_resp(codes::UNSUPPORTED_ASSIGNOR));
    }

    // Kafka's `throwIfConsumerGroupIsFull`: only a member id the group does
    // not hold is refused.
    if state.members.len() >= config.max_size
        && (req.member_id.is_empty() || !state.members.contains_key(&req.member_id))
    {
        return rejected(HeartbeatError {
            code: codes::GROUP_MAX_SIZE_REACHED,
            message: format!(
                "The consumer group has reached its maximum capacity of {} members.",
                config.max_size
            ),
        });
    }

    // KIP-848 (finalized): the consumer generates its own member UUID and
    // sends it with `member_epoch == 0` on first join. An empty `member_id` is
    // tolerated as a fallback (raw-RPC / older callers) by minting a
    // server-side UUID.
    let member_id = first_join_member_id(&req.member_id);
    let resolved = match resolve_member(state, req, &member_id) {
        Ok(resolved) => resolved,
        Err(error) => return rejected(error),
    };

    // ─── First join ──────────────────────────────────────────────
    // Kafka's `getOrMaybeCreateMember` creates the member with its defaults,
    // and the heartbeat then updates it like any other member: the topic
    // names change from none, and the pattern from none.
    if resolved == Resolved::New {
        let mut m = match try_build_member(&member_id, req, client, now) {
            Ok(m) => m,
            Err(message) => return HeartbeatStep::answer(invalid_regex_resp(message)),
        };
        let recorder = Recorder::start(state, &[&member_id]);
        let new_regex = m.subscribed_topic_regex.take();
        let names_changed = !m.subscribed_topic_names.is_empty();
        state.add_or_update_member(m);
        let owned = reported_owned(req);
        let update = after_member_update(
            state,
            config,
            metadata,
            MemberChange {
                member_id: &member_id,
                old_regex: None,
                new_regex,
                names_changed,
                owned: owned.as_ref(),
            },
            regexes,
        );
        state.track_rebalance_timeout(&member_id, now);
        let MemberUpdateRecords {
            pending,
            resolutions,
        } = member_update_records(recorder, state, update, &member_id, None);
        let response = build_assignment_resp(state, &member_id, config, true);
        return HeartbeatStep {
            response,
            pending,
            resolutions,
        };
    }

    // ─── Static replacement ──────────────────────────────────────
    // Kafka's `getOrMaybeSubscribeStaticConsumerGroupMember`: the new member
    // takes the released member's subscription, target and assignment under
    // its own id, at epoch 0, and the released member goes. Kafka's
    // `replaceMember` writes those records first in the batch.
    let assigned_before = state
        .members
        .get(&member_id)
        .map(|member| member.assigned_partitions.clone());
    let replacement = match &resolved {
        Resolved::Replaces { previous } => {
            if let Some(check) = req
                .subscribed_topic_regex
                .as_deref()
                .and_then(|pattern| check_subscribed_topic_regex(pattern).err())
            {
                return HeartbeatStep::answer(invalid_regex_resp(check));
            }
            Some(replace_static_member(state, previous, &member_id))
        }
        Resolved::New | Resolved::Existing => None,
    };

    // ─── Steady state ────────────────────────────────────────────
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
    let recorder = Recorder::start(state, &[&member_id]);
    let update = match update_member_state(state, config, metadata, req, client, now, regexes) {
        Ok(update) => update,
        Err(message) => return HeartbeatStep::answer(invalid_regex_resp(message)),
    };
    state.track_rebalance_timeout(&member_id, now);
    let MemberUpdateRecords {
        pending,
        resolutions,
    } = member_update_records(recorder, state, update, &member_id, replacement);
    // Kafka sends the assignment only on a join (epoch 0), on a full request,
    // or when the member's assigned partitions changed.
    let assignment_changed = assigned_before.as_ref()
        != state
            .members
            .get(&member_id)
            .map(|member| &member.assigned_partitions);
    let include_assignment = req.member_epoch == 0 || is_full_request(req) || assignment_changed;
    let response = build_assignment_resp(state, &member_id, config, include_assignment);
    HeartbeatStep {
        response,
        pending,
        resolutions,
    }
}

/// Kafka's `isFullRequest`: a member sends every non-optional field when it
/// joins, rejoins, or recovers from an error.
fn is_full_request(req: &ConsumerGroupHeartbeatRequest) -> bool {
    req.rebalance_timeout_ms != -1
        && (req.subscribed_topic_names.is_some() || req.subscribed_topic_regex.is_some())
        && req.topic_partitions.is_some()
}

/// Kafka's `replaceMember`: moves the static member `previous` to
/// `member_id` at epoch 0 and returns its records, the released member's
/// tombstones, then the new member's subscription, target and current
/// assignment.
pub(super) fn replace_static_member(
    state: &mut GroupState,
    previous: &str,
    member_id: &str,
) -> PendingRecords {
    let recorder = Recorder::start(state, &[previous, member_id]);
    state.replace_static_member(previous, member_id);
    let mut pending = recorder.finish(state, None, false);
    let target = state
        .target
        .per_member
        .get(member_id)
        .cloned()
        .unwrap_or_default();
    pending
        .target_per_member
        .push((member_id.to_owned(), Some(target_assignment_value(&target))));
    pending
}

/// What [`member_update_records`] gives back: the records of the update, and
/// the regex resolutions to apply in a batch of their own afterwards.
pub(super) struct MemberUpdateRecords {
    pub(super) pending: PendingRecords,
    pub(super) resolutions: Option<Resolutions>,
}

/// The records of a heartbeat or classic join that ran the member update
/// `update` for `member_id`, recorded by `recorder`, and the regex
/// resolutions the group applies in a batch of their own afterwards.
///
/// When the member replaced a static member, `replacement` holds Kafka's
/// `replaceMember` records, which come first in the batch. Kafka computes the
/// target before it replays them, so the new member id holds no target yet
/// and gets a target record whenever the update computed a target.
pub(super) fn member_update_records(
    recorder: Recorder,
    state: &GroupState,
    update: MemberUpdate,
    member_id: &str,
    replacement: Option<PendingRecords>,
) -> MemberUpdateRecords {
    let mut target = update.target;
    if let (Some(changed), Some(_)) = (target.as_mut(), replacement.as_ref())
        && !changed.iter().any(|changed| changed == member_id)
    {
        changed.push(member_id.to_owned());
        changed.sort_unstable();
    }
    let mut pending = recorder.finish(
        state,
        target.as_deref(),
        update.partition_metadata_tombstone,
    );
    pending.resolved_regexes = update.regex_records;
    if let Some(replacement) = replacement {
        pending = replacement.followed_by(pending);
    }
    MemberUpdateRecords {
        pending,
        resolutions: update.resolutions,
    }
}

/// Pure form of the leave path (`member_epoch` -1 or -2), after Kafka's
/// `consumerGroupLeave`.
///
/// A dynamic member, or a static member that sends -1, is fenced: Kafka's
/// `consumerGroupFenceMembers` tombstones its records and its unused regular
/// expressions and bumps the group epoch; the next heartbeat computes the
/// target. A static member that sends -2 stays in the group at epoch -2 with
/// its assignment, so a new member with the same instance id can take its
/// place; only its current assignment record is written.
///
/// The response echoes the request's member id and epoch. A member the group
/// does not hold, or an instance id that another member owns, gets Kafka's
/// error. The async caller flushes the returned `pending`.
fn leave_step(
    state: &mut GroupState,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    let member_id = match resolve_leaving_member(state, req) {
        Ok(member) => member.member_id.clone(),
        Err(error) => return rejected(error),
    };
    let pending =
        if req.instance_id.is_some() && req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
            state.release_static_member(&member_id);
            PendingRecords {
                current_per_member: state
                    .members
                    .get(&member_id)
                    .map(|member| (member_id.clone(), Some(current_assignment_value(member))))
                    .into_iter()
                    .collect(),
                ..PendingRecords::default()
            }
        } else {
            fence_members(state, metadata, &[member_id])
        };
    HeartbeatStep {
        response: ConsumerGroupHeartbeatResponse {
            member_id: Some(req.member_id.clone()),
            ..base_resp(codes::NONE, req.member_epoch)
        },
        pending,
        resolutions: None,
    }
}

/// Kafka's `consumerGroupFenceMembers` for a group that does not downgrade:
/// removes `member_ids`, with the tombstones of `removeMember` for each and
/// of `maybeDeleteResolvedRegularExpressions`, then bumps the group epoch with
/// the metadata hash of the remaining subscriptions. The target waits for the
/// next heartbeat.
pub(super) fn fence_members(
    state: &mut GroupState,
    metadata: &dyn MetadataProvider,
    member_ids: &[String],
) -> PendingRecords {
    if member_ids.is_empty() {
        return PendingRecords::default();
    }
    let recorder = Recorder::start(state, member_ids);
    for member_id in member_ids {
        state.remove_member(member_id);
    }
    let regex_records = delete_unsubscribed_regexes(state);
    if reconciler::bump_with_metadata_hash(state, &metadata.snapshot()).is_err() {
        tracing::warn!(group_id = %state.group_id, "the group epoch is exhausted");
    }
    let mut pending = recorder.finish(state, None, false);
    pending.resolved_regexes = regex_records;
    pending
}

/// The error response of a refused heartbeat, with Kafka's message.
fn rejected(error: HeartbeatError) -> HeartbeatStep {
    HeartbeatStep::answer(ConsumerGroupHeartbeatResponse {
        error_message: Some(error.message),
        ..error_resp(error.code)
    })
}

/// Runs [`step_heartbeat`] and writes its records after `prefix`, the records
/// of a classic group that the heartbeat replaces, in one batch. A refused
/// heartbeat writes nothing. What the heartbeat's regex resolution found is
/// applied and written next, in a batch of its own.
async fn handle_heartbeat(
    state: &mut GroupState,
    services: ActorServices<'_>,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    regex_resolver: &dyn TopicRegexResolver,
    prefix: Option<PendingRecords>,
) -> Result<ConsumerGroupHeartbeatResponse, BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    let regexes = RegexResolution::of(
        services.config,
        regex_resolver,
        services.coordinator.regex_refresh_version(),
        now_ms,
    );
    let step = step_heartbeat(
        state,
        services.config,
        services.metadata,
        req,
        client,
        now,
        &regexes,
    );
    if step.response.error_code != codes::NONE {
        return Ok(step.response);
    }
    let pending = match prefix {
        Some(prefix) => prefix.followed_by(step.pending),
        None => step.pending,
    };
    services
        .flush_with_regex_result(state, pending, now_ms, step.resolutions, || {
            services.metadata.snapshot()
        })
        .await?;
    Ok(step.response)
}

/// A response with no heartbeat interval and no assignment: Kafka's error
/// responses carry only the error code and message, and its leave responses
/// only the member id and epoch.
fn base_resp(error_code: ErrorCode, member_epoch: i32) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        member_epoch,
        ..Default::default()
    }
}

fn error_resp(error_code: ErrorCode) -> ConsumerGroupHeartbeatResponse { base_resp(error_code, 0) }

/// The `INVALID_REGULAR_EXPRESSION` (128) rejection Kafka answers to a
/// heartbeat whose `SubscribedTopicRegex` does not compile. Kafka carries the
/// exception's message in `error_message`, so do the same.
fn invalid_regex_resp(message: String) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_message: Some(message),
        ..error_resp(codes::INVALID_REGULAR_EXPRESSION)
    }
}

/// The success response of `member_id`, carrying its assignment only when
/// `include_assignment` is set.
fn build_assignment_resp(
    state: &GroupState,
    member_id: &str,
    config: &NextGenConfig,
    include_assignment: bool,
) -> ConsumerGroupHeartbeatResponse {
    let Some(m) = state.members.get(member_id) else {
        return error_resp(codes::UNKNOWN_MEMBER_ID);
    };
    // KIP-848: the `assignment` field carries the member's *current* assignment —
    // the partitions it may own right now — NOT the raw target. `reconcile_member`
    // computes this each heartbeat, withholding any target partition still held by
    // another member until that member revokes it. Returning the current
    // assignment (`assigned_partitions`) rather than the target is what prevents
    // two members from owning the same partition during a handoff.
    let assignment = include_assignment.then(|| RespAssignment {
        topic_partitions: m
            .assigned_partitions
            .iter()
            .map(|(tid, parts)| TopicPartitions {
                topic_id: *tid,
                partitions: parts.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });
    ConsumerGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(FALLBACK_HEARTBEAT_INTERVAL_MS),
        assignment,
        ..Default::default()
    }
}
