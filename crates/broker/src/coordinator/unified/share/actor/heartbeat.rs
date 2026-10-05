//! The `ShareGroupHeartbeat` request path: first join, the steady-state
//! subscription and liveness update, and leave. It is the largest single
//! concern of the share-group actor, so it lives in its own file.

use std::{collections::HashSet, time::Instant};

use krabka_protocol::owned::{
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
};

use super::{
    assignment::reconcile,
    records::{PendingShareRecords, chrono_now_ms, flush_pending, snapshot_pending_after_change},
    response::{build_assignment_resp, error_resp},
    share_state::reconcile_share_state,
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, GroupCoordinator,
        actor::MetadataProvider,
        offsets_log::OffsetsLog,
        share::{
            config::ShareGroupConfig,
            persistence::ShareGroupMetadataValue,
            state::{ShareGroupState, ShareMemberState},
        },
    },
};

pub(super) async fn handle_heartbeat(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<ShareGroupHeartbeatResponse, crate::error::BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return handle_leave(state, config, offsets_log, coordinator, req, now_ms).await;
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-932 mirrors KIP-848: the client mints its own member UUID and
    // sends it with `member_epoch == 0`. Epoch 0 from an unknown member is a
    // first join under the client's id, which the handler has checked is set
    // (`KafkaApis.isMemberIdValid`). Epoch 0 from a known member is a rejoin
    // and takes the existing-member path below.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        if state.members.len() >= config.max_size {
            return Ok(error_resp(codes::GROUP_MAX_SIZE_REACHED, config));
        }
        let new_member_id = req.member_id.clone();
        let m = build_member(&new_member_id, req, client, now);
        state.add_or_update_member(m);
        if !reconcile(state, metadata, config.assignment_interval) {
            state.remove_member(&new_member_id);
            return Ok(error_resp(codes::INVALID_REQUEST, config));
        }
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
        reconcile_share_state(state, config, offsets_log, coordinator, now_ms).await;
        return Ok(build_assignment_resp(state, &new_member_id, config, true));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = match state.validate_member_epoch(&req.member_id, req.member_epoch) {
        Ok(epoch) => epoch,
        Err(error_code) => return Ok(error_resp(error_code, config)),
    };

    // ─── Steady-state: update subscription / last_seen ───────────
    let assigned_before = state
        .members
        .get(&req.member_id)
        .map(|m| m.assigned_partitions.clone());
    let Some(changed) = update_member_state(state, config, metadata, req, client, now, cur_epoch)
    else {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    if changed {
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    }
    // KIP-932 lifecycle: every steady-state heartbeat initializes the
    // subscribed partitions that are not initialized yet.
    reconcile_share_state(state, config, offsets_log, coordinator, now_ms).await;
    // Kafka sends the assignment only on a full request (a rejoin at epoch 0
    // or a request that carries the subscription) or when it changed.
    let assigned_changed = state
        .members
        .get(&req.member_id)
        .map(|m| &m.assigned_partitions)
        != assigned_before.as_ref();
    let with_assignment =
        req.member_epoch == 0 || req.subscribed_topic_names.is_some() || assigned_changed;
    Ok(build_assignment_resp(
        state,
        &req.member_id,
        config,
        with_assignment,
    ))
}

/// Apply steady-state member updates and run reconciliation. Returns `true`
/// if anything changed that requires a log write.
fn update_member_state(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
    cur_epoch: i32,
) -> Option<bool> {
    let mut member_metadata_changed = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if m.client_id != client.id {
            m.client_id = client.id.to_string();
            member_metadata_changed = true;
        }
        if m.client_host != client.host {
            m.client_host = client.host.to_string();
            member_metadata_changed = true;
        }
        // Kafka's `ShareGroupMember.Builder.maybeUpdateRackId`: a heartbeat that
        // carries a rack id replaces the stored one, a rejoin included.
        if req.rack_id.is_some() && m.rack_id != req.rack_id {
            m.rack_id.clone_from(&req.rack_id);
            member_metadata_changed = true;
        }
        if let Some(ref names) = req.subscribed_topic_names {
            let set: HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
                member_metadata_changed = true;
            }
        }
    }
    let group_epoch_before = state.group_epoch;
    if !reconcile(state, metadata, config.assignment_interval) {
        return None;
    }
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    Some(member_metadata_changed || state.group_epoch != group_epoch_before || epoch_advanced)
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
///
/// It follows Kafka's `GroupMetadataManager.shareGroupLeave`. An unknown
/// member answers `UNKNOWN_MEMBER_ID` with Kafka's message and writes no
/// record. A known member is fenced: its records are tombstoned, the group
/// epoch is bumped, and the response echoes the member id and epoch `-1`.
async fn handle_leave(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ShareGroupHeartbeatResponse, crate::error::BrokerError> {
    if !state.members.contains_key(&req.member_id) {
        return Ok(ShareGroupHeartbeatResponse {
            error_code: codes::UNKNOWN_MEMBER_ID,
            error_message: Some(format!(
                "Member {} is not a member of group {}.",
                req.member_id, state.group_id
            )),
            ..Default::default()
        });
    }
    if crate::metadata_epoch::next_i32(state.group_epoch).is_none() {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    }
    let mut pending = PendingShareRecords::default();
    pending.member_metadata.push((req.member_id.clone(), None));
    pending
        .target_per_member
        .push((req.member_id.clone(), None));
    pending
        .current_per_member
        .push((req.member_id.clone(), None));
    state.remove_member(&req.member_id);
    if !state.bump_epoch() {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    }
    pending.group_metadata = Some(ShareGroupMetadataValue {
        epoch: state.group_epoch,
    });
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    // Initialize the partitions that the remaining members gained. The share
    // state of a dropped partition stays, as in Kafka.
    reconcile_share_state(state, config, offsets_log, coordinator, now_ms).await;
    Ok(leave_resp(&req.member_id, req.member_epoch))
}

/// The response to a successful leave: Kafka's `shareGroupLeave` sets only
/// the member id and the request's member epoch.
fn leave_resp(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        member_id: Some(member_id.to_owned()),
        member_epoch,
        ..Default::default()
    }
}

pub(super) fn build_member(
    member_id: &str,
    req: &ShareGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> ShareMemberState {
    let subs: HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut m = ShareMemberState::joining(member_id, client.id, client.host, subs);
    m.rack_id.clone_from(&req.rack_id);
    m.last_seen = now;
    m
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        config::NextGenConfig,
        offsets_log::fake::InMemoryOffsetsLog,
        share::actor::test_support::{
            heartbeat, make_coordinator, metadata_with_topic, seed_initialized,
        },
    };

    /// Kafka's `shareGroupHeartbeat`: only initialized partitions are
    /// assigned, the assignor keeps what it can when a member joins, and the
    /// response carries the assignment only for a join, a request with a
    /// subscription, or a changed assignment. Each row is one heartbeat and
    /// the whole response it gets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_assignments_match_kafka() {
        use krabka_protocol::owned::{
            common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
            share_group_heartbeat_response::Assignment,
        };

        let (metadata, id) = metadata_with_topic("t", 4);
        let response =
            |member: &str, epoch: i32, partitions: Option<Vec<i32>>| ShareGroupHeartbeatResponse {
                member_id: Some(member.into()),
                member_epoch: epoch,
                heartbeat_interval_ms: 5_000,
                assignment: partitions.map(|partitions| Assignment {
                    topic_partitions: if partitions.is_empty() {
                        Vec::new()
                    } else {
                        vec![TopicPartitions {
                            topic_id: id,
                            partitions,
                            ..Default::default()
                        }]
                    },
                    ..Default::default()
                }),
                ..Default::default()
            };
        let request = |member: &str, epoch: i32, subscribe: bool| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member.into(),
            member_epoch: epoch,
            subscribed_topic_names: subscribe.then(|| vec!["t".into()]),
            ..Default::default()
        };
        // (initialized partitions, [(request, expected response)])
        let scenarios = [
            (
                None,
                vec![
                    (request("m1", 0, true), response("m1", 1, Some(vec![]))),
                    (request("m1", 1, false), response("m1", 1, None)),
                ],
            ),
            (
                Some(vec![0, 1, 2, 3]),
                vec![
                    (
                        request("m1", 0, true),
                        response("m1", 1, Some(vec![0, 1, 2, 3])),
                    ),
                    (request("m1", 1, false), response("m1", 1, None)),
                    (request("m2", 0, true), response("m2", 2, Some(vec![0, 1]))),
                    (request("m1", 1, false), response("m1", 2, Some(vec![2, 3]))),
                    (request("m1", 2, false), response("m1", 2, None)),
                    (request("m1", 2, true), response("m1", 2, Some(vec![2, 3]))),
                ],
            ),
        ];
        for (index, (initialized, steps)) in scenarios.into_iter().enumerate() {
            let (coord, _log) = make_coordinator(metadata.clone());
            let handle = coord.get_or_create_share("g");
            if let Some(partitions) = initialized {
                seed_initialized(&handle, id, "t", partitions).await;
            }
            for (step, (req, expected)) in steps.into_iter().enumerate() {
                let resp = heartbeat(&handle, req).await;
                check!(resp == expected, "scenario {index} step {step}");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_limit_rejects_only_new_members() {
        let (metadata, _id) = metadata_with_topic("t", 1);
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::assigning_at_once(),
            ShareGroupConfig {
                max_size: 1,
                ..ShareGroupConfig::assigning_at_once()
            },
            metadata,
            log,
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_share("g");
        let request = |member_id: &str, member_epoch| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };

        let joined = heartbeat(&handle, request("m1", 0)).await;
        check!(joined.error_code == codes::NONE);

        let rejected = heartbeat(&handle, request("m2", 0)).await;
        check!(rejected.error_code == codes::GROUP_MAX_SIZE_REACHED);

        let existing = heartbeat(&handle, request("m1", joined.member_epoch)).await;
        check!(existing.error_code == codes::NONE);
        check!(existing.member_epoch == joined.member_epoch);
    }

    /// Kafka's `shareGroupLeave`: a known member is fenced, with its records
    /// tombstoned and the group epoch bumped, and the response echoes its id
    /// and epoch `-1`. An unknown member answers `UNKNOWN_MEMBER_ID` and
    /// leaves the group and the log untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_matches_kafka() {
        // (leaving member id, expected response, expected new batches,
        // expected group epoch afterwards)
        let rows = [
            (
                "m1",
                ShareGroupHeartbeatResponse {
                    member_id: Some("m1".into()),
                    member_epoch: -1,
                    ..Default::default()
                },
                1,
                2,
            ),
            (
                "m9",
                ShareGroupHeartbeatResponse {
                    error_code: codes::UNKNOWN_MEMBER_ID,
                    error_message: Some("Member m9 is not a member of group g.".into()),
                    ..Default::default()
                },
                0,
                1,
            ),
        ];
        for (member_id, expected, new_batches, group_epoch) in rows {
            let (metadata, _id) = metadata_with_topic("t", 4);
            let (coord, log) = make_coordinator(metadata);
            let handle = coord.get_or_create_share("g");
            let joined = heartbeat(
                &handle,
                ShareGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                },
            )
            .await;
            check!(joined.error_code == codes::NONE);
            let pre_leave = log.batches().await.len();

            let resp = heartbeat(
                &handle,
                ShareGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: member_id.into(),
                    member_epoch: -1,
                    ..Default::default()
                },
            )
            .await;

            check!(resp == expected, "{member_id}");
            let batches = log.batches().await;
            check!(batches.len() == pre_leave + new_batches, "{member_id}");
            if new_batches > 0 {
                check!(
                    batches[batches.len() - 1]
                        .records
                        .iter()
                        .any(|r| r.value.is_none()),
                    "leave batch must contain at least one tombstone"
                );
            }
            let seed = coord.cached_share_seed("g").expect("seed cached");
            check!(seed.group_epoch == group_epoch, "{member_id}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_epoch_is_fenced() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let joined = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(joined.member_epoch == 1);
        // Re-send with an epoch ahead of the server → fenced.
        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 99,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == codes::FENCED_MEMBER_EPOCH);
    }

    /// The member epoch rule of Kafka's `throwIfShareGroupMemberEpochIsInvalid`.
    /// Member `m1` is at epoch 3 with previous epoch 1: it joins at epoch 1,
    /// `m2` and `m3` join (group epochs 2 and 3), and `m1` heartbeats once at
    /// epoch 1. Each row sends one heartbeat on a fresh group and compares the
    /// whole response.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_epoch_rule_matches_kafka() {
        let request = |member_id: &str, member_epoch| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        // (member id, request epoch, accepted)
        let rows = [
            ("m1", 0, true),
            ("m1", 1, true),
            ("m1", 2, false),
            ("m1", 3, true),
            ("m1", 4, false),
            ("m9", 3, false),
        ];

        for (index, (member_id, member_epoch, accepted)) in rows.into_iter().enumerate() {
            let (metadata, _id) = metadata_with_topic("t", 4);
            let (coord, _log) = make_coordinator(metadata);
            let handle = coord.get_or_create_share("g");
            check!(heartbeat(&handle, request("m1", 0)).await.member_epoch == 1);
            check!(heartbeat(&handle, request("m2", 0)).await.member_epoch == 2);
            check!(heartbeat(&handle, request("m3", 0)).await.member_epoch == 3);
            let advanced = heartbeat(&handle, request("m1", 1)).await;
            check!(advanced.member_epoch == 3);

            let resp = heartbeat(&handle, request(member_id, member_epoch)).await;

            let config = ShareGroupConfig::assigning_at_once();
            let expected = if accepted {
                // A rejoin and the previous epoch get the current epoch and
                // the full assignment, as a current heartbeat does.
                ShareGroupHeartbeatResponse {
                    member_id: Some("m1".into()),
                    member_epoch: 3,
                    assignment: advanced.assignment.clone(),
                    ..super::super::response::base_resp(codes::NONE, 3, &config)
                }
            } else if member_id == "m1" {
                super::super::response::error_resp(codes::FENCED_MEMBER_EPOCH, &config)
            } else {
                super::super::response::error_resp(codes::UNKNOWN_MEMBER_ID, &config)
            };
            check!(resp == expected, "row {index}");
        }
    }

    /// A heartbeat that carries a rack id replaces the stored one, a rejoin at
    /// epoch 0 included, and the member metadata record carries the new rack.
    /// A heartbeat without a rack id keeps it (`maybeUpdateRackId`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn heartbeat_updates_the_rack_id() {
        let (metadata, _id) = metadata_with_topic("t", 1);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let request = |member_epoch, rack_id: Option<&str>| ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch,
            rack_id: rack_id.map(str::to_owned),
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        // (request epoch, request rack id, expected stored rack id)
        let rows = [
            (0, Some("rack-a"), Some("rack-a")),
            (0, Some("rack-b"), Some("rack-b")),
            (1, None, Some("rack-b")),
            (1, Some("rack-c"), Some("rack-c")),
        ];
        for (index, (member_epoch, rack_id, expected)) in rows.into_iter().enumerate() {
            let resp = heartbeat(&handle, request(member_epoch, rack_id)).await;
            check!(resp.error_code == codes::NONE, "row {index}");
            let seed = coord.cached_share_seed("g").expect("seed cached");
            check!(
                seed.members["m1"].rack_id.as_deref() == expected,
                "row {index}"
            );
        }
    }

    /// A heartbeat whose write fails answers the code of the failure, and the
    /// failed write leaves no partial batch. A write that is not committed
    /// answers what Kafka's `CoordinatorOperationExceptionHelper` answers for
    /// it, so a member that the coordinator never committed looks the
    /// coordinator up again rather than keep an epoch the next coordinator
    /// does not know.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_write_answers_its_code_and_writes_no_partial_batch() {
        let uncommitted = |code| {
            Some(crate::error::BrokerError::CoordinatorWriteUncommitted { partition: 0, code })
        };
        let cases = [
            (
                "the partition writer is gone",
                None,
                codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                "the leadership moved before the write committed",
                uncommitted(codes::NOT_COORDINATOR),
                codes::NOT_COORDINATOR,
            ),
            (
                "the write did not commit in time",
                uncommitted(codes::COORDINATOR_NOT_AVAILABLE),
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
        ];
        for (what, failure, expected) in cases {
            let (metadata, _id) = metadata_with_topic("t", 1);
            let (coord, log) = make_coordinator(metadata);
            let handle = coord.get_or_create_share("g");
            match failure {
                Some(error) => {
                    *log.fail_next_with.lock().expect("not poisoned") = Some(error);
                }
                None => log.fail_next.store(true, Ordering::SeqCst),
            }

            let response = heartbeat(
                &handle,
                ShareGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                },
            )
            .await;

            check!(
                response
                    == ShareGroupHeartbeatResponse {
                        error_code: expected,
                        ..Default::default()
                    },
                "{what}"
            );
            check!(log.batches().await.is_empty(), "{what}");
        }
    }
}
