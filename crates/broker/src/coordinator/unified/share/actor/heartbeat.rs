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
    response::{base_resp, build_assignment_resp, error_resp},
    share_state::reconcile_share_state,
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, GroupCoordinator,
        actor::MetadataProvider,
        first_join_member_id,
        offsets_log::OffsetsLog,
        share::{
            config::ShareGroupConfig,
            persistence::ShareGroupMetadataValue,
            state::{ShareGroupState, ShareMemberState},
        },
        validate_member_epoch,
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
    // sends it with `member_epoch == 0`. Treat epoch 0 from an unknown member
    // as a first-join, adopting the client-supplied id; an empty id is
    // tolerated by minting a server-side UUID.
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        if state.members.len() >= config.max_size {
            return Ok(error_resp(codes::GROUP_MAX_SIZE_REACHED, config));
        }
        let new_member_id = first_join_member_id(&req.member_id);
        let m = build_member(&new_member_id, req, client, now);
        state.add_or_update_member(m);
        if !reconcile(state, metadata) {
            state.remove_member(&new_member_id);
            return Ok(error_resp(codes::INVALID_REQUEST, config));
        }
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
        reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
        return Ok(build_assignment_resp(state, &new_member_id, config));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = match validate_member_epoch(
        state.members.get(&req.member_id).map(|m| m.member_epoch),
        req.member_epoch,
    ) {
        Ok(epoch) => epoch,
        Err(error_code) => return Ok(error_resp(error_code, config)),
    };

    // ─── Steady-state: update subscription / last_seen ───────────
    let Some(changed) = update_member_state(state, metadata, req, client, now, cur_epoch) else {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    };
    if changed {
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id));
        flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    }
    // KIP-932 lifecycle: every steady-state heartbeat re-checks the assignment
    // and Initializes any not-yet-initialized share-states (best-effort retry).
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(build_assignment_resp(state, &req.member_id, config))
}

/// Apply steady-state member updates and run reconciliation. Returns `true`
/// if anything changed that requires a log write.
fn update_member_state(
    state: &mut ShareGroupState,
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
        if let Some(ref names) = req.subscribed_topic_names {
            let set: HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
                member_metadata_changed = true;
            }
        }
    }
    let was_dirty = state.dirty;
    if !reconcile(state, metadata) {
        return None;
    }
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    Some(member_metadata_changed || was_dirty || epoch_advanced)
}

/// Handle a leave-group heartbeat (`member_epoch == -1`).
async fn handle_leave(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &ShareGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<ShareGroupHeartbeatResponse, crate::error::BrokerError> {
    if crate::metadata_epoch::next_i32(state.group_epoch).is_none() {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    }
    let mut pending = PendingShareRecords::default();
    if state.members.contains_key(&req.member_id) {
        pending.member_metadata.push((req.member_id.clone(), None));
        pending
            .target_per_member
            .push((req.member_id.clone(), None));
        pending
            .current_per_member
            .push((req.member_id.clone(), None));
    }
    state.remove_member(&req.member_id);
    if !state.bump_epoch() {
        return Ok(error_resp(codes::INVALID_REQUEST, config));
    }
    pending.group_metadata = Some(ShareGroupMetadataValue {
        epoch: state.group_epoch,
    });
    flush_pending(state, pending, offsets_log, coordinator, now_ms).await?;
    // Delete state for subscriptions dropped by remaining members. If the
    // group became empty, preserve its durable cursor for future consumers.
    reconcile_share_state(state, offsets_log, coordinator, now_ms).await;
    Ok(base_resp(0, req.member_epoch, config))
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
        share::actor::test_support::{heartbeat, make_coordinator, metadata_with_topic},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_member_join_gets_assignment() {
        let (metadata, id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(resp.member_epoch == 1, "epoch advances to group epoch 1");
        let asg = resp.assignment.expect("assignment present");
        let total: usize = asg
            .topic_partitions
            .iter()
            .map(|tp| tp.partitions.len())
            .sum();
        assert!(total == 4, "one member gets all 4 partitions");
        assert!(asg.topic_partitions[0].topic_id == id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_members_reconcile() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, _log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");

        let r1 = heartbeat(
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
        assert!(r1.error_code == 0);

        let r2 = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m2".into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        assert!(r2.error_code == 0);
        // Second join recomputes: each member should now own a 2-partition slice.
        let total2: usize = r2
            .assignment
            .expect("m2 assignment")
            .topic_partitions
            .iter()
            .map(|tp| tp.partitions.len())
            .sum();
        assert!(total2 == 2, "with two members each owns 2 of 4 partitions");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn member_limit_rejects_only_new_members() {
        let (metadata, _id) = metadata_with_topic("t", 1);
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig::default(),
            ShareGroupConfig {
                max_size: 1,
                ..ShareGroupConfig::default()
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_removes_member() {
        let (metadata, _id) = metadata_with_topic("t", 4);
        let (coord, log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        let joined = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            },
        )
        .await;
        let mid = joined.member_id.unwrap();
        let pre_leave = log.batches().await.len();

        let resp = heartbeat(
            &handle,
            ShareGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: mid,
                member_epoch: -1,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == 0);
        let batches = log.batches().await;
        assert!(batches.len() == pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        assert!(
            leave_batch.records.iter().any(|r| r.value.is_none()),
            "leave batch must contain at least one tombstone"
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn known_member_epoch_zero_is_stale_not_first_join() {
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

        let resp = heartbeat(
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

        assert!(resp.error_code == codes::STALE_MEMBER_EPOCH);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persistence_failure_returns_loading_and_writes_no_partial_batch() {
        let (metadata, _id) = metadata_with_topic("t", 1);
        let (coord, log) = make_coordinator(metadata);
        let handle = coord.get_or_create_share("g");
        log.fail_next.store(true, Ordering::SeqCst);

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

        check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        assert!(log.batches().await.is_empty());
    }
}
