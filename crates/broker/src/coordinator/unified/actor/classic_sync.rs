//! The classic `SyncGroup` path.
//!
//! The leader's request installs the round's assignments, persists the classic
//! k2 snapshot, and releases every follower parked behind it. A consumer-kind
//! group answers the same RPC from its reconciler target instead.

use krabka_protocol::owned::sync_group_request::SyncGroupRequest;
use tokio::sync::oneshot;

use super::{
    ActorServices, ParkedWaiters, SyncResult, chrono_now_ms,
    persistence::{flush_classic_metadata, flush_pending, snapshot_pending_after_change},
    waiters::drain_parked_followers,
};
use crate::{
    codes,
    coordinator::unified::{
        classic_ops, consumer_state::GroupState as ConsumerState, group::CoordinatorGroup,
        migration, persistence_next_gen::MemberAssignmentState,
    },
};

/// Serves `SyncGroup` for a classic member hosted in an upgraded consumer
/// group, and makes the sync durable.
///
/// The blob the member receives is its target assignment, so once the sync
/// succeeds the member holds exactly that target. Recording it as the member's
/// current assignment and appending the k8 record is what lets a coordinator
/// failover tell a member that has synced from one that still owes a sync:
/// [`super::seed::apply_seed`] translates the restored current assignment back
/// into the `ConsumerProtocolAssignment` blob the heartbeat path compares
/// against. That is Kafka's own bookkeeping — its `SyncGroup` for a hosted
/// classic member serializes `member.assignedPartitions()` — and it is why the
/// record needs no krabka-private field for the blob.
///
/// A failed append leaves the member as it was and answers
/// `COORDINATOR_LOAD_IN_PROGRESS`, so the client retries the sync.
///
/// Only a hosted classic member gets this far: `migration::serve_classic_sync`
/// answers `UNKNOWN_MEMBER_ID` for a native KIP-848 member, which returns
/// above, so none of the bookkeeping below can overwrite a native member's
/// assignment out from under its own reconciliation.
async fn hosted_classic_sync(
    state: &mut ConsumerState,
    services: ActorServices<'_>,
    request: &SyncGroupRequest,
) -> SyncResult {
    let previous_blob = state
        .members
        .get(&request.member_id)
        .and_then(|m| m.classic.as_ref())
        .map(|facade| facade.last_synced_assignment.clone());
    let result =
        migration::serve_classic_sync(state, &request.member_id, &services.metadata.snapshot());
    if result.error_code != codes::NONE {
        return result;
    }
    let synced = state
        .target
        .per_member
        .get(&request.member_id)
        .cloned()
        .unwrap_or_default();
    let Some(member) = state.members.get_mut(&request.member_id) else {
        return result;
    };
    // A re-sync that changes nothing writes nothing.
    if member.assigned_partitions == synced
        && member.partitions_pending_revocation.is_empty()
        && member.assignment_state == MemberAssignmentState::Stable
    {
        return result;
    }
    // The blob the member just received is its whole assignment, so the sync
    // both grants the target and completes any revocation the last target
    // change started: a classic client applies the assignment it is handed. A
    // partition the member no longer holds is then free for its next owner,
    // which is what drains `partitions_pending_revocation` for a member that
    // never reports what it owns.
    let previous_assigned = std::mem::replace(&mut member.assigned_partitions, synced);
    let previous_pending = std::mem::take(&mut member.partitions_pending_revocation);
    let previous_state = member.assignment_state;
    member.assignment_state = MemberAssignmentState::Stable;
    let pending =
        snapshot_pending_after_change(state, std::slice::from_ref(&request.member_id), false);
    if let Err(error) = flush_pending(
        state,
        pending,
        services.offsets_log,
        services.coordinator,
        chrono_now_ms(),
    )
    .await
    {
        tracing::warn!(group_id = %state.group_id, %error,
            "hosted classic SyncGroup log write failed");
        if let Some(member) = state.members.get_mut(&request.member_id) {
            member.assigned_partitions = previous_assigned;
            member.partitions_pending_revocation = previous_pending;
            member.assignment_state = previous_state;
            if let Some(facade) = member.classic.as_mut() {
                facade.last_synced_assignment = previous_blob.unwrap_or_default();
                facade.awaiting_sync = true;
            }
        }
        return SyncResult {
            error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
            ..SyncResult::default()
        };
    }
    result
}

pub(super) async fn handle_classic_sync_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: SyncGroupRequest,
    reply: oneshot::Sender<SyncResult>,
) {
    if group.as_classic_mut().is_none() {
        let result = match group.as_consumer_mut() {
            Some(state) => hosted_classic_sync(state, services, &request).await,
            None => SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            },
        };
        let _ = reply.send(result);
        return;
    }
    let state = group
        .as_classic_mut()
        .expect("group kind checked immediately above");
    let previous = state.clone();
    match classic_ops::handle_sync(state, &request) {
        classic_ops::SyncAction::Immediate(result) => {
            let _ = reply.send(result);
        }
        classic_ops::SyncAction::Park => {
            parked.followers.insert(request.member_id, reply);
        }
        classic_ops::SyncAction::LeaderInstalled(result) => {
            if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                *state = previous;
                tracing::warn!(group_id = %state.group_id, %error,
                    "classic SyncGroup log write failed");
                let failure = || SyncResult {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    ..SyncResult::default()
                };
                let _ = reply.send(failure());
                for (_, follower) in parked.followers.drain() {
                    let _ = follower.send(failure());
                }
                return;
            }
            let _ = reply.send(result);
            drain_parked_followers(state, &mut parked.followers);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            DescribeMember, GroupActorHandle, GroupActorMessage,
            test_support::{
                completing_classic_group, decode_assignment, last_classic_metadata,
                make_coordinator, make_coordinator_with_topic_policy, rpc, seed_and_upgrade,
            },
        },
        classic_state::GroupState as ClassicGroupState,
    };

    /// The live `Describe` view of one member.
    async fn describe_member(handle: &GroupActorHandle, member_id: &str) -> DescribeMember {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        rx.await
            .unwrap()
            .members
            .into_iter()
            .find(|m| m.member_id == member_id)
            .expect("member in the describe view")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_leader_sync_persists_complete_stable_snapshot() {
        use krabka_protocol::owned::sync_group_request::SyncGroupRequestAssignment;

        let (coord, log) = make_coordinator();
        let group = completing_classic_group(&["m1", "m2"]);
        let generation = group.as_classic().unwrap().generation_id;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicSync {
                req: SyncGroupRequest {
                    group_id: "g".into(),
                    generation_id: generation,
                    member_id: "m1".into(),
                    assignments: vec![SyncGroupRequestAssignment {
                        member_id: "m1".into(),
                        assignment: Bytes::from_static(b"assignment"),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                reply: tx,
            })
            .await
            .unwrap();

        check!(rx.await.unwrap().error_code == codes::NONE);
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == generation);
        check!(persisted.protocol_name.as_deref() == Some("range"));
        check!(persisted.members.len() == 2);
        check!(
            persisted
                .members
                .iter()
                .find(|member| member.member_id == "m1")
                .unwrap()
                .assignment
                == Bytes::from_static(b"assignment")
        );
        check!(
            persisted
                .members
                .iter()
                .find(|member| member.member_id == "m2")
                .unwrap()
                .assignment
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_sync_append_failure_rolls_back_and_can_retry() {
        use krabka_protocol::owned::sync_group_request::SyncGroupRequestAssignment;

        let (coord, log) = make_coordinator();
        let group = completing_classic_group(&["m1"]);
        let generation = group.as_classic().unwrap().generation_id;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let request = SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: "m1".into(),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: "m1".into(),
                assignment: Bytes::from_static(b"assignment"),
                ..Default::default()
            }],
            ..Default::default()
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicSync {
                req: request.clone(),
                reply: tx,
            })
            .await
            .unwrap();
        let failure = rx.await.unwrap();
        check!(failure.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        check!(failure.protocol_type.as_deref() == Some("consumer"));
        check!(failure.protocol_name.as_deref() == Some("range"));
        let view = rpc::classic_inspect(&handle).await;
        check!(view.state == ClassicGroupState::CompletingRebalance);
        check!(view.members[0].assignment.is_none());
        check!(log.batches().await.is_empty());

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicSync {
                req: request,
                reply: tx,
            })
            .await
            .unwrap();
        check!(rx.await.unwrap().error_code == codes::NONE);
        check!(log.batches().await.len() == 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_classic_member_syncs_translated_assignment() {
        // `Upgrade` policy: the native member's leave in `seed_and_upgrade`
        // must NOT downgrade the group back to classic — this test exercises
        // serving a hosted classic member from the consumer-kind reconciler.
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;

        // 1. Heartbeat: the upgrade gave m-classic a target that differs from
        //    its (empty) last-synced assignment → it owes a re-sync.
        assert!(
            rpc::classic_heartbeat(&handle, "m-classic").await == codes::REBALANCE_IN_PROGRESS,
            "post-upgrade heartbeat must signal a re-sync"
        );

        // 2. JoinGroup (rejoin of the existing member, unchanged subscription):
        //    success, server-assigned single-member view at group_epoch, self leader.
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        check!(join.error_code == codes::NONE);
        check!(join.leader.as_str() == "m-classic");
        check!(join.member_id.as_str() == "m-classic");
        // Generation equals the group epoch (read it back from Describe).
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let describe = rx.await.unwrap();
        assert!(join.generation_id == describe.group_epoch);

        // 3. SyncGroup: returns the translated target assignment for "t".
        let sync = rpc::classic_sync(&handle, "m-classic", join.generation_id).await;
        assert!(sync.error_code == codes::NONE);
        assert!(sync.protocol_type.as_deref() == Some("consumer"));
        let asn = decode_assignment(&sync.assignment);
        let t_assign = asn
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "t")
            .expect("assignment contains topic t");
        assert!(
            !t_assign.partitions.is_empty(),
            "m-classic must own partitions of t"
        );

        // 4. Heartbeat again: now in sync → NONE.
        assert!(
            rpc::classic_heartbeat(&handle, "m-classic").await == codes::NONE,
            "after sync the member is in sync → NONE"
        );
    }

    /// A native KIP-848 member of an upgraded group must not be served the
    /// classic `SyncGroup` path. It reconciles through
    /// `ConsumerGroupHeartbeat`, acknowledging each target itself, so granting
    /// it its whole target here would advertise a partition its previous owner
    /// still holds — the KIP-848 safety property `reconcile_member` documents.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_member_sync_group_is_rejected_and_changes_no_assignment() {
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;
        // The hosted classic member syncs, so it holds both partitions of "t".
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        assert!(
            rpc::classic_sync(&handle, "m-classic", join.generation_id)
                .await
                .error_code
                == codes::NONE
        );

        // A native member joins. Its target gains a partition that m-classic
        // still holds, so the reconciler withholds it: the native member's
        // assignment lags its target until it acknowledges.
        let native = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(native.error_code == codes::NONE);
        let native_id = native.member_id.expect("native member id");
        let before = describe_member(&handle, &native_id).await;
        let classic_before = describe_member(&handle, "m-classic").await;

        let sync = rpc::classic_sync(&handle, &native_id, join.generation_id).await;

        check!(sync.error_code == codes::UNKNOWN_MEMBER_ID);
        check!(sync.assignment.is_empty());
        let after = describe_member(&handle, &native_id).await;
        check!(after.assigned_partitions == before.assigned_partitions);
        check!(!after.is_classic);
        // Nor did the rejected sync move anything between the two members.
        let classic_after = describe_member(&handle, "m-classic").await;
        check!(classic_after.assigned_partitions == classic_before.assigned_partitions);
    }

    /// A coordinator failover must not turn a synced hosted classic member
    /// into a rebalancing one. The sync writes the member's k7/k8 records, and
    /// hydrating a fresh actor from exactly those records rebuilds what the
    /// member last synced — no krabka-private field in the k5 record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failover_keeps_a_synced_hosted_classic_member_in_sync() {
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        let sync = rpc::classic_sync(&handle, "m-classic", join.generation_id).await;
        assert!(sync.error_code == codes::NONE);

        // Fail over: a fresh coordinator hydrates the group from the records
        // the sync left behind.
        let seed = coord
            .cached_seed("g")
            .expect("records for the synced group");
        let (failover, _failover_log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let restored = failover.get_or_create_consumer("g");
        restored
            .tx
            .send(GroupActorMessage::Seed(seed))
            .await
            .unwrap();

        check!(
            rpc::classic_heartbeat(&restored, "m-classic").await == codes::NONE,
            "a member that synced before the failover owes no re-sync after it"
        );
        // The restored target still assigns the same partitions, so a re-sync
        // returns the identical blob.
        let resync = rpc::classic_sync(&restored, "m-classic", join.generation_id).await;
        check!(resync.error_code == codes::NONE);
        check!(resync.assignment == sync.assignment);
    }
}
