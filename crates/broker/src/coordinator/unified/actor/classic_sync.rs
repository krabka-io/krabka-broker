//! The classic `SyncGroup` path.
//!
//! The leader's request installs the round's assignments, persists the classic
//! k2 snapshot, and releases every follower parked behind it. A consumer-kind
//! group answers the same RPC from its reconciler target instead.

use krabka_protocol::owned::sync_group_request::SyncGroupRequest;
use tokio::sync::oneshot;

use super::{
    ActorServices, ParkedWaiters, SyncResult, persistence::flush_classic_metadata,
    waiters::drain_parked_followers,
};
use crate::{
    codes,
    coordinator::unified::{classic_ops, group::CoordinatorGroup, migration},
};

pub(super) async fn handle_classic_sync_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: SyncGroupRequest,
    reply: oneshot::Sender<SyncResult>,
) {
    let Some(state) = group.as_classic_mut() else {
        let result = group.as_consumer_mut().map_or_else(
            || SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            },
            |state| {
                migration::serve_classic_sync(
                    state,
                    &request.member_id,
                    &services.metadata.snapshot(),
                )
            },
        );
        let _ = reply.send(result);
        return;
    };
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
            GroupActorMessage,
            test_support::{
                completing_classic_group, decode_assignment, last_classic_metadata,
                make_coordinator, make_coordinator_with_topic_policy, rpc, seed_and_upgrade,
            },
        },
        classic_state::GroupState as ClassicGroupState,
    };

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
}
