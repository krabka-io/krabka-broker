//! The actor's message router.
//!
//! [`handle_actor_message`] is the single `match` over [`GroupActorMessage`].
//! It owns no policy of its own: every arm dispatches on the group's LIVE kind
//! and delegates to the module that implements that RPC. The return value is
//! the actor loop's keep-running flag.

use krabka_protocol::owned::heartbeat_request::HeartbeatRequest;

use super::{
    ActorServices, ErrorCode, GroupActorMessage, MetadataProvider, ParkedWaiters,
    classic_join::handle_classic_join_message,
    classic_leave::{handle_classic_delete_message, handle_classic_leave_message},
    classic_sync::handle_classic_sync_message,
    commit_validation::validate_commit_message,
    heartbeat::handle_actor_heartbeat,
    messages::classic_leave_result,
    seed::apply_seed,
    views::{build_classic_view, build_describe, inspect_any},
};
use crate::{
    codes,
    coordinator::unified::{ClientIdentity, classic_ops, group::CoordinatorGroup, migration},
};

fn handle_classic_heartbeat_message(
    group: &mut CoordinatorGroup,
    metadata: &dyn MetadataProvider,
    request: &HeartbeatRequest,
) -> ErrorCode {
    if let Some(state) = group.as_classic_mut() {
        classic_ops::handle_heartbeat(state, request)
    } else if let Some(state) = group.as_consumer_mut() {
        migration::serve_classic_heartbeat(state, &request.member_id, &metadata.snapshot())
    } else {
        codes::UNKNOWN_MEMBER_ID
    }
}

pub(super) async fn handle_actor_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    message: GroupActorMessage,
) -> bool {
    match message {
        GroupActorMessage::Heartbeat {
            request,
            client_id,
            client_host,
            reply,
        } => {
            handle_actor_heartbeat(
                group,
                services,
                request,
                ClientIdentity {
                    id: &client_id,
                    host: &client_host,
                },
                reply,
            )
            .await
        }
        GroupActorMessage::ValidateCommit {
            member_id,
            group_instance_id,
            generation_or_epoch,
            reply,
        } => {
            let result = validate_commit_message(
                group,
                &member_id,
                group_instance_id.as_deref(),
                generation_or_epoch,
            );
            let _ = reply.send(result);
            true
        }
        GroupActorMessage::Describe { reply } => {
            if let Some(state) = group.as_consumer() {
                let _ = reply.send(build_describe(state));
            }
            true
        }
        GroupActorMessage::ClassicJoin {
            req,
            version,
            client_id,
            client_host,
            reply,
        } => {
            handle_classic_join_message(
                group,
                parked,
                services,
                req,
                version,
                &client_id,
                &client_host,
                reply,
            )
            .await
        }
        GroupActorMessage::ClassicSync { req, reply } => {
            handle_classic_sync_message(group, parked, services, req, reply).await;
            true
        }
        GroupActorMessage::ClassicHeartbeat { req, reply } => {
            let code = handle_classic_heartbeat_message(group, services.metadata, &req);
            let _ = reply.send(code);
            true
        }
        GroupActorMessage::ClassicLeave {
            req,
            version,
            reply,
        } => {
            let consumer_kind = group.is_consumer();
            let result = handle_classic_leave_message(group, parked, services, &req, version).await;
            let keep_running = result.is_ok() || !consumer_kind;
            let result = classic_leave_result(version, result);
            let _ = reply.send(result);
            keep_running
        }
        GroupActorMessage::ClassicDelete { reply } => {
            handle_classic_delete_message(group, services.offsets_log, reply).await
        }
        GroupActorMessage::ClassicInspect { reply } => {
            if let Some(state) = group.as_classic() {
                let _ = reply.send(build_classic_view(state));
            }
            true
        }
        GroupActorMessage::InspectAny { reply } => {
            if let Some(snapshot) = inspect_any(group, services.metadata) {
                let _ = reply.send(snapshot);
            }
            true
        }
        GroupActorMessage::UpdateCommitted { entries, reply } => {
            group.committed_offsets.extend(entries);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::FetchOffsets { reply } => {
            let _ = reply.send(group.offsets());
            true
        }
        GroupActorMessage::RemoveCommitted { keys, reply } => {
            for key in keys {
                group.committed_offsets.remove(&key);
            }
            let _ = reply.send(());
            true
        }
        GroupActorMessage::AddPendingTxnOffsets {
            producer_id,
            keys,
            reply,
        } => {
            group.add_pending_txn_offsets(producer_id, keys);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::ResolveTxnOffsets {
            producer_id,
            committed,
            reply,
        } => {
            group.committed_offsets.extend(committed);
            group.clear_pending_txn_offsets(producer_id);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::Seed(seed) => {
            if let Some(state) = group.as_consumer_mut() {
                apply_seed(state, seed);
            }
            true
        }
        GroupActorMessage::ClassicSeed(seeded) => {
            *group = *seeded;
            true
        }
        GroupActorMessage::Shutdown(reply) => {
            let _ = reply.send(());
            false
        }
        #[cfg(test)]
        GroupActorMessage::TestForceConsumerKind => {
            *group = CoordinatorGroup::new_consumer(group.group_id.clone());
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;

    use super::*;
    use crate::coordinator::unified::actor::test_support::make_coordinator;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_seed_hydrates_group_and_blocks_delete_when_nonempty() {
        use std::time::Duration;

        use crate::coordinator::unified::{
            classic_state::{ClassicGroup as ClassicState, Member, OffsetEntry},
            group::{CoordinatorGroup, GroupKind},
        };
        let (coord, _log) = make_coordinator();

        let mut cs = ClassicState::new("g");
        cs.add_member(Member::new(
            "m1",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), bytes::Bytes::new())],
        ));
        let group = Box::new(CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(cs),
            [(
                ("t".to_string(), 0),
                OffsetEntry {
                    offset: Offset(7),
                    leader_epoch: 0,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                },
            )]
            .into(),
        ));
        coord.seed_classic("g", group);

        // Seeded committed offsets and member are visible.
        let handle = coord.find("g").expect("seeded actor");
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply: tx })
            .await
            .unwrap();
        assert!(
            rx.await
                .unwrap()
                .committed
                .get(&("t".to_string(), 0))
                .unwrap()
                .offset
                == 7
        );
        // Non-empty group cannot be deleted.
        assert!(
            coord.delete_group("g").await == Err(crate::coordinator::DeleteGroupError::NonEmpty)
        );
    }
}
