//! The actor's message router.
//!
//! [`handle_actor_message`] is the single `match` over [`GroupActorMessage`].
//! It owns no policy of its own: every arm dispatches on the group's LIVE kind
//! and delegates to the module that implements that RPC. The return value is
//! the actor loop's keep-running flag.

use {
    super::{
        ActorServices, ErrorCode, GroupActorMessage, ParkedWaiters,
        classic_join::handle_classic_join_message,
        classic_leave::{handle_classic_delete_message, handle_classic_leave_message},
        classic_sync::handle_classic_sync_message,
        commit_validation::validate_commit_message,
        heartbeat::handle_actor_heartbeat,
        messages::{LeaveResult, classic_leave_result},
        metadata_update::on_metadata_update,
        offset_delete::{delete_held_offsets, offset_delete_guard},
        pending_commits::CommitWaits,
        retention::handle_reap_message,
        seed::apply_seed,
        topic_deletion::reply_delete_topic_offsets,
        views::{build_classic_view, build_describe, inspect_any},
        waiters::drain_parked_for_unload,
    },
    crate::{
        codes,
        coordinator::unified::{ClientIdentity, classic_ops, group::CoordinatorGroup, migration},
    },
    krabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest, leave_group_request::LeaveGroupRequest,
    },
    tokio::sync::oneshot,
};

fn handle_classic_heartbeat_message(
    group: &mut CoordinatorGroup,
    request: &HeartbeatRequest,
) -> ErrorCode {
    if let Some(state) = group.as_classic_mut() {
        classic_ops::handle_heartbeat(state, request)
    } else if let Some(state) = group.as_consumer_mut() {
        migration::serve_classic_heartbeat(state, request)
    } else {
        codes::UNKNOWN_MEMBER_ID
    }
}

/// Answers a classic `LeaveGroup` and returns the actor's keep-running flag,
/// which is false only when a consumer-kind group's leave fails.
async fn reply_classic_leave(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: &LeaveGroupRequest,
    version: i16,
    reply: oneshot::Sender<LeaveResult>,
) -> bool {
    let consumer_kind = group.is_consumer();
    let result = handle_classic_leave_message(group, parked, services, request, version).await;
    let keep_running = result.is_ok() || !consumer_kind;
    let _ = reply.send(classic_leave_result(version, result));
    keep_running
}

pub(super) async fn handle_actor_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    commits: &mut CommitWaits,
    services: ActorServices<'_>,
    message: GroupActorMessage,
) -> bool {
    match message {
        GroupActorMessage::Heartbeat {
            request,
            client_id,
            client_host,
            regex_resolver,
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
                &*regex_resolver,
                reply,
            )
            .await
        }
        GroupActorMessage::ValidateCommit { commit, reply } => {
            let _ = reply.send(validate_commit_message(group, &commit));
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
            regex_resolver,
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
                &*regex_resolver,
                reply,
            )
            .await
        }
        GroupActorMessage::ClassicSync { req, reply } => {
            handle_classic_sync_message(group, parked, services, req, reply).await;
            true
        }
        GroupActorMessage::ClassicHeartbeat { req, reply } => {
            let code = handle_classic_heartbeat_message(group, &req);
            let _ = reply.send(code);
            true
        }
        GroupActorMessage::ClassicLeave {
            req,
            version,
            reply,
        } => reply_classic_leave(group, parked, services, &req, version, reply).await,
        GroupActorMessage::ClassicDelete { reply } => {
            handle_classic_delete_message(group, services.offsets_log, reply).await
        }
        GroupActorMessage::ClassicInspect { reply } => {
            if let Some(state) = group.as_classic() {
                let _ = reply.send(build_classic_view(state));
            }
            true
        }
        GroupActorMessage::OffsetDeleteGuard { reply } => {
            let _ = reply.send(offset_delete_guard(group));
            true
        }
        GroupActorMessage::InspectAny { reply } => {
            if let Some(snapshot) = inspect_any(group, services.metadata) {
                let _ = reply.send(snapshot);
            }
            true
        }
        GroupActorMessage::CommitOffsets {
            batch,
            entries,
            reply,
        } => {
            commits
                .begin(group, services.offsets_log, batch, entries, reply)
                .await;
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
        GroupActorMessage::FetchOffsetsForMember {
            member,
            selection,
            reply,
        } => {
            let _ = reply.send(group.offsets_for_member(&member, &selection));
            true
        }
        GroupActorMessage::RemoveCommitted { keys, reply } => {
            group.remove_committed(&keys);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::DeleteOffsets { keys, reply } => {
            let _ = reply.send(delete_held_offsets(group, services.offsets_log, &keys).await);
            true
        }
        GroupActorMessage::DeleteTopicOffsets { topics, reply } => {
            reply_delete_topic_offsets(group, services.offsets_log, &topics, reply).await
        }
        GroupActorMessage::MetadataUpdate { topics } => {
            on_metadata_update(group, &topics);
            true
        }
        GroupActorMessage::AddPendingTxnOffsets {
            producer_id,
            written_at,
            keys,
            reply,
        } => {
            group.add_pending_txn_offsets(producer_id, written_at, keys);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::TxnOffsetReservation(reservation) => reservation.apply(group),
        GroupActorMessage::ResolveTxnOffsets {
            producer_id,
            resolved_through,
            committed,
            reply,
        } => {
            group.publish_txn_offsets(resolved_through, committed);
            group.resolve_pending_txn_offsets(producer_id, resolved_through);
            let _ = reply.send(());
            true
        }
        GroupActorMessage::ReapExpiredOffsets {
            now_ms,
            retention_ms,
            empty_grace_ms,
            reply,
        } => {
            handle_reap_message(
                group,
                services.offsets_log,
                services.coordinator,
                now_ms,
                retention_ms,
                empty_grace_ms,
                reply,
            )
            .await
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
            // Kafka's `GroupMetadataManager.onUnloaded`: a parked `JoinGroup`
            // or `SyncGroup` learns that this broker no longer coordinates the
            // group, so its client looks the coordinator up again instead of
            // retrying here. Its runtime fails the shard's pending writes the
            // same way, so a commit still replicating answers `NOT_COORDINATOR`.
            drain_parked_for_unload(&mut parked.joiners, &mut parked.followers);
            commits.fail_all();
            let _ = reply.send(());
            false
        }
        #[cfg(test)]
        GroupActorMessage::TestForceConsumerKind => {
            *group = CoordinatorGroup::new_consumer(group.group_id.clone());
            true
        }
        #[cfg(test)]
        GroupActorMessage::TestEmptySinceMs { reply } => {
            let _ = reply.send(group.empty_since_ms);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::{
            test_support::offset_entry,
            unified::{
                GroupCoordinator,
                actor::{
                    GroupActorHandle,
                    test_support::{
                        classic_member, make_coordinator, rpc, seed_completing_classic,
                    },
                },
                classic_state::OffsetEntry,
                offsets_log::fake::InMemoryOffsetsLog,
            },
        },
        assert2::assert,
        krabka_log::Offset,
        krabka_protocol::records::RecordBatch,
        std::{collections::HashMap, sync::Arc, time::Duration},
        tokio::sync::oneshot::{Receiver, error::TryRecvError},
    };

    /// Kafka's `GroupMetadataManager.onUnloaded` answers every awaiting
    /// `JoinGroup` of a `PreparingRebalance` group, under the member's own id,
    /// and every awaiting `SyncGroup` of a `CompletingRebalance` group with
    /// `NOT_COORDINATOR`, so the client looks the coordinator up again instead
    /// of joining the old broker once more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_answers_parked_joiners_and_followers_not_coordinator() {
        use {
            crate::coordinator::unified::actor::{JoinResult, SyncResult, test_support::rpc},
            krabka_protocol::owned::sync_group_request::SyncGroupRequest,
        };

        // A member that waits in `JoinGroup`, behind the initial delay.
        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");
        coord.mark_classic("g");
        let member_id = rpc::classic_join(&handle, "", "t").await.member_id;
        let join_rx = rpc::begin(&handle, |reply| {
            rpc::classic_join_message(&member_id, "t", reply)
        })
        .await;
        rpc::shutdown(&handle).await;
        assert!(
            join_rx.await.unwrap()
                == JoinResult {
                    error_code: codes::NOT_COORDINATOR,
                    member_id,
                    ..JoinResult::default()
                }
        );

        // A follower that waits in `SyncGroup` for the leader.
        let (coord, _log) = make_coordinator();
        let seeded = seed_completing_classic(&coord, &["m1", "m2"]);
        let handle = seeded.handle;
        let generation = seeded.generation;
        let sync_rx = rpc::begin_classic_sync(&handle, SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: "m2".into(),
            ..Default::default()
        })
        .await;
        rpc::shutdown(&handle).await;
        assert!(
            sync_rx.await.unwrap()
                == SyncResult {
                    error_code: codes::NOT_COORDINATOR,
                    ..SyncResult::default()
                }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_seed_hydrates_group_and_blocks_delete_when_nonempty() {
        use crate::coordinator::unified::{
            classic_state::{ClassicGroup as ClassicState, OffsetEntry},
            group::{CoordinatorGroup, GroupKind},
        };
        let (coord, _log) = make_coordinator();

        let mut cs = ClassicState::new("g");
        cs.add_member(classic_member("m1"));
        let group = Box::new(CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(cs),
            [(("t".to_string(), 0), OffsetEntry {
                offset: Offset(7),
                leader_epoch: 0,
                metadata: String::new(),
                commit_timestamp_ms: 0,
                expire_timestamp_ms: None,
                topic_id: None,
            })]
            .into(),
        ));
        coord.seed_classic("g", group);

        // Seeded committed offsets and member are visible.
        let handle = coord.find("g").expect("seeded actor");
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::FetchOffsets { reply: tx }).await;
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

    /// How long a message may take to answer while a commit replicates. It is
    /// far below the 5 s write timeout the actor used to hold its turn for.
    const PROMPT: Duration = Duration::from_secs(1);

    /// Sends an `OffsetCommit` of `offset` for partition `t-partition` and
    /// returns its pending answer.
    async fn begin_commit(
        handle: &GroupActorHandle,
        partition: i32,
        offset: i64,
    ) -> Receiver<Result<(), ErrorCode>> {
        rpc::begin(handle, |reply| GroupActorMessage::CommitOffsets {
            batch: RecordBatch::default(),
            entries: vec![(("t".to_string(), partition), offset_entry(offset))],
            reply,
        })
        .await
    }

    /// A classic group `g` whose offsets log holds back its high watermark,
    /// with one commit of `t-0` in the log and waiting for it.
    struct HeldCommit {
        coord: Arc<GroupCoordinator>,
        log: Arc<InMemoryOffsetsLog>,
        handle: Arc<GroupActorHandle>,
        pending: Receiver<Result<(), ErrorCode>>,
    }

    /// Commits `offset` to `t-0` of a fresh group whose high watermark is
    /// held, and returns once the commit is in the log, still invisible.
    async fn held_commit(offset: i64) -> HeldCommit {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");
        log.hold_commits();
        let pending = begin_commit(&handle, 0, offset).await;
        // The fetch is answered after the commit's turn, so the record is in
        // the log by the time it returns.
        assert!(committed_now(&handle).await == offsets(&[]));
        HeldCommit {
            coord,
            log,
            handle,
            pending,
        }
    }

    async fn committed_now(handle: &GroupActorHandle) -> HashMap<(String, i32), OffsetEntry> {
        tokio::time::timeout(PROMPT, rpc::fetch_committed(handle))
            .await
            .expect("OffsetFetch answers while a commit replicates")
    }

    fn offsets(entries: &[(i32, i64)]) -> HashMap<(String, i32), OffsetEntry> {
        entries
            .iter()
            .map(|&(partition, offset)| (("t".to_string(), partition), offset_entry(offset)))
            .collect()
    }

    /// Kafka's `CoordinatorRuntime` keeps serving a group while one of its
    /// writes waits for the high watermark: `OffsetFetch` reads the state as
    /// of the last committed offset, so it answers at once with the offsets
    /// the group last acknowledged, and a heartbeat is not queued behind the
    /// commit either. The commit's own answer waits for the high watermark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fetch_and_a_heartbeat_answer_while_a_commit_replicates() {
        let (coord, log) = make_coordinator();
        let handle = coord.get_or_create_consumer("g");
        let joined = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(joined.error_code == codes::NONE);
        let member_id = joined.member_id.clone().expect("a joined member has an id");
        assert!(begin_commit(&handle, 0, 5).await.await.unwrap() == Ok(()));

        log.hold_commits();
        let mut pending = begin_commit(&handle, 0, 9).await;

        assert!(committed_now(&handle).await == offsets(&[(0, 5)]));
        let heartbeat = tokio::time::timeout(
            PROMPT,
            rpc::consumer_heartbeat(&handle, &member_id, joined.member_epoch, Some("t")),
        )
        .await
        .expect("a heartbeat answers while a commit replicates");
        assert!(heartbeat.error_code == codes::NONE);
        assert!(pending.try_recv() == Err(TryRecvError::Empty));

        log.release_commits();
        assert!(pending.await.unwrap() == Ok(()));
        assert!(committed_now(&handle).await == offsets(&[(0, 9)]));
    }

    /// Two commits of one partition replicate together. Whichever wait the
    /// actor sees end first, the offsets are published in log order, so the
    /// later commit's offset is the one that stays.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commits_that_replicate_together_publish_in_log_order() {
        let earlier = held_commit(5).await;
        let later = begin_commit(&earlier.handle, 0, 9).await;
        assert!(committed_now(&earlier.handle).await == offsets(&[]));

        earlier.log.release_commits();
        assert!(later.await.unwrap() == Ok(()));
        assert!(earlier.pending.await.unwrap() == Ok(()));
        assert!(committed_now(&earlier.handle).await == offsets(&[(0, 9)]));
    }

    /// A commit that times out answers `COORDINATOR_NOT_AVAILABLE` and stays
    /// invisible, but its record is in the log. Once the high watermark passes
    /// it after all, Kafka shows it, with no further write needed: so does the
    /// actor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_timed_out_commit_becomes_visible_once_the_high_watermark_passes_it() {
        let timed_out = held_commit(7).await;
        timed_out.log.time_out_uncommitted();
        assert!(timed_out.pending.await.unwrap() == Err(codes::COORDINATOR_NOT_AVAILABLE));
        assert!(committed_now(&timed_out.handle).await == offsets(&[]));

        timed_out.log.release_commits();
        tokio::time::timeout(PROMPT, async {
            while committed_now(&timed_out.handle).await != offsets(&[(0, 7)]) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the timed-out commit is published once it commits");
    }

    /// `DeleteGroups` of an empty group while a commit of it replicates: the
    /// group's tombstones come later in the log, so once they commit the
    /// commit has too. Kafka answers the commit by its own write's outcome,
    /// and only an unload fails it, so the client is not told to retry a
    /// commit that landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_replicating_under_a_group_delete_answers_its_own_outcome() {
        let commit = held_commit(7).await;
        let delete = tokio::spawn({
            let coord = Arc::clone(&commit.coord);
            async move { coord.delete_group("g").await }
        });
        // The delete's tombstones are in the log once a second batch is.
        while commit.log.batches().await.len() < 2 {
            tokio::task::yield_now().await;
        }

        commit.log.release_commits();
        assert!(delete.await.unwrap() == Ok(()));
        assert!(commit.pending.await.unwrap() == Ok(()));
    }

    /// Kafka's runtime fails the pending writes of a shard it unloads with
    /// `NOT_COORDINATOR`. A commit still replicating when the actor stops gets
    /// that answer rather than a dropped reply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_still_replicating_at_shutdown_answers_not_coordinator() {
        let unloaded = held_commit(7).await;
        rpc::shutdown(&unloaded.handle).await;
        assert!(unloaded.pending.await.unwrap() == Err(codes::NOT_COORDINATOR));
    }
}
