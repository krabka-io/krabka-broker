//! The classic `JoinGroup` path.
//!
//! A native classic group runs the 5-state machine: the request either answers
//! at once, parks until the rebalance boundary, or completes the round. An
//! upgraded consumer group serves the same RPC for a hosted classic member by
//! upserting it into the next-gen state and reconciling, so both flavours of
//! `JoinGroup` live together here.

use std::time::{Duration, Instant};

use bytes::Bytes;
use krabka_protocol::owned::join_group_request::JoinGroupRequest;
use tokio::sync::oneshot;

use super::{
    ActorServices, FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS, JoinResult,
    MetadataProvider, ParkedWaiters, chrono_now_ms,
    member_state::{refresh_expired_metadata, run_reconcile},
    persistence::{flush_classic_metadata, flush_pending, snapshot_pending_after_change},
    waiters::{complete_classic_rebalance, drain_followers_with, fence_replaced_classic_member},
};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator, classic_ops,
        classic_state::GroupState as ClassicGroupState,
        config::{ConsumerGroupMigrationPolicy, NextGenConfig},
        group::{CoordinatorGroup, GroupKind},
        migration,
        offsets_log::OffsetsLog,
    },
};

/// Kafka's `appendGroupMetadataErrorToResponseError`: the `JoinGroup` error
/// for a group metadata write that failed.
fn append_error_code(error: &crate::error::BrokerError) -> i16 {
    match codes::from_broker_error(error) {
        codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::NOT_ENOUGH_REPLICAS
        | codes::REQUEST_TIMED_OUT => codes::COORDINATOR_NOT_AVAILABLE,
        codes::NOT_LEADER_OR_FOLLOWER | codes::KAFKA_STORAGE_ERROR => codes::NOT_COORDINATOR,
        codes::MESSAGE_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => other,
    }
}

#[allow(clippy::too_many_arguments)] // Keeps the actor message boundary explicit.
pub(super) async fn handle_classic_join_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    mut request: JoinGroupRequest,
    version: i16,
    client_id: &str,
    client_host: &str,
    reply: oneshot::Sender<JoinResult>,
) -> bool {
    // Kafka's `JoinGroupRequest.maybeOverrideRebalanceTimeout`: v0 has no
    // rebalance timeout field, so the session timeout stands in for it. The
    // native and the hosted paths below then read the same value.
    if version == 0 {
        request.rebalance_timeout_ms = request.session_timeout_ms;
    }
    if let Some(consumer) = group.as_consumer() {
        if consumer.members.is_empty() {
            // Kafka's `classicGroupJoin` sends a join to an empty consumer
            // group down the classic path, which deletes the consumer group
            // and creates a classic one; the committed offsets stay with the
            // group id.
            let batch = super::retention::tombstone_batch(
                &group.group_id,
                &[],
                Some(&group.kind),
                chrono_now_ms(),
            );
            if services
                .offsets_log
                .append(&group.group_id, batch)
                .await
                .is_err()
            {
                let _ = reply.send(JoinResult {
                    error_code: codes::COORDINATOR_NOT_AVAILABLE,
                    member_id: request.member_id,
                    ..JoinResult::default()
                });
                return true;
            }
            services
                .coordinator
                .mark_classic_after_downgrade(&group.group_id);
            *group.kind_mut() = GroupKind::Classic(
                crate::coordinator::unified::classic_state::ClassicGroup::new(
                    group.group_id.clone(),
                ),
            );
        } else if services.config.migration_policy == ConsumerGroupMigrationPolicy::Disabled {
            // `throwIfClassicMemberCannotJoinConsumerGroup`.
            let _ = reply.send(JoinResult {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                member_id: request.member_id,
                ..JoinResult::default()
            });
            return true;
        }
    }
    if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let outcome = classic_ops::handle_join(
            state,
            &mut request,
            &classic_ops::JoinContext {
                client_id,
                client_host,
                version,
                initial_rebalance_delay: services.config.classic_initial_rebalance_delay,
                max_size: services.config.classic_max_size,
                now: Instant::now(),
            },
        );
        if let Some(fenced) = outcome.fenced_member.as_deref() {
            fence_replaced_classic_member(fenced, &mut parked.joiners, &mut parked.followers);
        }
        // Kafka's `prepareRebalance` from `CompletingRebalance` answers every
        // member that waits in `SyncGroup` with `REBALANCE_IN_PROGRESS`.
        if previous.state == ClassicGroupState::CompletingRebalance
            && state.state == ClassicGroupState::PreparingRebalance
        {
            drain_followers_with(&mut parked.followers, codes::REBALANCE_IN_PROGRESS);
        }
        match outcome.action {
            classic_ops::JoinAction::Immediate(result) => {
                let _ = reply.send(result);
            }
            classic_ops::JoinAction::PersistThenReply(result) => {
                if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                    // Kafka reverts the replacement and answers with the
                    // append error, under the unknown member id and the
                    // leader from before the join.
                    let leader = previous.leader_id.clone().unwrap_or_default();
                    *state = previous;
                    tracing::warn!(group_id = %state.group_id, %error,
                        "classic static rejoin log write failed");
                    let _ = reply.send(JoinResult {
                        error_code: append_error_code(&error),
                        generation_id: state.generation_id,
                        protocol_type: state.protocol_type.clone(),
                        protocol_name: state.protocol_name.clone(),
                        leader,
                        ..JoinResult::default()
                    });
                    return true;
                }
                let _ = reply.send(result);
            }
            classic_ops::JoinAction::Park => {
                parked.joiners.insert(request.member_id, reply);
            }
            classic_ops::JoinAction::CompleteNow => {
                parked.joiners.insert(request.member_id, reply);
                // Every member joined, so none is removed and the group stays
                // non-empty: there is nothing to persist.
                let _ =
                    complete_classic_rebalance(state, &mut parked.joiners, &mut parked.followers);
            }
        }
        return true;
    }
    if group.as_consumer().is_some() {
        return classic_join_hosted(
            group,
            services.config,
            services.metadata,
            services.offsets_log,
            services.coordinator,
            HostedJoin {
                request: &request,
                client_id,
                client_host,
                reply,
                now_ms: chrono_now_ms(),
            },
        )
        .await
        .is_ok();
    }
    let _ = reply.send(JoinResult {
        error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
        member_id: request.member_id,
        ..JoinResult::default()
    });
    true
}

/// KIP-848 live migration: serves a classic `JoinGroup` for a member hosted in
/// an upgraded consumer group.
///
/// This function upserts the member into the next-gen state. When the member's
/// subscription is new or changed, which makes the group dirty, it reconciles
/// and persists the membership change exactly as `handle_heartbeat`'s
/// first-join path does: `run_reconcile`, then `advance_member_epoch`, then
/// `snapshot_pending_after_change`, then `flush_pending`.
///
/// It replies on `reply` with a server-assigned single-member `JoinResult`.
/// The member receives the assignment on its next `SyncGroup`. It returns
/// `Err` only on a log-write failure, so the actor exits, and it first replies
/// with the same failure code the heartbeat path uses.
struct HostedJoin<'a> {
    request: &'a JoinGroupRequest,
    client_id: &'a str,
    client_host: &'a str,
    reply: oneshot::Sender<JoinResult>,
    now_ms: i64,
}

async fn classic_join_hosted(
    group: &mut CoordinatorGroup,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    hosted: HostedJoin<'_>,
) -> Result<(), crate::error::BrokerError> {
    let HostedJoin {
        request: req,
        client_id,
        client_host,
        reply,
        now_ms,
    } = hosted;
    // Decode the subscription from the first protocol whose metadata is a valid
    // `ConsumerProtocolSubscription` (mirrors `convert_classic_to_consumer`,
    // which derives topics from a member's selected protocol metadata). The
    // matching protocol's name is echoed back as the result's `protocol_name`.
    let decoded = req.protocols.iter().find_map(|p| {
        migration::decode_consumer_subscription(&p.metadata).map(|sub| (p.name.clone(), sub.topics))
    });
    let (protocol_name, topics) = match decoded {
        Some((name, topics)) => (Some(name), topics.into_iter().collect()),
        None => (
            req.protocols.first().map(|p| p.name.clone()),
            std::collections::HashSet::new(),
        ),
    };
    let protocols: Vec<(String, Bytes)> = req
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.clone()))
        .collect();
    let session_timeout = Duration::from_millis(
        u64::try_from(req.session_timeout_ms.max(0)).unwrap_or(FALLBACK_SESSION_TIMEOUT_MS),
    );
    let rebalance_timeout = Duration::from_millis(
        u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
    );

    let state = group
        .as_consumer_mut()
        .expect("caller verified consumer kind");
    migration::upsert_classic_member(
        state,
        migration::ClassicMemberRegistration {
            member_id: req.member_id.clone(),
            subscription_topics: topics,
            protocols,
            client_id: client_id.to_string(),
            client_host: client_host.to_string(),
            session_timeout,
            rebalance_timeout,
            instance_id: req.group_instance_id.clone(),
        },
    );
    refresh_expired_metadata(state, metadata);
    if state.dirty {
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&req.member_id);
        let pending =
            snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id), true);
        if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
            tracing::warn!(
                group_id = %state.group_id, error = %e,
                "next-gen actor exiting after hosted classic-join log-write failure",
            );
            let _ = reply.send(JoinResult {
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                member_id: req.member_id.clone(),
                ..JoinResult::default()
            });
            return Err(e);
        }
    }
    let result = migration::build_hosted_classic_join_result(state, &req.member_id, protocol_name);
    let _ = reply.send(result);
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            GroupActorMessage, SyncResult,
            test_support::{
                completing_classic_group, decode_assignment, last_classic_metadata,
                make_coordinator, make_coordinator_with_config, make_coordinator_with_topic_policy,
                rpc, seed_and_upgrade,
            },
        },
        classic_state::GroupState as ClassicGroupState,
        config::NextGenConfig,
    };

    /// Kafka's `group.initial.rebalance.delay.ms = 0`: the first member of a
    /// new group gets its `JoinGroup` answer as soon as the round opens, rather
    /// than after the three-second default batching window.
    ///
    /// The member joins as a v4 client does: an empty member id answers
    /// `MEMBER_ID_REQUIRED` with the id to use, and the rejoin with that id opens
    /// the round.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_initial_rebalance_delay_completes_a_new_groups_first_join_at_once() {
        let (coord, _log) = make_coordinator_with_config(NextGenConfig {
            classic_initial_rebalance_delay: std::time::Duration::ZERO,
            ..NextGenConfig::default()
        });
        let handle = coord.get_or_create_classic("g");
        coord.mark_classic("g");

        let assigned = rpc::classic_join(&handle, "", "t").await;
        check!(assigned.error_code == codes::MEMBER_ID_REQUIRED);
        let member_id = assigned.member_id;

        let join = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rpc::classic_join(&handle, &member_id, "t"),
        )
        .await
        .expect("a zero delay must not wait out a batching window");

        check!(join.error_code == codes::NONE);
        check!(join.generation_id == 1);
        check!(join.member_id == member_id);
        check!(join.leader == member_id);
        check!(join.protocol_name.as_deref() == Some("range"));
        let members: Vec<&str> = join.members.iter().map(|m| m.member_id.as_str()).collect();
        check!(members == [member_id.as_str()]);
    }

    /// Kafka's `JoinGroupRequest.maybeOverrideRebalanceTimeout`: a v0 request
    /// has no rebalance timeout field, so its session timeout is the member's
    /// rebalance timeout, and that is what the group persists. From v1 the
    /// request's own rebalance timeout stands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_v0_takes_the_rebalance_timeout_from_the_session_timeout() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        // (version, rebalance timeout the request carries, the one persisted)
        for (version, requested_ms, want_ms) in [(0, -1, 45_000), (1, 90_000, 90_000)] {
            let (coord, log) = make_coordinator_with_config(NextGenConfig {
                classic_initial_rebalance_delay: Duration::ZERO,
                ..NextGenConfig::default()
            });
            let handle = coord.get_or_create_classic("g");
            coord.mark_classic("g");
            let (tx, rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::ClassicJoin {
                    req: JoinGroupRequest {
                        group_id: "g".into(),
                        session_timeout_ms: 45_000,
                        rebalance_timeout_ms: requested_ms,
                        protocol_type: "consumer".into(),
                        protocols: vec![JoinGroupRequestProtocol {
                            name: "range".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    version,
                    client_id: "client-a".into(),
                    client_host: "127.0.0.1".into(),
                    reply: tx,
                })
                .await
                .unwrap();
            let joined = rx.await.unwrap();
            check!(joined.error_code == codes::NONE, "v{version}");

            let synced = rpc::classic_sync(&handle, &joined.member_id, joined.generation_id).await;
            check!(synced.error_code == codes::NONE, "v{version}");

            let persisted = last_classic_metadata(&log).await;
            check!(persisted.members.len() == 1, "v{version}");
            check!(
                persisted.members[0].rebalance_timeout_ms == want_ms,
                "v{version}"
            );
        }
    }

    /// A `Stable` group whose one member, `m1`, is the static member
    /// `instance-1`, and a `JoinGroup` v4 from a restarted `instance-1`.
    fn stable_static_group_and_rejoin() -> (CoordinatorGroup, JoinGroupRequest) {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let member = state.members.get_mut("m1").unwrap();
        member.group_instance_id = Some("instance-1".into());
        member.assignment = Some(Bytes::from_static(b"assignment"));
        state
            .static_members
            .insert("instance-1".into(), "m1".into());
        state.state = ClassicGroupState::Stable;
        let request = JoinGroupRequest {
            group_id: "g".into(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: String::new(),
            group_instance_id: Some("instance-1".into()),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: Bytes::from_static(b"new-subscription"),
                ..Default::default()
            }],
            ..Default::default()
        };
        (group, request)
    }

    async fn send_join(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        req: JoinGroupRequest,
    ) -> JoinResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req,
                version: 4,
                client_id: "new-client".into(),
                client_host: "new-host".into(),
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// #789: Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin` in
    /// `Stable`: a new member id replaces the old one, keeps the client and
    /// the assignment, and is persisted before the reply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_stable_static_rejoin_persists_replaced_member() {
        let (coord, log) = make_coordinator();
        let (group, request) = stable_static_group_and_rejoin();
        let generation = group.as_classic().unwrap().generation_id;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();

        let response = send_join(&handle, request).await;

        check!(response.member_id.starts_with("instance-1-"));
        check!(
            response
                == JoinResult {
                    error_code: codes::NONE,
                    generation_id: generation,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    leader: "m1".into(),
                    skip_assignment: false,
                    member_id: response.member_id.clone(),
                    members: Vec::new(),
                }
        );
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == generation);
        check!(persisted.members.len() == 1);
        check!(persisted.members[0].member_id == response.member_id);
        check!(persisted.members[0].client_id == "client");
        check!(persisted.members[0].client_host == "host");
        check!(persisted.members[0].subscription == Bytes::from_static(b"new-subscription"));
        check!(persisted.members[0].assignment == Bytes::from_static(b"assignment"));
    }

    /// Kafka reverts the replacement when the write fails and answers with
    /// the append error under the unknown member id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_static_rejoin_append_failure_rolls_back_and_reports_error() {
        let (coord, log) = make_coordinator();
        let (group, request) = stable_static_group_and_rejoin();
        let generation = group.as_classic().unwrap().generation_id;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let response = send_join(&handle, request).await;

        check!(
            response
                == JoinResult {
                    error_code: codes::NOT_COORDINATOR,
                    generation_id: generation,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    leader: "m1".into(),
                    ..JoinResult::default()
                }
        );
        let view = rpc::classic_inspect(&handle).await;
        check!(view.state == ClassicGroupState::Stable);
        check!(view.members.len() == 1);
        check!(view.members[0].member_id == "m1");
        check!(view.members[0].protocol_metadata == Bytes::from_static(b"subscription"));
        check!(view.members[0].assignment.as_deref() == Some(&b"assignment"[..]));
        check!(log.batches().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_classic_member_joins_upgraded_group_and_gets_assignment() {
        // `Upgrade` policy: keep the group consumer-kind after the native
        // member leaves in `seed_and_upgrade` (see the note above).
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;

        // First bring m-classic fully in sync so it holds a stable assignment.
        let join_c = rpc::classic_join(&handle, "m-classic", "t").await;
        let _ = rpc::classic_sync(&handle, "m-classic", join_c.generation_id).await;

        // A brand-new classic member m2 joins the already-upgraded group.
        let join2 = rpc::classic_join(&handle, "m2", "t").await;
        assert!(join2.error_code == codes::NONE);
        assert!(join2.leader == "m2");

        // Both members re-sync at the (new) group epoch to pick up the
        // rebalanced two-way split.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .unwrap();
        let epoch = rx.await.unwrap().group_epoch;
        let sync_c = rpc::classic_sync(&handle, "m-classic", epoch).await;
        let sync2 = rpc::classic_sync(&handle, "m2", epoch).await;
        assert!(sync_c.error_code == codes::NONE);
        assert!(sync2.error_code == codes::NONE);

        // Collect each member's partitions of "t".
        let parts = |s: &SyncResult| -> Vec<i32> {
            decode_assignment(&s.assignment)
                .assigned_partitions
                .iter()
                .find(|tp| tp.topic == "t")
                .map(|tp| tp.partitions.clone())
                .unwrap_or_default()
        };
        let p_c = parts(&sync_c);
        let p_2 = parts(&sync2);
        assert!(!p_2.is_empty(), "the new member must receive an assignment");

        // Disjoint, and together cover {0, 1}.
        let set_c: std::collections::HashSet<i32> = p_c.iter().copied().collect();
        let set_2: std::collections::HashSet<i32> = p_2.iter().copied().collect();
        assert!(
            set_c.is_disjoint(&set_2),
            "the two members must hold disjoint partitions"
        );
        let mut union: Vec<i32> = set_c.union(&set_2).copied().collect();
        union.sort_unstable();
        assert!(
            union == vec![0, 1],
            "the union of partitions must be {{0, 1}}"
        );
    }

    /// Kafka's `classicGroupJoin` against a consumer group: (label, policy,
    /// native members) to (join error code, consumer group tombstoned, group
    /// classic afterwards). An empty consumer group is replaced by a classic
    /// group; a live one under policy `disabled` refuses the classic member
    /// with `INCONSISTENT_GROUP_PROTOCOL`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_join_against_a_consumer_group_follows_kafka() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, config::ConsumerGroupMigrationPolicy as Policy,
        };

        let rows = [
            (
                "empty consumer group",
                Policy::Disabled,
                0,
                (codes::MEMBER_ID_REQUIRED, true, true),
            ),
            (
                "live consumer group, policy disabled",
                Policy::Disabled,
                1,
                (codes::INCONSISTENT_GROUP_PROTOCOL, false, false),
            ),
        ];
        for (label, policy, members, want) in rows {
            let (coord, log) = make_coordinator_with_topic_policy("t", 1, policy);
            let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
            for index in 0..members {
                let joined =
                    rpc::consumer_heartbeat(&handle, &format!("native-{index}"), 0, Some("t"))
                        .await;
                assert!(joined.error_code == codes::NONE, "{label}");
            }

            let joined = rpc::classic_join(&handle, "", "t").await;

            let (tx, rx) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .unwrap();
            let is_classic = rx.await.is_ok();
            let got = (
                joined.error_code,
                log.has_next_gen_group_metadata_tombstone("g").await,
                is_classic,
            );
            check!(got == want, "{label}");
        }
    }
}
