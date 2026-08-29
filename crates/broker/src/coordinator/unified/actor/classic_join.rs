//! The classic `JoinGroup` path.
//!
//! A native classic group runs the 5-state machine: the request either answers
//! at once, parks until the rebalance boundary, or completes the round. An
//! upgraded consumer group serves the same RPC for a hosted classic member by
//! upserting it into the next-gen state and reconciling, so both flavours of
//! `JoinGroup` live together here.

use std::time::Duration;

use bytes::Bytes;
use krabka_protocol::owned::join_group_request::JoinGroupRequest;
use tokio::sync::oneshot;

use super::{
    ActorServices, FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS, JoinResult,
    MetadataProvider, ParkedWaiters, chrono_now_ms,
    member_state::run_reconcile,
    persistence::{flush_classic_metadata, flush_pending, snapshot_pending_after_change},
    waiters::complete_classic_rebalance,
};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator, classic_ops, config::NextGenConfig, group::CoordinatorGroup, migration,
        offsets_log::OffsetsLog,
    },
};

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
    if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        match classic_ops::handle_join(
            state,
            &mut request,
            client_id,
            client_host,
            version >= 4,
            services.config.classic_initial_rebalance_delay,
        ) {
            classic_ops::JoinAction::Immediate(result) => {
                if result.error_code == codes::NONE
                    && let Err(error) = flush_classic_metadata(state, services.offsets_log).await
                {
                    *state = previous;
                    tracing::warn!(group_id = %state.group_id, %error,
                        "classic static rejoin log write failed");
                    let _ = reply.send(JoinResult {
                        error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                        member_id: request.member_id,
                        protocol_type: state.protocol_type.clone(),
                        protocol_name: state.protocol_name.clone(),
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
                make_coordinator, make_coordinator_with_topic_policy, rpc, seed_and_upgrade,
            },
        },
        classic_state::GroupState as ClassicGroupState,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_stable_static_rejoin_persists_refreshed_member() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let member = state.members.get_mut("m1").unwrap();
        member.group_instance_id = Some("instance-1".into());
        member.assignment = Some(Bytes::from_static(b"assignment"));
        state
            .static_members
            .insert("instance-1".into(), "m1".into());
        state.state = ClassicGroupState::Stable;
        let generation = state.generation_id;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    member_id: "m1".into(),
                    group_instance_id: Some("instance-1".into()),
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        metadata: Bytes::from_static(b"new-subscription"),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version: 4,
                client_id: "new-client".into(),
                client_host: "new-host".into(),
                reply: tx,
            })
            .await
            .unwrap();

        let response = rx.await.unwrap();
        check!(response.error_code == codes::NONE);
        check!(response.generation_id == generation);
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == generation);
        check!(persisted.members.len() == 1);
        check!(persisted.members[0].client_id == "new-client");
        check!(persisted.members[0].client_host == "new-host");
        check!(persisted.members[0].subscription == Bytes::from_static(b"new-subscription"));
        check!(persisted.members[0].assignment == Bytes::from_static(b"assignment"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_static_rejoin_append_failure_rolls_back_and_reports_error() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let member = state.members.get_mut("m1").unwrap();
        member.group_instance_id = Some("instance-1".into());
        member.assignment = Some(Bytes::from_static(b"assignment"));
        state
            .static_members
            .insert("instance-1".into(), "m1".into());
        state.state = ClassicGroupState::Stable;
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    member_id: "m1".into(),
                    group_instance_id: Some("instance-1".into()),
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        metadata: Bytes::from_static(b"new-subscription"),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version: 4,
                client_id: "new-client".into(),
                client_host: "new-host".into(),
                reply: tx,
            })
            .await
            .unwrap();

        let response = rx.await.unwrap();
        check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        check!(response.member_id == "m1");
        check!(response.protocol_type.as_deref() == Some("consumer"));
        check!(response.protocol_name.as_deref() == Some("range"));
        let view = rpc::classic_inspect(&handle).await;
        check!(view.state == ClassicGroupState::Stable);
        check!(view.members.len() == 1);
        check!(view.members[0].client_id == "client");
        check!(view.members[0].host == "host");
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
}
