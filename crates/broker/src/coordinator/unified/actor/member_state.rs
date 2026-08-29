//! Per-member bookkeeping for the next-gen protocol: building a member from a
//! heartbeat, applying steady-state updates to one, choosing the assignor, and
//! driving the reconciler when the group is dirty.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_protocol::{
    owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, primitives::uuid::Uuid,
};

use super::{FALLBACK_REBALANCE_TIMEOUT_MS, MetadataProvider};
use crate::coordinator::unified::{
    ClientIdentity,
    assignor::Assignor,
    config::NextGenConfig,
    consumer_state::{GroupState, MemberState},
    persistence_next_gen::MemberAssignmentState,
    reconciler,
};

/// The partitions a member reports that it owns in its heartbeat. An absent
/// `topic_partitions` means "unchanged". The caller then substitutes the
/// member's current assignment, so that a keepalive can still take newly freed
/// partitions.
pub(super) fn reported_owned(req: &ConsumerGroupHeartbeatRequest) -> HashMap<Uuid, Vec<i32>> {
    req.topic_partitions
        .as_ref()
        .map(|tp| {
            tp.iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Applies steady-state member updates and runs reconciliation. It returns
/// `true` when a change happened that needs a log write.
pub(super) fn update_member_state(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
    cur_epoch: i32,
) -> bool {
    let mut member_metadata_changed = false;
    let mut became_dirty = false;
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
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                became_dirty = true;
                member_metadata_changed = true;
            }
        }
        // KIP-848 v1+: `subscribed_topic_regex` may change independently
        // of `subscribed_topic_names`. Only mark dirty when it actually
        // changes; the client re-sends the same regex on every
        // heartbeat as long as the subscription is stable.
        if req.subscribed_topic_regex != m.subscribed_topic_regex {
            // Recompile the cached regex only here — the one place the
            // pattern actually changes (the client re-sends the same regex
            // every heartbeat while the subscription is stable).
            m.set_regex(req.subscribed_topic_regex.clone());
            state.dirty = true;
        }
    }
    if became_dirty {
        state.dirty = true;
    }
    let was_dirty = state.dirty;
    run_reconcile(state, config, metadata);
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    // Reconcile this member's current assignment against the (possibly new)
    // target and what it reports owning: grant free target partitions, mark
    // revocations, and withhold partitions still held by another member. A
    // heartbeat without `topic_partitions` is a keepalive — reuse the member's
    // current assignment as its owned set so it can still pick up freed partitions.
    let owned = if req.topic_partitions.is_some() {
        reported_owned(req)
    } else {
        state
            .members
            .get(&req.member_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default()
    };
    let assignment_changed = state.reconcile_member(&req.member_id, &owned);
    member_metadata_changed || was_dirty || epoch_advanced || assignment_changed
}

pub(super) fn run_reconcile(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
) {
    // `metadata.snapshot()` rebuilds HashMaps over every cluster topic /
    // partition — far too expensive to run on a steady-state no-op
    // heartbeat. `reconcile_if_dirty` early-returns when `!dirty`, so gate
    // the snapshot on the same condition: only pay for it when we will
    // actually recompute. Behavior when dirty is identical to before.
    if !state.dirty {
        return;
    }
    let input = metadata.snapshot();
    let assignor = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &*assignor);
}

fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> Arc<dyn Assignor> {
    for m in state.members.values() {
        if let Some(name) = m.server_assignor.as_deref()
            && let Some(a) = config.find_assignor(name)
        {
            return a;
        }
    }
    config
        .assignors
        .first()
        .cloned()
        .expect("NextGenConfig must have at least one registered assignor")
}

pub(super) fn build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> MemberState {
    let subs: std::collections::HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    MemberState {
        member_id: member_id.into(),
        instance_id: req.instance_id.clone(),
        rack_id: req.rack_id.clone(),
        client_id: client.id.into(),
        client_host: client.host.into(),
        subscribed_topic_names: subs,
        subscribed_topic_regex: req.subscribed_topic_regex.clone(),
        compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
        server_assignor: req.server_assignor.clone(),
        rebalance_timeout: Duration::from_millis(
            u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
        ),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        last_seen: now,
        classic: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        GroupCoordinator,
        actor::{
            GroupActorMessage,
            heartbeat::step_heartbeat,
            test_support::{StaticMetadata, empty_metadata},
        },
        assignor::{Assignment, MemberSubscription, TopicMetadata},
        offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
    };

    #[test]
    fn subscription_change_persists_every_reconciled_assignment() {
        let config = NextGenConfig::default();
        let first_topic = Uuid([10; 16]);
        let second_topic = Uuid([11; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [
                    ("first".into(), first_topic),
                    ("second".into(), second_topic),
                ]
                .into(),
                partitions_per_topic: [(first_topic, 2), (second_topic, 2)].into(),
                ..Default::default()
            },
        };
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            state.add_or_update_member(build_member(
                member_id,
                &ConsumerGroupHeartbeatRequest {
                    subscribed_topic_names: Some(vec!["first".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                crate::coordinator::unified::ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            ));
        }
        run_reconcile(&mut state, &config, &metadata);
        state.advance_member_epoch("m1");
        state.advance_member_epoch("m2");
        let member_epoch = state.group_epoch;

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m2".into(),
                member_epoch,
                subscribed_topic_names: Some(vec!["second".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        );

        let mut target_ids: Vec<&str> = step
            .pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        let mut current_ids: Vec<&str> = step
            .pending
            .current_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        target_ids.sort_unstable();
        current_ids.sort_unstable();

        check!(step.pending.target_metadata.is_some());
        check!(target_ids == vec!["m1", "m2"]);
        assert!(current_ids == vec!["m1", "m2"]);
    }

    #[derive(Debug)]
    struct CountingAssignor {
        calls: Arc<AtomicUsize>,
    }
    impl Assignor for CountingAssignor {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn assign(&self, _members: &[MemberSubscription], _topics: &TopicMetadata) -> Assignment {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::collections::HashMap::new()
        }
    }

    #[test]
    fn pick_assignor_skips_unregistered_member_preference() {
        let config = NextGenConfig::default();
        let mut state = crate::coordinator::unified::consumer_state::GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        m.server_assignor = Some("ghost".into());
        state.members.insert("m1".into(), m);

        let picked = pick_assignor(&state, &config);
        assert!(picked.name() == "uniform");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_assignor_invoked_when_requested() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut config = NextGenConfig::default();
        config
            .register_assignor(Arc::new(CountingAssignor {
                calls: calls.clone(),
            }))
            .unwrap();

        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(GroupCoordinator::new(
            config,
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            empty_metadata(),
            log,
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_consumer("g");

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    server_assignor: Some("counting".into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_id: "client-a".into(),
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert!(resp.error_code == 0);
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "custom assignor must be invoked at least once",
        );
    }
}
