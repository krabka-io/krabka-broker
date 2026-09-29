//! The periodic session-expiry tick.
//!
//! One kind-agnostic timer drives both protocols: it expires classic members
//! past their session timeout and completes any rebalance their departure
//! unblocks, and it evicts next-gen members and writes their tombstones.

use std::time::Instant;

use super::{
    ActorServices, MetadataProvider, ParkedWaiters, chrono_now_ms,
    downgrade::maybe_downgrade,
    member_state::run_reconcile,
    persistence::{flush_classic_metadata, flush_pending, snapshot_pending_after_change},
    regex_resolution::delete_unsubscribed_regexes,
    waiters::{drain_followers_with, drain_removed_classic_waiters, maybe_complete_classic},
};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator,
        classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
        config::NextGenConfig,
        consumer_state::GroupState,
        group::CoordinatorGroup,
        offsets_log::OffsetsLog,
    },
};

pub(super) async fn handle_actor_tick(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    let group_id = group.group_id.clone();
    if let Some(state) = group.as_consumer_mut() {
        if handle_session_tick(
            state,
            services.config,
            services.metadata,
            services.offsets_log,
            services.coordinator,
        )
        .await
        .is_err()
        {
            return false;
        }
        if let Err(error) = maybe_downgrade(
            group,
            services.config,
            services.metadata,
            services.offsets_log,
            services.coordinator,
        )
        .await
        {
            tracing::warn!(%group_id, %error,
                "next-gen actor exiting after tick downgrade log-write failure");
            return false;
        }
    } else if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let now = Instant::now();
        let expired_pending = state.expire_pending_members(now);
        let dropped =
            state.expire_dead_members(now, services.config.classic_initial_rebalance_delay);
        if !dropped.is_empty() || !expired_pending.is_empty() {
            tracing::info!(group = %group_id, ?dropped, ?expired_pending,
                "expired members; waking joiners");
            return settle_classic_removal(state, previous, &dropped, parked, services).await;
        }
    }
    true
}

/// Runs when a classic group's pending-sync timer fires: Kafka's
/// `expirePendingSync`. Every member that still owes a `SyncGroup` is removed
/// and the group prepares a rebalance for the rest. It returns the actor's
/// keep-running flag.
pub(super) async fn handle_classic_sync_expiry(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    let Some(state) = group.as_classic_mut() else {
        return true;
    };
    let previous = state.clone();
    let removed = state.expire_pending_sync(
        services.config.classic_initial_rebalance_delay,
        Instant::now(),
    );
    if removed.is_empty() {
        return true;
    }
    tracing::info!(group = %state.group_id, ?removed,
        "removed members that never sent SyncGroup; preparing a rebalance");
    let keep_running = settle_classic_removal(state, previous, &removed, parked, services).await;
    // A failed write restores the group with its timer already due. Retry on
    // the session-expiry cadence instead of spinning on the log.
    if let Some(state) = group.as_classic_mut()
        && state.sync_deadline.is_some_and(|due| due <= Instant::now())
    {
        state.sync_deadline = Some(Instant::now() + services.config.session_expiry_tick);
    }
    keep_running
}

/// What a classic group owes after its timers removed `removed` members, in
/// the order of Kafka's `removeMemberAndUpdateClassicGroup`. An emptied group
/// persists its next generation first. The removed members' parked calls get
/// `UNKNOWN_MEMBER_ID`, the followers parked in `SyncGroup` of a group that left
/// `CompletingRebalance` get `REBALANCE_IN_PROGRESS` at once, as
/// `prepareRebalance` answers them, and a join phase the survivors have all
/// rejoined completes. `previous` is the group before the removal, which a
/// failed write restores. It returns the actor's keep-running flag.
async fn settle_classic_removal(
    state: &mut ClassicState,
    previous: ClassicState,
    removed: &[String],
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
) -> bool {
    if state.members.is_empty() && !removed.is_empty() {
        let Some(generation_id) = crate::metadata_epoch::next_i32(state.generation_id) else {
            *state = previous;
            tracing::warn!(group = %state.group_id,
                "classic expiration stopped because the generation is exhausted");
            return false;
        };
        state.generation_id = generation_id;
        if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
            *state = previous;
            tracing::warn!(group = %state.group_id, %error,
                "classic expiration log write failed; retrying on the next tick");
            return true;
        }
    }
    drain_removed_classic_waiters(removed, &mut parked.joiners, &mut parked.followers);
    if previous.state == ClassicGroupState::CompletingRebalance
        && state.state == ClassicGroupState::PreparingRebalance
    {
        drain_followers_with(&mut parked.followers, codes::REBALANCE_IN_PROGRESS);
    }
    maybe_complete_classic(state, &mut parked.joiners, &mut parked.followers);
    true
}

/// Runs on every heartbeat-interval tick. It evicts expired members and writes
/// the resulting tombstones to `__consumer_offsets`. It returns `Err` when the
/// log write fails, and the actor must then exit.
async fn handle_session_tick(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let now = Instant::now();
    let mut evicted = state.evict_expired(now, config.session_timeout);
    // KIP-848: a member that did not revoke its partitions within its
    // rebalance timeout is fenced like a member whose session expired.
    evicted.extend(state.fence_rebalance_timeouts(now));
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` → `remove_member` already set `dirty`. Let the
    // reconciler own the single `bump_epoch` (via `reconcile_if_dirty`); an
    // explicit pre-bump here would double-advance `group_epoch` per eviction.
    let regex_records = delete_unsubscribed_regexes(state);
    run_reconcile(state, config, metadata);
    let mut pending = snapshot_pending_after_change(state, &[], true);
    pending.resolved_regexes = regex_records;
    for mid in &evicted {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    let now_ms = chrono_now_ms();
    if let Err(e) = flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        tracing::warn!(
            group_id = %state.group_id,
            error = %e,
            "next-gen actor exiting after tick log-write failure",
        );
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            GroupActorMessage, GroupKindTag, RegexResolution,
            member_state::build_member,
            test_support::{
                completing_classic_group, empty_metadata, last_classic_metadata, make_coordinator,
            },
        },
        classic_state::GroupState as ClassicGroupState,
        offsets_log::fake::InMemoryOffsetsLog,
    };

    #[tokio::test]
    async fn classic_last_member_expiration_persists_empty_generation() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        state.state = ClassicGroupState::Stable;
        state.members.get_mut("m1").unwrap().session_timeout = Duration::ZERO;
        let prior_generation = state.generation_id;
        let mut parked = ParkedWaiters::default();
        let services = ActorServices {
            config: &coord.config,
            metadata: coord.metadata.as_ref(),
            offsets_log: log.as_ref(),
            coordinator: &coord,
        };

        check!(handle_actor_tick(&mut group, &mut parked, services).await);
        let state = group.as_classic().unwrap();
        check!(state.state == ClassicGroupState::Empty);
        check!(state.generation_id == prior_generation + 1);
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == prior_generation + 1);
        check!(persisted.members.is_empty());
    }

    /// Regression for the epoch double-bump: a single session-timeout eviction
    /// must advance `group_epoch` by exactly 1. `handle_session_tick` has no
    /// explicit `state.bump_epoch()`, so the reconciler (`reconcile_if_dirty`)
    /// is the only place that raises the epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_eviction_advances_epoch_by_one() {
        use crate::coordinator::unified::consumer_state::GroupState;

        let (coord, log) = make_coordinator();
        // Tiny session timeout so a member whose `last_seen` is a few ms in
        // the past counts as expired — avoids subtracting a large duration
        // from `Instant::now()`, which `checked_sub` rejects on low-uptime
        // CI runners (e.g. a freshly-booted Windows agent).
        let config = NextGenConfig {
            session_timeout: Duration::from_millis(1),
            ..NextGenConfig::assigning_at_once()
        };
        let metadata = empty_metadata();

        // Seed a member and reconcile once so the join settles into a clean
        // (non-dirty) baseline epoch.
        let mut state = GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        // Force the member to look session-expired. 50ms is always within
        // `Instant`'s range (no underflow on any host) yet far exceeds the
        // 1ms `session_timeout` set above.
        m.last_seen = Instant::now()
            .checked_sub(Duration::from_millis(50))
            .expect("50ms is always within Instant range");
        state.add_or_update_member(m);
        run_reconcile(&mut state, &config, &*metadata);
        assert!(!state.dirty, "baseline must be clean before eviction");
        let epoch_before = state.group_epoch;

        // One eviction tick.
        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        assert!(
            state.members.is_empty(),
            "expired member must have been evicted"
        );
        assert!(
            state.group_epoch == epoch_before + 1,
            "a single eviction must advance the group epoch by exactly 1"
        );
    }

    /// KIP-848: Kafka fences a member that does not revoke its partitions
    /// within its rebalance timeout (`scheduleConsumerGroupRebalanceTimeout`),
    /// even when it keeps heartbeating. The fence frees the partitions for
    /// their new owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_member_that_misses_its_rebalance_timeout_is_fenced() {
        use std::collections::HashMap;

        use krabka_protocol::{
            owned::consumer_group_heartbeat_request::TopicPartitions, primitives::uuid::Uuid,
        };

        use crate::coordinator::unified::{
            actor::{step_heartbeat, test_support::StaticMetadata},
            reconciler::ReconcileInput,
        };

        struct Row {
            name: &'static str,
            rebalance_timeout_ms: i32,
            /// The second heartbeat of m1 revokes the partition it lost.
            revokes: bool,
            m1_present_after: bool,
            m2_partitions_after: usize,
        }
        let rows = [
            Row {
                name: "heartbeats but never revokes",
                rebalance_timeout_ms: 100,
                revokes: false,
                m1_present_after: false,
                m2_partitions_after: 2,
            },
            Row {
                name: "revokes before the timeout",
                rebalance_timeout_ms: 100,
                revokes: true,
                m1_present_after: true,
                m2_partitions_after: 1,
            },
            Row {
                name: "never revokes, timeout not reached",
                rebalance_timeout_ms: 600_000,
                revokes: false,
                m1_present_after: true,
                m2_partitions_after: 0,
            },
        ];

        let topic = Uuid([42; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: HashMap::from([("t".to_string(), topic)]),
                partitions_per_topic: HashMap::from([(topic, 2)]),
                ..Default::default()
            },
        };
        let client = crate::coordinator::unified::ClientIdentity { id: "c", host: "h" };
        let owned = |partitions: Vec<i32>| {
            Some(vec![TopicPartitions {
                topic_id: topic,
                partitions,
                ..Default::default()
            }])
        };

        for row in rows {
            let (coord, log) = make_coordinator();
            let config = NextGenConfig::assigning_at_once();
            let mut state = GroupState::new("g");
            // Every heartbeat happened a second ago, so a 100 ms timeout armed
            // by them has fired when the tick runs, and a session has not.
            let earlier = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
            let heartbeat =
                |state: &mut GroupState, member_id: &str, partitions: Option<Vec<i32>>| {
                    let member_epoch = state.members.get(member_id).map_or(0, |m| m.member_epoch);
                    step_heartbeat(
                        state,
                        &config,
                        &metadata,
                        &ConsumerGroupHeartbeatRequest {
                            group_id: "g".into(),
                            member_id: member_id.into(),
                            member_epoch,
                            subscribed_topic_names: Some(vec!["t".into()]),
                            rebalance_timeout_ms: row.rebalance_timeout_ms,
                            topic_partitions: partitions.and_then(owned),
                            ..Default::default()
                        },
                        client,
                        earlier,
                        &RegexResolution::none(),
                    )
                    .response
                };

            heartbeat(&mut state, "m1", Some(vec![]));
            heartbeat(&mut state, "m1", Some(vec![0, 1]));
            heartbeat(&mut state, "m2", Some(vec![]));
            // m1 learns that it must give up one partition, and does not.
            heartbeat(&mut state, "m1", Some(vec![0, 1]));
            let kept: Vec<i32> = state.members["m1"]
                .assigned_partitions
                .get(&topic)
                .cloned()
                .unwrap_or_default();
            check!(kept.len() == 1, "{}", row.name);
            let second = if row.revokes { kept } else { vec![0, 1] };
            heartbeat(&mut state, "m1", Some(second));

            handle_session_tick(&mut state, &config, &metadata, &*log, &coord)
                .await
                .expect("tick");

            check!(
                state.members.contains_key("m1") == row.m1_present_after,
                "{}",
                row.name
            );
            let response = heartbeat(&mut state, "m2", None);
            let m2_partitions: usize = response
                .assignment
                .map(|a| {
                    a.topic_partitions
                        .iter()
                        .map(|tp| tp.partitions.len())
                        .sum()
                })
                .unwrap_or_default();
            check!(m2_partitions == row.m2_partitions_after, "{}", row.name);
        }
    }

    /// The actor wakes at a member's rebalance deadline instead of waiting
    /// for the session tick, and a heartbeat's `rebalance_timeout_ms` replaces
    /// the stored timeout before the deadline is armed. The session tick here
    /// never fires, so only the deadline wake can fence the member.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_actor_fences_at_the_rebalance_deadline_between_session_ticks() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;
        use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

        use crate::coordinator::unified::actor::{GroupActorMessage, test_support::StaticMetadata};

        let topic = krabka_protocol::primitives::uuid::Uuid([42; 16]);
        let clock = ManualMonotonicClock::new_shared();
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                timer: clock.new_timer(),
                session_expiry_tick: Duration::from_hours(1),
                ..NextGenConfig::assigning_at_once()
            },
            crate::coordinator::unified::share::config::ShareGroupConfig::assigning_at_once(),
            Arc::new(StaticMetadata {
                input: crate::coordinator::unified::reconciler::ReconcileInput {
                    topic_id_by_name: [("t".to_string(), topic)].into(),
                    partitions_per_topic: [(topic, 2)].into(),
                    ..Default::default()
                },
            }),
            Arc::new(InMemoryOffsetsLog::default()),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));
        let handle = coord.get_or_create_consumer("g");
        let heartbeat = |member_id: &'static str,
                         member_epoch: i32,
                         rebalance_timeout_ms: i32,
                         owned: Option<Vec<i32>>| {
            let handle = Arc::clone(&handle);
            async move {
                let (reply, response) = tokio::sync::oneshot::channel();
                handle
                    .tx
                    .send(GroupActorMessage::Heartbeat {
                        request: ConsumerGroupHeartbeatRequest {
                            group_id: "g".into(),
                            member_id: member_id.into(),
                            member_epoch,
                            subscribed_topic_names: Some(vec!["t".into()]),
                            rebalance_timeout_ms,
                            topic_partitions: owned.map(|partitions| {
                                vec![TopicPartitions {
                                    topic_id: topic,
                                    partitions,
                                    ..Default::default()
                                }]
                            }),
                            ..Default::default()
                        },
                        client_id: "c".into(),
                        client_host: "h".into(),
                        reply,
                        regex_resolver:
                            crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
                    })
                    .await
                    .unwrap();
                response.await.unwrap()
            }
        };

        // m1 joins with a long timeout and owns both partitions.
        let joined = heartbeat("m1", 0, 600_000, Some(vec![])).await;
        let owned = heartbeat("m1", joined.member_epoch, -1, Some(vec![0, 1])).await;
        heartbeat("m2", 0, 600_000, Some(vec![])).await;
        // m1 shortens its timeout to 50 ms and keeps both partitions.
        let kept = heartbeat("m1", owned.member_epoch, 50, Some(vec![0, 1])).await;
        check!(kept.error_code == crate::codes::NONE);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (reply, view) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::Describe { reply })
                .await
                .unwrap();
            let members: Vec<String> = view
                .await
                .unwrap()
                .members
                .into_iter()
                .map(|member| member.member_id)
                .collect();
            if members == vec!["m2".to_string()] {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "m1 was not fenced at its rebalance deadline: {members:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// KIP-848 live migration: the tick must dispatch on the LIVE
    /// `group.kind`, not on the captured spawn-time kind. This test spawns a
    /// classic actor, flips it to a consumer group in place, and fires a tick.
    /// The actor must keep running rather than panic on a kind-mismatched
    /// `expect(...)`.
    ///
    /// An injected manual timer drives the session-expiry tick, so the tick
    /// fires on a controlled timeline instead of a real 1.2 s wall-clock
    /// sleep. The test is therefore deterministic and instant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_tick_does_not_panic_after_in_place_flip() {
        use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

        let clock = ManualMonotonicClock::new_shared();
        let log = Arc::new(InMemoryOffsetsLog::default());
        let tick_interval = Duration::from_millis(37);
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                timer: clock.new_timer(),
                session_expiry_tick: tick_interval,
                ..NextGenConfig::assigning_at_once()
            },
            crate::coordinator::unified::share::config::ShareGroupConfig::assigning_at_once(),
            empty_metadata(),
            log.clone(),
            crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        ));

        let handle = coord.get_or_create_group("g", GroupKindTag::Classic);

        // Flip the group to consumer in place, then round-trip a synchronous
        // inspect. The mpsc is FIFO with a single consumer, so the inspect reply
        // proves the flip message was already processed before we fire a tick.
        handle
            .tx
            .send(GroupActorMessage::TestForceConsumerKind)
            .await
            .unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::InspectAny { reply: tx })
            .await
            .unwrap();
        let _ = rx.await;

        // The actor is now parked on the re-armed session-expiry tick sleep.
        // The manual clock has a single kind of waiter, so a count of one is
        // that tick registration and nothing else. Confirm the waiter is
        // registered (only advance the timeline once parked), fire exactly one
        // tick, then confirm the loop re-parks — which proves the tick body ran
        // to completion on the LIVE consumer kind without panicking.
        // `wait_for_waiters` blocks, so it runs on a blocking thread and never
        // stalls the runtime driving the actor. Its five-second real-time bound
        // turns a lost tick into a failure rather than a hung test.
        let waiting = Arc::clone(&clock);
        let parked = tokio::task::spawn_blocking(move || {
            waiting.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(parked, "actor should park on the session-expiry tick sleep");

        clock
            .advance(tick_interval)
            .expect("manual time moves forward");

        let waiting = Arc::clone(&clock);
        let reparked = tokio::task::spawn_blocking(move || {
            waiting.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(reparked, "actor should re-park after processing the tick");
        assert!(!handle.tx.is_closed());
    }

    /// Kafka's `removeMemberAndUpdateClassicGroup` reaches `prepareRebalance`,
    /// which answers every member waiting in `SyncGroup` with
    /// `REBALANCE_IN_PROGRESS` at once and gives the survivors the whole group
    /// rebalance timeout to rejoin. The tick used to leave the follower parked
    /// and to wake the survivors after the initial rebalance delay instead.
    #[tokio::test]
    async fn leader_expiry_in_completing_rebalance_releases_followers_and_keeps_survivors() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1", "m2"]);
        let state = group.as_classic_mut().unwrap();
        state.members.get_mut("m1").unwrap().session_timeout = Duration::ZERO;
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut parked = ParkedWaiters::default();
        parked.followers.insert("m2".into(), tx);
        let services = ActorServices {
            config: &coord.config,
            metadata: coord.metadata.as_ref(),
            offsets_log: log.as_ref(),
            coordinator: &coord,
        };
        let before = Instant::now();

        check!(handle_actor_tick(&mut group, &mut parked, services).await);

        let state = group.as_classic().unwrap();
        check!(
            rx.try_recv().expect("the follower is answered by the tick")
                == crate::coordinator::unified::actor::SyncResult {
                    error_code: codes::REBALANCE_IN_PROGRESS,
                    ..Default::default()
                }
        );
        check!(state.state == ClassicGroupState::PreparingRebalance);
        check!(state.members.keys().collect::<Vec<_>>() == vec!["m2"]);
        // The survivor's 60 s rebalance timeout, not the 3 s initial delay.
        check!(
            state
                .rebalance_deadline
                .is_some_and(|deadline| deadline >= before + Duration::from_mins(1))
        );
    }

    /// Kafka's `removePendingMemberAndUpdateClassicGroup`: a `MEMBER_ID_REQUIRED`
    /// id that times out completes a join phase that it alone was holding up.
    #[tokio::test]
    async fn pending_member_expiry_completes_the_join_phase() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let generation = state.generation_id;
        state.state = ClassicGroupState::PreparingRebalance;
        state.mark_awaiting_join("m1");
        state.rebalance_deadline = Some(Instant::now() + Duration::from_hours(1));
        state.add_pending_member(
            "pending".into(),
            Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
        );
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut parked = ParkedWaiters::default();
        parked.joiners.insert("m1".into(), tx);
        let services = ActorServices {
            config: &coord.config,
            metadata: coord.metadata.as_ref(),
            offsets_log: log.as_ref(),
            coordinator: &coord,
        };

        check!(handle_actor_tick(&mut group, &mut parked, services).await);

        let joined = rx.try_recv().expect("the join phase completed");
        check!(joined.error_code == codes::NONE);
        check!(joined.generation_id == generation + 1);
        let state = group.as_classic().unwrap();
        check!(state.state == ClassicGroupState::CompletingRebalance);
        check!(state.pending_members.is_empty());
    }

    /// Kafka's `expirePendingSync`: a leader that never sent `SyncGroup` is
    /// removed when the timer fires, and the followers waiting for it are
    /// answered `REBALANCE_IN_PROGRESS` as `prepareRebalance` does.
    #[tokio::test]
    async fn pending_sync_expiry_removes_the_silent_leader_and_releases_followers() {
        let (coord, log) = make_coordinator();
        let mut group = completing_classic_group(&["m1", "m2"]);
        let state = group.as_classic_mut().unwrap();
        state.arm_pending_sync(Instant::now());
        // The follower synced and waits for the leader.
        state.remove_pending_sync_member("m2");
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        let mut parked = ParkedWaiters::default();
        parked.followers.insert("m2".into(), tx);
        let services = ActorServices {
            config: &coord.config,
            metadata: coord.metadata.as_ref(),
            offsets_log: log.as_ref(),
            coordinator: &coord,
        };

        check!(handle_classic_sync_expiry(&mut group, &mut parked, services).await);

        check!(
            rx.try_recv()
                .expect("the follower is answered at once")
                .error_code
                == codes::REBALANCE_IN_PROGRESS
        );
        let state = group.as_classic().unwrap();
        check!(state.state == ClassicGroupState::PreparingRebalance);
        check!(state.members.keys().collect::<Vec<_>>() == vec!["m2"]);
        check!(state.sync_deadline.is_none());
    }

    /// The whole actor: a generation whose leader never syncs is torn down by
    /// the pending-sync timer alone, although the leader's session is fine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_removes_a_member_that_never_syncs_after_the_rebalance_timeout() {
        use crate::coordinator::unified::actor::test_support::rpc;

        let (coord, _log) = make_coordinator();
        let mut group = completing_classic_group(&["m1", "m2"]);
        let state = group.as_classic_mut().unwrap();
        state.arm_pending_sync(Instant::now());
        state.remove_pending_sync_member("m2");
        state.sync_deadline = Some(Instant::now() + Duration::from_millis(50));
        coord.seed_classic("g", Box::new(group));
        let handle = coord.find("g").unwrap();

        let view = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let view = rpc::classic_inspect(&handle).await;
                if view.state != ClassicGroupState::CompletingRebalance {
                    break view;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the pending-sync timer must fire");

        check!(view.state == ClassicGroupState::PreparingRebalance);
        check!(view.members.len() == 1);
        check!(view.members[0].member_id == "m2");
    }
}
