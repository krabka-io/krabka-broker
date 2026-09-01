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
    waiters::{drain_removed_classic_waiters, maybe_complete_classic},
};
use crate::coordinator::unified::{
    GroupCoordinator, config::NextGenConfig, consumer_state::GroupState, group::CoordinatorGroup,
    offsets_log::OffsetsLog,
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
        let dropped = state.expire_dead_members(
            Instant::now(),
            services.config.classic_initial_rebalance_delay,
        );
        if !dropped.is_empty() {
            if state.members.is_empty() {
                let Some(generation_id) = crate::metadata_epoch::next_i32(state.generation_id)
                else {
                    *state = previous;
                    tracing::warn!(group = %group_id,
                        "classic expiration stopped because the generation is exhausted");
                    return false;
                };
                state.generation_id = generation_id;
                if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                    *state = previous;
                    tracing::warn!(group = %group_id, %error,
                        "classic expiration log write failed; retrying on the next tick");
                    return true;
                }
            }
            tracing::info!(group = %group_id, ?dropped, "expired members; waking joiners");
            drain_removed_classic_waiters(&dropped, &mut parked.joiners, &mut parked.followers);
            maybe_complete_classic(state, &mut parked.joiners, &mut parked.followers);
        }
    }
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
    let evicted = state.evict_expired(Instant::now(), config.session_timeout);
    if evicted.is_empty() {
        return Ok(());
    }
    // `evict_expired` → `remove_member` already set `dirty`. Let the
    // reconciler own the single `bump_epoch` (via `reconcile_if_dirty`); an
    // explicit pre-bump here would double-advance `group_epoch` per eviction.
    run_reconcile(state, config, metadata);
    let mut pending = snapshot_pending_after_change(state, &[], true);
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
            GroupActorMessage, GroupKindTag,
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
            ..NextGenConfig::default()
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
                ..NextGenConfig::default()
            },
            crate::coordinator::unified::share::config::ShareGroupConfig::default(),
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
}
