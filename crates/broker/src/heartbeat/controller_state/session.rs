//! The heartbeat session of a broker: how it opens, how it is refreshed, and
//! how it expires.
//!
//! Recording a heartbeat, expiring a stale entry on the liveness tick, and
//! seeding or discovering an entry are the three ways a session's clock moves,
//! so they sit together and the edge transitions they emit are compared in one
//! place.

use super::{
    BrokerLivenessState, ControllerLivenessState, LivenessTransition, registry::BrokerEntry,
};

impl ControllerLivenessState {
    /// Record a heartbeat from `broker_id`. A broker that has no existing
    /// session starts fenced until the handler confirms metadata catch-up.
    pub(crate) async fn record_fenced_heartbeat(
        &self,
        broker_id: u64,
    ) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, true).await
    }

    #[cfg(test)]
    pub(crate) async fn record_heartbeat(&self, broker_id: u64) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, false).await
    }

    async fn record_heartbeat_inner(
        &self,
        broker_id: u64,
        initially_fenced: bool,
    ) -> Option<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let entry = map.entry(broker_id).or_insert(BrokerEntry {
            last_heartbeat: now,
            state: BrokerLivenessState::Alive,
            fenced: initially_fenced,
        });
        let prev = entry.state;
        entry.last_heartbeat = now;
        entry.state = BrokerLivenessState::Alive;
        if prev == BrokerLivenessState::Dead {
            tracing::info!(
                broker_id,
                "broker liveness: DEAD -> ALIVE (heartbeat resumed)"
            );
            Some(LivenessTransition::DeadToAlive(broker_id))
        } else {
            None
        }
    }

    /// Scan all registered brokers and mark those that have not sent a
    /// heartbeat within `timeout` as `Dead`. Returns the list of
    /// transitions that occurred this tick.
    pub(crate) async fn tick(&self) -> Vec<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let mut transitions = Vec::new();
        for (&id, entry) in map.iter_mut() {
            if entry.state == BrokerLivenessState::Alive
                && now.duration_since(entry.last_heartbeat) > self.timeout
            {
                entry.state = BrokerLivenessState::Dead;
                tracing::warn!(
                    broker_id = id,
                    since_last_heartbeat_ms =
                        u64::try_from(now.duration_since(entry.last_heartbeat).as_millis())
                            .unwrap_or(u64::MAX),
                    timeout_ms = u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX),
                    "broker liveness: ALIVE -> DEAD (heartbeat session timeout) — triggers partition-leader failover"
                );
                transitions.push(LivenessTransition::AliveToDead(id));
            }
        }
        transitions
    }

    /// Start a session for every broker in `broker_ids` that the registry does
    /// not know yet. Each new entry starts `Alive` with `last_heartbeat =
    /// now`, so the broker gets one full timeout window to send its first
    /// heartbeat. It also starts fenced, as a first heartbeat would leave it:
    /// a broker that has not yet proved metadata catch-up must not be elected
    /// or receive replicas, and only [`apply_fencing`](Self::apply_fencing)
    /// with `is_caught_up` lifts the fence. Known entries keep their state,
    /// their fence, and their death clock.
    ///
    /// The controller leader calls this on every liveness tick with the
    /// brokers registered in the metadata image. Without it the registry
    /// only knows brokers that heartbeated this controller or that a
    /// leadership change seeded. A broker that registers and dies before its
    /// first heartbeat reaches this controller would then never expire, and
    /// the partitions it leads would never fail over.
    pub(crate) async fn track_registered(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for id in broker_ids {
            map.entry(id).or_insert(BrokerEntry {
                last_heartbeat: now,
                state: BrokerLivenessState::Alive,
                fenced: true,
            });
        }
    }

    /// Seed the liveness registry with the given broker ids as `Alive` with
    /// `last_heartbeat = now`. The broker calls this when it becomes the raft
    /// leader. Live peers then get a full timeout window to redirect their
    /// heartbeat loop at the new controller. [`tick`](Self::tick) still
    /// detects dead peers after `timeout` ms.
    pub(crate) async fn seed_brokers(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for id in broker_ids {
            map.entry(id)
                .and_modify(|entry| {
                    entry.last_heartbeat = now;
                    entry.state = BrokerLivenessState::Alive;
                })
                .or_insert(BrokerEntry {
                    last_heartbeat: now,
                    state: BrokerLivenessState::Alive,
                    fenced: false,
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::heartbeat::controller_state::TestClock;

    #[tokio::test]
    async fn new_broker_starts_alive_after_first_heartbeat() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        let transition = liveness.record_heartbeat(1).await;
        assert!(transition == None); // first heartbeat: not a revival
        assert!(liveness.state(1).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_marks_expired_broker_dead() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(2).await;
        // Advance past the timeout deterministically (no wall-clock sleep).
        clock.advance(Duration::from_millis(11));
        let transitions = liveness.tick().await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(2)]);
        assert!(liveness.state(2).await == Some(BrokerLivenessState::Dead));
    }

    #[tokio::test]
    async fn heartbeat_after_dead_emits_revival() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(3).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await; // broker 3 → Dead
        let transition = liveness.record_heartbeat(3).await;
        assert!(transition == Some(LivenessTransition::DeadToAlive(3)));
        assert!(liveness.state(3).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn track_registered_adds_unknown_brokers_and_keeps_known_state() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        // Broker 1 is known and expires.
        liveness.record_heartbeat(1).await;
        clock.advance(Duration::from_millis(11));
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);

        // Broker 1 keeps its dead state. Broker 2 starts a fresh session that
        // is fenced until it proves catch-up: not dead, not electable, and
        // unavailable for new replicas.
        liveness.track_registered([1, 2]).await;
        assert!(liveness.dead_snapshot().await == [1].into_iter().collect());
        assert!(!liveness.is_alive(2).await);
        assert!(liveness.unavailable_snapshot().await.contains(&2));

        // A caught-up heartbeat lifts the fence and only then makes it alive.
        liveness.record_fenced_heartbeat(2).await;
        assert!(!liveness.apply_fencing(2, false, true).await);
        assert!(liveness.is_alive(2).await);

        // Broker 3 is discovered and never heartbeats. It expires one full
        // window later, while broker 2 keeps heartbeating.
        liveness.track_registered([3]).await;
        clock.advance(Duration::from_millis(11));
        liveness.record_fenced_heartbeat(2).await;
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(3)]);
        assert!(liveness.dead_snapshot().await == [1, 3].into_iter().collect());
    }

    #[tokio::test]
    async fn track_registered_does_not_refresh_a_stale_session() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(5).await;
        clock.advance(Duration::from_millis(9));

        // Unlike `seed_brokers`, a track call must not extend the window.
        liveness.track_registered([5]).await;
        clock.advance(Duration::from_millis(2));

        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(5)]);
    }

    #[tokio::test]
    async fn seed_moves_dead_broker_out_of_dead_snapshot() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(4).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await;
        assert!(liveness.dead_snapshot().await.contains(&4));

        liveness.seed_brokers([4]).await;

        assert!(liveness.dead_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn normal_seed_gives_brokers_full_timeout_window() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(50), clock.clock());
        liveness.seed_brokers([7]).await;
        // Well within the 50ms window — deterministically still alive.
        clock.advance(Duration::from_millis(1));

        let transitions = liveness.tick().await;

        assert!(transitions.is_empty());
        assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn normal_seed_refreshes_existing_entries() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(7).await;
        // Let the original heartbeat go stale relative to the 10ms window...
        clock.advance(Duration::from_millis(20));

        // ...a normal re-seed must REFRESH the existing entry to a full window,
        liveness.seed_brokers([7]).await;
        // so 1ms later it is nowhere near expiry. Were the refresh missing, the
        // entry would be ~21ms stale here and `tick` would mark it dead — which
        // is exactly the regression this test guards.
        clock.advance(Duration::from_millis(1));
        let transitions = liveness.tick().await;

        assert!(transitions.is_empty());
        assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_does_not_expire_recently_heartbeated_broker() {
        let liveness = ControllerLivenessState::new(krabka_units::minutes(1));
        liveness.record_heartbeat(4).await;
        let transitions = liveness.tick().await;
        assert!(
            transitions.is_empty(),
            "broker 4 should not expire with 60s timeout"
        );
        assert!(liveness.state(4).await == Some(BrokerLivenessState::Alive));
    }
}
