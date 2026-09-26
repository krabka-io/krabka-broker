//! Per-partition replica progress tracking, lives on the partition leader.
//!
//! `ReplicaState` records each follower's last-fetched offset, which is the
//! follower's persisted LEO from the leader's perspective, and it caches the
//! high watermark that Kafka's `Partition.maybeIncrementLeaderHW` would hold:
//! frozen while the ISR is below `min.insync.replicas`, otherwise the lowest
//! log end among the leader, the ISR and every caught-up ISR-eligible replica,
//! and never lower than before. ISR-lag tracking in `FollowerStats`
//! (`last_fetch`, `last_caught_up`) follows Kafka's
//! `Replica.updateFetchStateOrThrow` and lets the `isr_maintenance` task
//! shrink and expand the ISR.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_ids::LeaderEpoch;
use krabka_log::Offset;
use krabka_raft::NodeId;
use krabka_verified::isr::{
    CaughtUpCredit, HighWatermarkFacts, HwmReplica, IsrEligibilityFacts, leader_high_watermark,
};

/// What the leader knows of one follower's progress, Kafka's `ReplicaState`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FollowerStats {
    /// The follower's fetch offset, clamped to the leader's log end offset.
    pub(crate) leo: Offset,
    /// Kafka's `lastFetchTimeMs`: when the leader received the follower's
    /// last Fetch. `None` until the follower fetches from this leader.
    pub(crate) last_fetch: Option<Instant>,
    /// Kafka's `lastFetchLeaderLogEndOffset`: the highest leader log end
    /// offset seen at any of the follower's fetches, -1 before the first.
    pub(crate) last_fetch_leader_leo: Offset,
    /// Kafka's `lastCaughtUpTimeMs`: the latest time at which the follower's
    /// fetch offset was known to reach the leader's log end offset. `None`
    /// means never, Kafka's 0.
    pub(crate) last_caught_up: Option<Instant>,
    /// Kafka's `ReplicaState.brokerEpoch`: the broker epoch the follower's
    /// last Fetch carried (KIP-841), -1 for a Fetch that carried none, `None`
    /// until one is recorded.
    pub(crate) broker_epoch: Option<i64>,
}

impl FollowerStats {
    /// A follower this leader has heard nothing from, Kafka's
    /// `ReplicaState.EMPTY`.
    const UNKNOWN: Self = Self {
        leo: Offset(0),
        last_fetch: None,
        last_fetch_leader_leo: Offset(-1),
        last_caught_up: None,
        broker_epoch: None,
    };

    /// An ISR member when this broker installs the ISR: Kafka's
    /// `Replica.resetReplicaState` for a follower in sync, caught up as of
    /// `now` but not yet fetched from this leader.
    fn in_sync_at(now: Instant) -> Self {
        Self {
            last_caught_up: Some(now),
            ..Self::UNKNOWN
        }
    }

    /// Kafka's `ReplicaState.logEndOffset`: the follower's last fetch offset,
    /// or -1, Kafka's `UNKNOWN_OFFSET`, before it fetched from this leader.
    pub(crate) fn log_end(&self) -> i64 {
        if self.last_fetch.is_some() {
            self.leo.0
        } else {
            -1
        }
    }

    /// Kafka's `Replica.updateFetchStateOrThrow` for a fetch at `fetch_offset`
    /// received at `now`, while the leader's log ended at `leader_leo`.
    fn record_fetch(&mut self, fetch_offset: Offset, leader_leo: Offset, now: Instant) {
        let credited = match krabka_verified::isr::follower_caught_up_credit(
            fetch_offset.0,
            leader_leo.0,
            self.last_fetch_leader_leo.0,
        ) {
            CaughtUpCredit::ThisFetch => Some(now),
            CaughtUpCredit::PreviousFetch => self.last_fetch,
            CaughtUpCredit::Unchanged => None,
        };
        self.last_caught_up = self.last_caught_up.max(credited);
        self.leo = fetch_offset.min(leader_leo);
        self.last_fetch_leader_leo = self.last_fetch_leader_leo.max(leader_leo);
        self.last_fetch = Some(now);
    }
}

/// One replica's standing in the leader's metadata image, the metadata-cache
/// half of Kafka's `Partition.isReplicaIsrEligible`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrokerStanding {
    /// The controller has published the broker as fenced.
    pub(crate) fenced: bool,
    /// Kafka's `metadataCache.getAliveBrokerEpoch`: the registered broker
    /// epoch of a registered, unfenced broker.
    pub(crate) alive_epoch: Option<i64>,
}

/// What the leader's high-watermark and ISR rules read from outside the
/// partition. The ISR maintenance scan refreshes it from the metadata image
/// on every pass over a partition this broker leads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LeaderPolicy {
    /// Kafka's `Partition.effectiveMinIsr`: `min.insync.replicas` capped by
    /// the replica count.
    pub(crate) effective_min_isr: usize,
    /// `replica.lag.time.max.ms`.
    pub(crate) replica_lag_time_max: Duration,
    /// The image's standing of every assigned replica. A replica with no entry
    /// is not ISR-eligible.
    pub(crate) brokers: HashMap<NodeId, BrokerStanding>,
}

impl LeaderPolicy {
    /// The policy before the first scan: Kafka's default `min.insync.replicas`
    /// of 1, and no replica known to be eligible, so only the ISR holds the
    /// high watermark back.
    fn unscanned() -> Self {
        Self {
            effective_min_isr: 1,
            replica_lag_time_max: Duration::ZERO,
            brokers: HashMap::new(),
        }
    }

    /// The policy `image` gives partition `record`.
    ///
    /// `min.insync.replicas` resolves through
    /// [`effective_min_insync_replicas`](crate::config_keys::effective_min_insync_replicas),
    /// the threshold the controller maintains the KIP-966 eligible-leader set
    /// against, so the high watermark freezes exactly when the controller
    /// starts to name eligible leaders. The image carries no controlled
    /// shutdown state; the controller's `AlterPartition` check refuses a
    /// broker in controlled shutdown as `INELIGIBLE_REPLICA`.
    pub(crate) fn from_image(
        image: &krabka_metadata::MetadataImage,
        record: &krabka_metadata::PartitionRecord,
        replica_lag_time_max: Duration,
    ) -> Self {
        let brokers = record
            .replicas
            .iter()
            .map(|&replica| {
                let fenced = crate::config_keys::resolve_broker_fenced(image, replica);
                let alive_epoch = image.broker_epoch(replica).filter(|_| !fenced);
                (
                    replica,
                    BrokerStanding {
                        fenced,
                        alive_epoch,
                    },
                )
            })
            .collect();
        Self {
            effective_min_isr: crate::config_keys::effective_min_insync_replicas(
                image,
                &record.topic,
                record.replicas.len(),
            ),
            replica_lag_time_max,
            brokers,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    replicas: HashSet<NodeId>,
    pub(crate) per_follower: HashMap<NodeId, FollowerStats>,
    /// KIP-107: the log start offset each follower reported in its last
    /// Fetch. A replica with no entry has not fetched since this broker
    /// became its leader, which Kafka's `ReplicaState` holds as
    /// `UNKNOWN_OFFSET` (-1).
    follower_log_start: HashMap<NodeId, Offset>,
    pub(crate) hw: Offset,
    pub(crate) current_leader_epoch: LeaderEpoch,
    leader: Option<NodeId>,
    policy: LeaderPolicy,
}

impl ReplicaState {
    pub(crate) fn new() -> Self {
        Self {
            isr: HashSet::new(),
            replicas: HashSet::new(),
            per_follower: HashMap::new(),
            follower_log_start: HashMap::new(),
            hw: Offset(0),
            current_leader_epoch: LeaderEpoch(0),
            leader: None,
            policy: LeaderPolicy::unscanned(),
        }
    }

    /// Install (or reinstall) the ISR membership and seed non-leader
    /// `per_follower` entries to zero. The call is idempotent, so a
    /// re-install of the same `(isr, replicas, leader)` keeps existing
    /// follower progress.
    ///
    /// `isr` is the committed in-sync set. `replicas` is the full replica
    /// assignment. `per_follower` is keyed by the **replica set** without
    /// the leader, and not by the ISR. A replica that the ISR shrank out,
    /// or that has not yet rejoined after a restart, still catches up
    /// through follower-fetch, and `isr_maintenance` reads exactly its
    /// fetch-driven log end to expand it back in. A key on the
    /// ISR instead would discard that progress on every
    /// metadata-image reconcile and starve ISR re-admission under image
    /// churn. This method drops only nodes that are no longer in the
    /// replica set, for example after a reassignment removed them.
    ///
    /// A newly seeded ISR member is caught up as of `now` and has not
    /// fetched, as Kafka's `Replica.resetReplicaState` leaves a follower in
    /// sync: it stays in the ISR for `replica.lag.time.max.ms` to prove
    /// itself.
    pub(crate) fn install_isr(
        &mut self,
        isr: &[NodeId],
        replicas: &[NodeId],
        leader: NodeId,
        now: Instant,
    ) {
        self.leader = Some(leader);
        self.isr = isr.iter().copied().collect();
        self.replicas = replicas.iter().copied().collect();
        self.per_follower.remove(&leader);
        // Seed only ISR members, as Kafka's `Replica.resetReplicaState` does:
        // seeding a non-ISR replica with `last_caught_up = now` would count
        // it as caught up and let it hold the high watermark back before it
        // fetched anything.
        for &r in isr {
            if r != leader {
                self.per_follower
                    .entry(r)
                    .or_insert_with(|| FollowerStats::in_sync_at(now));
            }
        }
        self.per_follower.retain(|k, _| self.replicas.contains(k));
        self.follower_log_start
            .retain(|k, _| self.replicas.contains(k) && *k != leader);
    }

    pub(crate) fn reset_for_leader(&mut self, leader: NodeId) {
        self.leader = Some(leader);
        self.replicas.insert(leader);
        self.per_follower.clear();
        self.follower_log_start.clear();
    }

    /// Record the log start offset a follower's Fetch reported, as Kafka's
    /// `Replica.updateFetchStateOrThrow` does.
    pub(crate) fn record_follower_log_start(&mut self, follower: NodeId, log_start: Offset) {
        if self.leader != Some(follower) {
            self.follower_log_start.insert(follower, log_start);
        }
    }

    /// Record the broker epoch a follower's Fetch carried in
    /// `ReplicaState.ReplicaEpoch`, -1 when it carried none, as Kafka's
    /// `Replica.updateFetchStateOrThrow` does. The fetch path records it
    /// before [`Self::update_follower_leo`], whose watermark step reads it.
    pub(crate) fn record_follower_broker_epoch(&mut self, follower: NodeId, broker_epoch: i64) {
        if self.leader != Some(follower) {
            self.per_follower
                .entry(follower)
                .or_insert(FollowerStats::UNKNOWN)
                .broker_epoch = Some(broker_epoch);
        }
    }

    /// Replace what the high-watermark and ISR rules read from the metadata
    /// image and the broker's configuration.
    pub(crate) fn set_policy(&mut self, policy: LeaderPolicy) {
        self.policy = policy;
    }

    /// Kafka's `Partition.isReplicaIsrEligible` facts for `replica`.
    pub(crate) fn eligibility(&self, replica: NodeId) -> IsrEligibilityFacts {
        let standing = self.policy.brokers.get(&replica);
        IsrEligibilityFacts {
            fenced: standing.is_some_and(|standing| standing.fenced),
            // The metadata image carries no controlled-shutdown state; see
            // `LeaderPolicy::from_image`.
            shutting_down: false,
            fetch_broker_epoch: self
                .per_follower
                .get(&replica)
                .and_then(|stats| stats.broker_epoch),
            alive_broker_epoch: standing.and_then(|standing| standing.alive_epoch),
        }
    }

    /// Whether `at` lies within `replica.lag.time.max.ms` of `now`.
    pub(crate) fn within_lag(&self, at: Option<Instant>, now: Instant) -> bool {
        at.is_some_and(|at| now.saturating_duration_since(at) <= self.policy.replica_lag_time_max)
    }

    /// Kafka's `Partition.lowWatermarkIfLeader`: the lowest log start offset
    /// of the leader and of every other replica whose broker is alive.
    ///
    /// `replicas` is the partition's assignment as the metadata image holds
    /// it, and `leader` is this broker. The caller passes the assignment
    /// rather than the replica set installed here, because a broker that just
    /// became leader publishes its leadership before it installs that set.
    /// `alive` holds the brokers that are registered and not fenced. A replica
    /// that has not fetched since this broker became leader counts as -1, so
    /// a live replica that never reported holds the low watermark down.
    pub(crate) fn low_watermark(
        &self,
        leader: NodeId,
        leader_log_start: Offset,
        replicas: &[NodeId],
        alive: &HashSet<u64>,
    ) -> Offset {
        replicas
            .iter()
            .filter(|replica| **replica != leader && alive.contains(&replica.0))
            .map(|replica| {
                self.follower_log_start
                    .get(replica)
                    .copied()
                    .unwrap_or(Offset(-1))
            })
            .fold(leader_log_start, std::cmp::min)
    }

    pub(crate) fn leader_and_replicas(&self) -> (Option<NodeId>, &HashSet<NodeId>) {
        (self.leader, &self.replicas)
    }

    /// Whether `follower`'s reported range can serve `fetch_offset`, as
    /// Kafka's `ReplicaManager.findPreferredReadReplica` checks it: the
    /// follower's log end offset must be at least the fetch offset, and its
    /// log start at most the fetch offset. KIP-392 never redirects a consumer
    /// to a replica that would have to answer it `OFFSET_OUT_OF_RANGE`.
    ///
    /// A follower that has not fetched since this broker became leader has no
    /// `per_follower` entry, so its LEO reads 0 -- excluding it from any
    /// `fetch_offset` above 0, the same as Kafka's freshly reset `Replica` --
    /// and no `follower_log_start` entry, which reads `UNKNOWN_OFFSET` (-1)
    /// and never excludes it on the low end.
    pub(crate) fn follower_can_serve(&self, follower: NodeId, fetch_offset: Offset) -> bool {
        let leo = self
            .per_follower
            .get(&follower)
            .map_or(Offset(0), |stats| stats.leo);
        let log_start = self
            .follower_log_start
            .get(&follower)
            .copied()
            .unwrap_or(Offset(-1));
        leo >= fetch_offset && log_start <= fetch_offset
    }

    /// Record one follower Fetch at `follower_leo` (its fetch offset),
    /// received at `now` while the leader's log ended at `leader_leo`, and
    /// recompute the high watermark.
    ///
    /// ISR members and replicas outside the ISR are tracked alike: the
    /// former so `isr_maintenance` can shrink them out when they stop
    /// catching up, the latter so it can expand them back in. A replica with
    /// no entry starts from Kafka's empty state, never caught up.
    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: Offset,
        leader_leo: Offset,
        now: Instant,
    ) -> Offset {
        self.per_follower
            .entry(follower)
            .or_insert(FollowerStats::UNKNOWN)
            .record_fetch(follower_leo, leader_leo, now);
        self.recompute_hw_at(leader_leo, now)
    }

    /// Recompute the high watermark after the leader's log end moved to
    /// `leader_leo`, as of now.
    pub(crate) fn recompute_hw_for_leader_append(&mut self, leader_leo: Offset) -> Offset {
        self.recompute_hw_at(leader_leo, Instant::now())
    }

    /// Recompute the high watermark with the leader's log ending at
    /// `leader_leo`, reading every lag bound at `now`.
    pub(crate) fn recompute_hw_at(&mut self, leader_leo: Offset, now: Instant) -> Offset {
        self.hw = self.compute_hw(leader_leo, now);
        self.hw
    }

    /// Advance the high watermark after the WAL has made records durable up to
    /// `durable_leo`.
    ///
    /// The WAL quorum replaces the partition ISR as the durability authority
    /// for diskless records. Applying [`Self::compute_hw`] here would combine
    /// two independent quorums and could pin the client-visible watermark to a
    /// Kafka follower's stale LEO after the WAL quorum had already committed
    /// the records. The watermark remains monotonic within this runtime.
    pub(crate) fn recompute_hw_for_wal_durable(&mut self, durable_leo: Offset) -> Offset {
        self.hw = self.hw.max(durable_leo);
        self.hw
    }

    /// Kafka's `partitionState.isr.size`. Kafka's ISR always holds its leader;
    /// a state that has installed no ISR yet is this broker leading alone, so
    /// the leader counts once whether or not an installed ISR names it.
    fn isr_size(&self) -> usize {
        let leader_listed = self.leader.is_some_and(|leader| self.isr.contains(&leader));
        self.isr.len() + usize::from(!leader_listed)
    }

    /// Kafka's `Partition.maybeIncrementLeaderHW` over every remote replica
    /// and ISR member. The committed ISR stands in for Kafka's maximal ISR: an
    /// expansion this leader proposed is not tracked until it commits, and the
    /// replica it adds is caught up and eligible, so it holds the watermark
    /// back either way.
    fn compute_hw(&self, leader_leo: Offset, now: Instant) -> Offset {
        let remotes = self
            .replicas
            .union(&self.isr)
            .filter(|replica| self.leader != Some(**replica))
            .map(|replica| {
                let stats = self.per_follower.get(replica);
                HwmReplica {
                    log_end: stats.map_or(-1, FollowerStats::log_end),
                    in_isr: self.isr.contains(replica),
                    caught_up_within_lag: self
                        .within_lag(stats.and_then(|stats| stats.last_caught_up), now),
                    eligibility: self.eligibility(*replica),
                }
            })
            .collect::<Vec<_>>();
        Offset(leader_high_watermark(
            HighWatermarkFacts {
                // Kafka's `UnifiedLog.truncateTo` lowers the high watermark
                // with the log end; krabka's truncation leaves it here.
                current: self.hw.0.min(leader_leo.0),
                leader_log_end: leader_leo.0,
                isr_size: self.isr_size(),
                effective_min_isr: self.policy.effective_min_isr,
            },
            &remotes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::{assert, check};

    use super::*;

    /// Shorthand for wrapping a raw offset in the test asserts below.
    fn o(v: i64) -> Offset {
        Offset(v)
    }

    fn fresh() -> ReplicaState {
        ReplicaState::new()
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn new_state_has_zero_hw_and_empty_membership() {
        let s = fresh();
        let expected = ReplicaState {
            isr: HashSet::new(),
            replicas: HashSet::new(),
            per_follower: HashMap::new(),
            follower_log_start: HashMap::new(),
            hw: Offset(0),
            current_leader_epoch: LeaderEpoch(0),
            leader: None,
            policy: LeaderPolicy::unscanned(),
        };
        assert!(s == expected);
    }

    #[test]
    fn leader_and_replicas_reports_installed_state() {
        let mut s = fresh();
        let empty: HashSet<NodeId> = HashSet::new();
        assert!(s.leader_and_replicas() == (None, &empty));

        let replicas = [NodeId(1), NodeId(2), NodeId(3)];
        s.install_isr(&replicas, &replicas, NodeId(1), now());
        let expected_replicas: HashSet<NodeId> = replicas.into_iter().collect();
        assert!(s.leader_and_replicas() == (Some(NodeId(1)), &expected_replicas));

        s.reset_for_leader(NodeId(2));
        assert!(s.leader_and_replicas() == (Some(NodeId(2)), &expected_replicas));
    }

    #[test]
    fn install_isr_seeds_non_leader_followers_at_zero() {
        let mut s = fresh();
        let t = now();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            t,
        );
        let seeded = FollowerStats {
            leo: Offset(0),
            last_fetch: None,
            last_fetch_leader_leo: Offset(-1),
            last_caught_up: Some(t),
            broker_epoch: None,
        };
        // Only the non-leader followers (2 and 3) are seeded; the leader (1)
        // gets no per_follower entry.
        let expected = ReplicaState {
            isr: [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
            replicas: [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
            per_follower: [(NodeId(2), seeded), (NodeId(3), seeded)]
                .into_iter()
                .collect(),
            follower_log_start: HashMap::new(),
            hw: Offset(0),
            current_leader_epoch: LeaderEpoch(0),
            leader: Some(NodeId(1)),
            policy: LeaderPolicy::unscanned(),
        };
        assert!(s == expected);
    }

    #[test]
    fn install_isr_idempotent_preserves_follower_progress() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(2), o(50), o(100), now());
        s.update_follower_leo(NodeId(3), o(75), o(100), now());
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        assert!(s.per_follower.get(&NodeId(2)).map(|f| f.leo) == Some(o(50)));
        assert!(s.per_follower.get(&NodeId(3)).map(|f| f.leo) == Some(o(75)));
    }

    #[test]
    fn install_isr_drops_stale_follower_leo_for_removed_replicas() {
        // Node 3 leaves the *replica set* entirely (e.g. reassignment) →
        // its progress entry is dropped.
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(3), o(75), o(100), now());
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            now(),
        );
        assert!(!s.per_follower.contains_key(&NodeId(3)));
    }

    #[test]
    fn install_isr_keeps_catching_up_replica_shrunk_from_isr() {
        // Node 3 is shrunk out of the ISR but stays a replica (it's
        // catching back up). Its fetch-driven progress must survive an
        // ISR reinstall so isr_maintenance can later expand it back in.
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(3), o(75), o(100), now());
        // Committed ISR shrinks to {1,2}; replica set is still {1,2,3}.
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        assert!(
            s.per_follower.contains_key(&NodeId(3)),
            "a replica catching up toward ISR re-admission must keep its progress"
        );
        assert!(s.per_follower.get(&NodeId(3)).map(|f| f.leo) == Some(o(75)));
    }

    #[test]
    fn hw_advances_when_trailing_follower_catches_up() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        // Ordered steps over shared state: (follower, follower_leo, expected_hw).
        let steps = [(2, 50, 0), (3, 75, 50), (2, 80, 75)];
        for (follower, leo, expected_hw) in steps {
            let hw = s.update_follower_leo(NodeId(follower), o(leo), o(100), now());
            assert!(hw == o(expected_hw), "step: follower {follower} leo {leo}");
        }
    }

    #[test]
    fn hw_pins_at_slowest_isr_follower() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(2), o(100), o(100), now());
        s.update_follower_leo(NodeId(3), o(30), o(100), now());
        assert!(s.hw == o(30));
    }

    #[test]
    fn non_isr_follower_leo_update_uses_leader_path() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            now(),
        );
        // Node 3 is not in ISR. Its progress is tracked for possible
        // re-admission, but it is excluded from HW; follower 2 has not
        // fetched from this leader, so HW stays where it was.
        let hw = s.update_follower_leo(NodeId(3), o(999), o(100), now());
        assert!(hw == o(0));
        assert!(s.hw == o(0));
    }

    #[test]
    fn single_replica_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[NodeId(1)], &[NodeId(1)], NodeId(1), now());
        let hw = s.recompute_hw_for_leader_append(o(42));
        assert!(hw == o(42));
    }

    #[test]
    fn wal_durable_advances_hw_to_durable_offset_for_singleton_isr() {
        let mut s = fresh();
        s.install_isr(&[NodeId(1)], &[NodeId(1)], NodeId(1), now());
        let hw = s.recompute_hw_for_wal_durable(o(5));
        assert!(hw == o(5));
    }

    #[test]
    fn wal_durable_hw_is_independent_of_partition_isr_progress() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );

        let hw = s.recompute_hw_for_wal_durable(o(5));

        assert!(hw == o(5));
        assert!(s.per_follower.get(&NodeId(2)).map(|f| f.leo) == Some(o(0)));
        assert!(s.per_follower.get(&NodeId(3)).map(|f| f.leo) == Some(o(0)));
    }

    #[test]
    fn wal_durable_hw_does_not_regress() {
        let mut s = fresh();
        assert!(s.recompute_hw_for_wal_durable(o(8)) == o(8));

        assert!(s.recompute_hw_for_wal_durable(o(5)) == o(8));
    }

    #[test]
    fn follower_overshoot_clamps_to_leader_leo() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            now(),
        );
        let hw = s.update_follower_leo(NodeId(2), o(200), o(100), now());
        assert!(hw == o(100));
        assert!(s.per_follower.get(&NodeId(2)).map(|f| f.leo) == Some(o(100)));
    }

    #[test]
    fn empty_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        let hw = s.recompute_hw_for_leader_append(o(50));
        assert!(hw == o(50));
    }

    #[test]
    fn missing_isr_replica_progress_pins_high_watermark() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(2), o(30), o(100), now());
        s.per_follower.remove(&NodeId(3));

        assert!(s.recompute_hw_for_leader_append(o(100)) == o(0));
    }

    /// A new leadership neither lowers the watermark it inherited nor raises
    /// it before the ISR fetches: Kafka's `Replica.resetReplicaState` leaves
    /// each follower's log end unknown, and `maybeIncrementHighWatermark`
    /// only ever raises it.
    #[test]
    fn leadership_change_gap_pins_high_watermark() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            now(),
        );
        s.update_follower_leo(NodeId(2), o(30), o(100), now());

        s.reset_for_leader(NodeId(2));

        assert!(s.recompute_hw_for_leader_append(o(100)) == o(30));
    }

    /// Kafka's `Replica.updateFetchStateOrThrow`, fetch by fetch, for
    /// follower 2 of an ISR installed at `t0`. Each step is a fetch at
    /// `t0 + ms`: the follower's fetch offset, the leader's log end offset
    /// when it arrived, and the whole follower state it leaves.
    #[test]
    fn follower_fetches_update_state_as_kafka_does() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let stats = |leo, last_fetch, last_fetch_leader_leo, last_caught_up| FollowerStats {
            leo: o(leo),
            last_fetch: Some(at(last_fetch)),
            last_fetch_leader_leo: o(last_fetch_leader_leo),
            last_caught_up,
            broker_epoch: None,
        };
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            t0,
        );
        let steps = [
            (
                "the first fetch cannot credit a fetch this leader never saw",
                100,
                5,
                10,
                stats(5, 100, 10, Some(t0)),
            ),
            (
                "a fetch short of the previous fetch's log end credits nothing",
                200,
                9,
                20,
                stats(9, 200, 20, Some(t0)),
            ),
            (
                "a fetch reaching the previous fetch's log end credits that fetch",
                300,
                20,
                30,
                stats(20, 300, 30, Some(at(200))),
            ),
            (
                "a fetch reaching the current log end credits itself",
                400,
                30,
                30,
                stats(30, 400, 30, Some(at(400))),
            ),
            (
                "a later short fetch keeps the latest caught-up time",
                500,
                25,
                40,
                stats(25, 500, 40, Some(at(400))),
            ),
            (
                "an overshooting fetch clamps to the leader's log end",
                600,
                99,
                40,
                stats(40, 600, 40, Some(at(600))),
            ),
        ];
        for (label, ms, fetch_offset, leader_leo, expected) in steps {
            s.update_follower_leo(NodeId(2), o(fetch_offset), o(leader_leo), at(ms));
            check!(s.per_follower.get(&NodeId(2)) == Some(&expected), "{label}");
        }
    }

    /// A replica outside the ISR that this leader has not heard from starts
    /// from Kafka's empty state: its first fetch proves nothing unless it
    /// reaches the leader's log end, so it cannot be re-admitted on progress
    /// it never made.
    #[test]
    fn a_replica_outside_the_isr_is_never_caught_up_until_it_reaches_the_log_end() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2), NodeId(3)],
            NodeId(1),
            t0,
        );

        s.update_follower_leo(NodeId(3), o(0), o(10), at(100));
        check!(
            s.per_follower.get(&NodeId(3))
                == Some(&FollowerStats {
                    leo: o(0),
                    last_fetch: Some(at(100)),
                    last_fetch_leader_leo: o(10),
                    last_caught_up: None,
                    broker_epoch: None,
                })
        );

        s.update_follower_leo(NodeId(3), o(10), o(10), at(200));
        check!(
            s.per_follower.get(&NodeId(3))
                == Some(&FollowerStats {
                    leo: o(10),
                    last_fetch: Some(at(200)),
                    last_fetch_leader_leo: o(10),
                    last_caught_up: Some(at(200)),
                    broker_epoch: None,
                })
        );
    }

    /// Kafka's `Partition.lowWatermarkIfLeader` over a leader at log start
    /// 50 and followers 2 and 3.
    #[test]
    fn low_watermark_is_the_lowest_live_replica_log_start() {
        let alive_all: HashSet<u64> = [1, 2, 3].into_iter().collect();
        let only_2: HashSet<u64> = [1, 2].into_iter().collect();
        for (label, reported, alive, expected) in [
            ("no follower reported yet", vec![], &alive_all, -1),
            ("one follower reported", vec![(2, 50)], &alive_all, -1),
            (
                "both followers caught up",
                vec![(2, 50), (3, 60)],
                &alive_all,
                50,
            ),
            ("a follower behind", vec![(2, 40), (3, 60)], &alive_all, 40),
            ("a dead follower does not count", vec![(2, 50)], &only_2, 50),
            (
                "the leader's own start is the ceiling",
                vec![(2, 70), (3, 80)],
                &alive_all,
                50,
            ),
        ] {
            let mut s = fresh();
            s.install_isr(
                &[NodeId(1), NodeId(2), NodeId(3)],
                &[NodeId(1), NodeId(2), NodeId(3)],
                NodeId(1),
                now(),
            );
            for (follower, log_start) in reported {
                s.record_follower_log_start(NodeId(follower), o(log_start));
            }
            assert2::check!(
                s.low_watermark(NodeId(1), o(50), &[NodeId(1), NodeId(2), NodeId(3)], alive)
                    == o(expected),
                "{label}"
            );
        }
    }

    /// A new leadership forgets what the followers reported to the old one,
    /// and the low watermark reads the assignment it is given, not the replica
    /// set installed here: a broker that just became leader and has not
    /// installed its replica set yet still waits for every assigned follower.
    #[test]
    fn follower_log_starts_do_not_outlive_the_leadership() {
        let alive: HashSet<u64> = [1, 2, 3].into_iter().collect();
        let assignment = [NodeId(1), NodeId(2), NodeId(3)];
        let mut s = fresh();
        s.install_isr(&assignment, &assignment, NodeId(1), now());
        s.record_follower_log_start(NodeId(2), o(50));
        s.record_follower_log_start(NodeId(3), o(50));
        assert2::check!(s.low_watermark(NodeId(1), o(50), &assignment, &alive) == o(50));

        s.reset_for_leader(NodeId(1));
        assert2::check!(s.low_watermark(NodeId(1), o(50), &assignment, &alive) == o(-1));
    }
    /// Replicas 1, 2 and 3 led by 1, each registered and unfenced at broker
    /// epoch 7, with a 1 s `replica.lag.time.max.ms`.
    fn policy(effective_min_isr: usize) -> LeaderPolicy {
        LeaderPolicy {
            effective_min_isr,
            replica_lag_time_max: Duration::from_secs(1),
            brokers: [1, 2, 3]
                .into_iter()
                .map(|node| {
                    (
                        NodeId(node),
                        BrokerStanding {
                            fenced: false,
                            alive_epoch: Some(7),
                        },
                    )
                })
                .collect(),
        }
    }

    /// Kafka's `Partition.maybeIncrementLeaderHW` returns early while
    /// `isUnderMinIsr`: with `min.insync.replicas` 2, a leader left alone in
    /// its ISR keeps appending but its high watermark stays put until the ISR
    /// is back at two, even while a follower outside it has every record.
    #[test]
    fn the_high_watermark_freezes_while_the_isr_is_under_min_isr() {
        let t0 = Instant::now();
        let replicas = [NodeId(1), NodeId(2), NodeId(3)];
        let mut s = fresh();
        s.install_isr(&replicas, &replicas, NodeId(1), t0);
        s.set_policy(policy(2));
        for follower in [NodeId(2), NodeId(3)] {
            s.record_follower_broker_epoch(follower, 7);
            s.update_follower_leo(follower, o(10), o(10), t0);
        }
        check!(s.hw == o(10), "the full ISR commits the first ten records");

        // Both followers stop fetching; two seconds later the controller has
        // shrunk the ISR to the leader alone.
        let later = t0 + Duration::from_secs(2);
        s.install_isr(&[NodeId(1)], &replicas, NodeId(1), later);
        check!(
            s.recompute_hw_at(o(20), later) == o(10),
            "an append under min ISR does not move the watermark"
        );
        check!(
            s.update_follower_leo(NodeId(2), o(20), o(20), later) == o(10),
            "nor does a caught-up follower outside the ISR"
        );

        s.install_isr(&[NodeId(1), NodeId(2)], &replicas, NodeId(1), later);
        check!(
            s.recompute_hw_at(o(20), later) == o(20),
            "the ISR back at min ISR releases the watermark"
        );
    }

    /// Kafka's `shouldWaitForReplicaToJoinIsr`: a follower outside the ISR
    /// that is caught up and ISR-eligible holds the watermark back, so the
    /// watermark does not run away from a follower about to rejoin. The ISR
    /// is {1, 2}; follower 3 caught up at offset 10 and the leader has since
    /// appended to 12, which follower 2 already has.
    #[test]
    fn a_caught_up_eligible_follower_outside_the_isr_holds_the_watermark_back() {
        let t0 = Instant::now();
        let replicas = [NodeId(1), NodeId(2), NodeId(3)];
        for (label, fetch_epoch, standing, expected) in [
            ("an eligible follower holds it", Some(7), policy(1), 10),
            (
                "a follower whose fetch carried no epoch holds it",
                Some(-1),
                policy(1),
                10,
            ),
            (
                "a follower whose fetch carried a stale epoch does not",
                Some(6),
                policy(1),
                12,
            ),
            (
                "a follower with no recorded fetch epoch does not",
                None,
                policy(1),
                12,
            ),
            (
                "a fenced follower does not",
                Some(7),
                LeaderPolicy {
                    brokers: [(
                        NodeId(3),
                        BrokerStanding {
                            fenced: true,
                            alive_epoch: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    ..policy(1)
                },
                12,
            ),
        ] {
            let mut s = fresh();
            s.install_isr(&[NodeId(1), NodeId(2)], &replicas, NodeId(1), t0);
            s.set_policy(standing);
            if let Some(epoch) = fetch_epoch {
                s.record_follower_broker_epoch(NodeId(3), epoch);
            }
            s.update_follower_leo(NodeId(3), o(10), o(10), t0);
            let hw = s.update_follower_leo(NodeId(2), o(12), o(12), t0);
            check!(hw == o(expected), "{label}");
        }
    }

    /// A leader's own broker epoch is never recorded as a follower's.
    #[test]
    fn a_fetch_epoch_is_recorded_for_followers_only() {
        let mut s = fresh();
        s.install_isr(
            &[NodeId(1), NodeId(2)],
            &[NodeId(1), NodeId(2)],
            NodeId(1),
            now(),
        );
        s.set_policy(policy(1));
        s.record_follower_broker_epoch(NodeId(1), 7);
        s.record_follower_broker_epoch(NodeId(2), 7);
        check!(!s.per_follower.contains_key(&NodeId(1)));
        check!(
            s.eligibility(NodeId(2))
                == IsrEligibilityFacts {
                    fenced: false,
                    shutting_down: false,
                    fetch_broker_epoch: Some(7),
                    alive_broker_epoch: Some(7),
                }
        );
    }

    /// The policy the ISR scan reads out of the metadata image: the topic's
    /// `min.insync.replicas` capped by the replica count, and each replica's
    /// fencing and registered epoch.
    #[test]
    fn a_policy_reads_min_isr_and_broker_standing_from_the_image() {
        use krabka_metadata::{
            BrokerConfigRecord, BrokerRegistrationRecord, MetadataImage, MetadataRecord,
            PartitionRecord, TopicConfigRecord, TopicRecord,
        };
        let register = |node: u64, broker_epoch: i64| {
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(node),
                broker_epoch,
                incarnation_id: uuid::Uuid::nil(),
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            })
        };
        let record = |replicas: &[u64]| PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: replicas.iter().copied().map(NodeId).collect(),
            ..Default::default()
        };
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [(
                crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                "2".to_string(),
            )]
            .into_iter()
            .collect(),
        }));
        image.apply(&register(1, 11));
        image.apply(&register(2, 12));
        image.apply(&register(3, 13));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(3),
            config_name: crate::config_keys::BROKER_FENCED.to_string(),
            config_value: Some(crate::config_keys::FENCED_TRUE.to_string()),
        }));
        let standing = |fenced, alive_epoch| BrokerStanding {
            fenced,
            alive_epoch,
        };
        let lag = Duration::from_secs(30);
        check!(
            LeaderPolicy::from_image(&image, &record(&[1, 2, 3, 4]), lag)
                == LeaderPolicy {
                    effective_min_isr: 2,
                    replica_lag_time_max: lag,
                    brokers: [
                        (NodeId(1), standing(false, Some(11))),
                        (NodeId(2), standing(false, Some(12))),
                        (NodeId(3), standing(true, None)),
                        (NodeId(4), standing(false, None)),
                    ]
                    .into_iter()
                    .collect(),
                }
        );
        check!(
            LeaderPolicy::from_image(&image, &record(&[1]), lag).effective_min_isr == 1,
            "a single replica caps min ISR at one"
        );
    }
}

#[cfg(test)]
#[path = "replica_state_model.rs"]
mod replica_state_model;
