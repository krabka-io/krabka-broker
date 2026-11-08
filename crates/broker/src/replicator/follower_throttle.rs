//! KIP-73 follower-side fetch throttling.
//!
//! The module decides whether a partition may join a fetch round at all, by
//! reading the topic's `follower.replication.throttled.replicas` list and
//! asking the broker-wide follower-in quota whether it is exceeded, as Kafka's
//! `RemoteLeaderEndPoint.shouldFollowerThrottle` asks
//! `ReplicationQuotaManager.isQuotaExceeded`. A partition that joins asks for
//! the whole `replica.fetch.max.bytes`, and [`record_replicated`] records what
//! arrives (`ReplicaFetcherThread.processPartitionData` records every
//! throttled partition). A batch larger than the bound thus arrives whole, and
//! the rate it raises keeps the partition out of the rounds after it until the
//! measured rate, averaged over the quota's samples, falls back to the bound.
//! The quota forgets a sample once it leaves the 11 s monitored window, so no
//! burst keeps a partition out for longer than that.
//!
//! A follower that has caught up is never held back: Kafka's
//! `shouldFollowerThrottle` throttles a replica only while it is not in sync,
//! and a replica is in sync when its lag is zero
//! (`PartitionFetchState.isReplicaInSync`), to avoid ISR thrashing. What such a
//! follower fetches still counts against the quota.

use {
    super::Config,
    crate::throttle::TopicThrottle,
    std::sync::atomic::{
        AtomicI64, AtomicU64,
        Ordering::{Acquire, Relaxed, Release},
    },
};

/// How far a followed partition trails the leader's high watermark, as the
/// `lag` of Kafka's `PartitionFetchState`.
///
/// The lag is unknown until a response has been applied, and an unknown lag is
/// not in sync. An empty response leaves a known lag as it was, because Kafka
/// recomputes it only when the response carried bytes.
#[derive(Debug, krabka_macros::FieldDefaults)]
pub struct ReplicaLag {
    /// The lag in offsets, never below zero, or [`Self::UNKNOWN`].
    #[default(AtomicI64::new(Self::UNKNOWN))]
    lag: AtomicI64,
    /// The bytes appended since the lag was last computed.
    #[default(AtomicU64::new(0))]
    appended: AtomicU64,
}

impl ReplicaLag {
    const UNKNOWN: i64 = -1;

    /// Whether the partition is in sync: its lag is known and is zero.
    fn in_sync(&self) -> bool { self.lag.load(Acquire) == 0 }

    fn note_appended(&self, bytes: u64) { self.appended.fetch_add(bytes, Relaxed); }

    /// Recomputes the lag from the leader's high watermark and this replica's
    /// log end, once a response row has been applied: `max(0, hw - leo)`. Like
    /// Kafka it does so only when the row appended bytes or the lag was
    /// unknown.
    pub(super) fn update(&self, leader_high_watermark: i64, log_end_offset: i64) {
        let appended = self.appended.swap(0, Relaxed);
        if appended > 0 || self.lag.load(Acquire) == Self::UNKNOWN {
            self.lag.store(
                leader_high_watermark.saturating_sub(log_end_offset).max(0),
                Release,
            );
        }
    }
}

/// Whether the `follower.replication.throttled.replicas` list of the topic
/// throttles this partition on this broker.
fn follower_partition_throttled(cfg: &Config) -> bool {
    let image = cfg.controller.current_image();
    TopicThrottle::for_topic(&image, &cfg.topic, cfg.node_id)
        .follower
        .contains(cfg.partition.get())
}

/// Accounts `bytes` of one appended batch: the replication-in metric, the lag
/// bookkeeping, and, for a throttled partition, the follower quota, in sync or
/// not.
///
/// Like `ReplicaFetcherThread.processPartitionData`, it records a partition
/// the list throttles whether or not a throttle rate is configured, and
/// whether or not the quota is already exceeded.
pub(super) fn record_replicated(cfg: &Config, bytes: u64) {
    cfg.metrics
        .record_replication_in(&cfg.topic, cfg.partition.get(), bytes);
    cfg.lag.note_appended(bytes);
    if follower_partition_throttled(cfg) {
        cfg.throttle_state.follower_in.record(bytes);
        cfg.metrics.record_replication_throttled_in(bytes);
    }
}

/// Whether a partition joins this fetch round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchThrottleDecision {
    Fetch,
    Sleep,
}

/// Kafka's `RemoteLeaderEndPoint.shouldFollowerThrottle`: a partition sits the
/// round out while it is throttled on this follower, is not in sync, and the
/// follower-in quota is exceeded.
pub(super) fn follower_fetch_decision(cfg: &Config) -> FetchThrottleDecision {
    // Kafka throttles a follower only while it is not in sync, so the cheap
    // checks come before the image lookup. Without a bound the quota is
    // never exceeded, so the lookup is skipped then too.
    if cfg.throttle_state.follower_in.bound().is_none()
        || cfg.lag.in_sync()
        || !follower_partition_throttled(cfg)
        || !cfg.throttle_state.follower_in.quota_exceeded()
    {
        return FetchThrottleDecision::Fetch;
    }
    // The partition sits the round out entirely. That refusal is what this
    // counter records; there is no delay to observe.
    cfg.metrics.record_replication_throttle_sleep();
    FetchThrottleDecision::Sleep
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::replicator::test_support::{
            ConfigWithDir, LEADER_ID, NODE_ID, image_with_follower_throttle, image_with_leader,
            test_config,
        },
        assert2::{assert, check},
        krabka_units::bytes_per_sec,
        std::{thread, time::Duration},
    };

    /// Sets a follower-in bound of `rate` bytes per second and records ten
    /// seconds' worth of it, so the quota is exceeded for several seconds.
    fn exhaust_follower_in(cfg: &Config, rate: u32) {
        let quota = &cfg.throttle_state.follower_in;
        quota.set_bound(Some(bytes_per_sec(rate)));
        quota.record(u64::from(rate) * 10);
    }

    #[test]
    fn follower_fetch_decision_ignores_unthrottled_partitions() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        exhaust_follower_in(&cfg, 1234);

        assert!(follower_fetch_decision(&cfg) == FetchThrottleDecision::Fetch);
    }

    /// With no throttle rate configured the quota has no bound and is never
    /// exceeded, however much traffic it has measured.
    #[test]
    fn follower_fetch_decision_ignores_a_quota_without_a_bound() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state.follower_in.record(1_000_000);

        assert!(follower_fetch_decision(&cfg) == FetchThrottleDecision::Fetch);
    }

    /// A configured rate of zero is a bound like any other: once any traffic
    /// is measured, a throttled partition that lags sits the round out.
    #[test]
    fn follower_fetch_decision_honours_a_zero_bound() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        let quota = &cfg.throttle_state.follower_in;
        quota.set_bound(Some(bytes_per_sec(0)));
        let before = follower_fetch_decision(&cfg);
        quota.record(1);
        let after = follower_fetch_decision(&cfg);

        check!((before, after) == (FetchThrottleDecision::Fetch, FetchThrottleDecision::Sleep));
    }

    #[test]
    fn follower_fetch_decision_sleeps_when_the_throttled_quota_is_exceeded() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        exhaust_follower_in(&cfg, 1024);

        assert!(follower_fetch_decision(&cfg) == FetchThrottleDecision::Sleep);
    }

    /// While the measured rate is within the bound the partition joins the
    /// round, and the leader may answer with as much as
    /// `replica.fetch.max.bytes` allows: the quota gates the round and does not
    /// cap what the round asks for.
    #[test]
    fn follower_fetch_decision_fetches_while_the_quota_is_within_its_bound() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_bound(Some(bytes_per_sec(1234)));

        assert!(follower_fetch_decision(&cfg) == FetchThrottleDecision::Fetch);
    }

    /// The follower list names the destination replica, which is the broker
    /// that reads it, so a list that names another broker throttles nothing
    /// here, and one that names this broker throttles the partition (#1210).
    #[test]
    fn follower_fetch_decision_reads_only_the_entries_naming_this_broker() {
        // (list, whether the partition is throttled on the follower, broker 2)
        for (list, throttled) in [
            (format!("0:{}", NODE_ID.0), true),
            (format!("0:{}", LEADER_ID.0), false),
            (format!("1:{}", NODE_ID.0), false),
        ] {
            let (cfg, _log_dir) = test_config(image_with_follower_throttle(&list));
            exhaust_follower_in(&cfg, 1024);

            let want = if throttled {
                FetchThrottleDecision::Sleep
            } else {
                FetchThrottleDecision::Fetch
            };
            check!(follower_fetch_decision(&cfg) == want, "{list}");
        }
    }

    /// A follower that has caught up to the leader's high watermark is never
    /// throttled, as Kafka's `shouldFollowerThrottle` requires the replica not
    /// to be in sync (#1211). A lag that is unknown, or above zero, is not in
    /// sync, and an empty response leaves a known lag as it was.
    #[test]
    fn a_caught_up_follower_is_never_throttled() {
        // (label, (leader hw, log end, bytes appended) of each response, whether the
        // next round is throttled)
        let cases = [
            ("no response yet: the lag is unknown", vec![], true),
            (
                "a response that leaves it behind the high watermark",
                vec![(10, 4, 100)],
                true,
            ),
            (
                "a response that reaches the high watermark",
                vec![(10, 10, 100)],
                false,
            ),
            (
                "a log end past the high watermark is no lag either",
                vec![(10, 12, 100)],
                false,
            ),
            (
                "an empty response keeps the lag it had: caught up",
                vec![(10, 10, 100), (20, 10, 0)],
                false,
            ),
            (
                "an empty response keeps the lag it had: behind",
                vec![(10, 4, 100), (10, 10, 0)],
                true,
            ),
            (
                "a response with bytes recomputes it: behind again",
                vec![(10, 10, 100), (30, 20, 50)],
                true,
            ),
        ];
        for (label, responses, throttled) in cases {
            let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
            exhaust_follower_in(&cfg, 1024);
            for (leader_hw, log_end, appended) in responses {
                cfg.lag.note_appended(appended);
                cfg.lag.update(leader_hw, log_end);
            }

            let want = if throttled {
                FetchThrottleDecision::Sleep
            } else {
                FetchThrottleDecision::Fetch
            };
            check!(follower_fetch_decision(&cfg) == want, "{label}");
        }
    }

    fn throttled_follower_at(leader_hw: i64, follower_offset: i64) -> ConfigWithDir {
        let (cfg, log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_bound(Some(bytes_per_sec(1000)));
        cfg.lag.note_appended(1);
        cfg.lag.update(leader_hw, follower_offset);
        ConfigWithDir { cfg, dir: log_dir }
    }

    /// What a caught-up follower fetches still counts against the follower
    /// quota, so that total replication stays within it (#1211): the bytes are
    /// recorded as they arrive, and the rate they raise holds back the follower
    /// once it is no longer in sync.
    #[test]
    fn a_caught_up_follower_still_charges_the_quota() {
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 10);

        // Caught up: the round is not throttled, and the batch that arrives
        // is recorded in full, four times the 1000 B/s bound.
        let fetching = follower_fetch_decision(&cfg);
        record_replicated(&cfg, 4_000);
        // Behind again: the measured rate is above the bound.
        cfg.lag.note_appended(1);
        cfg.lag.update(20, 10);
        let behind = follower_fetch_decision(&cfg);

        check!((fetching, behind) == (FetchThrottleDecision::Fetch, FetchThrottleDecision::Sleep));
    }

    /// Kafka records every throttled partition, whether or not a throttle rate
    /// is configured, so a bound set later meets the traffic already measured.
    #[test]
    fn a_throttled_partition_is_recorded_without_a_bound() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));

        record_replicated(&cfg, 5_000);

        // Within the first second the rate is measured over one whole window.
        check!(cfg.throttle_state.follower_in.measured_rate() == bytes_per_sec(5_000));
    }

    /// A huge burst from a caught-up follower holds back a lagging partition
    /// of the same broker for one monitored window at most, as Kafka's quota
    /// forgets a sample that leaves its 11 s window. A burst of `100_000`
    /// bytes on a `1_000` B/s quota is measured as `100_000` B/s over the
    /// first one-second window and keeps the lagging partition out; the
    /// quota's own tests cover the samples expiring.
    #[test]
    fn a_caught_up_burst_is_measured_and_holds_back_a_lagging_partition() {
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 10);

        record_replicated(&cfg, 100_000);
        let measured = cfg.throttle_state.follower_in.measured_rate();
        cfg.lag.note_appended(1);
        cfg.lag.update(20, 10);
        let lagging = follower_fetch_decision(&cfg);

        check!((measured, lagging) == (bytes_per_sec(100_000), FetchThrottleDecision::Sleep));
    }

    /// A batch larger than the bound arrives whole, because the leader serves
    /// the first batch of a fetch whatever `partition_max_bytes` says. Kafka
    /// records what arrived (`ReplicaFetcherThread.processPartitionData`), so
    /// the overshoot holds back the rounds after it. A follower that keeps
    /// asking gets one oversized batch and then sits out while the measured
    /// rate is above the bound. It does not pull a whole batch every round
    /// (#1340).
    #[test]
    fn an_oversized_batch_is_charged_in_full_and_throttles_the_rounds_after_it() {
        // 1000 B/s and a 5000-byte batch: the measured rate stays above the
        // bound for about five seconds, far longer than the rounds below.
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 4);

        let mut fetched_rounds = 0;
        for _ in 0..20 {
            if follower_fetch_decision(&cfg) == FetchThrottleDecision::Fetch {
                fetched_rounds += 1;
                record_replicated(&cfg, 5_000);
            }
            thread::sleep(Duration::from_millis(5));
        }

        check!(fetched_rounds == 1);
    }
}
