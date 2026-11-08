//! Gherkin scenarios for the broker's replication and log-directory-move
//! quotas, bound to the features under `tests/features`.

use {
    krabka_throttle::{QuotaWindow, ReplicationQuota, ThrottleState},
    krabka_units::prelude::{ByteRate, ByteRateExt as _, secs},
    qubit_clock::ManualMonotonicClock,
    std::{num::NonZeroU32, sync::Arc, time::Duration},
};

/// One kind of throttled traffic, each measured by its own broker quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Traffic {
    /// `leader.replication.throttled.rate`.
    #[step_param("leader replication")]
    LeaderReplication,
    /// `follower.replication.throttled.rate`.
    #[step_param("follower replication")]
    FollowerReplication,
    /// `replica.alter.log.dirs.io.max.bytes.per.second`.
    #[step_param("log directory move")]
    LogDirectoryMove,
}

/// A `server.properties` key that sets a replication quota window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum WindowConfig {
    #[step_param("replication.quota.window.num")]
    ReplicationSamples,
    #[step_param("replication.quota.window.size.seconds")]
    ReplicationSampleSeconds,
    #[step_param("alter.log.dirs.replication.quota.window.num")]
    AlterLogDirsSamples,
    #[step_param("alter.log.dirs.replication.quota.window.size.seconds")]
    AlterLogDirsSampleSeconds,
}

/// How a quota reads one millisecond before a wait ends and when it ends.
#[derive(Debug, PartialEq, Eq)]
struct QuotaOverTime {
    exceeded_one_ms_before: bool,
    exceeded_at_end_of_wait: bool,
}

/// A broker's throttles on a manual clock, driven in domain terms.
struct ThrottledBroker {
    clock: Arc<ManualMonotonicClock>,
    replication_window: QuotaWindow,
    alter_log_dirs_window: QuotaWindow,
    throttles: ThrottleState,
}

impl ThrottledBroker {
    fn new() -> Self {
        let clock = ManualMonotonicClock::new_shared();
        let throttles = ThrottleState::with_clock(Arc::<ManualMonotonicClock>::clone(&clock));
        Self {
            clock,
            replication_window: QuotaWindow::KAFKA_DEFAULT,
            alter_log_dirs_window: QuotaWindow::KAFKA_DEFAULT,
            throttles,
        }
    }

    /// Sets one window config and restarts the throttles with it, as a
    /// broker reads `server.properties` once at startup.
    fn configure(&mut self, config: WindowConfig, value: NonZeroU32) {
        let with_samples = |window: QuotaWindow| QuotaWindow::new(value, window.sample());
        let with_seconds = |window: QuotaWindow| {
            QuotaWindow::new(
                NonZeroU32::new(window.samples()).unwrap_or(NonZeroU32::MIN),
                secs(value.get()),
            )
        };
        match config {
            WindowConfig::ReplicationSamples => {
                self.replication_window = with_samples(self.replication_window);
            }
            WindowConfig::ReplicationSampleSeconds => {
                self.replication_window = with_seconds(self.replication_window);
            }
            WindowConfig::AlterLogDirsSamples => {
                self.alter_log_dirs_window = with_samples(self.alter_log_dirs_window);
            }
            WindowConfig::AlterLogDirsSampleSeconds => {
                self.alter_log_dirs_window = with_seconds(self.alter_log_dirs_window);
            }
        }
        self.throttles = ThrottleState::with_windows()
            .clock(Arc::<ManualMonotonicClock>::clone(&self.clock))
            .replication_window(self.replication_window)
            .alter_log_dirs_window(self.alter_log_dirs_window)
            .call();
    }

    fn quota(&self, traffic: Traffic) -> &ReplicationQuota {
        match traffic {
            Traffic::LeaderReplication => &self.throttles.leader_out,
            Traffic::FollowerReplication => &self.throttles.follower_in,
            Traffic::LogDirectoryMove => &self.throttles.alter_log_dirs,
        }
    }

    /// Sets the throttle rate, as a dynamic broker config change does.
    fn throttle_at(&self, traffic: Traffic, rate: i64) {
        self.quota(traffic)
            .set_bound(Some(ByteRate::from_bytes_per_sec(rate)));
    }

    /// Removes the throttle rate, as deleting the dynamic broker config does.
    fn unthrottle(&self, traffic: Traffic) { self.quota(traffic).set_bound(None); }

    fn pass(&self, millis: u64) {
        let advanced = self.clock.advance(Duration::from_millis(millis));
        assert2::assert!(let Ok(()) = advanced, "manual time moves forward");
    }

    /// Records bytes already moved, as a broker does after its read.
    fn send(&self, traffic: Traffic, sent: u64) { self.quota(traffic).record(sent); }

    fn quota_exceeded(&self, traffic: Traffic) -> bool { self.quota(traffic).quota_exceeded() }

    fn quota_over(&self, traffic: Traffic, millis: u64) -> QuotaOverTime {
        self.pass(millis.saturating_sub(1));
        let exceeded_one_ms_before = self.quota_exceeded(traffic);
        self.pass(1);
        QuotaOverTime {
            exceeded_one_ms_before,
            exceeded_at_end_of_wait: self.quota_exceeded(traffic),
        }
    }
}

struct ThrottleScenario {
    broker: ThrottledBroker,
}

#[rstest::fixture]
fn throttle() -> ThrottleScenario {
    ThrottleScenario {
        broker: ThrottledBroker::new(),
    }
}

#[rstest_bdd_macros::given("the broker sets {config} to {value}")]
fn broker_sets_window(throttle: &mut ThrottleScenario, config: WindowConfig, value: NonZeroU32) {
    throttle.broker.configure(config, value);
}

#[rstest_bdd_macros::given("{traffic} traffic is throttled at {rate} bytes per second")]
fn traffic_is_throttled(throttle: &ThrottleScenario, traffic: Traffic, rate: i64) {
    throttle.broker.throttle_at(traffic, rate);
}

#[rstest_bdd_macros::given("{traffic} traffic has sent {sent} bytes")]
fn traffic_has_sent(throttle: &ThrottleScenario, traffic: Traffic, sent: u64) {
    throttle.broker.send(traffic, sent);
}

#[rstest_bdd_macros::when("{traffic} traffic is throttled at {rate} bytes per second")]
fn traffic_becomes_throttled(throttle: &ThrottleScenario, traffic: Traffic, rate: i64) {
    throttle.broker.throttle_at(traffic, rate);
}

#[rstest_bdd_macros::when("{traffic} traffic sends {sent} bytes")]
fn traffic_sends(throttle: &ThrottleScenario, traffic: Traffic, sent: u64) {
    throttle.broker.send(traffic, sent);
}

#[rstest_bdd_macros::when("the {traffic} throttle rate changes to {rate} bytes per second")]
fn rate_changes(throttle: &ThrottleScenario, traffic: Traffic, rate: i64) {
    throttle.broker.throttle_at(traffic, rate);
}

#[rstest_bdd_macros::when("the {traffic} throttle is removed")]
fn throttle_is_removed(throttle: &ThrottleScenario, traffic: Traffic) {
    throttle.broker.unthrottle(traffic);
}

#[rstest_bdd_macros::then("the {traffic} quota is exceeded")]
fn quota_is_exceeded(throttle: &ThrottleScenario, traffic: Traffic) {
    assert2::assert!(throttle.broker.quota_exceeded(traffic));
}

#[rstest_bdd_macros::then("the {traffic} quota is not exceeded")]
fn quota_is_not_exceeded(throttle: &ThrottleScenario, traffic: Traffic) {
    assert2::assert!(!throttle.broker.quota_exceeded(traffic));
}

#[rstest_bdd_macros::then("the {traffic} quota stays exceeded for {millis} ms")]
fn quota_stays_exceeded_for(throttle: &ThrottleScenario, traffic: Traffic, millis: u64) {
    assert2::assert!(
        throttle.broker.quota_over(traffic, millis)
            == QuotaOverTime {
                exceeded_one_ms_before: true,
                exceeded_at_end_of_wait: false,
            }
    );
}

krabka_bdd::scenarios!("tests/features/replication", fixtures = [throttle: ThrottleScenario]);
