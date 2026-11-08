//! Gherkin scenarios for the broker's replication and log-directory-move
//! throttles, bound to the features under `tests/features`.

use {
    krabka_bdd::Given,
    krabka_throttle::{ThrottleState, TokenBucket},
    krabka_units::prelude::{Time, bytes_per_sec, secs},
    qubit_clock::ManualMonotonicClock,
    std::{sync::Arc, time::Duration},
};

const ASKS: &str = "When <traffic> traffic asks for <requested> bytes";

/// Kafka's default replication quota window: `replication.quota.window.num`
/// 11 samples of `replication.quota.window.size.seconds` 1. The broker records
/// replication traffic against it.
const REPLICATION_QUOTA_WINDOW: Time = secs(11);

/// One kind of throttled traffic, each metered by its own broker bucket.
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

/// How a quota reads at the end of a wait and one millisecond after it.
#[derive(Debug, PartialEq, Eq)]
struct QuotaOverTime {
    exceeded_at_end_of_wait: bool,
    exceeded_one_ms_later: bool,
}

/// A broker's throttles on a manual clock, driven in domain terms.
struct ThrottledBroker {
    clock: Arc<ManualMonotonicClock>,
    throttles: ThrottleState,
}

impl ThrottledBroker {
    fn new() -> Self {
        let clock = ManualMonotonicClock::new_shared();
        let throttles = ThrottleState::with_clock(Arc::<ManualMonotonicClock>::clone(&clock));
        Self { clock, throttles }
    }

    fn throttle(&self, traffic: Traffic) -> &TokenBucket {
        match traffic {
            Traffic::LeaderReplication => &self.throttles.leader_out,
            Traffic::FollowerReplication => &self.throttles.follower_in,
            Traffic::LogDirectoryMove => &self.throttles.alter_log_dirs,
        }
    }

    /// Sets the throttle rate, as a dynamic broker config change does.
    fn set_rate(&self, traffic: Traffic, rate: u32) {
        self.throttle(traffic).set_byte_rate(bytes_per_sec(rate));
    }

    fn pass(&self, millis: u64) {
        let advanced = self.clock.advance(Duration::from_millis(millis));
        assert2::assert!(let Ok(()) = advanced, "manual time moves forward");
    }

    /// Asks for up to `requested` bytes, as a log directory copy step does.
    fn ask(&self, traffic: Traffic, requested: u64) -> u64 {
        self.throttle(traffic).try_consume(requested)
    }

    /// Records bytes already sent, as replication does after its read.
    fn send(&self, traffic: Traffic, sent: u64) {
        self.throttle(traffic)
            .record_bounded(sent, REPLICATION_QUOTA_WINDOW);
    }

    fn quota_exceeded(&self, traffic: Traffic) -> bool { self.throttle(traffic).quota_exceeded() }

    fn quota_over(&self, traffic: Traffic, millis: u64) -> QuotaOverTime {
        self.pass(millis);
        let exceeded_at_end_of_wait = self.quota_exceeded(traffic);
        self.pass(1);
        QuotaOverTime {
            exceeded_at_end_of_wait,
            exceeded_one_ms_later: self.quota_exceeded(traffic),
        }
    }
}

struct ThrottleScenario {
    broker: ThrottledBroker,
    granted: Given<(Traffic, u64)>,
}

#[rstest::fixture]
fn throttle() -> ThrottleScenario {
    ThrottleScenario {
        broker: ThrottledBroker::new(),
        granted: Given::new("granted", ASKS),
    }
}

#[rstest_bdd_macros::given("{traffic} traffic is throttled at {rate} bytes per second")]
fn traffic_is_throttled(throttle: &ThrottleScenario, traffic: Traffic, rate: u32) {
    throttle.broker.set_rate(traffic, rate);
}

#[rstest_bdd_macros::given("{traffic} traffic has spent a throttle of {rate} bytes per second")]
fn traffic_has_spent_its_throttle(throttle: &ThrottleScenario, traffic: Traffic, rate: u32) {
    throttle.broker.set_rate(traffic, rate);
    let rate = u64::from(rate);
    assert2::assert!(throttle.broker.ask(traffic, rate) == rate);
}

#[rstest_bdd_macros::given("{traffic} traffic has sent {sent} bytes")]
fn traffic_has_sent(throttle: &ThrottleScenario, traffic: Traffic, sent: u64) {
    throttle.broker.send(traffic, sent);
}

#[rstest_bdd_macros::given(
    "the {traffic} throttle rate has been changed to {rate} bytes per second"
)]
fn rate_has_been_changed(throttle: &ThrottleScenario, traffic: Traffic, rate: u32) {
    throttle.broker.set_rate(traffic, rate);
}

#[rstest_bdd_macros::given("{millis} ms have passed")]
fn time_has_passed(throttle: &ThrottleScenario, millis: u64) { throttle.broker.pass(millis); }

#[rstest_bdd_macros::when("{traffic} traffic sends {sent} bytes")]
fn traffic_sends(throttle: &ThrottleScenario, traffic: Traffic, sent: u64) {
    throttle.broker.send(traffic, sent);
}

#[rstest_bdd_macros::when("{traffic} traffic asks for {requested} bytes")]
fn traffic_asks(throttle: &mut ThrottleScenario, traffic: Traffic, requested: u64) {
    let granted = throttle.broker.ask(traffic, requested);
    throttle.granted.set((traffic, granted));
}

#[rstest_bdd_macros::when("the {traffic} throttle rate changes to {rate} bytes per second")]
fn rate_changes(throttle: &ThrottleScenario, traffic: Traffic, rate: u32) {
    throttle.broker.set_rate(traffic, rate);
}

#[rstest_bdd_macros::when("the {traffic} throttle is removed")]
fn throttle_is_removed(throttle: &ThrottleScenario, traffic: Traffic) {
    throttle.broker.set_rate(traffic, 0);
}

#[rstest_bdd_macros::then("{traffic} traffic is granted {granted} bytes")]
fn traffic_is_granted(throttle: &ThrottleScenario, traffic: Traffic, granted: u64) {
    assert2::assert!(*throttle.granted.get() == (traffic, granted));
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
                exceeded_at_end_of_wait: true,
                exceeded_one_ms_later: false,
            }
    );
}

krabka_bdd::scenarios!("tests/features", fixtures = [throttle: ThrottleScenario]);
