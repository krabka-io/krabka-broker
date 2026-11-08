//! Gherkin scenarios for the KIP-13 and KIP-124 client quota, Kafka's
//! `ClientQuotaManager` quota sensor over a `Rate`, bound to the features
//! under `tests/features/client_quota`.

mod support;

use {
    krabka_throttle::{ClientQuota, QuotaWindow},
    krabka_units::prelude::{Time, TimeExt, millis, secs},
    qubit_clock::ManualMonotonicClock,
    std::{num::NonZeroU32, sync::Arc},
};

/// A `server.properties` key that sets the client quota window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum WindowConfig {
    #[step_param("quota.window.num")]
    Samples,
    #[step_param("quota.window.size.seconds")]
    SampleSeconds,
}

/// Milliseconds of request handler time one `request_percentage` point
/// allows each second: one percent of a second. The scenarios record handler
/// time in milliseconds, so a request quota's bound is in milliseconds per
/// second. The throttle does not depend on the unit.
const HANDLER_MS_PER_PERCENT: f64 = 10.0;

/// The throttles a client gets for a quota's worth of traffic and one byte
/// more.
#[derive(Debug, PartialEq)]
struct Headroom {
    throttle_at_the_headroom: Time,
    throttle_one_byte_more: Time,
}

/// A client's quota on a manual clock, driven in domain terms.
struct QuotaClient {
    clock: Arc<ManualMonotonicClock>,
    window: QuotaWindow,
    quota: krabka_bdd::Given<ClientQuota>,
}

impl QuotaClient {
    fn new() -> Self {
        Self {
            clock: ManualMonotonicClock::new_shared(),
            window: QuotaWindow::KAFKA_DEFAULT,
            quota: krabka_bdd::Given::new(
                "quota",
                "Given a client quota of <rate> bytes per second",
            ),
        }
    }

    /// Sets one window config, as a broker reads `server.properties` before
    /// it creates any quota.
    fn configure(&mut self, config: WindowConfig, value: NonZeroU32) {
        self.window = match config {
            WindowConfig::Samples => QuotaWindow::new(value, self.window.sample()),
            WindowConfig::SampleSeconds => QuotaWindow::new(
                NonZeroU32::new(self.window.samples()).unwrap_or(NonZeroU32::MIN),
                secs(value.get()),
            ),
        };
    }

    /// Gives the client a quota of `bound` per second over the configured
    /// window.
    fn limit_to(&mut self, bound: f64) {
        let clock = Arc::<ManualMonotonicClock>::clone(&self.clock);
        self.quota
            .set(ClientQuota::with_clock(clock, self.window, bound));
    }

    /// Changes the bound, as a dynamic quota config change does.
    fn change_bound(&self, bound: f64) { self.quota.get().set_bound(bound); }

    fn pass(&self, millis: u64) { support::pass(&self.clock, millis); }

    /// Records a request in full and returns the client's throttle.
    fn send(&self, value: u64) -> Time { self.quota.get().record(value) }

    /// Sends `bytes` once a second for `seconds` seconds, then waits for the
    /// next second.
    fn send_every_second(&self, bytes: u64, seconds: u32) {
        for _ in 0..seconds {
            self.send(bytes);
            self.pass(1_000);
        }
    }

    /// Takes the bytes of a throttled fetch back off the quota.
    fn take_back(&self, bytes: u64) { self.quota.get().unrecord(bytes); }
}

struct ClientQuotaScenario {
    client: QuotaClient,
    throttle: krabka_bdd::Given<Time>,
}

#[rstest::fixture]
fn client_quota() -> ClientQuotaScenario {
    ClientQuotaScenario {
        client: QuotaClient::new(),
        throttle: krabka_bdd::Given::new("throttle", "When the client sends <bytes> bytes"),
    }
}

#[rstest_bdd_macros::given("the broker sets {config} to {value}")]
fn broker_sets_window(
    client_quota: &mut ClientQuotaScenario,
    config: WindowConfig,
    value: NonZeroU32,
) {
    client_quota.client.configure(config, value);
}

#[rstest_bdd_macros::given("a client quota of {rate} bytes per second")]
fn client_quota_of(client_quota: &mut ClientQuotaScenario, rate: f64) {
    client_quota.client.limit_to(rate);
}

#[rstest_bdd_macros::given("a request quota of {percent} percent")]
fn request_quota_of(client_quota: &mut ClientQuotaScenario, percent: f64) {
    client_quota
        .client
        .limit_to(percent * HANDLER_MS_PER_PERCENT);
}

#[rstest_bdd_macros::given("the client has sent {bytes} bytes")]
fn client_has_sent(client_quota: &ClientQuotaScenario, bytes: u64) {
    client_quota.client.send(bytes);
}

#[rstest_bdd_macros::given("the client has fetched {bytes} bytes")]
fn client_has_fetched(client_quota: &ClientQuotaScenario, bytes: u64) {
    client_quota.client.send(bytes);
}

#[rstest_bdd_macros::given("the client has sent {bytes} bytes once a second for {seconds} seconds")]
fn client_has_sent_every_second(client_quota: &ClientQuotaScenario, bytes: u64, seconds: u32) {
    client_quota.client.send_every_second(bytes, seconds);
}

#[rstest_bdd_macros::given("the quota has been changed to {rate} bytes per second")]
fn quota_changed(client_quota: &ClientQuotaScenario, rate: f64) {
    client_quota.client.change_bound(rate);
}

#[rstest_bdd_macros::given("{millis} ms have passed")]
fn time_has_passed(client_quota: &ClientQuotaScenario, millis: u64) {
    client_quota.client.pass(millis);
}

#[rstest_bdd_macros::when("the client sends {bytes} bytes")]
fn client_sends(client_quota: &mut ClientQuotaScenario, bytes: u64) {
    let throttle = client_quota.client.send(bytes);
    client_quota.throttle.set(throttle);
}

#[rstest_bdd_macros::when("the client sends an empty request")]
fn client_sends_empty_request(client_quota: &mut ClientQuotaScenario) {
    let throttle = client_quota.client.send(0);
    client_quota.throttle.set(throttle);
}

#[rstest_bdd_macros::when("the client's requests take {handler_ms} ms of handler time")]
fn requests_take_handler_time(client_quota: &mut ClientQuotaScenario, handler_ms: u64) {
    let throttle = client_quota.client.send(handler_ms);
    client_quota.throttle.set(throttle);
}

#[rstest_bdd_macros::when("the broker takes back the {bytes} bytes of the throttled fetch")]
fn broker_takes_back(client_quota: &ClientQuotaScenario, bytes: u64) {
    client_quota.client.take_back(bytes);
}

#[rstest_bdd_macros::then("the client is throttled for {throttle_ms} ms")]
fn client_is_throttled(client_quota: &ClientQuotaScenario, throttle_ms: u32) {
    assert2::assert!(*client_quota.throttle.get() == millis(throttle_ms));
}

#[rstest_bdd_macros::then("the client is not throttled")]
fn client_is_not_throttled(client_quota: &ClientQuotaScenario) {
    assert2::assert!(*client_quota.throttle.get() == <Time as TimeExt>::ZERO);
}

#[rstest_bdd_macros::then("the client can fetch {bytes} bytes before it is throttled")]
fn client_headroom(client_quota: &ClientQuotaScenario, bytes: u64) {
    let headroom = Headroom {
        throttle_at_the_headroom: client_quota.client.send(bytes),
        throttle_one_byte_more: client_quota.client.send(1),
    };
    assert2::assert!(
        headroom
            == Headroom {
                throttle_at_the_headroom: <Time as TimeExt>::ZERO,
                throttle_one_byte_more: millis(1),
            }
    );
}

krabka_bdd::scenarios!("tests/features/client_quota", fixtures = [client_quota: ClientQuotaScenario]);
