//! Gherkin scenarios for the token bucket behind the client quotas, bound to
//! the features under `tests/features/client_quota`.

use {
    krabka_throttle::{MICROS_PER_TOKEN, TokenBucket},
    krabka_units::prelude::{ByteRate, ByteRateExt as _, ByteSize, Time, secs},
    qubit_clock::ManualMonotonicClock,
    std::{sync::Arc, time::Duration},
};

/// Kafka's default client quota window: `quota.window.num` 11 samples of
/// `quota.window.size.seconds` 1. The broker sizes a client bucket's burst
/// to one window of its rate.
const QUOTA_WINDOW: Time = secs(11);

/// What a client can send before the bucket reports any debt.
#[derive(Debug, PartialEq)]
struct Headroom {
    owed_after_the_window: f64,
    owed_after_one_byte_more: f64,
}

/// A client quota's token bucket on a manual clock, driven in domain terms.
struct ClientQuota {
    clock: Arc<ManualMonotonicClock>,
    bucket: TokenBucket,
}

impl ClientQuota {
    fn new() -> Self {
        let clock = ManualMonotonicClock::new_shared();
        let bucket = TokenBucket::with_clock(Arc::<ManualMonotonicClock>::clone(&clock));
        Self { clock, bucket }
    }

    /// Sets the quota rate with one quota window of burst, as the broker's
    /// quota buckets do.
    fn set_rate(&self, bytes_per_sec: f64) {
        let rate = ByteRate::from_bytes_per_sec_f64(bytes_per_sec);
        let burst: ByteSize = (rate * QUOTA_WINDOW).into();
        self.bucket.set_byte_rate_with_burst(rate, burst);
    }

    fn pass(&self, millis: u64) {
        let advanced = self.clock.advance(Duration::from_millis(millis));
        assert2::assert!(let Ok(()) = advanced, "manual time moves forward");
    }

    /// Charges a request in full, as the broker records it, and returns what
    /// the client owes after it, in bytes.
    fn send(&self, bytes: u64) -> f64 { debt_bytes(self.bucket.record(bytes)) }

    /// What the client owes right now, in bytes: a charge of nothing.
    fn owed(&self) -> f64 { self.send(0) }
}

/// The bucket's micro-token debt in bytes, part byte included.
fn debt_bytes(micros: u64) -> f64 {
    let tokens = micros / MICROS_PER_TOKEN;
    let fraction = micros % MICROS_PER_TOKEN;
    f64::from(u32::try_from(tokens).unwrap_or(u32::MAX))
        + f64::from(u32::try_from(fraction).unwrap_or(u32::MAX)) / 1_000_000.0
}

struct ClientQuotaScenario {
    quota: ClientQuota,
    owed: krabka_bdd::Given<f64>,
    granted: krabka_bdd::Given<u64>,
}

#[rstest::fixture]
fn client_quota() -> ClientQuotaScenario {
    ClientQuotaScenario {
        quota: ClientQuota::new(),
        owed: krabka_bdd::Given::new("owed", "When the client sends <bytes> bytes"),
        granted: krabka_bdd::Given::new(
            "granted",
            "When the acceptor asks for <count> connections",
        ),
    }
}

#[rstest_bdd_macros::given("a client quota of {rate} bytes per second")]
fn client_quota_of(client_quota: &ClientQuotaScenario, rate: f64) {
    client_quota.quota.set_rate(rate);
}

#[rstest_bdd_macros::given("the client has sent {bytes} bytes")]
fn client_has_sent(client_quota: &ClientQuotaScenario, bytes: u64) {
    client_quota.quota.send(bytes);
}

#[rstest_bdd_macros::given("the quota has been raised to {rate} bytes per second")]
fn quota_raised(client_quota: &ClientQuotaScenario, rate: f64) {
    client_quota.quota.set_rate(rate);
}

#[rstest_bdd_macros::given(
    "a connection quota of {rate} connections per second with a burst of {burst}"
)]
fn connection_quota(client_quota: &ClientQuotaScenario, rate: u64, burst: u64) {
    client_quota
        .quota
        .bucket
        .set_token_rate_with_burst(rate, burst);
}

#[rstest_bdd_macros::given("the acceptor has been granted {count} connections")]
fn acceptor_has_been_granted(client_quota: &ClientQuotaScenario, count: u64) {
    assert2::assert!(client_quota.quota.bucket.try_consume(count) == count);
}

#[rstest_bdd_macros::given("{count} connections have been recorded")]
fn connections_recorded(client_quota: &ClientQuotaScenario, count: u64) {
    client_quota.quota.bucket.record(count);
}

#[rstest_bdd_macros::given("{millis} ms have passed")]
fn time_has_passed(client_quota: &ClientQuotaScenario, millis: u64) {
    client_quota.quota.pass(millis);
}

#[rstest_bdd_macros::when("the client sends {bytes} bytes")]
fn client_sends(client_quota: &mut ClientQuotaScenario, bytes: u64) {
    let owed = client_quota.quota.send(bytes);
    client_quota.owed.set(owed);
}

#[rstest_bdd_macros::when(
    "the client sends {bytes} bytes under a throttle bound of {seconds} seconds"
)]
fn client_sends_bounded(client_quota: &mut ClientQuotaScenario, bytes: u64, seconds: u32) {
    let owed = debt_bytes(
        client_quota
            .quota
            .bucket
            .record_bounded(bytes, secs(seconds)),
    );
    client_quota.owed.set(owed);
}

#[rstest_bdd_macros::when("the broker refunds {bytes} bytes to the client")]
fn broker_refunds(client_quota: &ClientQuotaScenario, bytes: u64) {
    client_quota.quota.bucket.refund(bytes);
}

#[rstest_bdd_macros::when("{millis} ms pass")]
fn time_passes(client_quota: &mut ClientQuotaScenario, millis: u64) {
    client_quota.quota.pass(millis);
    let owed = client_quota.quota.owed();
    client_quota.owed.set(owed);
}

#[rstest_bdd_macros::when("the acceptor asks for {count} connections")]
fn acceptor_asks(client_quota: &mut ClientQuotaScenario, count: u64) {
    let granted = client_quota.quota.bucket.try_consume(count);
    client_quota.granted.set(granted);
}

#[rstest_bdd_macros::then("the client owes {bytes} bytes")]
fn client_owes(client_quota: &ClientQuotaScenario, bytes: f64) {
    let owed = *client_quota.owed.get();
    assert2::assert!((owed - bytes).abs() < 1.0e-6, "owed {owed}, want {bytes}");
}

#[rstest_bdd_macros::then("the client can send {bytes} bytes before it owes anything")]
fn client_headroom(client_quota: &ClientQuotaScenario, bytes: u64) {
    let headroom = Headroom {
        owed_after_the_window: client_quota.quota.send(bytes),
        owed_after_one_byte_more: client_quota.quota.send(1),
    };
    assert2::assert!(
        headroom
            == Headroom {
                owed_after_the_window: 0.0,
                owed_after_one_byte_more: 1.0,
            }
    );
}

#[rstest_bdd_macros::then("the acceptor is granted {count} connections")]
fn acceptor_is_granted(client_quota: &ClientQuotaScenario, count: u64) {
    assert2::assert!(*client_quota.granted.get() == count);
}

krabka_bdd::scenarios!("tests/features/client_quota", fixtures = [client_quota: ClientQuotaScenario]);
