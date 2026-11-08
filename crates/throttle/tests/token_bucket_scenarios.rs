//! Gherkin scenarios for the token bucket behind the KIP-612 connection
//! creation rate: its debt ledger, refill, refunds and whole-token grants,
//! bound to the features under `tests/features/token_bucket`.

mod support;

use {
    krabka_throttle::{MICROS_PER_TOKEN, TokenBucket},
    krabka_units::prelude::{ByteRate, ByteRateExt as _, ByteSize, Time, secs},
    qubit_clock::ManualMonotonicClock,
    std::sync::Arc,
};

/// The burst the byte scenarios give a bucket: 11 seconds of its rate.
const BURST_WINDOW: Time = secs(11);

/// What a bucket takes before it reports any debt.
#[derive(Debug, PartialEq)]
struct Headroom {
    owed_after_the_burst: f64,
    owed_after_one_byte_more: f64,
}

/// A token bucket on a manual clock, driven in domain terms.
struct Bucket {
    clock: Arc<ManualMonotonicClock>,
    bucket: TokenBucket,
}

impl Bucket {
    fn new() -> Self {
        let clock = ManualMonotonicClock::new_shared();
        let bucket = TokenBucket::with_clock(Arc::<ManualMonotonicClock>::clone(&clock));
        Self { clock, bucket }
    }

    /// Sets a byte rate with a burst of [`BURST_WINDOW`] of it.
    fn set_rate(&self, bytes_per_sec: f64) {
        let rate = ByteRate::from_bytes_per_sec_f64(bytes_per_sec);
        let burst: ByteSize = (rate * BURST_WINDOW).into();
        self.bucket.set_byte_rate_with_burst(rate, burst);
    }

    fn pass(&self, millis: u64) { support::pass(&self.clock, millis); }

    /// Charges `bytes` in full and returns what the bucket owes after it.
    fn record(&self, bytes: u64) -> f64 { debt_bytes(self.bucket.record(bytes)) }

    /// What the bucket owes right now: a charge of nothing.
    fn owed(&self) -> f64 { self.record(0) }
}

/// The bucket's micro-token debt in bytes, part byte included.
fn debt_bytes(micros: u64) -> f64 {
    let tokens = micros / MICROS_PER_TOKEN;
    let fraction = micros % MICROS_PER_TOKEN;
    f64::from(u32::try_from(tokens).unwrap_or(u32::MAX))
        + f64::from(u32::try_from(fraction).unwrap_or(u32::MAX)) / 1_000_000.0
}

struct TokenBucketScenario {
    bucket: Bucket,
    owed: krabka_bdd::Given<f64>,
    granted: krabka_bdd::Given<u64>,
}

#[rstest::fixture]
fn token_bucket() -> TokenBucketScenario {
    TokenBucketScenario {
        bucket: Bucket::new(),
        owed: krabka_bdd::Given::new("owed", "When <bytes> bytes are recorded"),
        granted: krabka_bdd::Given::new(
            "granted",
            "When the acceptor asks for <count> connections",
        ),
    }
}

#[rstest_bdd_macros::given("a token bucket of {rate} bytes per second")]
fn token_bucket_of(token_bucket: &TokenBucketScenario, rate: f64) {
    token_bucket.bucket.set_rate(rate);
}

#[rstest_bdd_macros::given("{bytes} bytes have been recorded")]
fn bytes_have_been_recorded(token_bucket: &TokenBucketScenario, bytes: u64) {
    token_bucket.bucket.record(bytes);
}

#[rstest_bdd_macros::given("the bucket's rate has been raised to {rate} bytes per second")]
fn rate_raised(token_bucket: &TokenBucketScenario, rate: f64) {
    token_bucket.bucket.set_rate(rate);
}

#[rstest_bdd_macros::given(
    "a connection quota of {rate} connections per second with a burst of {burst}"
)]
fn connection_quota(token_bucket: &TokenBucketScenario, rate: u64, burst: u64) {
    token_bucket
        .bucket
        .bucket
        .set_token_rate_with_burst(rate, burst);
}

#[rstest_bdd_macros::given("the acceptor has been granted {count} connections")]
fn acceptor_has_been_granted(token_bucket: &TokenBucketScenario, count: u64) {
    assert2::assert!(token_bucket.bucket.bucket.try_consume(count) == count);
}

#[rstest_bdd_macros::given("{count} connections have been recorded")]
fn connections_recorded(token_bucket: &TokenBucketScenario, count: u64) {
    token_bucket.bucket.bucket.record(count);
}

#[rstest_bdd_macros::given("{millis} ms have passed")]
fn time_has_passed(token_bucket: &TokenBucketScenario, millis: u64) {
    token_bucket.bucket.pass(millis);
}

#[rstest_bdd_macros::when("{bytes} bytes are recorded")]
fn bytes_are_recorded(token_bucket: &mut TokenBucketScenario, bytes: u64) {
    let owed = token_bucket.bucket.record(bytes);
    token_bucket.owed.set(owed);
}

#[rstest_bdd_macros::when("{bytes} bytes are recorded under a throttle bound of {seconds} seconds")]
fn bytes_are_recorded_bounded(token_bucket: &mut TokenBucketScenario, bytes: u64, seconds: u32) {
    let owed = debt_bytes(
        token_bucket
            .bucket
            .bucket
            .record_bounded(bytes, secs(seconds)),
    );
    token_bucket.owed.set(owed);
}

#[rstest_bdd_macros::when("{bytes} bytes are refunded")]
fn bytes_are_refunded(token_bucket: &TokenBucketScenario, bytes: u64) {
    token_bucket.bucket.bucket.refund(bytes);
}

#[rstest_bdd_macros::when("{millis} ms pass")]
fn time_passes(token_bucket: &mut TokenBucketScenario, millis: u64) {
    token_bucket.bucket.pass(millis);
    let owed = token_bucket.bucket.owed();
    token_bucket.owed.set(owed);
}

#[rstest_bdd_macros::when("the acceptor asks for {count} connections")]
fn acceptor_asks(token_bucket: &mut TokenBucketScenario, count: u64) {
    let granted = token_bucket.bucket.bucket.try_consume(count);
    token_bucket.granted.set(granted);
}

#[rstest_bdd_macros::then("the bucket owes {bytes} bytes")]
fn bucket_owes(token_bucket: &TokenBucketScenario, bytes: f64) {
    let owed = *token_bucket.owed.get();
    assert2::assert!((owed - bytes).abs() < 1.0e-6, "owed {owed}, want {bytes}");
}

#[rstest_bdd_macros::then("the bucket can take {bytes} bytes before it owes anything")]
fn bucket_headroom(token_bucket: &TokenBucketScenario, bytes: u64) {
    let headroom = Headroom {
        owed_after_the_burst: token_bucket.bucket.record(bytes),
        owed_after_one_byte_more: token_bucket.bucket.record(1),
    };
    assert2::assert!(
        headroom
            == Headroom {
                owed_after_the_burst: 0.0,
                owed_after_one_byte_more: 1.0,
            }
    );
}

#[rstest_bdd_macros::then("the acceptor is granted {count} connections")]
fn acceptor_is_granted(token_bucket: &TokenBucketScenario, count: u64) {
    assert2::assert!(*token_bucket.granted.get() == count);
}

krabka_bdd::scenarios!("tests/features/token_bucket", fixtures = [token_bucket: TokenBucketScenario]);
