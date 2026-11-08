//! [`ClientQuota`], one entity's KIP-13 or KIP-124 client quota: the quota
//! sensor of Kafka's `ClientQuotaManager` over a `Rate`.
//!
//! The broker keeps one per quota entity and quota type:
//! `producer_byte_rate`, `consumer_byte_rate` and `request_percentage`, and
//! the KIP-612 `connection_creation_rate` of each `ip` entity, which Kafka's
//! `ConnectionQuotas` measures with the same sensor. A request is recorded in
//! full and then checked, as Kafka's `recordAndGetThrottleTimeMs` records
//! with `checkQuotas` set, so a request over the quota still counts, and the
//! requests after it pay for it until its sample leaves the window.
//!
//! The measurement is [`SampledRate`] with the `Rate` window size: a young
//! window is padded to `quota.window.num - 1` whole samples, so a new client
//! may send `quota.window.num - 1` samples' worth of traffic at once before
//! it is throttled. The quota is exceeded while the rate is above the bound,
//! and the throttle is Kafka's `QuotaUtils.throttleTime`:
//!
//! ```text
//! (rate - bound) / bound * window size
//! ```
//!
//! rounded to the millisecond. Once the window is past its padding, that is
//! the wait that brings the measured rate back to the bound if the client
//! sends nothing more. While it is padded, the window stays the same length
//! as time passes, so a client that waits out its throttle and sends again
//! can be throttled again, until its burst's sample leaves the window.

use {
    crate::{
        QuotaWindow,
        sampled_rate::{self, SampledRate, WindowSize},
    },
    krabka_units::prelude::{Time, TimeExt},
    num_traits::ToPrimitive as _,
    qubit_clock::{MonotonicClock, StdMonotonicClock},
    std::sync::{Arc, Mutex, MutexGuard},
};

/// What the quota's lock guards: the bound and the measured rate.
#[derive(Debug)]
struct QuotaState {
    /// Per second, in the unit the quota records.
    bound: f64,
    rate: SampledRate,
}

/// One entity's client quota. See the module documentation.
#[derive(derive_more::Debug)]
pub struct ClientQuota {
    #[debug("{:?}", *self.lock_state())]
    state: Mutex<QuotaState>,
    #[debug(skip)]
    clock: Arc<dyn MonotonicClock>,
}

impl ClientQuota {
    /// A quota of `bound` per second, in the unit it records, over `window`
    /// on a caller-supplied clock. Tests pass a
    /// [`qubit_clock::ManualMonotonicClock`].
    ///
    /// The bound is a byte rate for `producer_byte_rate` and
    /// `consumer_byte_rate`, and handler time per second for
    /// `request_percentage`. A bound that is not finite and above zero
    /// throttles nothing.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>, window: QuotaWindow, bound: f64) -> Self {
        Self {
            state: Mutex::new(QuotaState {
                bound,
                rate: SampledRate::over(window, WindowSize::Padded),
            }),
            clock,
        }
    }

    /// A quota of `bound` per second over `window`, on the system's monotonic
    /// clock.
    #[must_use]
    pub fn new(window: QuotaWindow, bound: f64) -> Self {
        Self::with_clock(Arc::new(StdMonotonicClock::new()), window, bound)
    }

    /// Kafka's `updateQuotaMetricConfigs`: sets a new bound and keeps the
    /// traffic already measured.
    pub fn set_bound(&self, bound: f64) { self.lock_state().bound = bound; }

    /// The bound in force, per second.
    #[must_use]
    pub fn bound(&self) -> f64 { self.lock_state().bound }

    /// Kafka's `ClientQuotaManager.recordAndGetThrottleTimeMs`: records
    /// `value`, then returns how long the client is throttled for, zero while
    /// the measured rate is within the bound.
    pub fn record(&self, value: u64) -> Time {
        let now_ms = self.now_ms();
        let (bound, measured, window_ms) = {
            let mut state = self.lock_state();
            // Kafka records the long value as a double.
            state
                .rate
                .record(value.to_f64().unwrap_or(f64::INFINITY), now_ms);
            let measured = state.rate.measure(now_ms);
            // `QuotaUtils.throttleTime` reads the window the rate was
            // measured across.
            let window_ms = state.rate.window_size_ms(now_ms);
            (state.bound, measured, window_ms)
        };
        // `Quota.acceptable` for an upper bound is `value <= bound`.
        if !bound.is_finite() || bound <= 0.0 || measured <= bound {
            return <Time as TimeExt>::ZERO;
        }
        let window_ms = window_ms.to_f64().unwrap_or(f64::INFINITY);
        let throttle_ms = ((measured - bound) / bound * window_ms).round();
        Time::from_millis(throttle_ms.to_i64().unwrap_or(i64::MAX))
    }

    /// Kafka's `ClientQuotaManager.unrecordQuotaSensor`: takes `value` back
    /// off the measured rate, without a quota check. A throttled fetch sends
    /// no records, so its bytes come back.
    pub fn unrecord(&self, value: u64) {
        let now_ms = self.now_ms();
        self.lock_state()
            .rate
            .record(-value.to_f64().unwrap_or(f64::INFINITY), now_ms);
    }

    /// The measured rate right now, per second: Kafka's quota metric.
    #[must_use]
    pub fn measured_rate(&self) -> f64 {
        let now_ms = self.now_ms();
        self.lock_state().rate.measure(now_ms)
    }

    fn now_ms(&self) -> i64 { sampled_rate::now_ms(&*self.clock) }

    fn lock_state(&self) -> MutexGuard<'_, QuotaState> { sampled_rate::lock(&self.state) }
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::check, krabka_units::prelude::millis, qubit_clock::ManualMonotonicClock,
        std::time::Duration,
    };

    fn quota(bound: f64) -> (Arc<ManualMonotonicClock>, ClientQuota) {
        let clock = ManualMonotonicClock::new_shared();
        let quota = ClientQuota::with_clock(
            Arc::<ManualMonotonicClock>::clone(&clock),
            QuotaWindow::KAFKA_DEFAULT,
            bound,
        );
        (clock, quota)
    }

    /// A new client is measured over ten padded samples, so 10 000 bytes at
    /// 1000 B/s are within the quota and one byte more is over it by
    /// 0.1 B/s, a 1 ms throttle.
    #[yare::parameterized(
        within = { 10_000, 0 },
        one_byte_over = { 10_001, 1 },
        twice_the_window = { 20_000, 10_000 },
    )]
    fn a_first_burst_is_measured_over_the_padded_window(sent: u64, throttle_ms: u32) {
        let (_, quota) = quota(1_000.0);

        check!(quota.record(sent) == millis(throttle_ms));
    }

    /// 15 000 bytes at 1000 B/s are 1500 B/s over the padded window, a 5 s
    /// throttle. The padding holds the window at 10 s for the first ten
    /// seconds, so the rate, and the throttle, are the same 5 s later. The
    /// burst is forgotten once its sample is a whole window old.
    #[yare::parameterized(
        at_once = { 0, 5_000 },
        five_seconds_later = { 5_000, 5_000 },
        ten_seconds_later = { 10_000, 5_000 },
        a_window_later = { 11_000, 0 },
    )]
    fn a_burst_throttles_until_its_sample_leaves_the_window(later_ms: u64, throttle_ms: u32) {
        let (clock, quota) = quota(1_000.0);
        quota.record(15_000);

        let advanced = clock.advance(Duration::from_millis(later_ms));
        check!(let Ok(()) = advanced);
        check!(quota.record(0) == millis(throttle_ms));
    }

    /// Past the padding the window is the time since the oldest sample
    /// started, so the throttle is the wait that brings the rate back to the
    /// bound: 1000 B/s for 10 s and then 5000 bytes more is 1500 B/s over
    /// 10 s, which takes 5 s more to come back to 1000 B/s.
    #[test]
    fn past_the_padding_the_throttle_is_the_wait_back_to_the_bound() {
        let (clock, quota) = quota(1_000.0);
        for _ in 0..10 {
            check!(quota.record(1_000) == millis(0));
            let advanced = clock.advance(Duration::from_millis(1_000));
            check!(let Ok(()) = advanced);
        }
        // The oldest sample started at 0 s and it is 10 s now: 15 000 bytes
        // over 10 s.
        check!(quota.record(5_000) == millis(5_000));
    }

    /// Taking a fetch's bytes back leaves the quota as it was before it.
    #[test]
    fn unrecord_takes_the_charge_back() {
        let (_, quota) = quota(1_000.0);
        quota.record(15_000);
        quota.unrecord(15_000);

        check!(quota.record(10_000) == millis(0));
    }

    /// A bound that is not a positive rate throttles nothing.
    #[yare::parameterized(
        zero = { 0.0 },
        negative = { -1.0 },
        infinite = { f64::INFINITY },
        nan = { f64::NAN },
    )]
    fn a_bound_that_is_not_a_positive_rate_throttles_nothing(bound: f64) {
        let (_, quota) = quota(bound);

        check!(quota.record(u64::from(u32::MAX)) == millis(0));
    }

    /// A new bound applies to the traffic already measured.
    #[test]
    fn a_new_bound_keeps_the_measured_traffic() {
        let (_, quota) = quota(1_000.0);
        quota.record(15_000);
        quota.set_bound(2_000.0);

        check!((quota.bound() - 2_000.0).abs() < 1.0e-9);
        check!(quota.record(0) == millis(0));
        // 20 010 bytes over 10 s is 1 B/s over 2000 B/s: 5 ms.
        check!(quota.record(5_010) == millis(5));
    }
}
