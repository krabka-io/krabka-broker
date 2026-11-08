//! [`ReplicationQuota`], the KIP-73 replication quota: Kafka's
//! `ReplicationQuotaManager` over a `SimpleRate`.
//!
//! The broker meters leader replication, follower replication and KIP-113 log
//! directory moves with one quota each. A reader asks [`ReplicationQuota::
//! quota_exceeded`] before it reads throttled traffic and calls
//! [`ReplicationQuota::record`] with the bytes it moved after, as Kafka's
//! fetchers ask `isQuotaExceeded` and call `record`.
//!
//! The measurement is [`SampledRate`] with the `SimpleRate` window size: the
//! sum of the samples over the time since the oldest sample started, and
//! never over less than one sample, so a first burst is not measured over a
//! near-zero span. The quota is exceeded while that rate is above the bound.
//! A rate equal to the bound is within it (`Quota.acceptable` is
//! `value <= bound`).

use {
    crate::{
        QuotaWindow,
        sampled_rate::{SampledRate, WindowSize},
    },
    krabka_units::prelude::{ByteRate, ByteRateExt},
    num_traits::ToPrimitive as _,
    qubit_clock::{MonotonicClock, StdMonotonicClock},
    std::sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};

/// What the quota's lock guards: the bound and the measured rate.
#[derive(Debug)]
struct QuotaState {
    /// Bytes per second, `None` when no throttle rate is configured.
    bound: Option<f64>,
    rate: SampledRate,
}

/// The KIP-73 quota of one kind of throttled replica traffic. See the module
/// documentation.
#[derive(derive_more::Debug)]
pub struct ReplicationQuota {
    #[debug("{:?}", *self.lock_state())]
    state: Mutex<QuotaState>,
    /// Whether a bound is set, for the unthrottled fast path of
    /// [`Self::quota_exceeded`]. Written only while `state` is locked.
    #[debug(skip)]
    bounded: AtomicBool,
    #[debug(skip)]
    clock: Arc<dyn MonotonicClock>,
}

impl ReplicationQuota {
    /// An unbounded quota on a caller-supplied clock, with the sample
    /// `window` its `*.quota.window.*` configs set. Tests pass a
    /// [`qubit_clock::ManualMonotonicClock`].
    #[must_use]
    pub fn with_window(clock: Arc<dyn MonotonicClock>, window: QuotaWindow) -> Self {
        Self {
            state: Mutex::new(QuotaState {
                bound: None,
                rate: SampledRate::over(window, WindowSize::Simple),
            }),
            bounded: AtomicBool::new(false),
            clock,
        }
    }

    /// An unbounded quota on a caller-supplied clock, with Kafka's default
    /// window.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        Self::with_window(clock, QuotaWindow::KAFKA_DEFAULT)
    }

    /// An unbounded quota on the system's monotonic clock, with Kafka's
    /// default window of 11 samples of 1 second.
    #[must_use]
    pub fn new() -> Self { Self::with_clock(Arc::new(StdMonotonicClock::new())) }

    /// Kafka's `ReplicationQuotaManager.updateQuota`: sets the bound, or
    /// removes it with `None`, and keeps the traffic already measured.
    pub fn set_bound(&self, bound: Option<ByteRate>) {
        let mut state = self.lock_state();
        state.bound = bound.map(ByteRateExt::bytes_per_sec_f64);
        self.bounded.store(state.bound.is_some(), Relaxed);
    }

    /// The bound in force, `None` when the traffic is unthrottled.
    #[must_use]
    pub fn bound(&self) -> Option<ByteRate> {
        self.lock_state()
            .bound
            .map(ByteRate::from_bytes_per_sec_f64)
    }

    /// Kafka's `ReplicationQuotaManager.isQuotaExceeded`: whether the measured
    /// rate is above the bound right now. A quota without a bound is never
    /// exceeded.
    #[must_use]
    pub fn quota_exceeded(&self) -> bool {
        if !self.bounded.load(Relaxed) {
            return false;
        }
        let now_ms = self.now_ms();
        let mut state = self.lock_state();
        let rate = state.rate.measure(now_ms);
        // `Quota.acceptable` for an upper bound is `value <= bound`.
        state.bound.is_some_and(|bound| rate > bound)
    }

    /// Kafka's `ReplicationQuotaManager.record`: adds `bytes` to the measured
    /// rate whether or not the quota is exceeded, bound or no bound.
    pub fn record(&self, bytes: u64) {
        let now_ms = self.now_ms();
        // Kafka records the long byte count as a double.
        let bytes = bytes.to_f64().unwrap_or(f64::INFINITY);
        self.lock_state().rate.record(bytes, now_ms);
    }

    /// The measured rate right now, Kafka's `byte-rate` metric of the quota.
    #[must_use]
    pub fn measured_rate(&self) -> ByteRate {
        let now_ms = self.now_ms();
        ByteRate::from_bytes_per_sec_f64(self.lock_state().rate.measure(now_ms))
    }

    /// The clock reading in whole milliseconds, Kafka's `time.milliseconds()`.
    fn now_ms(&self) -> i64 {
        let millis = self.clock.now().elapsed_since_origin().as_millis();
        i64::try_from(millis).unwrap_or(i64::MAX)
    }

    /// Locks the state. A poisoned lock is still used: no critical section
    /// leaves the state half updated.
    fn lock_state(&self) -> MutexGuard<'_, QuotaState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for ReplicationQuota {
    fn default() -> Self { Self::new() }
}
