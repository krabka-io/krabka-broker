//! [`ReplicationQuota`], the KIP-73 replication quota: Kafka's
//! `ReplicationQuotaManager` over a `SimpleRate`.
//!
//! The broker meters leader replication, follower replication and KIP-113 log
//! directory moves with one quota each. A reader asks [`ReplicationQuota::
//! quota_exceeded`] before it reads throttled traffic and calls
//! [`ReplicationQuota::record`] with the bytes it moved after, as Kafka's
//! fetchers ask `isQuotaExceeded` and call `record`.
//!
//! The measurement ports Kafka's `SampledStat` with its `WindowedSum` and the
//! `SimpleRate` window size, quirks included, so that the moment a quota
//! closes and reopens matches a JVM broker:
//!
//! * The quota keeps up to `samples + 1` samples, one more than the configured
//!   count, for the sample that overlaps the start of the monitored window.
//! * A record lands in the current sample, or starts the next one once the
//!   current sample has been open for a whole window.
//! * A sample whose last event is `samples * window` old is reset to empty and
//!   restarted at the reading's time.
//! * The rate is the sum of the samples divided by the time since the oldest
//!   sample started, and never by less than one window, so a first burst is
//!   not measured over a near-zero span.
//! * The quota is exceeded while that rate is above the bound. A rate equal to
//!   the bound is within it (`Quota.acceptable` is `value <= bound`).
//!
//! Kafka creates a quota's sensor on first use, from a record or from a
//! quota check, so the first sample starts at whichever comes first. The
//! port does the same.

use {
    krabka_units::prelude::{ByteRate, ByteRateExt, Time, TimeExt as _},
    num_traits::ToPrimitive as _,
    qubit_clock::{MonotonicClock, StdMonotonicClock},
    std::{
        num::NonZeroU32,
        sync::{
            Arc, Mutex, MutexGuard, PoisonError,
            atomic::{AtomicBool, Ordering::Relaxed},
        },
    },
};

/// Kafka's default `replication.quota.window.num` and
/// `alter.log.dirs.replication.quota.window.num`
/// (`QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT`).
const DEFAULT_SAMPLES: u32 = 11;

/// Kafka's default `replication.quota.window.size.seconds` and
/// `alter.log.dirs.replication.quota.window.size.seconds`
/// (`QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT`), in milliseconds.
const DEFAULT_WINDOW_MS: i64 = 1_000;

/// The sample window of a [`ReplicationQuota`].
///
/// It is how many samples the quota keeps and how long each one is: Kafka's
/// `replication.quota.window.num` and `replication.quota.window.size.seconds`,
/// or their `alter.log.dirs.replication.quota.window.*` twins for log
/// directory moves.
///
/// Kafka validates both as `atLeast(1)`, so neither can be zero. A sample
/// shorter than a millisecond is held as one millisecond, the resolution the
/// quota reads its clock at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaWindow {
    samples: u32,
    sample_ms: i64,
}

impl QuotaWindow {
    /// Kafka's default window: 11 samples of 1 second.
    pub const KAFKA_DEFAULT: Self = Self {
        samples: DEFAULT_SAMPLES,
        sample_ms: DEFAULT_WINDOW_MS,
    };

    /// A window of `samples` samples, each `sample` long.
    #[must_use]
    pub fn new(samples: NonZeroU32, sample: Time) -> Self {
        Self {
            samples: samples.get(),
            sample_ms: sample.millis_i64().max(1),
        }
    }

    /// The number of samples, `*.quota.window.num`.
    #[must_use]
    pub const fn samples(self) -> u32 { self.samples }

    /// The length of one sample, `*.quota.window.size.seconds`.
    #[must_use]
    pub fn sample(self) -> Time { Time::from_millis(self.sample_ms) }
}

impl Default for QuotaWindow {
    fn default() -> Self { Self::KAFKA_DEFAULT }
}

/// One sample of Kafka's `SampledStat`: the bytes recorded while it was
/// current, when it started, and when it last took a record.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Sample {
    start_ms: i64,
    last_event_ms: i64,
    value: f64,
}

impl Sample {
    const fn new(now_ms: i64) -> Self {
        Self {
            start_ms: now_ms,
            last_event_ms: now_ms,
            value: 0.0,
        }
    }
}

/// Kafka's `SimpleRate` over a `WindowedSum`, with its `MetricConfig` window,
/// as a pure value over millisecond readings.
#[derive(Debug, Clone, PartialEq)]
struct SampledRate {
    samples: Vec<Sample>,
    current: usize,
    num_samples: u32,
    window_ms: i64,
}

impl SampledRate {
    fn new(num_samples: u32, window_ms: i64) -> Self {
        Self {
            samples: Vec::new(),
            current: 0,
            num_samples,
            window_ms,
        }
    }

    /// `SampledStat.advance`: moves to the next of `samples + 1` slots, adding
    /// a sample while there are fewer and recycling the oldest after.
    fn advance(&mut self, now_ms: i64) {
        let slots = usize::try_from(self.num_samples)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        self.current = (self.current + 1) % slots;
        match self.samples.get_mut(self.current) {
            Some(recycled) => *recycled = Sample::new(now_ms),
            None => self.samples.push(Sample::new(now_ms)),
        }
    }

    /// `SampledStat.record` with `WindowedSum.update`. The event window is
    /// Kafka's default `Long.MAX_VALUE`, so only time completes a sample.
    /// `SampledStat.current` creates the first sample on first use.
    fn record(&mut self, bytes: f64, now_ms: i64) {
        if self.samples.is_empty() {
            self.samples.push(Sample::new(now_ms));
        }
        let complete = self
            .samples
            .get(self.current)
            .is_some_and(|sample| now_ms - sample.start_ms >= self.window_ms);
        if complete {
            self.advance(now_ms);
        }
        if let Some(sample) = self.samples.get_mut(self.current) {
            sample.value += bytes;
            sample.last_event_ms = now_ms;
        }
    }

    /// `SampledStat.purgeObsoleteSamples`: resets every sample whose last
    /// event is a whole monitored window old.
    fn purge(&mut self, now_ms: i64) {
        let expire_age = i64::from(self.num_samples).saturating_mul(self.window_ms);
        for sample in &mut self.samples {
            if now_ms - sample.last_event_ms >= expire_age {
                *sample = Sample::new(now_ms);
            }
        }
    }

    /// `SampledStat.oldest`: the earliest-started sample, creating the first
    /// sample if there is none.
    fn oldest_start(&mut self, now_ms: i64) -> i64 {
        if self.samples.is_empty() {
            self.samples.push(Sample::new(now_ms));
        }
        self.samples
            .iter()
            .map(|sample| sample.start_ms)
            .min()
            .unwrap_or(now_ms)
    }

    /// `Rate.measure` with `SimpleRate.windowSize`: the windowed sum over
    /// `max(now - oldest start, window)`, in bytes per second.
    fn measure(&mut self, now_ms: i64) -> f64 {
        self.purge(now_ms);
        let total: f64 = self.samples.iter().map(|sample| sample.value).sum();
        self.purge(now_ms);
        let elapsed_ms = now_ms - self.oldest_start(now_ms);
        let window_size_ms = elapsed_ms.max(self.window_ms);
        // `Rate.convert` for `TimeUnit.SECONDS`: milliseconds over 1000.
        let window_size_secs = window_size_ms.to_f64().unwrap_or(f64::INFINITY) / 1000.0;
        total / window_size_secs
    }
}

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
                rate: SampledRate::new(window.samples, window.sample_ms),
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

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Kafka creates the sensor on first use, so a quota check before any
    /// traffic starts the first sample, and a later burst is measured over
    /// the time since that check rather than over one window.
    #[yare::parameterized(
        checked_first = { true, 600.0 },
        recorded_first = { false, 3_000.0 },
    )]
    fn the_first_sample_starts_at_first_use(checked_first: bool, want: f64) {
        let mut rate = SampledRate::new(DEFAULT_SAMPLES, DEFAULT_WINDOW_MS);
        if checked_first {
            rate.measure(0);
        }
        rate.record(3_000.0, 5_000);

        let measured = rate.measure(5_000);
        check!((measured - want).abs() < 1.0e-9, "{measured}");
    }

    /// `SampledStat` keeps `samples + 1` slots. One byte a second for 13
    /// seconds wraps onto the first slot, and the reading at 12 s forgets the
    /// sample whose last event is 11 s old: 11 bytes over the 10 s since the
    /// oldest kept sample started.
    #[test]
    fn samples_wrap_after_one_spare_slot_and_expire_after_the_window() {
        let mut rate = SampledRate::new(DEFAULT_SAMPLES, DEFAULT_WINDOW_MS);
        for second in 0..=12 {
            rate.record(1.0, second * 1_000);
        }

        check!(rate.samples.len() == 12);
        check!(rate.current == 0);
        let measured = rate.measure(12_000);
        check!((measured - 1.1).abs() < 1.0e-9, "{measured}");
    }

    /// A quota checked just before each one-second record at the bound reads
    /// exactly the bound, so steady traffic at the bound is never throttled.
    #[test]
    fn steady_traffic_at_the_bound_reads_the_bound_before_each_record() {
        let mut rate = SampledRate::new(DEFAULT_SAMPLES, DEFAULT_WINDOW_MS);
        let mut readings = Vec::new();
        for second in 0..30 {
            if second > 0 {
                readings.push(rate.measure(second * 1_000));
            }
            rate.record(1_000.0, second * 1_000);
        }

        check!(
            readings.iter().all(|reading| *reading <= 1_000.0),
            "{readings:?}"
        );
    }
}
