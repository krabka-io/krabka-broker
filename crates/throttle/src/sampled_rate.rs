//! [`SampledRate`], a port of Kafka's `SampledStat` with its `WindowedSum`,
//! measured as a `Rate` or as a `SimpleRate`.
//!
//! Both of the broker's sampled quotas measure with it: the KIP-73
//! [`ReplicationQuota`](crate::ReplicationQuota) as a `SimpleRate`, and the
//! KIP-13 and KIP-124 [`ClientQuota`](crate::ClientQuota) as a `Rate`. The
//! port keeps Kafka's quirks, so that the moment a quota closes and reopens
//! matches a JVM broker:
//!
//! * The stat keeps up to `samples + 1` samples, one more than the configured
//!   count, for the sample that overlaps the start of the monitored window.
//! * A record lands in the current sample, or starts the next one once the
//!   current sample has been open for a whole window.
//! * A sample whose last event is `samples * window` old is reset to empty and
//!   restarted at the reading's time.
//! * The rate is the sum of the samples divided by a window size. The window
//!   size starts at the time since the oldest sample started, and
//!   [`WindowSize`] says how a young window is padded.
//!
//! Kafka creates a quota's sensor on first use, so the first sample starts at
//! whichever comes first, a record or a reading. The port does the same.

use {
    krabka_units::prelude::{Time, TimeExt as _},
    num_traits::ToPrimitive as _,
    qubit_clock::MonotonicClock,
    std::{
        num::NonZeroU32,
        sync::{Mutex, MutexGuard, PoisonError},
    },
};

/// Kafka's default sample count: `quota.window.num`,
/// `replication.quota.window.num` and
/// `alter.log.dirs.replication.quota.window.num`
/// (`QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT`).
const DEFAULT_SAMPLES: u32 = 11;

/// Kafka's default sample length: `quota.window.size.seconds` and its
/// replication twins (`QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT`), in
/// milliseconds.
const DEFAULT_WINDOW_MS: i64 = 1_000;

/// The sample window of a sampled quota.
///
/// It is how many samples the quota keeps and how long each one is: Kafka's
/// `quota.window.num` and `quota.window.size.seconds` for a
/// [`ClientQuota`](crate::ClientQuota), and `replication.quota.window.*` or
/// `alter.log.dirs.replication.quota.window.*` for a
/// [`ReplicationQuota`](crate::ReplicationQuota).
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

/// The clock reading in whole milliseconds, Kafka's `time.milliseconds()`.
pub fn now_ms(clock: &dyn MonotonicClock) -> i64 {
    let millis = clock.now().elapsed_since_origin().as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Locks a quota's state. A poisoned lock is still used: no quota's critical
/// section leaves its state half updated.
pub fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How a [`SampledRate`] sizes a window that has not yet run its full length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowSize {
    /// Kafka's `SimpleRate.windowSize`: the elapsed time, and never less than
    /// one sample. The replication quotas measure this way.
    Simple,
    /// Kafka's `Rate.windowSize`: the elapsed time, padded with whole samples
    /// until it spans `samples - 1` of them, and never less than 1 ms. The
    /// client quotas measure this way, so a new client is measured as if it
    /// had been quiet for the rest of the window.
    Padded,
}

/// One sample of Kafka's `SampledStat`: the value recorded while it was
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

/// Kafka's `Rate` or `SimpleRate` over a `WindowedSum`, with its
/// `MetricConfig` window, as a pure value over millisecond readings.
#[derive(Debug, Clone, PartialEq)]
pub struct SampledRate {
    samples: Vec<Sample>,
    current: usize,
    num_samples: u32,
    window_ms: i64,
    window_size: WindowSize,
}

impl SampledRate {
    pub fn new(num_samples: u32, window_ms: i64, window_size: WindowSize) -> Self {
        Self {
            samples: Vec::new(),
            current: 0,
            num_samples,
            window_ms,
            window_size,
        }
    }

    /// An empty stat over `window`.
    pub fn over(window: QuotaWindow, window_size: WindowSize) -> Self {
        Self::new(window.samples, window.sample_ms, window_size)
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
    /// `SampledStat.current` creates the first sample on first use. A negative
    /// value takes back part of what was recorded, as Kafka's
    /// `unrecordQuotaSensor` records `-value`.
    pub fn record(&mut self, value: f64, now_ms: i64) {
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
            sample.value += value;
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

    /// `Rate.windowSize` or `SimpleRate.windowSize`, in milliseconds, after a
    /// purge.
    pub fn window_size_ms(&mut self, now_ms: i64) -> i64 {
        self.purge(now_ms);
        let elapsed_ms = now_ms - self.oldest_start(now_ms);
        match self.window_size {
            WindowSize::Simple => elapsed_ms.max(self.window_ms),
            WindowSize::Padded => {
                let full_windows = elapsed_ms / self.window_ms;
                let min_full_windows = i64::from(self.num_samples) - 1;
                let padded = if full_windows < min_full_windows {
                    elapsed_ms + (min_full_windows - full_windows) * self.window_ms
                } else {
                    elapsed_ms
                };
                padded.max(1)
            }
        }
    }

    /// `Rate.measure`: the windowed sum over the window size, per second.
    pub fn measure(&mut self, now_ms: i64) -> f64 {
        self.purge(now_ms);
        let total: f64 = self.samples.iter().map(|sample| sample.value).sum();
        // `Rate.convert` for `TimeUnit.SECONDS`: milliseconds over 1000.
        let window_size_secs = self
            .window_size_ms(now_ms)
            .to_f64()
            .unwrap_or(f64::INFINITY)
            / 1000.0;
        total / window_size_secs
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    const SAMPLES: u32 = DEFAULT_SAMPLES;
    const WINDOW_MS: i64 = DEFAULT_WINDOW_MS;

    /// Kafka creates the sensor on first use, so a quota check before any
    /// traffic starts the first sample, and a later burst is measured over
    /// the time since that check rather than over one window.
    #[yare::parameterized(
        checked_first = { true, 600.0 },
        recorded_first = { false, 3_000.0 },
    )]
    fn the_first_sample_starts_at_first_use(checked_first: bool, want: f64) {
        let mut rate = SampledRate::new(SAMPLES, WINDOW_MS, WindowSize::Simple);
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
        let mut rate = SampledRate::new(SAMPLES, WINDOW_MS, WindowSize::Simple);
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
        let mut rate = SampledRate::new(SAMPLES, WINDOW_MS, WindowSize::Simple);
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

    /// Kafka's `Rate.windowSize` pads a young window to `samples - 1` whole
    /// samples and keeps the part sample on top, so a window 2.5 s old with
    /// 11 samples is 10.5 s, and one 10.5 s old is left as it is. A window
    /// with one sample is never padded, and a reading at its very start is
    /// 1 ms, not 0.
    #[yare::parameterized(
        young = { SAMPLES, 2_500, 10_500 },
        at_the_start = { SAMPLES, 0, 10_000 },
        exactly_padded = { SAMPLES, 10_000, 10_000 },
        past_the_padding = { SAMPLES, 10_500, 10_500 },
        one_sample_at_its_start = { 1, 0, 1 },
        one_sample_part_way = { 1, 400, 400 },
    )]
    fn a_padded_window_spans_samples_minus_one_whole_samples(
        samples: u32,
        elapsed_ms: i64,
        want_ms: i64,
    ) {
        let mut rate = SampledRate::new(samples, WINDOW_MS, WindowSize::Padded);
        rate.record(1.0, 0);
        // Keep the first sample alive past the purge age of a one-sample
        // window.
        if elapsed_ms > 0 {
            rate.record(0.0, elapsed_ms);
        }

        check!(rate.window_size_ms(elapsed_ms) == want_ms);
    }

    /// The simple window is never shorter than one sample, the padded one
    /// starts at `samples - 1` of them: 1000 bytes at once read 1000 B/s and
    /// 100 B/s.
    #[yare::parameterized(
        simple = { WindowSize::Simple, 1_000.0 },
        padded = { WindowSize::Padded, 100.0 },
    )]
    fn a_first_burst_is_measured_over_the_window_size(window_size: WindowSize, want: f64) {
        let mut rate = SampledRate::new(SAMPLES, WINDOW_MS, window_size);
        rate.record(1_000.0, 0);

        let measured = rate.measure(0);
        check!((measured - want).abs() < 1.0e-9, "{measured}");
    }
}
