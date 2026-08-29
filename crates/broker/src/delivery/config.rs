//! Tunables of the delivery scheduler.
//!
//! The struct carries the two injected time sources as well as the two
//! durations, so a test drives the scheduler on a mock timeline instead of
//! wall-clock time. [`Default`] wires the system clock and the system sleeper.

use std::sync::Arc;

use krabka_units::{Time, millis, secs};
use qubit_clock::{
    Clock, SystemClock,
    sleep::{AsyncSleeper, SystemSleeper},
};

/// How the broker-wide delivery scheduler paces itself, and where it reads
/// time.
///
/// The struct is [`Clone`] but neither [`Debug`] nor [`PartialEq`], because the
/// two injected trait objects are neither.
#[derive(Clone)]
pub(crate) struct DeliveryConfig {
    /// Longest the scheduler sleeps when no partition it leads has a batch
    /// waiting, and the cap on every sleep.
    ///
    /// It bounds how long a newly scheduled partition, or a produce that lands
    /// a deadline further out than the instant the task sleeps on, stays
    /// undiscovered. The [`waker`](crate::delivery::waker) covers the deadlines
    /// that fall *before* that instant, so this value only has to be short
    /// enough to keep discovery honest, not short enough to be prompt.
    pub(crate) idle_sleep: Time,

    /// Shortest sleep the scheduler arms.
    ///
    /// A deadline that is already in the past would otherwise ask for a
    /// zero-length sleep, and a partition whose watermark cannot advance for an
    /// unrelated reason would spin the task. The floor turns that into a slow
    /// retry.
    pub(crate) min_sleep: Time,

    /// Wall clock the scheduler reads to decide which batches are due.
    ///
    /// Production uses [`qubit_clock::SystemClock`]. Tests inject a
    /// [`qubit_clock::MockClock`], so the activation boundary is an assertion
    /// and not a race against real time.
    pub(crate) clock: Arc<dyn Clock>,

    /// Relative sleeper that drives the scheduler's cadence. Production uses
    /// [`qubit_clock::sleep::SystemSleeper`]. Tests inject a
    /// [`qubit_clock::sleep::MockSleeper`] on the same timeline as
    /// [`Self::clock`].
    pub(crate) sleeper: Arc<dyn AsyncSleeper>,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            idle_sleep: secs(1),
            min_sleep: millis(1),
            clock: Arc::new(SystemClock::new()),
            sleeper: Arc::new(SystemSleeper::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::convert::TimeExt as _;

    use super::*;

    #[test]
    fn the_defaults_are_the_documented_values() {
        let config = DeliveryConfig::default();
        check!(config.idle_sleep == secs(1));
        check!(config.min_sleep == millis(1));
    }

    #[test]
    fn the_sleep_floor_is_below_the_idle_bound() {
        let config = DeliveryConfig::default();
        check!(config.min_sleep.millis_i64() < config.idle_sleep.millis_i64());
    }
}
