//! Shared wall-clock time helpers, and the two guards that every cadence loop
//! puts around its injected [`Timer`].
//!
//! The wall-clock half is the single source of truth for the `SystemTime →
//! UNIX_EPOCH → as_millis() → i64` sequence that the transaction, OAuth, and
//! delegation-token handlers use, and that every reader of an injected
//! [`WallClock`](qubit_clock::WallClock) narrows through. The helpers saturate
//! on overflow and on pre-epoch clock skew, and do not panic.
//!
//! The timer half is [`arm`] and [`fired`]. Registering a deadline is fallible
//! -- a timer backend reports [`TimeError`] when it cannot take a registration
//! or cannot see one through -- and so is the completion the registration
//! yields. A cadence loop has nothing left to do once its ticker cannot be
//! armed, and re-arming it in a loop would spin the task at full speed, so both
//! helpers log the failure against the task's name and report it as "stop".

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qubit_clock::{TimeError, Timer, TimerFuture};

/// Returns `instant` in milliseconds since the Unix epoch.
///
/// The value saturates to `0` if `instant` falls before the epoch. It
/// saturates to `i64::MAX` if the duration overflows `i64`, which is about
/// 292 million years from now and therefore safe in practice.
#[inline]
pub(crate) fn epoch_millis(instant: SystemTime) -> i64 {
    instant
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// This reads the system clock directly. A component that takes an injected
/// [`WallClock`](qubit_clock::WallClock) so a test can drive it reads
/// `epoch_millis(clock.now())` instead.
#[inline]
pub(crate) fn now_ms() -> i64 {
    epoch_millis(SystemTime::now())
}

/// Registers a deadline `delay` from now on `timer`, for the loop named
/// `task`.
///
/// `None` means the timer refused the registration, and the caller must stop:
/// the failure is already logged with `task` naming which cadence went away.
pub(crate) fn arm(timer: &dyn Timer, delay: Duration, task: &'static str) -> Option<TimerFuture> {
    match timer.after(delay) {
        Ok(future) => Some(future),
        Err(error) => {
            tracing::error!(%error, task, "could not arm the timer; stopping the task");
            None
        }
    }
}

/// Reports whether a deadline armed by [`arm`] completed, for the loop named
/// `task`.
///
/// `false` means the timer gave up on a registration it had accepted, and the
/// caller must stop for the same reason [`arm`] returning `None` does.
pub(crate) fn fired(outcome: Result<(), TimeError>, task: &'static str) -> bool {
    match outcome {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, task, "the armed timer failed; stopping the task");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

    use super::*;

    const TASK: &str = "a test loop";

    #[test]
    fn epoch_millis_reads_an_instant_and_saturates_outside_the_range() {
        let cases = [
            (UNIX_EPOCH, 0),
            // A sub-second remainder, so a conversion that truncated to whole
            // seconds would fail here rather than pass.
            (
                UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
                1_700_000_000_123,
            ),
            // Before the epoch: a backwards clock reads as the epoch itself
            // rather than as a negative timestamp.
            (UNIX_EPOCH - Duration::from_millis(1), 0),
            // Past `i64::MAX` milliseconds: saturates instead of wrapping.
            (UNIX_EPOCH + Duration::from_secs(1 << 60), i64::MAX),
        ];

        for (instant, want) in cases {
            check!(epoch_millis(instant) == want, "{instant:?}");
        }
    }

    #[test]
    fn now_ms_reads_the_system_clock() {
        let before = epoch_millis(SystemTime::now());
        let sampled = now_ms();
        let after = epoch_millis(SystemTime::now());

        check!(before <= sampled && sampled <= after);
    }

    #[test]
    fn arm_registers_a_deadline_the_timer_accepts() {
        let clock = ManualMonotonicClock::new_shared();
        let timer = clock.new_timer();

        check!(arm(&*timer, Duration::from_secs(1), TASK).is_some());
        check!(clock.pending_waiters() == 1);
    }

    /// A clock whose elapsed span has moved off its origin, so that a
    /// `Duration::MAX` delay overflows the deadline arithmetic and the timer
    /// refuses the registration.
    fn clock_past_its_origin() -> Arc<ManualMonotonicClock> {
        let clock = ManualMonotonicClock::new_shared();
        clock
            .advance(Duration::from_nanos(1))
            .expect("manual time moves forward");
        clock
    }

    #[test]
    fn arm_reports_a_deadline_the_timer_refuses() {
        let clock = clock_past_its_origin();
        let timer = clock.new_timer();

        check!(arm(&*timer, Duration::MAX, TASK).is_none());
        check!(clock.pending_waiters() == 0);
    }

    #[test]
    fn fired_separates_a_completed_deadline_from_a_failed_one() {
        let clock = clock_past_its_origin();
        // `expect_err` would need `Debug` on the success type, and a
        // `TimerFuture` is a boxed trait object that has none.
        let Err(refusal) = clock.new_timer().after(Duration::MAX) else {
            panic!("an overflowing delay must be refused");
        };

        check!(fired(Ok(()), TASK));
        check!(!fired(Err(refusal), TASK));
    }
}
