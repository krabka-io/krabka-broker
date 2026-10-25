//! The monotonic time source the liveness registry reads.
//!
//! The registry never calls `Instant::now` directly. It asks a [`Clock`], so a
//! test replaces the real clock with one it advances by hand and the liveness
//! windows stay deterministic.

use std::time::Instant;
#[cfg(test)]
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// Monotonic time source for liveness tracking.
///
/// Production uses the real clock. Tests use a controllable clock, so
/// explicit advances drive the liveness windows instead of wall-clock
/// `std::thread::sleep`. Sleeps flake when the CI runner is loaded. The gap
/// between a re-seed and the next `tick` can then exceed a short timeout and
/// mark a broker dead by mistake.
pub(super) enum Clock {
    Real,
    #[cfg(test)]
    Test(Arc<TestClockInner>),
}

impl Clock {
    pub(super) fn now(&self) -> Instant {
        match self {
            Self::Real => Instant::now(),
            #[cfg(test)]
            Self::Test(inner) => {
                inner.base + Duration::from_nanos(inner.offset_nanos.load(Ordering::Relaxed))
            }
        }
    }
}

#[cfg(test)]
pub(super) struct TestClockInner {
    base: Instant,
    offset_nanos: AtomicU64,
}

/// Test handle for the controllable [`Clock`]. It shares its inner state with
/// the `Clock::Test` handed to [`ControllerLivenessState::with_clock`], so the
/// liveness state under test observes every `advance`. Tests in other modules
/// reach it through [`ControllerLivenessState::with_test_clock`].
#[cfg(test)]
pub struct TestClock(Arc<TestClockInner>);

#[cfg(test)]
impl TestClock {
    pub(crate) fn new() -> Self {
        Self(Arc::new(TestClockInner {
            base: Instant::now(),
            offset_nanos: AtomicU64::new(0),
        }))
    }

    pub(crate) fn advance(&self, by: Duration) {
        self.0.offset_nanos.fetch_add(
            u64::try_from(by.as_nanos()).expect("advance fits u64 nanos"),
            Ordering::Relaxed,
        );
    }

    pub(super) fn clock(&self) -> Clock { Clock::Test(Arc::clone(&self.0)) }
}
