//! Fixtures shared by the unit tests of the share-partition state machine.
//!
//! The concern modules under `state` each carry their own `#[cfg(test)] mod
//! tests`, and they take one clock origin and one lock duration from here, so a
//! test reads the same timings wherever it sits.

use std::time::{Duration, Instant};

pub(super) fn t0() -> Instant {
    Instant::now()
}

pub(super) const LOCK: Duration = Duration::from_secs(30);
