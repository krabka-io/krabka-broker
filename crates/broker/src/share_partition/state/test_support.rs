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

/// A materialized zero-based window already held by m1 with the ordinary test lock.
pub(crate) fn acquired_state(end: i64) -> super::AcquisitionState {
    let mut state = super::AcquisitionState::new(krabka_log::Offset(0));
    acquire_window(&mut state, end, 10, false);
    state
}

/// Populate an existing fixture under the caller's lock, using its original record limit.
pub(crate) fn acquire_window(
    state: &mut super::AcquisitionState,
    end: i64,
    record_limit: i32,
    with_dlq: bool,
) {
    if with_dlq {
        state.set_dlq_enabled(true);
    }
    state.materialize(krabka_log::Offset(end), 100);
    let _ = state.acquire(
        "m1",
        record_limit,
        krabka_log::Offset(i64::MAX),
        t0(),
        LOCK,
        5,
    );
}

pub(crate) fn dlq_range(
    first: i64,
    last: i64,
    delivery_count: i16,
    cause: Option<super::DlqCause>,
) -> super::DlqRange {
    super::DlqRange {
        first: krabka_log::Offset(first),
        last: krabka_log::Offset(last),
        delivery_count,
        cause,
    }
}
