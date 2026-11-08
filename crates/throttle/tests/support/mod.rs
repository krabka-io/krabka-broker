//! Helpers the scenario binaries share.

use {qubit_clock::ManualMonotonicClock, std::time::Duration};

/// Moves a scenario's manual clock forward by `millis` milliseconds.
pub fn pass(clock: &ManualMonotonicClock, millis: u64) {
    let advanced = clock.advance(Duration::from_millis(millis));
    assert2::assert!(let Ok(()) = advanced, "manual time moves forward");
}
