//! The concurrent [`TokenBucket`] runtime around the pure [`plan_consume`](crate::plan_consume)
//! arithmetic.
//!
//! This module holds the atomics, the seqlock generation protocol, and the
//! injected [`NanoClock`]. It is separate from `lib.rs` so that the Creusot
//! verifier sees the pure kernel only. Creusot cannot translate the atomics and
//! `dyn` trait objects that this module uses.
//!
//! The bucket's own operations live in submodules that each add an
//! `impl TokenBucket` block: [`self::rate`] holds the rate and burst
//! accessors together with the seqlock write section that publishes them, and
//! [`self::consume`] holds the `try_consume` CAS loop that reads them.
//! [`self::state`] holds the broker-wide [`ThrottleState`] bundle. The fields
//! below stay in this module so every one of those submodules, as a
//! descendant, can reach them.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

use qubit_clock::{NanoClock, NanoMonotonicClock};

mod consume;
mod rate;
mod state;

pub use self::state::ThrottleState;

/// Reads the injected clock's current epoch-nanoseconds as a `u64`.
///
/// The refill arithmetic uses **differences** of this value only, so the
/// absolute anchor does not matter. A wall-clock-anchored epoch, about 1.75e18
/// ns today, and a mock timeline anchored at the Unix epoch both fit in `u64`.
#[inline]
fn clock_nanos(clock: &dyn NanoClock) -> u64 {
    u64::try_from(clock.nanos()).expect("clock nanoseconds must fit in u64")
}

pub struct TokenBucket {
    rate_per_sec: AtomicU64,
    burst: AtomicU64,
    available: AtomicU64,
    last_refill_nanos: AtomicU64,
    /// Seqlock generation that guards the `{rate, burst, available,
    /// last_refill}` group. `set_token_rate_with_burst` makes it odd while it
    /// writes and even when it is quiescent. A consumer that reads an odd
    /// value, or a value that changed across its read-compute-commit, tries
    /// again. A stale `available` CAS thus never clobbers a straddled reset.
    /// See the stateright model in `tests/bucket_model.rs`.
    generation: AtomicU64,
    /// Monotonic nanosecond time source. The caller injects it, so tests can
    /// drive refills deterministically with a [`qubit_clock::MockClock`]
    /// instead of sleeping.
    clock: Arc<dyn NanoClock>,
}

impl std::fmt::Debug for TokenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenBucket")
            .field("rate_per_sec", &self.rate_per_sec.load(Relaxed))
            .field("burst", &self.burst.load(Relaxed))
            .field("available", &self.available.load(Relaxed))
            .field("last_refill_nanos", &self.last_refill_nanos.load(Relaxed))
            .field("generation", &self.generation.load(Relaxed))
            .finish_non_exhaustive()
    }
}

impl TokenBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(NanoMonotonicClock::new()))
    }

    /// Constructs a bucket backed by a caller-supplied [`NanoClock`].
    ///
    /// Production code uses [`TokenBucket::new`], which supplies a
    /// [`NanoMonotonicClock`]. Tests pass a [`qubit_clock::MockClock`], so
    /// refill windows advance by an exact, controlled amount instead of by
    /// wall-clock sleeping.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn NanoClock>) -> Self {
        let last_refill_nanos = AtomicU64::new(clock_nanos(&*clock));
        Self {
            rate_per_sec: AtomicU64::new(0),
            burst: AtomicU64::new(0),
            available: AtomicU64::new(0),
            last_refill_nanos,
            generation: AtomicU64::new(0),
            clock,
        }
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        clock_nanos(&*self.clock)
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, bytes_per_sec};

    use super::*;

    #[test]
    fn debug_renders_bucket_fields() {
        let b = TokenBucket::new();
        b.set_byte_rate_with_burst(bytes_per_sec(100), bytes(200));
        let s = format!("{b:?}");
        check!(s.contains("TokenBucket"));
        check!(s.contains("rate_per_sec"));
        check!(s.contains("burst"));
        check!(s.contains("last_refill_nanos"));
    }
}
