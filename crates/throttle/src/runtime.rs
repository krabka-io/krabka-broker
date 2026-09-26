//! The concurrent [`TokenBucket`] runtime around the pure
//! [`plan_consume`](krabka_verified::throttle::plan_consume) arithmetic.
//!
//! This module holds the bucket's state, the lock that serializes every
//! update of it, and the injected [`MonotonicClock`].
//!
//! # Concurrency design
//!
//! The four values a consume reads and writes, `{rate, burst, available,
//! last_refill}`, live together in one [`BucketState`] behind one [`Mutex`].
//! [`TokenBucket::try_consume`] and [`TokenBucket::set_token_rate_with_burst`]
//! each hold that lock for their whole read-compute-write, so every consume and
//! every reset is one indivisible step on the group:
//!
//! * A reset can never land between a consume's reads and its writes, so a
//!   consume can never commit a balance computed under the old burst on top of
//!   the new one.
//! * A consume advances `last_refill` and adds the matching refill to
//!   `available` in the same critical section, so time a consume claims is
//!   always turned into tokens or capped away by the burst. It is never dropped.
//!
//! An earlier lock-free design sampled a seqlock generation, claimed the refill
//! with a compare-and-exchange on `last_refill`, re-checked the generation,
//! then committed `available` with a second compare-and-exchange. It had both
//! failures above: a whole reset could run between the generation re-check and
//! the commit and store a value equal to the one the commit expected, and a
//! failed commit threw away the refill its earlier claim had taken. Two words
//! cannot be committed by one `u64` compare-and-exchange, so the group takes a
//! lock. Kafka makes the same choice: `Sensor.record` updates its quota stats
//! inside `synchronized` blocks. The critical section is a clock read and a
//! few integer operations.
//!
//! One value is read outside the lock. `rate_per_sec` mirrors the locked
//! rate, and it is written only while the lock is held, so the unthrottled
//! fast path in `try_consume` and [`TokenBucket::token_rate`] skip the lock.
//! A fast-path read that sees `0` takes effect at that read, which orders the
//! consume before any reset that is still in flight. A non-zero read proves
//! nothing: the consume takes the lock and re-reads the rate there.
//!
//! The stateright model in `tests/bucket_model.rs` checks this design, and
//! shows that the lock-free design it replaces breaks both properties.
//!
//! The bucket's own operations live in submodules that each add an
//! `impl TokenBucket` block: [`self::rate`] holds the rate and burst
//! accessors together with the reset that publishes them, and
//! [`self::consume`] holds `try_consume`. [`self::state`] holds the
//! broker-wide [`ThrottleState`] bundle.

use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU64, Ordering::Relaxed},
};

use qubit_clock::{MonotonicClock, StdMonotonicClock};

mod consume;
mod rate;
mod state;

pub use self::state::ThrottleState;

/// Reads the injected clock's nanoseconds elapsed since its origin as a `u64`.
///
/// The refill arithmetic uses **differences** of this value only, so the
/// absolute anchor does not matter. The span starts at zero when the clock is
/// created and needs about 584 years to overflow `u64`, so it is a strictly
/// safer anchor than the wall-clock epoch it replaces, which already stood at
/// about 1.75e18 ns.
#[inline]
fn clock_nanos(clock: &dyn MonotonicClock) -> u64 {
    u64::try_from(clock.now().elapsed_since_origin().as_nanos())
        .expect("nanoseconds elapsed since the clock's origin must fit in u64")
}

/// The group every consume and every reset reads and writes as one unit.
///
/// It lives behind [`TokenBucket::state`]. `available <= burst` holds whenever
/// the lock is free, and `last_refill_nanos` never moves backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BucketState {
    /// Tokens per second. `0` means no limit.
    rate_per_sec: u64,
    /// The most tokens the bucket holds.
    burst: u64,
    /// Tokens the bucket can grant now.
    available: u64,
    /// The clock reading up to which elapsed time has become tokens.
    last_refill_nanos: u64,
}

pub struct TokenBucket {
    /// The group, and the lock that makes each consume and each reset one
    /// indivisible step on it. See the module documentation.
    state: Mutex<BucketState>,
    /// A copy of `state.rate_per_sec` for the unthrottled fast path. It is
    /// written only while `state` is locked, right after the locked copy.
    rate_per_sec: AtomicU64,
    /// Monotonic time source. The caller injects it, so tests can drive
    /// refills deterministically with a [`qubit_clock::ManualMonotonicClock`]
    /// instead of sleeping.
    clock: Arc<dyn MonotonicClock>,
}

impl std::fmt::Debug for TokenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = *self.lock_state();
        f.debug_struct("TokenBucket")
            .field("rate_per_sec", &state.rate_per_sec)
            .field("burst", &state.burst)
            .field("available", &state.available)
            .field("last_refill_nanos", &state.last_refill_nanos)
            .finish_non_exhaustive()
    }
}

impl TokenBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(StdMonotonicClock::new()))
    }

    /// Constructs a bucket backed by a caller-supplied [`MonotonicClock`].
    ///
    /// Production code uses [`TokenBucket::new`], which supplies a
    /// [`StdMonotonicClock`]. Tests pass a
    /// [`qubit_clock::ManualMonotonicClock`], so refill windows advance by an
    /// exact, controlled amount instead of by wall-clock sleeping.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        let state = BucketState {
            rate_per_sec: 0,
            burst: 0,
            available: 0,
            last_refill_nanos: clock_nanos(&*clock),
        };
        Self {
            state: Mutex::new(state),
            rate_per_sec: AtomicU64::new(0),
            clock,
        }
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        clock_nanos(&*self.clock)
    }

    /// Locks the group.
    ///
    /// A poisoned lock is still used. Every critical section computes its new
    /// values before it stores any of them, and nothing between those stores
    /// can panic, so a panicking holder leaves the group as it found it.
    fn lock_state(&self) -> MutexGuard<'_, BucketState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The rate the unthrottled fast path sees, read without the lock.
    #[inline]
    fn fast_path_rate(&self) -> u64 {
        self.rate_per_sec.load(Relaxed)
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
        check!(s.contains("rate_per_sec: 100"));
        check!(s.contains("burst: 200"));
        check!(s.contains("available: 200"));
        check!(s.contains("last_refill_nanos"));
    }
}
