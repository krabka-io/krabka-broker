//! [`ThrottleState`], the broker-wide bundle of [`TokenBucket`]s that meters
//! replica traffic and intra-broker log directory moves.
//!
//! It is its own module because it is a container of buckets rather than part
//! of the bucket itself.

use {
    super::TokenBucket,
    qubit_clock::{MonotonicClock, StdMonotonicClock},
    std::sync::Arc,
};

/// Broker-wide throttle state for replica traffic and intra-broker log moves.
#[derive(Debug)]
pub struct ThrottleState {
    pub leader_out: Arc<TokenBucket>,
    pub follower_in: Arc<TokenBucket>,
    pub alter_log_dirs: Arc<TokenBucket>,
}

impl ThrottleState {
    #[must_use]
    pub fn new() -> Self { Self::with_clock(Arc::new(StdMonotonicClock::new())) }

    /// Builds the three buckets, unthrottled, on one caller-supplied clock.
    ///
    /// Production uses [`ThrottleState::new`], which supplies a
    /// [`StdMonotonicClock`]. A test passes a
    /// [`qubit_clock::ManualMonotonicClock`] to drive every bucket's refill
    /// with one controlled timeline.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            leader_out: Arc::new(TokenBucket::with_clock(Arc::clone(&clock))),
            follower_in: Arc::new(TokenBucket::with_clock(Arc::clone(&clock))),
            alter_log_dirs: Arc::new(TokenBucket::with_clock(clock)),
        }
    }
}

impl Default for ThrottleState {
    fn default() -> Self { Self::new() }
}
