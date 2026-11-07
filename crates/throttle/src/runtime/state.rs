//! [`ThrottleState`], the broker-wide bundle of [`ReplicationQuota`]s that
//! meters replica traffic and intra-broker log directory moves.

use {
    crate::ReplicationQuota,
    qubit_clock::{MonotonicClock, StdMonotonicClock},
    std::sync::Arc,
};

/// Broker-wide throttle state for replica traffic and intra-broker log moves.
///
/// It holds one quota each for `leader.replication.throttled.rate`,
/// `follower.replication.throttled.rate` and
/// `replica.alter.log.dirs.io.max.bytes.per.second`.
#[derive(Debug)]
pub struct ThrottleState {
    pub leader_out: Arc<ReplicationQuota>,
    pub follower_in: Arc<ReplicationQuota>,
    pub alter_log_dirs: Arc<ReplicationQuota>,
}

impl ThrottleState {
    /// Builds the three quotas, unbounded, on one caller-supplied clock.
    ///
    /// Production uses [`ThrottleState::new`], which supplies a
    /// [`StdMonotonicClock`]. A test passes a
    /// [`qubit_clock::ManualMonotonicClock`] to drive every quota's samples
    /// with one controlled timeline.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            leader_out: Arc::new(ReplicationQuota::with_clock(Arc::clone(&clock))),
            follower_in: Arc::new(ReplicationQuota::with_clock(Arc::clone(&clock))),
            alter_log_dirs: Arc::new(ReplicationQuota::with_clock(clock)),
        }
    }

    #[must_use]
    pub fn new() -> Self { Self::with_clock(Arc::new(StdMonotonicClock::new())) }
}

impl Default for ThrottleState {
    fn default() -> Self { Self::new() }
}
