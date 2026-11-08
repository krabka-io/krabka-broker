//! [`ThrottleState`], the broker-wide bundle of [`ReplicationQuota`]s that
//! meters replica traffic and intra-broker log directory moves.

use {
    crate::{QuotaWindow, ReplicationQuota},
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

#[bon::bon]
impl ThrottleState {
    /// Builds the three quotas, unbounded, on one clock.
    ///
    /// The leader-out and follower-in quotas take `replication_window`, Kafka's
    /// `replication.quota.window.*`. The log directory move quota takes
    /// `alter_log_dirs_window`, Kafka's
    /// `alter.log.dirs.replication.quota.window.*`, as Kafka's `QuotaManagers`
    /// builds them. Every argument defaults: the system's monotonic clock and
    /// Kafka's default window. A test passes a
    /// [`qubit_clock::ManualMonotonicClock`] to drive every quota's samples with
    /// one controlled timeline.
    #[builder]
    pub fn with_windows(
        #[builder(default = Arc::new(StdMonotonicClock::new()))] clock: Arc<dyn MonotonicClock>,
        #[builder(default)] replication_window: QuotaWindow,
        #[builder(default)] alter_log_dirs_window: QuotaWindow,
    ) -> Self {
        Self {
            leader_out: Arc::new(ReplicationQuota::with_window(
                Arc::clone(&clock),
                replication_window,
            )),
            follower_in: Arc::new(ReplicationQuota::with_window(
                Arc::clone(&clock),
                replication_window,
            )),
            alter_log_dirs: Arc::new(ReplicationQuota::with_window(clock, alter_log_dirs_window)),
        }
    }

    /// The three quotas with Kafka's default windows on one caller-supplied
    /// clock.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn MonotonicClock>) -> Self {
        Self::with_windows().clock(clock).call()
    }

    /// The three quotas with Kafka's default windows on the system's
    /// monotonic clock.
    #[must_use]
    pub fn new() -> Self { Self::with_windows().call() }
}

impl Default for ThrottleState {
    fn default() -> Self { Self::new() }
}
