//! The engine side of a graceful shutdown: hand the leadership over, then
//! wait, within a bound, for the quorum to elect a successor.
//!
//! Kafka's `KafkaRaftClientDriver.initiateShutdown` calls
//! `KafkaRaftClient.shutdown(5000)`. While that shutdown is pending, the poll
//! loop resigns a leader, keeps a resigned or unattached voter where it is,
//! and completes the shutdown as soon as a remote leader is known, or when the
//! five seconds run out. A leader that simply stopped would leave its followers
//! to notice through their fetch watchdog, which takes the configured number
//! of fetch timeouts, and then to elect.

use {
    super::{Engine, timing::election_timeout_ms},
    krabka_units::prelude::Time,
    tokio::time::Duration,
};

/// Kafka's graceful shutdown bound: `KafkaRaftClientDriver.initiateShutdown`
/// gives `KafkaRaftClient.shutdown` 5000 ms.
const KAFKA_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a graceful shutdown waits for a successor.
///
/// Kafka's five seconds, or one election timeout when that is shorter. A
/// successor named first in `EndQuorumEpoch` elects at once, so the election
/// timeout is room enough for it. The default election timeout is five
/// seconds, which keeps Kafka's bound. A quorum configured with a short timeout
/// does not hold every stop of its last voter for five seconds.
pub fn graceful_shutdown_window(election_timeout: Time) -> Duration {
    Duration::from_millis(election_timeout_ms(election_timeout))
        .min(KAFKA_GRACEFUL_SHUTDOWN_TIMEOUT)
}

impl Engine {
    /// One turn of a graceful shutdown: resign the leadership this replica
    /// holds, and report whether the shutdown is complete.
    ///
    /// The engine calls this after every turn while the shutdown is pending,
    /// so a candidate that wins its election during the wait resigns at once,
    /// as Kafka's `pollLeader` does with a shutdown set.
    pub fn step_graceful_shutdown(&mut self) -> bool {
        let prev_role = self.core.role().name();
        let actions = self.core.resign_for_shutdown(self.now());
        if !actions.is_empty() {
            tracing::info!(
                node = self.me.0,
                epoch = self.core.quorum_state().leader_epoch,
                "kraft: resigning the leadership for a graceful shutdown"
            );
            self.execute(actions);
            self.reconcile_timers(prev_role);
            self.publish_leader();
        }
        let complete = self.core.graceful_shutdown_complete();
        if complete {
            tracing::info!(
                node = self.me.0,
                leader = ?self.core.known_leader(),
                "kraft: graceful shutdown completed"
            );
        }
        complete
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, krabka_units::prelude::millis};

    #[yare::parameterized(
        short_test_timeout = { millis(500), Duration::from_millis(500) },
        default_timeout = { millis(5_000), Duration::from_secs(5) },
        longer_than_kafka = { millis(20_000), Duration::from_secs(5) },
    )]
    fn the_window_is_kafkas_five_seconds_or_one_election_timeout(
        election_timeout: Time,
        window: Duration,
    ) {
        check!(graceful_shutdown_window(election_timeout) == window);
    }
}
