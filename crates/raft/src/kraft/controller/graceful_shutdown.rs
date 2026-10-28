//! The engine side of a graceful shutdown: hand the leadership over, then
//! wait, within a bound, for the quorum to elect a successor.
//!
//! Kafka's `KafkaRaftClientDriver.initiateShutdown` calls
//! `KafkaRaftClient.shutdown(5000)`. While that shutdown is pending, the poll
//! loop resigns a leader, keeps a resigned or unattached voter where it is,
//! and completes the shutdown as soon as a remote leader is known, or when the
//! five seconds run out. A leader that simply stopped would leave its followers
//! to notice through their fetch watchdog, which takes one fetch timeout, and
//! then to elect.

use {
    super::{Engine, timing::whole_millis},
    krabka_units::prelude::Time,
    tokio::time::Duration,
};

/// Kafka's graceful shutdown bound: `KafkaRaftClientDriver.initiateShutdown`
/// gives `KafkaRaftClient.shutdown` 5000 ms.
const KAFKA_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Numerator of the fetch timeouts a graceful shutdown waits: two and a half,
/// the ratio of Kafka's 5000 ms bound to its 2000 ms default fetch timeout.
const WINDOW_FETCH_TIMEOUTS_NUM: u64 = 5;
/// Denominator of [`WINDOW_FETCH_TIMEOUTS_NUM`].
const WINDOW_FETCH_TIMEOUTS_DEN: u64 = 2;

/// How long a graceful shutdown waits for a successor.
///
/// Kafka's five seconds, or two and a half fetch timeouts when that is
/// shorter. Kafka's five seconds are two and a half of its default two-second
/// fetch timeouts, so the default fetch timeout keeps Kafka's bound exactly. A
/// successor named first in `EndQuorumEpoch` elects at once, well inside that
/// room. A quorum configured with a short fetch timeout does not hold every stop
/// of its last voter for five seconds.
pub fn graceful_shutdown_window(fetch_timeout: Time) -> Duration {
    Duration::from_millis(
        whole_millis(fetch_timeout)
            .saturating_mul(WINDOW_FETCH_TIMEOUTS_NUM)
            .div_euclid(WINDOW_FETCH_TIMEOUTS_DEN),
    )
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
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::millis,
        rstest::fixture,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    struct ShutdownWindow {
        fetch_timeout: Option<Time>,
        window: Option<Duration>,
    }

    #[fixture]
    fn shutdown() -> ShutdownWindow {
        // Start this scenario with fresh inputs and observations.

        ShutdownWindow {
            fetch_timeout: None,
            window: None,
        }
    }

    #[given("a KRaft fetch timeout of {timeout_ms} ms")]
    fn configured_timeout(shutdown: &mut ShutdownWindow, timeout_ms: u32) {
        shutdown.fetch_timeout = Some(millis(timeout_ms));
    }

    #[when("the controller chooses its graceful shutdown window")]
    fn choose_shutdown_window(shutdown: &mut ShutdownWindow) {
        shutdown.window = Some(graceful_shutdown_window(
            shutdown
                .fetch_timeout
                .expect("a fetch timeout was configured"),
        ));
    }

    #[then("the graceful shutdown window is {window_ms} ms")]
    fn bounded_shutdown_window(shutdown: &ShutdownWindow, window_ms: u64) {
        check!(shutdown.window == Some(Duration::from_millis(window_ms)));
    }

    scenarios!("tests/features/migrated/src/kraft/controller/graceful_shutdown.feature", fixtures = [shutdown: ShutdownWindow]);
}
