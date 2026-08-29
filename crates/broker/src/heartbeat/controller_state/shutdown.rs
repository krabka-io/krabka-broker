//! The controlled-shutdown intent a broker signals on its heartbeat.
//!
//! The set lives beside the liveness map but answers a different question, and
//! nothing here reads or writes a broker's liveness state, so it sits in its
//! own module.

use super::ControllerLivenessState;

impl ControllerLivenessState {
    /// Record whether `broker_id` is currently asking to shut down.
    /// `true` adds to the set. `false` removes from the set, which
    /// covers a broker that retracts the request. In practice the
    /// controller only clears state when it observes the broker dead.
    pub(crate) async fn set_wants_shutdown(&self, broker_id: u64, want: bool) {
        let mut set = self.wants_shutdown.lock().await;
        if want {
            set.insert(broker_id);
        } else {
            set.remove(&broker_id);
        }
    }

    /// Returns `true` if `broker_id` is currently in the wants-shutdown
    /// set.
    #[cfg(test)]
    async fn wants_shutdown(&self, broker_id: u64) -> bool {
        self.wants_shutdown.lock().await.contains(&broker_id)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn wants_shutdown_set_and_unset() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        assert!(!liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, true).await;
        assert!(liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, false).await;
        assert!(!liveness.wants_shutdown(5).await);
    }

    #[tokio::test]
    async fn wants_shutdown_is_per_broker() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        liveness.set_wants_shutdown(1, true).await;
        liveness.set_wants_shutdown(2, true).await;
        for (broker, want) in [(1, true), (2, true), (3, false)] {
            assert!(
                liveness.wants_shutdown(broker).await == want,
                "broker {broker}"
            );
        }
        liveness.set_wants_shutdown(1, false).await;
        assert!(!liveness.wants_shutdown(1).await);
        assert!(liveness.wants_shutdown(2).await);
    }
}
