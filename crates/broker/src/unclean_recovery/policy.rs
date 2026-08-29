//! The tunables the controller applies to one unclean recovery run.
//!
//! `RecoveryPolicy` carries the per-strategy response deadline, the job-queue
//! capacity, and the listener details the manager needs to reach a replica.
//! It is built once from `BrokerConfig` and read on every job.

use krabka_units::Time;

use super::BackgroundRecovery;
use crate::config_keys::RecoveryStrategy;

#[derive(Debug, Clone)]
pub(crate) struct RecoveryPolicy {
    pub aggressive_deadline: Time,
    pub balanced_deadline: Time,
    pub queue_capacity: usize,
    pub listener_protocol: krabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    /// KFC-9: what the URM does for a job that no operator approved.
    pub background: BackgroundRecovery,
}

impl RecoveryPolicy {
    pub(super) fn deadline(&self, strategy: RecoveryStrategy) -> Time {
        match strategy {
            RecoveryStrategy::Aggressive | RecoveryStrategy::None => self.aggressive_deadline,
            RecoveryStrategy::Balanced => self.balanced_deadline,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_audit::AuditLog;
    use krabka_units::millis;

    use super::*;
    use crate::config::BreakGlassConfig;

    #[test]
    fn recovery_policy_selects_configured_deadlines() {
        let policy = RecoveryPolicy {
            aggressive_deadline: millis(7),
            balanced_deadline: millis(19),
            queue_capacity: 3,
            listener_protocol: krabka_security::ListenerProtocol::Ssl,
            inter_broker_server_name: "broker.internal".to_string(),
            background: BackgroundRecovery::new(&BreakGlassConfig::default(), AuditLog::disabled()),
        };

        assert!(policy.deadline(RecoveryStrategy::Aggressive) == millis(7));
        assert!(policy.deadline(RecoveryStrategy::Balanced) == millis(19));
        assert!(policy.queue_capacity == 3);
        assert!(policy.listener_protocol == krabka_security::ListenerProtocol::Ssl);
        assert!(policy.inter_broker_server_name == "broker.internal");
    }
}
