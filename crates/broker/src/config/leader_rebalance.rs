//! KIP-460 automatic leader rebalancing: the cadence and the imbalance
//! threshold the ticker reads, and the bounds they must stay inside.

use krabka_units::{
    Ratio, Time,
    convert::{RatioExt, TimeExt},
};

use crate::{BrokerError, config::BrokerConfig};

impl BrokerConfig {
    pub(super) fn validate_leader_rebalance(&self) -> Result<(), BrokerError> {
        if self.leader_imbalance_check_interval <= <Time as TimeExt>::ZERO {
            return Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 });
        }
        if self.leader_imbalance_per_broker > <Ratio as RatioExt>::ONE {
            // The error reports the operator-facing percentage, which is how
            // `leader.imbalance.per.broker.percentage` is written.
            return Err(BrokerError::InvalidLeaderRebalanceThreshold {
                percent: self.leader_imbalance_per_broker.percent_f64(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{minutes, percent};

    use super::*;

    #[test]
    fn auto_leader_rebalance_defaults_to_true_in_default() {
        let c = BrokerConfig::default();
        check!(c.features.auto_leader_rebalance_enable);
        check!(c.leader_imbalance_check_interval == minutes(5));
        check!(c.leader_imbalance_per_broker == percent(10));
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_false_in_for_tests() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(!c.features.auto_leader_rebalance_enable);
    }

    #[test]
    fn rebalance_zero_interval_rejected_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_check_interval: <Time as TimeExt>::ZERO,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 })
        ));
    }

    #[test]
    fn rebalance_threshold_over_100_rejected_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_per_broker: percent(101),
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceThreshold { percent })
                if (percent - 101.0).abs() < 1e-9
        ));
    }

    #[test]
    fn rebalance_threshold_100_is_allowed_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_per_broker: percent(100),
            ..BrokerConfig::default()
        };

        c.validate()
            .expect("100% leader imbalance threshold is the maximum valid value");
    }
}
