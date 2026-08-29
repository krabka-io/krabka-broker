//! The broker's log directories and the per-log IO policy: where partition
//! data lands, and the read budgets every hosted log inherits.

use std::path::PathBuf;

use crate::{BrokerError, config::BrokerConfig};

impl BrokerConfig {
    pub(super) fn validate_log_io_policy(&self) -> Result<(), BrokerError> {
        if self.log_config.read_buffer_cap <= krabka_units::bytes(0) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "log_read_buffer_cap must be positive".into(),
            ));
        }
        if self.log_config.timestamp_scan_window <= krabka_units::bytes(0) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "log_timestamp_scan_window must be positive".into(),
            ));
        }
        Ok(())
    }

    /// All log directories this broker stores partition data in, primary
    /// first and de-duplicated. This is the placement and `DescribeLogDirs`
    /// surface (KIP-113). The list excludes `__cluster_metadata`, which lives
    /// on [`log_dir`][Self::log_dir] only.
    #[must_use]
    pub fn all_log_dirs(&self) -> Vec<PathBuf> {
        let mut out = vec![self.log_dir.clone()];
        for d in &self.extra_log_dirs {
            if !out.contains(d) {
                out.push(d.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn all_log_dirs_keeps_primary_first_and_deduplicates_extras() {
        let primary = std::path::PathBuf::from("/data/primary");
        let extra = std::path::PathBuf::from("/data/extra");
        let mut c = BrokerConfig::for_tests(primary.clone());
        c.extra_log_dirs = vec![extra.clone(), primary.clone(), extra.clone()];

        assert!(c.all_log_dirs() == vec![primary, extra]);
    }
}
