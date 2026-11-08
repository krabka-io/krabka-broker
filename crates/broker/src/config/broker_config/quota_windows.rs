//! The sampling windows of the KIP-73 replication quotas, as
//! [`QuotaWindow`] values the throttle state is built from.

use {super::BrokerConfig, krabka_throttle::QuotaWindow, krabka_units::Time, std::num::NonZeroU32};

impl BrokerConfig {
    /// The window of the leader-out and follower-in replication quotas:
    /// Kafka's `replication.quota.window.num` samples of
    /// `replication.quota.window.size.seconds` each.
    #[must_use]
    pub fn replication_quota_window(&self) -> QuotaWindow {
        window(
            self.replication_quota_window_num,
            self.replication_quota_window_size,
        )
    }

    /// The window of the log-directory move quota: Kafka's
    /// `alter.log.dirs.replication.quota.window.num` samples of
    /// `alter.log.dirs.replication.quota.window.size.seconds` each.
    #[must_use]
    pub fn alter_log_dirs_replication_quota_window(&self) -> QuotaWindow {
        window(
            self.alter_log_dirs_replication_quota_window_num,
            self.alter_log_dirs_replication_quota_window_size,
        )
    }
}

/// A window of `samples` samples of `sample` each. Validation refuses a zero
/// sample count, so the one-sample floor never applies to a validated
/// configuration; it only keeps this function total.
fn window(samples: u32, sample: Time) -> QuotaWindow {
    QuotaWindow::new(NonZeroU32::new(samples).unwrap_or(NonZeroU32::MIN), sample)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{millis, secs},
    };

    #[test]
    fn the_default_windows_are_kafkas() {
        let config = BrokerConfig::default();

        assert!(config.replication_quota_window() == QuotaWindow::KAFKA_DEFAULT);
        assert!(config.alter_log_dirs_replication_quota_window() == QuotaWindow::KAFKA_DEFAULT);
    }

    #[test]
    fn each_window_reads_its_own_fields() {
        let config = BrokerConfig {
            replication_quota_window_num: 4,
            replication_quota_window_size: secs(2),
            alter_log_dirs_replication_quota_window_num: 7,
            alter_log_dirs_replication_quota_window_size: millis(500),
            ..BrokerConfig::default()
        };

        assert!(
            config.replication_quota_window()
                == QuotaWindow::new(NonZeroU32::new(4).unwrap(), secs(2))
        );
        assert!(
            config.alter_log_dirs_replication_quota_window()
                == QuotaWindow::new(NonZeroU32::new(7).unwrap(), millis(500))
        );
    }
}
