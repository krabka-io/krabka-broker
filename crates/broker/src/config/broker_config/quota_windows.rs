//! The sampling windows of the KIP-13 and KIP-124 client quotas and of the
//! KIP-73 replication quotas, as [`QuotaWindow`] values the quota state is
//! built from.

use {
    super::BrokerConfig,
    krabka_throttle::QuotaWindow,
    krabka_units::{Time, convert::TimeExt as _},
    num_traits::ToPrimitive as _,
    std::num::NonZeroU32,
};

impl BrokerConfig {
    /// The window of the `producer_byte_rate`, `consumer_byte_rate` and
    /// `request_percentage` quotas: Kafka's `quota.window.num` samples of
    /// `quota.window.size.seconds` each.
    ///
    /// `quota_throttle_max` is one `quota.window.size.seconds` and
    /// `quota_window` is the product, so the sample is `quota_throttle_max`
    /// and the count is `quota_window / quota_throttle_max`, rounded to the
    /// nearest and at least one.
    #[must_use]
    pub fn client_quota_window(&self) -> QuotaWindow {
        let sample_ms = self.quota_throttle_max.millis_i64().max(1);
        let samples = (self.quota_window.millis_i64().to_f64().unwrap_or(0.0)
            / sample_ms.to_f64().unwrap_or(1.0))
        .round()
        .to_u32()
        .unwrap_or(0);
        window(samples, self.quota_throttle_max)
    }

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

        assert!(config.client_quota_window() == QuotaWindow::KAFKA_DEFAULT);
        assert!(config.replication_quota_window() == QuotaWindow::KAFKA_DEFAULT);
        assert!(config.alter_log_dirs_replication_quota_window() == QuotaWindow::KAFKA_DEFAULT);
    }

    /// The client quota window is `quota_window / quota_throttle_max`
    /// samples of `quota_throttle_max`, rounded, and never no sample.
    #[yare::parameterized(
        kafka_default = { secs(11), secs(1), 11, secs(1) },
        two_second_samples = { secs(10), secs(2), 5, secs(2) },
        rounds_down = { millis(10_400), secs(1), 10, secs(1) },
        rounds_up = { millis(10_600), secs(1), 11, secs(1) },
        one_sample = { secs(1), secs(1), 1, secs(1) },
        shorter_than_a_sample = { millis(400), secs(1), 1, secs(1) },
    )]
    fn the_client_quota_window_is_derived_from_the_window_and_the_sample(
        quota_window: Time,
        quota_throttle_max: Time,
        samples: u32,
        sample: Time,
    ) {
        let config = BrokerConfig {
            quota_throttle_max,
            quota_window,
            ..BrokerConfig::default()
        };

        assert2::check!(
            config.client_quota_window()
                == QuotaWindow::new(NonZeroU32::new(samples).unwrap(), sample)
        );
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
