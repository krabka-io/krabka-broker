//! Log-directory health accounting: the count of dirs this broker has taken
//! offline, whether at the startup probe or under live traffic.

use super::BrokerMetrics;

impl BrokerMetrics {
    /// Publish the number of log directories currently marked offline. The
    /// broker gauge updater samples
    /// [`LogDirRegistry`](crate::log_dir_status::LogDirRegistry) on its own
    /// timer, so the series exists at zero on a broker whose dirs are all
    /// healthy and rises without waiting for a `DescribeLogDirs` to ask.
    pub fn set_offline_log_dirs(&self, count: usize) {
        self.offline_log_dirs
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}
