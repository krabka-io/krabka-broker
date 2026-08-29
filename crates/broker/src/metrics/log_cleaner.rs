//! Log-compaction accounting: the sweep counter the cleaner bumps once per
//! pass, and the per-partition counter of compactions that pass completed.

use super::{BrokerMetrics, PartitionLabel};

impl BrokerMetrics {
    /// Account one completed log-compaction sweep (a full `tick_all`
    /// pass). Called once per cleaner tick, whether or not any partition
    /// was eligible, so a test can observe that a full pass ran after it
    /// sealed a segment.
    pub fn record_cleaner_run(&self) {
        self.log_cleaner_runs_total.inc();
    }

    /// Account one successful per-partition compaction pass
    /// (`Partition::compact_log` returned `Ok`).
    pub fn record_compaction(&self, topic: &str, partition: i32) {
        let lbl = PartitionLabel {
            topic: topic.to_string(),
            partition,
        };
        self.log_compactions_total.get_or_create(&lbl).inc();
    }
}
