//! KIP-835's pending-record processing time: how long each apply pass that
//! consumes the committed records takes, over the life of the process.

/// The count, total and maximum duration of the apply passes that reached the
/// committed boundary. `DescribeQuorum` reports their mean and maximum.
#[derive(Debug, Default)]
pub(super) struct RecordProcessingStats {
    total_us: u128,
    samples: u64,
    max_us: u64,
}

impl RecordProcessingStats {
    /// Count one pass that took `elapsed_us` microseconds.
    pub(super) fn record(&mut self, elapsed_us: u64) {
        self.total_us = self.total_us.saturating_add(u128::from(elapsed_us));
        self.samples = self.samples.saturating_add(1);
        self.max_us = self.max_us.max(elapsed_us);
    }

    /// The mean pass in whole microseconds, rounded down. Zero before the first
    /// pass, and `u64::MAX` when the mean does not fit.
    pub(super) fn avg_us(&self) -> u64 {
        if self.samples == 0 {
            0
        } else {
            u64::try_from(self.total_us / u128::from(self.samples)).unwrap_or(u64::MAX)
        }
    }

    /// The longest pass in microseconds, or zero before the first pass.
    pub(super) const fn max_us(&self) -> u64 { self.max_us }

    /// How many passes were counted.
    #[cfg(test)]
    pub(super) const fn samples(&self) -> u64 { self.samples }
}
