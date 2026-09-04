//! Publication of the KIP-405 reader-pool and index-cache series.
//!
//! The [`ReaderPool`](crate::remote_reader::ReaderPool) and the
//! [`RemoteIndexCache`](krabka_remote_storage::RemoteIndexCache) both keep
//! their own totals: they sit under the read path, where a metrics handle
//! would be one more argument threaded through every call. A sampler on the
//! broker's gauge cadence reads those totals instead. Prometheus counters can
//! only be advanced, never set, so the sampler passes both the totals it
//! reported last time and the current ones and [`observe_remote_reader`]
//! advances each counter by the difference.
//!
//! [`observe_remote_reader`]: super::BrokerMetrics::observe_remote_reader

use super::BrokerMetrics;

/// The reader's monotonic totals since startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RemoteReaderTotals {
    /// Index lookups served from the cache.
    pub index_cache_hits: u64,
    /// Index lookups that downloaded the object.
    pub index_cache_misses: u64,
    /// Cache entries dropped for the byte budget.
    pub index_cache_evictions: u64,
    /// Cold-tier reads the pool refused.
    pub rejected_reads: u64,
}

/// The reader's current levels, which are gauges and need no differencing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RemoteReaderLevels {
    /// Reads waiting for a reader slot.
    pub task_queue_size: u64,
    /// The share of reader slots that are free, as a percentage.
    pub idle_percent: f64,
    /// Bytes the index cache holds.
    pub index_cache_bytes: u64,
    /// Entries the index cache holds.
    pub index_cache_entries: u64,
}

impl BrokerMetrics {
    /// Advances the reader's counters by what happened since `reported` and
    /// sets its gauges to `levels`.
    ///
    /// A total that went backwards -- which nothing in the reader does, but
    /// which a restarted sampler could observe -- advances the counter by
    /// nothing rather than panicking on the subtraction.
    pub fn observe_remote_reader(
        &self,
        reported: &RemoteReaderTotals,
        current: RemoteReaderTotals,
        levels: RemoteReaderLevels,
    ) {
        for (counter, from, to) in [
            (
                &self.remote_index_cache_hits_total,
                reported.index_cache_hits,
                current.index_cache_hits,
            ),
            (
                &self.remote_index_cache_misses_total,
                reported.index_cache_misses,
                current.index_cache_misses,
            ),
            (
                &self.remote_index_cache_evictions_total,
                reported.index_cache_evictions,
                current.index_cache_evictions,
            ),
            (
                &self.remote_log_reader_rejected_total,
                reported.rejected_reads,
                current.rejected_reads,
            ),
        ] {
            counter.inc_by(to.saturating_sub(from));
        }
        self.remote_log_reader_task_queue_size
            .set(i64::try_from(levels.task_queue_size).unwrap_or(i64::MAX));
        self.remote_log_reader_avg_idle_percent
            .set(levels.idle_percent);
        self.remote_index_cache_bytes
            .set(i64::try_from(levels.index_cache_bytes).unwrap_or(i64::MAX));
        self.remote_index_cache_entries
            .set(i64::try_from(levels.index_cache_entries).unwrap_or(i64::MAX));
    }

    /// Records how long one cold-tier read held its reader slot.
    pub fn observe_remote_reader_fetch(&self, elapsed: std::time::Duration) {
        self.remote_log_reader_fetch_duration_seconds
            .observe(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests;
