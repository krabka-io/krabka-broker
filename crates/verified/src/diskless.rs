//! Pure local-trim decision for the diskless WAL flusher.

use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Whether and where to advance one diskless partition's local log start.
    pub struct DisklessTrimDecision {
        pub should_trim: bool,
        pub target: i64,
    }

    /// One decoded-batch step in a diskless cold-read run.
    pub enum DisklessBatchStep {
        Invalid,
        Skip(usize),
        Start(usize),
        Continue(usize),
        Stop,
    }
}

model_types! {
    @copy_only
    /// One diskless partition's retention configuration and floor, read at `now_ms`.
    pub struct DisklessRetentionPolicy {
        /// `retention.ms`, or `None` for Kafka's unlimited sentinel.
        pub retention_ms: Option<i64>,
        /// `retention.bytes`, or `None` for Kafka's unlimited sentinel.
        pub retention_bytes: Option<u64>,
        /// The `DeleteRecords` floor.
        pub log_start_offset: i64,
        pub now_ms: i64,
    }
}

mod logical_range;
pub use logical_range::{
    diskless_logical_range, diskless_object_reclaimable, diskless_retention_prefix,
};
#[cfg(creusot)]
pub use logical_range::{expirable, indexed_bytes, size_debt_before};

mod trim_decision;
pub use trim_decision::{diskless_batch_step, diskless_span_extension, diskless_trim_decision};

#[cfg(test)]
mod tests;
