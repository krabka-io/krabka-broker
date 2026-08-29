//! Log compaction primitives. These are almost-pure helpers that work on
//! [`Segment`] handles and the on-disk file layout. [`crate::Log::compact`]
//! uses them.
//!
//! The algorithm makes a single pass over the **sealed** segment list, from
//! oldest to newest. It builds a key-to-latest-offset map, then rewrites the
//! surviving records into a single new segment at the lowest input base
//! offset. It never touches the active segment.
//!
//! The algorithm drops records with `key.is_none()`, as Kafka's `LogCleaner`
//! does. A tombstone is a record with `key.is_some()` and `value.is_none()`.
//! The algorithm treats a tombstone like any other value and keeps it as the
//! most-recent entry for its key. `delete.retention.ms` ages tombstones out.

#[cfg(test)]
use krabka_ids::ProducerId;

mod batch_reader;
mod decision;
mod offset_map;
mod rewrite;
mod swap;

#[cfg(test)]
mod test_support;

pub(crate) use self::decision::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, retain_decision, should_index_key,
};
pub use self::{
    offset_map::{CleanedTransactionMetadata, build_offset_map},
    rewrite::{RewriteOutput, RewriteRetention, rewrite_segments},
    swap::atomic_swap,
};

// Exhaustive stateright enumeration of the KIP-534 retention contract over the
// pure decision cores above (reachable via `super::`).
#[cfg(test)]
#[path = "compact_model.rs"]
mod compact_model;

#[cfg(test)]
mod retention_fuzz;
