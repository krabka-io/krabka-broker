//! Producer-snapshot selection, validation, replay, and deletion decisions.
//!
//! The rules are Kafka's, from three places:
//!
//! - `LogLoader.load` picks the log start the reload runs against, and first
//!   calls `ProducerStateManager.removeStraySnapshots` with the base offset
//!   of every local segment.
//! - `ProducerStateManager.truncateAndReload(logStartOffset, logEndOffset, _)`
//!   deletes every snapshot outside `(logStartOffset, logEndOffset]`, then
//!   `loadFromSnapshot` loads the newest snapshot that is left.
//! - `UnifiedLog.rebuildProducerState` replays each local segment from
//!   `max(segment.baseOffset, mapEndOffset, logStartOffset)`.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// The offsets one producer-state reload runs against.
///
/// These are three `i64` values with three different meanings, so they
/// travel as one struct and a transposed call site does not compile.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerReloadRange {
    /// The `logStartOffset` Kafka hands `truncateAndReload`, as
    /// [`producer_snapshot_reload_log_start`] picks it.
    pub log_start: i64,
    /// The first offset a local segment can serve: the greater of the
    /// oldest local segment's base and the log start.
    pub local_start: i64,
    /// The log end offset the state is rebuilt up to.
    pub log_end: i64,
}

/// One decoded producer-state snapshot entry, field for field as Kafka's
/// `ProducerStateManager` snapshot schema (version 1) lays it out, minus the
/// timestamp, which no validity rule reads.
///
/// Seven integers of four widths with seven meanings travel as one struct, so
/// a transposed call site does not compile.
#[cfg_attr(creusot, derive(Clone, Copy))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct ProducerSnapshotEntryFacts {
    /// `ProducerId`.
    pub producer_id: i64,
    /// `ProducerEpoch`.
    pub producer_epoch: i16,
    /// `LastSequence`, or `-1` for a producer whose only record is a marker.
    pub last_sequence: i32,
    /// `LastOffset`, or `-1` with the same meaning.
    pub last_offset: i64,
    /// `OffsetDelta`: the last batch's offset count less one.
    pub offset_delta: i32,
    /// `CoordinatorEpoch`, or `-1` before any marker.
    pub coordinator_epoch: i32,
    /// `CurrentTxnFirstOffset`, or `-1` with no open transaction.
    pub current_txn_first_offset: i64,
}

mod snapshot_transaction_valid;
#[cfg(creusot)]
pub use snapshot_transaction_valid::{
    is_segment_base, kafka_reload_keeps, kafka_replay_start, kafka_stray_removed,
    snapshot_last_record_valid, snapshot_transaction_valid,
};
pub use snapshot_transaction_valid::{
    producer_snapshot_latest_index, producer_snapshot_reload_keeps,
    producer_snapshot_reload_log_start, producer_snapshot_stray,
};

mod replay_start;
#[cfg(creusot)]
pub use replay_start::snapshot_entry_valid_model;
pub use replay_start::{producer_snapshot_entry_valid, producer_snapshot_replay_start};

#[cfg(test)]
mod tests;
