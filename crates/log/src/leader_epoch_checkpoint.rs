//! Per-partition `.leader-epoch-checkpoint` file. The two-column text format
//! matches Apache Kafka exactly:
//!
//! ```text
//!   0          <-- header version
//!   <n>        <-- row count
//!   <epoch_0> <start_offset_0>
//!   <epoch_1> <start_offset_1>
//!   ...
//! ```
//!
//! The byte layout stays the same, so `kafka-dump-log` can read these files.

use std::path::PathBuf;

use krabka_ids::{LeaderEpoch, Offset};

mod file;
mod lookup;
mod mutation;

#[cfg(test)]
mod fuzz;
#[cfg(test)]
mod test_support;

pub use self::lookup::epoch_and_offset_for_entries;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EpochEntry {
    pub epoch: LeaderEpoch,
    pub start_offset: Offset,
}

#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<EpochEntry>,
}

/// Kafka sentinel: "no leader epoch information".
pub const UNDEFINED_EPOCH: LeaderEpoch = LeaderEpoch(-1);
/// Kafka sentinel: "no offset".
pub const UNDEFINED_OFFSET: Offset = Offset(-1);

impl LeaderEpochCheckpoint {
    #[must_use]
    pub fn latest_epoch(&self) -> Option<LeaderEpoch> {
        self.entries.iter().map(|e| e.epoch).max()
    }

    #[must_use]
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }
}

/// Pure core of [`LeaderEpochCheckpoint::append`]: an idempotent
/// push-if-absent. It returns `true` when it added a new entry, so the caller
/// knows that it must flush.
pub(crate) fn append_to(
    entries: &mut Vec<EpochEntry>,
    epoch: LeaderEpoch,
    start_offset: Offset,
) -> bool {
    if entries.iter().any(|e| e.epoch == epoch) {
        return false;
    }
    entries.push(EpochEntry {
        epoch,
        start_offset,
    });
    true
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_end`]: drop entries that
/// begin at or after `end_offset`.
pub(crate) fn truncate_to(entries: &mut Vec<EpochEntry>, end_offset: Offset) {
    entries.retain(|e| e.start_offset < end_offset);
}

#[cfg(test)]
#[path = "leader_epoch_model.rs"]
mod leader_epoch_model;
