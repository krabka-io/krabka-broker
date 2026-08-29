//! The growable in-memory log a simulated node replicates.
//!
//! [`SimLog`] is a focused copy of the integration harness log. It implements
//! the [`LogView`] seam the state machine reads through, and it stores a leader
//! epoch per record, so the diverging-epoch lookup is real and not a stub.

use crate::{
    event::LogEnd,
    types::{Epoch, LogView},
};

/// A growable in-memory replicated log.
///
/// Each appended record stores the leader epoch that produced it, so
/// `end_offset_for_epoch` is a real lookup and not a stub.
#[derive(Debug, Clone, Default)]
pub(super) struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<Epoch>,
}

impl LogView for SimLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }

    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        if epoch > self.last_epoch() {
            return None;
        }
        for (i, &e) in self.epochs.iter().enumerate() {
            if e > epoch {
                return Some(i64::try_from(i).expect("offset fits in i64"));
            }
        }
        Some(self.end_offset())
    }
}

impl SimLog {
    pub(super) fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        for _ in 0..count {
            self.epochs.push(epoch);
        }
    }

    pub(super) fn truncate_to(&mut self, offset: i64) {
        let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if offset < self.epochs.len() {
            self.epochs.truncate(offset);
        }
    }

    pub(super) fn replicate_from(&mut self, leader: &Self) {
        let leader_epochs = &leader.epochs;
        if self.epochs.len() < leader_epochs.len() {
            self.epochs.clone_from(leader_epochs);
        }
    }

    pub(super) fn record_count(&self) -> usize {
        self.epochs.len()
    }

    pub(super) fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}
