//! The per-node replicated log the model state holds. It lives apart from the
//! state types because it is the one component that must satisfy the core's
//! `LogView` query trait as well as `Eq + Hash` fingerprinting.

use krabka_raft::kraft::{
    event::LogEnd,
    types::{Epoch, LogOffsetMetadata, LogView},
};

/// In-memory replicated log, where `epochs[i]` is the leader epoch of offset
/// `i`. This is a self-contained copy of the sim-harness `SimLog`, made
/// `Eq + Hash` so that it can live in fingerprinted model state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ModelLog {
    pub(super) epochs: Vec<Epoch>,
}

impl ModelLog {
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
    /// Whether every entry of this log is the leader's entry at the same
    /// offset, so that the leader's log extends this one.
    pub(super) fn is_prefix_of(&self, leader: &ModelLog) -> bool {
        leader.epochs.starts_with(&self.epochs)
    }

    /// Append the leader's entries past this log's end, when the leader's log
    /// extends this one. It returns whether it did, that is whether the two
    /// logs agree on every offset this one holds.
    ///
    /// A log that disagrees with the leader is left alone, however short: the
    /// leader answers its fetch with a diverging epoch and the production
    /// core's `TruncateTo` cuts it back first. Overwriting it here would heal
    /// the divergence without the truncation path ever running.
    pub(super) fn extend_from(&mut self, leader: &ModelLog) -> bool {
        if !self.is_prefix_of(leader) {
            return false;
        }
        self.epochs
            .extend_from_slice(&leader.epochs[self.epochs.len()..]);
        true
    }
    /// The leader epoch stamped on `offset`, or `None` when the log is shorter
    /// than that.
    pub(super) fn epoch_at(&self, offset: i64) -> Option<Epoch> {
        usize::try_from(offset)
            .ok()
            .and_then(|offset| self.epochs.get(offset).copied())
    }
    pub(super) fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

impl LogView for ModelLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }
    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        LogOffsetMetadata::end_of_epoch_in(&self.epochs, epoch)
    }
}
