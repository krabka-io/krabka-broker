//! The per-node replicated log the model state holds. It lives apart from the
//! state types because it is the one component that must satisfy the core's
//! `LogView` query trait as well as `Eq + Hash` fingerprinting.

use krabka_raft::kraft::{
    event::LogEnd,
    types::{Epoch, LogView},
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
    pub(super) fn replicate_from(&mut self, leader: &ModelLog) {
        if self.epochs.len() < leader.epochs.len() {
            self.epochs.clone_from(&leader.epochs);
        }
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
