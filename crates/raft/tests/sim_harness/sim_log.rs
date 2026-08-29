//! The in-memory fake per-node log (slice 3a). It is separate from the trait it
//! satisfies so that the real-log binary compiles the trait without also
//! carrying the fake's implementation in the same file.

use krabka_raft::kraft::types::{Epoch, LogView};

use super::node_log::SimNodeLog;

/// A growable in-memory replicated log.
///
/// Each appended record stores the leader epoch that produced it, so
/// `end_offset_for_epoch` is a real lookup and not a stub. It returns the offset
/// of the first record whose epoch is strictly greater than the queried epoch,
/// which is where that epoch's run ends.
#[derive(Debug, Clone, Default)]
pub struct SimLog {
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
        // Unknown epoch (strictly newer than anything we hold).
        if epoch > self.last_epoch() {
            return None;
        }
        // The end offset for `epoch` is the offset of the first record with a
        // strictly greater epoch, or the log end if no such record exists.
        for (i, &e) in self.epochs.iter().enumerate() {
            if e > epoch {
                return Some(i64::try_from(i).expect("offset fits in i64"));
            }
        }
        Some(self.end_offset())
    }
}

impl SimNodeLog for SimLog {
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        for _ in 0..count {
            self.epochs.push(epoch);
        }
    }

    fn truncate_to(&mut self, offset: i64) {
        let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if offset < self.epochs.len() {
            self.epochs.truncate(offset);
        }
    }

    fn replicate_from(&mut self, leader: &Self) {
        let leader_epochs = &leader.epochs;
        if self.epochs.len() < leader_epochs.len() {
            // If the follower's existing prefix matches, extend it; otherwise the
            // leader's divergence reply (TruncateTo) will fix it first. In this
            // simulation followers never accept conflicting entries, so a simple
            // suffix copy is sufficient and epoch-faithful.
            self.epochs.clone_from(leader_epochs);
        }
    }

    fn record_count(&self) -> usize {
        self.epochs.len()
    }
}
