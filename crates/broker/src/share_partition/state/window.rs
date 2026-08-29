//! Geometry of the in-flight batch list that backs the acquisition window.
//!
//! This module holds the operations that reshape the run list without deciding
//! a delivery outcome. It splits a run at an offset boundary, merges neighbours
//! that agree on state, count, and lock, advances the share-partition start
//! offset over a terminal prefix, and answers whether one member holds a whole
//! offset range. The concern-specific modules call these helpers; none of them
//! is part of the public surface of `AcquisitionState`.

use krabka_log::Offset;

use super::{AcquisitionState, InFlightBatch, RecordState};

impl AcquisitionState {
    /// True if and only if `member` currently holds every offset in
    /// `[first, last]` as Acquired.
    pub(super) fn range_acquired_by(&self, member: &str, first: Offset, last: Offset) -> bool {
        let mut cursor = first;
        for b in &self.batches {
            if b.last_offset < first || b.first_offset > last {
                continue;
            }
            // The covered batches must be contiguous from `first`.
            if b.first_offset > cursor {
                return false;
            }
            if b.state != RecordState::Acquired || b.acquired_by.as_deref() != Some(member) {
                return false;
            }
            cursor = b.last_offset + 1;
            if cursor > last {
                break;
            }
        }
        cursor > last
    }

    /// Splits the batch at index `i` so that `split` becomes the first offset
    /// of a new trailing batch. It does nothing when `split` is at a
    /// boundary.
    pub(super) fn split_at(&mut self, i: usize, split: Offset) {
        let b = &self.batches[i];
        if split <= b.first_offset || split > b.last_offset {
            return;
        }
        let tail = InFlightBatch {
            first_offset: split,
            last_offset: b.last_offset,
            state: b.state,
            delivery_count: b.delivery_count,
            acquired_by: b.acquired_by.clone(),
            lock_deadline: b.lock_deadline,
        };
        self.batches[i].last_offset = split - 1;
        self.batches.insert(i + 1, tail);
    }

    /// Splits whichever batch holds the boundary, so that `offset` becomes a
    /// batch `first_offset`. It does nothing when `offset` already lands on a
    /// boundary, and when `offset` is outside the window.
    pub(super) fn split_at_offset(&mut self, offset: Offset) {
        if let Some(i) = self.batches.iter().position(|b| {
            b.first_offset
                .0
                .checked_add(1)
                .is_some_and(|first_split_offset| {
                    (first_split_offset..=b.last_offset.0).contains(&offset.0)
                })
        }) {
            self.split_at(i, offset);
        }
    }

    /// Advances the SPSO over any terminal prefix, that is Acknowledged or
    /// Archived, and drops those batches. It then merges adjacent same-state
    /// neighbors.
    pub(super) fn advance_spso(&mut self) {
        while let Some(b) = self.batches.first() {
            if b.first_offset == self.start_offset
                && matches!(b.state, RecordState::Acknowledged | RecordState::Archived)
            {
                self.start_offset = b.last_offset + 1;
                self.batches.remove(0);
            } else {
                break;
            }
        }
        if self.end_offset < self.start_offset {
            self.end_offset = self.start_offset;
        }
        self.coalesce();
    }

    /// Merges adjacent batches that have the same delivery state, delivery
    /// count, and acquisition, that is the same owner and deadline. This keeps
    /// the batch list compact.
    pub(super) fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.batches.len() {
            let mergeable = {
                let a = &self.batches[i];
                let b = &self.batches[i + 1];
                a.last_offset + 1 == b.first_offset
                    && a.state == b.state
                    && a.delivery_count == b.delivery_count
                    && a.acquired_by == b.acquired_by
                    && a.lock_deadline == b.lock_deadline
            };
            if mergeable {
                let next_last = self.batches[i + 1].last_offset;
                self.batches[i].last_offset = next_last;
                self.batches.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }
}
