//! The bridge between the live window and the share coordinator's records.
//!
//! This module holds the projection of the in-flight batch list into
//! `StateBatch` values that the share coordinator writes, and the reload that
//! rebuilds the machine from them. It also holds the two counters a caller
//! reads out of the machine, `delivery_complete_count` and
//! `count_acquired_batches`. Both directions of the mapping are stated once,
//! here, so a change to the persisted delivery-state codes touches one file.

use krabka_log::Offset;

use super::{
    AcquisitionState, DS_ACKNOWLEDGED, DS_ARCHIVED, DS_AVAILABLE, InFlightBatch, RecordState,
};
use crate::share_coordinator::persistence::StateBatch;

impl AcquisitionState {
    /// Projects the live window into persistable batches.
    ///
    /// It returns `(start_offset, delivery_complete_count, batches)` for
    /// `[start_offset, end_offset)`. It persists a transient `Acquired` record
    /// as `Available(0)`, so a leader that crashes and reloads offers the
    /// record again. It emits Acknowledged and Archived batches with their
    /// terminal codes.
    ///
    /// A `Deferred` record persists as `Available(0)` for the same reason: it
    /// is derived from a clock reading, and the next leader must re-derive it
    /// from its own clock. The `__share_group_state` encoding therefore does
    /// not change by one byte on a scheduled topic.
    #[must_use]
    pub fn to_persist_batches(&self) -> (Offset, i32, Vec<StateBatch>) {
        let mut out = Vec::with_capacity(self.batches.len());
        for b in &self.batches {
            let delivery_state = match b.state {
                RecordState::Available | RecordState::Acquired | RecordState::Deferred => {
                    DS_AVAILABLE
                }
                RecordState::Acknowledged => DS_ACKNOWLEDGED,
                RecordState::Archived => DS_ARCHIVED,
            };
            out.push(StateBatch {
                first_offset: b.first_offset,
                last_offset: b.last_offset,
                delivery_state,
                delivery_count: b.delivery_count,
            });
        }
        (self.start_offset, self.delivery_complete_count, out)
    }

    /// Cumulative count of offsets that have reached a terminal state,
    /// Acknowledged or Archived. This is the persister's
    /// `delivery_complete_count`. Only the state-machine tests read this
    /// method today. The value also leaves through
    /// [`Self::to_persist_batches`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn delivery_complete_count(&self) -> i32 {
        self.delivery_complete_count
    }

    /// Number of in-flight batches currently in `Acquired` state. Test-only.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn count_acquired_batches(&self) -> i32 {
        i32::try_from(
            self.batches
                .iter()
                .filter(|b| b.state == RecordState::Acquired)
                .count(),
        )
        .unwrap_or(i32::MAX)
    }

    /// Rebuilds the machine from persisted state.
    ///
    /// It restores the SPSO to `start_offset`. It restores the cumulative
    /// `delivery_complete_count`, so the consumer-lag accounting survives a
    /// leader change. It rebuilds the batches, and maps a persisted
    /// `Acquired(1)` to `Available`, because a lock does not survive a leader
    /// change. It sets `end_offset` to `max(last_offset)+1`, or to
    /// `start_offset` when the batch list is empty.
    pub fn load_from(
        &mut self,
        start_offset: Offset,
        state_epoch: i32,
        leader_epoch: i32,
        delivery_complete_count: i32,
        batches: &[StateBatch],
    ) {
        self.start_offset = start_offset;
        self.state_epoch = state_epoch;
        self.leader_epoch = leader_epoch;
        self.dirty = false;
        self.delivery_complete_count = delivery_complete_count;
        self.batches = batches
            .iter()
            .map(|sb| {
                // Persisted Acquired(1) maps to Available: locks don't survive a
                // leader change, so re-offer those records.
                let state = match sb.delivery_state {
                    DS_ACKNOWLEDGED => RecordState::Acknowledged,
                    DS_ARCHIVED => RecordState::Archived,
                    _ => RecordState::Available,
                };
                InFlightBatch {
                    first_offset: sb.first_offset,
                    last_offset: sb.last_offset,
                    state,
                    delivery_count: sb.delivery_count,
                    acquired_by: None,
                    lock_deadline: None,
                }
            })
            .collect();
        self.end_offset = self
            .batches
            .iter()
            .map(|b| b.last_offset + 1)
            .max()
            .unwrap_or(start_offset)
            .max(start_offset);
        self.coalesce();
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::share_partition::state::{
        AckType, AcquiredRange,
        test_support::{LOCK, t0},
    };

    #[test]
    fn to_persist_batches_maps_acquired_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        let (start, dcc, batches) = s.to_persist_batches();
        check!(start == 0);
        check!(dcc == 0); // nothing terminal yet
        // Acquired persists as Available(0) but retains its delivery_count.
        check!(
            batches
                == vec![StateBatch {
                    first_offset: Offset(0),
                    last_offset: Offset(4),
                    delivery_state: DS_AVAILABLE,
                    delivery_count: 1
                }]
        );
    }

    #[test]
    fn load_from_round_trip() {
        // Build a state, acquire part of it, persist, reload into a fresh one.
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let _ = s.acquire("m1", 4, i32::MAX, t0(), LOCK, 5); // [0,3] Acquired, [4,9] Available
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, t0())
            .unwrap(); // SPSO -> 4
        let (start, _dcc, batches) = s.to_persist_batches();
        assert!(start == 4);

        let mut reloaded = AcquisitionState::new(Offset(0));
        reloaded.load_from(start, 7, 3, 0, &batches);
        check!(reloaded.start_offset == 4);
        check!(reloaded.end_offset == 10);
        check!(reloaded.state_epoch == 7);
        check!(reloaded.leader_epoch == 3);
        check!(!reloaded.dirty);
        // The remaining records are Available again and re-acquirable.
        let acq = reloaded.acquire("m2", 100, i32::MAX, t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(4),
                last: Offset(9),
                delivery_count: 1
            }]
        );
    }

    #[test]
    fn load_from_restores_delivery_complete_count() {
        // F3: the cumulative delivery-complete count must survive a reload so
        // consumer-lag accounting is preserved across a leader change.
        let mut s = AcquisitionState::new(Offset(4));
        s.load_from(Offset(4), 0, 0, 5, &[]);
        assert!(s.delivery_complete_count() == 5);
        // It round-trips back out through the persist projection.
        let (_start, dcc, _batches) = s.to_persist_batches();
        assert!(dcc == 5);
    }
}
