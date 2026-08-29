//! Terminal disposition of an acquired offset range.
//!
//! This module holds `acknowledge`, the step where a share consumer reports
//! what it did with the records it holds. `Accept` completes them, `Release`
//! puts them back for redelivery, and `Reject` and `Gap` archive them. It is
//! separate from acquisition because it is the only place that moves a record
//! into a terminal state on a consumer's word, and so the only place the
//! delivery-complete accounting grows outside an internal archive.

use std::time::Instant;

use krabka_log::Offset;

use super::{AckType, AcquisitionState, RecordState, clamp_i32};

impl AcquisitionState {
    /// Acknowledges the offset range `[first, last]` that `member` acquired
    /// earlier.
    ///
    /// `member` must currently hold the whole range as `Acquired`. If it does
    /// not, this method returns `Err(INVALID_RECORD_STATE)`. It splits the
    /// range into its own batches at the boundaries, then applies the
    /// acknowledgement. `Accept` gives Acknowledged. `Release` gives
    /// Available, clears the lock and the owner, and keeps `delivery_count`
    /// for redelivery. `Reject` and `Gap` give Archived. The method then
    /// advances the SPSO over any new terminal prefix and marks the state
    /// dirty.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn acknowledge(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        ack: AckType,
        _now: Instant,
    ) -> Result<(), i16> {
        if first > last {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Validate the entire range is Acquired by this member.
        if !self.range_acquired_by(member, first, last) {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Carve the range out at its boundaries.
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        for b in &mut self.batches {
            if b.first_offset < first || b.last_offset > last {
                continue;
            }
            if b.state != RecordState::Acquired {
                continue;
            }
            let n = clamp_i32(b.len());
            match ack {
                AckType::Accept => {
                    b.state = RecordState::Acknowledged;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    self.delivery_complete_count += n;
                }
                AckType::Release => {
                    b.state = RecordState::Available;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    // delivery_count retained: next acquire redelivers at +1.
                }
                AckType::Reject | AckType::Gap => {
                    b.state = RecordState::Archived;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    self.delivery_complete_count += n;
                }
            }
        }
        self.dirty = true;
        self.advance_spso();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::share_partition::state::{
        AcquiredRange,
        test_support::{LOCK, t0},
    };

    #[test]
    fn acquire_then_accept_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100); // [0,4] Available
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(4),
                delivery_count: 1
            }]
        );
        s.acknowledge("m1", Offset(0), Offset(4), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 5);
    }

    #[test]
    fn release_redelivers_with_incremented_count() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, t0())
            .unwrap();
        let acq2 = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq2[0].delivery_count == 2);
        // Released records stay in the window; SPSO did not advance.
        assert!(s.start_offset == 0);
    }

    #[test]
    fn partial_acknowledge_splits_a_batch() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100); // [0,9] Available
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq.len() == 1);
        // Accept only [0,3]; [4,9] remain Acquired.
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 4);
        // The remaining acquired range can still be acknowledged.
        s.acknowledge("m1", Offset(4), Offset(9), AckType::Accept, t0())
            .unwrap();
        assert!(s.start_offset == 10);
    }

    #[test]
    fn reject_archives_and_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Reject, t0())
            .unwrap();
        assert!(s.start_offset == 3); // archived prefix dropped
        let acq = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(acq.is_empty()); // nothing left
    }

    #[test]
    fn gap_archives() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(2), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Gap, t0())
            .unwrap();
        assert!(s.start_offset == 2);
        let (_start, dcc, batches) = s.to_persist_batches();
        assert!(batches.is_empty()); // archived prefix dropped from window
        assert!(dcc == 2); // both offsets reached a terminal state
    }

    #[test]
    fn acknowledge_wrong_member_is_invalid_record_state() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        let err = s.acknowledge("m2", Offset(0), Offset(2), AckType::Accept, t0());
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));
    }
}
