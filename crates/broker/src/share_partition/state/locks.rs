//! The lifetime of an acquisition lock: renewal, expiry, and member release.
//!
//! A record handed to a share consumer stays out of circulation only while its
//! lock holds. This module holds the three ways that ends: `renew` pushes the
//! deadline out, `expire_locks` sweeps the deadlines that have passed, and
//! `release_member` gives back everything one member holds when its session
//! closes. All three return records to `Available` and keep `delivery_count`,
//! so the next hand-out counts as a redelivery.

use std::time::{Duration, Instant};

use krabka_log::Offset;

use super::{AcquisitionState, RecordState};

impl AcquisitionState {
    /// Renews the acquisition lock on the range `[first, last]` that `member`
    /// holds. It resets each covered Acquired batch's `lock_deadline` to
    /// `now + lock_dur`. This is the KIP-932 RENEW acknowledgement.
    ///
    /// `member` must currently hold the whole range as `Acquired`. If it does
    /// not, this method returns `Err(INVALID_RECORD_STATE)`. The state, the
    /// owner, and `delivery_count` do not change. Only the deadline moves. The
    /// method does NOT advance the SPSO, because a renew keeps records in
    /// flight. It marks the state dirty.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn renew(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        now: Instant,
        lock_dur: Duration,
    ) -> Result<(), i16> {
        if first > last {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // The entire range must be Acquired by this member.
        if !self.range_acquired_by(member, first, last) {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }
        // Carve the range out at its boundaries, then extend each covered lock.
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        let new_deadline = now + lock_dur;
        for b in &mut self.batches {
            if b.first_offset < first || b.last_offset > last {
                continue;
            }
            if b.state != RecordState::Acquired {
                continue;
            }
            b.lock_deadline = Some(new_deadline);
        }
        self.dirty = true;
        Ok(())
    }

    /// Returns any Acquired batch whose lock has expired to Available. It
    /// clears the lock and the owner but keeps `delivery_count`, so the next
    /// acquire counts as a redelivery. It marks the state dirty when something
    /// changed.
    pub fn expire_locks(&mut self, now: Instant) {
        let mut changed = false;
        for b in &mut self.batches {
            if b.state == RecordState::Acquired
                && let Some(deadline) = b.lock_deadline
                && now >= deadline
            {
                b.state = RecordState::Available;
                b.acquired_by = None;
                b.lock_deadline = None;
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.coalesce();
        }
    }

    /// Releases every record currently acquired by `member` back to
    /// `Available`. The delivery count is retained for the next delivery.
    ///
    /// Session close and connection disconnect call this method so records do
    /// not remain locked until their timeout after the consumer is gone.
    pub fn release_member(&mut self, member: &str) {
        let mut changed = false;
        for batch in &mut self.batches {
            if batch.state == RecordState::Acquired && batch.acquired_by.as_deref() == Some(member)
            {
                batch.state = RecordState::Available;
                batch.acquired_by = None;
                batch.lock_deadline = None;
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.coalesce();
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::share_partition::state::{
        AckType, AcquiredRange,
        test_support::{LOCK, t0},
    };

    #[test]
    fn session_release_makes_only_members_records_available() {
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(4), 100);
        let _ = state.acquire("m1", 2, i32::MAX, t0(), LOCK, 5);
        let _ = state.acquire("m2", 2, i32::MAX, t0(), LOCK, 5);

        state.release_member("m1");

        let reacquired = state.acquire("m3", 10, i32::MAX, t0(), LOCK, 5);
        assert!(reacquired.len() == 1);
        assert!(reacquired[0].first == Offset(0));
        assert!(reacquired[0].last == Offset(1));
        assert!(reacquired[0].delivery_count == 2);
    }

    #[test]
    fn expire_locks_reverts_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        // Before expiry: re-acquire finds nothing (all Acquired).
        let none = s.acquire("m2", 10, i32::MAX, t0(), LOCK, 5);
        assert!(none.is_empty());
        s.expire_locks(t0() + Duration::from_secs(31));
        // Now another member can acquire; redelivery bumps the count.
        let acq = s.acquire("m2", 10, i32::MAX, t0() + Duration::from_secs(31), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(3),
                delivery_count: 2
            }]
        );
        assert!(s.start_offset == 0);
    }

    #[test]
    fn renew_extends_lock_keeping_acquired() {
        // F1: acquire with a short lock, renew with a longer one. An
        // `expire_locks` at the ORIGINAL deadline must not release the records.
        let t0 = Instant::now();
        let short = Duration::from_secs(10);
        let long = Duration::from_mins(1);

        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let acq = s.acquire("m1", 10, i32::MAX, t0, short, 5);
        assert!(acq.len() == 1);
        let original_deadline = t0 + short;

        // Renew extends the lock well past the original deadline.
        s.renew("m1", Offset(0), Offset(3), t0, long).unwrap();
        assert!(s.dirty);

        // Sweeping at the original deadline must NOT release the renewed lock.
        s.expire_locks(original_deadline);
        // Still Acquired by m1 -> a different member acquires nothing.
        let none = s.acquire("m2", 10, i32::MAX, original_deadline, short, 5);
        assert!(none.is_empty());
        // And m1 can still acknowledge it (proves it stayed Acquired by m1).
        s.acknowledge(
            "m1",
            Offset(0),
            Offset(3),
            AckType::Accept,
            original_deadline,
        )
        .unwrap();
        assert!(s.start_offset == 4);
    }

    #[test]
    fn renew_on_unacquired_range_is_invalid_record_state() {
        let t0 = Instant::now();
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, i32::MAX, t0, LOCK, 5);
        // Wrong member.
        let err = s.renew("m2", Offset(0), Offset(2), t0, LOCK);
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));

        // Non-Acquired range: release [0,2] back to Available, then renew fails.
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, t0)
            .unwrap();
        let err2 = s.renew("m1", Offset(0), Offset(2), t0, LOCK);
        assert!(err2 == Err(crate::codes::INVALID_RECORD_STATE));
    }
}
