//! Terminal disposition of an acquired offset range.
//!
//! This module holds `acknowledge`, the step where a share consumer reports
//! what it did with the records it holds. `Accept` completes them, `Release`
//! puts them back for redelivery, and `Reject` and `Gap` archive them. It is
//! separate from acquisition because it is the only place that moves a record
//! into a terminal state on a consumer's word, and so the only place the
//! delivery-complete accounting grows outside an internal archive.

use {
    super::{AckType, AcquisitionState, RecordState, clamp_i32, dlq::DlqCause, window::give_back},
    krabka_log::Offset,
    std::time::Instant,
};

/// One per-offset operation in a mixed acknowledgement batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckAction {
    /// Apply a terminal disposition.
    Disposition(AckType),
    /// Extend the acquisition lock to this deadline.
    Renew { deadline: Instant },
}

impl AcquisitionState {
    /// Applies one action per offset in `[first, last]` in one window pass.
    ///
    /// The actions must have exactly the same length as the inclusive range.
    /// Like [`Self::acknowledge`], offsets below the SPSO are already complete,
    /// while every remaining offset must be acquired by `member`.
    ///
    /// # Errors
    ///
    /// `INVALID_REQUEST` for a malformed range or a range past the records
    /// handed out, and `INVALID_RECORD_STATE` when `member` does not hold the
    /// complete live part of the range.
    pub fn acknowledge_mixed(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        actions: &[AckAction],
        max_attempts: i16,
    ) -> Result<(), i16> {
        let range_len = last
            .0
            .checked_sub(first.0)
            .and_then(|len| len.checked_add(1))
            .and_then(|len| usize::try_from(len).ok());
        if range_len != Some(actions.len()) {
            return Err(crate::codes::INVALID_REQUEST);
        }
        let Some(bounds) = self.ack_bounds(first, last)? else {
            return Ok(());
        };
        if !self.range_acquired_by(member, bounds.first, bounds.last) {
            return Err(crate::codes::INVALID_RECORD_STATE);
        }

        let old_batches = std::mem::take(&mut self.batches);
        let mut batches = Vec::with_capacity(old_batches.len().saturating_add(actions.len()));
        let mut archive = super::dlq::ArchiveSink {
            dlq_enabled: self.dlq_enabled,
            delivery_complete_count: &mut self.delivery_complete_count,
            pending_dlq: &mut self.pending_dlq,
        };
        let mut action_index =
            usize::try_from(bounds.first.0 - first.0).map_err(|_| crate::codes::INVALID_REQUEST)?;
        let mut action_run_end = action_index;

        for batch in old_batches {
            if batch.last_offset < bounds.first || batch.first_offset > bounds.last {
                batches.push(batch);
                continue;
            }
            if batch.first_offset < bounds.first {
                let mut prefix = batch.clone();
                prefix.last_offset = bounds.first - 1;
                batches.push(prefix);
            }

            let mut offset = batch.first_offset.max(bounds.first);
            let batch_last = batch.last_offset.min(bounds.last);
            while offset <= batch_last {
                let action = actions[action_index];
                if action_index == action_run_end {
                    action_run_end += actions[action_run_end..]
                        .iter()
                        .take_while(|candidate| **candidate == action)
                        .count();
                }
                let same = action_run_end - action_index;
                let run_last = Offset(
                    offset
                        .0
                        .saturating_add(i64::try_from(same).unwrap_or(i64::MAX) - 1)
                        .min(batch_last.0),
                );
                let mut run = batch.clone();
                run.first_offset = offset;
                run.last_offset = run_last;
                let n = clamp_i32(run.len());
                match action {
                    AckAction::Disposition(AckType::Accept) => {
                        run.state = RecordState::Acknowledged;
                        run.acquired_by = None;
                        run.lock_deadline = None;
                        *archive.delivery_complete_count += n;
                    }
                    AckAction::Disposition(AckType::Release) => {
                        give_back(&mut run, max_attempts, &mut archive);
                    }
                    AckAction::Disposition(AckType::Reject) => {
                        archive.archive(&mut run, DlqCause::ClientReject);
                    }
                    AckAction::Disposition(AckType::Gap) => {
                        run.state = RecordState::Archived;
                        run.acquired_by = None;
                        run.lock_deadline = None;
                        *archive.delivery_complete_count += n;
                    }
                    AckAction::Renew { deadline } => run.lock_deadline = Some(deadline),
                }
                batches.push(run);
                let consumed = usize::try_from(run_last.0 - offset.0 + 1).unwrap_or(actions.len());
                action_index += consumed;
                offset = run_last + 1;
            }

            if batch.last_offset > bounds.last {
                let mut suffix = batch;
                suffix.first_offset = bounds.last + 1;
                batches.push(suffix);
            }
        }
        self.batches = batches;
        self.dirty = true;
        self.advance_spso();
        Ok(())
    }

    /// Acknowledges the offset range `[first, last]` that `member` acquired
    /// earlier.
    ///
    /// A range below the SPSO is already done, and a range that starts below
    /// it is cut there, as `ack_bounds` says. `member`
    /// must currently hold the rest of the range as `Acquired`. The method
    /// splits the range into its own batches at the boundaries, then applies
    /// the acknowledgement. `Accept` gives Acknowledged. `Release` gives
    /// Available, clears the lock and the owner, and keeps `delivery_count`
    /// for redelivery, unless the count has reached `max_attempts`: then the
    /// records are archived at once, as Kafka's `InFlightState.tryUpdateState`
    /// does. `Reject` and `Gap` give Archived. With a dead-letter queue
    /// ([`AcquisitionState::set_dlq_enabled`]), a `Reject` and a `Release` at
    /// the limit give `Archiving` instead. The method then advances the
    /// SPSO over any new terminal prefix and marks the state dirty.
    ///
    /// # Errors
    ///
    /// `INVALID_REQUEST` for a range past the records ever handed out, and
    /// `INVALID_RECORD_STATE` for a range that `member` does not hold.
    pub fn acknowledge(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        ack: AckType,
        max_attempts: i16,
    ) -> Result<(), i16> {
        let Some((first, last)) = self.split_acquired_range(member, first, last)? else {
            return Ok(());
        };
        let (batches, mut archive) = self.runs_and_sink();
        for b in batches {
            if !b.acquired_within(first, last) {
                continue;
            }
            let n = clamp_i32(b.len());
            match ack {
                AckType::Accept => {
                    b.state = RecordState::Acknowledged;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    *archive.delivery_complete_count += n;
                }
                AckType::Release => {
                    // delivery_count retained: next acquire redelivers at +1.
                    give_back(b, max_attempts, &mut archive);
                }
                // Kafka's `recordStateWithDlq`: only a `Reject` goes through
                // the dead-letter queue. A gap has no record to send.
                AckType::Reject => archive.archive(b, DlqCause::ClientReject),
                AckType::Gap => {
                    b.state = RecordState::Archived;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    *archive.delivery_complete_count += n;
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
    use {
        super::*,
        crate::share_partition::state::{
            AcquiredRange,
            test_support::{LOCK, acquired_state, t0},
        },
        assert2::assert,
    };

    #[test]
    fn acquire_then_accept_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100); // [0,4] Available
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(4),
                delivery_count: 1
            }]
        );
        s.acknowledge("m1", Offset(0), Offset(4), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 5);
    }

    #[test]
    fn release_redelivers_with_incremented_count() {
        let mut s = acquired_state(krabka_log::Offset(3));
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, 5)
            .unwrap();
        let acq2 = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq2[0].delivery_count == 2);
        // Released records stay in the window; SPSO did not advance.
        assert!(s.start_offset == 0);
    }

    #[test]
    fn partial_acknowledge_splits_a_batch() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100); // [0,9] Available
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq.len() == 1);
        // Accept only [0,3]; [4,9] remain Acquired.
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 4);
        // The remaining acquired range can still be acknowledged.
        s.acknowledge("m1", Offset(4), Offset(9), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 10);
    }

    #[test]
    fn reject_archives_and_advances_spso() {
        let mut s = acquired_state(krabka_log::Offset(3));
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Reject, 5)
            .unwrap();
        assert!(s.start_offset == 3); // archived prefix dropped
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq.is_empty()); // nothing left
    }

    #[test]
    fn gap_archives() {
        let mut s = acquired_state(krabka_log::Offset(2));
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Gap, 5)
            .unwrap();
        assert!(s.start_offset == 2);
        let (_start, dcc, batches) = s.to_persist_batches();
        assert!(batches.is_empty()); // archived prefix dropped from window
        // Both offsets became terminal, then left the window with the SPSO.
        assert!(dcc == 0);
    }

    #[test]
    fn acknowledge_wrong_member_is_invalid_record_state() {
        let mut s = acquired_state(krabka_log::Offset(3));
        let err = s.acknowledge("m2", Offset(0), Offset(2), AckType::Accept, 5);
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));
    }
}
