//! Growth of the acquisition window and the hand-out of records to a member.
//!
//! This module holds the three steps that put records in front of a share
//! consumer: `materialize` extends the live window with newly produced records,
//! `archive_internal` takes broker-internal offsets such as transaction control
//! batches out of it, and `acquire` walks the window and locks the available
//! runs for one member. The poison-pill rule, which archives a record that has
//! reached the delivery-attempt limit instead of giving it out again, lives
//! here because `acquire` is the only place that counts an attempt.

use std::time::{Duration, Instant};

use krabka_log::Offset;

use super::{AcquiredRange, AcquisitionState, InFlightBatch, RecordState, clamp_i32};

impl AcquisitionState {
    /// Extends the live window with newly produced records.
    ///
    /// When no `Available` record remains in the window and the log has
    /// advanced past `end_offset`, this method appends one `Available` batch
    /// that spans `[end_offset, min(hwm-1, end_offset + max_inflight - 1)]`
    /// and advances `end_offset`. `max_inflight` caps how many records can be
    /// in flight at once. The machine approximates that cap as a record
    /// count.
    ///
    /// A `Deferred` record is not `Available`, so a window that holds only
    /// records the schedule has not released yet does not block this method.
    /// That is what lets a share group reach a due record sitting behind a
    /// waiting one. The caller bounds how far it goes, because a deferred run
    /// is not in flight and so does not spend the in-flight budget itself.
    pub fn materialize(&mut self, hwm: Offset, max_inflight: i32) {
        let has_available = self
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        if has_available || self.end_offset >= hwm {
            return;
        }
        let max_inflight = i64::from(max_inflight.max(1));
        let last = (hwm - 1).min(self.end_offset + max_inflight - 1);
        if last < self.end_offset {
            return;
        }
        self.batches.push(InFlightBatch {
            first_offset: self.end_offset,
            last_offset: last,
            state: RecordState::Available,
            delivery_count: 0,
            acquired_by: None,
            lock_deadline: None,
        });
        self.end_offset = last + 1;
        self.coalesce();
    }

    /// Archives an internal log offset range so it can never be delivered to
    /// a share consumer.
    ///
    /// Transaction control batches occupy offsets in the partition log but
    /// are broker metadata, not user records. The `ShareFetch` handler calls
    /// this after materialization for every control-batch range in the live
    /// window.
    pub fn archive_internal(&mut self, first: Offset, last: Offset) {
        if first > last {
            return;
        }
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        let mut changed = false;
        for batch in &mut self.batches {
            if batch.last_offset < first || batch.first_offset > last {
                continue;
            }
            if matches!(
                batch.state,
                RecordState::Acknowledged | RecordState::Archived
            ) {
                continue;
            }
            self.delivery_complete_count = self
                .delivery_complete_count
                .saturating_add(clamp_i32(batch.len()));
            batch.state = RecordState::Archived;
            batch.acquired_by = None;
            batch.lock_deadline = None;
            changed = true;
        }
        if changed {
            self.dirty = true;
            self.advance_spso();
        }
    }

    /// Acquires up to `max_records` Available records for `member`. It walks
    /// the window from `start_offset`.
    ///
    /// An Available batch whose `delivery_count >= max_attempts` is a bad
    /// record. This method archives it, does not give it out, and advances the
    /// SPSO past it. An Available batch under the limit moves to Acquired. The
    /// method splits the batch when it would exceed `max_records`, sets
    /// `acquired_by` and `lock_deadline`, adds 1 to `delivery_count`, and adds
    /// the batch to the returned ranges. The walk stops once it has acquired
    /// `max_records`.
    ///
    /// A `Deferred` batch is not `Available`, so the walk steps over it as it
    /// steps over an acquired or a terminal one, and it keeps its
    /// `delivery_count`. A record the schedule holds back is therefore never
    /// archived as a poison pill for an attempt nobody made.
    ///
    /// This method accepts `max_bytes` for API symmetry, but approximates it
    /// here with the record count `max_records`. The handler enforces byte
    /// limits at its log-read step, not in this pure machine.
    pub fn acquire(
        &mut self,
        member: &str,
        max_records: i32,
        _max_bytes: i32,
        now: Instant,
        lock_dur: Duration,
        max_attempts: i16,
    ) -> Vec<AcquiredRange> {
        let mut acquired = Vec::new();
        let mut remaining = i64::from(max_records.max(0));
        let mut i = 0;
        let mut any_change = false;
        while i < self.batches.len() {
            if remaining == 0 {
                break;
            }
            if self.batches[i].state != RecordState::Available {
                i += 1;
                continue;
            }
            // Poison pill: archive without handing out.
            if self.batches[i].delivery_count >= max_attempts {
                let n = clamp_i32(self.batches[i].len());
                self.batches[i].state = RecordState::Archived;
                self.batches[i].acquired_by = None;
                self.batches[i].lock_deadline = None;
                self.delivery_complete_count += n;
                any_change = true;
                i += 1;
                continue;
            }
            // Split if the Available run exceeds the remaining budget.
            let avail_len = self.batches[i].len();
            if avail_len > remaining {
                let split_at = self.batches[i].first_offset + remaining;
                self.split_at(i, split_at);
            }
            let b = &mut self.batches[i];
            b.state = RecordState::Acquired;
            b.delivery_count += 1;
            b.acquired_by = Some(member.to_string());
            b.lock_deadline = Some(now + lock_dur);
            acquired.push(AcquiredRange {
                first: b.first_offset,
                last: b.last_offset,
                delivery_count: b.delivery_count,
            });
            remaining -= b.len();
            any_change = true;
            i += 1;
        }
        if any_change {
            self.dirty = true;
            self.advance_spso();
        }
        acquired
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::share_partition::state::test_support::{LOCK, t0};

    #[test]
    fn delivery_limit_archives_poison_pill() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(1), 100);
        for _ in 0..2 {
            // max_attempts = 2
            let _ = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 2);
            s.expire_locks(t0() + Duration::from_secs(31));
        }
        let acq = s.acquire("m1", 10, i32::MAX, t0() + Duration::from_secs(62), LOCK, 2);
        assert!(acq.is_empty()); // archived, not redelivered
        assert!(s.start_offset == 1); // SPSO advanced past the poison pill
    }

    #[test]
    fn internal_offsets_are_archived_before_acquire() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100);
        s.archive_internal(Offset(2), Offset(2));

        let acquired = s.acquire("m1", 10, i32::MAX, t0(), LOCK, 5);
        assert!(
            acquired
                == vec![
                    AcquiredRange {
                        first: Offset(0),
                        last: Offset(1),
                        delivery_count: 1,
                    },
                    AcquiredRange {
                        first: Offset(3),
                        last: Offset(4),
                        delivery_count: 1,
                    },
                ]
        );
        check!(s.delivery_complete_count() == 1);
    }

    #[test]
    fn materialize_respects_max_inflight() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(100), 10); // hwm far ahead, but cap at 10 in flight
        assert!(s.end_offset == 10);
        let acq = s.acquire("m1", 100, i32::MAX, t0(), LOCK, 5);
        assert!(acq[0].first == 0);
        assert!(acq[0].last == 9);
    }

    #[test]
    fn acquire_splits_at_max_records() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let acq = s.acquire("m1", 4, i32::MAX, t0(), LOCK, 5);
        assert!(acq.len() == 1);
        assert!(acq[0].first == 0 && acq[0].last == 3);
        // The remaining [4,9] is still Available.
        let acq2 = s.acquire("m2", 100, i32::MAX, t0(), LOCK, 5);
        assert!(acq2[0].first == 4 && acq2[0].last == 9);
    }
}
