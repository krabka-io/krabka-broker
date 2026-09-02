//! Log-level accessors and the whole-log reset.
//!
//! These are the coordinates the broker reads off a partition -- the log
//! start and end, the size, the last stable offset, the producer and
//! transaction state -- together with the config swap and the hard reset
//! that empties the log at a new base offset.

use std::{fs, path::Path};

use krabka_ids::{Offset, ProducerId};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::Log;
use crate::{
    config::LogConfig,
    error::LogError,
    leader_epoch_checkpoint::LeaderEpochCheckpoint,
    name,
    producer_snapshot::{self, ProducerSnapshotEntry},
    segment::Segment,
    txn_index::TxnIndex,
};

impl Log {
    /// Directory this log was opened against. The broker's intra-broker
    /// log-dir reassignment (KIP-113) reads this to find the current owning
    /// `log.dir` of a partition. The broker does not have to repeat the
    /// directory-layout convention.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// First absolute offset still in the log.
    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        let derived = if let Some(first) = self.segments.first() {
            first.base_offset()
        } else if let Some(active) = &self.active {
            active.base_offset()
        } else {
            Offset(0)
        };
        if let Some(o) = self.start_offset_override {
            return derived.max(o);
        }
        derived
    }

    /// Advance `log_start_offset` to `new_start`.
    ///
    /// `new_start` must be in `[current log_start, log_end]`.
    /// `trim_to_offset` uses this method for the active-segment case, and the
    /// broker's `DeleteRecords` handler uses it too. This method does NOT
    /// truncate on-disk segments. It only moves the in-memory start pointer.
    ///
    /// `new_start` must be non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `new_start` is negative.
    pub fn set_log_start_offset(&mut self, new_start: Offset) -> Result<(), LogError> {
        if new_start < 0 {
            return Err(LogError::InvalidArgument(
                "set_log_start_offset: new_start must be >= 0".into(),
            ));
        }
        self.start_offset_override = Some(new_start);
        Ok(())
    }

    /// Reset the log to be empty at `new_base`.
    ///
    /// This method drops every segment and every on-disk file, then creates
    /// a fresh active segment at `new_base`. The replicator's
    /// `OFFSET_OUT_OF_RANGE` recovery path uses it when the follower has
    /// fallen behind the leader's `log_start`. `truncate_to` cannot help
    /// there, because `log_start` must move *forward* past the point where
    /// no local data exists.
    #[instrument(level = "info", skip_all, fields(new_base = new_base.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn reset_to(&mut self, new_base: Offset) -> Result<(), LogError> {
        if new_base < 0 {
            return Err(LogError::OffsetMismatch {
                expected: Offset(0),
                actual: new_base,
            });
        }

        producer_snapshot::remove_all(&self.dir)?;
        self.invalidate_delivery_schedule(new_base);

        // Drop every sealed segment + its on-disk files.
        while let Some(popped) = self.segments.pop() {
            let base = popped.base_offset();
            drop(popped);
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
        }

        // Drop the active segment + its on-disk files.
        if let Some(active) = self.active.take() {
            let base = active.base_offset();
            drop(active);
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
        }

        // Clear the start override so the derived value takes over.
        self.start_offset_override = None;

        let mut new_active = Segment::create(&self.dir, new_base)?;
        new_active.set_io(self.io.clone());
        self.active_txn_index = TxnIndex::open(new_active.txn_index_path())?;
        let stamp_index_path = new_active.stamp_index_path();
        self.pending.clear(); // reset_to is a hard reset (after divergence)
        self.pending_stamp_ranges.clear();
        self.coordinator_epochs.clear();
        self.producer_state.clear();
        self.sealed_txn_indexes.clear();
        self.stamp_indexes.clear();
        self.lso = new_active.last_offset() + 1; // = new_base (empty segment)
        self.active = Some(new_active);
        self.dir_sync_needed = true;
        // Preserve any injected stamp source; reopen its (fresh) sidecar.
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        // The log now holds no records, so the leader-epoch cache must hold no
        // entries (Kafka's truncateFullyAndStartAt → leaderEpochCache.clearAndFlush).
        // Leaving stale entries makes a follower advertise a `last_fetched_epoch`
        // it has no record for, so the leader's KIP-320 reconciliation serves a
        // batch at a mismatched base offset and the follower loops forever on
        // append_at — a phantom ISR member that pins the high-watermark.
        self.epoch_checkpoint.clear()?;
        Ok(())
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        Offset(0)
    }

    /// Total `.log` size across sealed and active segments.
    ///
    /// The value comes from the segments' tracked logical size, not from a
    /// filesystem stat. It therefore shows buffered appends immediately and
    /// in the same way on every platform. On some operating systems a
    /// directory stat can lag an open, unflushed write handle.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        let active = self.active.as_ref().map_or(ByteSize::ZERO, Segment::size);
        self.segments
            .iter()
            .fold(active, |total, seg| total + seg.size())
    }

    /// Last-Stable-Offset: the highest offset that consumers in
    /// `read_committed` isolation may see. Advances only when no
    /// transactions are in flight; held back at the first offset of any
    /// open (uncommitted/unaborted) transactional batch.
    #[must_use]
    pub fn lso(&self) -> Offset {
        self.lso
    }

    /// First offset of `producer_id`'s currently open transaction on this
    /// partition, or `None` when no transaction from that producer is pending.
    #[must_use]
    pub fn pending_transaction_start(&self, producer_id: ProducerId) -> Option<Offset> {
        self.pending.get(&producer_id).copied()
    }

    /// Transaction fields reported by `DescribeProducers` for `producer_id`.
    ///
    /// The coordinator epoch is the last epoch embedded in a durable end
    /// marker, or `-1` before the first marker. The start offset is present
    /// only while a transaction is open on this partition.
    #[must_use]
    pub fn producer_transaction_state(&self, producer_id: ProducerId) -> (i32, Option<Offset>) {
        (
            self.coordinator_epochs
                .get(&producer_id)
                .copied()
                .unwrap_or(-1),
            self.pending_transaction_start(producer_id),
        )
    }

    /// Producer and coordinator generations used to admit one transaction
    /// marker, plus whether that producer currently has an open transaction.
    /// Missing generations use Kafka's `-1` sentinel.
    #[must_use]
    pub fn transaction_marker_state(&self, producer_id: ProducerId) -> (i16, i32, bool) {
        let producer_epoch = self
            .producer_state
            .get(&producer_id)
            .map_or(-1, |entry| entry.producer_epoch);
        let coordinator_epoch = self
            .coordinator_epochs
            .get(&producer_id)
            .copied()
            .unwrap_or(-1);
        (
            producer_epoch,
            coordinator_epoch,
            self.pending.contains_key(&producer_id),
        )
    }

    /// Producer state restored from the newest valid Kafka-compatible
    /// snapshot and the uncovered local log tail.
    #[must_use]
    pub fn producer_state_snapshot(&self) -> Vec<ProducerSnapshotEntry> {
        self.producer_state.values().copied().collect()
    }

    /// Close all segments. Drop runs automatically when `self` moves;
    /// this method just names the operation explicitly.
    pub fn close(self) {
        drop(self);
    }

    /// Atomically swap the active `LogConfig`.
    ///
    /// The next retention or roll check reads the new value. In-flight
    /// `append` calls hold the lock for very short windows and will not see
    /// a half-applied config.
    ///
    /// Callers can use this method through `&self`. The `Arc<RwLock<…>>`
    /// wrapper permits mutation of the inner value without an exclusive
    /// borrow on the `Log`.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn set_config(&self, new: LogConfig) {
        *self.config.write().unwrap() = new;
    }

    /// Snapshot the current config. This allocates a clone, which is cheap
    /// because `LogConfig` is small and `Clone`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn config_snapshot(&self) -> LogConfig {
        self.config.read().unwrap().clone()
    }

    /// Replace the active log-file I/O implementation for fault-injection tests.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_io(&mut self, io: std::sync::Arc<dyn crate::io::LogIo>) {
        self.io = io.clone();
        for segment in &mut self.segments {
            segment.set_io(io.clone());
        }
        if let Some(active) = &mut self.active {
            active.set_io(io);
        }
    }

    /// Return all aborted transactions whose offset range overlaps
    /// `[start, end)`, including entries in sealed segments.
    #[must_use]
    pub fn aborted_in_range(
        &self,
        start: Offset,
        end: Offset,
    ) -> Vec<crate::txn_index::AbortedTxn> {
        let mut aborted = Vec::new();
        if let Some(first_base) = self
            .segments
            .iter()
            .find(|segment| segment.last_offset() >= start)
            .map(Segment::base_offset)
        {
            for index in self
                .sealed_txn_indexes
                .range(first_base..)
                .map(|(_, index)| index)
            {
                aborted.extend(index.aborted_in_range(start, end).copied());
            }
        }
        aborted.extend(self.active_txn_index.aborted_in_range(start, end).copied());
        aborted
    }

    /// Access the per-partition leader-epoch checkpoint.
    #[must_use]
    pub fn epoch_checkpoint(&self) -> &LeaderEpochCheckpoint {
        &self.epoch_checkpoint
    }

    /// Reconcile append-at offset assignment to an external next-offset frontier.
    ///
    /// Diskless partitions use the `KRaft` metadata log as the offset authority.
    /// After a crash, `KRaft` may have committed a next-offset that is ahead of the
    /// recovered local WAL tail. In that case the gap is intentional: the caller
    /// sets this frontier and the next append-at must use it instead of the local
    /// LEO. Classic logs never call this method and keep the default frontier 0.
    pub fn reconcile_next_offset(&mut self, frontier: Offset) {
        self.reconciled_frontier = self.reconciled_frontier.max(frontier);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use krabka_units::prelude::{kibibytes, minutes};
    use tempfile::tempdir;

    use super::*;
    use crate::log::test_support::{sample_batch, sample_batch_with_epoch};

    /// A hard reset leaves the log empty at the new base, with the last stable
    /// offset there too.
    ///
    /// `lso` is derived from the fresh segment rather than from the base it was
    /// asked for, and it gates what a `read_committed` consumer may see -- one
    /// short of the base would expose an offset the log does not have.
    #[test]
    fn a_reset_puts_the_stable_offset_at_the_new_base() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..3 {
            let mut batch = sample_batch(2);
            log.append(&mut batch).expect("append");
        }
        check!(log.log_end_offset() == Offset(6));

        log.reset_to(Offset(50)).expect("reset");
        check!(
            log.log_start_offset() == Offset(50),
            "starts at the new base"
        );
        check!(log.log_end_offset() == Offset(50), "and is empty there");
        check!(
            log.lso() == Offset(50),
            "the stable offset is the base, got {:?}",
            log.lso()
        );
    }

    /// Zero is a legal log start; only a negative one is rejected.
    #[test]
    fn the_log_start_may_be_set_to_zero_but_not_below() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        check!(
            log.set_log_start_offset(Offset(0)).is_ok(),
            "zero is a real offset"
        );
        check!(log.set_log_start_offset(Offset(7)).is_ok());
        check!(
            log.set_log_start_offset(Offset(-1)).is_err(),
            "negative is not"
        );
    }

    /// The log's size is every segment's size added up, the sealed ones as
    /// well as the active one.
    #[test]
    fn log_size_sums_the_sealed_segments_and_the_active_one() {
        let dir = tempdir().unwrap();
        // A tiny segment cap, so appending rolls and leaves sealed segments
        // behind the active one -- with only an active segment the fold has
        // nothing to add and the accumulator is returned untouched.
        let config = LogConfig {
            segment_size: kibibytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..40 {
            let mut batch = sample_batch(4);
            log.append(&mut batch).expect("append");
        }
        check!(
            !log.segments.is_empty(),
            "the appends should have rolled a segment"
        );

        let expected = log.segments.iter().map(Segment::size).fold(
            log.active.as_ref().map_or(ByteSize::ZERO, Segment::size),
            |a, b| a + b,
        );
        check!(
            log.size() == expected,
            "size {:?}, expected {:?}",
            log.size(),
            expected
        );
        check!(
            log.size() > kibibytes(1),
            "several segments should exceed one"
        );
    }

    #[test]
    fn dir_returns_open_path() {
        // The broker's KIP-113 move machinery reads this back to
        // determine a partition's current owning `log.dir` without
        // re-implementing the directory-layout convention.
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.dir() == dir.path());
    }

    #[test]
    fn reset_to_clears_leader_epoch_checkpoint() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // A follower that replicated real data builds an epoch history.
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        log.append(&mut sample_batch_with_epoch(2, 2)).unwrap(); // epoch 2 @ 3
        log.append(&mut sample_batch_with_epoch(1, 5)).unwrap(); // epoch 5 @ 5
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(5)));

        // Hard reset to an empty log — the replicator's OFFSET_OUT_OF_RANGE
        // recovery path (Kafka's `truncateFullyAndStartAt`). The log now has
        // NO records, so it must advertise NO leader epoch. Otherwise the
        // follower keeps sending a stale `last_fetched_epoch` and the leader's
        // KIP-320 reconciliation serves a batch at a mismatched base offset,
        // looping forever on `append_at` (phantom ISR member → pinned HW →
        // acks=all stall).
        log.reset_to(Offset(0)).unwrap();

        assert2::assert!(log.epoch_checkpoint().latest_epoch() == None);
        assert2::assert!(log.epoch_checkpoint().entries() == &[][..]);
        // The cleared state must survive a reopen (a restarted broker re-reads
        // the on-disk checkpoint file).
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(reopened.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn reset_to_nonzero_base_clears_all_epochs_not_just_tail() {
        // Guards against the subtly-wrong fix `truncate_from_end(new_base)`,
        // which retains an entry whose `start_offset < new_base` even though
        // the reset log holds no records below `new_base`.
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
        log.reset_to(Offset(1000)).unwrap(); // empty log starting at 1000
        assert2::assert!(log.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn set_config_swaps_active_config() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(
            dir.path(),
            LogConfig {
                retention: Some(minutes(1)),
                ..LogConfig::default()
            },
        )
        .expect("open");
        log.set_config(LogConfig {
            retention: Some(minutes(2)),
            ..LogConfig::default()
        });
        assert2::assert!(log.config_snapshot().retention == Some(minutes(2)));
    }
}
