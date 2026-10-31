//! Truncation of the log tail and trimming of the log head.
//!
//! `truncate_to` discards every record at or past an offset, which
//! replication and leader election need after a divergence, and
//! `trim_to_offset` moves the log start forward without touching the
//! active segment.

#[cfg(not(target_os = "wasi"))]
use krabka_units::prelude::ByteSizeExt as _;
use {
    super::Log,
    crate::{
        error::LogError, producer_snapshot, retention::DirSync, segment::Segment,
        txn_index::TxnIndex,
    },
    krabka_ids::Offset,
    std::{
        collections::HashSet,
        sync::atomic::Ordering,
        time::{Instant, SystemTime},
    },
    tracing::{field, instrument},
};

/// What [`Log::remove_sealed_segments`] got off disk.
#[derive(Debug)]
pub(super) struct RemovedSegments {
    /// How many segments left the disk and were forgotten.
    pub(super) deleted: usize,
    /// The error that stopped the run, if one did.
    pub(super) failure: Option<LogError>,
}

impl Log {
    /// Remove sealed segments, their cached indexes, and their producer snapshots.
    /// The snapshot at the first surviving base stays for local and remote reads.
    ///
    /// Unlink first, and forget only what actually left the disk. A failed
    /// unlink otherwise drops the segment from `self.segments` -- and from
    /// `Log::size`, and from the partition's disk gauge -- while its bytes
    /// stay on the filesystem with nothing left to retry them. `bases` is a
    /// prefix, so stopping at the first failure keeps what is forgotten one.
    pub(super) fn remove_sealed_segments(
        &mut self,
        bases: &[Offset],
        now: SystemTime,
    ) -> RemovedSegments {
        let mut deleted: HashSet<Offset> = HashSet::with_capacity(bases.len());
        let mut failure: Option<LogError> = None;
        for &base in bases {
            if let Err(error) = self.retire_segment(base, now) {
                failure = Some(error);
                break;
            }
            deleted.insert(base);
            // Kafka's `deleteSegments` → `deleteProducerSnapshots`: the
            // snapshot at a deleted segment's base goes with it.
            if let Err(error) = producer_snapshot::remove_at(&self.dir, base) {
                failure = Some(error);
                break;
            }
        }
        self.segments
            .retain(|segment| !deleted.contains(&segment.base_offset()));
        self.sealed_txn_indexes
            .retain(|base, _| !deleted.contains(base));
        self.stamp_indexes.retain(|base, _| !deleted.contains(base));
        RemovedSegments {
            deleted: deleted.len(),
            failure,
        }
    }

    /// Retire a discarded tail before dropping its segment handle.
    pub(super) fn remove_truncated_segment_files(&mut self, base: Offset) -> Result<(), LogError> {
        // Producer snapshots are pruned separately by retain_reload_range:
        // the snapshot exactly at the cut must survive deleting its segment.
        let now = SystemTime::now();
        self.retire_files(
            base,
            now,
            &["log", "index", "timeindex", "txnindex", "stampindex"],
            DirSync::Now,
        )
    }

    /// Retire the files of the newest sealed segment, then forget the segment
    /// and its cached transaction and stamp indexes. Does nothing when no
    /// segment is sealed.
    pub(super) fn drop_last_sealed_segment(&mut self) -> Result<(), LogError> {
        let Some(base) = self.segments.last().map(Segment::base_offset) else {
            return Ok(());
        };
        self.remove_truncated_segment_files(base)?;
        self.segments.pop();
        self.sealed_txn_indexes.remove(&base);
        self.stamp_indexes.remove(&base);
        Ok(())
    }

    /// Truncate the log so that no record at offset `>= offset` remains.
    /// Replication and leader election use this method.
    ///
    /// The target can lie below the log start. This is Kafka's
    /// `UnifiedLog.truncateTo`, which never refuses: when no local segment
    /// reaches down to `offset`, the log is reset empty at `offset`
    /// ([`Log::reset_to`], Kafka's `truncateFullyAndStartAt`); otherwise the
    /// log is truncated and the log start moves down to `offset` if it was
    /// above it. A follower whose start sits above the leader's divergence
    /// point, after a long outage and retention or an unclean election, can
    /// therefore always truncate.
    #[instrument(level = "info", skip(self), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn truncate_to(&mut self, offset: Offset) -> Result<(), LogError> {
        self.rollover_flusher.finish()?;
        // Before any file changes: a read planned earlier and still running
        // sees the count move and serves nothing, whatever it has copied.
        self.truncations.fetch_add(1, Ordering::SeqCst);
        let log_end = self.log_end_offset();
        let empty_rolled_active_at_cut = offset == log_end
            && !self.segments.is_empty()
            && self
                .active
                .as_ref()
                .is_some_and(|active| active.base_offset() == offset);
        if offset > log_end || (offset == log_end && !empty_rolled_active_at_cut) {
            // No record to discard, but Kafka's `UnifiedLog.truncateTo` still
            // trims the epoch cache at the log end: a replica that took a
            // leader epoch (`assign_epoch_start_offset` at its log end) and
            // lost leadership before writing keeps an entry no record backs,
            // which would otherwise conflict with the new leader's epoch.
            self.epoch_checkpoint.truncate_from_end(log_end)?;
            return Ok(());
        }
        // The discarded tail may hold the batch that stopped the last
        // activation walk, so what that walk learned about this offset and
        // above no longer describes the log.
        self.invalidate_delivery_schedule(offset);
        if offset < self.first_local_offset() {
            return self.reset_to(offset);
        }

        // Kafka's `truncateAndReload` deletes every snapshot outside
        // `(log start, cut]` on every truncation. Delete them before any
        // segment goes, so a snapshot of the discarded tail can never stand
        // in for the one the next roll at the same offset writes.
        producer_snapshot::retain_reload_range(&self.dir, self.producer_reload_range(offset))?;

        let sealed_bases: Vec<i64> = self
            .segments
            .iter()
            .map(|segment| segment.base_offset().0)
            .collect();
        let plan = krabka_verified::local_truncation_plan(
            &sealed_bases,
            self.active.as_ref().map(|segment| segment.base_offset().0),
            offset.0,
        );

        // Drop exactly the sealed suffix selected by the verified plan.
        while self.segments.len() > plan.retained_sealed {
            self.drop_last_sealed_segment()?;
        }

        // Drop the active segment if its base_offset >= offset.
        if !plan.keep_active
            && let Some(active) = &self.active
        {
            let base = active.base_offset();
            self.remove_truncated_segment_files(base)?;
            self.active = None;
            self.stamp_indexes.remove(&base);
        }

        // If no active segment, promote the last sealed one (if any) and
        // truncate it in place. Otherwise, create a fresh one at `offset`.
        if self.active.is_none() {
            if let Some(mut seg) = self.segments.pop() {
                let base = seg.base_offset();
                let rel = krabka_verified::truncation_relative_offset(base.0, offset.0)
                    .ok_or_else(|| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                let mut active_txn_index =
                    self.sealed_txn_indexes.remove(&base).ok_or_else(|| {
                        LogError::Corrupt(format!(
                            "sealed segment {} has no cached transaction index",
                            base.0
                        ))
                    })?;
                active_txn_index.truncate_from(offset)?;
                self.active_txn_index = active_txn_index;
                let stamp_index_path = seg.stamp_index_path();
                // The promoted segment's pages were handed over while it was
                // active; the pacing goes on from its cut.
                #[cfg(not(target_os = "wasi"))]
                let handed = seg.size().bytes_u64();
                self.active = Some(seg);
                #[cfg(not(target_os = "wasi"))]
                self.restart_writeback_pacing(handed);
                self.stamp_indexes
                    .retain(|segment_base, _| *segment_base <= base);
                self.reopen_active_stamp_index(base, stamp_index_path)?;
                if let Some(index) = self.stamp_indexes.get_mut(&base) {
                    index.truncate_from(offset)?;
                }
            } else {
                let (_, stamp_index_path) = self.install_new_active_segment(offset)?;
                self.stamp_indexes.clear();
                self.reopen_active_stamp_index(offset, stamp_index_path)?;
            }
        } else if let Some(active) = self.active.as_mut()
            && active.last_offset() >= offset
        {
            // The surviving active segment contains records at or past
            // `offset`; truncate them in place.
            let rel = krabka_verified::truncation_relative_offset(active.base_offset().0, offset.0)
                .ok_or_else(|| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(rel)?;
            self.active_txn_index = TxnIndex::open(active.txn_index_path())?;
            self.active_txn_index.truncate_from(offset)?;
            let base = active.base_offset();
            let stamp_index_path = active.stamp_index_path();
            // The pages handed over past the cut are gone; the pacing goes on
            // from the cut or from where it was, whichever comes first.
            #[cfg(not(target_os = "wasi"))]
            let handed = self.writeback_handed.min(active.size().bytes_u64());
            #[cfg(not(target_os = "wasi"))]
            self.restart_writeback_pacing(handed);
            self.reopen_active_stamp_index(base, stamp_index_path)?;
            if let Some(index) = self.stamp_indexes.get_mut(&base) {
                index.truncate_from(offset)?;
            }
        }
        // Kafka's `rebuildProducerState` skips the replay when there were no
        // producers and no unreplicated transactions before the truncation:
        // cutting the log cannot create any.
        if !self.producer_state.is_empty() || !self.unreplicated.is_empty() {
            self.rebuild_producer_and_transaction_state()?;
        }
        // Kafka's `UnifiedLog.truncateTo` rebuilds the producer state and
        // takes a snapshot at the new log end, so a reopen replays nothing.
        self.take_producer_snapshot()?;
        // A verification started against the discarded tail proves nothing
        // about the retained log. A reopen starts without one too.
        self.verification_states.clear();
        let new_end = self.log_end_offset();
        // Every cached visibility frontier is clamped to the actual retained
        // batch prefix, which can end before a cut that lands inside a batch.
        self.lso = Offset(krabka_verified::truncation_frontier(self.lso.0, new_end.0));
        self.invalidate_delivery_schedule(new_end);
        // Drop leader-epoch checkpoint entries for the truncated-away tail so
        // latest_epoch()/end_offset_for_epoch() don't report epochs that no
        // longer have records (mirrors Kafka's truncateFromEnd).
        self.epoch_checkpoint.truncate_from_end(new_end)?;
        // Kafka's `logStartOffset = Math.min(targetOffset, logStartOffset)`.
        // The retained log ends at `new_end`, which sits below `offset` when
        // the cut landed inside a batch, and a start past the end would name
        // records the log no longer holds.
        self.lower_log_start_offset(offset.min(new_end))?;
        self.reap_deleted_files(SystemTime::now())
    }

    /// Trim from the start of the log and return the resulting
    /// `log_start_offset`.
    ///
    /// This method drops every sealed segment whose last offset is
    /// `< target`. It advances `log_start_offset` when `target` falls inside
    /// the active segment. It never deletes the active segment.
    ///
    /// `target` is clamped to `[0, log_end_offset()]`. A caller that asks for
    /// a trim past the LEO gets a trim to the LEO.
    ///
    /// # Errors
    ///
    /// Returns `LogError::InvalidArgument` if `target < 0`.
    #[instrument(
        level = "info",
        skip(self),
        fields(new_log_start = field::Empty),
        err,
    )]
    pub fn trim_to_offset(&mut self, target: Offset) -> Result<Offset, LogError> {
        self.rollover_flusher.finish()?;
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "trim_to_offset: target must be >= 0".into(),
            ));
        }
        let leo = self.log_end_offset();
        let target = target.min(leo);
        let log_start = self.log_start_offset();
        if target <= log_start {
            tracing::Span::current().record("new_log_start", log_start.0);
            return Ok(log_start);
        }

        let to_drop: Vec<Offset> = self
            .sealed_segments_with_next_base()
            .take_while(|sealed| sealed.next_base <= target)
            .map(|sealed| sealed.segment.base_offset())
            .collect();
        if let Some(error) = self
            .remove_sealed_segments(&to_drop, SystemTime::now())
            .failure
        {
            // The records in a deleted prefix are gone, so the floor follows
            // what did come off disk, as retention's does, before the failure
            // is reported. That floor is still below `target`.
            self.follow_retained_segments(Instant::now())?;
            return Err(error);
        }

        // Every dropped segment ended below `target`, so the first offset
        // still in the log is `target` itself: either it falls inside the
        // first surviving sealed segment or inside the active one. The early
        // return above already established `target > log_start`.
        self.set_log_start_offset(target)?;
        let result = self.log_start_offset();
        tracing::Span::current().record("new_log_start", result.0);
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
