//! Opening a log directory and rebuilding the producer, transaction, and
//! stamp state that no sidecar file holds.
//!
//! Recovery restores the newest valid producer snapshot and then replays
//! the log tail that snapshot does not cover, so a reopened log reaches
//! exactly the state the append path would have left.

#[cfg(not(target_os = "wasi"))]
use std::collections::VecDeque;
use {
    super::{
        Log,
        control::{
            ControlBatchKind, TransactionMarkerFlags, control_batch_kind, transaction_marker_flags,
        },
        pending_transactions::PendingTransactions,
        producer_state::ProducerStateManager,
    },
    crate::{
        config::LogConfig,
        error::LogError,
        io::FileIo,
        leader_epoch_checkpoint::LeaderEpochCheckpoint,
        log_start_offset_checkpoint, name,
        producer_snapshot::{self, LoadedSnapshot},
        recovery::{deleted_orphan_recover, swap_orphan_recover},
        segment::Segment,
        stamp_index::OffsetRange,
        txn_index::TxnIndex,
    },
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::RecordBatch,
    krabka_verified::{ProducerReloadRange, increment_sequence},
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        fs,
        path::Path,
        sync::{Arc, RwLock, atomic::AtomicU64},
        time::Instant,
    },
    tracing::{field, instrument},
};

/// The last offset of a replayed batch and the offset the replay cursor
/// advances to after it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RecoveredBatchOffsets {
    pub(super) last: Offset,
    pub(super) advanced_to: Offset,
}

/// The sequence and offset a producer's entry ends at after one data batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProducerTail {
    pub(super) last_sequence: i32,
    pub(super) last_offset: Offset,
}

/// One data batch a producer appended, as the producer state records it.
#[derive(Debug, Clone, Copy)]
pub(super) struct DataProducerAppend {
    pub(super) producer_id: ProducerId,
    pub(super) producer_epoch: i16,
    pub(super) base_sequence: i32,
    pub(super) last_offset_delta: i32,
    pub(super) base_offset: Offset,
    pub(super) timestamp: i64,
    pub(super) is_transactional: bool,
}

impl Log {
    /// Open or create a `Log` at `dir`.
    ///
    /// This method finds existing segments by `.log` filename and marks all
    /// but the latest as sealed. If the directory is empty, it creates a
    /// fresh active segment at offset 0.
    #[instrument(
        level = "info",
        skip_all,
        fields(
            dir = %dir.as_ref().display(),
            segments = field::Empty,
            log_end = field::Empty,
        ),
        err,
    )]
    // cargo-mutants: the only mutant here is the `segments.len() + 1` in the `span.record`
    // call, a tracing-span diagnostic field with no behavioral effect. The
    // sibling `seal_at(next_base - 1)` recovery arithmetic is separately pinned
    // by `reopen_seals_recovered_segments_at_next_base_minus_one`.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Heal any orphaned compaction `.swap` files before
        // we scan the directory for segments.
        swap_orphan_recover(&dir)?;
        // …and reclaim what an interrupted segment deletion left: `.deleted`
        // tombstones, and sidecars whose `.log` the scan below can no longer
        // see. Both are invisible to every later pass otherwise.
        deleted_orphan_recover(&FileIo, &dir)?;

        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let Ok(file_name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 names: ignore (unlikely)
            };
            if let Ok(base) = name::parse_log_filename(&file_name) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort_unstable();
        base_offsets.dedup();
        if !krabka_verified::local_recovery_segment_chain(&base_offsets) {
            return Err(LogError::Corrupt(
                "log segment bases are not a nonnegative ordered chain".into(),
            ));
        }

        let mut segments: Vec<Segment> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            if let Some(&next_base) = base_offsets.get(i + 1) {
                let mut seg = Segment::open_unscanned(&dir, Offset(*base))?;
                // The no-scan loader leaves
                // `last_offset = base - 1`. A sealed segment's true last offset
                // is one below the next segment's base; set it so `read_raw`
                // (which skips a segment whose `last_offset() < fetch_offset`)
                // doesn't skip this recovered segment and serve a later base
                // offset — which after a restart manufactures an offset gap that
                // strands a follower fetching from a low offset.
                let last = krabka_verified::local_recovery_sealed_last(*base, next_base)
                    .ok_or_else(|| LogError::Corrupt("invalid sealed segment boundary".into()))?;
                seg.seal_at(Offset(last));
                // The no-scan loader also leaves `max_timestamp` unknown. Without
                // this restore, retention would age every reopened segment by
                // its file's modification time -- Kafka's `largestTimestamp()`
                // fallback for a segment with no record timestamp -- instead
                // of by its newest record.
                seg.restore_max_timestamp()?;
                segments.push(seg);
            } else {
                active = Some(Segment::open_active_with_index_interval(
                    &dir,
                    Offset(*base),
                    config.validate_on_open,
                    config.index_interval,
                )?);
            }
        }

        let (active, dir_sync_needed) = match active {
            // We cannot know whether the process that created this segment
            // fsynced the parent directory before crashing. Conservatively
            // require one directory fsync on the next explicit `sync()` so a
            // diskless WAL ack never relies only on file data durability.
            Some(s) => (s, true),
            None => (Segment::create(&dir, Offset(0))?, true),
        };

        let sealed_txn_indexes = segments
            .iter()
            .map(|segment| {
                Ok((
                    segment.base_offset(),
                    TxnIndex::open(segment.txn_index_path())?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LogError>>()?;
        let active_txn_index = TxnIndex::open(active.txn_index_path())?;
        let mut epoch_checkpoint =
            LeaderEpochCheckpoint::open(active.leader_epoch_checkpoint_path())?;
        // LSO starts at log_end_offset(); computed before moving `active`.
        let lso = active.last_offset() + 1;
        epoch_checkpoint.truncate_from_end(lso)?;

        let remote_storage_enable = config.remote_storage_enable;
        let config = Arc::new(RwLock::new(config));

        let span = tracing::Span::current();
        span.record("segments", segments.len() + 1);
        span.record("log_end", lso.0);

        // The segment names witness only where the surviving files begin. The
        // checkpoint below carries the rest, and is what turns this into a
        // floor somebody deleted up to.
        let start_offset = segments
            .first()
            .map_or_else(|| active.base_offset(), Segment::base_offset);

        let mut log = Self {
            dir,
            config,
            io: Arc::new(FileIo),
            segments,
            active: Some(active),
            dir_sync_needed,
            rollover_flusher: super::rollover_flush::Flusher::default(),
            compaction_gate: Arc::default(),
            fenced: false,
            opened_end: Offset(0),
            unflushed_messages: 0,
            last_flush: Instant::now(),
            #[cfg(not(target_os = "wasi"))]
            writeback_unsynced: 0,
            #[cfg(not(target_os = "wasi"))]
            writeback_jobs: VecDeque::new(),
            #[cfg(not(target_os = "wasi"))]
            writeback_handed: 0,
            truncations: Arc::new(AtomicU64::new(0)),
            roll_jitter: None,
            pending_deletes: Vec::new(),
            start_offset,
            // Derived from the files on disk, not deleted up to by anyone.
            // The checkpoint restore below is what may set it: see
            // `Log::established_log_start`.
            start_offset_established: false,
            checkpointed_start: None,
            start_checkpoint_due: None,
            // Nothing has cleaned this log yet, so every sealed segment is
            // dirty until the first pass says otherwise.
            clean_prefix_segments: 0,
            lso,
            pending: PendingTransactions::default(),
            unreplicated: BTreeMap::new(),
            pending_stamp_ranges: HashMap::new(),
            producers: ProducerStateManager::default(),
            active_txn_index,
            sealed_txn_indexes,
            stamp_source: None,
            stamp_indexes: BTreeMap::new(),
            epoch_checkpoint,
            reconciled_frontier: Offset(0),
            delivery_watermark: Offset(0),
            delivery_pending_ms: None,
        };
        // Restore a log start that the segment names cannot express: a trim
        // that landed inside a segment left its records on disk, and only the
        // checkpoint says they are gone.
        let checkpoint = log_start_offset_checkpoint::read(&log.dir)?;
        log.checkpointed_start = checkpoint;
        if let Some(checkpointed) = checkpoint {
            // Cap at the log end. Above it, the whole surviving log is below a
            // start that was acknowledged, so every record left is one the
            // trim removed and the log start is the log end -- a crash between
            // a trim and the fsync of the records it trimmed past lands there.
            let capped = checkpointed.min(log.log_end_offset());
            // Kafka's `LogLoader.load`: without remote storage the start is
            // never below the first segment, whose base offset is the start
            // derived above. Retention writes the start it moves on a timer,
            // so a crash before the timer leaves an older, lower value here.
            //
            // A tiered partition (KIP-405) gets no such floor. Its global
            // start belongs *below* the oldest local segment, in the band the
            // archive serves, and raising the checkpoint to meet the surviving
            // files would hide every offset the remote tier still holds.
            // `Log::local_log_start_offset` is the floor that follows the
            // files there.
            let effective = if remote_storage_enable {
                capped
            } else {
                capped.max(log.start_offset)
            };
            log.start_offset = effective;
            // A checkpoint exists, so somebody deleted up to this floor: it is
            // one a reader may be refused against and remote retention may
            // delete against.
            log.start_offset_established = true;
            if capped != checkpointed {
                // Persist what was resolved instead of leaving the
                // out-of-range value on disk. It is inert against this log,
                // but appends move the log end, and the next open would find
                // the stale value back in range and hide live records.
                log.write_start_checkpoint(effective)?;
            } else if effective != capped {
                // A start raised to the first segment goes out on the timer
                // instead: the segment names record it, and every open of this
                // log raises it again until then.
                log.defer_start_checkpoint(effective, Instant::now());
            }
            // Kafka's `LogLoader.load` → `truncateFromStart(logStartOffsetCheckpoint)`:
            // a crash can land between a start increment and the epoch-cache
            // rewrite that followed it, so the reload trims the cache again.
            log.epoch_checkpoint.truncate_from_start(effective)?;
        }
        // Kafka's `LogLoader.load` first drops every snapshot no segment
        // accounts for, then reloads against the log start it just restored,
        // so producer state is rebuilt only after the checkpoint is applied.
        let segment_bases: Vec<i64> = log
            .segments
            .iter()
            .chain(log.active.iter())
            .map(|segment| segment.base_offset().0)
            .collect();
        producer_snapshot::remove_strays(&log.dir, &segment_bases)?;
        log.rebuild_producer_and_transaction_state()?;
        // Recovery needs no durable watermark: the schedule is in the records,
        // so the first advance rebuilds it from the log start.
        log.delivery_watermark = log.log_start_offset();
        log.opened_end = log.log_end_offset();
        Ok(log)
    }

    /// The range a producer-state reload up to `log_end` runs against.
    ///
    /// The log start is the one Kafka's `LogLoader.load` hands
    /// `truncateAndReload`: on a tiered partition (KIP-405) the checkpointed
    /// floor, read as 0 when nobody established one, and otherwise the
    /// greater of that floor and the oldest local segment's base.
    pub(super) fn producer_reload_range(&self, log_end: Offset) -> ProducerReloadRange {
        let local_start = self.local_log_start_offset();
        ProducerReloadRange {
            log_start: krabka_verified::producer_snapshot_reload_log_start(
                self.config().remote_storage_enable,
                self.established_log_start().map(|start| start.0),
                local_start.0,
            ),
            local_start: local_start.0,
            log_end: log_end.0,
        }
    }

    /// Rebuild producer state the way Kafka's
    /// `UnifiedLog.rebuildProducerState` does: reload the newest snapshot in
    /// `(log start, log end]`, deleting every snapshot outside that range,
    /// then replay the log tail the snapshot does not cover.
    ///
    /// A producer whose last batch lies below the log start is kept only when
    /// a surviving snapshot carries it, exactly as in Kafka. Missing boundary
    /// snapshots are created during the replay so every sealed segment can be
    /// copied to remote storage with its matching producer state, and a
    /// snapshot is taken at the log end once the replay is done.
    pub(super) fn rebuild_producer_and_transaction_state(&mut self) -> Result<(), LogError> {
        self.clear_producer_and_transaction_state();
        let end = self.log_end_offset();
        let range = self.producer_reload_range(end);
        let snapshot = producer_snapshot::reload(&self.dir, range)?;
        let snapshot_offset = snapshot.as_ref().map(|loaded| loaded.offset.0);
        if let Some(LoadedSnapshot { state, .. }) = snapshot {
            self.producers.entries = state;
            for (&producer_id, entry) in &self.producers.entries {
                if let Some(first_offset) = entry.current_txn_first_offset {
                    self.pending.insert(producer_id, first_offset);
                }
            }
        }
        let mut next = krabka_verified::producer_snapshot_replay_start(range, snapshot_offset)
            .map(Offset)
            .ok_or_else(|| LogError::Corrupt("invalid producer snapshot replay frontier".into()))?;

        let mut boundaries: BTreeSet<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(self.active.iter().map(Segment::base_offset))
            .collect();
        let mut boundaries = boundaries.split_off(&next);
        let _ = boundaries.remove(&next);
        let mut first_read = true;
        while next < end {
            let read = self.read(next, krabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery made no progress at offset {next}"
                )));
            }
            let mut advanced_to = next;
            for (index, batch) in read.batches.iter().enumerate() {
                let cursor = Self::replay_cursor(first_read && index == 0, advanced_to, batch);
                advanced_to = Self::recovered_batch_offsets(cursor, end, batch)?.advanced_to;
                self.apply_recovered_batch_state(batch)?;
                let covered: Vec<_> = boundaries.range(..=advanced_to).copied().collect();
                for boundary in covered {
                    producer_snapshot::write(
                        &*self.io,
                        &self.dir,
                        boundary,
                        &self.producers.entries,
                    )?;
                    let _ = boundaries.remove(&boundary);
                }
            }
            if advanced_to <= next {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery did not advance past offset {next}"
                )));
            }
            next = advanced_to;
            first_read = false;
        }
        // Kafka's `rebuildProducerState` ends with `updateMapEndOffset(lastOffset)`
        // and `takeSnapshot()`, which writes a snapshot at the log end unless
        // the map end is no further than the last snapshot taken. The next
        // reload then starts from here instead of replaying the tail again.
        // `write` keeps a snapshot already at `end`, which a replay boundary
        // or the loaded snapshot put there with this same state.
        if end.0 > range.log_start {
            producer_snapshot::write(&*self.io, &self.dir, end, &self.producers.entries)?;
        }
        self.rebuild_pending_stamp_ranges(|_| {})?;
        self.refresh_lso()?;
        Ok(())
    }

    /// The cursor a replayed batch must start at or after.
    ///
    /// The replay can start inside a batch, where a trim left the log start.
    /// Kafka's `LogSegment.read(startOffset, ..)` then begins with the batch
    /// that holds `startOffset`, so the first batch of a replay may start
    /// below the cursor. Every later batch must start at or after it.
    fn replay_cursor(first: bool, cursor: Offset, batch: &RecordBatch) -> Offset {
        if first {
            cursor.min(Offset(batch.base_offset))
        } else {
            cursor
        }
    }

    fn apply_recovered_batch_state(&mut self, batch: &RecordBatch) -> Result<(), LogError> {
        if control_batch_kind(batch) == Some(ControlBatchKind::Barrier) {
            // The append path keeps no producer state and no transaction state
            // for a barrier marker. Recovery reaches the same result.
            return Ok(());
        }
        let producer_id = ProducerId(batch.producer_id);
        if producer_id.get() < 0 {
            return Ok(());
        }
        self.update_owned_producer_entry(batch)?;
        if batch.attributes.is_control_batch() {
            let TransactionMarkerFlags {
                is_abort,
                is_commit,
            } = transaction_marker_flags(batch)?;
            if krabka_verified::transaction_marker_closes(
                is_abort,
                is_commit,
                self.pending.contains_key(producer_id),
            ) && let Some(first_offset) = self.pending.remove(producer_id)
            {
                self.unreplicated.insert(
                    first_offset,
                    Offset(batch.base_offset + i64::from(batch.last_offset_delta)),
                );
            }
        } else if batch.attributes.is_transactional() {
            self.pending
                .insert_first(producer_id, Offset(batch.base_offset));
        }
        Ok(())
    }

    /// Rebuild the stamp ranges of the transactions that survived
    /// producer-state recovery as open, from the earliest of their first
    /// offsets. `on_batch` sees each batch the walk reads.
    fn rebuild_pending_stamp_ranges(
        &mut self,
        mut on_batch: impl FnMut(&RecordBatch),
    ) -> Result<(), LogError> {
        self.pending_stamp_ranges.clear();
        if self.pending.is_empty() {
            return Ok(());
        }
        // The snapshot tells us exactly which transactions remain open and
        // where each began. Nothing before the earliest surviving beginning
        // can contribute a range to those transactions. Clamp that beginning
        // to the oldest local segment for tiered retention and truncation.
        // `read` deliberately returns a batch covering this floor, so a first
        // offset inside a retained batch still reconstructs the whole range.
        let local_start = self.local_log_start_offset();
        let mut next = self
            .pending
            .values()
            .copied()
            .min()
            .unwrap_or(local_start)
            .max(local_start);
        let end = self.log_end_offset();
        let mut is_first_read = true;
        while next < end {
            let slice = self.read(next, krabka_units::mebibytes(1))?;
            if slice.batches.is_empty() {
                let msg = format!("transaction-stamp recovery made no progress at offset {next}");
                return Err(LogError::Corrupt(msg));
            }
            for (index, batch) in slice.batches.iter().enumerate() {
                let producer_id = ProducerId(batch.producer_id);
                let cursor = Self::replay_cursor(is_first_read && index == 0, next, batch);
                let RecoveredBatchOffsets { last, advanced_to } =
                    Self::recovered_batch_offsets(cursor, end, batch)?;
                on_batch(batch);
                match control_batch_kind(batch) {
                    // A barrier marker closes no transaction, so it clears no
                    // stamp range. The append path reaches the same result.
                    Some(ControlBatchKind::Barrier) => {}
                    Some(ControlBatchKind::Transaction) => {
                        self.pending_stamp_ranges.remove(&producer_id);
                    }
                    None => {
                        if batch.attributes.is_transactional()
                            && producer_id.get() >= 0
                            && self.pending.contains_key(producer_id)
                        {
                            self.pending_stamp_ranges
                                .entry(producer_id)
                                .or_default()
                                .push(OffsetRange {
                                    first: Offset(batch.base_offset),
                                    last,
                                });
                        }
                    }
                }
                next = advanced_to;
            }
            is_first_read = false;
        }
        Ok(())
    }

    fn recovered_batch_offsets(
        current: Offset,
        end: Offset,
        batch: &RecordBatch,
    ) -> Result<RecoveredBatchOffsets, LogError> {
        match krabka_verified::replay_batch_cursor_decision(
            current.0,
            end.0,
            Some((batch.base_offset, batch.last_offset_delta)),
        ) {
            krabka_verified::ReplayCursorDecision::Advance(next) => Ok(RecoveredBatchOffsets {
                last: Offset(next - 1),
                advanced_to: Offset(next),
            }),
            krabka_verified::ReplayCursorDecision::Stop => Err(LogError::Corrupt(format!(
                "log recovery rejected batch at offset {current} before end {end}"
            ))),
        }
    }

    pub(super) fn update_owned_producer_entry(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), LogError> {
        let producer_id = ProducerId(batch.producer_id);
        if producer_id.get() < 0 {
            return Ok(());
        }
        if batch.attributes.is_control_batch() {
            return self.producers.apply_marker(producer_id, batch);
        }
        self.update_data_producer_entry(DataProducerAppend {
            producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            last_offset_delta: batch.last_offset_delta,
            base_offset: Offset(batch.base_offset),
            timestamp: batch.max_timestamp,
            is_transactional: batch.attributes.is_transactional(),
        })
    }

    pub(super) fn update_data_producer_entry(
        &mut self,
        append: DataProducerAppend,
    ) -> Result<(), LogError> {
        let Some(tail) = Self::data_producer_tail(
            append.producer_id,
            append.base_sequence,
            append.last_offset_delta,
            append.base_offset,
        )?
        else {
            return Ok(());
        };
        self.producers.apply_data_batch(append, tail);
        Ok(())
    }

    pub(super) fn data_producer_tail(
        producer_id: ProducerId,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: Offset,
    ) -> Result<Option<ProducerTail>, LogError> {
        if producer_id.get() < 0 || base_sequence < 0 {
            return Ok(None);
        }
        if last_offset_delta < 0 {
            return Err(LogError::InvalidArgument(format!(
                "negative producer offset delta for producer {producer_id}"
            )));
        }
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let last_offset = base_offset
            .0
            .checked_add(i64::from(last_offset_delta))
            .map(Offset)
            .ok_or_else(|| {
                LogError::InvalidArgument(format!(
                    "producer offset overflow for producer {producer_id}"
                ))
            })?;
        Ok(Some(ProducerTail {
            last_sequence,
            last_offset,
        }))
    }
}

#[cfg(test)]
mod tests;
