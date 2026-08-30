//! Opening a log directory and rebuilding the producer, transaction, and
//! stamp state that no sidecar file holds.
//!
//! Recovery restores the newest valid producer snapshot and then replays
//! the log tail that snapshot does not cover, so a reopened log reaches
//! exactly the state the append path would have left.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::Path,
};

use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::RecordBatch;
use krabka_verified::increment_sequence;
use tracing::instrument;

use super::{
    Log,
    control::{ControlBatchKind, control_batch_kind, parse_control_marker_coordinator_epoch},
};
use crate::{
    config::LogConfig,
    error::LogError,
    io::FileIo,
    leader_epoch_checkpoint::LeaderEpochCheckpoint,
    name,
    producer_snapshot::{self, ProducerSnapshotEntry},
    segment::Segment,
    txn_index::TxnIndex,
};

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
            segments = tracing::field::Empty,
            log_end = tracing::field::Empty,
        ),
        err,
    )]
    // The only mutant here is the `segments.len() + 1` in the `span.record`
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
        crate::recovery::swap_orphan_recover(&dir)?;

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

        let mut segments: Vec<Segment> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            if i + 1 < base_offsets.len() {
                let mut seg = Segment::open(&dir, Offset(*base))?;
                // `Segment::open` is a no-scan load that leaves
                // `last_offset = base - 1`. A sealed segment's true last offset
                // is one below the next segment's base; set it so `read_raw`
                // (which skips a segment whose `last_offset() < fetch_offset`)
                // doesn't skip this recovered segment and serve a later base
                // offset — which after a restart manufactures an offset gap that
                // strands a follower fetching from a low offset.
                seg.seal_at(Offset(base_offsets[i + 1] - 1));
                // `Segment::open` also leaves `max_timestamp` unknown, and
                // `retention::time_based_evict` reads it as "older than any
                // cutoff". Without this the first tick after a restart deletes
                // every sealed segment.
                seg.restore_max_timestamp()?;
                segments.push(seg);
            } else {
                active = Some(Segment::open_active(
                    &dir,
                    Offset(*base),
                    config.validate_on_open,
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

        let config = std::sync::Arc::new(std::sync::RwLock::new(config));

        let span = tracing::Span::current();
        span.record("segments", segments.len() + 1);
        span.record("log_end", lso.0);

        let mut log = Self {
            dir,
            config,
            io: std::sync::Arc::new(FileIo),
            segments,
            active: Some(active),
            dir_sync_needed,
            start_offset_override: None,
            lso,
            pending: HashMap::new(),
            pending_stamp_ranges: HashMap::new(),
            coordinator_epochs: HashMap::new(),
            producer_state: HashMap::new(),
            active_txn_index,
            sealed_txn_indexes,
            stamp_source: None,
            stamp_indexes: BTreeMap::new(),
            epoch_checkpoint,
            reconciled_frontier: Offset(0),
            delivery_watermark: Offset(0),
            delivery_pending_ms: None,
        };
        // Recovery needs no durable watermark: the schedule is in the records,
        // so the first advance rebuilds it from the log start.
        log.delivery_watermark = log.log_start_offset();
        log.rebuild_producer_and_transaction_state()?;
        Ok(log)
    }

    /// Restore the latest valid producer snapshot, then replay the uncovered
    /// log tail. Missing boundary snapshots are created during the replay so
    /// every sealed segment can be copied to remote storage with its matching
    /// producer state.
    pub(super) fn rebuild_producer_and_transaction_state(&mut self) -> Result<(), LogError> {
        self.pending.clear();
        self.pending_stamp_ranges.clear();
        self.coordinator_epochs.clear();
        self.producer_state.clear();
        let end = self.log_end_offset();
        let mut next = self.log_start_offset();
        if let Some((snapshot_offset, entries)) =
            producer_snapshot::latest_at_or_before(&self.dir, end)?
        {
            self.producer_state = entries;
            for (&producer_id, entry) in &self.producer_state {
                if let Some(first_offset) = entry.current_txn_first_offset {
                    self.pending.insert(producer_id, first_offset);
                }
                if entry.coordinator_epoch >= 0 {
                    self.coordinator_epochs
                        .insert(producer_id, entry.coordinator_epoch);
                }
            }
            next = snapshot_offset.max(next);
        }

        let mut boundaries: BTreeSet<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(self.active.iter().map(Segment::base_offset))
            .collect();
        let mut boundaries = boundaries.split_off(&next);
        let _ = boundaries.remove(&next);
        while next < end {
            let read = self.read(next, krabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery made no progress at offset {next}"
                )));
            }
            let mut advanced_to = next;
            for batch in &read.batches {
                self.apply_recovered_batch_state(batch)?;
                (_, advanced_to) = Self::recovered_batch_offsets(advanced_to, batch)?;
                let covered: Vec<_> = boundaries.range(..=advanced_to).copied().collect();
                for boundary in covered {
                    producer_snapshot::write(&self.dir, boundary, &self.producer_state)?;
                    let _ = boundaries.remove(&boundary);
                }
            }
            if advanced_to <= next {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery did not advance past offset {next}"
                )));
            }
            next = advanced_to;
        }
        self.rebuild_pending_stamp_ranges()?;
        self.lso = self
            .pending
            .values()
            .copied()
            .min()
            .unwrap_or_else(|| self.log_end_offset());
        Ok(())
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
            self.pending.remove(&producer_id);
            if let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
            {
                self.coordinator_epochs.insert(producer_id, epoch);
            }
        } else if batch.attributes.is_transactional() {
            self.pending
                .entry(producer_id)
                .or_insert(Offset(batch.base_offset));
        }
        Ok(())
    }

    fn rebuild_pending_stamp_ranges(&mut self) -> Result<(), LogError> {
        self.pending_stamp_ranges.clear();
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut next = self.log_start_offset();
        let end = self.log_end_offset();
        while next < end {
            let read = self.read(next, krabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "transaction-stamp recovery made no progress at offset {next}"
                )));
            }
            for batch in &read.batches {
                let producer_id = ProducerId(batch.producer_id);
                let (last, advanced_to) = Self::recovered_batch_offsets(next, batch)?;
                if advanced_to <= next {
                    return Err(LogError::Corrupt(format!(
                        "transaction-stamp recovery did not advance past offset {next}"
                    )));
                }
                match control_batch_kind(batch) {
                    // A barrier marker closes no transaction, so it clears no
                    // stamp range. The append path reaches the same result.
                    Some(ControlBatchKind::Barrier) => {}
                    Some(ControlBatchKind::Transaction) => {
                        self.pending_stamp_ranges.remove(&producer_id);
                    }
                    None => {
                        if batch.attributes.is_transactional() && producer_id.get() >= 0 {
                            self.pending_stamp_ranges
                                .entry(producer_id)
                                .or_default()
                                .push((Offset(batch.base_offset), last));
                        }
                    }
                }
                next = advanced_to;
            }
        }
        Ok(())
    }

    fn recovered_batch_offsets(
        current: Offset,
        batch: &RecordBatch,
    ) -> Result<(Offset, Offset), LogError> {
        let last = batch
            .base_offset
            .checked_add(i64::from(batch.last_offset_delta))
            .map(Offset)
            .ok_or_else(|| {
                LogError::Corrupt(format!("log recovery offset overflow at {current}"))
            })?;
        let advanced_to = last.0.checked_add(1).map(Offset).ok_or_else(|| {
            LogError::Corrupt(format!("log recovery offset overflow at {current}"))
        })?;
        if advanced_to <= current {
            return Err(LogError::Corrupt(format!(
                "log recovery did not advance past offset {current}"
            )));
        }
        Ok((last, advanced_to))
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
            let entry = self
                .producer_state
                .entry(producer_id)
                .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, batch.producer_epoch));
            if entry.producer_epoch != batch.producer_epoch {
                // Kafka clears the retained data-batch metadata when an end
                // marker advances the producer epoch (transaction version 2).
                entry.last_sequence = -1;
                entry.last_offset = Offset(-1);
                entry.offset_delta = 0;
            }
            entry.producer_epoch = batch.producer_epoch;
            entry.timestamp = batch.max_timestamp;
            entry.current_txn_first_offset = None;
            if let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
            {
                entry.coordinator_epoch = epoch;
            }
            return Ok(());
        }
        self.update_data_producer_entry(
            (producer_id, batch.producer_epoch),
            (batch.base_sequence, batch.last_offset_delta),
            (
                Offset(batch.base_offset),
                batch.max_timestamp,
                batch.attributes.is_transactional(),
            ),
        )
    }

    pub(super) fn update_data_producer_entry(
        &mut self,
        producer: (ProducerId, i16),
        sequence: (i32, i32),
        append: (Offset, i64, bool),
    ) -> Result<(), LogError> {
        let (producer_id, producer_epoch) = producer;
        let (base_sequence, last_offset_delta) = sequence;
        let (base_offset, timestamp, is_transactional) = append;
        let Some((last_sequence, last_offset)) =
            Self::data_producer_tail(producer_id, base_sequence, last_offset_delta, base_offset)?
        else {
            return Ok(());
        };
        let entry = self
            .producer_state
            .entry(producer_id)
            .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, producer_epoch));
        entry.producer_epoch = producer_epoch;
        entry.last_sequence = last_sequence;
        entry.last_offset = last_offset;
        entry.offset_delta = last_offset_delta;
        entry.timestamp = timestamp;
        if is_transactional && entry.current_txn_first_offset.is_none() {
            entry.current_txn_first_offset = Some(base_offset);
        }
        Ok(())
    }

    pub(super) fn data_producer_tail(
        producer_id: ProducerId,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: Offset,
    ) -> Result<Option<(i32, Offset)>, LogError> {
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
        Ok(Some((last_sequence, last_offset)))
    }
}

#[cfg(test)]
mod tests;
