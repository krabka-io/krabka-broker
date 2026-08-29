//! The second compaction pass. It streams the sealed segments into `.swap`
//! files, applying the KIP-534 retain decision to every record, and owns the
//! rewrite's input and output types together with the `.swap` path naming.

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use bytes::{Bytes, BytesMut};
use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, Time, TimeExt};
use tracing::instrument;

use super::{
    BatchMeta, CleanedTransactionMetadata, RecordMeta, RetainDecision,
    batch_reader::read_all_batches, retain_decision,
};
use crate::{
    error::LogError,
    name,
    segment::Segment,
    txn_index::{AbortedTxn, TxnIndex},
};

#[cfg(test)]
mod tests;

/// Result of [`rewrite_segments`]: paths to the three `.swap` files that
/// [`atomic_swap`] should promote.
pub struct RewriteOutput {
    pub log_swap: PathBuf,
    pub index_swap: PathBuf,
    pub timeindex_swap: PathBuf,
    /// `base_offset` of the new segment. It equals the lowest input segment.
    pub new_base_offset: Offset,
    /// Highest absolute offset of any surviving record.
    #[cfg(test)]
    pub new_last_offset: Offset,
    /// Path to the rewritten survivor `.txnindex`. The rewrite writes this
    /// file only when it carries forward one or more aborted-txn entries. It
    /// is `None` when no aborted transaction survives.
    pub txnindex_swap: Option<PathBuf>,
}

/// Time-based retention inputs used while rewriting compacted segments.
#[derive(Debug, Clone, Copy)]
pub struct RewriteRetention {
    /// Current wall-clock time in milliseconds. An instant, so it stays raw.
    pub now_ms: i64,
    /// How long a tombstone remains eligible for reads before deletion.
    pub delete_retention: Time,
}

/// Stream `segments`, oldest to newest, into new `.swap` files and apply the
/// KIP-534 per-record [`retain_decision`].
///
/// For each record the decision is:
///   - `Keep` → write it through.
///   - `SetHorizon(h)` → write it through, and stamp the output batch with
///     delete horizon `h` (bit 6 set, `base_timestamp = h`).
///   - `Delete` → drop it.
///
/// Records keep their **absolute** offsets. The output `RecordBatch`es can
/// therefore hold gaps in their `offset_delta` values where superseded records
/// used to live. This matches Kafka's on-disk format for compacted topics.
///
/// `RETAIN_EMPTY`: this function normally skips a batch that ends up with no
/// kept records. It writes such a batch again as a bare header with no records
/// in two cases: when the batch is the last batch of an active producer in
/// `active_producers`, and when it is the last batch of the consolidated
/// output. The producer sequence, the producer epoch, and the log-end offset
/// therefore survive. This is Kafka's `retainEmpty`.
///
/// This function writes the `.swap` files to the segments' shared directory.
/// The caller must fsync them and promote them through [`atomic_swap`].
#[instrument(
    level = "info",
    skip_all,
    fields(
        dir = %dir.display(),
        segments = segments.len(),
        new_base = tracing::field::Empty,
        new_last_offset = tracing::field::Empty,
    ),
    err,
)]
pub fn rewrite_segments(
    dir: &Path,
    segments: &[&Segment],
    offset_map: &HashMap<Bytes, Offset>,
    txn_meta: &CleanedTransactionMetadata,
    retention: RewriteRetention,
    active_producers: &HashMap<ProducerId, Offset>,
    _index_interval: ByteSize,
) -> Result<RewriteOutput, LogError> {
    // The Creusot-verified retain kernel is stated over integer milliseconds,
    // and the horizon it computes is stamped into an on-disk `base_timestamp`,
    // so the extent crosses to a raw count once, here, truncating rather than
    // rounding so a stamped horizon can never land a millisecond late.
    let delete_retention_ms = retention.delete_retention.millis_i64_trunc();

    let first = segments
        .first()
        .ok_or_else(|| LogError::Io(std::io::Error::other("rewrite_segments: empty input")))?;
    let new_base = first.base_offset();
    tracing::Span::current().record("new_base", new_base.0);

    let log_swap = swap_path(dir, new_base.0, "log");
    let index_swap = swap_path(dir, new_base.0, "index");
    let timeindex_swap = swap_path(dir, new_base.0, "timeindex");

    // Truncate (or create) all three swap files. We rewrite the .log
    // file proper here; for the index sidecars we write empty files
    // and let Segment::open populate them via tail-scan in the recovery
    // promotion path. (Sparse indexes are derivable from the .log; an
    // empty index is correct and small.)
    let mut log_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&log_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&index_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&timeindex_swap)?;

    // Flatten all batches across all segments so we can identify the last
    // batch (for RETAIN_EMPTY) and the last batch per active producer.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for seg in segments {
        all_batches.extend(read_all_batches(seg)?);
    }
    let last_batch_index = all_batches.len().saturating_sub(1);
    // The index of each active producer's last batch in `all_batches`.
    let mut producer_last_batch: HashMap<ProducerId, usize> = HashMap::new();
    for (i, batch) in all_batches.iter().enumerate() {
        let pid = ProducerId(batch.producer_id);
        if active_producers.contains_key(&pid) {
            producer_last_batch.insert(pid, i);
        }
    }

    let mut last_kept_offset = new_base - 1;

    for (batch_idx, batch) in all_batches.iter().enumerate() {
        let is_control = batch.attributes.is_control_batch();
        let producer_id = ProducerId(batch.producer_id);
        let txn = txn_meta.txn_state(producer_id);
        let batch_meta = BatchMeta {
            is_control,
            producer_id,
            existing_horizon: batch.delete_horizon_ms(),
        };

        let mut kept: Vec<krabka_protocol::records::Record> =
            Vec::with_capacity(batch.records.len());
        // Stamp the output batch with a delete horizon if any record's
        // decision asks for it (stamp once per batch).
        let mut stamp_horizon: Option<i64> = None;
        for record in &batch.records {
            let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
            let is_newest_for_key = record
                .key
                .as_ref()
                .is_some_and(|k| offset_map.get(k.as_ref()).copied() == Some(absolute));
            let rec_meta = RecordMeta {
                has_key: record.key.is_some(),
                has_value: record.value.is_some(),
            };
            match retain_decision(
                rec_meta,
                batch_meta,
                is_newest_for_key,
                txn,
                retention.now_ms,
                delete_retention_ms,
            ) {
                RetainDecision::Keep => kept.push(record.clone()),
                RetainDecision::SetHorizon(h) => {
                    kept.push(record.clone());
                    stamp_horizon = Some(h);
                }
                RetainDecision::Delete => {}
            }
        }

        if kept.is_empty() {
            // RETAIN_EMPTY: re-emit a bare header for an emptied batch when
            // it is the last batch of an active producer or the last batch
            // of the consolidated output, so producer sequence/epoch and the
            // log-end offset survive.
            let is_producer_last =
                producer_last_batch.get(&producer_id).copied() == Some(batch_idx);
            let is_output_last = batch_idx == last_batch_index;
            if !(is_producer_last || is_output_last) {
                continue;
            }
            let out_batch = RecordBatch {
                base_offset: batch.base_offset,
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                base_timestamp: batch.base_timestamp,
                attributes: batch.attributes,
                producer_id: batch.producer_id,
                producer_epoch: batch.producer_epoch,
                base_sequence: batch.base_sequence,
                partition_leader_epoch: batch.partition_leader_epoch,
                records: vec![],
            };
            let mut buf = BytesMut::with_capacity(out_batch.encoded_len());
            out_batch.encode(&mut buf)?;
            log_file.write_all(&buf)?;
            let batch_last = Offset(out_batch.base_offset + i64::from(out_batch.last_offset_delta));
            if batch_last > last_kept_offset {
                last_kept_offset = batch_last;
            }
            continue;
        }

        // Compute new last_offset_delta covering the kept range (relative to
        // the batch's original base_offset). Kafka preserves base_offset and
        // only updates last_offset_delta when records are removed mid-batch.
        let last_delta = kept
            .iter()
            .map(|r| r.offset_delta)
            .max()
            .expect("kept non-empty");
        let mut out_batch = RecordBatch {
            last_offset_delta: last_delta,
            records: kept,
            ..batch.clone()
        };
        // Stamp the delete horizon once, after the kept batch is built. This
        // rewrites each kept record's timestamp_delta so absolute timestamps
        // are preserved (see `RecordBatch::with_delete_horizon`).
        if let Some(h) = stamp_horizon {
            out_batch = out_batch.with_delete_horizon(h);
        }

        let mut buf = BytesMut::with_capacity(out_batch.encoded_len());
        out_batch.encode(&mut buf)?;
        log_file.write_all(&buf)?;

        let batch_last = Offset(out_batch.base_offset + i64::from(out_batch.last_offset_delta));
        if batch_last > last_kept_offset {
            last_kept_offset = batch_last;
        }
    }
    log_file.sync_all()?;

    // Rebuild the survivor `.txnindex`: carry forward aborted-txn entries
    // whose aborted data still partially survives. Producers whose data is
    // fully compacted away have their entries (and markers) dropped.
    let retained: Vec<AbortedTxn> = txn_meta.retained_aborted().copied().collect();
    let txnindex_swap = if retained.is_empty() {
        None
    } else {
        let path = swap_path(dir, new_base.0, "txnindex");
        // Truncate any stale swap, then append the retained entries.
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let mut idx = TxnIndex::open(path.clone())?;
        for entry in retained {
            idx.append(entry)?;
        }
        Some(path)
    };

    tracing::Span::current().record("new_last_offset", last_kept_offset.0);
    Ok(RewriteOutput {
        log_swap,
        index_swap,
        timeindex_swap,
        new_base_offset: new_base,
        #[cfg(test)]
        new_last_offset: last_kept_offset,
        txnindex_swap,
    })
}

fn swap_path(dir: &Path, base_offset: i64, ext: &str) -> PathBuf {
    dir.join(format!(
        "{}.{}.swap",
        name::format_base_offset(base_offset),
        ext
    ))
}
