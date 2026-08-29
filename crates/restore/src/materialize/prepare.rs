//! Deciding one archived record batch's fate under the bound, and shaping what
//! the target log is asked to append for it.
//!
//! Segment writing hands every decoded batch to this module and gets back a
//! `PreparedBatch`: either the archived bytes untouched, for the log's
//! zero-copy verbatim path, or an owned `RecordBatch`, for the log's
//! decode-and-append path. It comes with the `BatchTally` that says how the
//! batch folds into the segment's counts. Nothing here touches the log, so a
//! dry run takes exactly the path a real run takes.

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_log::{FilteredBatch, VerbatimBatch, filter_batch};
use krabka_protocol::records::{Attributes, RecordBatch, RecordBatchBorrowed, RecordBatchHeader};

use crate::{
    args::PartitionRef,
    bound::{BatchDecision, Predicates, RecordDecision},
    error::RestoreError,
};

/// What one archived batch becomes when it is appended to the target log: the exact bytes the log's zero-copy path needs, or an owned batch for the log's decode-and-append path.
pub(super) enum PreparedBatch {
    Verbatim(VerbatimBatch),
    Owned(RecordBatch),
}

impl PreparedBatch {
    pub(super) fn last_offset_delta(&self) -> i32 {
        match self {
            Self::Verbatim(batch) => batch.last_offset_delta,
            Self::Owned(batch) => batch.last_offset_delta,
        }
    }

    pub(super) fn encoded_len(&self) -> usize {
        match self {
            Self::Verbatim(batch) => batch.bytes.len(),
            Self::Owned(batch) => batch.encoded_len(),
        }
    }
}

/// How [`prepare_batch`]'s outcome folds into
/// [`SegmentOutcome`](super::SegmentOutcome)'s counts.
pub(super) enum BatchTally {
    Kept,
    Rewritten { kept: u64, dropped: u64 },
    Emptied,
}

/// Decide one archived batch's fate under `predicates`, and prepare what gets appended to the target log without touching the log itself, so `--dry-run` can share this exact path with a real run.
pub(super) fn prepare_batch(
    partition_ref: &PartitionRef,
    predicates: &Predicates,
    batch: &RecordBatchBorrowed<'_>,
    batch_bytes: Bytes,
    records_in_batch: u64,
) -> Result<(PreparedBatch, BatchTally), RestoreError> {
    let header = batch.header();
    match predicates.decide_batch(partition_ref, batch) {
        BatchDecision::Keep => {
            let verbatim = verbatim_from_header(header, batch_bytes, batch.attributes());
            Ok((PreparedBatch::Verbatim(verbatim), BatchTally::Kept))
        }
        BatchDecision::Empty => Ok((
            PreparedBatch::Owned(bare_header_batch(header)),
            BatchTally::Emptied,
        )),
        BatchDecision::Filter => prepare_filtered_batch(
            partition_ref,
            predicates,
            batch,
            batch_bytes,
            records_in_batch,
        ),
    }
}

/// The [`BatchDecision::Filter`] arm of [`prepare_batch`], split out because it is the one path that has to decide record by record before it can decide the whole batch.
fn prepare_filtered_batch(
    partition_ref: &PartitionRef,
    predicates: &Predicates,
    batch: &RecordBatchBorrowed<'_>,
    batch_bytes: Bytes,
    records_in_batch: u64,
) -> Result<(PreparedBatch, BatchTally), RestoreError> {
    let header = batch.header();
    let mut keep_flags = Vec::with_capacity(usize::try_from(records_in_batch).unwrap_or(0));
    for record in batch {
        let record = record?;
        let record_offset = Offset(header.base_offset.get() + i64::from(record.offset_delta));
        let timestamp_ms = header.base_timestamp.get() + record.timestamp_delta;
        let producer_id = ProducerId(header.producer_id.get());
        let decision = predicates.decide_record(
            partition_ref,
            record_offset,
            timestamp_ms,
            producer_id,
            &record,
        );
        keep_flags.push(decision == RecordDecision::Keep);
    }

    let owned = batch.to_owned()?;
    let mut flags = keep_flags.into_iter();
    let filtered = filter_batch(&owned, |_record| {
        flags.next().expect(
            "filter_batch calls keep exactly once per record, matching the per-record walk \
             that built keep_flags",
        )
    });

    Ok(match filtered {
        FilteredBatch::Unchanged => {
            let verbatim = verbatim_from_header(header, batch_bytes, batch.attributes());
            (PreparedBatch::Verbatim(verbatim), BatchTally::Kept)
        }
        FilteredBatch::Filtered(rewritten) => {
            let kept = u64::try_from(rewritten.records.len()).unwrap_or(0);
            let dropped = records_in_batch.saturating_sub(kept);
            // `filter_batch` recomputes `last_offset_delta` as the highest
            // surviving record's delta, which is right for compaction: the
            // cleaner writes raw bytes to a fresh `.log`/`.index` and never
            // re-checks contiguity between what it just wrote and what comes
            // next. This restore appends through `Log::append_at`, which
            // demands every batch land exactly at `log_end_offset()` --  so a
            // shrunk `last_offset_delta` here would silently strand every
            // batch archived after this one at an offset the target log no
            // longer expects, the moment an exclude predicate happens to
            // drop a batch's trailing record. The fix is the same one
            // `FilteredBatch::Empty` already applies below: keep the
            // archived `last_offset_delta`, so the batch claims its full
            // original offset span regardless of which records inside it
            // survive.
            let rewritten = RecordBatch {
                last_offset_delta: owned.last_offset_delta,
                ..rewritten
            };
            (
                PreparedBatch::Owned(rewritten),
                BatchTally::Rewritten { kept, dropped },
            )
        }
        FilteredBatch::Empty => {
            let bare = RecordBatch {
                records: Vec::new(),
                ..owned
            };
            (PreparedBatch::Owned(bare), BatchTally::Emptied)
        }
    })
}

/// Build the [`VerbatimBatch`] that reproduces `header`'s archived bytes unchanged: every field the log needs for offset assignment, LSO tracking, and the leader-epoch checkpoint, copied straight from the header the producer wrote.
fn verbatim_from_header(
    header: &RecordBatchHeader,
    bytes: Bytes,
    attributes: Attributes,
) -> VerbatimBatch {
    VerbatimBatch {
        bytes,
        last_offset_delta: header.last_offset_delta.get(),
        max_timestamp: header.max_timestamp.get(),
        leader_epoch: LeaderEpoch(header.partition_leader_epoch.get()),
        producer_id: ProducerId(header.producer_id.get()),
        producer_epoch: header.producer_epoch.get(),
        base_sequence: header.base_sequence.get(),
        is_transactional: attributes.is_transactional(),
    }
}

/// Build a zero-record batch that claims `header`'s archived offset range without holding any of its records: `base_offset` and `last_offset_delta` are copied unchanged, so the target log's end offset still advances by the batch's full archived span. See [`BatchDecision::Empty`].
fn bare_header_batch(header: &RecordBatchHeader) -> RecordBatch {
    RecordBatch {
        base_offset: header.base_offset.get(),
        partition_leader_epoch: header.partition_leader_epoch.get(),
        attributes: Attributes(header.attributes.get()),
        last_offset_delta: header.last_offset_delta.get(),
        base_timestamp: header.base_timestamp.get(),
        max_timestamp: header.max_timestamp.get(),
        producer_id: header.producer_id.get(),
        producer_epoch: header.producer_epoch.get(),
        base_sequence: header.base_sequence.get(),
        records: Vec::new(),
    }
}
