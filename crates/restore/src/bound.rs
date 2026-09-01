//! The restore bound: every predicate an operator gave, and what it decides.
//!
//! This module owns the point in time the restore stops at. It compiles the
//! `--to-offset`, `--to-timestamp`, `--exclude-key`, `--exclude-header`,
//! `--exclude-producer-id`, and `--exclude-offset` flags into one predicate
//! set, and answers two questions about the archived bytes. For a batch it
//! answers whether the batch passes through untouched, must be replaced with
//! a bare header because every record is excluded, or has to be re-encoded
//! because only some of its records survive; the borrowed batch view makes
//! that decision without an owned decode of the batches that pass. Dropping a
//! batch's bytes is never the same as dropping its offset range: the target
//! log accepts an append only at its current end offset
//! (`Log::append_at`/`append_verbatim_at`), so a batch whose records are all
//! excluded still has to claim its offsets, or every batch archived after it
//! in the partition becomes unappendable. For one record inside a batch that
//! must be re-encoded, it answers whether the record survives. The exclude
//! patterns match the raw key and header bytes. This module decodes no
//! payload and knows no schema.

use std::collections::{HashMap, HashSet};

use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::{
    RecordBatchBorrowed, RecordBatchHeader, RecordBorrowed, RecordsError,
};
use regex::Regex;

use crate::{
    args::{HeaderPattern, PartitionRef},
    error::RestoreError,
};

mod compile;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

/// What the bound decides about one archived batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDecision {
    /// No predicate touches the batch. It is written verbatim, so its bytes
    /// stay byte-identical to the archived copy.
    Keep,
    /// Every record in the batch is excluded. The batch's offset range is
    /// still written, as a bare header with zero records:
    /// [`krabka_log::filter_batch`] calls this outcome
    /// [`krabka_log::FilteredBatch::Empty`] and leaves the caller to decide
    /// what an emptied batch becomes, and for a restore that must preserve
    /// offsets, becoming a bare header is the only choice that does not
    /// silently shift every later record. The bare header keeps the archived
    /// `base_offset` and `last_offset_delta` unchanged, exactly as krabka's
    /// own log cleaner does on its `RETAIN_EMPTY` path for the same reason.
    Empty,
    /// Some records survive. The batch is re-encoded from the records that
    /// [`Predicates::decide_record`] keeps, so its bytes differ from the
    /// archived copy. [`krabka_log::filter_batch`] keeps the batch's
    /// `base_offset` and recomputes `last_offset_delta` from the survivors,
    /// so surviving records keep their absolute offsets and the gap left by a
    /// dropped record is invisible to the log format.
    Filter,
}

/// What the bound decides about one record inside a filtered batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordDecision {
    /// The record is written.
    Keep,
    /// The record is not written.
    Drop,
}

/// The compiled predicate set.
#[derive(Debug)]
pub struct Predicates {
    /// The last offset kept per partition, from `--to-offset`.
    to_offset: HashMap<PartitionRef, Offset>,
    /// The global `--to-timestamp` bound, in epoch milliseconds. Unlike
    /// `to_offset`, this applies to every partition alike.
    to_timestamp: Option<i64>,
    /// `--exclude-key` patterns. A record is dropped when any one matches.
    exclude_key: Vec<Regex>,
    /// `--exclude-header` patterns. A record is dropped when any one matches
    /// a header by name and value.
    exclude_header: Vec<HeaderPattern>,
    /// `--exclude-producer-id` values.
    exclude_producer_id: HashSet<ProducerId>,
    /// `--exclude-offset` ranges, grouped per partition as half-open
    /// `(start, end_exclusive)` pairs.
    exclude_offset: HashMap<PartitionRef, Vec<(Offset, Offset)>>,
}

impl Predicates {
    /// The highest offset the restore keeps in `partition`, when
    /// `--to-offset` names it.
    ///
    /// `--to-timestamp` gives no equivalent lookup here, because it is a
    /// global bound that applies to every partition alike, while
    /// `--to-offset` is named per partition. Turning a timestamp bound into
    /// an offset a caller could look up the same way would need a batch's
    /// own timestamp, and the only place that reads one is
    /// [`Self::decide_batch`]; there is no offset this method can hand back
    /// for a timestamp bound without decoding a batch first.
    #[must_use]
    pub fn offset_bound(&self, partition: &PartitionRef) -> Option<Offset> {
        self.to_offset.get(partition).copied()
    }

    /// Whether this batch and every later ordered batch start past the
    /// partition's inclusive offset bound.
    #[must_use]
    pub fn batch_past_offset_bound(&self, partition: &PartitionRef, batch_base: Offset) -> bool {
        let (applies, bound) = self
            .offset_bound(partition)
            .map_or((false, 0), |bound| (true, bound.0));
        krabka_verified::restore_batch_past_offset_bound(batch_base.0, applies, bound)
    }

    /// Decide the fate of one archived batch.
    ///
    /// The caller applies [`Self::batch_past_offset_bound`] first and stops
    /// walking the partition once a batch's base offset is past it. A batch
    /// that straddles the inclusive offset bound is still decoded here so only
    /// its records at or below the bound survive.
    ///
    /// # Errors
    ///
    /// While evaluating an applicable record predicate, returns an integrity
    /// error if a record fails to parse, lies outside the batch's declared
    /// offset range, or its absolute offset or timestamp cannot be represented.
    pub fn decide_batch(
        &self,
        partition: &PartitionRef,
        batch: &RecordBatchBorrowed<'_>,
    ) -> Result<BatchDecision, RestoreError> {
        // A control batch carries a transaction commit or abort marker, not
        // operator data -- no exclude predicate is about transaction
        // bookkeeping, and an operator writing `--exclude-producer-id` for a
        // producer's data batches has no way to know, or reason to expect,
        // that the same id also names the marker that closes out that
        // producer's transaction. Filtering or emptying a control batch would
        // silently corrupt the restored partition's transaction state, which
        // is a worse outcome than retaining marker metadata past a timestamp
        // bound. The caller still stops before a control batch whose base is
        // past `--to-offset`; a valid Kafka control batch carries its one
        // marker at that base.
        if batch.attributes().is_control_batch() {
            return Ok(BatchDecision::Keep);
        }

        if self.keeps_everything_in(partition) {
            return Ok(BatchDecision::Keep);
        }

        let header = batch.header();
        let producer_id = ProducerId(header.producer_id.get());

        let mut saw_keep = false;
        let mut saw_drop = false;
        for parsed in batch {
            let record = parsed?;
            let (offset, timestamp_ms) = record_coordinates(header, &record)?;
            match self.decide_record(partition, offset, timestamp_ms, producer_id, &record) {
                RecordDecision::Keep => saw_keep = true,
                RecordDecision::Drop => saw_drop = true,
            }
            // Once both are seen the answer is Filter regardless of what the
            // rest of the batch holds, and `decide_record` runs again per
            // record during the actual rewrite either way, so nothing here is
            // worth precomputing.
            if saw_keep && saw_drop {
                return Ok(batch_decision(saw_keep, saw_drop));
            }
        }
        // No drops, including an empty batch, stays byte-identical.
        Ok(batch_decision(saw_keep, saw_drop))
    }

    /// Whether no exclude predicate could possibly touch a record in
    /// `partition`, so [`Self::decide_batch`] can answer
    /// [`BatchDecision::Keep`] without decoding a single record. This is the
    /// common case for most restores, which run with no exclude flags at
    /// all.
    fn keeps_everything_in(&self, partition: &PartitionRef) -> bool {
        self.to_timestamp.is_none()
            && !self.to_offset.contains_key(partition)
            && self.exclude_key.is_empty()
            && self.exclude_header.is_empty()
            && self.exclude_producer_id.is_empty()
            && !self.exclude_offset.contains_key(partition)
    }

    /// Decide the fate of one record inside a batch that must be re-encoded.
    ///
    /// The record is dropped when any predicate matches, OR-combined: its
    /// absolute `offset` falls in an `--exclude-offset` range for
    /// `partition`, `producer_id` is named by `--exclude-producer-id`,
    /// `record.key` matches an `--exclude-key` pattern, or one of `record`'s
    /// headers matches an `--exclude-header NAME=REGEX` pattern by name and
    /// value. A record is also dropped when it is above the partition's
    /// inclusive `--to-offset` bound or at or after the global exclusive
    /// `--to-timestamp` bound. A record with `key: None` never matches a key
    /// pattern, and a header whose value is absent never matches a header
    /// pattern; neither is treated as matching an empty string.
    ///
    /// `--exclude-key` and `--exclude-header` match the raw bytes only when
    /// those bytes are valid UTF-8. `regex::Regex::is_match` takes `&str`,
    /// and a byte string an operator's regex cannot even be checked against
    /// must not silently match it merely because it fails to decode, so
    /// invalid UTF-8 never matches.
    #[must_use]
    pub fn decide_record(
        &self,
        partition: &PartitionRef,
        offset: Offset,
        timestamp_ms: i64,
        producer_id: ProducerId,
        record: &RecordBorrowed<'_>,
    ) -> RecordDecision {
        let (offset_bound_applies, offset_bound) = self
            .offset_bound(partition)
            .map_or((false, 0), |bound| (true, bound.0));
        let (timestamp_bound_applies, timestamp_bound) =
            self.to_timestamp.map_or((false, 0), |bound| (true, bound));
        let producer_excluded = self.exclude_producer_id.contains(&producer_id);
        let offset_excluded = self.exclude_offset.get(partition).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|&(start, end_exclusive)| offset >= start && offset < end_exclusive)
        });
        let key_excluded = record.key.is_some_and(|key| {
            self.exclude_key
                .iter()
                .any(|pattern| matches_utf8(pattern, key))
        });
        let header_excluded = record.headers.iter().any(|header| {
            self.exclude_header.iter().any(|candidate| {
                candidate.name == header.key
                    && header
                        .value
                        .is_some_and(|value| matches_utf8(&candidate.pattern, value))
            })
        });
        if krabka_verified::restore_record_selected(
            offset.0,
            offset_bound_applies,
            offset_bound,
            timestamp_ms,
            timestamp_bound_applies,
            timestamp_bound,
            producer_excluded,
            offset_excluded,
            key_excluded,
            header_excluded,
        ) {
            RecordDecision::Keep
        } else {
            RecordDecision::Drop
        }
    }
}

fn batch_decision(saw_keep: bool, saw_drop: bool) -> BatchDecision {
    match krabka_verified::restore_batch_filter_decision(saw_keep, saw_drop) {
        krabka_verified::RestoreFilterDecision::Keep => BatchDecision::Keep,
        krabka_verified::RestoreFilterDecision::Empty => BatchDecision::Empty,
        krabka_verified::RestoreFilterDecision::Filter => BatchDecision::Filter,
    }
}

/// Validate and adapt one decoded record's primitive coordinates at the
/// verified boundary.
pub(crate) fn record_coordinates(
    header: &RecordBatchHeader,
    record: &RecordBorrowed<'_>,
) -> Result<(Offset, i64), RestoreError> {
    krabka_verified::restore_record_coordinates(
        header.base_offset.get(),
        header.last_offset_delta.get(),
        header.base_timestamp.get(),
        record.offset_delta,
        record.timestamp_delta,
    )
    .map(|(offset, timestamp)| (Offset(offset), timestamp))
    .ok_or_else(|| {
        RecordsError::RecordParse(format!(
            "record coordinates are outside the batch: base offset {}, last offset delta {}, \
             record offset delta {}, base timestamp {}, record timestamp delta {}",
            header.base_offset.get(),
            header.last_offset_delta.get(),
            record.offset_delta,
            header.base_timestamp.get(),
            record.timestamp_delta,
        ))
        .into()
    })
}

/// Whether `bytes` is valid UTF-8 and `pattern` matches the decoded text.
fn matches_utf8(pattern: &Regex, bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|text| pattern.is_match(text))
}
