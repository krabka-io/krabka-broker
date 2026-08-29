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
use krabka_protocol::records::{RecordBatchBorrowed, RecordBorrowed};
use regex::Regex;

use crate::args::{HeaderPattern, PartitionRef};

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

    /// Decide the fate of one archived batch.
    ///
    /// The caller applies [`Self::offset_bound`] first and stops walking the
    /// partition once a batch's base offset is past it; this method only
    /// judges the exclude predicates against a batch that is within bound, so
    /// its answer is never [`BatchDecision::Empty`] on that account alone.
    ///
    /// # Panics
    ///
    /// Panics if a record inside `batch` fails to parse. A batch reaches this
    /// method only after `crate::verify::verify_segment` has checked its CRC
    /// over the raw body, and a body whose CRC matches the bytes the
    /// producer wrote parses record by record without error; a parse failure
    /// here would mean corruption the CRC check missed, which its own
    /// guarantee rules out.
    #[must_use]
    pub fn decide_batch(
        &self,
        partition: &PartitionRef,
        batch: &RecordBatchBorrowed<'_>,
    ) -> BatchDecision {
        // A control batch carries a transaction commit or abort marker, not
        // operator data -- no exclude predicate is about transaction
        // bookkeeping, and an operator writing `--exclude-producer-id` for a
        // producer's data batches has no way to know, or reason to expect,
        // that the same id also names the marker that closes out that
        // producer's transaction. Filtering or emptying a control batch would
        // silently corrupt the restored partition's transaction state, which
        // is a worse outcome than restoring one record the operator meant to
        // exclude. The offset it claims is unaffected either way: the caller
        // applies `--to-offset`/`--to-timestamp` tail truncation before ever
        // calling this, so a control batch past that bound is still dropped
        // by not being written at all, exactly like any other batch.
        if batch.attributes().is_control_batch() {
            return BatchDecision::Keep;
        }

        if self.keeps_everything_in(partition) {
            return BatchDecision::Keep;
        }

        let header = batch.header();
        let producer_id = ProducerId(header.producer_id.get());
        let base_offset = header.base_offset.get();
        let base_timestamp = header.base_timestamp.get();

        let mut saw_keep = false;
        let mut saw_drop = false;
        for parsed in batch {
            let record = parsed.expect(
                "a batch already checked by verify_segment's CRC parses record by record \
                 without error",
            );
            let offset = Offset(base_offset + i64::from(record.offset_delta));
            let timestamp_ms = base_timestamp + record.timestamp_delta;
            match self.decide_record(partition, offset, timestamp_ms, producer_id, &record) {
                RecordDecision::Keep => saw_keep = true,
                RecordDecision::Drop => saw_drop = true,
            }
            // Once both are seen the answer is Filter regardless of what the
            // rest of the batch holds, and `decide_record` runs again per
            // record during the actual rewrite either way, so nothing here is
            // worth precomputing.
            if saw_keep && saw_drop {
                return BatchDecision::Filter;
            }
        }
        if saw_drop {
            BatchDecision::Empty
        } else {
            // Covers both "no record was dropped" and "the batch holds no
            // records", which filter_batch also treats as unchanged.
            BatchDecision::Keep
        }
    }

    /// Whether no exclude predicate could possibly touch a record in
    /// `partition`, so [`Self::decide_batch`] can answer
    /// [`BatchDecision::Keep`] without decoding a single record. This is the
    /// common case for most restores, which run with no exclude flags at
    /// all.
    fn keeps_everything_in(&self, partition: &PartitionRef) -> bool {
        self.to_timestamp.is_none()
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
    /// value. A record with `key: None` never matches a key pattern, and a
    /// header whose value is absent never matches a header pattern; neither
    /// is treated as matching an empty string. `timestamp_ms >= bound` is the
    /// only timestamp check, on the global `--to-timestamp` bound; it never
    /// reads `partition`, and it uses the identical `>=` comparison
    /// [`Self::decide_batch`] would need if it judged a record's timestamp
    /// itself, so the two can never disagree about the same record.
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
        if self.to_timestamp.is_some_and(|bound| timestamp_ms >= bound) {
            return RecordDecision::Drop;
        }
        if self.exclude_producer_id.contains(&producer_id) {
            return RecordDecision::Drop;
        }
        let offset_excluded = self.exclude_offset.get(partition).is_some_and(|ranges| {
            ranges
                .iter()
                .any(|&(start, end_exclusive)| offset >= start && offset < end_exclusive)
        });
        if offset_excluded {
            return RecordDecision::Drop;
        }
        if let Some(key) = record.key
            && self
                .exclude_key
                .iter()
                .any(|pattern| matches_utf8(pattern, key))
        {
            return RecordDecision::Drop;
        }
        let header_excluded = record.headers.iter().any(|header| {
            self.exclude_header.iter().any(|candidate| {
                candidate.name == header.key
                    && header
                        .value
                        .is_some_and(|value| matches_utf8(&candidate.pattern, value))
            })
        });
        if header_excluded {
            RecordDecision::Drop
        } else {
            RecordDecision::Keep
        }
    }
}

/// Whether `bytes` is valid UTF-8 and `pattern` matches the decoded text.
fn matches_utf8(pattern: &Regex, bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|text| pattern.is_match(text))
}
