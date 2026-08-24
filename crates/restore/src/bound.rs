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

use crabka_ids::{Offset, ProducerId};
use crabka_protocol::records::{RecordBatchBorrowed, RecordBorrowed};
use regex::Regex;

use crate::{
    args::{HeaderPattern, PartitionRef, RestoreArgs},
    error::RestoreError,
};

/// What the bound decides about one archived batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDecision {
    /// No predicate touches the batch. It is written verbatim, so its bytes
    /// stay byte-identical to the archived copy.
    Keep,
    /// Every record in the batch is excluded. The batch's offset range is
    /// still written, as a bare header with zero records:
    /// [`crabka_log::filter_batch`] calls this outcome
    /// [`crabka_log::FilteredBatch::Empty`] and leaves the caller to decide
    /// what an emptied batch becomes, and for a restore that must preserve
    /// offsets, becoming a bare header is the only choice that does not
    /// silently shift every later record. The bare header keeps the archived
    /// `base_offset` and `last_offset_delta` unchanged, exactly as krabka's
    /// own log cleaner does on its `RETAIN_EMPTY` path for the same reason.
    Empty,
    /// Some records survive. The batch is re-encoded from the records that
    /// [`Predicates::decide_record`] keeps, so its bytes differ from the
    /// archived copy. [`crabka_log::filter_batch`] keeps the batch's
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
    /// Compile the bound flags into a predicate set.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when a partition's
    /// `--exclude-offset` ranges, merged, cover every offset from `0` through
    /// that partition's `--to-offset` bound: nothing between `0` and the
    /// bound can survive, so the bound keeps zero records for that partition.
    /// That is the one flag combination this function can prove empties a
    /// partition without reading the archive, because both windows are named
    /// entirely by the flags. Two related cases are deliberately not
    /// flagged here. A negative `--to-offset`, which would exclude a whole
    /// partition outright, is already rejected by [`RestoreArgs`]'s own
    /// parser and never reaches this function. A `--exclude-key` or
    /// `--exclude-header` pattern that matches every possible byte string,
    /// such as an empty pattern or `.*`, is not detected either: proving a
    /// regex is universal is not a check this function attempts, a partial
    /// heuristic would catch some universal patterns and not others, and
    /// unlike an offset window, a key predicate never empties a partition on
    /// its own anyway, because a keyless record still survives it.
    pub fn from_args(args: &RestoreArgs) -> Result<Self, RestoreError> {
        let mut to_offset = HashMap::with_capacity(args.to_offset.len());
        for bound in &args.to_offset {
            to_offset.insert(bound.partition.clone(), bound.last_offset);
        }

        let mut exclude_offset: HashMap<PartitionRef, Vec<(Offset, Offset)>> = HashMap::new();
        for range in &args.exclude_offset {
            exclude_offset
                .entry(range.partition.clone())
                .or_default()
                .push((range.start, range.end_exclusive));
        }

        for (partition, &last_offset) in &to_offset {
            let fully_excluded = exclude_offset
                .get(partition)
                .is_some_and(|ranges| fully_covers(ranges, last_offset));
            if fully_excluded {
                return Err(RestoreError::InvalidArgument(format!(
                    "--exclude-offset excludes every offset that --to-offset keeps in {partition}"
                )));
            }
        }

        Ok(Self {
            to_offset,
            to_timestamp: args.to_timestamp,
            exclude_key: args.exclude_key.clone(),
            exclude_header: args.exclude_header.clone(),
            exclude_producer_id: args.exclude_producer_id.iter().copied().collect(),
            exclude_offset,
        })
    }

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

/// Whether `ranges`, merged, cover every offset in `0..=last_offset`.
///
/// This is the arithmetic behind the "can never keep a record" check in
/// [`Predicates::from_args`]: a partition's whole possible keep window, from
/// offset zero through its `--to-offset` bound, counts as covered only when
/// the exclude ranges leave no gap in it.
fn fully_covers(ranges: &[(Offset, Offset)], last_offset: Offset) -> bool {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|&(start, _)| start);

    let mut covered_through = Offset::ZERO;
    for (start, end_exclusive) in sorted {
        if start > covered_through {
            return false;
        }
        if end_exclusive > covered_through {
            covered_through = end_exclusive;
        }
        if covered_through > last_offset {
            return true;
        }
    }
    covered_through > last_offset
}

/// Whether `bytes` is valid UTF-8 and `pattern` matches the decoded text.
fn matches_utf8(pattern: &Regex, bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok_and(|text| pattern.is_match(text))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::{Bytes, BytesMut};
    use clap::Parser as _;
    use crabka_protocol::{
        DecodeBorrow as _,
        records::{Attributes, Record, RecordBatch, RecordHeader},
    };

    use super::*;

    const BASE_OFFSET: i64 = 1_000;
    const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

    fn partition(topic: &str, index: i32) -> PartitionRef {
        PartitionRef {
            topic: topic.to_owned(),
            partition: index,
        }
    }

    /// Parses `RestoreArgs` the same way the binary does, with a fixed
    /// archive source and target so a test only has to state the bound
    /// flags under test.
    fn args_from(extra: &[&str]) -> RestoreArgs {
        let mut argv = vec![
            "krabka-restore",
            "--archive-local",
            "/archive",
            "--log-dir",
            "/target",
        ];
        argv.extend_from_slice(extra);
        crate::Cli::try_parse_from(argv)
            .expect("valid command line")
            .args
    }

    fn predicates(extra: &[&str]) -> Predicates {
        Predicates::from_args(&args_from(extra)).expect("valid predicates")
    }

    /// A minimal record at `offset_delta`, with an arbitrary value and no key
    /// or headers. Override fields with struct-update syntax.
    fn record(offset_delta: i32) -> Record {
        Record {
            offset_delta,
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        }
    }

    fn header(name: &str, value: &[u8]) -> RecordHeader {
        RecordHeader {
            key: name.to_owned(),
            value: Some(Bytes::copy_from_slice(value)),
        }
    }

    // Every header field holds a distinctive value, matching
    // `crabka_log::filter::tests::batch`'s convention, so a mistaken swap
    // between two header fields would show up as a wrong test outcome.
    fn batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            base_offset: BASE_OFFSET,
            partition_leader_epoch: 3,
            attributes: Attributes::default(),
            last_offset_delta: records.iter().map(|r| r.offset_delta).max().unwrap_or(0),
            base_timestamp: BASE_TIMESTAMP,
            max_timestamp: BASE_TIMESTAMP
                + records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0),
            producer_id,
            producer_epoch: 0,
            base_sequence: 0,
            records,
        }
    }

    fn encode(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf).expect("encode");
        buf.to_vec()
    }

    fn borrow(bytes: &[u8]) -> RecordBatchBorrowed<'_> {
        let mut cursor = bytes;
        // `version` is unused by the v2 batch decoder; any value does.
        RecordBatchBorrowed::decode_borrow(&mut cursor, 0)
            .expect("decode a borrowed batch back out of its own encoding")
    }

    /// Runs both `decide_batch` and `decide_record` for every record, the way
    /// `materialize.rs` would: a batch-level verdict, and the per-record
    /// verdicts in batch order.
    fn decide(
        predicates: &Predicates,
        partition: &PartitionRef,
        owned: &RecordBatch,
    ) -> (BatchDecision, Vec<RecordDecision>) {
        let encoded = encode(owned);
        let borrowed = borrow(&encoded);
        let header = borrowed.header();
        let base_offset = header.base_offset.get();
        let base_timestamp = header.base_timestamp.get();
        let producer_id = ProducerId(header.producer_id.get());

        let batch_decision = predicates.decide_batch(partition, &borrowed);
        let record_decisions = borrowed
            .iter()
            .map(|parsed| {
                let record = parsed.expect("parse record");
                let offset = Offset(base_offset + i64::from(record.offset_delta));
                let timestamp_ms = base_timestamp + record.timestamp_delta;
                predicates.decide_record(partition, offset, timestamp_ms, producer_id, &record)
            })
            .collect();
        (batch_decision, record_decisions)
    }

    #[test]
    fn no_predicates_keep_everything() {
        let predicates = predicates(&[]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    key: Some(Bytes::from_static(b"k0")),
                    ..record(0)
                },
                Record {
                    key: None,
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
    }

    #[test]
    fn to_offset_bound_is_inclusive_at_the_named_offset() {
        let predicates = predicates(&["--to-offset", "orders:0=42"]);

        check!(predicates.offset_bound(&partition("orders", 0)) == Some(Offset(42)));
        check!(predicates.offset_bound(&partition("orders", 1)).is_none());
        check!(predicates.offset_bound(&partition("other", 0)).is_none());
    }

    #[test]
    fn exclude_key_filters_only_matching_records() {
        let predicates = predicates(&["--exclude-key", "^alpha"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    key: Some(Bytes::from_static(b"alpha-1")),
                    ..record(0)
                },
                Record {
                    key: Some(Bytes::from_static(b"beta-1")),
                    ..record(1)
                },
                Record {
                    key: Some(Bytes::from_static(b"alpha-2")),
                    ..record(2)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Filter);
        check!(
            records
                == [
                    RecordDecision::Drop,
                    RecordDecision::Keep,
                    RecordDecision::Drop,
                ]
        );
    }

    #[test]
    fn exclude_key_matching_every_record_empties_the_batch() {
        let predicates = predicates(&["--exclude-key", "^k"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    key: Some(Bytes::from_static(b"k1")),
                    ..record(0)
                },
                Record {
                    key: Some(Bytes::from_static(b"k2")),
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Empty);
        check!(records == [RecordDecision::Drop, RecordDecision::Drop]);
    }

    #[test]
    fn exclude_key_matching_nothing_keeps_the_batch() {
        let predicates = predicates(&["--exclude-key", "^zzz"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    key: Some(Bytes::from_static(b"k1")),
                    ..record(0)
                },
                Record {
                    key: Some(Bytes::from_static(b"k2")),
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
    }

    #[test]
    fn a_keyless_record_never_matches_an_exclude_key_pattern_even_dot_star() {
        let predicates = predicates(&["--exclude-key", ".*"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    key: None,
                    ..record(0)
                },
                Record {
                    key: Some(Bytes::from_static(b"anything")),
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Filter);
        check!(records == [RecordDecision::Keep, RecordDecision::Drop]);
    }

    #[test]
    fn exclude_header_matches_on_name_and_value_not_name_alone() {
        let predicates = predicates(&["--exclude-header", "trace=^bad"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    headers: vec![header("trace", b"bad-1")],
                    ..record(0)
                },
                Record {
                    headers: vec![header("trace", b"good-1")],
                    ..record(1)
                },
                Record {
                    headers: vec![header("other", b"bad-1")],
                    ..record(2)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Filter);
        check!(
            records
                == [
                    RecordDecision::Drop,
                    RecordDecision::Keep,
                    RecordDecision::Keep,
                ]
        );
    }

    #[test]
    fn exclude_producer_id_drops_every_record_from_that_producer_and_no_other() {
        let predicates = predicates(&["--exclude-producer-id", "7"]);
        let orders_0 = partition("orders", 0);

        let named = batch(7, vec![record(0), record(1)]);
        let (batch_decision, records) = decide(&predicates, &orders_0, &named);
        check!(batch_decision == BatchDecision::Empty);
        check!(records == [RecordDecision::Drop, RecordDecision::Drop]);

        let other = batch(8, vec![record(0), record(1)]);
        let (batch_decision, records) = decide(&predicates, &orders_0, &other);
        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
    }

    #[test]
    fn exclude_offset_range_is_half_open() {
        // BASE_OFFSET is 1_000, so offset_delta N is absolute offset 1_000+N.
        // The range 1001..1003 must drop 1001 (inclusive start) and 1002, and
        // keep 1000 and 1003 (exclusive end).
        let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(1, vec![record(0), record(1), record(2), record(3)]);

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Filter);
        check!(
            records
                == [
                    RecordDecision::Keep,
                    RecordDecision::Drop,
                    RecordDecision::Drop,
                    RecordDecision::Keep,
                ]
        );
    }

    #[test]
    fn exclude_offset_only_applies_to_its_named_partition() {
        let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
        let orders_1 = partition("orders", 1);
        let owned = batch(1, vec![record(1)]);

        let (batch_decision, records) = decide(&predicates, &orders_1, &owned);

        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep]);
    }

    #[test]
    fn to_timestamp_entirely_before_the_bound_keeps_the_batch() {
        let bound = BASE_TIMESTAMP + 100;
        let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    timestamp_delta: 0,
                    ..record(0)
                },
                Record {
                    timestamp_delta: 50,
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
    }

    #[test]
    fn to_timestamp_entirely_at_or_after_the_bound_empties_the_batch() {
        let bound = BASE_TIMESTAMP + 100;
        let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    timestamp_delta: 100,
                    ..record(0)
                },
                Record {
                    timestamp_delta: 200,
                    ..record(1)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Empty);
        check!(records == [RecordDecision::Drop, RecordDecision::Drop]);
    }

    #[test]
    fn to_timestamp_straddling_the_bound_filters_the_right_split() {
        let bound = BASE_TIMESTAMP + 100;
        let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            1,
            vec![
                Record {
                    timestamp_delta: 0,
                    ..record(0)
                },
                Record {
                    timestamp_delta: 100,
                    ..record(1)
                },
                Record {
                    timestamp_delta: 150,
                    ..record(2)
                },
            ],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Filter);
        check!(
            records
                == [
                    RecordDecision::Keep,
                    RecordDecision::Drop,
                    RecordDecision::Drop,
                ]
        );
    }

    #[test]
    fn predicates_that_both_match_one_record_still_drop_it_once() {
        let predicates = predicates(&["--exclude-key", "^bad", "--exclude-producer-id", "9"]);
        let orders_0 = partition("orders", 0);
        let owned = batch(
            9,
            vec![Record {
                key: Some(Bytes::from_static(b"bad-1")),
                ..record(0)
            }],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Empty);
        check!(records == [RecordDecision::Drop]);
    }

    #[test]
    fn non_utf8_key_bytes_never_match_and_do_not_panic() {
        let predicates = predicates(&["--exclude-key", ".*"]);
        let orders_0 = partition("orders", 0);
        let invalid_utf8: &[u8] = &[0xFF, 0xFE, 0xFD];
        let owned = batch(
            1,
            vec![Record {
                key: Some(Bytes::copy_from_slice(invalid_utf8)),
                ..record(0)
            }],
        );

        let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

        check!(batch_decision == BatchDecision::Keep);
        check!(records == [RecordDecision::Keep]);
    }

    #[test]
    fn exclude_offset_fully_covering_the_to_offset_window_is_rejected() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..6",
        ]));

        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[test]
    fn exclude_offset_covering_the_window_through_several_ranges_is_rejected() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..3",
            "--exclude-offset",
            "orders:0=3..6",
        ]));

        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[test]
    fn exclude_offset_leaving_any_gap_in_the_window_is_accepted() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:0=0..3",
        ]));

        check!(result.is_ok());
    }

    #[test]
    fn exclude_offset_covering_an_unrelated_partition_does_not_reject() {
        let result = Predicates::from_args(&args_from(&[
            "--to-offset",
            "orders:0=5",
            "--exclude-offset",
            "orders:1=0..100",
        ]));

        check!(result.is_ok());
    }

    #[test]
    fn no_to_offset_bound_means_no_coverage_check_at_all() {
        let result =
            Predicates::from_args(&args_from(&["--exclude-offset", "orders:0=0..1000000"]));

        check!(result.is_ok());
    }
}
