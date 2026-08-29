//! Writing the restored cluster: the format, then the partition data.
//!
//! This module owns everything that touches the target log directory. It
//! formats the target through `krabka_format::run_from_args_with_records`,
//! forwarding the `--cluster-id`, `--node-id`, `--standalone`,
//! `--initial-controllers`, `--no-initial-controllers`, and
//! `--controller-listener` flags and seeding the `TopicRecord` and one
//! `PartitionRecord` per partition that the inventory recovered, so the
//! restored cluster boots with its topics already present. It then writes each
//! verified segment into the target partition, applying [`Predicates`] as it
//! walks the batches: a batch the bound keeps is written verbatim through
//! [`Log::append_verbatim_at`], with its base offset and leader epoch
//! restamped and its producer CRC untouched; a batch the bound filters is
//! rewritten through [`krabka_log::filter_batch`] and written through
//! [`Log::append_at`]; and a batch every one of whose records the bound
//! excludes is still written through [`Log::append_at`], as a bare header
//! with zero records and the archived `base_offset` and `last_offset_delta`
//! preserved. That third case is not optional: [`Log::append_at`] and
//! [`Log::append_verbatim_at`] both require `offset == log_end_offset()`, so
//! skipping a batch's write entirely leaves the target log's end offset
//! behind the archive's and makes every later batch in the partition
//! unappendable. Under `--dry-run` it does the same work and writes nothing.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_log::{FilteredBatch, Log, LogConfig, VerbatimBatch, filter_batch, name};
use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use krabka_protocol::records::{Attributes, RecordBatch, RecordBatchBorrowed, RecordBatchHeader};
use krabka_remote_storage::TopicIdPartition;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    args::{PartitionRef, RestoreArgs},
    bound::{BatchDecision, Predicates, RecordDecision},
    discover::ArchiveInventory,
    error::RestoreError,
    verify::VerifiedSegment,
};

/// What writing one segment into the target produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SegmentOutcome {
    /// The per-copy segment id in the archive.
    pub segment_id: Uuid,
    /// First offset the written segment holds.
    pub base_offset: Offset,
    /// Last offset the written segment holds.
    pub end_offset: Offset,
    /// Batches written unchanged.
    pub batches_kept: u64,
    /// Batches re-encoded because the bound dropped some of their records.
    pub batches_rewritten: u64,
    /// Batches written as a bare header because the bound excluded every
    /// record. The offset range stays claimed; see
    /// [`crate::bound::BatchDecision::Empty`].
    pub batches_emptied: u64,
    /// Records written.
    pub records_kept: u64,
    /// Records the bound dropped.
    pub records_dropped: u64,
    /// Bytes written into the target segment.
    pub bytes_written: u64,
}

/// Format the target log directory, seed it with the recovered topics, and
/// return the cluster id it was formatted with.
///
/// The formatter generates a cluster id when none is given and does not report
/// it back, so this passes an explicit `--cluster-id` and keeps it for the
/// report. An operator who restores a cluster has to know its identity.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when `--node-id` is absent: every restored partition names the target node as leader and sole replica, and defaulting that identity to node 0 could silently name a node the operator never said exists. Returns [`RestoreError::Format`] when the formatter rejects the target-side flags, and [`RestoreError::Io`] when the target cannot be written.
pub async fn format_target(
    args: &RestoreArgs,
    inventory: &ArchiveInventory,
) -> Result<Uuid, RestoreError> {
    let node_id = args.target.node_id.ok_or_else(|| {
        RestoreError::InvalidArgument(
            "--node-id is required: every restored partition names the target node as leader \
             and sole replica, so a restore cannot default that identity to node 0"
                .to_owned(),
        )
    })?;
    let cluster_id = args.target.cluster_id.unwrap_or_else(Uuid::new_v4);

    let mut format_argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        args.target.log_dir.to_string_lossy().into_owned(),
        "--cluster-id".to_owned(),
        cluster_id.to_string(),
        "--node-id".to_owned(),
        node_id.to_string(),
    ];
    if args.target.standalone {
        format_argv.push("--standalone".to_owned());
    }
    if !args.target.initial_controllers.is_empty() {
        format_argv.push("--initial-controllers".to_owned());
        format_argv.push(args.target.initial_controllers.join(","));
    }
    if args.target.no_initial_controllers {
        format_argv.push("--no-initial-controllers".to_owned());
    }
    if let Some(listener) = &args.target.controller_listener {
        format_argv.push("--controller-listener".to_owned());
        format_argv.push(listener.clone());
    }

    let extra = seed_metadata_records(inventory, node_id);
    let code = krabka_format::run_from_args_with_records(format_argv, extra).await;
    if code == 0 {
        Ok(cluster_id)
    } else {
        Err(RestoreError::Format { code })
    }
}

/// Build the topic and partition records a restore seeds into the target formatter, from what the archive scan recovered.
///
/// Every topic's [`MetadataRecord::V1Topic`] precedes every [`MetadataRecord::V1Partition`], which is the ordering `krabka_format::run_with_records`'s own doc requires: a `MetadataImage` derives a topic's partition count from the partition records that apply after it, so a partition can only follow its own topic.
///
/// Pulled out as a pure function, separate from [`format_target`]'s formatter call, so a test can check exactly what gets seeded without running the formatter at all.
fn seed_metadata_records(inventory: &ArchiveInventory, node_id: NodeId) -> Vec<MetadataRecord> {
    let mut topic_order: Vec<&str> = Vec::new();
    let mut topics: HashMap<&str, (Uuid, i32)> = HashMap::new();
    for entry in &inventory.partitions {
        let topic = entry.partition.topic.as_str();
        let counted = topics.entry(topic).or_insert_with(|| {
            topic_order.push(topic);
            (entry.partition.topic_id, 0)
        });
        counted.1 += 1;
    }

    let mut records = Vec::with_capacity(topic_order.len() + inventory.partitions.len());
    for topic in &topic_order {
        let (topic_id, partitions) = topics
            .get(topic)
            .copied()
            .expect("every topic in topic_order was inserted into topics above");
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: (*topic).to_owned(),
            topic_id,
            partitions,
            replication_factor: 1,
        }));
    }
    for entry in &inventory.partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: entry.partition.topic.clone(),
            partition: entry.partition.partition,
            leader: node_id,
            replicas: vec![node_id],
            isr: vec![node_id],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: Vec::new(),
            // KIP-631: 0 on creation. `PartitionRecord::default()`'s
            // `partition_epoch` is -1, the on-disk deserialization default for
            // a record written before this field existed; a freshly restored
            // partition is neither, so this is set explicitly rather than
            // pulled from `..Default::default()`.
            partition_epoch: 0,
        }));
    }
    records
}

/// Write one verified segment into its target partition, under the bound.
///
/// # Errors
///
/// Returns [`RestoreError::Records`] when a batch will not re-encode, and
/// [`RestoreError::Log`] or [`RestoreError::Io`] when the target rejects the
/// write.
pub async fn write_segment(
    args: &RestoreArgs,
    partition: &TopicIdPartition,
    segment: &VerifiedSegment,
    predicates: &Predicates,
) -> Result<SegmentOutcome, RestoreError> {
    let partition_ref = PartitionRef {
        topic: partition.topic.clone(),
        partition: partition.partition,
    };
    let bound = predicates.offset_bound(&partition_ref);

    let mut outcome = SegmentOutcome {
        segment_id: segment.facts.segment_id,
        base_offset: segment.facts.base_offset,
        end_offset: segment.facts.end_offset,
        batches_kept: 0,
        batches_rewritten: 0,
        batches_emptied: 0,
        records_kept: 0,
        records_dropped: 0,
        bytes_written: 0,
    };

    // Every batch in this segment starts past the keep bound: an earlier
    // segment already ended the partition's restored history, so nothing
    // here survives and the target log is never opened for it.
    if bound.is_some_and(|b| segment.facts.base_offset > b) {
        return Ok(outcome);
    }

    let mut log = if args.dry_run {
        None
    } else {
        let dir = name::partition_dir(&args.target.log_dir, &partition.topic, partition.partition);
        std::fs::create_dir_all(&dir)?;
        Some(Log::open(&dir, LogConfig::default())?)
    };
    if let Some(log) = log.as_mut() {
        align_log_to_segment(log, segment, partition)?;
    }

    let raw: &[u8] = &segment.log;
    let mut pos = 0usize;
    while pos < raw.len() {
        let mut cursor = &raw[pos..];
        let remaining_before = cursor.len();
        // `<_>::default()` stands in for `RecordDecompressionPolicy::default()`.
        // `krabka-compression`, which defines that type, is only a transitive
        // dependency here (through `krabka-protocol`), so this crate has no
        // path to name it; the default policy this crate would otherwise ask
        // for by name is exactly what a bare `decode_borrow_with_policy`
        // caller already gets through inference here.
        let batch = RecordBatchBorrowed::decode_borrow_with_policy(&mut cursor, <_>::default())?;
        let consumed = remaining_before - cursor.len();
        let header = batch.header();
        let batch_base_offset = Offset(header.base_offset.get());

        if bound.is_some_and(|b| batch_base_offset > b) {
            break;
        }

        let batch_bytes = segment.log.slice(pos..pos + consumed);
        let records_in_batch = nonneg_u64(header.records_count.get());

        let (mut prepared, tally) = prepare_batch(
            &partition_ref,
            predicates,
            &batch,
            batch_bytes,
            records_in_batch,
        )?;

        let last_offset_delta = prepared.last_offset_delta();
        outcome.bytes_written += bytes_len(prepared.encoded_len());
        outcome.end_offset = batch_base_offset + i64::from(last_offset_delta);

        if let Some(log) = log.as_mut() {
            match &mut prepared {
                PreparedBatch::Verbatim(verbatim) => {
                    log.append_verbatim_at(verbatim, batch_base_offset)?;
                }
                PreparedBatch::Owned(owned) => {
                    log.append_at(owned, batch_base_offset)?;
                }
            }
        }

        match tally {
            BatchTally::Kept => {
                outcome.batches_kept += 1;
                outcome.records_kept += records_in_batch;
            }
            BatchTally::Rewritten { kept, dropped } => {
                outcome.batches_rewritten += 1;
                outcome.records_kept += kept;
                outcome.records_dropped += dropped;
            }
            BatchTally::Emptied => {
                outcome.batches_emptied += 1;
                outcome.records_dropped += records_in_batch;
            }
        }

        pos += consumed;
    }

    if let Some(log) = log.as_mut() {
        log.sync()?;
    }

    Ok(outcome)
}

/// Align `log`'s end offset to `segment`'s declared base offset before any of its batches are appended.
///
/// `Log::append_at`/`Log::append_verbatim_at` both require the offset passed in to equal `log.log_end_offset()`. Segments arrive in base-offset order within a partition, so the common case is already aligned. The one legitimate exception is the very first segment written into a brand-new, still-empty target log, when the archive's first surviving base offset is not zero: `Log::open` always starts a fresh directory's active segment at offset zero, so that case needs one `reset_to` to slide the empty log's base up to match. Anything else — a log that already holds data whose end offset does not match this segment's base offset — is a sequencing bug upstream, not a case this function may paper over by resetting: `reset_to` is destructive, and calling it here would discard whatever an earlier call already wrote for this partition.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when `log` already holds data that does not end where `segment` begins.
fn align_log_to_segment(
    log: &mut Log,
    segment: &VerifiedSegment,
    partition: &TopicIdPartition,
) -> Result<(), RestoreError> {
    let leo = log.log_end_offset();
    if leo == segment.facts.base_offset {
        return Ok(());
    }
    if leo == Offset(0) {
        log.reset_to(segment.facts.base_offset)?;
        return Ok(());
    }
    Err(RestoreError::InvalidArgument(format!(
        "segment {} of {}-{} starts at offset {}, but the target log already ends at {leo}; \
         segments must be written in contiguous base-offset order",
        segment.facts.segment_id, partition.topic, partition.partition, segment.facts.base_offset,
    )))
}

/// What one archived batch becomes when it is appended to the target log: the exact bytes the log's zero-copy path needs, or an owned batch for the log's decode-and-append path.
enum PreparedBatch {
    Verbatim(VerbatimBatch),
    Owned(RecordBatch),
}

impl PreparedBatch {
    fn last_offset_delta(&self) -> i32 {
        match self {
            Self::Verbatim(batch) => batch.last_offset_delta,
            Self::Owned(batch) => batch.last_offset_delta,
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Verbatim(batch) => batch.bytes.len(),
            Self::Owned(batch) => batch.encoded_len(),
        }
    }
}

/// How [`prepare_batch`]'s outcome folds into [`SegmentOutcome`]'s counts.
enum BatchTally {
    Kept,
    Rewritten { kept: u64, dropped: u64 },
    Emptied,
}

/// Decide one archived batch's fate under `predicates`, and prepare what gets appended to the target log without touching the log itself, so `--dry-run` can share this exact path with a real run.
fn prepare_batch(
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

/// Convert a header's `records_count` (or any small non-negative `i32` count) to `u64`, treating a corrupt-but-CRC-valid negative count as zero rather than panicking. [`crate::verify::verify_segment`] has already checked every batch's CRC by the time this module sees it.
fn nonneg_u64(count: i32) -> u64 {
    u64::try_from(count.max(0)).unwrap_or(0)
}

/// Convert a byte length to `u64` for [`SegmentOutcome::bytes_written`].
fn bytes_len(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::BytesMut;
    use clap::Parser as _;

    use super::*;
    use crate::{discover::PartitionInventory, verify::SegmentFacts};

    /// Parses `RestoreArgs` the same way the binary does, with a fixed dummy
    /// archive source and the given `log_dir`, so a test only has to state
    /// the target and bound flags under test.
    fn args_from(extra: &[&str], log_dir: &std::path::Path) -> RestoreArgs {
        let mut argv = vec![
            "krabka-restore".to_owned(),
            "--archive-local".to_owned(),
            "/archive".to_owned(),
            "--log-dir".to_owned(),
            log_dir.display().to_string(),
        ];
        argv.extend(extra.iter().map(|s| (*s).to_owned()));
        crate::Cli::try_parse_from(argv)
            .expect("valid command line")
            .args
    }

    fn topic_id_partition(topic: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition {
            topic_id: Uuid::new_v4(),
            topic: topic.to_owned(),
            partition,
        }
    }

    /// A minimal record at `offset_delta`, with no key or headers.
    fn record(offset_delta: i32, value: &str) -> krabka_protocol::records::Record {
        krabka_protocol::records::Record {
            attributes: 0,
            timestamp_delta: i64::from(offset_delta),
            offset_delta,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        }
    }

    /// A record like [`record`], but with a key an `--exclude-key` pattern
    /// can match.
    fn record_with_key(offset_delta: i32, key: &str) -> krabka_protocol::records::Record {
        krabka_protocol::records::Record {
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            ..record(offset_delta, "v")
        }
    }

    /// A batch at `base_offset` holding `records`, with `last_offset_delta`
    /// derived from the highest `offset_delta` among them, matching how a
    /// real producer batch is shaped.
    fn batch(base_offset: i64, records: Vec<krabka_protocol::records::Record>) -> RecordBatch {
        let last_offset_delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
        RecordBatch {
            base_offset,
            partition_leader_epoch: 7,
            attributes: Attributes::default(),
            last_offset_delta,
            base_timestamp: 1_700_000_000_000,
            max_timestamp: 1_700_000_000_000 + i64::from(last_offset_delta),
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }

    fn encode(batches: &[RecordBatch]) -> Bytes {
        let mut buf = BytesMut::new();
        for b in batches {
            b.encode(&mut buf).expect("encode");
        }
        buf.freeze()
    }

    /// Build a [`VerifiedSegment`] from already-encoded `batches`, the way
    /// `verify_segment` would have handed it to `write_segment`.
    fn verified_segment(base_offset: i64, batches: &[RecordBatch]) -> VerifiedSegment {
        let log = encode(batches);
        let last = batches.last().expect("at least one batch");
        let end_offset = last.base_offset + i64::from(last.last_offset_delta);
        let records: u64 = batches
            .iter()
            .map(|b| u64::try_from(b.records.len()).unwrap_or(0))
            .sum();
        VerifiedSegment {
            facts: SegmentFacts {
                segment_id: Uuid::new_v4(),
                base_offset: Offset(base_offset),
                end_offset: Offset(end_offset),
                max_timestamp_ms: batches.iter().map(|b| b.max_timestamp).max().unwrap_or(-1),
                batches: u64::try_from(batches.len()).unwrap_or(0),
                records,
                log_bytes: u64::try_from(log.len()).unwrap_or(0),
                leader_epochs: Vec::new(),
            },
            log,
        }
    }

    fn partition_inventory(topic: &str, topic_id: Uuid, partition: i32) -> PartitionInventory {
        PartitionInventory {
            partition: TopicIdPartition {
                topic_id,
                topic: topic.to_owned(),
                partition,
            },
            segments: Vec::new(),
        }
    }

    #[tokio::test]
    async fn keep_only_batches_round_trip_verbatim_at_their_original_offsets() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let batches = vec![
            batch(0, vec![record(0, "a"), record(1, "b"), record(2, "c")]),
            batch(3, vec![record(0, "d"), record(1, "e")]),
        ];
        let segment = verified_segment(0, &batches);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_kept == 2);
        check!(outcome.batches_rewritten == 0);
        check!(outcome.batches_emptied == 0);
        check!(outcome.records_kept == 5);
        check!(outcome.records_dropped == 0);
        check!(outcome.end_offset == Offset(4));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == batches);
    }

    #[tokio::test]
    async fn an_emptied_batch_becomes_a_bare_header_and_the_next_batch_still_appends() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&["--exclude-key", "^drop$"], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let excluded = vec![
            record_with_key(0, "drop"),
            record_with_key(1, "drop"),
            record_with_key(2, "drop"),
        ];
        let batch_a = batch(0, excluded);
        let batch_b = batch(3, vec![record(0, "kept")]);
        let segment = verified_segment(0, &[batch_a, batch_b]);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_emptied == 1);
        check!(outcome.batches_kept == 1);
        check!(outcome.records_dropped == 3);
        check!(outcome.records_kept == 1);
        check!(outcome.end_offset == Offset(3));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        // The bare header's full archived span (offsets 0..=2) is still
        // claimed, so log_end_offset already accounts for batch_b at offset 3.
        check!(log.log_end_offset() == Offset(4));

        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches.len() == 2);
        check!(read.batches[0].base_offset == 0);
        check!(read.batches[0].last_offset_delta == 2);
        check!(read.batches[0].records.is_empty());
        check!(read.batches[1].base_offset == 3);
    }

    #[tokio::test]
    async fn a_filtered_batch_keeps_the_survivors_original_absolute_offsets() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&["--exclude-key", "^drop$"], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let batch_a = batch(
            0,
            vec![
                record(0, "keep0"),
                record_with_key(1, "drop"),
                record(2, "keep2"),
            ],
        );
        let segment = verified_segment(0, std::slice::from_ref(&batch_a));

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");

        check!(outcome.batches_rewritten == 1);
        check!(outcome.batches_kept == 0);
        check!(outcome.batches_emptied == 0);
        check!(outcome.records_kept == 2);
        check!(outcome.records_dropped == 1);
        check!(outcome.end_offset == Offset(2));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        let expected = RecordBatch {
            last_offset_delta: 2,
            records: vec![record(0, "keep0"), record(2, "keep2")],
            ..batch_a
        };
        check!(read.batches == vec![expected]);
    }

    #[tokio::test]
    async fn a_filtered_batch_that_drops_its_trailing_record_does_not_strand_the_next_batch() {
        // The archive is contiguous by construction: `batch_b`'s base offset
        // (3) is exactly where `batch_a`'s ORIGINAL span (offsets 0..=2)
        // ends. If `write_segment` used `filter_batch`'s recomputed
        // `last_offset_delta` (which shrinks to the highest SURVIVING
        // delta -- 1, once offset 2 is excluded) instead of the archived
        // value, `log.log_end_offset()` after `batch_a` would be 2, not 3,
        // and appending `batch_b` at its true archived offset 3 would fail
        // with `LogError::OffsetMismatch`. This is the regression a Filter
        // decision that happens to exclude a batch's LAST record must not
        // reintroduce.
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&["--exclude-key", "^drop$"], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let batch_a = batch(0, vec![record(0, "keep0"), record_with_key(2, "drop")]);
        let batch_b = batch(3, vec![record(0, "keep3")]);
        let segment = verified_segment(0, &[batch_a.clone(), batch_b.clone()]);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment must not fail with an offset mismatch");

        check!(outcome.batches_rewritten == 1);
        check!(outcome.batches_kept == 1);
        check!(outcome.records_kept == 2);
        check!(outcome.records_dropped == 1);
        check!(outcome.end_offset == Offset(3));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches.len() == 2);
        // `last_offset_delta` stays at the archived value (2), not the
        // shrunk-to-survivors value (0), so `batch_b` below still lands at
        // its true offset.
        check!(read.batches[0].base_offset == 0);
        check!(read.batches[0].last_offset_delta == 2);
        check!(read.batches[0].records == vec![record(0, "keep0")]);
        check!(read.batches[1].base_offset == 3);
        check!(read.batches[1].records == vec![record(0, "keep3")]);
    }

    #[tokio::test]
    async fn a_second_segment_continues_without_resetting_the_first() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let batch_1 = batch(0, vec![record(0, "a"), record(1, "b")]);
        let segment_1 = verified_segment(0, std::slice::from_ref(&batch_1));
        let outcome_1 = write_segment(&args, &partition, &segment_1, &predicates)
            .await
            .expect("segment 1");
        check!(outcome_1.end_offset == Offset(1));

        let batch_2 = batch(2, vec![record(0, "c")]);
        let segment_2 = verified_segment(2, std::slice::from_ref(&batch_2));
        let outcome_2 = write_segment(&args, &partition, &segment_2, &predicates)
            .await
            .expect("segment 2");
        check!(outcome_2.end_offset == Offset(2));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        // The first segment's data survives the second call: no destructive
        // reset happened in between.
        check!(read.batches == vec![batch_1, batch_2]);
    }

    #[tokio::test]
    async fn the_first_segment_resets_an_empty_log_to_a_nonzero_base() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let partition = topic_id_partition("orders", 0);
        let predicates = Predicates::from_args(&args).expect("predicates");

        let batches = vec![batch(1000, vec![record(0, "x"), record(1, "y")])];
        let segment = verified_segment(1000, &batches);

        let outcome = write_segment(&args, &partition, &segment, &predicates)
            .await
            .expect("write_segment");
        check!(outcome.base_offset == Offset(1000));
        check!(outcome.end_offset == Offset(1001));

        let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen");
        check!(log.log_start_offset() == Offset(1000));
        check!(log.log_end_offset() == Offset(1002));
        let read = log
            .read(Offset(1000), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == batches);
    }

    #[tokio::test]
    async fn dry_run_matches_a_real_run_but_writes_nothing() {
        let batches = vec![
            batch(0, vec![record(0, "a"), record(1, "b")]),
            batch(2, vec![record(0, "c")]),
        ];
        let partition = topic_id_partition("orders", 0);
        // One shared fixture: `SegmentOutcome::segment_id` echoes
        // `segment.facts.segment_id`, and that id is random per fixture, so
        // both runs must see the exact same `VerifiedSegment` for their
        // outcomes to compare equal on that field too.
        let segment = verified_segment(0, &batches);

        let real_target = tempfile::tempdir().expect("tempdir");
        let real_args = args_from(&[], real_target.path());
        let real_predicates = Predicates::from_args(&real_args).expect("predicates");
        let real_outcome = write_segment(&real_args, &partition, &segment, &real_predicates)
            .await
            .expect("real write");

        let dry_target = tempfile::tempdir().expect("tempdir");
        let mut dry_args = args_from(&[], dry_target.path());
        dry_args.dry_run = true;
        let dry_predicates = Predicates::from_args(&dry_args).expect("predicates");
        let dry_outcome = write_segment(&dry_args, &partition, &segment, &dry_predicates)
            .await
            .expect("dry write");

        check!(dry_outcome == real_outcome);
        check!(!dry_target.path().join("orders-0").exists());
        check!(real_target.path().join("orders-0").exists());
    }

    #[test]
    fn seed_metadata_records_emits_every_topic_before_any_partition() {
        let orders_id = Uuid::new_v4();
        let payments_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", orders_id, 0),
                partition_inventory("orders", orders_id, 1),
                partition_inventory("payments", payments_id, 0),
            ],
            unrecognized: Vec::new(),
        };

        let records = seed_metadata_records(&inventory, NodeId(7));

        let partition_record = |topic: &str, partition: i32| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition,
                leader: NodeId(7),
                replicas: vec![NodeId(7)],
                isr: vec![NodeId(7)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            })
        };
        let expected = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id: orders_id,
                partitions: 2,
                replication_factor: 1,
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "payments".to_owned(),
                topic_id: payments_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("orders", 0),
            partition_record("orders", 1),
            partition_record("payments", 0),
        ];
        check!(records == expected);
    }

    #[tokio::test]
    async fn format_target_requires_node_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: Vec::new(),
        };

        let result = format_target(&args, &inventory).await;
        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn format_target_formats_the_target_and_returns_the_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
            ],
            target.path(),
        );
        let topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", topic_id, 0),
                partition_inventory("orders", topic_id, 1),
            ],
            unrecognized: Vec::new(),
        };

        let cluster_id = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(args.target.cluster_id.is_none() || Some(cluster_id) == args.target.cluster_id);
        check!(target.path().join("bootstrap.json").exists());
        check!(target.path().join("bootstrap.records.bin").exists());
    }

    #[tokio::test]
    async fn format_target_honors_an_explicit_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let fixed = Uuid::new_v4();
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
                "--cluster-id",
                &fixed.to_string(),
            ],
            target.path(),
        );
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: Vec::new(),
        };

        let cluster_id = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(cluster_id == fixed);
    }
}
