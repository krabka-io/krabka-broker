//! Validation and materialization of captured committed diskless-WAL state.

use {
    crate::{
        ArchiveInventory, ArchiveStore, PartitionInventory, RestoreArgs, RestoreError,
        args::{PartitionRef, offset_partitions},
        bound::Predicates,
        materialize::{
            open_partition_log,
            prepare::{PrepareBatchSetup, prepare_batch},
        },
        report::{DisklessPartitionReport, DisklessRestoreReport},
        verify::{batch_record_count, fetch_capped},
    },
    bytes::Bytes,
    krabka_ids::Offset,
    krabka_log::Log,
    krabka_object_store::MAX_WAL_OBJECT_BYTES,
    krabka_protocol::records::{RecordBatchBorrowed, validate_one_v2_batch},
    krabka_remote_storage::{
        ObjectEntry, TopicIdPartition,
        diskless::{
            capture::{CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture},
            object::parse_wal_object,
        },
    },
    object_store::path::Path as ObjectPath,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        ops::Range,
        path::Path,
    },
    tokio::fs,
};

pub async fn load(path: Option<&Path>) -> Result<Option<DisklessWalCapture>, RestoreError> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = fs::read(path).await?;
    DisklessWalCapture::from_slice(&bytes)
        .map(Some)
        .map_err(RestoreError::Integrity)
}

pub fn add_partitions(
    inventory: &mut ArchiveInventory,
    capture: &DisklessWalCapture,
    args: &RestoreArgs,
) -> Result<(), RestoreError> {
    let mut seen = HashSet::new();
    let mut topic_ids = HashMap::new();
    let mut named_partitions = HashSet::new();
    for existing in &inventory.partitions {
        if topic_ids
            .insert(
                existing.partition.topic.clone(),
                existing.partition.topic_id,
            )
            .is_some_and(|topic_id| topic_id != existing.partition.topic_id)
        {
            return Err(RestoreError::Integrity(format!(
                "topic {} has multiple topic ids in the restore inputs",
                existing.partition.topic
            )));
        }
        named_partitions.insert((
            existing.partition.topic.clone(),
            existing.partition.partition,
        ));
    }
    for partition in &capture.partitions {
        if !args.selects_topic(&partition.topic) {
            continue;
        }
        if !seen.insert((partition.topic_id, partition.partition)) {
            return Err(RestoreError::Integrity(format!(
                "duplicate diskless capture partition {}-{}",
                partition.topic, partition.partition
            )));
        }
        if topic_ids
            .insert(partition.topic.clone(), partition.topic_id)
            .is_some_and(|topic_id| topic_id != partition.topic_id)
        {
            return Err(RestoreError::Integrity(format!(
                "topic {} has multiple topic ids in the restore inputs",
                partition.topic
            )));
        }
        if !named_partitions.insert((partition.topic.clone(), partition.partition)) {
            return Err(RestoreError::Integrity(format!(
                "{}-{} is both classic and diskless in the restore inputs",
                partition.topic, partition.partition
            )));
        }
        if inventory.partitions.iter().any(|existing| {
            existing.partition.topic_id == partition.topic_id
                && existing.partition.partition == partition.partition
        }) {
            return Err(RestoreError::Integrity(format!(
                "{}-{} is both classic and diskless in the restore inputs",
                partition.topic, partition.partition
            )));
        }
        if args.to_offset.iter().any(|bound| {
            bound.partition.topic == partition.topic
                && bound.partition.partition == partition.partition
                && bound.last_offset.0 < partition.delete_floor
        }) {
            return Err(RestoreError::InvalidArgument(format!(
                "--to-offset for {}-{} precedes diskless delete floor {}",
                partition.topic, partition.partition, partition.delete_floor
            )));
        }
        inventory.partitions.push(PartitionInventory {
            partition: TopicIdPartition::new(
                partition.topic_id,
                partition.topic.clone(),
                partition.partition,
            ),
            segments: Vec::new(),
        });
    }
    PartitionInventory::sort_by_topic_and_partition(&mut inventory.partitions);
    for requested in offset_partitions(args) {
        if !inventory.holds(&requested.topic, requested.partition) {
            return Err(RestoreError::UnknownPartition {
                topic: requested.topic.clone(),
                partition: requested.partition,
            });
        }
    }
    Ok(())
}

/// The offset a restored diskless partition's next write takes: the end the
/// restore materializes it to.
///
/// `KRaft` assigns a diskless partition's offsets, so the restored cluster's
/// metadata must start the partition there. Without it the first write is
/// assigned offset 0 and the materialized log refuses it (#1524).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisklessNextOffset {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) next_offset: i64,
}

/// The end every selected partition of `capture` will be materialized to,
/// computed before anything is written so the target can be formatted with
/// it.
///
/// A partition that no bound or exclusion touches keeps every captured
/// batch, so it ends at the capture's recovery cutoff and no object is
/// fetched for it. Any other partition is walked exactly as
/// [`materialize`] walks it, without writing, because a bound or an
/// exclusion decides its end batch by batch. [`materialize`] then requires
/// each partition to end where this said.
pub async fn next_offsets(
    store: &ArchiveStore,
    predicates: &Predicates,
    capture: &DisklessWalCapture,
    args: &RestoreArgs,
    authenticated: Option<&BTreeMap<String, ObjectEntry>>,
) -> Result<Vec<DisklessNextOffset>, RestoreError> {
    capture.validate().map_err(RestoreError::Integrity)?;
    let mut cached_object: Option<CachedObject> = None;
    let mut out = Vec::new();
    for partition in capture
        .partitions
        .iter()
        .filter(|partition| args.selects_topic(&partition.topic))
    {
        let partition_ref = PartitionRef {
            topic: partition.topic.clone(),
            partition: partition.partition,
        };
        let next_offset = if predicates.keeps_everything_in(&partition_ref) {
            partition.recovery_cutoff
        } else {
            walk_partition(
                store,
                &mut cached_object,
                predicates,
                partition,
                authenticated,
                None,
            )
            .await?
            .materialized_end
        };
        out.push(DisklessNextOffset {
            topic: partition.topic.clone(),
            partition: partition.partition,
            next_offset,
        });
    }
    Ok(out)
}

pub async fn materialize(
    store: &ArchiveStore,
    args: &RestoreArgs,
    predicates: &Predicates,
    capture: &DisklessWalCapture,
    authenticated: Option<&BTreeMap<String, ObjectEntry>>,
) -> Result<DisklessRestoreReport, RestoreError> {
    capture.validate().map_err(RestoreError::Integrity)?;
    let mut cached_object: Option<CachedObject> = None;
    let mut reports = Vec::new();
    for partition in capture
        .partitions
        .iter()
        .filter(|partition| args.selects_topic(&partition.topic))
    {
        let mut log = open_partition_log(args, &partition.topic, partition.partition)?;
        let Walked {
            objects,
            batches,
            records,
            records_dropped,
            materialized_end,
        } = walk_partition(
            store,
            &mut cached_object,
            predicates,
            partition,
            authenticated,
            log.as_mut(),
        )
        .await?;
        if let Some(log) = log.as_mut() {
            if log.log_end_offset() == Offset(0) && materialized_end != 0 {
                log.reset_to(Offset(materialized_end))?;
            }
            if log.log_end_offset().0 != materialized_end {
                return Err(RestoreError::Integrity(format!(
                    "{}-{} materialized through {}, expected {} after restore bounds",
                    partition.topic,
                    partition.partition,
                    log.log_end_offset(),
                    materialized_end
                )));
            }
            log.set_log_start_offset(Offset(partition.delete_floor))?;
            log.sync()?;
        }
        reports.push(DisklessPartitionReport {
            topic: partition.topic.clone(),
            partition: partition.partition,
            topic_id: partition.topic_id,
            delete_floor: partition.delete_floor,
            recovery_cutoff: materialized_end,
            objects,
            batches,
            records,
            records_dropped,
        });
    }
    Ok(DisklessRestoreReport {
        captured_at_ms: capture.captured_at_ms,
        source_cutoffs: capture.source_cutoffs.clone(),
        partitions: reports,
        limitations: limitations(),
    })
}

/// Require every materialized partition to end at the next offset the target
/// was formatted with, so its metadata never starts it anywhere else.
pub fn check_next_offsets(
    report: &DisklessRestoreReport,
    planned: &[DisklessNextOffset],
) -> Result<(), RestoreError> {
    let materialized: Vec<DisklessNextOffset> = report
        .partitions
        .iter()
        .map(|partition| DisklessNextOffset {
            topic: partition.topic.clone(),
            partition: partition.partition,
            next_offset: partition.recovery_cutoff,
        })
        .collect();
    if materialized == planned {
        Ok(())
    } else {
        Err(RestoreError::Integrity(format!(
            "diskless partitions materialized to {materialized:?}, but the target was formatted \
             with next offsets {planned:?}"
        )))
    }
}

/// What walking one partition's captured ranges found.
struct Walked {
    objects: u64,
    batches: u64,
    records: u64,
    records_dropped: u64,
    /// The exclusive end of the batches the restore keeps.
    materialized_end: i64,
}

/// Validate every captured range of `partition` against its WAL object, and
/// append the batches the restore keeps to `log` when one is given.
async fn walk_partition(
    store: &ArchiveStore,
    cached_object: &mut Option<CachedObject>,
    predicates: &Predicates,
    partition: &DisklessPartitionCapture,
    authenticated: Option<&BTreeMap<String, ObjectEntry>>,
    mut log: Option<&mut Log>,
) -> Result<Walked, RestoreError> {
    let mut walk = PartitionWalk::new(partition);
    let partition_ref = PartitionRef {
        topic: partition.topic.clone(),
        partition: partition.partition,
    };
    for range in &partition.ranges {
        let object = fetch_object(store, cached_object, &range.object_key, authenticated).await?;
        walk.saw_object(&range.object_key);
        validate_footer_range(&object, range)?;
        let mut range_batches = RangeBatches::new(&object, range)?;
        while let Some(framed) = range_batches.next_batch()? {
            let base = Offset(framed.base_offset);
            if framed.last_offset >= partition.delete_floor
                && !predicates.batch_past_offset_bound(&partition_ref, base)
            {
                let mut batch_cursor = object
                    .get(framed.bytes.clone())
                    .ok_or_else(|| outside_object(range))?;
                let batch = RecordBatchBorrowed::decode_borrow_with_policy(
                    &mut batch_cursor,
                    <_>::default(),
                )?;
                if !batch_cursor.is_empty() {
                    return Err(RestoreError::Integrity(format!(
                        "{}: decoded diskless batch left trailing bytes",
                        range.object_key
                    )));
                }
                let records_in_batch = batch_record_count(framed.records_count);
                let (mut prepared, tally) = prepare_batch(
                    PrepareBatchSetup::builder()
                        .partition_ref(&partition_ref)
                        .predicates(predicates)
                        .batch(&batch)
                        .batch_bytes(object.slice(framed.bytes))
                        .records_in_batch(records_in_batch)
                        .build(),
                )?;
                let batch_end = base
                    .0
                    .checked_add(i64::from(prepared.last_offset_delta()))
                    .and_then(|last| last.checked_add(1))
                    .ok_or_else(|| wal_overflow("batch offset"))?;
                if let Some(log) = log.as_deref_mut() {
                    if log.log_end_offset() == Offset(0) && base != Offset(0) {
                        log.reset_to(base)?;
                    }
                    log.reconcile_next_offset(base);
                    prepared.append_to(log, base)?;
                }
                let (kept, dropped) = tally.records(records_in_batch);
                walk.record_batch(batch_end, kept, dropped);
            }
        }
        range_batches.finish()?;
        walk.finish_range(range)?;
    }
    walk.finish()
}

/// The running tally of a walk over one partition's captured ranges.
struct PartitionWalk<'a> {
    partition: &'a DisklessPartitionCapture,
    object_names: HashSet<&'a str>,
    batches: u64,
    records: u64,
    records_dropped: u64,
    /// The exclusive end of the ranges validated so far.
    observed_end: i64,
    /// The exclusive end of the batches kept so far.
    materialized_end: i64,
}

impl<'a> PartitionWalk<'a> {
    fn new(partition: &'a DisklessPartitionCapture) -> Self {
        Self {
            partition,
            object_names: HashSet::new(),
            batches: 0,
            records: 0,
            records_dropped: 0,
            observed_end: partition.delete_floor,
            materialized_end: partition.delete_floor,
        }
    }

    /// Count the WAL object named `key` among the objects the walk read.
    fn saw_object(&mut self, key: &'a str) { self.object_names.insert(key); }

    /// Count one kept batch that ends before `batch_end` and of whose records
    /// the bound kept `kept` and dropped `dropped`.
    fn record_batch(&mut self, batch_end: i64, kept: u64, dropped: u64) {
        self.materialized_end = batch_end;
        self.batches += 1;
        self.records = self.records.saturating_add(kept);
        self.records_dropped = self.records_dropped.saturating_add(dropped);
    }

    /// Advance the validated end past `range`, never below the delete floor.
    fn finish_range(&mut self, range: &CapturedWalRange) -> Result<(), RestoreError> {
        self.observed_end = range
            .entry
            .last_offset
            .checked_add(1)
            .ok_or_else(|| wal_overflow("batch offset"))?
            .max(self.partition.delete_floor);
        Ok(())
    }

    /// Require the walk to have validated through the capture's recovery
    /// cutoff, and report what it found.
    fn finish(self) -> Result<Walked, RestoreError> {
        let partition = self.partition;
        if self.observed_end != partition.recovery_cutoff {
            return Err(RestoreError::Integrity(format!(
                "{}-{} validated through {}, capture declares cutoff {}",
                partition.topic, partition.partition, self.observed_end, partition.recovery_cutoff
            )));
        }
        Ok(Walked {
            objects: self.object_names.len() as u64,
            batches: self.batches,
            records: self.records,
            records_dropped: self.records_dropped,
            materialized_end: self.materialized_end,
        })
    }
}

/// One batch of a captured range whose framing and offset contiguity
/// [`RangeBatches::next_batch`] checked.
struct FramedBatch {
    /// The batch's bytes inside its WAL object.
    bytes: Range<usize>,
    base_offset: i64,
    /// The batch's last offset: its base offset plus its last offset delta.
    last_offset: i64,
    records_count: i32,
}

/// The batches of one captured range, in order, each checked for framing and
/// for offset contiguity with the batch before it.
///
/// A batch is checked only when [`Self::next_batch`] reaches it, so a walk
/// that fails on one batch never checks the batches after it.
/// [`Self::finish`] then checks that the batches span exactly the offsets the
/// capture declares for the range.
struct RangeBatches<'a> {
    object: &'a Bytes,
    range: &'a CapturedWalRange,
    cursor: usize,
    end: usize,
    first: Option<i64>,
    last: Option<i64>,
}

impl<'a> RangeBatches<'a> {
    fn new(object: &'a Bytes, range: &'a CapturedWalRange) -> Result<Self, RestoreError> {
        let Range { start, end } = captured_byte_range(range, object.len())?;
        Ok(Self {
            object,
            range,
            cursor: start,
            end,
            first: None,
            last: None,
        })
    }

    /// The next batch of the range, or `None` once the range is exhausted.
    fn next_batch(&mut self) -> Result<Option<FramedBatch>, RestoreError> {
        if self.cursor >= self.end {
            return Ok(None);
        }
        let range = self.range;
        let validated = validate_one_v2_batch(
            self.object
                .get(self.cursor..self.end)
                .ok_or_else(|| outside_object(range))?,
        )?;
        let header = validated.header;
        let batch_end = self
            .cursor
            .checked_add(validated.total_len)
            .ok_or_else(|| wal_overflow("byte range"))?;
        if validated.total_len == 0 || batch_end > self.end {
            return Err(RestoreError::Integrity(format!(
                "{}: captured range disagrees with batch framing",
                range.object_key
            )));
        }
        let batch_last = header
            .base_offset
            .get()
            .checked_add(i64::from(header.last_offset_delta.get()))
            .ok_or_else(|| wal_overflow("batch offset"))?;
        if self
            .last
            .is_some_and(|previous| previous.checked_add(1) != Some(header.base_offset.get()))
        {
            return Err(RestoreError::Integrity(format!(
                "{}: non-contiguous batch offsets inside captured run",
                range.object_key
            )));
        }
        self.first.get_or_insert_with(|| header.base_offset.get());
        self.last = Some(batch_last);
        let bytes = self.cursor..batch_end;
        self.cursor = batch_end;
        Ok(Some(FramedBatch {
            bytes,
            base_offset: header.base_offset.get(),
            last_offset: batch_last,
            records_count: header.records_count.get(),
        }))
    }

    /// Require the batches walked to span exactly the range's declared
    /// offsets.
    fn finish(self) -> Result<(), RestoreError> {
        if self.first != Some(self.range.entry.first_offset)
            || self.last != Some(self.range.entry.last_offset)
        {
            return Err(RestoreError::Integrity(format!(
                "{}: captured run disagrees with batch boundaries",
                self.range.object_key
            )));
        }
        Ok(())
    }
}

/// The byte range `range` captures inside its WAL object of `object_len` bytes.
fn captured_byte_range(
    range: &CapturedWalRange,
    object_len: usize,
) -> Result<Range<usize>, RestoreError> {
    let start = usize::try_from(range.entry.byte_start).map_err(|_| wal_overflow("byte start"))?;
    let len = usize::try_from(range.entry.byte_len).map_err(|_| wal_overflow("byte length"))?;
    let end = start
        .checked_add(len)
        .filter(|end| *end <= object_len)
        .ok_or_else(|| outside_object(range))?;
    Ok(start..end)
}

/// The error for a captured byte range that reaches past its WAL object.
fn outside_object(range: &CapturedWalRange) -> RestoreError {
    RestoreError::Integrity(format!(
        "{}: captured byte range is outside object",
        range.object_key
    ))
}

/// The error for a diskless WAL `what` whose arithmetic overflows.
fn wal_overflow(what: &str) -> RestoreError {
    RestoreError::Integrity(format!("diskless WAL {what} overflow"))
}

fn validate_footer_range(object: &Bytes, range: &CapturedWalRange) -> Result<(), RestoreError> {
    let manifests = parse_wal_object(object)
        .map_err(|reason| RestoreError::Integrity(format!("{}: {reason}", range.object_key)))?;
    if manifests.iter().any(|run| {
        run.topic_id == range.entry.topic_id
            && run.partition == range.entry.partition
            && run.first_offset <= range.entry.first_offset
            && run.last_offset >= range.entry.last_offset
            && run.byte_start <= range.entry.byte_start
            && run
                .byte_start
                .checked_add(u64::from(run.byte_len))
                .zip(
                    range
                        .entry
                        .byte_start
                        .checked_add(u64::from(range.entry.byte_len)),
                )
                .is_some_and(|(run_end, range_end)| range_end <= run_end)
    }) {
        Ok(())
    } else {
        Err(RestoreError::Integrity(format!(
            "{}: capture range is not contained in a CKWL footer run",
            range.object_key
        )))
    }
}

fn limitations() -> Vec<String> {
    vec![
        "diskless records at or after each recovery cutoff were not archived and are unavailable"
            .into(),
        "in-flight transaction state is not restored".into(),
    ]
}

/// The most recently fetched WAL object, kept so consecutive ranges in one
/// object cost a single fetch.
struct CachedObject {
    key: String,
    bytes: Bytes,
}

async fn fetch_object(
    store: &ArchiveStore,
    cached: &mut Option<CachedObject>,
    key: &str,
    authenticated: Option<&BTreeMap<String, ObjectEntry>>,
) -> Result<Bytes, RestoreError> {
    if let Some(object) = cached.as_ref().filter(|object| object.key == key) {
        return Ok(object.bytes.clone());
    }
    *cached = None;
    let bytes = fetch_capped(
        store.ops(),
        &ObjectPath::from(key),
        MAX_WAL_OBJECT_BYTES,
        authenticated,
    )
    .await?;
    *cached = Some(CachedObject {
        key: key.to_owned(),
        bytes: bytes.clone(),
    });
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use {
        super::{DisklessNextOffset, Range, captured_byte_range, check_next_offsets, load},
        crate::{
            RestoreError,
            report::{DisklessPartitionReport, DisklessRestoreReport},
        },
        assert2::check,
        krabka_remote_storage::diskless::{WalIndexEntry, capture::CapturedWalRange},
        tokio::fs,
        uuid::Uuid,
    };

    /// A captured range resolves to its byte span only when it lies inside its
    /// WAL object; otherwise the error names the object key.
    #[yare::parameterized(
        inside = { 4, 6, Ok(4..10) },
        to_the_last_byte = { 0, 16, Ok(0..16) },
        past_the_end = { 10, 7, Err("wal/0001: captured byte range is outside object".to_owned()) },
        start_plus_length_overflows = {
            u64::MAX,
            1,
            Err("wal/0001: captured byte range is outside object".to_owned()),
        },
    )]
    fn a_captured_range_must_lie_inside_its_object(
        byte_start: u64,
        byte_len: u32,
        expected: Result<Range<usize>, String>,
    ) {
        let range = CapturedWalRange {
            object_key: "wal/0001".to_owned(),
            entry: WalIndexEntry {
                topic_id: Uuid::from_u128(1),
                partition: 0,
                first_offset: 0,
                last_offset: 0,
                byte_start,
                byte_len,
                max_timestamp_ms: 0,
            },
        };

        let resolved = captured_byte_range(&range, 16).map_err(|error| match error {
            RestoreError::Integrity(message) => message,
            other => other.to_string(),
        });

        check!(resolved == expected);
    }

    /// A restore whose materialized end differs from the next offset the
    /// target was formatted with fails, because the restored metadata would
    /// assign the partition's next write an offset its log does not end at.
    #[yare::parameterized(
        the_planned_end = { 712, true },
        short_of_it = { 700, false },
        past_it = { 713, false },
    )]
    fn a_partition_must_end_at_the_next_offset_its_metadata_starts_it_at(
        materialized_end: i64,
        accepted: bool,
    ) {
        let report = DisklessRestoreReport {
            captured_at_ms: 0,
            source_cutoffs: Vec::new(),
            partitions: vec![DisklessPartitionReport {
                topic: "orders".to_owned(),
                partition: 0,
                topic_id: Uuid::from_u128(1524),
                delete_floor: 0,
                recovery_cutoff: materialized_end,
                objects: 1,
                batches: 1,
                records: 1,
                records_dropped: 0,
            }],
            limitations: Vec::new(),
        };
        let planned = [DisklessNextOffset {
            topic: "orders".to_owned(),
            partition: 0,
            next_offset: 712,
        }];

        let result = check_next_offsets(&report, &planned);

        check!(result.is_ok() == accepted, "{result:?}");
        if let Err(error) = result {
            check!(matches!(error, RestoreError::Integrity(_)), "{error}");
        }
    }

    #[yare::parameterized(
        pre_1_0 = {
            "pre-1.0 capture",
            r#"{"captured_at_ms":1,"source_cutoffs":[],"partitions":[]}"#,
            "diskless WAL capture has no format_version: it predates krabka 1.0, which this build does not read",
        },
        future = {
            "future version",
            r#"{"format_version":2,"captured_at_ms":1,"source_cutoffs":[],"partitions":[]}"#,
            "unsupported diskless WAL capture format version 2: this build reads version 1",
        },
    )]
    #[test_macro(tokio::test)]
    async fn a_capture_with_a_missing_or_unknown_version_is_refused(
        name: &str,
        json: &str,
        expected: &str,
    ) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("diskless-wal-index.json");
        fs::write(&path, json).await.expect("write the capture");

        let error = load(Some(&path)).await.expect_err(name);

        assert2::assert!(
            let RestoreError::Integrity(message) = &error,
            "case {name}: {error}"
        );
        check!(message == expected, "case {name}");
    }
}
