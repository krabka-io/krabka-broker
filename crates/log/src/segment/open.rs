//! Constructors for a segment and the tail recovery that an active segment
//! needs when it is reopened.
//!
//! Fresh segments are created, sealed segments are validated for reading,
//! and active segments recover their tail. Log recovery also uses a private
//! no-scan loader before it supplies the sealed segment bounds.

use {
    super::{Segment, io::seek_to_log_size},
    crate::{
        config::DEFAULT_INDEX_INTERVAL,
        error::LogError,
        index::{OffsetIndex, TimeIndex},
        io::{FileIo, LogIo},
        name,
    },
    krabka_ids::Offset,
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSize, ByteSizeExt},
    std::{
        fs::{File, OpenOptions},
        path::Path,
        sync::Arc,
    },
    tracing::instrument,
};

/// One sparse-index point the tail scan found, as Kafka's `LogSegment.recover`
/// writes it: the offset index gets the indexed batch's last offset and its
/// byte position, and the time index gets the running maximum timestamp with
/// the last offset of the batch that set it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexPoint {
    /// Last offset of the indexed batch, relative to the segment base.
    last_offset: u32,
    /// Byte position where the indexed batch starts.
    position: u32,
    /// Maximum timestamp of every batch up to and including the indexed one.
    max_timestamp: i64,
    /// Last offset of the batch that set `max_timestamp`, relative to the
    /// segment base.
    max_timestamp_offset: u32,
}

/// What a walk over a segment's batches derives: the end of their valid
/// prefix, the segment's frontiers, and the sparse-index points Kafka's
/// `LogSegment.recover` writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailScan {
    valid_end: u64,
    last_offset: Offset,
    max_timestamp: i64,
    max_timestamp_offset: Offset,
    index_points: Vec<IndexPoint>,
}

/// Builds a [`TailScan`] one batch at a time, in file order.
///
/// The tail scan feeds it the batches it decodes from a file, and a
/// compaction rewrite the batches it writes, so the two derive the same
/// frontiers and index points by the same rule.
pub(crate) struct FrontierScan {
    base_offset: Offset,
    file_end: u64,
    index_interval: ByteSize,
    next_offset: i64,
    scan: TailScan,
}

impl FrontierScan {
    /// A walk over a segment at `base_offset` whose file holds `file_end`
    /// bytes, indexing every `index_interval` bytes.
    pub(crate) fn new(
        base_offset: Offset,
        file_end: u64,
        index_interval: ByteSize,
    ) -> Result<Self, LogError> {
        let last_offset = base_offset
            .0
            .checked_sub(1)
            .map(Offset)
            .ok_or_else(|| LogError::Corrupt("recovery offset underflow".into()))?;
        Ok(Self {
            base_offset,
            file_end,
            index_interval,
            next_offset: base_offset.0,
            scan: TailScan {
                valid_end: 0,
                last_offset,
                max_timestamp: i64::MIN,
                max_timestamp_offset: last_offset,
                index_points: Vec::new(),
            },
        })
    }

    /// Take `batch`, `encoded_len` bytes long, which starts where the valid
    /// prefix ends. Returns `false`, and takes nothing, when the batch does not
    /// extend the valid prefix: it runs past the file, or its offsets do not
    /// follow the prefix's.
    pub(crate) fn push(&mut self, batch: &RecordBatch, encoded_len: u64) -> Result<bool, LogError> {
        let batch_position = self.scan.valid_end;
        let Some(step) = krabka_verified::local_recovery_batch_step(
            batch_position,
            self.file_end,
            self.next_offset,
            batch.base_offset,
            batch.last_offset_delta,
            encoded_len,
        ) else {
            return Ok(false);
        };
        let scan = &mut self.scan;
        scan.valid_end = step.valid_end;
        scan.last_offset = Offset(step.last_offset);
        self.next_offset = step.next_offset;
        if batch.max_timestamp > scan.max_timestamp {
            scan.max_timestamp = batch.max_timestamp;
            scan.max_timestamp_offset = scan.last_offset;
        }
        // Kafka's `LogSegment.recover`: `validBytes - lastIndexEntry >
        // indexIntervalBytes`, the last entry starting at zero.
        let last_indexed = scan
            .index_points
            .last()
            .map_or(0, |previous| u64::from(previous.position));
        if batch_position.saturating_sub(last_indexed) > self.index_interval.bytes_u64() {
            let base_offset = self.base_offset;
            let relative = |offset: Offset| {
                krabka_verified::truncation_relative_offset(base_offset.0, offset.0).ok_or_else(
                    || LogError::Corrupt("recovered batch offset exceeds index range".into()),
                )
            };
            let position = u32::try_from(batch_position).map_err(|_| {
                LogError::Corrupt("recovered batch position exceeds index range".into())
            })?;
            scan.index_points.push(IndexPoint {
                last_offset: relative(scan.last_offset)?,
                position,
                max_timestamp: scan.max_timestamp,
                max_timestamp_offset: relative(scan.max_timestamp_offset)?,
            });
        }
        Ok(true)
    }

    /// The walk's result.
    pub(crate) fn finish(self) -> TailScan { self.scan }
}

impl TailScan {
    /// The end of the valid prefix: the length of a file that holds exactly
    /// what was scanned.
    pub(crate) fn valid_end(&self) -> u64 { self.valid_end }

    /// Check that the scanned prefix fits a segment: its end in a `u32`
    /// position and its offsets in the 32-bit relative index.
    fn check_index_range(&self, base_offset: Offset) -> Result<(), LogError> {
        u32::try_from(self.valid_end)
            .map_err(|_| LogError::Corrupt("recovered segment position exceeds u32".into()))?;
        krabka_verified::local_recovery_index_frontier(base_offset.0, self.last_offset.0)
            .ok_or_else(|| {
                LogError::Corrupt("recovered segment offset exceeds index range".into())
            })?;
        Ok(())
    }

    /// Append the scan's index points to `offset_index` and `time_index`,
    /// then the running maximum timestamp, as a segment that closed normally
    /// always had its newest timestamp in the index (Kafka's
    /// `LogSegment.recover` ends the same way).
    fn write_index_points(
        &self,
        base_offset: Offset,
        offset_index: &mut OffsetIndex,
        time_index: &mut TimeIndex,
    ) -> Result<(), LogError> {
        for point in &self.index_points {
            offset_index.append(point.last_offset, point.position)?;
            time_index.maybe_append(point.max_timestamp, point.max_timestamp_offset)?;
        }
        if self.max_timestamp >= 0 {
            let relative = krabka_verified::truncation_relative_offset(
                base_offset.0,
                self.max_timestamp_offset.0,
            )
            .ok_or_else(|| LogError::BadSegmentName("offset overflow in segment".into()))?;
            time_index.maybe_append(self.max_timestamp, relative)?;
        }
        Ok(())
    }

    /// Write the scan's sparse indexes for a segment at `base_offset` into
    /// the index files at `index_path` and `timeindex_path`, which a
    /// compaction rewrite creates empty beside its output, through `io`.
    ///
    /// The index points are then dropped: the files hold them, and a
    /// compaction pass keeps every output's scan until it installs them all.
    pub(crate) fn write_indexes(
        &mut self,
        io: &Arc<dyn LogIo>,
        base_offset: Offset,
        index_path: &Path,
        timeindex_path: &Path,
    ) -> Result<(), LogError> {
        self.check_index_range(base_offset)?;
        let mut offset_index = OffsetIndex::open(index_path)?;
        offset_index.set_io(Arc::clone(io));
        let mut time_index = TimeIndex::open(timeindex_path)?;
        time_index.set_io(Arc::clone(io));
        self.write_index_points(base_offset, &mut offset_index, &mut time_index)?;
        self.index_points = Vec::new();
        Ok(())
    }
}

impl Segment {
    /// Attach the sparse indexes and initial frontiers to an already-open log file.
    fn from_log_file(
        dir: &Path,
        base_offset: Offset,
        log_file: File,
        log_size: u64,
    ) -> Result<Self, LogError> {
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
            io: Arc::new(FileIo),
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            max_timestamp_offset: base_offset - 1,
            first_timestamp: None,
            last_offset: base_offset - 1,
            reserved_end: 0,
            reserve_requested: 0,
            direct: None,
            direct_asked: false,
            wrote_direct: false,
        })
    }

    /// Create a fresh active segment at the given base offset. This fails if
    /// the `.log` file already exists.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn create(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let path = name::log_path(dir, base_offset.0);
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        let log_file = opts.open(&path)?;
        Self::from_log_file(dir, base_offset, log_file, 0)
    }

    /// Open as the active segment.
    ///
    /// When `validate` is true, this method scans from byte zero to EOF. It
    /// truncates a partial or invalid trailing batch, rebuilds both sparse
    /// indexes, and updates `last_offset` and `max_timestamp` from the same
    /// maximal valid prefix.
    #[instrument(
        level = "info",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0, validate),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open_active(dir: &Path, base_offset: Offset, validate: bool) -> Result<Self, LogError> {
        Self::open_active_with_index_interval(dir, base_offset, validate, DEFAULT_INDEX_INTERVAL)
    }

    pub(crate) fn open_active_with_index_interval(
        dir: &Path,
        base_offset: Offset,
        validate: bool,
        index_interval: ByteSize,
    ) -> Result<Self, LogError> {
        let mut seg = Self::open_unscanned(dir, base_offset)?;
        if validate {
            seg.recover_active_tail(index_interval)?;
        }
        Ok(seg)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            base_offset = self.base_offset.0,
            log_size = self.log_size,
            recovered_last_offset = tracing::field::Empty,
        ),
        err,
    )]
    fn recover_active_tail(&mut self, index_interval: ByteSize) -> Result<(), LogError> {
        let recovered = self.scan_valid_tail(index_interval)?;
        recovered.check_index_range(self.base_offset)?;
        if recovered.valid_end < self.log_size {
            self.log_file.set_len(recovered.valid_end)?;
            self.renew_reservation();
        }
        self.log_size = recovered.valid_end;
        seek_to_log_size(&self.log_file, self.log_size)?;
        self.offset_index.truncate_by_position(0)?;
        self.time_index.truncate_by_relative_offset(0)?;
        recovered.write_index_points(
            self.base_offset,
            &mut self.offset_index,
            &mut self.time_index,
        )?;
        self.last_offset = recovered.last_offset;
        self.max_timestamp = recovered.max_timestamp;
        self.max_timestamp_offset = recovered.max_timestamp_offset;
        tracing::Span::current().record("recovered_last_offset", self.last_offset.0);
        Ok(())
    }

    fn scan_valid_tail(&self, index_interval: ByteSize) -> Result<TailScan, LogError> {
        let mut buf = Vec::new();
        let to_read = usize::try_from(self.log_size).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

        let mut scan = FrontierScan::new(self.base_offset, self.log_size, index_interval)?;
        let mut cur: &[u8] = &buf;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let encoded_len = u64::try_from(before - cur.len())
                .map_err(|_| LogError::Corrupt("decoded batch length exceeds u64".into()))?;
            if !scan.push(&batch, encoded_len)? {
                break;
            }
        }
        Ok(scan.finish())
    }

    /// Open an existing segment for reading and refuse further appends.
    ///
    /// Scans the batches to restore the last offset and maximum timestamp. A corrupt or incomplete batch returns an error without truncating the file. Use [`Segment::open_active`] to recover a writable segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment or its indexes cannot be opened, its batches cannot be read, or its offset chain or batch bytes are corrupt.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    pub fn open(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let mut segment = Self::open_unscanned(dir, base_offset)?;
        let scanned = segment.scan_valid_tail(DEFAULT_INDEX_INTERVAL)?;
        if scanned.valid_end != segment.log_size {
            return Err(LogError::Corrupt(
                "sealed segment contains an invalid batch".into(),
            ));
        }
        segment.last_offset = scanned.last_offset;
        segment.max_timestamp = scanned.max_timestamp;
        segment.max_timestamp_offset = scanned.max_timestamp_offset;
        segment.sealed = true;
        Ok(segment)
    }

    /// Open the segment a compaction rewrite wrote at `base_offset`, from the
    /// [`TailScan`] the rewrite derived while it wrote it and the indexes it
    /// wrote with [`TailScan::write_indexes`], without decoding the file
    /// again. The file must hold exactly the bytes the scan covers.
    pub(crate) fn open_rewritten(
        dir: &Path,
        base_offset: Offset,
        scan: &TailScan,
    ) -> Result<Self, LogError> {
        let mut segment = Self::open_unscanned(dir, base_offset)?;
        if segment.log_size != scan.valid_end {
            return Err(LogError::Corrupt(format!(
                "rewritten segment {} holds {} bytes, the rewrite wrote {}",
                base_offset.0, segment.log_size, scan.valid_end
            )));
        }
        scan.check_index_range(base_offset)?;
        segment.last_offset = scan.last_offset;
        segment.max_timestamp = scan.max_timestamp;
        segment.max_timestamp_offset = scan.max_timestamp_offset;
        Ok(segment)
    }

    // Log recovery supplies sealed bounds from the following segment and
    // restores timestamps separately, without decoding every record body.
    pub(crate) fn open_unscanned(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let metadata = log_file.metadata()?;
        let log_size = metadata.len();
        seek_to_log_size(&log_file, log_size)?;
        let mut segment = Self::from_log_file(dir, base_offset, log_file, log_size)?;
        // A reservation outlives the process that made it, and nothing on
        // disk records it but the blocks themselves. Count them, so sealing
        // this segment later gives back what an earlier run reserved.
        segment.reserved_end = allocated_bytes(&metadata);
        Ok(segment)
    }
}

/// Bytes of disk the file behind `metadata` occupies, which is past its
/// length when blocks are reserved beyond its end.
#[cfg(unix)]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::blocks(metadata).saturating_mul(512)
}

/// The targets without block counts cannot reserve blocks either.
#[cfg(not(unix))]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    let _ = metadata;
    0
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::segment::test_support::{
            DENSE_INDEX, FIVE_OFFSET_TIMESTAMP_BATCHES, NO_LIMIT, ONE_RECORD_BATCH,
            SECOND_SINGLE_RECORD_BATCH, THIRD_SINGLE_RECORD_BATCH, sample_batch, seeded_segment,
        },
        krabka_units::prelude::kibibytes,
        std::io::Write,
        tempfile::tempdir,
    };

    #[test]
    fn append_after_open_active_writes_at_eof() {
        let dir = tempdir().unwrap();
        drop(
            seeded_segment()
                .dir(dir.path())
                .batches(&[ONE_RECORD_BATCH, SECOND_SINGLE_RECORD_BATCH])
                .call(),
        );

        let mut seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        let position = seg
            .append(
                &sample_batch()
                    .offset(THIRD_SINGLE_RECORD_BATCH.offset.0)
                    .records(THIRD_SINGLE_RECORD_BATCH.records.0)
                    .timestamp(THIRD_SINGLE_RECORD_BATCH.timestamp.0)
                    .call(),
                DENSE_INDEX,
            )
            .unwrap();

        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(position > 0);
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(read == crate::segment::test_support::three_single_record_batches());
    }

    /// Tail recovery must physically truncate a partial or garbage tail and
    /// rebuild both indexes from exactly the batches in the retained prefix.
    #[test]
    fn recover_active_tail_truncates_trailing_garbage() {
        let dir = tempdir().unwrap();
        let valid_size = {
            let mut seg = seeded_segment()
                .dir(dir.path())
                .batches(FIVE_OFFSET_TIMESTAMP_BATCHES)
                .call();
            seg.flush().unwrap();
            let valid_size = seg.log_size;
            let stale_position = u32::try_from(valid_size).unwrap();
            seg.offset_index.append(999, stale_position).unwrap();
            seg.time_index.append(i64::MAX, 999).unwrap();
            valid_size
        };

        // Append 16 bytes of garbage (an undecodable partial batch tail).
        let log_path = name::log_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xCD; 16]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen with validation: the tail scan must clip the garbage.
        let seg =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(seg.log_size == valid_size);
        assert2::assert!(std::fs::metadata(&log_path).unwrap().len() == valid_size);
        // The first batch takes no entry (Kafka's `LogSegment.recover` indexes
        // a batch once more than the interval lies between it and the last
        // entry, which starts at zero), so the second batch's is the one.
        assert2::assert!(seg.offset_index.entry_count() == 1);
        assert2::assert!(seg.time_index.entry_count() == 1);
        assert2::assert!(u64::from(seg.offset_index.lookup(999)) < valid_size);
        assert2::assert!(seg.time_index.last_entry().unwrap().relative_offset < 999);
        drop(seg);

        // Recovery is idempotent once the file and both indexes share the
        // proved frontiers.
        let retry =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(retry.last_offset() == Offset(4));
        assert2::assert!(retry.log_size == valid_size);
        assert2::assert!(std::fs::metadata(&log_path).unwrap().len() == valid_size);
        assert2::assert!(retry.offset_index.entry_count() == 1);
        assert2::assert!(retry.time_index.entry_count() == 1);
    }

    /// Recovery ends with Kafka's final time-index entry, `(maxTimestampSoFar,
    /// shallowOffsetOfMaxTimestampSoFar)`: the sparse index holds no point for
    /// these small batches, so without it the index would not know any of
    /// their timestamps.
    #[test]
    fn recover_active_tail_appends_the_final_time_index_entry() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            for (base, timestamp) in [(0, 100), (1, 300), (2, 200)] {
                seg.append(
                    &sample_batch().offset(base).timestamp(timestamp).call(),
                    kibibytes(4),
                )
                .unwrap();
            }
        }

        let seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();

        assert2::assert!(seg.time_index.entry_count() == 1);
        assert2::assert!(
            seg.time_index
                .last_entry()
                .map(|entry| (entry.timestamp, entry.relative_offset))
                == Some((300, 1))
        );
    }

    #[test]
    fn unvalidated_open_reports_last_offset_as_base_minus_one() {
        let dir = tempdir().unwrap();
        Segment::create(dir.path(), Offset(10)).unwrap();
        let seg = Segment::open_unscanned(dir.path(), Offset(10)).unwrap();
        assert2::assert!(seg.last_offset() == Offset(9));
    }

    /// Writing a scan's indexes keeps its frontiers and drops its index
    /// points, which the files then hold.
    #[test]
    fn writing_the_indexes_drops_the_points_and_keeps_the_frontiers() {
        let dir = tempdir().unwrap();
        let seg = seeded_segment()
            .dir(dir.path())
            .batches(FIVE_OFFSET_TIMESTAMP_BATCHES)
            .call();
        let mut scan = seg.scan_valid_tail(DENSE_INDEX).unwrap();
        let scanned = scan.clone();
        let out = tempdir().unwrap();
        let index = name::index_path(out.path(), 0);
        let timeindex = name::timeindex_path(out.path(), 0);
        let io: Arc<dyn LogIo> = Arc::new(FileIo);

        scan.write_indexes(&io, Offset(0), &index, &timeindex)
            .unwrap();

        assert2::check!(!scanned.index_points.is_empty());
        assert2::check!(
            scan == TailScan {
                index_points: Vec::new(),
                ..scanned
            }
        );
        assert2::check!(OffsetIndex::open(&index).unwrap().entry_count() > 0);
    }
}
