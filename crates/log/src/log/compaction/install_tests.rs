//! What [`Log::finish_compaction`] does under the log: it opens each output
//! from what the build derived while writing it, rather than scanning the
//! output again, and it syncs the directory a fixed number of times however
//! many segments the pass retires.

use {
    super::Log,
    crate::{
        config::LogConfig,
        error::LogError,
        io::{FileIo, IoTarget, LogIo},
        log::test_support::{
            TempLog, append_keyed_samples, cleaned_files, compact_test_log, compacting_segments,
            compaction_ctx, configured_test_log, set_segment_size,
        },
        name,
        segment::Segment,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_ids::Offset,
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    krabka_units::prelude::{ByteSize, bytes, gibibytes, mebibytes},
    std::{
        fs::File,
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tempfile::tempdir,
};

/// The index interval of [`mixed_log`]: small, so every output has many
/// index points.
const INDEX_INTERVAL: ByteSize = bytes(100);

/// Batch `i` of [`mixed_log`]: three records of forty rotating keys, with
/// timestamps that rise and fall from batch to batch, every third batch gzip,
/// and every seventh record a tombstone.
fn mixed_batch(i: i64) -> RecordBatch {
    let base_timestamp = 1_000_000 + (i * 7_919) % 50 * 1_000;
    let records = (0..3)
        .map(|delta: i32| {
            let n = i * 3 + i64::from(delta);
            Record {
                offset_delta: delta,
                timestamp_delta: i64::from(delta) * 10,
                key: Some(Bytes::from(format!("k{}", n % 40))),
                value: (n % 7 != 0).then(|| Bytes::from(format!("value-{n}"))),
                ..Record::default()
            }
        })
        .collect();
    let attributes = if i % 3 == 0 {
        Attributes::default().with_compression(CompressionType::Gzip)
    } else {
        Attributes::default()
    };
    RecordBatch {
        last_offset_delta: 2,
        base_timestamp,
        max_timestamp: base_timestamp + 20,
        attributes,
        records,
        ..RecordBatch::default()
    }
}

/// Eighty [`mixed_batch`]es, several to a segment, to be compacted in groups
/// of `segment_bytes` on disk.
fn mixed_appends(segment_bytes: ByteSize) -> TempLog {
    let mut fixture = configured_test_log(LogConfig {
        index_interval: INDEX_INTERVAL,
        segment_size: bytes(600),
        ..compacting_segments()
    });
    for i in 0..80 {
        fixture.log.append(&mut mixed_batch(i)).unwrap();
    }
    set_segment_size(&mut fixture.log, segment_bytes);
    fixture
}

/// [`mixed_appends`], compacted.
fn mixed_log(segment_bytes: ByteSize) -> TempLog {
    let mut fixture = mixed_appends(segment_bytes);
    fixture.log.compact(&compaction_ctx()).unwrap();
    fixture
}

fn read(path: &Path) -> Vec<u8> { std::fs::read(path).unwrap() }

/// Every batch of the log, from its start.
fn batches(log: &Log) -> Vec<RecordBatch> {
    log.read(log.log_start_offset(), mebibytes(1))
        .unwrap()
        .batches
}

/// Every output of a pass opens with the frontiers and the sparse-index files
/// a recovery scan of its `.log` derives, which is how the install opened it
/// before: whether the pass writes one output or one per consumed segment.
#[yare::parameterized(
    one_output = { gibibytes(1) },
    an_output_per_segment = { bytes(1) },
)]
fn each_output_opens_as_a_recovery_scan_of_it_would(segment_bytes: ByteSize) {
    let TempLog { dir, log } = mixed_log(segment_bytes);
    let outputs = &log.segments[..log.clean_prefix_segments];
    check!(!outputs.is_empty());
    let mut index_bytes = 0;

    for output in outputs {
        let base = output.base_offset().0;
        let scratch = tempdir().unwrap();
        std::fs::copy(
            name::log_path(dir.path(), base),
            name::log_path(scratch.path(), base),
        )
        .unwrap();
        let mut scanned = Segment::open_active_with_index_interval(
            scratch.path(),
            Offset(base),
            true,
            INDEX_INTERVAL,
        )
        .unwrap();
        scanned.seal().unwrap();

        check!(output.last_offset() == scanned.last_offset(), "{base}");
        check!(output.max_timestamp() == scanned.max_timestamp(), "{base}");
        let index = read(&name::index_path(dir.path(), base));
        check!(
            index == read(&name::index_path(scratch.path(), base)),
            "{base}"
        );
        check!(
            read(&name::timeindex_path(dir.path(), base))
                == read(&name::timeindex_path(scratch.path(), base)),
            "{base}"
        );
        index_bytes += index.len();
    }
    // The comparison covers index points, not only empty files.
    check!(index_bytes > 0);
}

/// A [`LogIo`] that counts directory syncs and performs none.
#[derive(Debug, Default)]
struct CountedDirSyncs(AtomicUsize);

impl LogIo for CountedDirSyncs {
    fn sync_dir(&self, _dir: &Path) -> std::io::Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The directory syncs one pass makes over `segments` sealed segments,
/// merged into one output.
fn directory_syncs_of_a_pass(segments: i64) -> usize {
    let TempLog { dir: _dir, mut log } = compact_test_log();
    append_keyed_samples(&mut log, segments + 1, |i| format!("k{}", i % 2));
    set_segment_size(&mut log, gibibytes(1));
    let io = Arc::new(CountedDirSyncs::default());
    log.test_set_io(io.clone());
    log.compact(&compaction_ctx()).unwrap();
    io.0.load(Ordering::SeqCst)
}

/// Retiring the consumed segments costs no directory sync each: the swap
/// syncs the directory once after it retires them all. A pass over forty
/// segments syncs it as often as a pass over four.
#[test]
fn a_pass_syncs_the_directory_as_often_however_many_segments_it_retires() {
    check!(directory_syncs_of_a_pass(4) == directory_syncs_of_a_pass(40));
}

/// The install opens an output only when its file holds exactly what the
/// build wrote, and refuses one that does not before it changes anything: the
/// log keeps its segments, and the outputs are gone.
#[test]
fn an_output_that_is_not_what_the_build_wrote_is_refused() {
    let TempLog { dir, mut log } = compact_test_log();
    append_keyed_samples(&mut log, 6, |i| format!("k{}", i % 2));
    let before = batches(&log);
    let job = log.prepare_compaction(&compaction_ctx()).unwrap().unwrap();
    let built = job.build();
    let output = built.rewrites.as_ref().unwrap()[0].rewrite.log_swap.clone();
    std::fs::OpenOptions::new()
        .append(true)
        .open(output)
        .and_then(|mut file| std::io::Write::write_all(&mut file, &[0; 16]))
        .unwrap();

    let refused = log.finish_compaction(built);

    check!(matches!(refused, Err(LogError::Corrupt(_))));
    check!(batches(&log) == before);
    check!(cleaned_files(dir.path()).is_empty());
}

/// A [`LogIo`] whose disk is full for sparse-index writes.
#[derive(Debug)]
struct FullIndexDisk;

impl LogIo for FullIndexDisk {
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        match target {
            IoTarget::OffsetIndex | IoTarget::TimeIndex => {
                Err(std::io::Error::from(std::io::ErrorKind::StorageFull))
            }
            _ => FileIo.write_at(target, file, buf),
        }
    }
}

/// The build writes each output's sparse indexes through the log's I/O, so a
/// failed index write fails the pass, and the log keeps what it held.
#[test]
fn the_build_writes_the_indexes_through_the_logs_io() {
    let TempLog { dir, mut log } = mixed_appends(gibibytes(1));
    let before = batches(&log);
    log.test_set_io(Arc::new(FullIndexDisk));

    let failed = log.compact(&compaction_ctx());

    assert!(let Err(LogError::Io(error)) = failed);
    check!(error.kind() == std::io::ErrorKind::StorageFull);
    check!(batches(&log) == before);
    check!(cleaned_files(dir.path()).is_empty());
}
