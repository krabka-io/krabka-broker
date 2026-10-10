//! What `preallocate` asks of the disk, and what it leaves on it.
//!
//! [`SegmentAllocation::Preallocate`] reserves a segment's blocks before its
//! first append and gives back the rest when it is sealed. A
//! reservation is invisible to a reader, so the cases here watch it two ways:
//! through a [`LogIo`] that records each request, which says exactly what the
//! log asked for, and on Linux through the blocks a real file holds, which
//! says the kernel did it.

use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};

use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, ByteSizeExt as _, mebibytes};

use super::{
    Log,
    test_support::{NO_LIMIT, append_samples, configured_test_log, sample_batch},
};
use crate::{
    config::{LogConfig, SegmentAllocation},
    io::LogIo,
    name,
};

/// One reservation or release a [`Recorder`] saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Request {
    Reserve { offset: u64, len: u64 },
    Release { len: u64 },
}

/// A disk that records every reservation and release, and refuses every
/// reservation when `refuse` is set, as a filesystem without `fallocate`
/// does.
#[derive(Debug, Default)]
struct Recorder {
    refuse: bool,
    requests: Mutex<Vec<Request>>,
}

impl LogIo for Recorder {
    fn reserve(&self, _file: &File, offset: u64, len: u64) -> std::io::Result<()> {
        self.requests
            .lock()
            .unwrap()
            .push(Request::Reserve { offset, len });
        if self.refuse {
            return Err(std::io::ErrorKind::Unsupported.into());
        }
        Ok(())
    }

    fn release(&self, _file: &File, len: u64) -> std::io::Result<()> {
        self.requests.lock().unwrap().push(Request::Release { len });
        Ok(())
    }
}

const SEGMENT: ByteSize = mebibytes(1);

fn preallocating() -> LogConfig {
    LogConfig {
        segment_size: SEGMENT,
        segment_allocation: SegmentAllocation::Preallocate,
        ..LogConfig::default()
    }
}

fn log_len(dir: &Path, base: i64) -> u64 {
    std::fs::metadata(name::log_path(dir, base)).unwrap().len()
}

/// A log opened without `preallocate` that turns it on afterwards, under
/// `io`, as the broker opens a partition with its own config and only then
/// applies the topic's.
fn switched_on(allocation: SegmentAllocation, io: &Arc<Recorder>) -> (tempfile::TempDir, Log) {
    let (dir, mut log) = configured_test_log(LogConfig::default());
    log.test_set_io(io.clone());
    log.set_config(LogConfig {
        segment_allocation: allocation,
        ..preallocating()
    });
    (dir, log)
}

/// Append two batches and roll, twice: into the segment at 0, which was
/// created before the config changed, and into the one at 6.
///
/// Returns the requests `io` saw and the lengths of the two segments.
fn roll_twice(allocation: SegmentAllocation, io: &Arc<Recorder>) -> (Vec<Request>, [u64; 2]) {
    let (dir, mut log) = switched_on(allocation, io);
    for _ in 0..2 {
        append_samples(&mut log, 2, 3);
        assert2::assert!(log.roll().unwrap());
    }
    // Whatever the disk said, the log holds what was written.
    let read = log.read(Offset(0), NO_LIMIT).unwrap();
    let batches: Vec<(i64, i32)> = read
        .batches
        .iter()
        .map(|batch| (batch.base_offset, batch.last_offset_delta))
        .collect();
    assert2::assert!(batches == [(0, 2), (3, 2), (6, 2), (9, 2)]);
    let requests = io.requests.lock().unwrap().clone();
    (requests, [log_len(dir.path(), 0), log_len(dir.path(), 6)])
}

/// A reservation of a whole segment from its start.
fn whole() -> Request {
    Request::Reserve {
        offset: 0,
        len: SEGMENT.bytes_u64(),
    }
}

/// A segment reserves its whole `segment.bytes` before its first append,
/// once, and gives back everything past the bytes it holds when it is
/// sealed, by truncating to the length it already has. The config change
/// reaches the segment that was already active, not only the next one.
#[test]
fn every_segment_reserves_before_its_first_append_and_releases_at_the_seal() {
    let io = Arc::new(Recorder::default());
    let (requests, [first, second]) = roll_twice(SegmentAllocation::Preallocate, &io);

    assert2::assert!(
        requests
            == [
                whole(),
                Request::Release { len: first },
                whole(),
                Request::Release { len: second },
            ]
    );
}

/// A filesystem that cannot reserve leaves a segment that allocates as it
/// grows: the appends and rolls go on, each segment asks once and not on
/// every append, and there is nothing to give back.
#[test]
fn a_refused_reservation_is_asked_once_and_leaves_a_working_log() {
    let io = Arc::new(Recorder {
        refuse: true,
        ..Recorder::default()
    });
    let (requests, _) = roll_twice(SegmentAllocation::Preallocate, &io);

    assert2::assert!(requests == [whole(), whole()]);
}

/// Kafka's default asks the disk for nothing.
#[test]
fn allocating_on_write_never_reserves_or_releases() {
    let io = Arc::new(Recorder::default());
    let (requests, _) = roll_twice(SegmentAllocation::OnWrite, &io);

    assert2::assert!(requests.is_empty());
}

/// A truncate frees the blocks past the new end, the reservation's with
/// them, as a follower's truncation to the leader does. The segment takes
/// the reservation again from where it now ends, and the appends after it
/// ask for nothing more.
#[test]
fn a_truncated_segment_takes_its_reservation_again() {
    let io = Arc::new(Recorder::default());
    let (dir, mut log) = switched_on(SegmentAllocation::Preallocate, &io);
    append_samples(&mut log, 2, 3);

    log.truncate_to(Offset(3)).unwrap();
    let kept = log_len(dir.path(), 0);
    append_samples(&mut log, 1, 3);

    let requests = io.requests.lock().unwrap().clone();
    assert2::assert!(
        requests
            == [
                whole(),
                Request::Reserve {
                    offset: kept,
                    len: SEGMENT.bytes_u64() - kept,
                },
            ]
    );
}

/// The kernel's side, on a real file: what a segment occupies on disk.
#[cfg(target_os = "linux")]
fn allocated(dir: &Path, base: i64) -> u64 {
    let metadata = std::fs::metadata(name::log_path(dir, base)).unwrap();
    std::os::unix::fs::MetadataExt::blocks(&metadata) * 512
}

/// On a real file the reservation takes the blocks and leaves the length
/// alone, so every reader still sees exactly the batches written, and the
/// seal hands the blocks back. A filesystem that cannot reserve fails the
/// first assertion; every one the broker runs on can.
#[cfg(target_os = "linux")]
#[test]
fn a_real_reservation_holds_blocks_past_the_end_until_the_seal() {
    let (dir, mut log) = configured_test_log(preallocating());

    log.append(&mut sample_batch(3)).unwrap();
    let written = log_len(dir.path(), 0);
    assert2::assert!(written == log.size().bytes_u64());
    assert2::assert!(allocated(dir.path(), 0) >= SEGMENT.bytes_u64());

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(log_len(dir.path(), 0) == written);
    assert2::assert!(allocated(dir.path(), 0) < SEGMENT.bytes_u64() / 2);

    log.append(&mut sample_batch(3)).unwrap();
    assert2::assert!(allocated(dir.path(), 3) >= SEGMENT.bytes_u64());
}

/// A reservation outlives the process that made it. A log reopened without
/// `preallocate` still finds the blocks an earlier run reserved, and the
/// seal gives them back rather than leaving them for as long as retention
/// keeps the segment.
#[cfg(target_os = "linux")]
#[test]
fn a_reopened_segment_gives_back_an_earlier_runs_reservation() {
    let (dir, mut log) = configured_test_log(preallocating());
    append_samples(&mut log, 1, 3);
    drop(log);

    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    assert2::assert!(log.log_end_offset() == Offset(3));
    assert2::assert!(allocated(dir.path(), 0) >= SEGMENT.bytes_u64());

    assert2::assert!(log.roll().unwrap());
    assert2::assert!(allocated(dir.path(), 0) < SEGMENT.bytes_u64() / 2);
}
