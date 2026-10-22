//! A compaction pass built without the log: what the log may do between
//! [`Log::prepare_compaction`] and [`Log::finish_compaction`], and what the
//! pass installs afterwards.

use {
    super::{CompactionOutcome, Log},
    crate::{
        error::LogError,
        io::{IoTarget, LogIo},
        log::test_support::{TempLog, append_keyed_samples, compact_test_log, compaction_ctx},
        name,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_ids::Offset,
    krabka_units::prelude::mebibytes,
    std::{
        fs::File,
        io::Write as _,
        path::Path,
        sync::{Arc, Mutex, mpsc},
        time::{Duration, SystemTime},
    },
};

/// Keys `k0`, `k1`, `k0`, ... at offsets `0..batches`, one batch per segment.
fn alternating_keys(log: &mut Log, batches: i64) {
    append_keyed_samples(log, batches, |i| format!("k{}", i % 2));
}

/// One record at the log end with key `key`.
fn append_key(log: &mut Log, key: &str) { append_keyed_samples(log, 1, |_| key.to_owned()); }

/// Every record of the log, as `(offset, key)`.
fn records(log: &Log) -> Vec<(i64, Bytes)> {
    log.read(log.log_start_offset(), mebibytes(1))
        .unwrap()
        .batches
        .iter()
        .flat_map(|batch| {
            batch.records.iter().map(|record| {
                (
                    batch.base_offset + i64::from(record.offset_delta),
                    record.key.clone().unwrap(),
                )
            })
        })
        .collect()
}

fn keyed(pairs: &[(i64, &'static str)]) -> Vec<(i64, Bytes)> {
    pairs
        .iter()
        .map(|&(offset, key)| (offset, Bytes::from_static(key.as_bytes())))
        .collect()
}

/// The `.cleaned` files in `dir`: a pass's output that was never promoted.
fn cleaned_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".cleaned"))
        .collect();
    names.sort();
    names
}

/// A log of six alternating keys, five of them sealed, and a job over them.
fn log_with_job() -> (TempLog, super::CompactionJob) {
    let mut fixture = compact_test_log();
    alternating_keys(&mut fixture.log, 6);
    let job = fixture
        .log
        .prepare_compaction(&compaction_ctx())
        .unwrap()
        .expect("five sealed segments to compact");
    (fixture, job)
}

/// Appends while the pass builds land past its input, and survive it: the
/// pass deduplicates the range it took and leaves the rest as written.
#[test]
fn appends_while_the_pass_builds_survive_it() {
    let (TempLog { dir: _dir, mut log }, job) = log_with_job();
    // Each append rolls, so these seal two more segments behind the input.
    append_key(&mut log, "k0");
    append_key(&mut log, "k1");

    let outcome = log.finish_compaction(job.build()).unwrap();

    check!(outcome == CompactionOutcome::Installed);
    assert!(records(&log) == keyed(&[(3, "k1"), (4, "k0"), (5, "k1"), (6, "k0"), (7, "k1")]));
}

/// A pass whose input a truncation took away installs nothing, and leaves
/// no file behind.
#[test]
fn a_truncation_into_the_input_makes_the_pass_stale() {
    let (TempLog { dir, mut log }, job) = log_with_job();
    log.truncate_to(Offset(3)).unwrap();

    let outcome = log.finish_compaction(job.build()).unwrap();

    check!(outcome == CompactionOutcome::Stale);
    check!(records(&log) == keyed(&[(0, "k0"), (1, "k1"), (2, "k0")]));
    check!(cleaned_files(dir.path()).is_empty());
}

/// Retention behind a `DeleteRecords` drops the oldest input segments, so the
/// pass installs nothing.
#[test]
fn retention_that_drops_an_input_segment_makes_the_pass_stale() {
    let (TempLog { dir, mut log }, job) = log_with_job();
    log.set_log_start_offset(Offset(2)).unwrap();
    log.tick(SystemTime::now(), Offset(i64::MAX)).unwrap();

    let outcome = log.finish_compaction(job.build()).unwrap();

    check!(outcome == CompactionOutcome::Stale);
    check!(records(&log) == keyed(&[(2, "k0"), (3, "k1"), (4, "k0"), (5, "k1")]));
    check!(cleaned_files(dir.path()).is_empty());
}

/// A build that fails is the pass's error while the log still holds the
/// input, and no error once the log has moved on: the failure then says
/// nothing about the disk.
#[test]
fn a_failed_build_is_an_error_only_while_its_input_stands() {
    for truncate_first in [false, true] {
        let mut fixture = compact_test_log();
        alternating_keys(&mut fixture.log, 6);
        // A torn batch after the first segment's records fails the build.
        std::fs::OpenOptions::new()
            .append(true)
            .open(name::log_path(fixture.dir.path(), 0))
            .unwrap()
            .write_all(&[0; 16])
            .unwrap();
        let job = fixture
            .log
            .prepare_compaction(&compaction_ctx())
            .unwrap()
            .unwrap();
        if truncate_first {
            fixture.log.truncate_to(Offset(3)).unwrap();
        }

        let finished = fixture.log.finish_compaction(job.build());

        if truncate_first {
            check!(matches!(finished, Ok(CompactionOutcome::Stale)));
        } else {
            check!(matches!(finished, Err(LogError::Records(_))));
        }
    }
}

/// The log runs one job at a time; dropping a job frees the log for the next.
#[test]
fn a_log_runs_one_job_at_a_time() {
    let (TempLog { dir: _dir, mut log }, job) = log_with_job();

    check!(log.prepare_compaction(&compaction_ctx()).unwrap().is_none());
    drop(job);
    check!(log.prepare_compaction(&compaction_ctx()).unwrap().is_some());
}

/// A fenced log takes no job, and a job prepared before the fence writes no
/// file and installs nothing.
#[test]
fn a_fence_before_the_build_stops_the_pass() {
    let (TempLog { dir, mut log }, job) = log_with_job();
    log.fence();

    let built = job.build();

    check!(matches!(built.rewrites, Err(LogError::Fenced)));
    check!(cleaned_files(dir.path()).is_empty());
    check!(log.finish_compaction(built).unwrap() == CompactionOutcome::Stale);
    check!(log.prepare_compaction(&compaction_ctx()).unwrap().is_none());
}

/// A [`LogIo`] that holds the first compaction write until the test lets it
/// go, so a fence can arrive while the build is writing.
#[derive(Debug)]
struct PausedRewrite {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl LogIo for PausedRewrite {
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        if target == IoTarget::CompactionSwap
            && let Some(entered) = self.entered.lock().unwrap().take()
        {
            entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        (&*file).write(buf)
    }
}

/// A fence during the build returns only once the build writes nothing more,
/// so the broker can delete the partition's directory as soon as it returns.
#[test]
fn a_fence_during_the_build_waits_for_it_to_stop_writing() {
    let (entered, build_entered) = mpsc::channel();
    let (let_go, release) = mpsc::channel();
    let mut fixture = compact_test_log();
    alternating_keys(&mut fixture.log, 6);
    fixture.log.test_set_io(Arc::new(PausedRewrite {
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(release),
    }));
    let job = fixture
        .log
        .prepare_compaction(&compaction_ctx())
        .unwrap()
        .unwrap();
    let TempLog { dir, mut log } = fixture;

    let built = std::thread::scope(|scope| {
        let builder = scope.spawn(|| job.build());
        build_entered.recv().unwrap();
        let fencer = scope.spawn(|| log.fence());
        // The build is held inside a write, so the fence has to wait for it.
        std::thread::sleep(Duration::from_millis(100));
        check!(!fencer.is_finished());

        let_go.send(()).unwrap();
        fencer.join().unwrap();
        let after_fence = cleaned_files(dir.path());
        let built = builder.join().unwrap();
        check!(cleaned_files(dir.path()) == after_fence);
        built
    });

    check!(matches!(built.rewrites, Err(LogError::Fenced)));
    check!(log.finish_compaction(built).unwrap() == CompactionOutcome::Stale);
    check!(cleaned_files(dir.path()).is_empty());
}
