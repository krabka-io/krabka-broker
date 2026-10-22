//! A compaction pass built without the log: what the log may do between
//! [`Log::prepare_compaction`] and [`Log::finish_compaction`], and what the
//! pass installs afterwards.

use {
    super::{CompactionOutcome, Log},
    crate::{
        compaction_pause::compaction_pause,
        error::LogError,
        log::test_support::{
            TempLog, append_keyed_samples, cleaned_files, compact_test_log, compaction_ctx,
        },
        name,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_ids::Offset,
    krabka_units::prelude::mebibytes,
    std::{
        io::Write as _,
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

/// Truncate the log into the input of [`log_with_job`]'s pass.
fn truncate_into_the_input(log: &mut Log) { log.truncate_to(Offset(3)).unwrap(); }

/// `DeleteRecords` and the retention behind it drop the oldest input
/// segments of [`log_with_job`]'s pass.
fn delete_the_oldest_input(log: &mut Log) {
    log.set_log_start_offset(Offset(2)).unwrap();
    log.tick(SystemTime::now(), Offset(i64::MAX)).unwrap();
}

/// A pass whose input the log dropped while it built installs nothing, and
/// leaves no file behind.
#[yare::parameterized(
    truncation = { truncate_into_the_input, &[(0, "k0"), (1, "k1"), (2, "k0")] },
    retention = { delete_the_oldest_input, &[(2, "k0"), (3, "k1"), (4, "k0"), (5, "k1")] },
)]
fn a_pass_whose_input_is_gone_is_stale(meanwhile: fn(&mut Log), left: &[(i64, &'static str)]) {
    let (TempLog { dir, mut log }, job) = log_with_job();
    meanwhile(&mut log);

    let outcome = log.finish_compaction(job.build()).unwrap();

    check!(outcome == CompactionOutcome::Stale);
    check!(records(&log) == keyed(left));
    check!(cleaned_files(dir.path()).is_empty());
}

/// A built pass that is dropped instead of finished, as when the task that
/// would finish it goes away, deletes its files and frees the log for the
/// next job.
#[test]
fn dropping_a_built_pass_deletes_its_files() {
    let (TempLog { dir, mut log }, job) = log_with_job();
    let built = job.build();
    check!(!cleaned_files(dir.path()).is_empty());

    drop(built);

    check!(cleaned_files(dir.path()).is_empty());
    check!(log.prepare_compaction(&compaction_ctx()).unwrap().is_some());
}

/// Closing the log stops a job as the fence does, so a caller can move or
/// delete the directory once the close returns.
#[test]
fn closing_the_log_stops_the_pass() {
    let (TempLog { dir, log }, job) = log_with_job();
    log.close();

    let built = job.build();

    check!(matches!(built.rewrites, Err(LogError::Fenced)));
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
            truncate_into_the_input(&mut fixture.log);
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

/// A fence during the build returns only once the build writes nothing more,
/// so the broker can delete the partition's directory as soon as it returns.
#[test]
fn a_fence_during_the_build_waits_for_it_to_stop_writing() {
    let (pause, control) = compaction_pause();
    let mut fixture = compact_test_log();
    alternating_keys(&mut fixture.log, 6);
    fixture.log.test_set_io(pause);
    let job = fixture
        .log
        .prepare_compaction(&compaction_ctx())
        .unwrap()
        .unwrap();
    let TempLog { dir, mut log } = fixture;

    let built = std::thread::scope(|scope| {
        let builder = scope.spawn(|| job.build());
        control.wait_until_paused();
        let fencer = scope.spawn(|| log.fence());
        // The build is held inside a write, so the fence has to wait for it.
        std::thread::sleep(Duration::from_millis(100));
        check!(!fencer.is_finished());

        control.resume();
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
