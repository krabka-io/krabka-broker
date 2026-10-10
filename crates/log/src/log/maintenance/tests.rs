use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use assert2::{assert, check};
use krabka_compression::CompressionType;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_units::{Time, millis};

use super::*;
use crate::{
    LogConfig,
    log::{
        sync::sync_observer,
        test_support::{
            NO_LIMIT, configured_test_log, sample_batch, test_log, tiny_segments, verbatim_from,
        },
    },
    name,
};

#[test]
fn message_threshold_counts_both_append_paths_and_sync_resets_it() {
    for verbatim in [false, true] {
        let (_dir, mut log) = configured_test_log(LogConfig {
            flush_messages: Some(3),
            ..LogConfig::default()
        });
        sync_observer::take_segment_flushes();
        let mut append = |n| {
            if verbatim {
                let (_, batch) = verbatim_from(&sample_batch(n), LeaderEpoch(0));
                log.append_verbatim(&batch).unwrap();
            } else {
                log.append(&mut sample_batch(n)).unwrap();
            }
        };
        append(2);
        check!(sync_observer::take_segment_flushes().is_empty());
        append(1);
        check!(sync_observer::take_segment_flushes() == vec![Offset(0)]);
        append(2);
        check!(sync_observer::take_segment_flushes().is_empty());
    }
}

#[test]
fn timer_flushes_dirty_idle_logs_even_with_remote_storage_enabled() {
    let (_dir, mut log) = configured_test_log(LogConfig {
        flush_interval: Some(millis(1000)),
        remote_storage_enable: true,
        ..LogConfig::default()
    });
    log.append(&mut sample_batch(1)).unwrap();
    let last = log.last_flush;
    sync_observer::take_segment_flushes();
    log.tick(last + Duration::from_millis(999), Offset(1))
        .unwrap();
    check!(sync_observer::take_segment_flushes().is_empty());
    log.tick(last + Duration::from_secs(1), Offset(1)).unwrap();
    check!(sync_observer::take_segment_flushes() == vec![Offset(0)]);
    check!(log.maintenance_delay(last + Duration::from_secs(2)) == None);
}

#[test]
fn changing_flush_config_activates_the_timer_for_existing_dirty_data() {
    let (_dir, mut log) = test_log();
    log.append(&mut sample_batch(1)).unwrap();
    check!(log.maintenance_delay(SystemTime::now()) == None);
    log.set_config(LogConfig {
        flush_interval: Some(Time::ZERO),
        ..LogConfig::default()
    });
    check!(log.maintenance_delay(SystemTime::now()) == Some(Duration::ZERO));
    log.maintain(SystemTime::now()).unwrap();
    check!(log.unflushed_messages == 0);
}

#[test]
fn segment_jitter_is_stable_bounded_and_changes_the_roll_boundary() {
    let (_dir, mut log) = configured_test_log(LogConfig {
        segment_roll_interval: millis(100),
        segment_jitter: millis(1000),
        ..LogConfig::default()
    });
    log.roll_jitter = Some((Offset(0), 75));
    for _ in 0..3 {
        check!(log.jittered_roll_interval(millis(100)) == millis(25));
    }
    log.append(&mut sample_batch(1)).unwrap();
    let mut batch = sample_batch(1);
    batch.max_timestamp = 26;
    log.append(&mut batch).unwrap();
    check!(log.segments.len() == 1);
    let interval = log.jittered_roll_interval(millis(100));
    check!(interval > Time::ZERO && interval <= millis(100));
    check!(log.jittered_roll_interval(millis(100)) == interval);
}

#[test]
fn retention_removes_visibility_before_reclaiming_files_and_reopen_cleans_tombstones() {
    let (dir, mut log) = configured_test_log(LogConfig {
        file_delete_delay: millis(1000),
        ..tiny_segments()
    });
    log.append(&mut sample_batch(1)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.trim_to_offset(Offset(1)).unwrap();
    check!(!name::log_path(dir.path(), 0).exists());
    check!(log.log_start_offset() == Offset(1));
    assert!(log.pending_deletes.len() == 1);
    let (deadline, paths) = log.pending_deletes[0].clone();
    check!(paths.iter().all(|p| p.exists()));
    log.maintain(deadline - Duration::from_millis(1)).unwrap();
    check!(paths.iter().all(|p| p.exists()));
    log.maintain(deadline).unwrap();
    check!(paths.iter().all(|p| !p.exists()));
    log.reset_to(Offset(2)).unwrap();
    let tombstones = log.pending_deletes[0].1.clone();
    drop(log);
    let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
    check!(reopened.log_end_offset() == Offset(2));
    check!(tombstones.iter().all(|p| !p.exists()));
}

#[test]
fn zero_delete_delay_unlinks_immediately() {
    let (dir, mut log) = configured_test_log(LogConfig {
        file_delete_delay: Time::ZERO,
        ..tiny_segments()
    });
    log.append(&mut sample_batch(1)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.trim_to_offset(Offset(1)).unwrap();
    check!(log.pending_deletes.is_empty());
    check!(!name::log_path(dir.path(), 0).exists());
}

#[test]
fn repeated_replacement_of_one_base_keeps_each_retired_generation() {
    let (_dir, mut log) = test_log();
    log.append(&mut sample_batch(1)).unwrap();
    log.reset_to(Offset(0)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.reset_to(Offset(0)).unwrap();
    check!(log.pending_deletes.len() == 2);
    let first = &log.pending_deletes[0].1;
    let second = &log.pending_deletes[1].1;
    check!(first.iter().chain(second).all(|p| p.exists()));
    check!(first.iter().all(|p| !second.contains(p)));
}

#[derive(Debug)]
struct FailUnlink;
impl crate::io::LogIo for FailUnlink {
    fn remove_file(&self, _: crate::io::IoTarget, _: &std::path::Path) -> std::io::Result<()> {
        Err(std::io::Error::other("injected deletion failure"))
    }
}

#[test]
fn a_failed_deferred_unlink_is_reported_and_retried() {
    let (_dir, mut log) = configured_test_log(tiny_segments());
    log.append(&mut sample_batch(1)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();
    log.trim_to_offset(Offset(1)).unwrap();
    let deadline = log.pending_deletes[0].0;
    log.test_set_io(Arc::new(FailUnlink));
    check!(log.maintain(deadline).is_err());
    check!(!log.pending_deletes.is_empty());
    log.test_set_io(crate::io::file_io());
    log.maintain(deadline).unwrap();
    check!(log.pending_deletes.is_empty());
}

#[test]
fn owned_gzip_batches_use_the_live_level_and_verbatim_keeps_producer_bytes() {
    for allocation in [
        crate::SegmentAllocation::OnWrite,
        crate::SegmentAllocation::Preallocate,
    ] {
        let (_dir, mut log) = test_log();
        for level in [1, 9] {
            log.set_config(LogConfig {
                compression_gzip_level: level,
                segment_allocation: allocation,
                ..LogConfig::default()
            });
            let mut batch = sample_batch(1);
            batch.records[0].value = Some(bytes::Bytes::from(vec![b'a'; 10_000]));
            batch.attributes = batch.attributes.with_compression(CompressionType::Gzip);
            let (base, _) = log.append(&mut batch).unwrap();
            let mut expected = bytes::BytesMut::new();
            batch
                .encode_with_compression_level(&mut expected, Some(level))
                .unwrap();
            let stored = log.read_raw(base, log.log_end_offset(), NO_LIMIT).unwrap();
            check!(stored.bytes == expected.freeze());
        }
        let mut batch = sample_batch(1);
        batch.attributes = batch.attributes.with_compression(CompressionType::Gzip);
        let (_, batch) = verbatim_from(&batch, LeaderEpoch(0));
        let (base, _) = log.append_verbatim(&batch).unwrap();
        let stored = log.read_raw(base, log.log_end_offset(), NO_LIMIT).unwrap();
        check!(stored.bytes[16..] == batch.bytes[16..]);
    }
}

#[test]
fn compaction_keeps_retired_files_until_the_deadline() {
    use crate::log::test_support::{compacting_segments, compaction_ctx, keyed_batch};
    let (_dir, mut log) = configured_test_log(compacting_segments());
    for value in [b"old", b"new", b"end"] {
        log.append(&mut keyed_batch(0, &[(0, b"key", value)]))
            .unwrap();
    }
    log.compact(&compaction_ctx()).unwrap();
    check!(!log.pending_deletes.is_empty());
    let paths: Vec<_> = log
        .pending_deletes
        .iter()
        .flat_map(|(_, paths)| paths.clone())
        .collect();
    check!(paths.iter().all(|path| path.exists()));
    let deadline = log
        .pending_deletes
        .iter()
        .map(|(deadline, _)| *deadline)
        .max()
        .unwrap();
    log.maintain(deadline).unwrap();
    check!(paths.iter().all(|path| !path.exists()));
    check!(log.log_end_offset() == Offset(3));
}
