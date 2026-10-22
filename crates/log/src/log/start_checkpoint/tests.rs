use {
    super::*,
    crate::{
        LogConfig,
        log::test_support::{append_samples, tiny_segments},
    },
    assert2::check,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, Time},
    std::{path::Path, time::SystemTime},
    tempfile::tempdir,
};

/// The default `start_offset_checkpoint_interval`, Kafka's one minute.
const INTERVAL: Duration = Duration::from_secs(60);

/// The high watermark the tests run retention against. Retention keeps every
/// segment that reaches past it, so it moves the start to offset 4 and no
/// further.
const RETAINED_FROM: Offset = Offset(4);

fn checkpoint(dir: &Path) -> Option<Offset> { log_start_offset_checkpoint::read(dir).unwrap() }

/// A log of four two-record segments, at 0, 2, 4 and 6, with nothing allowed
/// by `retention.bytes` and files unlinked as they retire. A `DeleteRecords`
/// inside the first segment has checkpointed a start of 1.
fn retained_log(dir: &Path) -> Log {
    let mut log = Log::open(dir, LogConfig {
        retention_size: Some(ByteSize::ZERO),
        file_delete_delay: Time::ZERO,
        ..tiny_segments()
    })
    .unwrap();
    append_samples(&mut log, 4, 2);
    log.set_log_start_offset(Offset(1)).unwrap();
    log
}

/// Retention moves the start to a segment boundary and leaves the file alone.
/// The write falls due one interval later, the maintenance delay wakes the
/// writer for it, and a pass that moves nothing owes nothing.
#[test]
fn retention_writes_the_start_it_moves_one_interval_later() {
    let dir = tempdir().unwrap();
    let mut log = retained_log(dir.path());
    check!(checkpoint(dir.path()) == Some(Offset(1)));

    let moved = Instant::now();
    log.tick(SystemTime::now(), RETAINED_FROM).unwrap();
    assert2::assert!(log.log_start_offset() == RETAINED_FROM);
    check!(
        checkpoint(dir.path()) == Some(Offset(1)),
        "retention leaves the write for the timer"
    );
    let due = log.start_checkpoint_due.expect("retention owes the write");
    check!(due >= moved + INTERVAL);
    check!(log.maintenance_delay_at(SystemTime::now(), due) == Some(Duration::ZERO));

    let just_before = due.checked_sub(Duration::from_millis(1)).unwrap();
    log.checkpoint_log_start_at(just_before).unwrap();
    check!(checkpoint(dir.path()) == Some(Offset(1)), "not due yet");
    log.checkpoint_log_start_at(due).unwrap();
    check!(checkpoint(dir.path()) == Some(RETAINED_FROM));
    check!(log.start_checkpoint_due.is_none());

    log.tick(SystemTime::now(), RETAINED_FROM).unwrap();
    check!(
        log.start_checkpoint_due.is_none(),
        "a pass that moves nothing owes nothing"
    );
}

/// What happens to a write retention owes when something else reaches the
/// file first.
#[derive(Clone, Copy, Debug)]
enum Before {
    /// A sync makes everything durable, the owed start included.
    Sync,
    /// A `DeleteRecords` writes its own start at once.
    DeleteRecords,
    /// A reset removes the file, and nothing is owed to it any more.
    Reset,
    /// A fenced log is being removed, so it writes nothing.
    FencedFlush,
}

#[yare::parameterized(
    sync = { Before::Sync, Some(RETAINED_FROM), false },
    delete_records = { Before::DeleteRecords, Some(Offset(5)), false },
    reset = { Before::Reset, None, false },
    fenced_flush = { Before::FencedFlush, Some(Offset(1)), true },
)]
fn an_owed_start_is_paid_or_dropped_by_what_reaches_the_file_first(
    before: Before,
    file: Option<Offset>,
    still_owed: bool,
) {
    let dir = tempdir().unwrap();
    let mut log = retained_log(dir.path());
    log.tick(SystemTime::now(), RETAINED_FROM).unwrap();
    assert2::assert!(log.start_checkpoint_due.is_some());

    match before {
        Before::Sync => log.sync().unwrap(),
        Before::DeleteRecords => log.set_log_start_offset(Offset(5)).unwrap(),
        Before::Reset => log.reset_to(Offset(20)).unwrap(),
        Before::FencedFlush => {
            log.fence();
            log.flush_log_start_checkpoint().unwrap();
        }
    }

    check!(checkpoint(dir.path()) == file, "{before:?}");
    check!(
        log.start_checkpoint_due.is_some() == still_owed,
        "{before:?}"
    );
}

/// A crash before the timer leaves the older, lower start on disk. Kafka's
/// `LogLoader` reopens a local log at its first segment all the same, and
/// leaves a tiered log's start where the file put it, below the local
/// segments, for the remote tier to serve. Only the local log owes the
/// raised start to the file.
#[yare::parameterized(
    local = { false, RETAINED_FROM },
    tiered = { true, Offset(1) },
)]
fn a_reopen_after_a_crash_before_the_write_floors_only_a_local_log(
    remote_storage_enable: bool,
    expected: Offset,
) {
    let dir = tempdir().unwrap();
    {
        let mut log = retained_log(dir.path());
        log.tick(SystemTime::now(), RETAINED_FROM).unwrap();
    }

    let log = Log::open(dir.path(), LogConfig {
        remote_storage_enable,
        ..tiny_segments()
    })
    .unwrap();
    check!(log.log_start_offset() == expected);
    check!(log.local_log_start_offset() == RETAINED_FROM);
    check!(
        checkpoint(dir.path()) == Some(Offset(1)),
        "the open writes nothing"
    );
    check!(log.start_checkpoint_due.is_some() == !remote_storage_enable);
}
