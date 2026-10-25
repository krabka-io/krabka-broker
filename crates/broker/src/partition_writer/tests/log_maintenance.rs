//! Writer-loop tests for the arms that maintain the log rather than write
//! to it: a config notification, a log-start trim, and the deletion of retired files.

use {
    super::*,
    crate::partition_writer::test_support::{sample_batch, spawn_default_writer},
    assert2::assert,
    krabka_log::{LogConfig, Offset, name},
    std::{
        fs,
        fs::File,
        io,
        path::Path,
        sync::Condvar,
        time::{Duration, SystemTime},
    },
    tempfile::tempdir,
    tokio::{sync::oneshot, time},
};

#[tokio::test]
async fn writer_trim_to_offset_advances_log_start() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    // Pre-populate with two batches → LEO = 4.
    for _ in 0..2 {
        log.lock()
            .expect("lock")
            .append(&mut sample_batch().count(2).call())
            .expect("append");
    }

    let (tx, writer) = spawn_default_writer(dir.path(), Arc::clone(&log));

    let (trim_ack, ack_rx) = oneshot::channel();
    let msg = WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack: trim_ack,
    };
    tx.send(msg).await.expect("send");
    let new_start = ack_rx.await.expect("ack").expect("trim ok");
    assert!(new_start >= 3);
    assert!(log.lock().expect("lock").log_start_offset() == new_start);

    drop(tx);
    writer.await.expect("writer join");
}

#[derive(Debug)]
struct ObserveFlush(Arc<Notify>);

impl krabka_log::LogIo for ObserveFlush {
    fn sync_data(&self, file: &File) -> io::Result<()> {
        file.sync_data()?;
        self.0.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn writer_flush_timer_runs_after_a_live_config_change_without_another_append() {
    for config in [
        LogConfig {
            flush_interval: Some(krabka_units::millis(10)),
            ..LogConfig::default()
        },
        LogConfig {
            flush_messages: Some(2),
            ..LogConfig::default()
        },
    ] {
        let dir = tempdir().unwrap();
        let log = open_default_log(dir.path());
        let flushed = Arc::new(Notify::new());
        {
            let mut log = log.lock().unwrap();
            log.append(&mut sample_batch().count(2).call()).unwrap();
            log.test_set_io(Arc::new(ObserveFlush(Arc::clone(&flushed))));
        }
        let (tx, writer) = spawn_default_writer(dir.path(), Arc::clone(&log));
        log.lock().unwrap().set_config(config);
        tx.send(WriterMessage::ConfigChanged).await.unwrap();
        time::timeout(Duration::from_secs(2), flushed.notified())
            .await
            .expect("idle partition flushes after the live threshold changes");
        drop(tx);
        writer.await.unwrap();
    }
}

/// Holds the first unlink until the test releases it, and reports that it
/// started.
#[derive(Debug)]
struct GatedUnlink {
    started: Mutex<Option<oneshot::Sender<()>>>,
    released: Mutex<bool>,
    changed: Condvar,
}

impl GatedUnlink {
    fn release_unlink(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl krabka_log::LogIo for GatedUnlink {
    fn remove_file(&self, _target: krabka_log::IoTarget, path: &Path) -> io::Result<()> {
        let started = self.started.lock().unwrap().take();
        if let Some(started) = started {
            let _ = started.send(());
            drop(
                self.changed
                    .wait_while(self.released.lock().unwrap(), |released| !*released)
                    .unwrap(),
            );
        }
        fs::remove_file(path)
    }
}

/// The writer deletes retired segment files off its own loop and outside the
/// log mutex, so a produce completes while an unlink is still in progress.
///
/// Unlinking a segment drops its page cache, tens of milliseconds a file on a
/// loaded disk. When the writer awaited the unlinks of a retention pass under
/// the log mutex, every append behind them waited too (#1394).
#[tokio::test]
async fn a_produce_completes_while_retired_files_are_being_deleted() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig {
        segment_size: krabka_units::bytes(1),
        file_delete_delay: krabka_units::millis(1),
        ..LogConfig::default()
    })
    .unwrap();
    log.append(&mut sample_batch().call()).unwrap();
    log.append(&mut sample_batch().call()).unwrap();
    log.trim_to_offset(Offset(1)).unwrap();
    let retired = name::log_path(dir.path(), 0);
    let tombstones: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".deleted"))
        .collect();
    assert!(!retired.exists());
    assert!(!tombstones.is_empty());
    let (started, unlink_started) = oneshot::channel();
    let gate = Arc::new(GatedUnlink {
        started: Mutex::new(Some(started)),
        released: Mutex::new(false),
        changed: Condvar::new(),
    });
    log.test_set_io(Arc::<GatedUnlink>::clone(&gate));
    let log = Arc::new(Mutex::new(log));
    let (tx, writer) = spawn_default_writer(dir.path(), Arc::clone(&log));

    let wait = Duration::from_secs(5);
    time::timeout(wait, unlink_started)
        .await
        .expect("the writer deletes the retired files once their delay passes")
        .unwrap();
    let ack = queue_batch(&tx, sample_batch().call()).await;
    let appended = time::timeout(wait, ack).await;
    gate.release_unlink();
    let appended = appended
        .expect("a produce completes while the unlink is held")
        .unwrap()
        .unwrap();
    assert!(appended.base_offset == Offset(2));
    time::timeout(wait, async {
        while tombstones.iter().any(|path| path.exists()) {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the retired files are deleted once the unlink is released");

    drop(tx);
    writer.await.unwrap();
}

/// A log start that retention moved is written on a timer, Kafka's
/// `log.flush.start.offset.checkpoint.interval.ms`, rather than by the pass
/// that moved it. An idle writer wakes for it with no message to prompt it.
#[tokio::test]
async fn an_idle_writer_checkpoints_the_start_retention_moved_once_it_is_due() {
    let dir = tempdir().unwrap();
    let checkpoint = name::log_start_offset_checkpoint_path(dir.path());
    let mut log = Log::open(dir.path(), LogConfig {
        segment_size: krabka_units::bytes(1),
        retention_size: Some(krabka_units::bytes(0)),
        file_delete_delay: krabka_units::millis(0),
        start_offset_checkpoint_interval: krabka_units::millis(20),
        ..LogConfig::default()
    })
    .unwrap();
    for _ in 0..3 {
        log.append(&mut sample_batch().call()).unwrap();
    }
    log.set_log_start_offset(Offset(1)).unwrap();
    log.tick(SystemTime::now(), Offset(2)).unwrap();
    assert!(log.log_start_offset() == Offset(2));
    assert!(fs::read_to_string(&checkpoint).unwrap() == "0\n1\n");
    let log = Arc::new(Mutex::new(log));
    let (tx, writer) = spawn_default_writer(dir.path(), Arc::clone(&log));

    time::timeout(Duration::from_secs(5), async {
        while fs::read_to_string(&checkpoint).unwrap() != "0\n2\n" {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the writer checkpoints the start once the interval passes");

    drop(tx);
    writer.await.unwrap();
}
