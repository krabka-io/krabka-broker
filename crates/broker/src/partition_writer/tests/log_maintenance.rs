//! Writer-loop tests for the arms that maintain the log rather than write
//! to it: a config notification, a log-start trim, and the deletion of retired files.

use {
    super::*,
    crate::partition_writer::test_support::{sample_batch, spawn_default_writer},
    assert2::assert,
    krabka_log::{LogConfig, Offset},
    tempfile::tempdir,
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

    let (tx, writer) = spawn_default_writer(dir.path(), log.clone());

    let (trim_ack, ack_rx) = tokio::sync::oneshot::channel();
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
struct ObserveFlush(std::sync::Arc<tokio::sync::Notify>);

impl krabka_log::LogIo for ObserveFlush {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
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
        let flushed = std::sync::Arc::new(tokio::sync::Notify::new());
        {
            let mut log = log.lock().unwrap();
            log.append(&mut sample_batch().count(2).call()).unwrap();
            log.test_set_io(std::sync::Arc::new(ObserveFlush(flushed.clone())));
        }
        let (tx, writer) = spawn_default_writer(dir.path(), log.clone());
        log.lock().unwrap().set_config(config);
        tx.send(WriterMessage::ConfigChanged).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), flushed.notified())
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
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

impl GatedUnlink {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl krabka_log::LogIo for GatedUnlink {
    fn remove_file(
        &self,
        _target: krabka_log::IoTarget,
        path: &std::path::Path,
    ) -> std::io::Result<()> {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
        }
        std::fs::remove_file(path)
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
    let retired = krabka_log::name::log_path(dir.path(), 0);
    let tombstones: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".deleted"))
        .collect();
    assert!(!retired.exists());
    assert!(!tombstones.is_empty());
    let (started, unlink_started) = tokio::sync::oneshot::channel();
    let gate = std::sync::Arc::new(GatedUnlink {
        started: std::sync::Mutex::new(Some(started)),
        released: std::sync::Mutex::new(false),
        changed: std::sync::Condvar::new(),
    });
    log.test_set_io(gate.clone());
    let log = std::sync::Arc::new(std::sync::Mutex::new(log));
    let (tx, writer) = spawn_default_writer(dir.path(), log.clone());

    let wait = std::time::Duration::from_secs(5);
    tokio::time::timeout(wait, unlink_started)
        .await
        .expect("the writer deletes the retired files once their delay passes")
        .unwrap();
    let ack = queue_batch(&tx, sample_batch().call()).await;
    let appended = tokio::time::timeout(wait, ack).await;
    gate.release();
    let appended = appended
        .expect("a produce completes while the unlink is held")
        .unwrap()
        .unwrap();
    assert!(appended.base_offset == Offset(2));
    tokio::time::timeout(wait, async {
        while tombstones.iter().any(|path| path.exists()) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the retired files are deleted once the unlink is released");

    drop(tx);
    writer.await.unwrap();
}
