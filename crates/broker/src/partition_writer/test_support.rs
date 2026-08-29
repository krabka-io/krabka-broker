//! Fixtures the partition-writer unit tests share: batch builders, a log
//! opener, and the stub sequencer and WAL the diskless tests drive.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
use krabka_protocol::records::{Record, RecordBatch};
use tokio::sync::oneshot;

#[derive(Debug)]
pub(super) struct FixedStamp(pub(super) u64);

impl krabka_log::StampSource for FixedStamp {
    fn next_stamp(&self) -> u64 {
        self.0
    }
}

struct TestSequencer {
    next: AtomicI64,
}

#[async_trait::async_trait]
impl crate::wal::OffsetSequencer for TestSequencer {
    async fn assign(
        &self,
        _topic: &str,
        _partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, crate::error::BrokerError> {
        let base = self.next.fetch_add(i64::from(count), Ordering::SeqCst);
        Ok(Offset(base))
    }
}

pub(super) fn test_sequencer() -> Arc<dyn crate::wal::OffsetSequencer> {
    Arc::new(TestSequencer {
        next: AtomicI64::new(0),
    })
}

pub(super) fn sample_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            ..Default::default()
        });
    }
    b
}

pub(super) struct GatedWal {
    sync_started: Mutex<Option<oneshot::Sender<()>>>,
    release_sync: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) trimmed_to: AtomicI64,
}

impl GatedWal {
    pub(super) fn new(
        sync_started: oneshot::Sender<()>,
        release_sync: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            sync_started: Mutex::new(Some(sync_started)),
            release_sync: tokio::sync::Mutex::new(Some(release_sync)),
            trimmed_to: AtomicI64::new(-1),
        }
    }
}

#[async_trait::async_trait]
impl crate::wal::WalStore for GatedWal {
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, crate::error::BrokerError> {
        if let Some(started) = self.sync_started.lock().unwrap().take() {
            let _ = started.send(());
        }
        let release = self
            .release_sync
            .lock()
            .await
            .take()
            .expect("sync release receiver present");
        release.await.expect("sync release sent");
        Ok(leo)
    }

    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, crate::error::BrokerError> {
        self.trimmed_to.store(new_start.0, Ordering::SeqCst);
        Ok(new_start)
    }
}

pub(super) fn open_log_with_records(path: &std::path::Path, records: i32) -> Log {
    let mut log = Log::open(path, LogConfig::default()).expect("open log");
    if records > 0 {
        log.append(&mut sample_batch(records)).expect("append");
    }
    log
}
