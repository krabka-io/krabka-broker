//! [`MetadataEventLog`] wrappers that let tests drive the projection's
//! catch-up gate: [`PacedReplayLog`] paces the stream `subscribe` returns, so
//! a replay can be slow or silent-but-open, and [`RacingAppendLog`] appends a
//! record from inside `subscribe`, so a test can land another broker's flush
//! in the window between establishing the subscription and reading the
//! watermark. They compose: wrap one in the other.

use std::{
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{FutureExt as _, StreamExt as _};
use krabka_remote_storage_topic::{
    AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart,
};

/// How fast the wrapped log's subscription delivers what it replays.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReplayPace {
    /// Deliver one record per interval.
    OneEvery(Duration),
    /// Deliver nothing, ever, without closing the stream. This is the shape a
    /// [`krabka_remote_storage_topic::KafkaMetadataEventLog`] partition takes
    /// when its fetch loop dies while connecting: the shared queue keeps its
    /// sender, so the stream stays open and silent.
    Never,
}

/// Wraps a [`MetadataEventLog`] and paces its subscription. Every other
/// method delegates.
pub(crate) struct PacedReplayLog {
    inner: Arc<dyn MetadataEventLog>,
    pace: ReplayPace,
}

impl PacedReplayLog {
    pub(crate) fn new(inner: Arc<dyn MetadataEventLog>, pace: ReplayPace) -> Arc<Self> {
        Arc::new(Self { inner, pace })
    }
}

#[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic, keyed)]
#[async_trait]
impl MetadataEventLog for PacedReplayLog {
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (stream, handle) = self.inner.subscribe(assignment);
        let pace = self.pace;
        let paced = stream.then(move |event| async move {
            match pace {
                ReplayPace::OneEvery(interval) => tokio::time::sleep(interval).await,
                ReplayPace::Never => std::future::pending::<()>().await,
            }
            event
        });
        (Box::pin(paced), handle)
    }
}

/// Publishes one record from inside `subscribe`, modelling another broker's
/// in-flight flush landing exactly while a restarting projection establishes
/// its subscription. A watermark read *before* subscribing steps over that
/// record; one read after cannot.
pub(crate) struct RacingAppendLog {
    inner: Arc<dyn MetadataEventLog>,
    racing: StdMutex<Option<(i32, Bytes, Bytes)>>,
}

impl RacingAppendLog {
    /// Race the keyed record `(key, event)` onto `partition`, as the flusher
    /// publishes every index record.
    pub(crate) fn new(
        inner: Arc<dyn MetadataEventLog>,
        partition: i32,
        key: Bytes,
        event: Bytes,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            racing: StdMutex::new(Some((partition, key, event))),
        })
    }
}

#[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic, keyed)]
#[async_trait]
impl MetadataEventLog for RacingAppendLog {
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let racing = self
            .racing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((partition, key, event)) = racing {
            // `subscribe` is synchronous, so this drives the append to
            // completion in one poll. The in-process fixture's `publish_keyed`
            // never yields, so it always finishes on the first.
            self.inner
                .publish_keyed(partition, key, Some(event))
                .now_or_never()
                .expect("the in-process fixture publishes without yielding")
                .expect("racing append");
        }
        self.inner.subscribe(assignment)
    }
}

/// One input WAL range; timestamps and byte spans vary only where a case needs them.
#[derive(Clone, Copy)]
pub(crate) struct WalFlushSetup<'a> {
    pub topic_id: uuid::Uuid,
    pub object_key: &'a str,
    pub offsets: (i64, i64, i64),
    pub byte_len: u32,
}

impl Default for WalFlushSetup<'_> {
    fn default() -> Self {
        Self {
            topic_id: uuid::Uuid::from_u128(7),
            object_key: "object-a",
            offsets: (0, 3, 0),
            byte_len: 10,
        }
    }
}

pub(crate) fn flush_record(setup: WalFlushSetup<'_>) -> crate::diskless::wal_index::WalFlushRecord {
    use crate::diskless::wal_index::{WalFlushRecord, WalIndexEntry};
    let WalFlushSetup {
        topic_id,
        object_key,
        offsets: (first_offset, last_offset, max_timestamp_ms),
        byte_len,
    } = setup;
    WalFlushRecord {
        object_key: object_key.into(),
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries: vec![WalIndexEntry {
            topic_id,
            partition: 0,
            first_offset,
            last_offset,
            byte_start: 0,
            byte_len,
            max_timestamp_ms,
        }],
    }
}
