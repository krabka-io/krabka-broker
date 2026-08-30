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
    AssignmentHandle, MetadataEventLog, MetadataEventStream, MetadataLogError, PartitionStart,
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

#[async_trait]
impl MetadataEventLog for PacedReplayLog {
    fn partition_count(&self) -> i32 {
        self.inner.partition_count()
    }

    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.inner.publish(partition, event).await
    }

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

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        self.inner.high_water_marks().await
    }
}

/// Publishes one record from inside `subscribe`, modelling another broker's
/// in-flight flush landing exactly while a restarting projection establishes
/// its subscription. A watermark read *before* subscribing steps over that
/// record; one read after cannot.
pub(crate) struct RacingAppendLog {
    inner: Arc<dyn MetadataEventLog>,
    racing: StdMutex<Option<(i32, Bytes)>>,
}

impl RacingAppendLog {
    pub(crate) fn new(inner: Arc<dyn MetadataEventLog>, partition: i32, event: Bytes) -> Arc<Self> {
        Arc::new(Self {
            inner,
            racing: StdMutex::new(Some((partition, event))),
        })
    }
}

#[async_trait]
impl MetadataEventLog for RacingAppendLog {
    fn partition_count(&self) -> i32 {
        self.inner.partition_count()
    }

    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
        self.inner.publish(partition, event).await
    }

    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let racing = self
            .racing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((partition, event)) = racing {
            // `subscribe` is synchronous, so this drives the append to
            // completion in one poll. The in-process fixture's `publish`
            // never yields, so it always finishes on the first.
            self.inner
                .publish(partition, event)
                .now_or_never()
                .expect("the in-process fixture publishes without yielding")
                .expect("racing append");
        }
        self.inner.subscribe(assignment)
    }

    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        self.inner.high_water_marks().await
    }
}
