//! [`MetadataEventLog`]: the publish/subscribe seam between the
//! [`TopicBasedRemoteLogMetadataManager`](crate::TopicBasedRemoteLogMetadataManager)
//! and the underlying durable event store.
//!
//! The in-process implementation, [`InProcessMetadataEventLog`], is an
//! in-memory broadcast-channel fixture that unit tests use. It is also a
//! single-process model for the multi-broker case, because multiple manager
//! instances that share the same fixture observe each other's writes. The
//! production Kafka-backed adapter implements the same trait.
//!
//! [`MetadataEventLog::subscribe`] does not consume every partition from
//! offset 0. It takes an explicit [`PartitionStart`] assignment, which is a
//! subset of partitions, each with its own start offset. It returns an
//! [`AssignmentHandle`] that can mutate the live assignment at runtime.

use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::Stream;

use crate::error::MetadataLogError;

mod in_process;

pub use self::in_process::InProcessMetadataEventLog;

/// One event read from the metadata log.
#[derive(Debug, Clone)]
pub struct MetadataEventRecord {
    /// Metadata-topic partition the event came from.
    pub partition: i32,
    /// Offset within that partition.
    pub offset: i64,
    /// Encoded event payload. See [`crate::serde`].
    pub payload: Bytes,
}

/// Boxed event stream the [`MetadataEventLog`] hands to subscribers.
pub type MetadataEventStream = Pin<Box<dyn Stream<Item = MetadataEventRecord> + Send + 'static>>;

/// One partition to consume and the offset to begin at (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStart {
    /// Metadata-topic partition to consume.
    pub partition: i32,
    /// First offset to deliver (inclusive). `0` replays from the start.
    pub start_offset: i64,
}

/// Runtime control over a live [`MetadataEventLog`] subscription's assigned
/// partition set. [`MetadataEventLog::subscribe`] returns it together with
/// the stream.
pub trait AssignmentHandle: Send + Sync {
    /// Begin to consume `start.partition` from `start.start_offset`. This
    /// method does nothing if the partition is already assigned. A
    /// newly-added partition emits its backlog from `start_offset` into the
    /// existing stream, and then live records.
    fn add(&self, start: PartitionStart);
    /// Stop the consumption of `partition` and stop the emission of its
    /// events. This method does nothing if the partition is not currently
    /// assigned.
    fn remove(&self, partition: i32);
    /// Current assigned partition set (unordered).
    fn assigned(&self) -> Vec<i32>;
}

/// Publish/subscribe transport that backs the `__remote_log_metadata`
/// topic.
///
/// Implementations must guarantee:
///
/// - `publish(p, _)` resolves to a monotonically-increasing offset
///   within partition `p`, and the assigned offset is also what every
///   subscriber observes for that record.
/// - The stream returned by `subscribe` replays each assigned
///   partition's backlog from its `start_offset` and then forwards new
///   records as they are published for currently-assigned partitions.
///   Subscribers attached after some records were already published
///   still see the history at/after their start offset.
/// - Records are delivered in publish order on a per-partition basis.
#[async_trait]
pub trait MetadataEventLog: Send + Sync {
    /// Number of partitions the log holds. It is stable for the lifetime of
    /// the log. The manager hashes user partitions into
    /// `[0, partition_count())`.
    fn partition_count(&self) -> i32;

    /// Append `event` to `partition`. Resolves to the assigned offset.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] if the transport refused the write. That
    /// happens when the partition is out of range, or when the log has been
    /// closed.
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError>;

    /// Start to consume the given partitions, each from its start offset,
    /// which is inclusive. Returns the event stream and a handle to mutate
    /// the live assignment.
    ///
    /// The stream replays each assigned partition's backlog from its
    /// `start_offset`, then forwards live appends for the currently
    /// assigned partitions. Records are delivered in publish order on
    /// a per-partition basis.
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>);

    /// One past the highest written offset for each partition,
    /// indexed by partition.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] only on an underlying store failure. An
    /// empty partition is `0`, not an error.
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;
}
