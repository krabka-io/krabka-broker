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

use {
    crate::error::MetadataLogError,
    async_trait::async_trait,
    bytes::Bytes,
    futures_util::stream::Stream,
    std::{pin::Pin, sync::Arc},
};

mod in_process;

pub use self::in_process::InProcessMetadataEventLog;

/// One event read from the metadata log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEventRecord {
    /// Metadata-topic partition the event came from.
    pub partition: i32,
    /// Offset within that partition.
    pub offset: i64,
    /// Record key. Compacted metadata topics use it to identify superseded
    /// state; ordinary append-only metadata events leave it unset.
    pub key: Option<Bytes>,
    /// Encoded event payload. See [`crate::serde`].
    pub payload: Bytes,
    /// Whether Kafka carried a null value for this key.
    pub tombstone: bool,
}

/// One keyed record of a [`MetadataEventLog::publish_keyed_group`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedEvent {
    /// Metadata-topic partition to append to.
    pub partition: i32,
    /// Compaction key.
    pub key: Bytes,
    /// Value, or `None` for a tombstone.
    pub event: Option<Bytes>,
}

/// Boxed event stream the [`MetadataEventLog`] hands to subscribers.
pub type MetadataEventStream = Pin<Box<dyn Stream<Item = MetadataEventRecord> + Send + 'static>>;

/// Receives each record of [`MetadataEventLog::visit_range`]. An error stops
/// the read, and the read returns it.
pub type RangeVisitor<'a> =
    dyn FnMut(MetadataEventRecord) -> Result<(), MetadataLogError> + Send + 'a;

/// One partition to consume and the offset to begin at (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStart {
    /// Metadata-topic partition to consume.
    pub partition: i32,
    /// First offset to deliver (inclusive). `0` replays from the start.
    pub start_offset: i64,
}

impl PartitionStart {
    /// Consume one partition beginning at the inclusive offset.
    #[must_use]
    pub const fn new(partition: i32, start_offset: i64) -> Self {
        Self {
            partition,
            start_offset,
        }
    }
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

    /// Append a keyed value or tombstone to a compacted metadata topic.
    ///
    /// The default rejects keyed publication. A transport used for compaction
    /// must override this method.
    async fn publish_keyed(
        &self,
        _partition: i32,
        _key: Bytes,
        _event: Option<Bytes>,
    ) -> Result<i64, MetadataLogError> {
        Err(MetadataLogError::Other(
            "metadata log does not support keyed publication".into(),
        ))
    }

    /// Append a group of keyed records and wait until every one of them is
    /// acknowledged. Resolves to their offsets, in the order given.
    ///
    /// Records for the same partition are appended in the order given. A
    /// transport that pipelines writes submits the whole group before it
    /// waits, so the group costs one acknowledgment round trip rather than
    /// one per record. It still waits for every submitted record before it
    /// returns, so none is left in flight after an error.
    ///
    /// The default publishes the records one at a time through
    /// [`Self::publish_keyed`] and stops at the first failure.
    ///
    /// # Errors
    ///
    /// Returns the first [`MetadataLogError`] that any record met. Any subset
    /// of the group may then have been appended.
    async fn publish_keyed_group(
        &self,
        events: Vec<KeyedEvent>,
    ) -> Result<Vec<i64>, MetadataLogError> {
        let mut offsets = Vec::with_capacity(events.len());
        for KeyedEvent {
            partition,
            key,
            event,
        } in events
        {
            offsets.push(self.publish_keyed(partition, key, event).await?);
        }
        Ok(offsets)
    }

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

    /// Earliest retained offset for each partition, indexed by partition.
    ///
    /// Append-only implementations retain offset zero. Compacted transports
    /// override this so a replay does not wait forever on deleted offsets.
    async fn low_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        Ok((0..self.partition_count()).map(|_| 0).collect())
    }

    /// One past the highest written offset for each partition,
    /// indexed by partition.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError`] only on an underlying store failure. An
    /// empty partition is `0`, not an error.
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;

    /// Pass every retained record of `partition` whose offset lies in
    /// `[start, end)` to `visit`, in offset order.
    ///
    /// The read neither subscribes nor writes, so a reader that holds only
    /// `READ` and `DESCRIBE` on the topic can use it. A transport hands
    /// records over as it fetches them and holds at most one fetched page, so
    /// the range can be larger than memory. A compacted transport steps over
    /// the offsets compaction removed and still returns once it reaches `end`.
    /// An empty range visits nothing.
    ///
    /// A networked transport retries transient failures, such as a leader
    /// change, without a bound of its own. Bound the read with a deadline.
    ///
    /// The default reports that the transport does not serve range reads.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataLogError::PartitionOutOfRange`] for a partition
    /// outside the log, the first error that `visit` returns, and
    /// [`MetadataLogError`] when the store fails or stops short of `end`.
    async fn visit_range(
        &self,
        partition: i32,
        start: i64,
        end: i64,
        visit: &mut RangeVisitor<'_>,
    ) -> Result<(), MetadataLogError> {
        let _ = (partition, start, end, visit);
        Err(MetadataLogError::Other(
            "metadata log does not serve range reads".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::sync::atomic::{AtomicUsize, Ordering},
    };

    struct AppendOnlyLog(AtomicUsize);

    #[async_trait]
    impl MetadataEventLog for AppendOnlyLog {
        fn partition_count(&self) -> i32 { 1 }

        async fn publish(&self, _partition: i32, _event: Bytes) -> Result<i64, MetadataLogError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(7)
        }

        fn subscribe(
            &self,
            _assignment: Vec<PartitionStart>,
        ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
            unreachable!("not used by these tests")
        }

        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> { Ok(vec![0]) }
    }

    #[tokio::test]
    async fn default_keyed_publish_rejects_values_and_tombstones_without_appending() {
        let log = AppendOnlyLog(AtomicUsize::new(0));
        assert2::assert!(log.partition_count() == 1);
        assert2::assert!(log.high_water_marks().await.unwrap() == vec![0]);
        assert2::assert!(
            log.publish(0, Bytes::from_static(b"append-only"))
                .await
                .unwrap()
                == 7
        );
        let value_error = log
            .publish_keyed(
                0,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
            )
            .await
            .unwrap_err();
        assert2::assert!(
            value_error
                .to_string()
                .contains("does not support keyed publication")
        );

        let error = log
            .publish_keyed(0, Bytes::from_static(b"key"), None)
            .await
            .unwrap_err();
        assert2::assert!(
            error
                .to_string()
                .contains("does not support keyed publication")
        );
        assert2::assert!(log.0.load(Ordering::Relaxed) == 1);
    }

    #[tokio::test]
    async fn default_range_read_refuses_without_visiting() {
        let mut visits = 0;
        let error = AppendOnlyLog(AtomicUsize::new(0))
            .visit_range(0, 0, 1, &mut |_record| {
                visits += 1;
                Ok(())
            })
            .await
            .unwrap_err();

        assert2::assert!(
            error.to_string() == "metadata log error: metadata log does not serve range reads"
        );
        assert2::assert!(visits == 0);
    }
}
