//! The [`RemoteLogMetadataManager`] SPI, which stores and queries
//! remote-segment metadata.

use {
    crate::{
        error::RemoteStorageError,
        metadata::{
            RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
            RemotePartitionDeleteMetadata, TopicIdPartition,
        },
    },
    krabka_ids::LeaderEpoch,
};

/// SPI for the store that holds metadata about remote log segments.
///
/// The reference implementation is
/// [`InmemoryRemoteLogMetadataManager`](crate::InmemoryRemoteLogMetadataManager);
/// a production implementation stores the metadata in an internal topic, as
/// Kafka's `TopicBasedRemoteLogMetadataManager` does. Implementations must be
/// `Send + Sync`.
///
/// The lifecycle invariants every implementation enforces:
///
/// - The caller introduces a segment with
///   [`add_remote_log_segment_metadata`](RemoteLogMetadataManager::add_remote_log_segment_metadata)
///   in state
///   [`CopySegmentStarted`](crate::RemoteLogSegmentState::CopySegmentStarted).
/// - Every later change is an
///   [`update_remote_log_segment_metadata`](RemoteLogMetadataManager::update_remote_log_segment_metadata)
///   that follows a valid state transition.
/// - Only
///   [`CopySegmentFinished`](crate::RemoteLogSegmentState::CopySegmentFinished)
///   segments are visible to the offset/epoch queries.
pub trait RemoteLogMetadataManager: Send + Sync {
    /// Records a newly-started segment copy. The metadata's state must be
    /// [`CopySegmentStarted`](crate::RemoteLogSegmentState::CopySegmentStarted)
    /// and its id must not already be known.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidAdd`] if the starting state is
    /// wrong or the segment id is a duplicate.
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;

    /// Advances a known segment's lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment is
    /// unknown, or [`RemoteStorageError::InvalidSegmentTransition`] if the
    /// transition is not permitted.
    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError>;

    /// Finds the finished segment that contains `offset` for the given
    /// `leader_epoch`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure;
    /// a simple "no such segment" is `Ok(None)`.
    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// The highest offset that the remote tier holds for `leader_epoch`.
    ///
    /// This is the maximum `end_offset` across the finished segments that
    /// carry that epoch.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError>;

    /// All segments known for a partition, in `start_offset` order. The
    /// state does not matter.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// All segments that carry `leader_epoch`, in `start_offset` order. The
    /// state does not matter.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// Records a partition-delete lifecycle event.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidPartitionDeleteTransition`] if
    /// the transition is not permitted from the partition's current
    /// delete-state.
    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError>;

    /// Whether this manager has applied every metadata event for
    /// `topic_id_partition` that the metadata store had committed when the
    /// call began.
    ///
    /// This is a read-your-peers'-writes check. Another broker can publish
    /// an event, for example a `DeleteSegmentStarted` update, and wait for
    /// the store to acknowledge it. A read on this manager that follows a
    /// `true` answer sees that event. A `false` answer means the local view
    /// can be stale, so the caller must not judge from it. A manager that
    /// does not consume the partition's metadata answers `false`, because
    /// it cannot vouch for the view.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::NotReady`] while the manager is still
    /// catching up after it took on the partition, and
    /// [`RemoteStorageError::Io`] if it cannot read the store's committed
    /// position.
    fn caught_up(&self, topic_id_partition: &TopicIdPartition) -> Result<bool, RemoteStorageError>;
}
