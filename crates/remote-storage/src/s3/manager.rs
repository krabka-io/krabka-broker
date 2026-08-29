//! The [`RemoteStorageManager`] impl that binds the copy, fetch, and delete
//! modules to the KIP-405 SPI.
//!
//! A trait impl cannot be split across files, so this module holds the whole
//! block and nothing else. Each method carries the tracing span for the
//! operation and hands the work to the inherent method that implements it.

use tracing::instrument;

use super::S3RemoteStorage;
use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
};

impl RemoteStorageManager for S3RemoteStorage {
    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_offset = metadata.start_offset(),
            end_offset = metadata.end_offset(),
        ),
        err
    )]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        self.copy_segment_objects(metadata, data)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_position,
            end_position = ?end_position,
        ),
        err
    )]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.fetch_segment_range(metadata, start_position, end_position)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            index_type = ?index_type,
        ),
        err
    )]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.fetch_index_bytes(metadata, index_type)
    }

    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
        ),
        err
    )]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.delete_segment_objects(metadata)
    }
}
