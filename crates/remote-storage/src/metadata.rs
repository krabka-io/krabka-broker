//! The data model exchanged across the two tiered-storage SPIs.
//!
//! Shapes mirror Apache Kafka's `storage-api`
//! (`org.apache.kafka.server.log.remote.storage`): [`TopicIdPartition`],
//! [`RemoteLogSegmentId`], [`RemoteLogSegmentMetadata`] +
//! [`RemoteLogSegmentMetadataUpdate`], the [`RemoteLogSegmentState`]
//! lifecycle, and the partition-delete lifecycle
//! ([`RemotePartitionDeleteMetadata`] / [`RemotePartitionDeleteState`]).

mod partition_delete;
mod segment_id;
mod segment_metadata;
mod segment_state;

pub use self::{
    partition_delete::{RemotePartitionDeleteMetadata, RemotePartitionDeleteState},
    segment_id::{RemoteLogSegmentId, TopicIdPartition},
    segment_metadata::{
        CustomMetadata, RemoteLogSegmentDetails, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate,
    },
    segment_state::RemoteLogSegmentState,
};
