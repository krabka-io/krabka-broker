//! Pure request rows for offset lookup, commit and group offset fetch.

use krabka_protocol::{
    owned::{
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_delete_request::{
            OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
    },
    primitives::uuid::Uuid,
};

pub fn list_offset_partition(partition_index: i32, timestamp: i64) -> ListOffsetsPartition {
    ListOffsetsPartition {
        partition_index,
        timestamp,
        ..Default::default()
    }
}

pub fn single_partition_list_offsets(
    name: impl Into<String>,
    partition: ListOffsetsPartition,
) -> ListOffsetsRequest {
    ListOffsetsRequest {
        topics: vec![ListOffsetsTopic {
            name: name.into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn offset_commit_partition(
    partition_index: i32,
    committed_offset: i64,
    committed_metadata: Option<String>,
) -> OffsetCommitRequestPartition {
    OffsetCommitRequestPartition {
        partition_index,
        committed_offset,
        committed_metadata,
        ..Default::default()
    }
}

pub fn offset_commit_topic(
    name: impl Into<String>,
    topic_id: Uuid,
    partitions: Vec<OffsetCommitRequestPartition>,
) -> OffsetCommitRequestTopic {
    OffsetCommitRequestTopic {
        name: name.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

pub fn offset_fetch_topic(
    name: impl Into<String>,
    topic_id: Uuid,
    partition_indexes: Vec<i32>,
) -> OffsetFetchRequestTopics {
    OffsetFetchRequestTopics {
        name: name.into(),
        topic_id,
        partition_indexes,
        ..Default::default()
    }
}

pub fn offset_fetch_group(
    group_id: impl Into<String>,
    topics: Option<Vec<OffsetFetchRequestTopics>>,
) -> OffsetFetchRequestGroup {
    OffsetFetchRequestGroup {
        group_id: group_id.into(),
        topics,
        ..Default::default()
    }
}

pub fn offset_fetch_request(group: OffsetFetchRequestGroup) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![group],
        ..Default::default()
    }
}

pub fn offset_delete_partition(partition_index: i32) -> OffsetDeleteRequestPartition {
    OffsetDeleteRequestPartition {
        partition_index,
        ..Default::default()
    }
}

pub fn offset_delete_topic(
    name: impl Into<String>,
    partitions: Vec<OffsetDeleteRequestPartition>,
) -> OffsetDeleteRequestTopic {
    OffsetDeleteRequestTopic {
        name: name.into(),
        partitions,
        ..Default::default()
    }
}

pub fn offset_delete_request(
    group_id: impl Into<String>,
    topics: Vec<OffsetDeleteRequestTopic>,
) -> OffsetDeleteRequest {
    OffsetDeleteRequest {
        group_id: group_id.into(),
        topics,
        ..Default::default()
    }
}

pub fn delete_records_partition(partition_index: i32, offset: i64) -> DeleteRecordsPartition {
    DeleteRecordsPartition {
        partition_index,
        offset,
        ..Default::default()
    }
}

pub fn delete_records_topic(
    name: impl Into<String>,
    partitions: Vec<DeleteRecordsPartition>,
) -> DeleteRecordsTopic {
    DeleteRecordsTopic {
        name: name.into(),
        partitions,
        ..Default::default()
    }
}

pub fn delete_records_request(
    topics: Vec<DeleteRecordsTopic>,
    timeout_ms: i32,
) -> DeleteRecordsRequest {
    DeleteRecordsRequest {
        topics,
        timeout_ms,
        ..Default::default()
    }
}
