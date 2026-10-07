//! Fetch fixtures with explicit limits and caller-owned session and replica fields.

use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    primitives::uuid::Uuid,
};

pub fn fetch_partition(
    partition: i32,
    fetch_offset: i64,
    partition_max_bytes: i32,
) -> FetchPartition {
    FetchPartition {
        partition,
        fetch_offset,
        partition_max_bytes,
        ..Default::default()
    }
}

pub fn single_partition_fetch(
    topic: impl Into<String>,
    topic_id: Uuid,
    partition: FetchPartition,
    (max_wait_ms, min_bytes, max_bytes): (i32, i32, i32),
) -> FetchRequest {
    fetch_request_for(
        vec![fetch_topic_row(topic, topic_id, vec![partition])],
        (max_wait_ms, min_bytes, max_bytes),
    )
}

pub fn fetch_topic_row(
    topic: impl Into<String>,
    topic_id: Uuid,
    partitions: Vec<FetchPartition>,
) -> FetchTopic {
    FetchTopic {
        topic: topic.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

pub fn fetch_request_for(
    topics: Vec<FetchTopic>,
    (max_wait_ms, min_bytes, max_bytes): (i32, i32, i32),
) -> FetchRequest {
    FetchRequest {
        max_wait_ms,
        min_bytes,
        max_bytes,
        topics,
        ..Default::default()
    }
}

/// The ordinary Fetch limits, for cases that override only session membership.
pub fn default_fetch_limits() -> (i32, i32, i32) {
    let request = FetchRequest::default();
    (request.max_wait_ms, request.min_bytes, request.max_bytes)
}

pub fn session_fetch_request(
    (session_id, session_epoch): (i32, i32),
    forgotten_topics_data: Vec<krabka_protocol::owned::fetch_request::ForgottenTopic>,
    request: FetchRequest,
) -> FetchRequest {
    FetchRequest {
        session_id,
        session_epoch,
        forgotten_topics_data,
        ..request
    }
}
