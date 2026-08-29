//! The request helpers that the broker unit suites share.
//!
//! Creating a topic and resolving its id are two round trips that most of the
//! suites need before they can drive the behaviour they test, so both live
//! here instead of once per module.

use assert2::assert;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid,
};

use crate::support::InProcess;

pub async fn create_topic(p: &InProcess, name: &str, num_partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "CreateTopics for {name}");
}

/// Resolves a topic's UUID through a Metadata round trip.
///
/// Produce v ≥ 13 sends only `topic_id` on the wire, so tests need this
/// helper to drive the broker with a non-zero UUID.
pub async fn topic_id_for(p: &InProcess, name: &str) -> Uuid {
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}
