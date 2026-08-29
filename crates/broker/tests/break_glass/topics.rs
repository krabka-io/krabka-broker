//! The ordinary Kafka topic requests the cases set a cluster up with and read
//! it back through.
//!
//! `delete_topic` is both of those and a gated transition in its own right, so
//! it answers the row's error code instead of asserting on it. The gate cases
//! need the refusal, and the setup cases assert on the code themselves.

use assert2::assert;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
};

/// Create `name` with one partition and the given replication factor.
pub(super) async fn create_topic(client: &Client, name: &str, replication_factor: i16) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_owned(),
                num_partitions: 1,
                replication_factor,
                ..CreatableTopic::default()
            }],
            timeout_ms: 10_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    let code = response.topics.first().map(|topic| topic.error_code);
    assert!(code == Some(codes::NONE), "create {name}: {response:?}");
}

/// Delete `name`, and answer the row's error code.
pub(super) async fn delete_topic(client: &Client, name: &str) -> i16 {
    let response = client
        .send(DeleteTopicsRequest {
            // The encoder writes `topics` at version 6 and later and
            // `topic_names` below it, so filling both leaves the negotiated
            // version to pick.
            topics: vec![DeleteTopicState {
                name: Some(name.to_owned()),
                ..DeleteTopicState::default()
            }],
            topic_names: vec![name.to_owned()],
            timeout_ms: 10_000,
            ..DeleteTopicsRequest::default()
        })
        .await
        .expect("DeleteTopics");
    response
        .responses
        .first()
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code)
}

/// Whether `client`'s cluster still knows `name`.
pub(super) async fn topic_exists(client: &Client, name: &str) -> bool {
    client
        .send(krabka_protocol::owned::metadata_request::MetadataRequest::default())
        .await
        .expect("Metadata")
        .topics
        .iter()
        .any(|topic| topic.name.as_deref() == Some(name) && topic.error_code == codes::NONE)
}
