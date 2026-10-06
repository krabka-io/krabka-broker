//! The request helpers that the broker unit suites share.
//!
//! Creating a topic and resolving its id are two round trips that most of the
//! suites need before they can drive the behaviour they test, so both live
//! here instead of once per module.

use assert2::assert;
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

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

pub use crate::support::topic_id_for;
