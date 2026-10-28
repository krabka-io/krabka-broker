//! KIP-516: `DeleteTopics` by `topic_id` error semantics.
#![cfg(test)]
use assert2::assert;
mod support;

use krabka_protocol::{
    owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    primitives::uuid::Uuid as WireUuid,
};

#[cfg(test)]
#[rstest_bdd_macros::then(
    "kip516 delete topics: delete topics by unknown id returns unknown topic id"
)]
async fn bdd_verify_delete_topics_by_unknown_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xc0ff_ee00).into_bytes());

    let resp = p
        .client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: None,
                topic_id: bogus,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("delete topics");

    let r = resp.responses.first().expect("one response row");
    assert!(r.error_code == 100); // UNKNOWN_TOPIC_ID
    assert!(r.topic_id == bogus); // requested id echoed
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kip516_delete_topics.feature",
    name = "delete topics by unknown id returns unknown topic id"
)]
#[tokio::test]
async fn delete_topics_by_unknown_id_returns_unknown_topic_id() {}
