//! KIP-516: `DeleteTopics` by `topic_id` error semantics.
#![cfg(test)]
mod support;

use {
    assert2::assert,
    krabka_protocol::{
        owned::{
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::DeleteTopicsResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};

struct DeleteById {
    broker: Option<InProcess>,
    requested_id: WireUuid,
    response: Option<DeleteTopicsResponse>,
}

#[fixture]
fn deletion() -> DeleteById {
    // Start this scenario with fresh inputs and observations.

    DeleteById {
        broker: None,
        requested_id: WireUuid::ZERO,
        response: None,
    }
}

#[given("a broker with no topic matching the deletion id")]
async fn unknown_id(deletion: &mut DeleteById) {
    deletion.broker = Some(support::start().await);
    deletion.requested_id = WireUuid(uuid::Uuid::from_u128(0xc0ff_ee00).into_bytes());
}

#[when("that topic id is deleted without supplying a name")]
async fn delete_by_id(deletion: &mut DeleteById) {
    let broker = deletion.broker.as_ref().expect("a broker was started");
    deletion.response = Some(
        broker
            .client
            .send(DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: None,
                    topic_id: deletion.requested_id,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("delete topics"),
    );
}

#[then("deletion refuses the unknown topic id and echoes it")]
fn refused_id(deletion: &DeleteById) {
    let response = deletion.response.as_ref().expect("deletion was requested");
    let row = response.responses.first().expect("one response row");
    assert!(row.error_code == 100);
    assert!(row.topic_id == deletion.requested_id);
}

#[scenario(
    path = "tests/features/migrated/tests/kip516_topic_identity.feature",
    name = "Deleting an unknown topic id preserves its identity in the error"
)]
#[tokio::test]
async fn delete_topics_by_unknown_id_returns_unknown_topic_id(deletion: DeleteById) {
    deletion
        .broker
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}
