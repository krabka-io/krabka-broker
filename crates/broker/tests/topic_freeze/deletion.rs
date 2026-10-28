//! The two paths that take data out of a topic, which a freeze also refuses.
//!
//! An operator who froze a topic for a disaster-recovery promotion needs the
//! frozen prefix byte-identical between sites, and both `DeleteRecords` and
//! `DeleteTopics` destroy exactly that. A freeze that stopped new writes and
//! let a trim through would be worse than no freeze at all.

use {
    crate::{
        control_plane::freeze_scope,
        support::{
            InProcess,
            offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        },
        wire::{CONTROL, controlled_fixture, produce},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::freeze::PATTERN_TYPE_LITERAL,
        owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

/// Trim `topic` up to `offset`, and hand back the partition row's error code.
async fn delete_records(client: &Client, topic: &str, offset: i64) -> i16 {
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(topic, vec![delete_records_partition(
                0, offset,
            )])],
            5_000,
        ))
        .await
        .expect("DeleteRecords");
    response.topics[0].partitions[0].error_code
}

/// Delete `topic`, and hand back the topic row's error code.
async fn delete_topic(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.to_owned()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    response.responses[0].error_code
}

#[derive(Default)]
struct FrozenDeletion {
    broker: Option<InProcess>,
    error: Option<i16>,
}

impl FrozenDeletion {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the frozen-topic broker is running")
    }
}

#[fixture]
fn deletion() -> FrozenDeletion {
    // Each scenario owns fresh services and observations.
    FrozenDeletion {
        broker: None,
        error: None,
    }
}

#[given("orders is frozen after two records and control also contains two records")]
async fn frozen_prefix(deletion: &mut FrozenDeletion) {
    let (broker, frozen, control) = Box::pin(controlled_fixture("orders")).await;
    for _ in 0..2 {
        check!(produce(&broker.client, "orders", frozen).await.error_code == codes::NONE);
        check!(produce(&broker.client, CONTROL, control).await.error_code == codes::NONE);
    }
    freeze_scope(&broker.client, PATTERN_TYPE_LITERAL, "orders", "promotion").await;
    deletion.broker = Some(broker);
}

#[when("the administrator trims {topic} through offset one")]
async fn trim_topic(deletion: &mut FrozenDeletion, topic: String) {
    deletion.error = Some(delete_records(&deletion.broker().client, &topic, 1).await);
}

#[when("the administrator deletes topic {topic}")]
async fn remove_topic(deletion: &mut FrozenDeletion, topic: String) {
    deletion.error = Some(delete_topic(&deletion.broker().client, &topic).await);
}

#[then("the frozen trim is refused and orders still spans offsets zero through two")]
fn frozen_prefix_unchanged(deletion: &FrozenDeletion) {
    check!(deletion.error == Some(codes::POLICY_VIOLATION));
    check!(
        deletion
            .broker()
            .broker
            .partition_log_start_for_test("orders", 0)
            == Some(0)
    );
    check!(deletion.broker().broker.local_log_end_offset("orders", 0) == Some(2));
}

#[then("the frozen deletion is refused and orders still exists")]
fn frozen_topic_unchanged(deletion: &FrozenDeletion) {
    check!(deletion.error == Some(codes::POLICY_VIOLATION));
    check!(
        deletion
            .broker()
            .broker
            .partition_exists_for_test("orders", 0)
    );
}

#[then("the control topic deletion operation succeeds")]
fn control_deletion_allowed(deletion: &FrozenDeletion) {
    check!(deletion.error == Some(codes::NONE));
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/deletion.feature",
    name = "delete records and delete topics refuse on a frozen topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_and_delete_topics_refuse_on_a_frozen_topic(deletion: FrozenDeletion) {
    deletion
        .broker
        .expect("the frozen-topic broker is running")
        .broker
        .shutdown()
        .await;
}
