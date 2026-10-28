//! `CreateTopics` and `DeleteTopics` over one broker.
//!
//! The suite covers the admin round trip and the three definitions the broker
//! refuses: no partition, a name that already exists, and a replication factor
//! above the number of brokers.

use {
    crate::support::{
        self,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    },
};

#[cfg(test)]
#[rstest_bdd_macros::then("unit topic admin: create then delete topic round trip")]
async fn bdd_verify_create_then_delete_topic_round_trip() {
    let p = support::start().await;

    let create = create_topic_request(
        creatable_topic()
            .name(("alpha").into())
            .partitions(2)
            .call(),
    );
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].error_code == 0);
    check!(resp.topics[0].num_partitions == 2);

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("alpha".into()),
            ..Default::default()
        }],
        topic_names: vec!["alpha".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let dresp = p.client.send(delete).await.expect("DeleteTopics");
    assert!(dresp.responses.len() == 1);
    assert!(dresp.responses[0].error_code == 0);

    p.broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create then delete topic round trip"
)]
#[tokio::test]
async fn create_then_delete_topic_round_trip() {}

#[cfg(test)]
#[rstest_bdd_macros::then("unit topic admin: create topic with zero partitions errors")]
async fn bdd_verify_create_topic_with_zero_partitions_errors() {
    let p = support::start().await;
    let create = create_topic_request(creatable_topic().name(("zero").into()).partitions(0).call());
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(resp.topics[0].error_code == 37); // INVALID_PARTITIONS
    p.broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create topic with zero partitions errors"
)]
#[tokio::test]
async fn create_topic_with_zero_partitions_errors() {}

#[cfg(test)]
#[rstest_bdd_macros::then("unit topic admin: duplicate create returns topic already exists")]
async fn bdd_verify_duplicate_create_returns_topic_already_exists() {
    let p = support::start().await;
    let req = || create_topic_request(creatable_topic().name(("dup").into()).call());
    let r1 = p.client.send(req()).await.expect("CreateTopics 1");
    assert!(r1.topics[0].error_code == 0);
    let r2 = p.client.send(req()).await.expect("CreateTopics 2");
    assert!(r2.topics[0].error_code == 36); // TOPIC_ALREADY_EXISTS
    p.broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "duplicate create returns topic already exists"
)]
#[tokio::test]
async fn duplicate_create_returns_topic_already_exists() {}

#[cfg(test)]
#[rstest_bdd_macros::then(
    "unit topic admin: create topics rf too high returns invalid replication factor"
)]
async fn bdd_verify_create_topics_rf_too_high_returns_invalid_replication_factor() {
    let p = support::start().await; // single-voter broker
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "boom".into(),
                num_partitions: 1,
                replication_factor: 5, // single broker → RF=5 is invalid
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 38 /* INVALID_REPLICATION_FACTOR */
    );
    p.broker.shutdown().await;
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create topics rf too high returns invalid replication factor"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_rf_too_high_returns_invalid_replication_factor() {}
