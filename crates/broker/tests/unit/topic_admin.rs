//! Topic creation, duplicate detection, and deletion through Kafka administration.

use {
    crate::support::{
        self, InProcess,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        create_topics_request::CreatableTopic,
        create_topics_response::CreateTopicsResponse,
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        delete_topics_response::DeleteTopicsResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct TopicAdministration {
    broker: Option<InProcess>,
    definition: Option<CreatableTopic>,
    created: Option<CreateTopicsResponse>,
    deleted: Option<DeleteTopicsResponse>,
}

impl TopicAdministration {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the administration broker is running")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the administration broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn administration() -> TopicAdministration {
    // Each scenario owns fresh services and observations.
    TopicAdministration {
        broker: None,
        ..Default::default()
    }
}

#[given("a single broker for topic administration")]
async fn administration_broker(administration: &mut TopicAdministration) {
    administration.broker = Some(support::start().await);
}

#[given("topic definition {name} has {partitions} partitions and replication factor {replicas}")]
fn topic_definition(
    administration: &mut TopicAdministration,
    name: String,
    partitions: i32,
    replicas: i16,
) {
    administration.definition = Some(
        creatable_topic()
            .name(name)
            .partitions(partitions)
            .replicas(replicas)
            .call(),
    );
}

#[given("the topic definition already exists")]
async fn existing_topic(administration: &TopicAdministration) {
    let response = administration
        .broker()
        .client
        .send(create_topic_request(
            administration
                .definition
                .clone()
                .expect("a topic is defined"),
        ))
        .await
        .expect("initial CreateTopics");
    assert!(response.topics[0].error_code == 0);
}

#[when("the administrator creates the defined topic")]
async fn create_defined_topic(administration: &mut TopicAdministration) {
    let request = create_topic_request(
        administration
            .definition
            .clone()
            .expect("a topic is defined"),
    );
    administration.created = Some(
        administration
            .broker()
            .client
            .send(request)
            .await
            .expect("CreateTopics"),
    );
}

#[then("creation returns one topic with {partitions} partitions")]
fn created_partitions(administration: &TopicAdministration, partitions: i32) {
    let response = administration
        .created
        .as_ref()
        .expect("topic creation completed");
    assert!(response.topics.len() == 1);
    check!(response.topics[0].error_code == 0);
    check!(response.topics[0].num_partitions == partitions);
}

#[then("topic creation returns Kafka error {code}")]
fn creation_error(administration: &TopicAdministration, code: i16) {
    let response = administration
        .created
        .as_ref()
        .expect("topic creation completed");
    assert!(response.topics[0].error_code == code);
}

#[when("the administrator deletes the defined topic")]
async fn delete_defined_topic(administration: &mut TopicAdministration) {
    let name = administration
        .definition
        .as_ref()
        .expect("a topic is defined")
        .name
        .clone();
    administration.deleted = Some(
        administration
            .broker()
            .client
            .send(DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: Some(name.clone()),
                    ..Default::default()
                }],
                topic_names: vec![name],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("DeleteTopics"),
    );
}

#[then("deletion returns one successful topic response")]
fn deletion_succeeds(administration: &TopicAdministration) {
    let response = administration
        .deleted
        .as_ref()
        .expect("topic deletion completed");
    assert!(response.responses.len() == 1);
    assert!(response.responses[0].error_code == 0);
}

#[scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create then delete topic round trip"
)]
#[tokio::test]
async fn create_then_delete_topic_round_trip(administration: TopicAdministration) {
    administration.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create topic with zero partitions errors"
)]
#[tokio::test]
async fn create_topic_with_zero_partitions_errors(administration: TopicAdministration) {
    administration.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "duplicate create returns topic already exists"
)]
#[tokio::test]
async fn duplicate_create_returns_topic_already_exists(administration: TopicAdministration) {
    administration.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/topic_admin.feature",
    name = "create topics rf too high returns invalid replication factor"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_rf_too_high_returns_invalid_replication_factor(
    administration: TopicAdministration,
) {
    administration.shutdown().await;
}
