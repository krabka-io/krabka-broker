//! In-process single-voter Controller. It validates the wiring of openraft,
//! `log_store`, `state_machine`, and the listener, and it needs no 3-node
//! cluster.
#![cfg(test)]

use {
    krabka_metadata::{MetadataError, MetadataRecord, NodeId},
    krabka_raft::{Controller, ControllerConfig, ControllerHandle, RaftError, SubmitChangeResult},
    krabka_units::prelude::{Time, millis},
    std::time::Duration,
    tempfile::TempDir,
    tokio::time,
    uuid::Uuid,
};

/// Single-voter elections are instant, and a short timeout keeps each test well
/// inside its 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

krabka_macros::topic_record_fixture!(single_partition_topic);

/// A single-voter controller configuration and the data directory it points
/// at, which must outlive the controller.
struct SingleVoterConfig {
    dir: TempDir,
    config: ControllerConfig,
}

fn single_voter_config() -> SingleVoterConfig {
    let dir = TempDir::new().unwrap();
    let mut config = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    config.election_timeout = FAST_ELECTION_TIMEOUT;
    // Pin the controller listen addr to a real loopback port so the network
    // factory has something to dial when initialize wants to seed members.
    config.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    SingleVoterConfig { dir, config }
}

async fn wait_for_leader(controller: &ControllerHandle) {
    let mut receiver = controller.watch_leader();
    time::timeout(Duration::from_secs(30), receiver.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");
}

use {
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

struct SingleVoterTopic {
    directory: Option<TempDir>,
    controller: Option<ControllerHandle>,
    topic: Option<MetadataRecord>,
    name: Option<String>,
    submission: Option<Result<SubmitChangeResult, RaftError>>,
}

#[fixture]
fn node() -> SingleVoterTopic {
    // Start this scenario with fresh inputs and observations.

    SingleVoterTopic {
        directory: None,
        controller: None,
        topic: None,
        name: None,
        submission: None,
    }
}

#[given("a fresh single-voter controller ready to create topic {name}")]
async fn fresh_controller(node: &mut SingleVoterTopic, name: String) {
    let SingleVoterConfig { dir, config } = single_voter_config();
    let controller = Controller::start(config).await.expect("controller start");
    wait_for_leader(&controller).await;
    node.directory = Some(dir);
    node.controller = Some(controller);
    node.topic = Some(MetadataRecord::V1Topic(single_partition_topic(
        &name,
        Uuid::new_v4(),
    )));
    node.name = Some(name);
}

#[given("that topic is already committed in the single-voter metadata log")]
async fn existing_topic(node: &mut SingleVoterTopic) {
    let topic = node
        .topic
        .as_ref()
        .expect("a topic request was given")
        .clone();
    node.controller
        .as_ref()
        .expect("a controller was started")
        .submit_change(vec![topic])
        .await
        .expect("commit the existing topic");
}

#[when("the single-voter controller is asked to create that topic")]
async fn create_topic(node: &mut SingleVoterTopic) {
    let topic = node
        .topic
        .as_ref()
        .expect("a topic request was given")
        .clone();
    node.submission = Some(
        node.controller
            .as_ref()
            .expect("a controller was started")
            .submit_change(vec![topic])
            .await,
    );
}

#[then("the topic creation succeeds and appears in the controller metadata image")]
fn created_topic(node: &SingleVoterTopic) {
    assert!(
        node.submission
            .as_ref()
            .expect("creation was requested")
            .is_ok()
    );
    assert!(
        node.controller
            .as_ref()
            .expect("a controller was started")
            .current_image()
            .topic(node.name.as_deref().expect("a topic name was given"))
            .is_some()
    );
}

#[then("the duplicate topic creation fails with TopicExists")]
fn duplicate_topic(node: &SingleVoterTopic) {
    assert!(matches!(
        node.submission.as_ref().expect("creation was requested"),
        Err(RaftError::Metadata(MetadataError::TopicExists(_)))
    ));
}

async fn stop_controller(mut node: SingleVoterTopic) {
    if let Some(controller) = node.controller.take() {
        controller.shutdown().await;
    }
}

// Explicit bindings preserve the original two-worker Tokio runtimes and test identities.
#[scenario(
    path = "tests/features/migrated/tests/single_node.feature",
    name = "A single-voter leader commits a new topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_create_topic_round_trip(node: SingleVoterTopic) {
    stop_controller(node).await;
}

#[scenario(
    path = "tests/features/migrated/tests/single_node.feature",
    name = "A single-voter leader refuses a duplicate topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_duplicate_topic_rejected(node: SingleVoterTopic) {
    stop_controller(node).await;
}
