//! A broker restart preserves the next-gen group state, through
//! `__consumer_offsets` replay.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::{connect, connect_client, create_topic},
        consumer_groups::{consumer_heartbeat, joining_consumer},
        heartbeat_config,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration},
    tokio::time,
};

fn rejoin_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = heartbeat_config(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

/// A running broker and a client connected to it.
struct BrokerWithClient {
    broker: krabka_broker::BrokerHandle,
    client: Arc<Client>,
}

async fn boot(log_dir: PathBuf) -> BrokerWithClient {
    let broker = Broker::start(heartbeat_config(log_dir)).await.unwrap();
    broker.wait_until_broker_alive(broker.node_id()).await;
    broker.wait_until_group_coordinator_ready().await;
    let client = connect(&broker.listen_addr().to_string(), "c").await;
    BrokerWithClient { broker, client }
}

async fn restart(log_dir: PathBuf, wait_for_group: bool) -> BrokerWithClient {
    let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
    if wait_for_group {
        broker.wait_until_group_coordinator_ready().await;
    }
    let bootstrap = broker.listen_addr().to_string();
    let client = Arc::new(connect_client(bootstrap.as_str(), Some("c")).await);
    BrokerWithClient { broker, client }
}

#[derive(Default)]
struct PersistedConsumer {
    running: Option<BrokerWithClient>,
    group: String,
    member: String,
    epoch: i32,
    response: Option<ConsumerGroupHeartbeatResponse>,
    live_wait_timed_out: Option<bool>,
    held_topics: HashSet<WireUuid>,
    polled_codes: Vec<i16>,
    directory_guard: Option<tempfile::TempDir>,
}
#[rstest::fixture]
fn persisted_consumer() -> PersistedConsumer {
    // The disk directory survives both native brokers in this scenario.
    PersistedConsumer::default()
}
#[rstest_bdd_macros::given(
    "a disk-backed consumer broker with topic {topic} and {partitions} partitions"
)]
async fn persisted_topic(
    persisted_consumer: &mut PersistedConsumer,
    topic: String,
    partitions: i32,
) {
    let dir = tempfile::tempdir().expect("persistent broker directory");
    let running = boot(dir.path().to_path_buf()).await;
    create_topic(&running.client, &topic, partitions).await;
    persisted_consumer.running = Some(running);
    persisted_consumer.directory_guard = Some(dir);
}
#[rstest_bdd_macros::when(
    "a member joins group {group} with named topics {topics} and regex {pattern}"
)]
async fn persisted_join(
    persisted_consumer: &mut PersistedConsumer,
    group: String,
    topics: String,
    pattern: String,
) {
    let topics: Option<Vec<String>> = serde_json::from_str(&topics).expect("named subscription");
    let pattern: Option<String> = serde_json::from_str(&pattern).expect("regex subscription");
    let request = ConsumerGroupHeartbeatRequest {
        subscribed_topic_names: topics,
        subscribed_topic_regex: pattern,
        ..joining_consumer(&group, uuid::Uuid::new_v4().to_string(), 60_000)
    };
    let response = persisted_consumer
        .running
        .as_ref()
        .expect("broker")
        .client
        .send(request)
        .await
        .expect("join");
    persisted_consumer.group = group;
    persisted_consumer.member = response.member_id.clone().expect("member identity");
    persisted_consumer.epoch = response.member_epoch;
    persisted_consumer.response = Some(response);
}
#[rstest_bdd_macros::then("the persisted consumer heartbeat returns Kafka code {code}")]
fn persisted_response_code(persisted_consumer: &PersistedConsumer, code: i16) {
    assert!(
        persisted_consumer
            .response
            .as_ref()
            .expect("heartbeat executed")
            .error_code
            == code
    );
}
#[rstest_bdd_macros::when(
    "the member heartbeats at its acknowledged epoch without repeating the regex"
)]
async fn persisted_pattern_resolution(persisted_consumer: &mut PersistedConsumer) {
    let request = consumer_heartbeat(
        &persisted_consumer.group,
        persisted_consumer.member.clone(),
        persisted_consumer.epoch,
    );
    let response = persisted_consumer
        .running
        .as_ref()
        .expect("broker")
        .client
        .send(request)
        .await
        .expect("resolve regex");
    persisted_consumer.epoch = response.member_epoch;
    persisted_consumer.response = Some(response);
}
#[rstest_bdd_macros::then("the heartbeat assignment includes exactly {count} topics")]
fn persisted_assigned_topics(persisted_consumer: &PersistedConsumer, count: usize) {
    assert!(
        persisted_consumer
            .response
            .as_ref()
            .expect("heartbeat executed")
            .assignment
            .as_ref()
            .is_some_and(|assignment| assignment.topic_partitions.len() == count)
    );
}
#[rstest_bdd_macros::when(
    "the broker waits {millis} milliseconds for the live group to become empty"
)]
async fn persisted_live_empty_wait(persisted_consumer: &mut PersistedConsumer, millis: u64) {
    let broker = &persisted_consumer.running.as_ref().expect("broker").broker;
    broker
        .wait_until_group_member_count(&persisted_consumer.group, 1)
        .await;
    persisted_consumer.live_wait_timed_out = Some(
        time::timeout(
            Duration::from_millis(millis),
            broker.wait_until_group_empty(&persisted_consumer.group),
        )
        .await
        .is_err(),
    );
}
#[rstest_bdd_macros::then("the group-empty waiter times out while the member is live")]
fn persisted_wait_keeps_live_group(persisted_consumer: &PersistedConsumer) {
    assert!(persisted_consumer.live_wait_timed_out == Some(true));
}
#[rstest_bdd_macros::when(
    "the member leaves at epoch minus one and the broker shuts down after its group is empty"
)]
async fn persisted_member_leave(persisted_consumer: &mut PersistedConsumer) {
    let running = persisted_consumer.running.take().expect("broker");
    let request = consumer_heartbeat(
        &persisted_consumer.group,
        persisted_consumer.member.clone(),
        -1,
    );
    let _ = running.client.send(request).await.expect("leave");
    running
        .broker
        .wait_until_group_empty(&persisted_consumer.group)
        .await;
    running.broker.shutdown().await;
}
#[rstest_bdd_macros::when("the broker shuts down after its one member is visible")]
async fn persisted_member_shutdown(persisted_consumer: &mut PersistedConsumer) {
    let running = persisted_consumer.running.take().expect("broker");
    running
        .broker
        .wait_until_group_member_count(&persisted_consumer.group, 1)
        .await;
    running.broker.shutdown().await;
}
#[rstest_bdd_macros::when(
    "the restarted broker receives the saved member heartbeat at epoch {epoch} with rebalance timeout {timeout} and named topics {topics}"
)]
async fn replayed_member_heartbeat(
    persisted_consumer: &mut PersistedConsumer,
    epoch: String,
    timeout: i32,
    topics: String,
) {
    let epoch = if epoch == "acknowledged" {
        persisted_consumer.epoch
    } else {
        epoch.parse().expect("request epoch")
    };
    let topics: Option<Vec<String>> = serde_json::from_str(&topics).expect("named subscription");
    let running = restart(
        persisted_consumer
            .directory_guard
            .as_ref()
            .expect("disk directory")
            .path()
            .to_path_buf(),
        false,
    )
    .await;
    let request = ConsumerGroupHeartbeatRequest {
        subscribed_topic_names: topics,
        rebalance_timeout_ms: timeout,
        ..consumer_heartbeat(
            &persisted_consumer.group,
            persisted_consumer.member.clone(),
            epoch,
        )
    };
    persisted_consumer.response = Some(
        running
            .client
            .send(request)
            .await
            .expect("post-restart heartbeat"),
    );
    persisted_consumer.running = Some(running);
}
#[rstest_bdd_macros::when(
    "the replayed coordinator creates topic {topic} with {partitions} partitions and polls the saved member up to {attempts} times every {millis} milliseconds"
)]
async fn replayed_regex_topic(
    persisted_consumer: &mut PersistedConsumer,
    topic: String,
    partitions: i32,
    attempts: usize,
    millis: u64,
) {
    let running = restart(
        persisted_consumer
            .directory_guard
            .as_ref()
            .expect("disk directory")
            .path()
            .to_path_buf(),
        true,
    )
    .await;
    create_topic(&running.client, &topic, partitions).await;
    for _ in 0..attempts {
        let response = running
            .client
            .send(consumer_heartbeat(
                &persisted_consumer.group,
                persisted_consumer.member.clone(),
                persisted_consumer.epoch,
            ))
            .await
            .expect("regex refresh heartbeat");
        persisted_consumer.polled_codes.push(response.error_code);
        persisted_consumer.epoch = response.member_epoch;
        if let Some(assignment) = response.assignment {
            persisted_consumer.held_topics = assignment
                .topic_partitions
                .into_iter()
                .map(|topic| topic.topic_id)
                .collect();
        }
        if persisted_consumer.held_topics.len() == 2 {
            break;
        }
        time::sleep(Duration::from_millis(millis)).await;
    }
    persisted_consumer.running = Some(running);
}
#[rstest_bdd_macros::then(
    "all replayed pattern heartbeats return Kafka code {code} and the member holds {count} topics"
)]
fn replayed_regex_assignment(persisted_consumer: &PersistedConsumer, code: i16, count: usize) {
    assert!(!persisted_consumer.polled_codes.is_empty());
    assert!(
        persisted_consumer
            .polled_codes
            .iter()
            .all(|observed| *observed == code)
    );
    assert!(persisted_consumer.held_topics.len() == count);
}
#[rstest_bdd_macros::then("the restarted consumer broker is shut down")]
async fn finish_persisted_consumer(persisted_consumer: &mut PersistedConsumer) {
    persisted_consumer
        .running
        .take()
        .expect("broker")
        .broker
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen_persistence.feature",
    name = "replay preserves group epoch and members"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_preserves_group_epoch_and_members(persisted_consumer: PersistedConsumer) {
    let _ = persisted_consumer;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen_persistence.feature",
    name = "next gen state cleared after leave then restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_gen_state_cleared_after_leave_then_restart(persisted_consumer: PersistedConsumer) {
    let _ = persisted_consumer;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen_persistence.feature",
    name = "replay keeps the topics a regex resolved to and finds new ones"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_keeps_the_topics_a_regex_resolved_to_and_finds_new_ones(
    persisted_consumer: PersistedConsumer,
) {
    let _ = persisted_consumer;
}
