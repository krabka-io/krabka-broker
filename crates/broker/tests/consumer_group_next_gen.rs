//! Raw-RPC integration tests for KIP-848 next-gen consumer groups,
//! driven against an in-process Krabka broker through `krabka-client-core`.
#![cfg(test)]

mod support;

use {
    crate::support::{
        classic::{ClassicTimeouts, classic_join_request},
        client::{connect_client, create_topic, list_groups_by_type},
        consumer_groups::{consumer_heartbeat, reported_assignment},
        discovery::named_topic_metadata,
    },
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig, coordinator::unified::config::RebalanceProtocol},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
        join_group_request::JoinGroupRequestProtocol,
        list_groups_request::ListGroupsRequest,
    },
    std::{
        pin::Pin,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// A running broker, its bootstrap address and the directory that backs it.
struct BootedBroker {
    broker: krabka_broker::BrokerHandle,
    bootstrap: String,
    dir: tempfile::TempDir,
}

fn boot() -> Pin<Box<impl Future<Output = BootedBroker>>> {
    Box::pin(async {
        let dir = tempfile::TempDir::new().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.heartbeat_timeout = krabka_units::secs(30);
        let started = Box::pin(
            support::start_group_coordinator_with_config()
                .config(config)
                .directory(dir)
                .call(),
        )
        .await;
        BootedBroker {
            broker: started.broker,
            bootstrap: started.bootstrap,
            dir: started.directory,
        }
    })
}

/// A heartbeat Kafka accepts at v1: an empty `member_id` becomes a
/// client-generated id, as a KIP-848 consumer sends it, and a join (epoch 0)
/// reports its empty owned partitions.
fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ConsumerGroupHeartbeatRequest {
    let member_id = if member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        member_id.into()
    };
    ConsumerGroupHeartbeatRequest {
        rebalance_timeout_ms: 60_000,
        topic_partitions: (epoch == 0).then(Vec::new),
        ..consumer_heartbeat(group, member_id, epoch)
    }
}

use {
    krabka_metadata::{GroupConfigRecord, MetadataRecord},
    krabka_protocol::{
        owned::{
            consumer_group_describe_response::ConsumerGroupDescribeResponse,
            list_groups_response::ListGroupsResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::collections::BTreeMap,
};
#[derive(Default)]
struct ConsumerBehavior {
    broker: Option<krabka_broker::BrokerHandle>,
    client: Option<Arc<Client>>,
    responses: BTreeMap<String, Vec<ConsumerGroupHeartbeatResponse>>,
    described: Option<ConsumerGroupDescribeResponse>,
    listed: Option<ListGroupsResponse>,
    late_topic_id: Option<WireUuid>,
    directory_guard: Option<tempfile::TempDir>,
}
#[rstest::fixture]
fn consumer_behavior() -> ConsumerBehavior {
    // Member identities, assignments and broker metadata belong to one scenario.
    ConsumerBehavior::default()
}
#[rstest_bdd_macros::given("a consumer broker with protocol policy {policy} and topics {topics}")]
async fn consumer_broker(consumer_behavior: &mut ConsumerBehavior, policy: String, topics: String) {
    let topics: Vec<(String, i32)> =
        serde_json::from_str(&topics).expect("topics and partition counts");
    let BootedBroker {
        broker,
        bootstrap,
        dir,
    } = if policy == "classic-only" {
        let dir = tempfile::tempdir().expect("broker directory");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.next_gen_consumer_group.rebalance_protocols = vec![RebalanceProtocol::Classic];
        let broker = Broker::start(config).await.expect("classic-only broker");
        let bootstrap = broker.listen_addr().to_string();
        BootedBroker {
            broker,
            bootstrap,
            dir,
        }
    } else {
        assert!(policy == "classic-and-consumer");
        boot().await
    };
    let client = Arc::new(connect_client(&bootstrap, Some("c")).await);
    for (topic, partitions) in topics {
        create_topic(&client, &topic, partitions).await;
    }
    consumer_behavior.client = Some(client);
    consumer_behavior.broker = Some(broker);
    consumer_behavior.directory_guard = Some(dir);
}
#[derive(serde::Deserialize)]
struct ConsumerHeartbeatInput {
    group: String,
    id: Option<String>,
    epoch: Option<i32>,
    topics: Option<Vec<String>>,
    regex: Option<String>,
    ownership: String,
}
#[rstest_bdd_macros::when("member {member} sends consumer heartbeat {request}")]
async fn consumer_member_heartbeat(
    consumer_behavior: &mut ConsumerBehavior,
    member: String,
    request: String,
) {
    let request: ConsumerHeartbeatInput =
        serde_json::from_str(&request).expect("native consumer heartbeat inputs");
    let previous = consumer_behavior
        .responses
        .get(&member)
        .and_then(|responses| responses.last());
    let id = request
        .id
        .or_else(|| previous?.member_id.clone())
        .unwrap_or_default();
    let epoch = request
        .epoch
        .unwrap_or_else(|| previous.expect("member's acknowledged epoch").member_epoch);
    let owned = match request.ownership.as_str() {
        "previous-assignment" => previous
            .expect("previous member response")
            .assignment
            .as_ref()
            .map(reported_assignment),
        "omitted" => None,
        "empty" => Some(vec![]),
        other => panic!("unknown owned-partition report {other}"),
    };
    let mut native = heartbeat(&request.group, &id, epoch);
    native.subscribed_topic_names = request.topics;
    native.subscribed_topic_regex = request.regex;
    native.topic_partitions = owned;
    let response = consumer_behavior
        .client
        .as_ref()
        .expect("consumer client")
        .send(native)
        .await
        .expect("consumer heartbeat");
    consumer_behavior
        .responses
        .entry(member)
        .or_default()
        .push(response);
}
fn last_consumer_response<'a>(
    world: &'a ConsumerBehavior,
    member: &str,
) -> &'a ConsumerGroupHeartbeatResponse {
    world
        .responses
        .get(member)
        .expect("member history")
        .last()
        .expect("heartbeat executed")
}
#[rstest_bdd_macros::then("member {member} receives Kafka code {code}")]
fn consumer_response_code(consumer_behavior: &ConsumerBehavior, member: String, code: i16) {
    assert!(last_consumer_response(consumer_behavior, &member).error_code == code);
}
#[rstest_bdd_macros::then("member {member} receives epoch {epoch}")]
fn consumer_response_epoch(consumer_behavior: &ConsumerBehavior, member: String, epoch: i32) {
    assert!(last_consumer_response(consumer_behavior, &member).member_epoch == epoch);
}
#[rstest_bdd_macros::then("member {member} receives {count} assigned partitions")]
fn consumer_assignment_count(consumer_behavior: &ConsumerBehavior, member: String, count: usize) {
    let assignment = last_consumer_response(consumer_behavior, &member)
        .assignment
        .as_ref()
        .expect("assignment present");
    assert!(
        assignment
            .topic_partitions
            .iter()
            .map(|topic| topic.partitions.len())
            .sum::<usize>()
            == count
    );
}
#[rstest_bdd_macros::then("member {member} receives id {identity}")]
fn consumer_echoed_identity(
    consumer_behavior: &ConsumerBehavior,
    member: String,
    identity: String,
) {
    assert!(
        last_consumer_response(consumer_behavior, &member)
            .member_id
            .as_deref()
            == Some(identity.as_str())
    );
}
#[rstest_bdd_macros::then("member {member} receives exact message {message}")]
fn consumer_exact_message(consumer_behavior: &ConsumerBehavior, member: String, message: String) {
    assert!(
        last_consumer_response(consumer_behavior, &member)
            .error_message
            .as_deref()
            == Some(message.as_str())
    );
}
#[rstest_bdd_macros::then(
    "member {member} receives diagnostic fragment {message} without admission"
)]
fn consumer_refused_pattern(consumer_behavior: &ConsumerBehavior, member: String, message: String) {
    let response = last_consumer_response(consumer_behavior, &member);
    check!(
        response
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains(&message)
    );
    check!(response.member_id.is_none() || response.member_epoch == 0);
}
#[rstest_bdd_macros::then(
    "member {member} response {index} and member {other} response {other_index} assign {count} partitions in total"
)]
fn consumer_split_assignment(
    consumer_behavior: &ConsumerBehavior,
    member: String,
    index: usize,
    other: String,
    other_index: usize,
    count: usize,
) {
    let count_for = |id: &str, index: usize| {
        consumer_behavior.responses[id][index]
            .assignment
            .as_ref()
            .expect("assignment present")
            .topic_partitions
            .iter()
            .map(|topic| topic.partitions.len())
            .sum::<usize>()
    };
    assert!(count_for(&member, index) + count_for(&other, other_index) == count);
}
#[rstest_bdd_macros::given(
    "a live classic group {group} uses protocol type connect with protocol default"
)]
async fn live_connect_group(consumer_behavior: &mut ConsumerBehavior, group: String) {
    let client = consumer_behavior.client.as_ref().expect("client");
    let join = |member_id: String| {
        classic_join_request()
            .group_id(group.clone())
            .member_id(member_id)
            .timeouts(ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            })
            .protocol_type("connect".into())
            .protocols(vec![JoinGroupRequestProtocol {
                name: "default".into(),
                ..Default::default()
            }])
            .call()
    };
    let required = client
        .send(join(String::new()))
        .await
        .expect("classic member identity");
    let joined = client
        .send(join(required.member_id))
        .await
        .expect("classic join");
    assert!(joined.error_code == 0);
}
#[rstest_bdd_macros::when("the client describes consumer group {group}")]
async fn describe_consumer_group(consumer_behavior: &mut ConsumerBehavior, group: String) {
    consumer_behavior.described = Some(
        consumer_behavior
            .client
            .as_ref()
            .expect("client")
            .send(ConsumerGroupDescribeRequest {
                group_ids: vec![group],
                include_authorized_operations: false,
                ..Default::default()
            })
            .await
            .expect("describe group"),
    );
}
#[rstest_bdd_macros::then(
    "the description contains {count} group with Kafka code {code} and state {state}"
)]
fn consumer_described_state(
    consumer_behavior: &ConsumerBehavior,
    count: usize,
    code: i16,
    state: String,
) {
    let response = consumer_behavior
        .described
        .as_ref()
        .expect("description requested");
    assert!(response.groups.len() == count);
    check!(response.groups[0].error_code == code);
    check!(response.groups[0].group_state == state);
}
#[rstest_bdd_macros::when("the client lists groups with type filter {kind}")]
async fn list_consumer_groups(consumer_behavior: &mut ConsumerBehavior, kind: String) {
    let client = consumer_behavior.client.as_ref().expect("client");
    consumer_behavior.listed = Some(if kind == "all" {
        client
            .send(ListGroupsRequest::default())
            .await
            .expect("ListGroups all")
    } else {
        list_groups_by_type(client, &kind).await
    });
}
#[rstest_bdd_macros::then("group {group} appears {count} times with type {kind}")]
fn listed_consumer_group(
    consumer_behavior: &ConsumerBehavior,
    group: String,
    count: usize,
    kind: String,
) {
    let matching = consumer_behavior
        .listed
        .as_ref()
        .expect("groups listed")
        .groups
        .iter()
        .filter(|row| row.group_id == group)
        .collect::<Vec<_>>();
    assert!(matching.len() == count);
    for row in matching {
        assert!(row.group_type == kind);
    }
}
#[rstest_bdd_macros::when(
    "topic {topic} is created with {partitions} partitions after member {member} joined group {group} at epoch {epoch}"
)]
async fn refresh_late_topic(
    consumer_behavior: &mut ConsumerBehavior,
    topic: String,
    partitions: i32,
    member: String,
    group: String,
    epoch: i32,
) {
    let client = consumer_behavior.client.as_ref().expect("client");
    create_topic(client, &topic, partitions).await;
    let metadata = client
        .send(named_topic_metadata(&topic))
        .await
        .expect("topic metadata");
    consumer_behavior.late_topic_id = Some(metadata.topics[0].topic_id);
    let id = last_consumer_response(consumer_behavior, &member)
        .member_id
        .clone()
        .expect("member identity");
    let created = Instant::now();
    let refreshed = loop {
        let answer = client
            .send(heartbeat(&group, &id, epoch))
            .await
            .expect("metadata refresh heartbeat");
        if answer.assignment.is_some() || created.elapsed() > Duration::from_secs(10) {
            break answer;
        }
        time::sleep(Duration::from_millis(20)).await;
    };
    consumer_behavior
        .responses
        .entry(member)
        .or_default()
        .push(refreshed);
}
#[rstest_bdd_macros::then(
    "member {member} receives the complete new-topic response at epoch {epoch} interval {interval} and partitions {partitions}"
)]
fn late_topic_response(
    consumer_behavior: &ConsumerBehavior,
    member: String,
    epoch: i32,
    interval: i32,
    partitions: String,
) {
    let partitions: Vec<i32> = serde_json::from_str(&partitions).expect("expected partition ids");
    let response = last_consumer_response(consumer_behavior, &member);
    let expected = ConsumerGroupHeartbeatResponse {
        member_id: consumer_behavior.responses[&member][0].member_id.clone(),
        member_epoch: epoch,
        heartbeat_interval_ms: interval,
        assignment: Some(Assignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: consumer_behavior.late_topic_id.expect("created topic id"),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    check!(response == &expected);
}
#[rstest_bdd_macros::then("member {member} receives an explicit empty assignment")]
fn empty_consumer_assignment(consumer_behavior: &ConsumerBehavior, member: String) {
    check!(
        last_consumer_response(consumer_behavior, &member).assignment
            == Some(Assignment::default())
    );
}
#[rstest_bdd_macros::given("group {group} has persisted consumer configuration {configs}")]
async fn tuned_consumer_group(
    consumer_behavior: &mut ConsumerBehavior,
    group: String,
    configs: String,
) {
    let configs: BTreeMap<String, String> =
        serde_json::from_str(&configs).expect("consumer group config");
    consumer_behavior
        .broker
        .as_ref()
        .expect("broker")
        .submit_metadata_record_for_test(MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group,
            configs,
        }))
        .await
        .expect("group configuration");
}
#[rstest_bdd_macros::then("member {member} receives heartbeat interval {interval} milliseconds")]
fn tuned_consumer_interval(consumer_behavior: &ConsumerBehavior, member: String, interval: i32) {
    check!(last_consumer_response(consumer_behavior, &member).heartbeat_interval_ms == interval);
}
#[rstest_bdd_macros::then("the consumer behavior broker is shut down")]
async fn finish_consumer_behavior(consumer_behavior: &mut ConsumerBehavior) {
    drop(consumer_behavior.client.take());
    consumer_behavior
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "single member full lifecycle"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_full_lifecycle(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "two members split partitions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_split_partitions(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "classic group locked against next gen"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_locked_against_next_gen(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "kill switch returns unsupported version"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_returns_unsupported_version(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "describe after join"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_after_join(consumer_behavior: ConsumerBehavior) { let _ = consumer_behavior; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "an old epoch is fenced"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_epoch_is_fenced(consumer_behavior: ConsumerBehavior) { let _ = consumer_behavior; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "first join with client member id echoes and assigns"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_with_client_member_id_echoes_and_assigns(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "list groups includes next gen consumer group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_next_gen_consumer_group(consumer_behavior: ConsumerBehavior) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "an invalid subscribed topic regex fails the heartbeat"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_subscribed_topic_regex_fails_the_heartbeat(
    consumer_behavior: ConsumerBehavior,
) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "a topic created after the member joined reaches its next heartbeat"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_created_after_the_member_joined_reaches_its_next_heartbeat(
    consumer_behavior: ConsumerBehavior,
) {
    let _ = consumer_behavior;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/consumer_group_next_gen.feature",
    name = "a group config overrides the heartbeat interval of its group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_config_overrides_the_heartbeat_interval_of_its_group(
    consumer_behavior: ConsumerBehavior,
) {
    let _ = consumer_behavior;
}
