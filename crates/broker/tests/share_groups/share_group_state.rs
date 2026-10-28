//! Share-state lifecycle scenarios: the group coordinator initializing the
//! per-partition share state in the `__share_group_state` persister, and that
//! state surviving a broker restart without being re-initialized.
//!
//! These tests assert on the persister rather than on what
//! `ShareGroupDescribe` reports about members, so they are kept apart from the
//! membership scenarios.

use {
    crate::share_group_harness::{
        boot, connect, create_topic, describe, heartbeat, rejoin, topic_id,
    },
    assert2::assert,
    std::{ops::Range, time::Duration},
    tokio::time,
};

/// Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`: the share state
/// exists but where the partition starts has not been decided yet.
const UNINITIALIZED_START_OFFSET: i64 = -1;

async fn initialize_share_partitions(
    broker: &krabka_broker::BrokerHandle,
    client: &krabka_client_core::Client,
    group: &str,
    topic: &str,
    partitions: i32,
) {
    let tid = topic_id(broker, topic);
    let mut join = heartbeat(group, &uuid::Uuid::new_v4().to_string(), 0);
    join.subscribed_topic_names = Some(vec![topic.into()]);
    let response = client.send(join).await.unwrap();
    assert!(
        response.error_code == 0,
        "join failed: {:?}",
        response.error_code
    );
    let member_id = response.member_id.unwrap();
    // Retry the lifecycle hook until every partition has a summary.
    let result = time::timeout(Duration::from_secs(30), async {
        loop {
            let mut request = heartbeat(group, &member_id, response.member_epoch);
            request.subscribed_topic_names = Some(vec![topic.into()]);
            client.send(request).await.unwrap();
            let mut all_done = true;
            for partition in 0..partitions {
                if broker
                    .share_state_summary_for_test(group, tid, partition)
                    .await
                    .is_none()
                {
                    all_done = false;
                    break;
                }
            }
            if all_done {
                break;
            }
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "lifecycle did not initialize {partitions} partitions within 30s"
    );
}

/// A group member, the epoch it last held, and the topics it subscribes to.
#[derive(Clone, Copy)]
struct MemberSubscription<'a> {
    member_id: &'a str,
    epoch: i32,
    topics: &'a [&'a str],
}

/// Heartbeats until `done` holds for the share state of every partition in
/// `partitions` of `topic`. Each member in `members` sends one heartbeat per
/// pass, with its own subscription and with the member epoch of its last
/// accepted response, so a heartbeat that bumps the epoch does not stop the
/// retries.
async fn heartbeat_until(
    broker: &krabka_broker::BrokerHandle,
    client: &krabka_client_core::Client,
    group: &str,
    members: &[MemberSubscription<'_>],
    topic: uuid::Uuid,
    partitions: Range<i32>,
    present: bool,
) {
    let mut epochs: Vec<i32> = members.iter().map(|member| member.epoch).collect();
    let reached = time::timeout(Duration::from_secs(30), async {
        loop {
            for (member, epoch) in members.iter().zip(epochs.iter_mut()) {
                let mut hb = heartbeat(group, member.member_id, *epoch);
                hb.subscribed_topic_names = Some(
                    member
                        .topics
                        .iter()
                        .map(|topic| (*topic).to_owned())
                        .collect(),
                );
                let resp = client.send(hb).await.unwrap();
                if resp.error_code == 0 {
                    *epoch = resp.member_epoch;
                }
            }
            let mut all = true;
            for p in partitions.clone() {
                all &= broker
                    .share_state_summary_for_test(group, topic, p)
                    .await
                    .is_some()
                    == present;
            }
            if all {
                break;
            }
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "share state of {group} never reached present={present}"
    );
}

use {
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        delete_groups_request::DeleteGroupsRequest,
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{collections::BTreeMap, sync::Arc},
    tempfile::TempDir,
};

#[derive(Default)]
struct ShareLifecycle {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Arc<Client>>,
    group: String,
    topics: BTreeMap<String, (uuid::Uuid, i32)>,
    members: BTreeMap<String, i32>,
    error: Option<i16>,
}

#[fixture]
fn share_lifecycle() -> ShareLifecycle {
    // Topic IDs and actual member epochs remain stable across lifecycle operations.
    ShareLifecycle::default()
}

impl ShareLifecycle {
    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("share broker") }

    fn client(&self) -> &Client { self.client.as_ref().expect("share client") }
}

#[given("a running share lifecycle coordinator for group {group}")]
async fn lifecycle_coordinator(share_lifecycle: &mut ShareLifecycle, group: String) {
    let (broker, bootstrap, directory) = boot().await;
    share_lifecycle.client = Some(connect(&bootstrap).await);
    share_lifecycle.broker = Some(broker);
    share_lifecycle.directory = Some(directory);
    share_lifecycle.group = group;
}

#[given("share lifecycle topic {topic} has {partitions} partitions")]
async fn lifecycle_topic(share_lifecycle: &mut ShareLifecycle, topic: String, partitions: i32) {
    create_topic(share_lifecycle.client(), &topic, partitions).await;
    let identity = topic_id(share_lifecycle.broker(), &topic);
    share_lifecycle.topics.insert(topic, (identity, partitions));
}

#[when("the share group lifecycle initializes topic {topic}")]
async fn initialize_lifecycle(share_lifecycle: &ShareLifecycle, topic: String) {
    initialize_share_partitions(
        share_lifecycle.broker(),
        share_lifecycle.client(),
        &share_lifecycle.group,
        &topic,
        share_lifecycle.topics[&topic].1,
    )
    .await;
}

#[then("every share state partition of {topic} exists with an uninitialized start offset")]
async fn uninitialized_share_state(share_lifecycle: &ShareLifecycle, topic: String) {
    let (identity, partitions) = share_lifecycle.topics[&topic];
    let mut offsets = Vec::new();
    for partition in 0..partitions {
        let (_, _, offset, _) = share_lifecycle
            .broker()
            .share_state_summary_for_test(&share_lifecycle.group, identity, partition)
            .await
            .expect("partition initialized");
        offsets.push(offset);
    }
    assert!(offsets == vec![UNINITIALIZED_START_OFFSET; usize::try_from(partitions).unwrap()]);
}

#[then("every share state partition of {topic} is present: {present}")]
async fn lifecycle_state_present(share_lifecycle: &ShareLifecycle, topic: String, present: bool) {
    let (identity, partitions) = share_lifecycle.topics[&topic];
    for partition in 0..partitions {
        assert!(
            share_lifecycle
                .broker()
                .share_state_summary_for_test(&share_lifecycle.group, identity, partition)
                .await
                .is_some()
                == present,
            "share state of {topic}-{partition} must be present={present}"
        );
    }
}

#[when("the lifecycle broker restarts and the group's state summaries recover")]
async fn restart_share_lifecycle(share_lifecycle: &mut ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = rejoin(
        share_lifecycle
            .directory
            .as_ref()
            .expect("durable directory")
            .path()
            .to_path_buf(),
    )
    .await;
    for (identity, partitions) in share_lifecycle.topics.values() {
        for partition in 0..*partitions {
            broker
                .wait_for_share_state_summary(&share_lifecycle.group, *identity, partition)
                .await;
        }
    }
    share_lifecycle.broker = Some(broker);
    share_lifecycle.client = Some(client);
}

#[when("the recovered member heartbeats again for topic {topic}")]
async fn heartbeat_recovered_lifecycle(share_lifecycle: &ShareLifecycle, topic: String) {
    let described = describe(share_lifecycle.client(), &share_lifecycle.group).await;
    let member = &described.groups[0].members[0].member_id;
    let mut request = heartbeat(
        &share_lifecycle.group,
        member,
        described.groups[0].group_epoch,
    );
    request.subscribed_topic_names = Some(vec![topic.clone()]);
    share_lifecycle.client().send(request).await.unwrap();
    let (identity, partitions) = share_lifecycle.topics[&topic];
    for partition in 0..partitions {
        share_lifecycle
            .broker()
            .wait_for_share_state_summary(&share_lifecycle.group, identity, partition)
            .await;
    }
}

#[given("member {member} has initialized its subscription to {subscriptions}")]
async fn initialized_subscription(
    share_lifecycle: &mut ShareLifecycle,
    member: String,
    subscriptions: String,
) {
    let topics: Vec<&str> = subscriptions.split(',').collect();
    let mut request = heartbeat(&share_lifecycle.group, &member, 0);
    request.subscribed_topic_names = Some(topics.iter().map(|topic| (*topic).to_owned()).collect());
    let response = share_lifecycle.client().send(request).await.unwrap();
    assert!(response.error_code == 0, "join failed: {response:?}");
    let actual_member = response.member_id.expect("joined member identity");
    for topic in &topics {
        let (identity, partitions) = share_lifecycle.topics[*topic];
        heartbeat_until(
            share_lifecycle.broker(),
            share_lifecycle.client(),
            &share_lifecycle.group,
            &[MemberSubscription {
                member_id: &actual_member,
                epoch: response.member_epoch,
                topics: &topics,
            }],
            identity,
            0..partitions,
            true,
        )
        .await;
    }
    share_lifecycle
        .members
        .insert(actual_member, response.member_epoch);
}

#[given(
    "member {member} has joined its subscription to {topic} without waiting for state initialization"
)]
async fn joined_lifecycle_subscription(
    share_lifecycle: &mut ShareLifecycle,
    member: String,
    topic: String,
) {
    let mut request = heartbeat(&share_lifecycle.group, &member, 0);
    request.subscribed_topic_names = Some(vec![topic]);
    let response = share_lifecycle.client().send(request).await.unwrap();
    assert!(response.error_code == 0, "second join failed: {response:?}");
    share_lifecycle.members.insert(
        response.member_id.expect("joined member identity"),
        response.member_epoch,
    );
}

#[when("an administrator deletes the share group")]
async fn delete_lifecycle_group(share_lifecycle: &mut ShareLifecycle) {
    let response = share_lifecycle
        .client()
        .send(DeleteGroupsRequest {
            groups_names: vec![share_lifecycle.group.clone()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    share_lifecycle.error = Some(response.results[0].error_code);
}

#[when("share lifecycle member {member} leaves at epoch minus one")]
async fn leave_lifecycle_group(share_lifecycle: &mut ShareLifecycle, member: String) {
    let response = share_lifecycle
        .client()
        .send(heartbeat(&share_lifecycle.group, &member, -1))
        .await
        .unwrap();
    share_lifecycle.error = Some(response.error_code);
}

#[then("the share lifecycle operation returns error {error}")]
fn lifecycle_operation_code(share_lifecycle: &ShareLifecycle, error: i16) {
    assert!(share_lifecycle.error == Some(error));
}

#[when("an administrator deletes share lifecycle topic {topic}")]
async fn delete_lifecycle_topic(share_lifecycle: &mut ShareLifecycle, topic: String) {
    let response = share_lifecycle
        .client()
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    share_lifecycle.error = Some(response.responses[0].error_code);
}

#[when(
    "member {member} heartbeats subscribed to {subscriptions} until topic {topic} state is present {present}"
)]
async fn reconcile_lifecycle_subscription(
    share_lifecycle: &ShareLifecycle,
    member: String,
    subscriptions: String,
    topic: String,
    present: bool,
) {
    let topics: Vec<&str> = subscriptions.split(',').collect();
    let (identity, partitions) = share_lifecycle.topics[&topic];
    heartbeat_until(
        share_lifecycle.broker(),
        share_lifecycle.client(),
        &share_lifecycle.group,
        &[MemberSubscription {
            member_id: &member,
            epoch: share_lifecycle.members[&member],
            topics: &topics,
        }],
        identity,
        0..partitions,
        present,
    )
    .await;
}

#[when("an administrator grows share lifecycle topic {topic} to {partitions} partitions")]
async fn grow_lifecycle_topic(
    share_lifecycle: &mut ShareLifecycle,
    topic: String,
    partitions: i32,
) {
    let response = share_lifecycle
        .client()
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: topic.clone(),
                count: partitions,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreatePartitions");
    share_lifecycle.error = Some(response.results[0].error_code);
    share_lifecycle
        .topics
        .get_mut(&topic)
        .expect("existing topic")
        .1 = partitions;
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "lifecycle initializes share state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_initializes_share_state(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "lifecycle metadata survives restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_metadata_survives_restart(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "delete groups deletes an empty share group and its state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_groups_deletes_an_empty_share_group_and_its_state(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "a deleted topic loses its share state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_topic_loses_its_share_state(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "unassigned partitions keep their share state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unassigned_partitions_keep_their_share_state(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "every initialized partition starts uninitialized"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_initialized_partition_starts_uninitialized(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_state.feature",
    name = "a member moving subscriptions keeps the old topic state"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_moving_subscriptions_keeps_the_old_topic_state(share_lifecycle: ShareLifecycle) {
    share_lifecycle
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_lifecycle);
}
