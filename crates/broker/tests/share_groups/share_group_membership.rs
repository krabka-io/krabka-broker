//! Share-group membership scenarios: a first join, a second member joining the
//! same group, a leave at `member_epoch == -1`, and replay of the membership
//! from `__consumer_offsets` after a broker restart.
//!
//! These tests assert on what `ShareGroupHeartbeat` and `ShareGroupDescribe`
//! report about members, so they are kept apart from the share-state
//! initialization scenarios that assert on the persister.

use {
    crate::share_group_harness::{
        boot, connect, create_topic, describe, heartbeat, join_share_group, rejoin, total_assigned,
    },
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        share_group_describe_response::ShareGroupDescribeResponse,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::time::Duration,
    tempfile::TempDir,
    tokio::time,
};

#[derive(Default)]
struct ShareMembership {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Arc<Client>>,
    topic: String,
    requested_ids: Vec<String>,
    member_ids: Vec<String>,
    joins: Vec<ShareGroupHeartbeatResponse>,
    heartbeat: Option<ShareGroupHeartbeatResponse>,
    described: Option<ShareGroupDescribeResponse>,
}

#[fixture]
fn share_membership() -> ShareMembership {
    // Keep actual join identities and generations for subsequent membership operations.
    ShareMembership::default()
}

#[given("a share coordinator with topic {topic} containing {partitions} partitions")]
async fn membership_topic(share_membership: &mut ShareMembership, topic: String, partitions: i32) {
    let (broker, bootstrap, directory) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, &topic, partitions).await;
    share_membership.broker = Some(broker);
    share_membership.directory = Some(directory);
    share_membership.client = Some(client);
    share_membership.topic = topic;
}

#[given("a share member has completed joining g1 for the topic")]
async fn joined_share_member(share_membership: &mut ShareMembership) {
    let member = join_share_group(
        share_membership
            .client
            .as_ref()
            .expect("share coordinator client"),
        "g1",
        &uuid::Uuid::new_v4().to_string(),
        &share_membership.topic,
    )
    .await;
    share_membership.member_ids.push(member);
}

#[when("a new member joins share group g1 at epoch zero")]
async fn join_share_member(share_membership: &mut ShareMembership) {
    let member = uuid::Uuid::new_v4().to_string();
    let mut request = heartbeat("g1", &member, 0);
    request.subscribed_topic_names = Some(vec![share_membership.topic.clone()]);
    let response = share_membership
        .client
        .as_ref()
        .expect("share coordinator client")
        .send(request)
        .await
        .unwrap();
    share_membership
        .member_ids
        .push(response.member_id.clone().expect("join member ID"));
    share_membership.requested_ids.push(member);
    share_membership.joins.push(response);
}

#[then(
    "the first share join succeeds at epoch 2 with the requested identity and an empty assignment"
)]
fn initial_share_assignment(share_membership: &ShareMembership) {
    let response = share_membership.joins.first().expect("first join answered");
    check!(response.error_code == 0, "join failed: {response:?}");
    check!(response.member_id.as_deref() == Some(share_membership.requested_ids[0].as_str()));
    check!(response.member_epoch == 2);
    check!(response.assignment.is_some() && total_assigned(response) == 0);
}

#[when("the first member heartbeats until initialized share partitions are assigned")]
async fn initialized_share_assignment(share_membership: &mut ShareMembership) {
    let client = share_membership
        .client
        .as_ref()
        .expect("share coordinator client");
    let member = &share_membership.member_ids[0];
    let mut epoch = share_membership.joins[0].member_epoch;
    share_membership.heartbeat = Some(
        time::timeout(Duration::from_secs(30), async {
            loop {
                let response = client.send(heartbeat("g1", member, epoch)).await.unwrap();
                assert!(response.error_code == 0, "heartbeat failed: {response:?}");
                epoch = response.member_epoch;
                if response.assignment.is_some() {
                    return response;
                }
                time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("initialized partitions assigned"),
    );
}

#[then("the single share member owns all four partitions at epoch 3")]
fn full_share_assignment(share_membership: &ShareMembership) {
    let response = share_membership
        .heartbeat
        .as_ref()
        .expect("assignment heartbeat answered");
    check!(response.member_epoch == 3);
    check!(
        total_assigned(response) == 4,
        "single member must own all four partitions"
    );
}

#[then("both share joins succeeded with distinct member identities")]
fn distinct_share_members(share_membership: &ShareMembership) {
    assert!(share_membership.joins.len() == 2);
    for response in &share_membership.joins {
        assert!(response.error_code == 0, "join failed: {response:?}");
    }
    assert!(share_membership.member_ids[0] != share_membership.member_ids[1]);
}

#[when("the first member heartbeats at its returned epoch with the same subscription")]
async fn rebalance_share_member(share_membership: &mut ShareMembership) {
    let mut request = heartbeat(
        "g1",
        &share_membership.member_ids[0],
        share_membership.joins[0].member_epoch,
    );
    request.subscribed_topic_names = Some(vec![share_membership.topic.clone()]);
    share_membership.heartbeat = Some(
        share_membership
            .client
            .as_ref()
            .expect("share coordinator client")
            .send(request)
            .await
            .unwrap(),
    );
}

#[when("the joined share member leaves at epoch minus one")]
async fn leave_share_member(share_membership: &mut ShareMembership) {
    share_membership.heartbeat = Some(
        share_membership
            .client
            .as_ref()
            .expect("share coordinator client")
            .send(heartbeat("g1", &share_membership.member_ids[0], -1))
            .await
            .unwrap(),
    );
}

#[then("the share membership heartbeat succeeds")]
fn successful_share_heartbeat(share_membership: &ShareMembership) {
    let response = share_membership
        .heartbeat
        .as_ref()
        .expect("heartbeat answered");
    assert!(response.error_code == 0, "{response:?}");
}

#[when("an administrator describes share group g1")]
async fn describe_share_members(share_membership: &mut ShareMembership) {
    share_membership.described = Some(
        describe(
            share_membership
                .client
                .as_ref()
                .expect("share coordinator client"),
            "g1",
        )
        .await,
    );
}

#[then("the described share group contains {members} members and has a positive group epoch")]
fn described_share_members(share_membership: &ShareMembership, members: usize) {
    let response = share_membership
        .described
        .as_ref()
        .expect("share group described");
    assert!(response.groups.len() == 1, "expected exactly one group row");
    let group = &response.groups[0];
    check!(group.error_code == 0, "describe error: {group:?}");
    check!(group.members.len() == members);
    check!(group.group_epoch >= 1);
}

#[then("the retained share group contains no members")]
fn empty_share_membership(share_membership: &ShareMembership) {
    let response = share_membership
        .described
        .as_ref()
        .expect("share group described");
    assert!(response.groups.len() == 1, "group row retained after leave");
    assert!(response.groups[0].error_code == 0);
    assert!(response.groups[0].members.is_empty());
}

#[when("the share coordinator restarts on its durable data directory")]
async fn recover_share_membership(share_membership: &mut ShareMembership) {
    share_membership
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = rejoin(
        share_membership
            .directory
            .as_ref()
            .expect("durable directory")
            .path()
            .to_path_buf(),
    )
    .await;
    share_membership.broker = Some(broker);
    share_membership.client = Some(client);
}

#[then("the recovered share group contains the original member identity and a positive epoch")]
fn recovered_share_membership(share_membership: &ShareMembership) {
    let response = share_membership
        .described
        .as_ref()
        .expect("recovered group described");
    assert!(response.groups.len() == 1);
    let group = &response.groups[0];
    check!(group.error_code == 0);
    check!(group.group_epoch >= 1);
    check!(
        group
            .members
            .iter()
            .any(|member| member.member_id == share_membership.member_ids[0])
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_membership.feature",
    name = "single member join assignment"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_join_assignment(share_membership: ShareMembership) {
    share_membership
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_membership);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_membership.feature",
    name = "two members then describe"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_then_describe(share_membership: ShareMembership) {
    share_membership
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_membership);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_membership.feature",
    name = "member leave epoch minus one"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_leave_epoch_minus_one(share_membership: ShareMembership) {
    share_membership
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_membership);
}

#[scenario(
    path = "tests/features/migrated/tests/share_groups/share_group_membership.feature",
    name = "state survives restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart(share_membership: ShareMembership) {
    share_membership
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_membership);
}

use std::sync::Arc;
