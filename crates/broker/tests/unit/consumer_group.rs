//! The classic group's membership, assignment, heartbeat, and offset lifecycle.

use {
    crate::{
        harness::{create_topic, topic_id_for},
        support::{
            InProcess,
            classic::{
                ClassicJoinSetup, ClassicTimeouts, classic_join_request, classic_join_request_from,
                classic_sync_request, required_member_id, sync_assignment,
            },
            offsets::{
                offset_commit_partition, offset_commit_topic, offset_fetch_group,
                offset_fetch_request, offset_fetch_topic,
            },
            start_ready_group,
        },
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            heartbeat_request::HeartbeatRequest,
            join_group_response::JoinGroupResponse,
            leave_group_request::LeaveGroupRequest,
            offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
            sync_group_response::SyncGroupResponse,
        },
        primitives::uuid::Uuid,
    },
    krabka_units::{Time, millis},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

struct ClassicGroupScenario {
    broker: Option<InProcess>,
    rebalance: Time,
    first_join: Option<JoinGroupResponse>,
    joined: Option<JoinGroupResponse>,
    synced: Option<SyncGroupResponse>,
    heartbeat_error: Option<i16>,
    commit_error: Option<i16>,
    fetched_offset: Option<i64>,
    leave_error: Option<i16>,
    topic_id: Option<Uuid>,
}

impl ClassicGroupScenario {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the group coordinator is running")
    }

    fn setup(&self) -> ClassicJoinSetup {
        ClassicJoinSetup {
            group_id: "g".into(),
            timeouts: ClassicTimeouts {
                rebalance: self.rebalance,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn member(&self) -> &JoinGroupResponse {
        self.joined.as_ref().expect("the member joined group g")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the group coordinator is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn group() -> ClassicGroupScenario {
    // Each scenario owns fresh services and observations.
    ClassicGroupScenario {
        broker: None,
        rebalance: millis(1_500),
        first_join: None,
        joined: None,
        synced: None,
        heartbeat_error: None,
        commit_error: None,
        fetched_offset: None,
        leave_error: None,
        topic_id: None,
    }
}

#[given("a single broker ready to coordinate classic group g")]
async fn group_coordinator(group: &mut ClassicGroupScenario) {
    group.broker = Some(start_ready_group().await);
}

#[given("the classic rebalance timeout is {timeout} milliseconds")]
fn rebalance_timeout(group: &mut ClassicGroupScenario, timeout: u32) {
    group.rebalance = millis(timeout);
}

#[given("group offsets refer to an existing single-partition topic t")]
async fn offset_topic(group: &mut ClassicGroupScenario) {
    create_topic(group.broker(), "t", 1).await;
    group.topic_id = Some(topic_id_for(&group.broker().client, "t").await);
}

#[when("a consumer joins group g without a member id")]
async fn initial_join(group: &mut ClassicGroupScenario) {
    group.first_join = Some(
        group
            .broker()
            .client
            .send(classic_join_request_from(group.setup()))
            .await
            .expect("JoinGroup1"),
    );
}

#[then("the broker requires the newly assigned nonempty member id")]
fn assigned_member(group: &ClassicGroupScenario) {
    let response = group
        .first_join
        .as_ref()
        .expect("the initial join completed");
    assert!(response.error_code == 79);
    assert!(!response.member_id.is_empty());
}

#[when("the consumer rejoins with its assigned member id")]
async fn rejoin(group: &mut ClassicGroupScenario) {
    let first = group
        .first_join
        .as_ref()
        .expect("the initial join completed");
    let member_id = required_member_id(first.error_code, first.member_id.clone());
    let setup = group.setup();
    group.joined = Some(
        group
            .broker()
            .client
            .send(
                classic_join_request()
                    .group_id(setup.group_id)
                    .member_id(member_id)
                    .timeouts(setup.timeouts)
                    .protocol_type(setup.protocol_type)
                    .protocols(setup.protocols)
                    .call(),
            )
            .await
            .expect("JoinGroup2"),
    );
}

#[then("the single consumer becomes leader and sees itself in the member list")]
fn joined_as_leader(group: &ClassicGroupScenario) {
    let first = group
        .first_join
        .as_ref()
        .expect("the initial join completed");
    let response = group.member();
    check!(response.error_code == 0);
    check!(response.leader == first.member_id);
    check!(response.member_id == first.member_id);
    check!(!response.members.is_empty(), "leader sees member list");
}

#[when("the group leader syncs assignment asgn for itself")]
async fn sync_group(group: &mut ClassicGroupScenario) {
    let member = group.member();
    let request = classic_sync_request()
        .group_id("g".into())
        .generation_id(member.generation_id)
        .member_id(member.member_id.clone())
        .assignments(vec![sync_assignment(
            member.member_id.clone(),
            Bytes::from_static(b"asgn"),
        )])
        .call();
    group.synced = Some(
        group
            .broker()
            .client
            .send(request)
            .await
            .expect("SyncGroup"),
    );
}

#[then("group synchronization succeeds and returns assignment asgn")]
fn synchronized_assignment(group: &ClassicGroupScenario) {
    let response = group.synced.as_ref().expect("SyncGroup completed");
    assert!(response.error_code == 0);
    assert!(response.assignment.as_ref() == b"asgn");
}

#[when("the member heartbeats in its joined generation")]
async fn heartbeat(group: &mut ClassicGroupScenario) {
    let member = group.member();
    let response = group
        .broker()
        .client
        .send(HeartbeatRequest {
            group_id: "g".into(),
            generation_id: member.generation_id,
            member_id: member.member_id.clone(),
            ..Default::default()
        })
        .await
        .expect("Heartbeat");
    group.heartbeat_error = Some(response.error_code);
}

#[then("the group heartbeat succeeds")]
fn heartbeat_accepted(group: &ClassicGroupScenario) {
    assert!(group.heartbeat_error == Some(0));
}

#[when("the member commits offset 42 for topic t partition zero")]
async fn commit_offset(group: &mut ClassicGroupScenario) {
    let member = group.member();
    let response = group
        .broker()
        .client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: member.generation_id,
            member_id: member.member_id.clone(),
            topics: vec![offset_commit_topic(
                "t",
                group.topic_id.expect("topic t was created"),
                vec![OffsetCommitRequestPartition {
                    committed_leader_epoch: 0,
                    ..offset_commit_partition(0, 42, Some(String::new()))
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    group.commit_error = Some(response.topics[0].partitions[0].error_code);
}

#[then("the group offset commit succeeds")]
fn committed_offset(group: &ClassicGroupScenario) {
    assert!(group.commit_error == Some(0));
}

#[when("the client fetches group g committed offset for topic t partition zero")]
async fn fetch_offset(group: &mut ClassicGroupScenario) {
    let response = group
        .broker()
        .client
        .send(offset_fetch_request(offset_fetch_group(
            "g",
            Some(vec![offset_fetch_topic(
                "t",
                group.topic_id.expect("topic t was created"),
                vec![0],
            )]),
        )))
        .await
        .expect("OffsetFetch");
    group.fetched_offset = Some(response.groups[0].topics[0].partitions[0].committed_offset);
}

#[then("the fetched committed offset is 42")]
fn fetched_committed_offset(group: &ClassicGroupScenario) {
    assert!(group.fetched_offset == Some(42));
}

#[when("the member leaves group g")]
async fn leave_group(group: &mut ClassicGroupScenario) {
    let response = group
        .broker()
        .client
        .send(LeaveGroupRequest {
            group_id: "g".into(),
            member_id: group.member().member_id.clone(),
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    group.leave_error = Some(response.error_code);
}

#[then("the group leave succeeds")]
fn left_group(group: &ClassicGroupScenario) {
    assert!(group.leave_error == Some(0));
}

#[scenario(
    path = "tests/features/migrated/tests/unit/consumer_group.feature",
    name = "join group with empty member returns member id required"
)]
#[tokio::test]
async fn join_group_with_empty_member_returns_member_id_required(group: ClassicGroupScenario) {
    group.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/consumer_group.feature",
    name = "join group single member completes after deadline"
)]
#[tokio::test]
async fn join_group_single_member_completes_after_deadline(group: ClassicGroupScenario) {
    group.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/consumer_group.feature",
    name = "full group flow join sync heartbeat commit fetch leave"
)]
#[tokio::test]
async fn full_group_flow_join_sync_heartbeat_commit_fetch_leave(group: ClassicGroupScenario) {
    group.shutdown().await;
}
