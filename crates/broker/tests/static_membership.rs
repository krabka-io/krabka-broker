//! KIP-345 static-membership integration tests.
//!
//! These tests exercise the full `JoinGroup` → `SyncGroup` → rejoin cycle,
//! where the rejoin uses the same `group.instance.id`, through the in-process
//! broker harness. They check the three KIP-345 invariants:
//!
//! 1. A static rejoin into a `Stable` group gets a new member id, fences
//!    the old one, keeps the prior assignment and does NOT advance
//!    `generation_id`.
//! 2. The broker rejects a second client that uses the same
//!    `group.instance.id` while the first is still live, with
//!    `FENCED_INSTANCE_ID`.
//! 3. `LeaveGroup` v3+ with a `MemberIdentity { member_id: "",
//!    group_instance_id: Some(...) }` resolves the static slot and
//!    removes it.
#![cfg(test)]

use {
    crate::support::classic::{
        ClassicTimeouts, classic_join_request, classic_sync_request, sync_assignment,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest,
        join_group_request::JoinGroupRequest,
        join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
        leave_group_request::{LeaveGroupRequest, MemberIdentity},
        sync_group_request::SyncGroupRequest,
    },
};

mod support;

/// `FENCED_INSTANCE_ID` (82, KIP-345).
const FENCED_INSTANCE_ID: i16 = 82;

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

fn join_request(group_id: &str, member_id: &str, instance_id: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: instance_id.map(str::to_owned),
        ..classic_join_request()
            .group_id((group_id).into())
            .member_id((member_id).into())
            .timeouts(ClassicTimeouts {
                rebalance: krabka_units::millis(1_500),
                ..Default::default()
            })
            .call()
    }
}

/// A static member, the generation it joined at and the assignment it was synced.
struct StaticMember {
    member_id: String,
    generation_id: i32,
    assignment: Bytes,
}

/// Boot a group with one static member and sync an assignment for it.
/// Returns the assigned member id, generation id and assignment.
async fn bootstrap_static_member(
    client: &krabka_client_core::Client,
    group_id: &str,
    instance_id: &str,
    assignment: Bytes,
) -> StaticMember {
    // 1. Empty member_id → a static member joins at once with a generated
    //    `<instance id>-<uuid>` member id and becomes the leader. Kafka's
    //    `MEMBER_ID_REQUIRED` round trip is for dynamic members only.
    let r1 = client
        .send(join_request(group_id, "", Some(instance_id)))
        .await
        .expect("JoinGroup");
    assert!(r1.error_code == 0);
    let mid = r1.member_id.clone();
    assert!(mid.starts_with(&format!("{instance_id}-")));
    assert!(r1.leader == mid);
    let generation = r1.generation_id;

    // 3. Leader SyncGroup installs an assignment for itself.
    let r3 = client
        .send(SyncGroupRequest {
            group_instance_id: Some(instance_id.into()),
            ..classic_sync_request()
                .group_id((group_id).into())
                .generation_id(generation)
                .member_id(mid.clone())
                .assignments(vec![sync_assignment(mid.clone(), assignment.clone())])
                .call()
        })
        .await
        .expect("SyncGroup");
    assert!(r3.error_code == 0);
    assert!(r3.assignment == assignment);

    StaticMember {
        member_id: mid,
        generation_id: generation,
        assignment,
    }
}

use {
    krabka_protocol::owned::{
        leave_group_response::LeaveGroupResponse, sync_group_response::SyncGroupResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct StaticMembership {
    process: Option<support::InProcess>,
    group: String,
    instance: String,
    incumbent: Option<StaticMember>,
    joined: Option<JoinGroupResponse>,
    heartbeat: Option<i16>,
    synced: Option<SyncGroupResponse>,
    left: Option<LeaveGroupResponse>,
}

#[fixture]
fn static_membership() -> StaticMembership {
    // Track the incumbent separately from replacement and intruder responses.
    StaticMembership::default()
}

#[given("a stable group {group} with static instance {instance} assigned {assignment}")]
async fn stable_static_group(
    static_membership: &mut StaticMembership,
    group: String,
    instance: String,
    assignment: String,
) {
    let process = start().await;
    static_membership.incumbent = Some(
        bootstrap_static_member(&process.client, &group, &instance, Bytes::from(assignment)).await,
    );
    static_membership.process = Some(process);
    static_membership.group = group;
    static_membership.instance = instance;
}

#[when("the static instance rejoins with an empty member ID")]
async fn static_rejoin(static_membership: &mut StaticMembership) {
    static_membership.joined = Some(
        static_membership
            .process
            .as_ref()
            .expect("group broker")
            .client
            .send(join_request(
                &static_membership.group,
                "",
                Some(&static_membership.instance),
            ))
            .await
            .expect("static rejoin"),
    );
}

#[then("the replacement keeps the generation and becomes leader without reassignment")]
fn static_replacement(static_membership: &StaticMembership) {
    let incumbent = static_membership
        .incumbent
        .as_ref()
        .expect("incumbent member");
    let rejoin = static_membership.joined.as_ref().expect("rejoin response");
    let member = rejoin.member_id.clone();
    check!(member != incumbent.member_id);
    check!(member.starts_with(&format!("{}-", static_membership.instance)));
    check!(
        rejoin
            == &JoinGroupResponse {
                generation_id: incumbent.generation_id,
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                leader: member.clone(),
                skip_assignment: true,
                member_id: member.clone(),
                members: vec![JoinGroupResponseMember {
                    member_id: member,
                    group_instance_id: Some(static_membership.instance.clone()),
                    metadata: Bytes::new(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        "static rejoin must keep generation and replace member ID"
    );
}

#[when("a member named imposter-member-id joins the incumbent static instance")]
async fn static_intruder(static_membership: &mut StaticMembership) {
    static_membership.joined = Some(
        static_membership
            .process
            .as_ref()
            .expect("group broker")
            .client
            .send(join_request(
                &static_membership.group,
                "imposter-member-id",
                Some(&static_membership.instance),
            ))
            .await
            .expect("intruder JoinGroup"),
    );
}

#[then("the intruder join is fenced with FENCED_INSTANCE_ID")]
fn intruder_fenced(static_membership: &StaticMembership) {
    assert!(
        static_membership
            .joined
            .as_ref()
            .expect("intruder response")
            .error_code
            == FENCED_INSTANCE_ID
    );
}

#[when("the {member} member heartbeats with the static instance ID")]
async fn static_heartbeat(static_membership: &mut StaticMembership, member: String) {
    let incumbent = static_membership
        .incumbent
        .as_ref()
        .expect("incumbent member");
    let member_id = match member.as_str() {
        "incumbent" => incumbent.member_id.clone(),
        "wrong" => "wrong-member-id".into(),
        _ => panic!("unknown member {member}"),
    };
    let response = static_membership
        .process
        .as_ref()
        .expect("group broker")
        .client
        .send(HeartbeatRequest {
            group_id: static_membership.group.clone(),
            generation_id: incumbent.generation_id,
            member_id,
            group_instance_id: Some(static_membership.instance.clone()),
            ..Default::default()
        })
        .await
        .expect("static Heartbeat");
    static_membership.heartbeat = Some(response.error_code);
}

#[then("the static heartbeat returns error {error}")]
fn static_heartbeat_code(static_membership: &StaticMembership, error: i16) {
    assert!(static_membership.heartbeat == Some(error));
}

#[when("the replacement member requests its existing assignment")]
async fn replacement_assignment(static_membership: &mut StaticMembership) {
    static_membership.synced = Some(
        static_membership
            .process
            .as_ref()
            .expect("group broker")
            .client
            .send(SyncGroupRequest {
                group_id: static_membership.group.clone(),
                generation_id: static_membership
                    .incumbent
                    .as_ref()
                    .expect("incumbent")
                    .generation_id,
                member_id: static_membership
                    .joined
                    .as_ref()
                    .expect("replacement")
                    .member_id
                    .clone(),
                group_instance_id: Some(static_membership.instance.clone()),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                ..Default::default()
            })
            .await
            .expect("replacement SyncGroup"),
    );
}

#[then("the replacement receives the incumbent's assignment successfully")]
fn assignment_preserved(static_membership: &StaticMembership) {
    let synced = static_membership
        .synced
        .as_ref()
        .expect("replacement SyncGroup answered");
    check!(synced.error_code == 0);
    check!(
        synced.assignment
            == static_membership
                .incumbent
                .as_ref()
                .expect("incumbent")
                .assignment
    );
}

#[when("the static instance leaves with an empty member ID")]
async fn instance_leave(static_membership: &mut StaticMembership) {
    static_membership.left = Some(
        static_membership
            .process
            .as_ref()
            .expect("group broker")
            .client
            .send(LeaveGroupRequest {
                group_id: static_membership.group.clone(),
                member_id: String::new(),
                members: vec![MemberIdentity {
                    member_id: String::new(),
                    group_instance_id: Some(static_membership.instance.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("instance LeaveGroup"),
    );
}

#[then("the leave response echoes the static instance and succeeds for its single member")]
fn instance_left(static_membership: &StaticMembership) {
    let response = static_membership
        .left
        .as_ref()
        .expect("LeaveGroup answered");
    check!(response.error_code == 0);
    assert!(response.members.len() == 1);
    check!(response.members[0].error_code == 0);
    check!(
        response.members[0].group_instance_id.as_deref()
            == Some(static_membership.instance.as_str())
    );
}

#[when("the departed incumbent heartbeats without a static instance ID")]
async fn departed_heartbeat(static_membership: &mut StaticMembership) {
    let response = static_membership
        .process
        .as_ref()
        .expect("group broker")
        .client
        .send(HeartbeatRequest {
            group_id: static_membership.group.clone(),
            generation_id: 1,
            member_id: static_membership
                .incumbent
                .as_ref()
                .expect("incumbent")
                .member_id
                .clone(),
            ..Default::default()
        })
        .await
        .expect("Heartbeat after leave");
    static_membership.heartbeat = Some(response.error_code);
}

#[then("the departed incumbent's heartbeat is rejected")]
fn departed_member_rejected(static_membership: &StaticMembership) {
    assert!(
        static_membership
            .heartbeat
            .expect("departed heartbeat answered")
            != 0
    );
}

#[scenario(
    path = "tests/features/migrated/tests/static_membership.feature",
    name = "static rejoin preserves assignment and generation"
)]
#[tokio::test]
async fn static_rejoin_preserves_assignment_and_generation(static_membership: StaticMembership) {
    static_membership
        .process
        .take()
        .expect("group broker")
        .broker
        .shutdown()
        .await;
    drop(static_membership);
}

#[scenario(
    path = "tests/features/migrated/tests/static_membership.feature",
    name = "second client with same instance id is fenced"
)]
#[tokio::test]
async fn second_client_with_same_instance_id_is_fenced(static_membership: StaticMembership) {
    static_membership
        .process
        .take()
        .expect("group broker")
        .broker
        .shutdown()
        .await;
    drop(static_membership);
}

#[scenario(
    path = "tests/features/migrated/tests/static_membership.feature",
    name = "leave group resolves static member by instance id"
)]
#[tokio::test]
async fn leave_group_resolves_static_member_by_instance_id(static_membership: StaticMembership) {
    static_membership
        .process
        .take()
        .expect("group broker")
        .broker
        .shutdown()
        .await;
    drop(static_membership);
}
