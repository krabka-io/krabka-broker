//! Live-broker tests for the `LeaveGroup` handler.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        broker::Broker,
        test_support::{DenyAll, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_protocol::owned::{
        leave_group_request::MemberIdentity,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
    },
};

async fn leave(broker: &Broker, request: &LeaveGroupRequest, version: i16) -> LeaveGroupResponse {
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["alice"];
        client ["leave-client"];
    };
    handle(broker, request.clone(), version, &context)
        .await
        .expect("LeaveGroup")
}

fn identity(member_id: &str, group_instance_id: Option<&str>) -> MemberIdentity {
    MemberIdentity {
        member_id: member_id.into(),
        group_instance_id: group_instance_id.map(Into::into),
        ..Default::default()
    }
}

fn unknown_row(member_id: &str, group_instance_id: Option<&str>) -> MemberResponse {
    MemberResponse {
        member_id: member_id.into(),
        group_instance_id: group_instance_id.map(Into::into),
        error_code: codes::UNKNOWN_MEMBER_ID,
        ..Default::default()
    }
}

struct LeaveGroupScenario {
    started: Option<HandlerStartedBroker>,
    request: Option<LeaveGroupRequest>,
    version: i16,
    response: Option<LeaveGroupResponse>,
}
#[rstest::fixture]
fn leave_group() -> LeaveGroupScenario {
    let started = None;
    LeaveGroupScenario {
        started,
        request: None,
        version: 0,
        response: None,
    }
}
#[rstest_bdd_macros::given("a ready group coordinator with no group ghost")]
async fn empty_leave_group_coordinator(leave_group: &mut LeaveGroupScenario) {
    let started =
        start_broker_no_audit_with(|config| config.offsets_topic_replication_factor = 1).await;
    started.broker.wait_until_group_coordinator_ready().await;
    leave_group.started = Some(started);
}
#[rstest_bdd_macros::given(
    "a LeaveGroup version {version} request for group {group} with identities {identities}"
)]
fn leave_group_request(
    leave_group: &mut LeaveGroupScenario,
    version: i16,
    group: String,
    identities: String,
) {
    leave_group.version = version;
    let group_id = if group == "empty" {
        String::new()
    } else {
        group
    };
    leave_group.request = Some(match identities.as_str() {
        "legacy member m" => LeaveGroupRequest {
            group_id,
            member_id: "m".into(),
            ..Default::default()
        },
        "member m-a and static instance inst-b" => LeaveGroupRequest {
            group_id,
            members: vec![identity("m-a", None), identity("", Some("inst-b"))],
            ..Default::default()
        },
        "member m-a" => LeaveGroupRequest {
            group_id,
            members: vec![identity("m-a", None)],
            ..Default::default()
        },
        _ => panic!("unknown leave identities: {identities}"),
    });
}
#[rstest_bdd_macros::when("the group coordinator handles the LeaveGroup request")]
async fn handle_leave_group(leave_group: &mut LeaveGroupScenario) {
    let broker = leave_group
        .started
        .as_ref()
        .unwrap()
        .broker
        .broker_arc_for_test();
    leave_group.response = Some(
        leave(
            &broker,
            leave_group.request.as_ref().unwrap(),
            leave_group.version,
        )
        .await,
    );
}
#[rstest_bdd_macros::then(
    "LeaveGroup returns top-level error {code} with {members} and does not create the missing group"
)]
async fn check_leave_group_response(
    leave_group: &mut LeaveGroupScenario,
    code: i16,
    members: String,
) {
    let members = match members.as_str() {
        "no member rows" => Vec::new(),
        "unknown-member rows for both identities" => {
            vec![unknown_row("m-a", None), unknown_row("", Some("inst-b"))]
        }
        _ => panic!("unknown expected member rows: {members}"),
    };
    assert!(
        leave_group.response.as_ref().unwrap()
            == &LeaveGroupResponse {
                error_code: code,
                members,
                ..Default::default()
            }
    );
    let started = leave_group.started.take().unwrap();
    assert!(
        started
            .broker
            .broker_arc_for_test()
            .group_coordinator
            .find("ghost")
            .is_none()
    );
    started.broker.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_leave_group/leave.feature",
    name = "Missing and invalid groups use Kafka version-specific member errors"
)]
#[tokio::test]
async fn leave_group_answers_kafka_shapes_for_missing_and_invalid_groups(
    leave_group: LeaveGroupScenario,
    version: i16,
    group: String,
    identities: String,
    code: i16,
    members: String,
) {
    let _ = (leave_group, version, group, identities, code, members);
}

/// The `Read` check on the group runs first, so a denied principal gets
/// `GROUP_AUTHORIZATION_FAILED` even for an empty group id.
#[tokio::test]
async fn denied_read_answers_group_authorization_failed_before_validation() {
    krabka_macros::broker_test_setup! { bind(broker_handle, _dir, broker); boot [start_broker_no_audit_with(|config| config.authorizer = Arc::new(DenyAll))]; };
    let request = LeaveGroupRequest {
        group_id: String::new(),
        members: vec![identity("m-a", None)],
        ..Default::default()
    };

    let response = leave(&broker, &request, 5).await;

    assert!(
        response
            == LeaveGroupResponse {
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            }
    );
    broker_handle.shutdown().await;
}

/// An actor that stops with the leave in its mailbox drops the reply. Kafka
/// fails an operation on a shard that unloads, so the member must not be told
/// it left: the routing check runs again, and a broker that still serves the
/// group answers `COORDINATOR_NOT_AVAILABLE` at the top level.
#[rstest::rstest]
#[case::legacy(0)]
#[case::batched(5)]
#[tokio::test]
async fn dropped_reply_answers_coordinator_not_available(#[case] version: i16) {
    use crate::coordinator::unified::actor::{GroupActorHandle, GroupKindTag};

    let started =
        start_broker_no_audit_with(|config| config.offsets_topic_replication_factor = 1).await;
    started.broker.wait_until_group_coordinator_ready().await;
    let broker = started.broker.broker_arc_for_test();
    broker.group_coordinator.groups.insert(
        "stopping".into(),
        Arc::new(GroupActorHandle::dropping_replies(GroupKindTag::Classic)),
    );
    let request = LeaveGroupRequest {
        group_id: "stopping".into(),
        member_id: if version < 3 { "m-a".into() } else { String::new() },
        members: if version < 3 {
            Vec::new()
        } else {
            vec![identity("m-a", None)]
        },
        ..Default::default()
    };

    let response = leave(&broker, &request, version).await;

    assert!(
        response
            == LeaveGroupResponse {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            }
    );
    started.broker.shutdown().await;
}
