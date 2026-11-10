//! Live-broker tests for the `LeaveGroup` handler.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        test_support::{DenyAll, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_bdd::Given,
    krabka_protocol::owned::{
        leave_group_request::MemberIdentity,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
    },
};

async fn leave(broker: &Broker, request: &LeaveGroupRequest, version: i16) -> LeaveGroupResponse {
    krabka_broker_macros::request_test_identity! {
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

/// A `LeaveGroup` request and the version it is sent at.
struct VersionedLeave {
    version: i16,
    request: LeaveGroupRequest,
}

struct LeaveGroupScenario {
    started: Given<HandlerStartedBroker>,
    request: Given<VersionedLeave>,
    response: Given<LeaveGroupResponse>,
}

#[rstest::fixture]
fn leave_group() -> LeaveGroupScenario {
    LeaveGroupScenario {
        started: Given::new("started", "Given a group coordinator"),
        request: Given::new(
            "request",
            "Given a LeaveGroup version <version> request for group <group> with identities <identities>",
        ),
        response: Given::new(
            "response",
            "When the group coordinator handles the LeaveGroup request",
        ),
    }
}

#[krabka_bdd::given("a ready group coordinator with no group ghost")]
async fn empty_leave_group_coordinator(leave_group: &mut LeaveGroupScenario) {
    let started =
        start_broker_no_audit_with(|config| config.offsets_topic_replication_factor = 1).await;
    started.broker.wait_until_group_coordinator_ready().await;
    leave_group.started.set(started);
}

#[krabka_bdd::given("a group coordinator that denies every caller")]
async fn denying_leave_group_coordinator(leave_group: &mut LeaveGroupScenario) {
    leave_group
        .started
        .set(start_broker_no_audit_with(|config| config.authorizer = Arc::new(DenyAll)).await);
}

#[krabka_bdd::given("a group coordinator that allows every caller")]
async fn allowing_leave_group_coordinator(leave_group: &mut LeaveGroupScenario) {
    leave_group.started.set(
        start_broker_no_audit_with(|config| config.authorizer = Arc::new(AllowAllAuthorizer)).await,
    );
}

#[krabka_bdd::given(
    "a LeaveGroup version {version} request for group {group} with identities {identities}"
)]
fn leave_group_request(
    leave_group: &mut LeaveGroupScenario,
    version: i16,
    group: String,
    identities: String,
) {
    let group_id = if group == "empty" {
        String::new()
    } else {
        group
    };
    let request = match identities.as_str() {
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
    };
    leave_group.request.set(VersionedLeave { version, request });
}

#[krabka_bdd::when("the group coordinator handles the LeaveGroup request")]
async fn handle_leave_group(leave_group: &mut LeaveGroupScenario) {
    let broker = leave_group.started.get().broker.broker_arc_for_test();
    let VersionedLeave { version, request } = leave_group.request.get();
    let response = leave(&broker, request, *version).await;
    leave_group.response.set(response);
}

#[krabka_bdd::then("LeaveGroup refuses the whole request with error {code}")]
async fn check_whole_leave_refusal(leave_group: &mut LeaveGroupScenario, code: i16) {
    assert!(
        *leave_group.response.get()
            == LeaveGroupResponse {
                error_code: code,
                ..Default::default()
            }
    );
    leave_group.started.take().broker.shutdown().await;
}

#[krabka_bdd::then(
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
        *leave_group.response.get()
            == LeaveGroupResponse {
                error_code: code,
                members,
                ..Default::default()
            }
    );
    let started = leave_group.started.take();
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

krabka_bdd::scenarios!(
    "tests/features/handler_leave_group",
    fixtures = [leave_group: LeaveGroupScenario],
);

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
        member_id: if version < 3 {
            "m-a".into()
        } else {
            String::new()
        },
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
