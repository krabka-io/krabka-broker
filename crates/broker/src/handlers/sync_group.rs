//! `SyncGroup` (`api_key=14`). The handler routes the request into the group's
//! unified actor as a `ClassicSync` message.
//!
//! The leader's call installs the assignments, and the actor then releases the
//! parked followers. The actor parks a follower that has no assignment yet
//! until that point, up to the broker's configured follower wait.
//!
//! From v5, KIP-559 makes the response carry `protocol_type` and
//! `protocol_name`, so an L7 proxy can route the call without remembering the
//! earlier `JoinGroup` exchange.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{ApiVersion, ErrorCodeResponse as _, RequestContext},
        task_util::ask,
    },
    krabka_protocol::owned::{
        sync_group_request::SyncGroupRequest, sync_group_response::SyncGroupResponse,
    },
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
    tokio::time,
};

pub async fn handle(
    broker: &Broker,
    req: SyncGroupRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<SyncGroupResponse, BrokerError> {
    use crate::{
        coordinator::unified::actor::GroupActorMessage, handlers::acl_gates::classic_group_refusal,
    };

    let coordinator = Arc::clone(&broker.group_coordinator);

    // Kafka's `KafkaApis.handleSyncGroupRequest` answers a v5+ request
    // without a protocol type or name before the ACL check
    // (`SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`).
    if !mandatory_protocol_type_and_name_present(&req, version) {
        return Ok(SyncGroupResponse::error(codes::INCONSISTENT_GROUP_PROTOCOL));
    }

    // Kafka's `GroupCoordinatorService.syncGroup` answers an empty group
    // id before any group lookup.
    if let Some(error_code) = classic_group_refusal(broker, ctx, &req.group_id) {
        return Ok(SyncGroupResponse::error(error_code));
    }

    let Some(handle) = coordinator.find(&req.group_id) else {
        return Ok(SyncGroupResponse::error(codes::UNKNOWN_MEMBER_ID));
    };

    // The leader and the already-Stable follower reply immediately; a
    // not-yet-synced follower is parked and resolved when the leader's
    // SyncGroup installs assignments, bounded by the configured follower wait.
    // A closed mailbox, a dropped reply and the wait running out all answer
    // REBALANCE_IN_PROGRESS.
    let asked = ask(&handle.tx, |reply| GroupActorMessage::ClassicSync {
        req,
        reply,
    });
    let Ok(Ok(result)) =
        time::timeout(broker.config.sync_group_follower_wait.to_std(), asked).await
    else {
        return Ok(SyncGroupResponse::error(codes::REBALANCE_IN_PROGRESS));
    };

    Ok(SyncGroupResponse {
        error_code: result.error_code,
        assignment: result.assignment,
        protocol_type: result.protocol_type,
        protocol_name: result.protocol_name,
        ..Default::default()
    })
}

/// Kafka's `SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`: from v5
/// the request must name both its protocol type and its protocol name.
fn mandatory_protocol_type_and_name_present(req: &SyncGroupRequest, version: ApiVersion) -> bool {
    version < 5 || (req.protocol_type.is_some() && req.protocol_name.is_some())
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            authorizer::{AllowAllAuthorizer, Authorizer},
            broker::Broker,
            handlers::{RequestContext, join_group::handle as join_group_handle},
            test_support::{
                self as broker_fixture_support, DenyAll, request_context,
                start_group_broker_no_audit,
            },
        },
        assert2::assert,
        bytes::Bytes,
        krabka_protocol::owned::{
            join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
            join_group_response,
            sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
            sync_group_response::{self, SyncGroupResponse},
        },
        krabka_security::Principal,
        std::{net::SocketAddr, sync::Arc},
    };

    const GROUP: &str = "sync-group-unit";
    const PROTOCOL_TYPE: &str = "consumer";
    const PROTOCOL_NAME: &str = "range";

    #[bon::builder]
    fn sync_request(
        member_id: String,
        generation_id: i32,
        #[builder(default)] assignments: Vec<SyncGroupRequestAssignment>,
    ) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: GROUP.into(),
            member_id,
            generation_id,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments,
            ..Default::default()
        }
    }

    fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        request_context(principal, peer, "sync-group-client")
    }

    /// A member that joined the group, and the generation it joined.
    struct JoinedMember {
        member_id: String,
        generation_id: i32,
    }

    async fn bootstrap_member(broker: &Broker, ctx: &RequestContext<'_>) -> JoinedMember {
        let version = join_group_response::MAX_VERSION;
        let join = |member_id: String| JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: PROTOCOL_TYPE.into(),
            member_id,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL_NAME.into(),
                metadata: Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let r1 = join_group_handle(broker, join(String::new()), version, ctx)
            .await
            .expect("JoinGroup bootstrap");
        assert!(r1.error_code == codes::MEMBER_ID_REQUIRED, "{r1:?}");
        assert!(!r1.member_id.is_empty());

        let r2 = join_group_handle(broker, join(r1.member_id.clone()), version, ctx)
            .await
            .expect("JoinGroup rejoin");
        assert!(
            (
                r2.error_code,
                r2.protocol_type.as_deref(),
                r2.protocol_name.as_deref()
            ) == (codes::NONE, Some(PROTOCOL_TYPE), Some(PROTOCOL_NAME)),
            "{r2:?}"
        );
        JoinedMember {
            member_id: r2.member_id,
            generation_id: r2.generation_id,
        }
    }

    use {
        super::*,
        crate::{
            handlers::group_read_denied,
            test_support::{EmptyAclImage, empty_acl_image},
        },
    };

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let EmptyAclImage { authorizer, image } = empty_acl_image();
        anonymous_request_identity!(principal, peer, ctx, "sync-client",);

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));
    }

    #[derive(Default)]
    struct SyncGroupValidationScenario {
        started: Option<HandlerStartedBroker>,
        request: Option<SyncGroupRequest>,
        version: i16,
        response: Option<SyncGroupResponse>,
    }
    #[rstest::fixture]
    fn sync_group_validation() -> SyncGroupValidationScenario {
        let started = None;
        SyncGroupValidationScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("the SyncGroup caller is authorized {allowed}")]
    async fn sync_group_authorization(
        sync_group_validation: &mut SyncGroupValidationScenario,
        allowed: bool,
    ) {
        let authorizer: Arc<dyn Authorizer> = if allowed {
            Arc::new(AllowAllAuthorizer)
        } else {
            Arc::new(DenyAll)
        };
        sync_group_validation.started = Some(start_group_broker_no_audit(authorizer).await);
    }
    #[rstest_bdd_macros::given(
        "a SyncGroup version {version} request names group {group} protocol type {protocol_type} and name {protocol_name}"
    )]
    fn sync_group_protocol_fields(
        sync_group_validation: &mut SyncGroupValidationScenario,
        version: i16,
        group: String,
        protocol_type: String,
        protocol_name: String,
    ) {
        sync_group_validation.version = version;
        sync_group_validation.request = Some(SyncGroupRequest {
            group_id: if group == "empty" {
                String::new()
            } else {
                group
            },
            member_id: "member-a".into(),
            generation_id: 1,
            protocol_type: if protocol_type == "null" {
                None
            } else {
                Some(protocol_type)
            },
            protocol_name: if protocol_name == "null" {
                None
            } else {
                Some(protocol_name)
            },
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when("alice synchronizes the requested group")]
    async fn synchronize_group(sync_group_validation: &mut SyncGroupValidationScenario) {
        let broker = sync_group_validation
            .started
            .as_ref()
            .unwrap()
            .broker
            .broker_arc_for_test();
        krabka_macros::request_test_identity! {bind(principal,peer,ctx);user ["alice"];context [context];};
        sync_group_validation.response = Some(
            handle(
                &broker,
                sync_group_validation.request.take().unwrap(),
                sync_group_validation.version,
                &ctx,
            )
            .await
            .expect("SyncGroup"),
        );
    }
    #[rstest_bdd_macros::then("the complete SyncGroup response contains only error {code}")]
    async fn check_sync_group_validation(
        sync_group_validation: &mut SyncGroupValidationScenario,
        code: i16,
    ) {
        assert!(
            sync_group_validation.response.as_ref().unwrap()
                == &SyncGroupResponse {
                    error_code: code,
                    ..Default::default()
                }
        );
        sync_group_validation
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=sync-group-unit (1)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=5, group=sync-group-unit (2)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order_case_2(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=4, group=sync-group-unit (3)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order_case_3(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=sync-group-unit (4)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order_case_4(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=true, version=5, group=empty (5)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order_case_5(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_sync_group_validation/sync.feature",
        name = "Protocol fields precede authorization and authorization precedes group identity: allowed=false, version=5, group=empty (6)"
    )]
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order_case_6(
        sync_group_validation: SyncGroupValidationScenario,
    ) {
        let _ = (sync_group_validation,);
    }

    #[tokio::test]
    async fn handle_denies_group_read_and_preserves_error_response_shape() {
        let version = sync_group_response::MAX_VERSION;
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
            context [context];
        };
        let req = sync_request()
            .member_id("member-a".into())
            .generation_id(1)
            .call();

        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("SyncGroup");

        let expected = krabka_macros::unthrottled_wire!(SyncGroupResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
        });
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_success_preserves_assignment_and_kip559_protocol_fields() {
        let version = sync_group_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [broker_fixture_support::start_group_broker_no_audit] allow_all;
        };
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
            context [context];
        };
        let JoinedMember {
            member_id,
            generation_id,
        } = bootstrap_member(&broker, &ctx).await;
        let assignment = Bytes::from_static(b"assignment-payload");
        let req = sync_request()
            .member_id(member_id.clone())
            .generation_id(generation_id)
            .assignments(vec![SyncGroupRequestAssignment {
                member_id,
                assignment: assignment.clone(),
                ..Default::default()
            }])
            .call();

        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("SyncGroup");

        let expected = krabka_macros::unthrottled_wire!(SyncGroupResponse {
            error_code: codes::NONE,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignment,
        });
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
