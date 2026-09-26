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

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{sync_group_request::SyncGroupRequest, sync_group_response::SyncGroupResponse},
};
use krabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::actor::GroupActorMessage, error::BrokerError,
    handlers::group_read_denied,
};

#[tracing::instrument(
    name = "handle_sync_group",
    level = "info",
    skip_all,
    fields(api = "SyncGroup", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: crate::handlers::ApiVersion,
    _correlation_id: crate::handlers::CorrelationId,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coordinator = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = SyncGroupRequest::decode(&mut cur, version)?;

        // Kafka's `KafkaApis.handleSyncGroupRequest` answers a v5+ request
        // without a protocol type or name before the ACL check
        // (`SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`).
        if !mandatory_protocol_type_and_name_present(&req, version) {
            return encode_err(version, codes::INCONSISTENT_GROUP_PROTOCOL);
        }

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        {
            let image = broker.controller.current_image();
            if group_read_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                &req.group_id,
            ) {
                return encode_err(version, codes::GROUP_AUTHORIZATION_FAILED);
            }
        }

        // Kafka's `GroupCoordinatorService.syncGroup` answers an empty group
        // id before any group lookup.
        let invalid_group = req.group_id.is_empty().then_some(codes::INVALID_GROUP_ID);
        if let Some(error_code) = invalid_group
            .or_else(|| crate::handlers::group_coordinator_error(broker, &req.group_id))
        {
            return encode_err(version, error_code);
        }

        let Some(handle) = coordinator.find(&req.group_id) else {
            return encode_err(version, codes::UNKNOWN_MEMBER_ID);
        };

        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::ClassicSync { req, reply: tx })
            .await
            .is_err()
        {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS);
        }
        // The leader and the already-Stable follower reply immediately; a
        // not-yet-synced follower is parked and resolved when the leader's
        // SyncGroup installs assignments, bounded by the configured follower wait.
        let Ok(Ok(result)) =
            tokio::time::timeout(broker.config.sync_group_follower_wait.to_std(), rx).await
        else {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS);
        };

        let resp = SyncGroupResponse {
            error_code: result.error_code,
            assignment: result.assignment,
            protocol_type: result.protocol_type,
            protocol_name: result.protocol_name,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

/// Kafka's `SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`: from v5
/// the request must name both its protocol type and its protocol name.
fn mandatory_protocol_type_and_name_present(
    req: &SyncGroupRequest,
    version: crate::handlers::ApiVersion,
) -> bool {
    version < 5 || (req.protocol_type.is_some() && req.protocol_name.is_some())
}

/// Kafka's `SyncGroupRequest.getErrorResponse`: only `error_code` is set, so
/// the protocol type and name stay null and the assignment stays empty.
fn encode_err(
    version: crate::handlers::ApiVersion,
    code: crate::handlers::ErrorCode,
) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::owned::{
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::{self, JoinGroupResponse},
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
        sync_group_response::{self, SyncGroupResponse},
    };
    use krabka_security::Principal;

    use crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        test_support::{DenyAll, encode_request},
    };

    const GROUP: &str = "sync-group-unit";
    const PROTOCOL_TYPE: &str = "consumer";
    const PROTOCOL_NAME: &str = "range";

    fn decode_join(bytes: &Bytes) -> JoinGroupResponse {
        crate::test_support::decode_response(bytes, join_group_response::MAX_VERSION)
    }

    fn decode_sync(bytes: &Bytes) -> SyncGroupResponse {
        crate::test_support::decode_response(bytes, sync_group_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "sync-group-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn bootstrap_member(
        broker: &Broker,
        ctx: &crate::handlers::RequestContext<'_>,
    ) -> (String, i32) {
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

        let r1 = crate::handlers::join_group::handle(
            broker,
            version,
            1,
            &encode_request(&join(String::new()), version),
            ctx,
        )
        .await
        .expect("JoinGroup bootstrap");
        let r1 = decode_join(&r1);
        assert!(r1.error_code == codes::MEMBER_ID_REQUIRED, "{r1:?}");
        assert!(!r1.member_id.is_empty());

        let r2 = crate::handlers::join_group::handle(
            broker,
            version,
            2,
            &encode_request(&join(r1.member_id.clone()), version),
            ctx,
        )
        .await
        .expect("JoinGroup rejoin");
        let r2 = decode_join(&r2);
        assert!(
            (
                r2.error_code,
                r2.protocol_type.as_deref(),
                r2.protocol_name.as_deref()
            ) == (codes::NONE, Some(PROTOCOL_TYPE), Some(PROTOCOL_NAME)),
            "{r2:?}"
        );
        (r2.member_id, r2.generation_id)
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "sync-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let bytes = encode_err(
            sync_group_response::MAX_VERSION,
            codes::GROUP_AUTHORIZATION_FAILED,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = SyncGroupResponse::decode(&mut cur, sync_group_response::MAX_VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    /// #736 and #793: from v5 a missing protocol type or name answers
    /// `INCONSISTENT_GROUP_PROTOCOL` before the group ACL, and an empty group
    /// id answers `INVALID_GROUP_ID` after it. Every error reply carries only
    /// its `error_code`, as Kafka's `SyncGroupRequest.getErrorResponse`.
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order() {
        struct Row {
            name: &'static str,
            version: i16,
            group_id: &'static str,
            protocol_type: Option<&'static str>,
            protocol_name: Option<&'static str>,
            allowed: bool,
            want: i16,
        }
        let rows = [
            Row {
                name: "v5 null type, denied",
                version: 5,
                group_id: GROUP,
                protocol_type: None,
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::INCONSISTENT_GROUP_PROTOCOL,
            },
            Row {
                name: "v5 null name, allowed",
                version: 5,
                group_id: GROUP,
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: None,
                allowed: true,
                want: codes::INCONSISTENT_GROUP_PROTOCOL,
            },
            Row {
                name: "v4 null both, allowed: normal path to an unknown group",
                version: 4,
                group_id: GROUP,
                protocol_type: None,
                protocol_name: None,
                allowed: true,
                want: codes::UNKNOWN_MEMBER_ID,
            },
            Row {
                name: "v5 both present, denied",
                version: 5,
                group_id: GROUP,
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::GROUP_AUTHORIZATION_FAILED,
            },
            Row {
                name: "empty group id, allowed",
                version: 5,
                group_id: "",
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: true,
                want: codes::INVALID_GROUP_ID,
            },
            Row {
                name: "empty group id, denied",
                version: 5,
                group_id: "",
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::GROUP_AUTHORIZATION_FAILED,
            },
        ];
        let (denied_handle, _denied_dir) = start_broker(Arc::new(DenyAll)).await;
        let (allowed_handle, _allowed_dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);

        for r in rows {
            let broker = if r.allowed {
                allowed_handle.broker_arc_for_test()
            } else {
                denied_handle.broker_arc_for_test()
            };
            let req = SyncGroupRequest {
                group_id: r.group_id.into(),
                member_id: "member-a".into(),
                generation_id: 1,
                protocol_type: r.protocol_type.map(String::from),
                protocol_name: r.protocol_name.map(String::from),
                ..Default::default()
            };

            let resp = handle(
                &broker,
                r.version,
                3,
                &encode_request(&req, r.version),
                &ctx,
            )
            .await
            .expect("SyncGroup");
            let resp: SyncGroupResponse = crate::test_support::decode_response(&resp, r.version);

            let expected = SyncGroupResponse {
                error_code: r.want,
                ..Default::default()
            };
            assert!(resp == expected, "{}: {resp:?}", r.name);
        }
        denied_handle.shutdown().await;
        allowed_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_group_read_and_preserves_error_response_shape() {
        let version = sync_group_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = SyncGroupRequest {
            group_id: GROUP.into(),
            member_id: "member-a".into(),
            generation_id: 1,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            ..Default::default()
        };

        let resp = handle(&broker, version, 3, &encode_request(&req, version), &ctx)
            .await
            .expect("SyncGroup");
        let resp = decode_sync(&resp);

        let expected = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_success_preserves_assignment_and_kip559_protocol_fields() {
        let version = sync_group_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let (member_id, generation_id) = bootstrap_member(&broker, &ctx).await;
        let assignment = Bytes::from_static(b"assignment-payload");
        let req = SyncGroupRequest {
            group_id: GROUP.into(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id,
                assignment: assignment.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let resp = handle(&broker, version, 3, &encode_request(&req, version), &ctx)
            .await
            .expect("SyncGroup");
        let resp = decode_sync(&resp);

        let expected = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignment,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
