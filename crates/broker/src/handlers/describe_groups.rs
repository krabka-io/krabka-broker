//! `DescribeGroups` (`api_key=15`). The response holds one entry per requested
//! `group_id`.
//!
//! Each member carries its `JoinGroup` protocol metadata (`member_metadata`)
//! and its current assignment bytes. The group reports its selected protocol
//! name (`protocol_data`) and its stored `protocol_type`. That type is `""`
//! for a typeless or dead group, which matches Kafka.
//!
//! KIP-430: when the request sets `include_authorized_operations`, each Allow
//! row carries a bitfield of the group operations that the principal may
//! perform. A row that fails the auth check, and a row for a group that does
//! not exist, keeps the `i32::MIN` "not present" sentinel.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup, DescribedGroupMember},
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::{
        GroupType, classic_state::GroupState, streams::actor::StreamsGroupActorMessage,
    },
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

#[tracing::instrument(
    name = "handle_describe_groups",
    level = "info",
    skip_all,
    fields(api = "DescribeGroups", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeGroupsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
    for gid in req.groups {
        // ── ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny → per-group
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code,
                ..Default::default()
            });
            continue;
        }

        // KIP-1071: a Streams-locked group's offset home is a drained classic
        // actor; describing it via the classic projection would mislabel it.
        // Report its streams identity (full task detail lives in
        // StreamsGroupDescribe, api 89). Exact protocol_type/state is matched
        // empirically (spec §7.4); the firm contract is "not classic/consumer".
        if broker.group_coordinator.group_type(&gid) == Some(GroupType::Streams)
            && let Some(handle) = broker.group_coordinator.find_streams(&gid)
        {
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
            {
                groups.push(DescribedGroup {
                    group_id: gid,
                    protocol_type: "streams".into(),
                    group_state: view.group_state,
                    error_code: codes::NONE,
                    ..Default::default()
                });
                continue;
            }
            // Streams-locked but no live streams actor (e.g. just downgraded) →
            // fall through to the classic describe path below.
        }

        let Some(snap) = broker.group_coordinator.describe_group(&gid).await else {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        };
        let state_str = state_to_str(snap.state);
        let members = snap
            .members
            .into_iter()
            .map(|m| DescribedGroupMember {
                member_id: m.member_id,
                client_id: m.client_id,
                client_host: m.client_host,
                // MemberSnapshot.{protocol_metadata,assignment} are Vec<u8>;
                // wire type is Bytes.
                member_metadata: m.protocol_metadata.into(),
                member_assignment: m.assignment.into(),
                ..Default::default()
            })
            .collect();
        // KIP-430: bitfield of group operations alice@host is authorized
        // for, when the request opted in. Otherwise the wire-default
        // `i32::MIN` "not present" sentinel is preserved.
        let authorized = if req.include_authorized_operations {
            authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                ResourceType::Group,
                snap.group_id.as_str(),
            )
        } else {
            i32::MIN
        };
        groups.push(DescribedGroup {
            group_id: snap.group_id,
            // Kafka returns "" for a typeless/dead group; real consumer
            // groups already carry Some("consumer").
            protocol_type: snap.protocol_type.clone().unwrap_or_default(),
            // Selected protocol NAME (e.g. "range"); "" for an empty group.
            protocol_data: snap.protocol_name.clone().unwrap_or_default(),
            group_state: state_str.into(),
            error_code: codes::NONE,
            members,
            authorized_operations: authorized,
            ..Default::default()
        });
    }

    let resp = DescribeGroupsResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

fn state_to_str(s: GroupState) -> &'static str {
    match s {
        GroupState::Empty => "Empty",
        GroupState::PreparingRebalance => "PreparingRebalance",
        GroupState::CompletingRebalance => "CompletingRebalance",
        GroupState::Stable => "Stable",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = krabka_protocol::owned::describe_groups_response::MAX_VERSION;

    crate::test_support::wire_helpers!(
        DescribeGroupsRequest,
        DescribeGroupsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn request(groups: &[&str], include_ops: bool) -> DescribeGroupsRequest {
        DescribeGroupsRequest {
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: include_ops,
            ..Default::default()
        }
    }

    /// A `DescribedGroup` carrying only an error: every projection field keeps
    /// its wire default, `authorized_operations` included.
    fn error_row(group_id: &str, error_code: i16) -> DescribedGroup {
        DescribedGroup {
            error_code,
            error_message: None,
            group_id: group_id.to_string(),
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: vec![],
            authorized_operations: i32::MIN,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    async fn drive(broker: &Broker, req: &DescribeGroupsRequest) -> DescribeGroupsResponse {
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        let bytes = handle(broker, VERSION, 123, &encode_request(req), &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    /// A Deny on `Describe Group` answers the row, not the request: each named
    /// group gets its own `GROUP_AUTHORIZATION_FAILED` and the coordinator is
    /// never consulted.
    #[tokio::test]
    async fn a_denied_group_is_refused_per_row() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(&broker, &request(&["group-a", "group-b"], false)).await;

        assert!(
            resp == DescribeGroupsResponse {
                throttle_time_ms: 0,
                groups: vec![
                    error_row("group-a", codes::GROUP_AUTHORIZATION_FAILED),
                    error_row("group-b", codes::GROUP_AUTHORIZATION_FAILED),
                ],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }
        );
        broker_handle.shutdown().await;
    }

    /// An allowed group the coordinator has never seen is `GROUP_ID_NOT_FOUND`,
    /// which is distinct from the authorization refusal above.
    #[tokio::test]
    async fn an_unknown_group_is_not_found() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(&broker, &request(&["never-seen"], false)).await;

        assert!(
            resp == DescribeGroupsResponse {
                throttle_time_ms: 0,
                groups: vec![error_row("never-seen", codes::GROUP_ID_NOT_FOUND)],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }
        );
        broker_handle.shutdown().await;
    }

    /// A live classic group is projected with its state and, per Kafka, the
    /// empty string for the `protocol_type` and `protocol_data` a group that
    /// has not yet joined a protocol carries. Without the KIP-430 flag the
    /// bitfield keeps the `i32::MIN` "not present" sentinel, which is what
    /// separates this row from `the_authorized_operations_bitfield_is_filled_only_on_opt_in`.
    #[tokio::test]
    async fn a_classic_group_is_projected_without_the_kip430_bitfield() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker.group_coordinator.get_or_create_classic("classic-a");

        let resp = drive(&broker, &request(&["classic-a"], false)).await;

        assert!(
            resp == DescribeGroupsResponse {
                throttle_time_ms: 0,
                groups: vec![DescribedGroup {
                    error_code: codes::NONE,
                    error_message: None,
                    group_id: "classic-a".into(),
                    group_state: "Empty".into(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: vec![],
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            },
            "{resp:?}"
        );
        broker_handle.shutdown().await;
    }

    /// KIP-430: with the flag set the row carries the group operations the
    /// principal holds, so the field moves off its sentinel.
    #[tokio::test]
    async fn the_authorized_operations_bitfield_is_filled_only_on_opt_in() {
        let authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        let (broker_handle, _dir) = start_broker(Arc::clone(&authorizer) as _).await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker.group_coordinator.get_or_create_classic("classic-a");

        let resp = drive(&broker, &request(&["classic-a"], true)).await;

        let p = principal("admin");
        let peer = peer();
        let expected = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &p,
            &peer,
            ResourceType::Group,
            "classic-a",
        );
        assert!(expected != i32::MIN);
        assert!(
            resp.groups
                .iter()
                .map(|g| (g.group_id.as_str(), g.error_code, g.authorized_operations))
                .collect::<Vec<_>>()
                == vec![("classic-a", codes::NONE, expected)]
        );
        broker_handle.shutdown().await;
    }

    /// The `GroupState` -> Kafka string projection is exhaustive; every state a
    /// classic group can report has its own name on the wire.
    #[test]
    fn every_group_state_has_its_kafka_name() {
        assert!(
            [
                GroupState::Empty,
                GroupState::PreparingRebalance,
                GroupState::CompletingRebalance,
                GroupState::Stable,
            ]
            .map(state_to_str)
                == ["Empty", "PreparingRebalance", "CompletingRebalance", "Stable"]
        );
    }
}
