//! `DescribeGroups` (`api_key=15`). The response holds one entry per requested
//! `group_id`.
//!
//! The rows follow Kafka's `GroupMetadataManager.describeGroups`, which
//! describes only classic groups:
//!
//! - A `Stable` classic group reports its selected protocol name
//!   (`protocol_data`), and each member carries its `JoinGroup` metadata for
//!   that protocol (`member_metadata`). Any other state leaves both empty, as
//!   `ClassicGroupMember.describeNoMetadata` does.
//! - Every member carries its `group_instance_id` and its current assignment
//!   bytes, in every state.
//! - `protocol_type` is the stored type, or `""` for a typeless group.
//! - An unknown group, and a KIP-848 consumer, streams or share group, is
//!   answered in state `Dead`. From v6 the row carries `GROUP_ID_NOT_FOUND`
//!   and Kafka's message; below v6 its error is `NONE`, which older admin
//!   clients read as "the group does not exist".
//!
//! As in Kafka's `KafkaApis.handleDescribeGroupsRequest`, every group the
//! principal may not `Describe` is answered first with
//! `GROUP_AUTHORIZATION_FAILED`, ahead of the coordinator results for the
//! allowed groups.
//!
//! KIP-430: when the request sets `include_authorized_operations`, each
//! coordinator row whose error is `NONE` carries a bitfield of the group
//! operations that the principal may perform. Every other row keeps the
//! `i32::MIN` "not present" sentinel.

use bytes::Bytes;
use krabka_metadata::ResourceType;
use krabka_protocol::owned::{
    describe_groups_request::DescribeGroupsRequest,
    describe_groups_response::{DescribeGroupsResponse, DescribedGroup, DescribedGroupMember},
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::{
        GroupType,
        actor::{ClassicView, GroupActorMessage},
        classic_state::GroupState,
    },
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

/// The first `DescribeGroups` version whose unknown-group row carries
/// `GROUP_ID_NOT_FOUND`. Older versions answer `NONE` in state `Dead`.
const GROUP_ID_NOT_FOUND_MIN_VERSION: i16 = 6;

/// The first `DescribeGroups` version with `authorized_operations`.
const AUTHORIZED_OPERATIONS_MIN_VERSION: i16 = 3;

#[tracing::instrument(
    name = "handle_describe_groups",
    level = "info",
    skip_all,
    fields(api = "DescribeGroups", version),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeGroupsRequest,
    version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<DescribeGroupsResponse, BrokerError> {
    let image = broker.controller.current_image();

    // Kafka answers every GROUP_AUTHORIZATION_FAILED row first, then the
    // coordinator results for the allowed groups in request order.
    let mut denied: Vec<DescribedGroup> = Vec::new();
    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
    for gid in req.groups {
        if crate::handlers::group_describe_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            gid.as_str(),
        ) {
            denied.push(DescribedGroup {
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
        groups.push(describe_one(broker, gid, version).await);
    }

    // KIP-430: Kafka fills the bitfield for every coordinator row whose error
    // is NONE, a below-v6 `Dead` row included.
    if version >= AUTHORIZED_OPERATIONS_MIN_VERSION && req.include_authorized_operations {
        for row in &mut groups {
            if row.error_code == codes::NONE {
                row.authorized_operations = authorized_operations_bits(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx.principal,
                    ctx.peer,
                    ResourceType::Group,
                    row.group_id.as_str(),
                );
            }
        }
    }

    denied.extend(groups);
    Ok(DescribeGroupsResponse {
        groups: denied,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// Describes one allowed group the way `GroupMetadataManager.describeGroups`
/// does: a live classic group is projected, and anything else is a `Dead` row.
async fn describe_one(broker: &Broker, group_id: String, version: i16) -> DescribedGroup {
    let coordinator = &broker.group_coordinator;
    // A streams or share group may keep a drained classic actor as its offset
    // home; Kafka's `classicGroup` lookup still rejects it by type.
    if matches!(
        coordinator.group_type(&group_id),
        Some(GroupType::Streams | GroupType::Share)
    ) {
        let message = format!("Group {group_id} is not a classic group.");
        return dead_row(group_id, version, message);
    }
    let Some(handle) = coordinator.find(&group_id) else {
        let message = format!("Group {group_id} not found.");
        return dead_row(group_id, version, message);
    };
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ClassicInspect { reply: tx })
        .await
        .is_err()
    {
        let message = format!("Group {group_id} not found.");
        return dead_row(group_id, version, message);
    }
    // `ClassicInspect` replies only while the live group is classic; a
    // KIP-848 consumer group drops the sender.
    if let Ok(view) = rx.await {
        described_classic(view)
    } else {
        let message = format!("Group {group_id} is not a classic group.");
        dead_row(group_id, version, message)
    }
}

/// The row for a group Kafka's `classicGroup` lookup does not find: state
/// `Dead`, and from v6 `GROUP_ID_NOT_FOUND` with the lookup's message.
fn dead_row(group_id: String, version: i16, message: String) -> DescribedGroup {
    let not_found = version >= GROUP_ID_NOT_FOUND_MIN_VERSION;
    DescribedGroup {
        group_id,
        group_state: "Dead".into(),
        error_code: if not_found {
            codes::GROUP_ID_NOT_FOUND
        } else {
            codes::NONE
        },
        error_message: not_found.then_some(message),
        ..Default::default()
    }
}

/// Projects a live classic group. Only a `Stable` group reports its protocol
/// name and the members' metadata for it.
fn described_classic(view: ClassicView) -> DescribedGroup {
    let stable = view.state == GroupState::Stable;
    let members = view
        .members
        .into_iter()
        .map(|m| DescribedGroupMember {
            member_id: m.member_id,
            group_instance_id: m.group_instance_id,
            client_id: m.client_id,
            client_host: m.host,
            member_metadata: if stable {
                m.protocol_metadata
            } else {
                Bytes::new()
            },
            member_assignment: m.assignment.unwrap_or_default(),
            ..Default::default()
        })
        .collect();
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.state.as_str().into(),
        protocol_type: view.protocol_type.unwrap_or_default(),
        protocol_data: if stable {
            view.protocol_name.unwrap_or_default()
        } else {
            String::new()
        },
        error_code: codes::NONE,
        members,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::AclOperation;

    use super::*;
    use crate::{
        coordinator::unified::actor::ClassicMemberView,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = krabka_protocol::owned::describe_groups_response::MAX_VERSION;

    crate::test_support::context_helper!(client_id = "admin-client");

    /// Start a broker with `authorizer` and audit off, and wait until its
    /// group coordinator serves `__consumer_offsets`.
    async fn start_broker(
        authorizer: Arc<dyn crate::authorizer::Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (handle, dir) = crate::test_support::start_broker_with_authorizer_no_audit(
            crate::test_support::controller_peer_allowed(authorizer),
        )
        .await;
        handle.wait_until_group_coordinator_ready().await;
        (handle, dir)
    }

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

    async fn drive(
        broker: &Broker,
        version: i16,
        req: &DescribeGroupsRequest,
    ) -> DescribeGroupsResponse {
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);
        handle(broker, req.clone(), version, &ctx)
            .await
            .expect("handle")
    }

    /// A Deny on `Describe Group` answers the row, not the request: each named
    /// group gets its own `GROUP_AUTHORIZATION_FAILED` and the coordinator is
    /// never consulted.
    #[tokio::test]
    async fn a_denied_group_is_refused_per_row() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(&broker, VERSION, &request(&["group-a", "group-b"], false)).await;

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

    /// A `Dead` row as `GroupMetadataManager.describeGroups` answers a group
    /// its `classicGroup` lookup rejects.
    fn dead(group_id: &str, error_code: i16, message: Option<&str>, ops: i32) -> DescribedGroup {
        DescribedGroup {
            group_state: "Dead".into(),
            error_message: message.map(str::to_string),
            authorized_operations: ops,
            ..error_row(group_id, error_code)
        }
    }

    /// An unknown group and a KIP-848 consumer group are both `Dead`: from v6
    /// with `GROUP_ID_NOT_FOUND` and Kafka's message, below v6 with `NONE`.
    /// A below-v6 `NONE` row still gets the KIP-430 bitfield, because Kafka
    /// fills it for every coordinator row whose error is `NONE`.
    #[tokio::test]
    async fn a_group_that_is_not_classic_is_dead() {
        let all_bits = {
            let p = principal("admin");
            authorized_operations_bits(
                &crate::authorizer::AllowAllAuthorizer,
                &krabka_metadata::MetadataImage::new(uuid::Uuid::nil()),
                &p,
                &peer(),
                ResourceType::Group,
                "x",
            )
        };
        // (version, group, include flag, expected row)
        let rows = [
            (
                5,
                "never-seen",
                false,
                dead("never-seen", codes::NONE, None, i32::MIN),
            ),
            (
                5,
                "never-seen",
                true,
                dead("never-seen", codes::NONE, None, all_bits),
            ),
            (
                6,
                "never-seen",
                true,
                dead(
                    "never-seen",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group never-seen not found."),
                    i32::MIN,
                ),
            ),
            (
                5,
                "next-gen",
                false,
                dead("next-gen", codes::NONE, None, i32::MIN),
            ),
            (
                6,
                "next-gen",
                false,
                dead(
                    "next-gen",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group next-gen is not a classic group."),
                    i32::MIN,
                ),
            ),
        ];
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker.group_coordinator.get_or_create_consumer("next-gen");

        for (version, group, include_ops, expected) in rows {
            let resp = drive(&broker, version, &request(&[group], include_ops)).await;

            assert!(
                resp == DescribeGroupsResponse {
                    throttle_time_ms: 0,
                    groups: vec![expected],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
                "v{version} {group}"
            );
        }
        broker_handle.shutdown().await;
    }

    fn member_view(assignment: Option<&'static [u8]>) -> ClassicMemberView {
        ClassicMemberView {
            member_id: "m-1".into(),
            client_id: "client-1".into(),
            host: "/10.0.0.1".into(),
            group_instance_id: Some("instance-1".into()),
            protocol_metadata: Bytes::from_static(b"range-metadata"),
            assignment: assignment.map(Bytes::from_static),
        }
    }

    fn classic_view(state: GroupState, assignment: Option<&'static [u8]>) -> ClassicView {
        ClassicView {
            group_id: "classic".into(),
            state,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            generation_id: 3,
            members: vec![member_view(assignment)],
        }
    }

    /// Only a `Stable` group reports its protocol name and member metadata;
    /// every state keeps the member's instance id and assignment, as
    /// `ClassicGroupMember.describeNoMetadata` does.
    #[test]
    fn only_a_stable_group_carries_protocol_metadata() {
        let member = |metadata: &'static [u8], assignment: &'static [u8]| DescribedGroupMember {
            member_id: "m-1".into(),
            group_instance_id: Some("instance-1".into()),
            client_id: "client-1".into(),
            client_host: "/10.0.0.1".into(),
            member_metadata: Bytes::from_static(metadata),
            member_assignment: Bytes::from_static(assignment),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        let group = |state: &str, protocol_data: &str, m: DescribedGroupMember| DescribedGroup {
            group_state: state.into(),
            protocol_type: "consumer".into(),
            protocol_data: protocol_data.into(),
            members: vec![m],
            ..error_row("classic", codes::NONE)
        };
        // (view, expected row)
        let rows = [
            (
                classic_view(GroupState::Stable, Some(b"assigned")),
                group("Stable", "range", member(b"range-metadata", b"assigned")),
            ),
            (
                classic_view(GroupState::CompletingRebalance, None),
                group("CompletingRebalance", "", member(b"", b"")),
            ),
            (
                classic_view(GroupState::PreparingRebalance, Some(b"assigned")),
                group("PreparingRebalance", "", member(b"", b"assigned")),
            ),
        ];
        for (view, expected) in rows {
            let state = view.state;
            assert!(described_classic(view) == expected, "{state:?}");
        }
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

        let resp = drive(&broker, VERSION, &request(&["classic-a"], false)).await;

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

        let resp = drive(&broker, VERSION, &request(&["classic-a"], true)).await;

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

    fn allow(group: &str, operation: AclOperation) -> krabka_metadata::MetadataRecord {
        krabka_metadata::MetadataRecord::V1AccessControlEntry(crate::test_support::allow_acl(
            ResourceType::Group,
            group,
            "User:admin",
            operation,
        ))
    }

    fn bit(op: AclOperation) -> i32 {
        1_i32 << crate::handlers::acl_wire::operation_to_wire(op)
    }

    /// `Empty` classic `allowed`, as `DescribeGroups` answers it with
    /// `authorized_operations` bits.
    fn empty_row(group_id: &str, authorized_operations: i32) -> DescribedGroup {
        DescribedGroup {
            group_state: "Empty".into(),
            authorized_operations,
            ..error_row(group_id, codes::NONE)
        }
    }

    /// Kafka's `handleDescribeGroupsRequest` answers the denied groups first
    /// and then the coordinator results, and fills KIP-430 bits from the
    /// group's whole supported set (`Read`, `Describe`, `Delete`,
    /// `DescribeConfigs`, `AlterConfigs`).
    #[tokio::test]
    async fn denied_rows_come_first_and_the_bits_cover_every_group_operation() {
        // (ACLs on `allowed`, include flag, expected response rows)
        let rows = [
            (
                vec![AclOperation::Describe],
                false,
                vec![
                    error_row("denied", codes::GROUP_AUTHORIZATION_FAILED),
                    empty_row("allowed", i32::MIN),
                ],
            ),
            (
                vec![AclOperation::Describe, AclOperation::AlterConfigs],
                true,
                vec![
                    error_row("denied", codes::GROUP_AUTHORIZATION_FAILED),
                    empty_row(
                        "allowed",
                        bit(AclOperation::Describe)
                            | bit(AclOperation::DescribeConfigs)
                            | bit(AclOperation::AlterConfigs),
                    ),
                ],
            ),
            (
                vec![AclOperation::All],
                true,
                vec![
                    error_row("denied", codes::GROUP_AUTHORIZATION_FAILED),
                    empty_row(
                        "allowed",
                        bit(AclOperation::Read)
                            | bit(AclOperation::Describe)
                            | bit(AclOperation::Delete)
                            | bit(AclOperation::DescribeConfigs)
                            | bit(AclOperation::AlterConfigs),
                    ),
                ],
            ),
        ];
        for (acls, include_ops, expected) in rows {
            let authorizer =
                crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
            let (broker_handle, _dir) = start_broker(Arc::new(authorizer)).await;
            let broker = broker_handle.broker_arc_for_test();
            broker
                .controller
                .submit_change(acls.iter().map(|op| allow("allowed", *op)).collect())
                .await
                .expect("grant ACLs");
            let _ = broker.group_coordinator.get_or_create_classic("allowed");

            let resp = drive(
                &broker,
                VERSION,
                &request(&["allowed", "denied"], include_ops),
            )
            .await;

            assert!(
                resp == DescribeGroupsResponse {
                    throttle_time_ms: 0,
                    groups: expected,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
                "{acls:?}"
            );
            broker_handle.shutdown().await;
        }
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
            .map(GroupState::as_str)
                == [
                    "Empty",
                    "PreparingRebalance",
                    "CompletingRebalance",
                    "Stable"
                ]
        );
    }
}
