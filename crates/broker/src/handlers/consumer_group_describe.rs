//! `ConsumerGroupDescribe` (`api_key` 69).
//!
//! This handler returns one `DescribedGroup` per requested `group_id`. It
//! renders each group from the actor's `Describe` view.
//!
//! Ordering and gating follow Kafka's `KafkaApis.handleConsumerGroupDescribe`:
//!
//! - The `group.version` protocol gate is checked once, before any
//!   authorization. When the next-gen consumer-group RPCs are not finalized,
//!   every requested group gets `UNSUPPORTED_VERSION` and no ACL is
//!   consulted.
//! - Past the gate, a group `Describe` denial is a per-row
//!   `GROUP_AUTHORIZATION_FAILED`, and those denied rows are placed first in
//!   the response, ahead of the coordinator results (which keep request
//!   order among themselves).
//! - KIP-430: when the request sets `include_authorized_operations`, every
//!   row with `error_code == NONE` carries the bitfield of group operations
//!   the principal holds. Every other row keeps the wire-default `i32::MIN`
//!   "not present" sentinel.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_describe_response::{ConsumerGroupDescribeResponse, DescribedGroup},
    },
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::actor::GroupActorMessage, error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

/// KIP-848/KIP-584: minimum finalized `group.version` feature level that
/// enables the next-gen consumer-group RPCs.
const NEXT_GEN_MIN_GROUP_VERSION: i16 = 1;

/// Wire `group_state` reported for a next-gen group with no members.
const GROUP_STATE_EMPTY: &str = "EMPTY";
/// Wire `group_state` reported for a next-gen group with at least one member.
const GROUP_STATE_STABLE: &str = "STABLE";

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coordinator = broker.group_coordinator.clone();
    let image = broker.controller.current_image();
    let mut cur: &[u8] = req_bytes;
    let req = ConsumerGroupDescribeRequest::decode(&mut cur, version)?;

    // KIP-848 / KIP-584 protocol gate, checked before any authorization —
    // Kafka's handleConsumerGroupDescribe answers UNSUPPORTED_VERSION for
    // every requested group up front and never touches ACLs when the
    // next-gen consumer-group RPCs are not finalized (group.version >= 1;
    // below that, including UNFINALIZED which means disabled, is rejected,
    // consistent with the heartbeat fallback).
    if group_version_disabled(&image) {
        let described = req
            .group_ids
            .iter()
            .map(|group_id| {
                let mut row = ok_row(group_id);
                row.error_code = codes::UNSUPPORTED_VERSION;
                row
            })
            .collect();
        let resp = response(described);
        return crate::handlers::encode_response(&resp, version);
    }

    let next_gen_enabled = coordinator.config.next_gen_enabled();
    // Kafka places every GROUP_AUTHORIZATION_FAILED row first, ahead of the
    // coordinator results, which keep request order among themselves.
    let mut denied: Vec<DescribedGroup> = Vec::new();
    let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for group_id in &req.group_ids {
        let mut row = ok_row(group_id);
        if crate::handlers::acl_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            krabka_metadata::ResourceType::Group,
            group_id,
            krabka_metadata::AclOperation::Describe,
        ) {
            row.error_code = codes::GROUP_AUTHORIZATION_FAILED;
            denied.push(row);
            continue;
        }
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, group_id) {
            row.error_code = error_code;
            described.push(row);
            continue;
        }
        if next_gen_config_disabled(next_gen_enabled) {
            row.error_code = codes::GROUP_ID_NOT_FOUND;
            described.push(row);
            continue;
        }
        // Only next-gen (consumer) groups are described here; a classic
        // group (or an unknown id) is GROUP_ID_NOT_FOUND. The `Describe` arm
        // dispatches on the actor's LIVE `group.kind`: it replies ONLY for a
        // consumer-kind group and drops the sender otherwise, so an UPGRADED
        // group (spawned classic, now consumer in place via KIP-848) is
        // reachable while a classic group's no-reply maps to
        // GROUP_ID_NOT_FOUND — without consulting the stale spawn-time
        // `h.kind`.
        let Some(handle) = coordinator.find(group_id) else {
            row.error_code = codes::GROUP_ID_NOT_FOUND;
            described.push(row);
            continue;
        };
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            row.error_code = codes::COORDINATOR_LOAD_IN_PROGRESS;
            described.push(row);
            continue;
        }
        if let Ok(view) = rx.await {
            row.group_state = group_state_for_member_count(view.members.len());
            described.push(row);
        } else {
            // No reply means the live group is classic (not describable via
            // api 69), which surfaces as GROUP_ID_NOT_FOUND — matching the
            // pre-refactor behavior for a classic group.
            row.error_code = codes::GROUP_ID_NOT_FOUND;
            described.push(row);
        }
    }

    // KIP-430: bitfield of group operations the principal is authorized for,
    // filled only on opt-in and only for rows that came back clean. Denied
    // and errored rows keep the wire-default `i32::MIN` sentinel.
    if req.include_authorized_operations {
        for row in &mut described {
            if row.error_code == codes::NONE {
                row.authorized_operations = authorized_operations_bits(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx.principal,
                    ctx.peer,
                    krabka_metadata::ResourceType::Group,
                    row.group_id.as_str(),
                );
            }
        }
    }

    denied.extend(described);
    let resp = response(denied);
    crate::handlers::encode_response(&resp, version)
}

fn ok_row(group_id: &str) -> DescribedGroup {
    DescribedGroup {
        group_id: group_id.into(),
        ..Default::default()
    }
}

fn group_version_disabled(image: &krabka_metadata::MetadataImage) -> bool {
    !crate::features::feature_enabled(
        image,
        krabka_metadata::group_version::GROUP_VERSION_FEATURE,
        NEXT_GEN_MIN_GROUP_VERSION,
    )
}

fn next_gen_config_disabled(next_gen_enabled: bool) -> bool {
    !next_gen_enabled
}

fn group_state_for_member_count(members: usize) -> String {
    match members {
        0 => GROUP_STATE_EMPTY.into(),
        _ => GROUP_STATE_STABLE.into(),
    }
}

fn response(groups: Vec<DescribedGroup>) -> ConsumerGroupDescribeResponse {
    ConsumerGroupDescribeResponse {
        groups,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
    use krabka_protocol::Encode;

    use super::*;

    const VERSION: i16 = krabka_protocol::owned::consumer_group_describe_request::MAX_VERSION;

    fn request(group_ids: Vec<&str>) -> Bytes {
        let req = ConsumerGroupDescribeRequest {
            group_ids: group_ids.into_iter().map(Into::into).collect(),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION)
            .expect("encode ConsumerGroupDescribeRequest");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> ConsumerGroupDescribeResponse {
        crate::test_support::decode_response(bytes, VERSION)
    }

    async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|_cfg| {}).await
    }

    fn image_with_group_version(level: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
            level,
        }));
        image
    }

    #[test]
    fn ok_row_preserves_requested_group_id() {
        let row = ok_row("orders");
        assert!(row.group_id == "orders");
        assert!(row.error_code == codes::NONE);
    }

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        // (finalized group.version level; None = fresh image) → disabled?
        let cases = [(None, true), (Some(1), false), (Some(0), true)];
        for (level, want_disabled) in cases {
            let image = match level {
                None => MetadataImage::new(uuid::Uuid::nil()),
                Some(level) => image_with_group_version(level),
            };
            assert!(
                group_version_disabled(&image) == want_disabled,
                "level {level:?}"
            );
        }
    }

    #[test]
    fn next_gen_config_gate_inverts_enabled_flag() {
        assert!(!next_gen_config_disabled(true));
        assert!(next_gen_config_disabled(false));
    }

    #[test]
    fn group_state_reflects_member_count() {
        let cases = [(0, "EMPTY"), (1, "STABLE"), (3, "STABLE")];
        for (members, want) in cases {
            assert!(
                group_state_for_member_count(members) == want,
                "members {members}"
            );
        }
    }

    #[test]
    fn response_preserves_group_rows() {
        let mut first = ok_row("a");
        first.error_code = codes::GROUP_ID_NOT_FOUND;
        let mut second = ok_row("b");
        second.error_code = codes::UNSUPPORTED_VERSION;

        let resp = response(vec![first, second]);

        let expected = ConsumerGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![
                DescribedGroup {
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    error_message: None,
                    group_id: "a".to_string(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: vec![],
                    authorized_operations: -2_147_483_648,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
                DescribedGroup {
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    group_id: "b".to_string(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: vec![],
                    authorized_operations: -2_147_483_648,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
    }

    #[tokio::test]
    async fn handle_unknown_group_preserves_requested_group_id() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let req = request(vec!["missing-group"]);
        let principal = crate::test_support::principal("admin");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

        let bytes = handle(&broker, VERSION, 3, &req, &ctx)
            .await
            .expect("ConsumerGroupDescribe handler");
        let resp = decode_response(&bytes);

        let expected = ConsumerGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: None,
                group_id: "missing-group".to_string(),
                group_state: String::new(),
                group_epoch: 0,
                assignment_epoch: 0,
                assignor_name: String::new(),
                members: vec![],
                authorized_operations: -2_147_483_648,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");

        broker_handle.shutdown().await;
    }

    /// Request `include_authorized_operations` on a request built with
    /// [`request`], which does not set it — [`request_with_ops`] below sets
    /// it explicitly.
    fn request_with_ops(group_ids: Vec<&str>, include_authorized_operations: bool) -> Bytes {
        let req = ConsumerGroupDescribeRequest {
            group_ids: group_ids.into_iter().map(Into::into).collect(),
            include_authorized_operations,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION)
            .expect("encode ConsumerGroupDescribeRequest");
        buf.freeze()
    }

    /// Lowers `group.version` back to 0 (unfinalized/disabled) on an
    /// already-started test broker, whose bootstrap otherwise finalizes it
    /// at the modern release default.
    async fn disable_group_version(broker: &crate::broker::Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
                level: 0,
            })])
            .await
            .expect("disable group.version");
    }

    /// The `group.version` protocol gate — `UNSUPPORTED_VERSION` when the
    /// next-gen consumer-group RPCs are not finalized — runs BEFORE the
    /// group ACL check, matching Kafka's `handleConsumerGroupDescribe`. A
    /// principal denied `Describe` on the group still gets
    /// `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`, when the
    /// protocol itself is unavailable, and every requested group gets it —
    /// none are individually authorization-checked.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl_for_every_row() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let (broker_handle, _dir) = crate::test_support::start_broker_with_authorizer_no_audit(
            std::sync::Arc::new(authorizer),
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        disable_group_version(&broker).await;
        let principal = crate::test_support::principal("nobody");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "consumer-client");
        let req = request(vec!["denied-group", "also-denied"]);

        let bytes = handle(&broker, VERSION, 5, &req, &ctx)
            .await
            .expect("ConsumerGroupDescribe handler");
        let resp = decode_response(&bytes);

        assert!(
            resp.groups
                .iter()
                .map(|g| (g.group_id.as_str(), g.error_code))
                .collect::<Vec<_>>()
                == vec![
                    ("denied-group", codes::UNSUPPORTED_VERSION),
                    ("also-denied", codes::UNSUPPORTED_VERSION),
                ],
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// Kafka puts every `GROUP_AUTHORIZATION_FAILED` row first, ahead of the
    /// coordinator results, regardless of the order the client requested the
    /// groups in.
    #[tokio::test]
    async fn handle_orders_denied_rows_before_allowed_rows() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let (broker_handle, _dir) = crate::test_support::start_broker_with_authorizer_no_audit(
            std::sync::Arc::new(authorizer),
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        // Grant "alice" Describe on "allowed" only; "denied" has no matching
        // ACL and stays denied under SimpleAclAuthorizer's default-deny.
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(
                krabka_metadata::AclEntry {
                    resource_type: krabka_metadata::ResourceType::Group,
                    resource_name: "allowed".into(),
                    pattern_type: krabka_metadata::PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: krabka_metadata::AclOperation::Describe,
                    permission_type: krabka_metadata::PermissionType::Allow,
                },
            )])
            .await
            .expect("grant alice Describe on allowed");
        let principal = crate::test_support::principal("alice");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "alice-client");
        let req = request(vec!["allowed", "denied"]);

        let bytes = handle(&broker, VERSION, 7, &req, &ctx)
            .await
            .expect("ConsumerGroupDescribe handler");
        let resp = decode_response(&bytes);

        // Requested in order [allowed, denied]; the denied row comes first
        // in the response, ahead of the (unknown, hence GROUP_ID_NOT_FOUND)
        // allowed row.
        assert!(
            resp.groups
                .iter()
                .map(|g| (g.group_id.as_str(), g.error_code))
                .collect::<Vec<_>>()
                == vec![
                    ("denied", codes::GROUP_AUTHORIZATION_FAILED),
                    ("allowed", codes::GROUP_ID_NOT_FOUND),
                ],
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// KIP-430: with the flag set, a row that comes back clean carries the
    /// bitfield of group operations the principal holds; the flag unset (or
    /// an errored row) keeps the wire-default `i32::MIN` sentinel.
    #[tokio::test]
    async fn handle_fills_authorized_operations_only_on_opt_in_for_clean_rows() {
        let authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
        let (broker_handle, _dir) = crate::test_support::start_broker_with_authorizer_no_audit(
            std::sync::Arc::clone(&authorizer) as _,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker
            .group_coordinator
            .get_or_create_consumer("live-group");
        let principal = crate::test_support::principal("admin");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");

        // Flag unset: sentinel preserved even for a clean row.
        let req_off = request_with_ops(vec!["live-group"], false);
        let resp_off = decode_response(
            &handle(&broker, VERSION, 9, &req_off, &ctx)
                .await
                .expect("ConsumerGroupDescribe handler"),
        );
        assert!(
            resp_off.groups
                == vec![DescribedGroup {
                    group_id: "live-group".into(),
                    group_state: GROUP_STATE_EMPTY.into(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    ..Default::default()
                }],
            "{resp_off:?}"
        );

        // Flag set: bitfield filled from the group's supported operations
        // (Read, Describe, Delete) under AllowAll.
        let req_on = request_with_ops(vec!["live-group"], true);
        let resp_on = decode_response(
            &handle(&broker, VERSION, 11, &req_on, &ctx)
                .await
                .expect("ConsumerGroupDescribe handler"),
        );
        let expected_bits = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &principal,
            &peer,
            krabka_metadata::ResourceType::Group,
            "live-group",
        );
        assert!(expected_bits != i32::MIN);
        assert!(
            resp_on.groups
                == vec![DescribedGroup {
                    group_id: "live-group".into(),
                    group_state: GROUP_STATE_EMPTY.into(),
                    authorized_operations: expected_bits,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    ..Default::default()
                }],
            "{resp_on:?}"
        );

        broker_handle.shutdown().await;
    }
}
