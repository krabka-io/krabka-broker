//! `ConsumerGroupDescribe` (`api_key` 69).
//!
//! This handler returns one `DescribedGroup` per requested `group_id`. It
//! renders each KIP-848 consumer group from the actor's `Describe` view the
//! way Kafka's `ConsumerGroup.asDescribedGroup` does: the group state, the
//! group and assignment epochs, the preferred server assignor (or the default
//! one), and every member with its current and target assignment.
//!
//! Ordering and gating follow Kafka's `KafkaApis.handleConsumerGroupDescribe`:
//!
//! - The consumer-protocol gate is checked once, before any authorization.
//!   When the `consumer` rebalance protocol is not enabled, or `group.version`
//!   does not finalize the next-gen consumer-group RPCs, every requested group
//!   gets `UNSUPPORTED_VERSION` and no ACL is consulted.
//! - Past the gate, a group `Describe` denial is a per-row
//!   `GROUP_AUTHORIZATION_FAILED`, and those denied rows are placed first in
//!   the response, ahead of the coordinator results (which keep request
//!   order among themselves).
//! - KIP-430: when the request sets `include_authorized_operations`, every
//!   row with `error_code == NONE` carries the bitfield of group operations
//!   the principal holds. Every other row keeps the wire-default `i32::MIN`
//!   "not present" sentinel.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Decode,
    owned::{
        common::consumer_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        },
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_describe_response::{ConsumerGroupDescribeResponse, DescribedGroup, Member},
    },
    primitives::uuid::Uuid,
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::actor::{DescribeMember, DescribeView, GroupActorMessage},
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

/// KIP-848/KIP-584: minimum finalized `group.version` feature level that
/// enables the next-gen consumer-group RPCs.
const NEXT_GEN_MIN_GROUP_VERSION: i16 = 1;

/// `member_type` of a member that speaks the classic protocol inside a
/// consumer group.
const MEMBER_TYPE_CLASSIC: i8 = 0;
/// `member_type` of a member that speaks the consumer protocol.
const MEMBER_TYPE_CONSUMER: i8 = 1;

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

    // Kafka's `isConsumerGroupProtocolEnabled` gate, checked before any
    // authorization: the `consumer` rebalance protocol must be enabled and
    // `group.version` must be finalized at 1 or above.
    if !coordinator.config.next_gen_enabled() || group_version_disabled(&image) {
        let described = req
            .group_ids
            .iter()
            .map(|group_id| error_row(group_id, codes::UNSUPPORTED_VERSION, None))
            .collect();
        let resp = response(described);
        return crate::handlers::encode_response(&resp, version);
    }

    let default_assignor = coordinator
        .config
        .assignors
        .first()
        .map(|a| a.name())
        .unwrap_or_default();
    // Kafka places every GROUP_AUTHORIZATION_FAILED row first, ahead of the
    // coordinator results, which keep request order among themselves.
    let mut denied: Vec<DescribedGroup> = Vec::new();
    let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for group_id in &req.group_ids {
        if crate::handlers::acl_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            krabka_metadata::ResourceType::Group,
            group_id,
            krabka_metadata::AclOperation::Describe,
        ) {
            denied.push(error_row(group_id, codes::GROUP_AUTHORIZATION_FAILED, None));
            continue;
        }
        // GroupCoordinatorService.consumerGroupDescribe rejects an empty id
        // before it routes the group to a shard.
        if group_id.is_empty() {
            described.push(error_row("", codes::INVALID_GROUP_ID, None));
            continue;
        }
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, group_id) {
            described.push(error_row(group_id, error_code, None));
            continue;
        }
        // The `Describe` arm dispatches on the actor's LIVE `group.kind`: it
        // replies ONLY for a consumer-kind group and drops the sender
        // otherwise, so an upgraded group is reachable and a classic group
        // is not.
        let Some(handle) = coordinator.find(group_id) else {
            described.push(not_found_row(group_id, "not found"));
            continue;
        };
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            described.push(error_row(
                group_id,
                codes::COORDINATOR_LOAD_IN_PROGRESS,
                None,
            ));
            continue;
        }
        match rx.await {
            Ok(view) => described.push(described_group(view, default_assignor, &image)),
            Err(_) => described.push(not_found_row(group_id, "is not a consumer group")),
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

fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> DescribedGroup {
    DescribedGroup {
        group_id: group_id.into(),
        error_code,
        error_message,
        ..Default::default()
    }
}

/// `GROUP_ID_NOT_FOUND` with the message of Kafka's `consumerGroup` lookup,
/// `Group <id> not found.` or `Group <id> is not a consumer group.`.
fn not_found_row(group_id: &str, reason: &str) -> DescribedGroup {
    error_row(
        group_id,
        codes::GROUP_ID_NOT_FOUND,
        Some(format!("Group {group_id} {reason}.")),
    )
}

/// Renders a consumer group as `ConsumerGroup.asDescribedGroup` does. Members
/// are sorted by id, topics by name and partitions ascending, so the answer
/// does not depend on map order.
fn described_group(
    view: DescribeView,
    default_assignor: &str,
    image: &MetadataImage,
) -> DescribedGroup {
    let mut members: Vec<Member> = view
        .members
        .into_iter()
        .map(|m| described_member(m, image))
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.group_state.into(),
        group_epoch: view.group_epoch,
        assignment_epoch: view.assignment_epoch,
        assignor_name: view
            .preferred_server_assignor
            .unwrap_or_else(|| default_assignor.to_string()),
        members,
        ..Default::default()
    }
}

/// `ConsumerGroupMember.asConsumerGroupDescribeMember`: the current assignment
/// is the assigned partitions plus those pending revocation, which the member
/// still owns until it confirms the revocation.
fn described_member(m: DescribeMember, image: &MetadataImage) -> Member {
    let mut owned = m.assigned_partitions;
    for (topic_id, partitions) in m.partitions_pending_revocation {
        owned.entry(topic_id).or_default().extend(partitions);
    }
    Member {
        member_id: m.member_id,
        instance_id: m.instance_id,
        rack_id: m.rack_id,
        member_epoch: m.member_epoch,
        client_id: m.client_id,
        client_host: m.client_host,
        subscribed_topic_names: m.subscribed_topic_names,
        subscribed_topic_regex: m.subscribed_topic_regex,
        assignment: assignment(owned, image),
        target_assignment: assignment(m.target_partitions, image),
        member_type: if m.is_classic {
            MEMBER_TYPE_CLASSIC
        } else {
            MEMBER_TYPE_CONSUMER
        },
        ..Default::default()
    }
}

/// Names each topic from the metadata image. Kafka drops a topic the image no
/// longer has.
fn assignment(partitions: HashMap<Uuid, Vec<i32>>, image: &MetadataImage) -> Assignment {
    let mut topic_partitions: Vec<TopicPartitions> = partitions
        .into_iter()
        .filter_map(|(topic_id, mut partitions)| {
            let topic_name = image.topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))?;
            partitions.sort_unstable();
            Some(TopicPartitions {
                topic_id,
                topic_name: topic_name.to_string(),
                partitions,
                ..Default::default()
            })
        })
        .collect();
    topic_partitions.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
    Assignment {
        topic_partitions,
        ..Default::default()
    }
}

fn group_version_disabled(image: &MetadataImage) -> bool {
    !crate::features::feature_enabled(
        image,
        krabka_metadata::group_version::GROUP_VERSION_FEATURE,
        NEXT_GEN_MIN_GROUP_VERSION,
    )
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
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
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

    const ORDERS: Uuid = Uuid([1; 16]);
    const PAYMENTS: Uuid = Uuid([2; 16]);
    /// A topic id the image does not hold, as after a topic deletion.
    const DELETED: Uuid = Uuid([3; 16]);

    fn image_with_topics() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (name, id) in [("orders", ORDERS), ("payments", PAYMENTS)] {
            image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::from_bytes(id.0),
                partitions: 4,
                replication_factor: 1,
            }));
        }
        image
    }

    fn partitions(entries: &[(Uuid, &[i32])]) -> HashMap<Uuid, Vec<i32>> {
        entries.iter().map(|(id, p)| (*id, p.to_vec())).collect()
    }

    fn view_member(member_id: &str) -> DescribeMember {
        DescribeMember {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            member_epoch: 5,
            client_id: "client".into(),
            client_host: "/10.0.0.1".into(),
            subscribed_topic_names: vec!["orders".into(), "payments".into()],
            subscribed_topic_regex: None,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            target_partitions: HashMap::new(),
            is_classic: false,
        }
    }

    fn view(group_state: &'static str, members: Vec<DescribeMember>) -> DescribeView {
        DescribeView {
            group_id: "cg".into(),
            group_epoch: 5,
            assignment_epoch: 5,
            group_state,
            preferred_server_assignor: None,
            members,
        }
    }

    fn topic(topic_id: Uuid, topic_name: &str, partitions: &[i32]) -> TopicPartitions {
        TopicPartitions {
            topic_id,
            topic_name: topic_name.into(),
            partitions: partitions.to_vec(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    fn assigned(topics: Vec<TopicPartitions>) -> Assignment {
        Assignment {
            topic_partitions: topics,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    fn wire_member(member_id: &str) -> Member {
        Member {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            member_epoch: 5,
            client_id: "client".into(),
            client_host: "/10.0.0.1".into(),
            subscribed_topic_names: vec!["orders".into(), "payments".into()],
            subscribed_topic_regex: None,
            assignment: assigned(vec![]),
            target_assignment: assigned(vec![]),
            member_type: MEMBER_TYPE_CONSUMER,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    fn wire_group(group_state: &str, assignor_name: &str, members: Vec<Member>) -> DescribedGroup {
        DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: "cg".into(),
            group_state: group_state.into(),
            group_epoch: 5,
            assignment_epoch: 5,
            assignor_name: assignor_name.into(),
            members,
            authorized_operations: i32::MIN,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    /// `ConsumerGroup.asDescribedGroup`: the view's state and epochs, the
    /// preferred assignor or the default, and each member with its owned
    /// partitions (assigned plus pending revocation) and its target, named
    /// from the image. A topic the image lacks is dropped.
    #[test]
    fn a_consumer_group_is_described_whole() {
        let stable = {
            let mut native = view_member("m-a");
            native.instance_id = Some("instance-a".into());
            native.rack_id = Some("rack-1".into());
            native.assigned_partitions = partitions(&[(PAYMENTS, &[1, 0])]);
            native.target_partitions = partitions(&[(PAYMENTS, &[0, 1])]);
            let mut classic = view_member("m-b");
            classic.is_classic = true;
            classic.subscribed_topic_regex = Some("ord.*".into());
            classic.assigned_partitions = partitions(&[(ORDERS, &[0]), (DELETED, &[0])]);
            classic.target_partitions = partitions(&[(ORDERS, &[0])]);
            let mut v = view("Stable", vec![classic, native]);
            v.preferred_server_assignor = Some("range".into());
            v
        };
        let stable_expected = {
            let mut native = wire_member("m-a");
            native.instance_id = Some("instance-a".into());
            native.rack_id = Some("rack-1".into());
            native.assignment = assigned(vec![topic(PAYMENTS, "payments", &[0, 1])]);
            native.target_assignment = assigned(vec![topic(PAYMENTS, "payments", &[0, 1])]);
            let mut classic = wire_member("m-b");
            classic.member_type = MEMBER_TYPE_CLASSIC;
            classic.subscribed_topic_regex = Some("ord.*".into());
            classic.assignment = assigned(vec![topic(ORDERS, "orders", &[0])]);
            classic.target_assignment = assigned(vec![topic(ORDERS, "orders", &[0])]);
            wire_group("Stable", "range", vec![native, classic])
        };
        let reconciling = {
            let mut m = view_member("m-a");
            m.member_epoch = 4;
            m.assigned_partitions = partitions(&[(ORDERS, &[2])]);
            m.partitions_pending_revocation = partitions(&[(ORDERS, &[1]), (PAYMENTS, &[3])]);
            m.target_partitions = partitions(&[(ORDERS, &[2])]);
            view("Reconciling", vec![m])
        };
        let reconciling_expected = {
            let mut m = wire_member("m-a");
            m.member_epoch = 4;
            m.assignment = assigned(vec![
                topic(ORDERS, "orders", &[1, 2]),
                topic(PAYMENTS, "payments", &[3]),
            ]);
            m.target_assignment = assigned(vec![topic(ORDERS, "orders", &[2])]);
            wire_group("Reconciling", "uniform", vec![m])
        };
        // (view, expected row)
        let rows = [
            (
                view("Empty", vec![]),
                wire_group("Empty", "uniform", vec![]),
            ),
            (stable, stable_expected),
            (reconciling, reconciling_expected),
        ];
        let image = image_with_topics();
        for (v, expected) in rows {
            let state = v.group_state;
            assert!(described_group(v, "uniform", &image) == expected, "{state}");
        }
    }

    #[test]
    fn response_preserves_group_rows() {
        let first = error_row("a", codes::GROUP_ID_NOT_FOUND, None);
        let second = error_row("b", codes::UNSUPPORTED_VERSION, None);

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

    /// A group Kafka's `consumerGroup` lookup rejects is `GROUP_ID_NOT_FOUND`
    /// with the lookup's message, and an empty id is `INVALID_GROUP_ID`.
    #[tokio::test]
    async fn handle_answers_a_group_that_is_not_a_consumer_group() {
        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker
            .group_coordinator
            .get_or_create_classic("classic-group");
        let principal = crate::test_support::principal("admin");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");
        let row = |group_id: &str, error_code, message: Option<&str>| DescribedGroup {
            error_code,
            error_message: message.map(str::to_string),
            group_id: group_id.to_string(),
            group_state: String::new(),
            group_epoch: 0,
            assignment_epoch: 0,
            assignor_name: String::new(),
            members: vec![],
            authorized_operations: i32::MIN,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        // (requested id, expected row)
        let rows = [
            (
                "missing-group",
                row(
                    "missing-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group missing-group not found."),
                ),
            ),
            (
                "classic-group",
                row(
                    "classic-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group classic-group is not a consumer group."),
                ),
            ),
            ("", row("", codes::INVALID_GROUP_ID, None)),
        ];
        for (group_id, expected) in rows {
            let bytes = handle(&broker, VERSION, 3, &request(vec![group_id]), &ctx)
                .await
                .expect("ConsumerGroupDescribe handler");
            let resp = decode_response(&bytes);

            assert!(
                resp == ConsumerGroupDescribeResponse {
                    throttle_time_ms: 0,
                    groups: vec![expected],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
                "{group_id:?}"
            );
        }

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
                    group_state: "Empty".into(),
                    assignor_name: "uniform".into(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    ..Default::default()
                }],
            "{resp_off:?}"
        );

        // Flag set: bitfield filled from the group's supported operations
        // (Read, Describe, Delete, DescribeConfigs, AlterConfigs) under
        // AllowAll.
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
                    group_state: "Empty".into(),
                    assignor_name: "uniform".into(),
                    authorized_operations: expected_bits,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    ..Default::default()
                }],
            "{resp_on:?}"
        );

        broker_handle.shutdown().await;
    }
}
