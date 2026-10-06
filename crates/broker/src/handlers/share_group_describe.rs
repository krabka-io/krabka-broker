//! `ShareGroupDescribe` (`api_key` 77), from KIP-932. It returns one
//! `DescribedGroup` per requested `group_id`, built from the share actor's
//! `Describe` view, as Kafka's `KafkaApis.handleShareGroupDescribe` does.
//!
//! `network::dispatch` intercepts this request inline, not through
//! `build_table`, so the handler receives the per-connection principal and the
//! peer `SocketAddr` for the per-group `Describe` ACL gate.

use std::collections::HashSet;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    share_group_describe_request::ShareGroupDescribeRequest,
    share_group_describe_response::{DescribedGroup, ShareGroupDescribeResponse},
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::{GroupType, share::actor::ShareGroupActorMessage},
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

/// The message of the row Kafka substitutes for a group whose assignment
/// names a topic the caller cannot `Describe`.
const UNAUTHORIZED_TOPICS_MESSAGE: &str =
    "The group has described topic(s) that the client is not authorized to describe.";

pub(crate) async fn handle(
    broker: &Broker,
    req: ShareGroupDescribeRequest,
    _version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<ShareGroupDescribeResponse, BrokerError> {
    // Kafka's `isShareGroupProtocolEnabled` gate comes before any ACL check,
    // and `getErrorResponse` answers every requested group. Share groups are
    // on from a finalized `share.version` of 1.
    let image = broker.controller.current_image();
    if !crate::features::share_groups_enabled(&image) {
        let groups = req
            .group_ids
            .iter()
            .map(|gid| error_row(gid, codes::UNSUPPORTED_VERSION, None))
            .collect();
        return Ok(response(groups));
    }

    let authorizer = broker.config.authorizer.as_ref();
    let coordinator = &broker.group_coordinator;
    // Kafka adds the GROUP_AUTHORIZATION_FAILED rows first, then the
    // coordinator results.
    let mut groups: Vec<DescribedGroup> = Vec::new();
    let mut invalid: Vec<DescribedGroup> = Vec::new();
    let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        if crate::handlers::group_describe_denied(authorizer, &image, ctx, gid) {
            groups.push(error_row(gid, codes::GROUP_AUTHORIZATION_FAILED, None));
            continue;
        }
        // GroupCoordinatorService.shareGroupDescribe rejects an empty id
        // before it routes the group to a shard, and its row comes ahead of
        // the shard results.
        if gid.is_empty() {
            invalid.push(error_row("", codes::INVALID_GROUP_ID, None));
            continue;
        }
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, gid) {
            described.push(error_row(gid, error_code, None));
            continue;
        }
        let handle = match coordinator.group_type(gid) {
            Some(GroupType::Share) | None => coordinator.find_share(gid),
            Some(_) => None,
        };
        let Some(handle) = handle else {
            described.push(error_row(
                gid,
                codes::GROUP_ID_NOT_FOUND,
                Some(crate::handlers::share_group_not_found_message(
                    coordinator,
                    gid,
                )),
            ));
            continue;
        };

        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            described.push(error_row(gid, codes::COORDINATOR_LOAD_IN_PROGRESS, None));
            continue;
        }
        match rx.await {
            Ok(view) => described.push(view.into_described_group(&image)),
            Err(_) => described.push(error_row(gid, codes::UNKNOWN_SERVER_ERROR, None)),
        }
    }

    // KIP-430: the group operations bitfield, only on opt-in and only for rows
    // that came back clean.
    if req.include_authorized_operations {
        for row in &mut described {
            if row.error_code == codes::NONE {
                row.authorized_operations = authorized_operations_bits(
                    authorizer,
                    &image,
                    ctx.principal,
                    ctx.peer,
                    ResourceType::Group,
                    row.group_id.as_str(),
                );
            }
        }
    }
    groups.extend(invalid);
    groups.extend(described);

    // Clients may not see topics they cannot `Describe`: a group whose
    // assignment names one is replaced by Kafka's TOPIC_AUTHORIZATION_FAILED
    // row with no members.
    let assigned: HashSet<&str> = groups
        .iter()
        .flat_map(|g| &g.members)
        .flat_map(|m| &m.assignment.topic_partitions)
        .map(|tp| tp.topic_name.as_str())
        .collect();
    let denied =
        crate::handlers::denied_topics(authorizer, &image, ctx, AclOperation::Describe, assigned);
    if !denied.is_empty() {
        for group in &mut groups {
            let hides_topic = group
                .members
                .iter()
                .flat_map(|m| &m.assignment.topic_partitions)
                .any(|tp| denied.contains(&tp.topic_name));
            if hides_topic {
                *group = error_row(
                    &group.group_id,
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    Some(UNAUTHORIZED_TOPICS_MESSAGE.to_owned()),
                );
            }
        }
    }

    Ok(response(groups))
}

fn response(groups: Vec<DescribedGroup>) -> ShareGroupDescribeResponse {
    ShareGroupDescribeResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> DescribedGroup {
    DescribedGroup {
        group_id: group_id.into(),
        error_code,
        error_message,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::{UnknownTaggedFields, owned::share_group_describe_response};
    use krabka_security::Principal;

    use super::*;
    use crate::{authorizer::Authorizer, test_support::DenyAll};

    fn request(group_ids: &[&str]) -> ShareGroupDescribeRequest {
        ShareGroupDescribeRequest {
            group_ids: group_ids.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        }
    }

    crate::test_support::context_helper!(client_id = "admin-client");

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (handle, dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = crate::test_support::controller_peer_allowed(authorizer);
        })
        .await;
        handle.wait_until_group_coordinator_ready().await;
        handle.wait_until_share_coordinator_ready().await;
        if !share_enabled {
            crate::test_support::finalize_share_version(&handle.broker_arc_for_test(), 0).await;
        }
        (handle, dir)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    #[tokio::test]
    async fn handle_denied_groups_preserve_group_ids_and_error_codes() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let resp = handle(&broker, request(&["g1", "g2"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![
                DescribedGroup {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    group_id: "g1".into(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: Vec::new(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                DescribedGroup {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    group_id: "g2".into(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: Vec::new(),
                    authorized_operations: i32::MIN,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka's protocol gate answers every group with `UNSUPPORTED_VERSION`,
    /// before any ACL check.
    #[tokio::test]
    async fn handle_disabled_feature_wins_even_when_share_actor_exists() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), false).await;
        let broker = broker_handle.broker_arc_for_test();
        broker.group_coordinator.mark_share("g1");
        let _actor = broker.group_coordinator.get_or_create_share("g1");
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let resp = handle(&broker, request(&["g1"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: codes::UNSUPPORTED_VERSION,
                error_message: None,
                group_id: "g1".into(),
                group_state: String::new(),
                group_epoch: 0,
                assignment_epoch: 0,
                assignor_name: String::new(),
                members: Vec::new(),
                authorized_operations: i32::MIN,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    fn acl(resource_type: ResourceType, name: &str) -> krabka_metadata::MetadataRecord {
        krabka_metadata::MetadataRecord::V1AccessControlEntry(crate::test_support::allow_acl(
            resource_type,
            name,
            "User:alice",
            AclOperation::Describe,
        ))
    }

    fn topic(name: &str, topic_id: uuid::Uuid, node: u64) -> Vec<krabka_metadata::MetadataRecord> {
        let replicas = vec![krabka_raft::NodeId(node)];
        vec![
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 2,
                replication_factor: 1,
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: krabka_raft::NodeId(node),
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ]
    }

    /// Seeds share group `group_id` with member `m1` assigned partition 0 of
    /// each of `topics`, plus partition 0 of a topic the image does not hold.
    async fn seed_group(broker: &Broker, group_id: &str, topics: &[uuid::Uuid]) {
        use crate::coordinator::unified::{
            ShareGroupSeed,
            share::persistence::{
                ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            },
        };
        let mut assigned: Vec<_> = topics
            .iter()
            .map(|t| {
                (
                    krabka_protocol::primitives::uuid::Uuid(*t.as_bytes()),
                    vec![0],
                )
            })
            .collect();
        assigned.push((krabka_protocol::primitives::uuid::Uuid([0xEE; 16]), vec![0]));
        let coordinator = &broker.group_coordinator;
        coordinator.mark_share(group_id);
        let handle = coordinator.get_or_create_share(group_id);
        handle
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                group_epoch: 2,
                target_epoch: 2,
                members: [(
                    "m1".to_owned(),
                    ShareGroupMemberMetadataValue {
                        rack_id: None,
                        client_id: "client".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                    },
                )]
                .into(),
                current_per_member: [(
                    "m1".to_owned(),
                    ShareGroupCurrentMemberAssignmentValue {
                        member_epoch: 2,
                        previous_member_epoch: 1,
                        assigned_partitions: assigned,
                    },
                )]
                .into(),
                ..ShareGroupSeed::default()
            }))
            .await
            .expect("seed share group");
    }

    /// Kafka's `handleShareGroupDescribe` with an ACL authorizer: denied rows
    /// come first, a missing group names itself in the message, topic names
    /// come from the image (a topic it lacks is left out), the operations
    /// bitfield is filled on request, and a group whose assignment names a
    /// topic the caller cannot `Describe` is replaced by Kafka's
    /// `TOPIC_AUTHORIZATION_FAILED` row.
    #[tokio::test]
    async fn handle_matches_kafka_with_acls() {
        use krabka_protocol::owned::{
            common::share_group_describe_response::{
                assignment::Assignment, topic_partitions::TopicPartitions,
            },
            share_group_describe_response::Member,
        };

        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(
            Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            )),
            true,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let node = broker_handle.node_id();
        let visible = uuid::Uuid::from_u128(1);
        let hidden = uuid::Uuid::from_u128(2);
        let mut records = vec![
            acl(ResourceType::Group, "g"),
            acl(ResourceType::Group, "h"),
            acl(ResourceType::Group, "missing"),
            acl(ResourceType::Topic, "t"),
        ];
        records.extend(topic("t", visible, node));
        records.extend(topic("secret", hidden, node));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("ACLs and topics");
        seed_group(&broker, "g", &[visible]).await;
        seed_group(&broker, "h", &[visible, hidden]).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "admin-client");
        let req = ShareGroupDescribeRequest {
            group_ids: vec!["g".into(), "denied".into(), "h".into(), "missing".into()],
            include_authorized_operations: true,
            ..Default::default()
        };

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![
                error_row("denied", codes::GROUP_AUTHORIZATION_FAILED, None),
                DescribedGroup {
                    group_id: "g".into(),
                    group_state: "Stable".into(),
                    group_epoch: 2,
                    assignment_epoch: 2,
                    assignor_name: "simple".into(),
                    members: vec![Member {
                        member_id: "m1".into(),
                        member_epoch: 2,
                        client_id: "client".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                        assignment: Assignment {
                            topic_partitions: vec![TopicPartitions {
                                topic_id: krabka_protocol::primitives::uuid::Uuid(
                                    *visible.as_bytes(),
                                ),
                                topic_name: "t".into(),
                                partitions: vec![0],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    }],
                    // DESCRIBE (8) is the only group operation granted.
                    authorized_operations: 1 << 8,
                    ..Default::default()
                },
                error_row(
                    "h",
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    Some(UNAUTHORIZED_TOPICS_MESSAGE.into()),
                ),
                error_row(
                    "missing",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group missing not found.".into()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// `INVALID_GROUP_ID` for an empty id, ahead of the shard rows, and
    /// Kafka's message for a group of another type.
    #[tokio::test]
    async fn handle_refuses_empty_and_foreign_group_ids() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let _classic = broker.group_coordinator.get_or_create_classic("classic");
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let resp = handle(&broker, request(&["classic", ""]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![
                error_row("", codes::INVALID_GROUP_ID, None),
                error_row(
                    "classic",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group classic is not a share group.".into()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
