//! Shared group-handler fixtures and subscription authorization cases.

use std::collections::HashSet;

use assert2::assert;
use krabka_metadata::{
    AclOperation, FeatureLevelRecord, MetadataImage, MetadataRecord, ResourceType,
};

pub(super) fn acl_authorizer() -> crate::authorizer::SimpleAclAuthorizer {
    crate::authorizer::SimpleAclAuthorizer::new(HashSet::new())
}

/// An operation on a named resource, allowed to `User:alice` from any host.
pub(super) fn acl(
    resource_type: ResourceType,
    name: &str,
    operation: AclOperation,
) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(crate::test_support::allow_acl(
        resource_type,
        name,
        "User:alice",
        operation,
    ))
}

/// `Describe` on `Topic(name)`, allowed to `User:alice` from any host.
pub(super) fn describe_acl(name: &str) -> MetadataRecord {
    acl(ResourceType::Topic, name, AclOperation::Describe)
}

/// `Read` on `Group(name)`, allowed to `User:alice` from any host.
pub(super) fn group_read_acl(name: &str) -> MetadataRecord {
    acl(ResourceType::Group, name, AclOperation::Read)
}

fn group_version(level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
        level,
    })
}

pub(super) fn image_with_group_version(level: i16) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&group_version(level));
    image
}

/// Override the broker's release-default finalized consumer-group feature.
pub(super) async fn set_group_version(broker: &crate::broker::Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![group_version(level)])
        .await
        .expect("set group.version");
}

/// A bare `V1Topic` record of `partitions` partitions.
fn topic_record(name: &str, topic_id: uuid::Uuid, partitions: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id,
        partitions,
        replication_factor: 1,
    })
}

/// A `V1Topic` record plus one `V1Partition` per index, assigned to
/// `node`. The KIP-631 wire framing does not carry `TopicRecord.partitions`
/// -- a decoded `V1Topic` round-trips back at `partitions == 0`, and the
/// real count comes from the `V1Partition` records that follow it -- so a
/// topic meant to be assignable needs both, unlike [`topic_record`] alone
/// (used only where a test never reaches the assignor).
pub(super) fn topic_with_partitions(
    name: &str,
    topic_id: uuid::Uuid,
    partitions: i32,
    node: krabka_raft::NodeId,
) -> Vec<MetadataRecord> {
    let mut records = vec![topic_record(name, topic_id, partitions)];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(crate::handlers::test_support::single_replica_partition(
            name, partition, node,
        ))
    }));
    records
}

/// The SASL/PLAIN principal `User:alice` that the ACLs above name.
pub(super) fn alice() -> krabka_security::Principal {
    crate::test_support::sasl_principal("alice")
}

/// Kafka's explicit-name Describe check, shared by consumer and share heartbeats.
pub(super) fn subscribed_names_describe_denied_table() {
    let authorizer = acl_authorizer();
    request_identity!(
        (principal, peer, ctx),
        alice(),
        client_id = "c",
        address = crate::test_support::peer()
    );
    for (label, granted, names, expected_denied) in [
        ("no subscription", &[][..], None, false),
        ("empty subscription", &[][..], Some(&[][..]), false),
        (
            "single name, fully authorized",
            &["orders"][..],
            Some(&["orders"][..]),
            false,
        ),
        (
            "single name, not authorized",
            &[][..],
            Some(&["orders"][..]),
            true,
        ),
        (
            "two names, one denied",
            &["orders"][..],
            Some(&["orders", "shipments"][..]),
            true,
        ),
        (
            "two names, both authorized",
            &["orders", "shipments"][..],
            Some(&["orders", "shipments"][..]),
            false,
        ),
    ] {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for name in granted {
            image.apply(&describe_acl(name));
        }
        let names = names.map(|names| {
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
        });
        assert!(
            crate::handlers::subscribed_names_describe_denied(
                &authorizer,
                &image,
                &ctx,
                names.as_deref(),
            ) == expected_denied,
            "{label}"
        );
    }
}
pub(super) fn streams_request(
    group_id: &str,
    member_id: &str,
) -> krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
    streams_request_with_topology(group_id, member_id, "in", Vec::new())
}

pub(super) fn streams_request_with_topology(
    group_id: &str,
    member_id: &str,
    source_topic: &str,
    state_changelog_topics: Vec<
        krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo,
    >,
) -> krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
    use krabka_protocol::owned::streams_group_heartbeat_request::{
        StreamsGroupHeartbeatRequest, Subtopology, Topology,
    };
    StreamsGroupHeartbeatRequest {
        group_id: group_id.into(),
        member_id: member_id.into(),
        member_epoch: 0,
        rebalance_timeout_ms: 1_000,
        active_tasks: Some(vec![]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        topology: Some(Topology {
            epoch: 1,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec![source_topic.into()],
                state_changelog_topics,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}
