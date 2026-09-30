//! Tests for finding the changed topics of an image and for the refresh
//! request that goes to the groups this broker coordinates.

use std::collections::BTreeMap;

use assert2::check;
use krabka_metadata::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataImage, MetadataRecord, NodeId,
    PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::owned::{
    common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
};
use uuid::Uuid;

use super::{changed_topics, on_metadata_update, regex_resolution_may_change};
use crate::coordinator::unified::{
    actor::{GroupActorHandle, GroupActorMessage},
    test_support::{SwitchableMetadata, make_coord_with_metadata, snapshot_of},
};

fn topic(name: &str, id: u128) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::from_u128(id),
        partitions: 0,
        replication_factor: 2,
    })
}

fn partition(name: &str, index: i32, leader: u64) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: name.into(),
        partition: index,
        leader: NodeId(leader),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1), NodeId(2)],
        ..Default::default()
    })
}

fn image(records: &[MetadataRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::from_u128(1));
    for record in records {
        image.apply(record);
    }
    image
}

/// Kafka's `TopicsDelta.changedTopics` holds every topic with a
/// `TopicRecord`, `PartitionRecord` or `PartitionChangeRecord` in the delta,
/// and `deletedTopicIds` the deleted ones. A configuration or a broker is not
/// a topic change.
#[test]
fn changed_topics_are_the_created_changed_and_deleted_topics() {
    type Row = (&'static str, Vec<MetadataRecord>, Vec<&'static str>);
    let base = [
        topic("orders", 10),
        partition("orders", 0, 1),
        topic("payments", 20),
        partition("payments", 0, 1),
    ];
    let rows: [Row; 8] = [
        ("no change", vec![], vec![]),
        (
            "a topic is created",
            vec![topic("refunds", 30), partition("refunds", 0, 1)],
            vec!["refunds"],
        ),
        (
            "a topic grows",
            vec![partition("orders", 1, 2)],
            vec!["orders"],
        ),
        (
            "a partition gets a new leader",
            vec![partition("orders", 0, 2)],
            vec!["orders"],
        ),
        (
            "a topic is deleted",
            vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "payments".into(),
            })],
            vec!["payments"],
        ),
        (
            "a topic is deleted and created again",
            vec![
                MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: "orders".into(),
                }),
                topic("orders", 11),
                partition("orders", 0, 1),
            ],
            vec!["orders"],
        ),
        (
            "a topic configuration changes",
            vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".into(),
                overrides: BTreeMap::from([("retention.ms".into(), "1000".into())]),
            })],
            vec![],
        ),
        (
            "a broker registers",
            vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    fenced: false,
                    in_controlled_shutdown: false,
                    cordoned_log_dirs: None,
                    node_id: NodeId(3),
                    broker_epoch: 3,
                    incarnation_id: Uuid::from_u128(3),
                    host: "broker-3".into(),
                    port: 9092,
                    rack: Some("rack-c".into()),
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: BTreeMap::new(),
                },
            )],
            vec![],
        ),
    ];
    let mut found = Vec::new();
    let mut expected = Vec::new();
    for (name, changes, topics) in rows {
        let previous = image(&base);
        let next = image(&[base.as_slice(), changes.as_slice()].concat());
        found.push((name, changed_topics(&previous, &next)));
        expected.push((name, topics.into_iter().map(str::to_owned).collect()));
    }
    check!(found == expected);
}

fn describe_acl(topic: &str) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(krabka_metadata::AclEntry {
        resource_type: krabka_metadata::ResourceType::Topic,
        resource_name: topic.into(),
        pattern_type: krabka_metadata::PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: krabka_metadata::AclOperation::Describe,
        permission_type: krabka_metadata::PermissionType::Allow,
    })
}

/// A created topic and a changed ACL can change what a regular expression
/// resolves to. A deleted topic, a grown one, a new leader and a
/// configuration cannot: the assignment follows those through the metadata
/// hash, and the next resolution drops a deleted topic.
#[test]
fn a_new_topic_or_a_changed_acl_can_change_a_resolution() {
    type Row = (&'static str, Vec<MetadataRecord>, bool);
    let base = [
        topic("orders", 10),
        partition("orders", 0, 1),
        topic("payments", 20),
        partition("payments", 0, 1),
        describe_acl("orders"),
    ];
    let rows: [Row; 9] = [
        ("no change", vec![], false),
        (
            "a topic is created",
            vec![topic("refunds", 30), partition("refunds", 0, 1)],
            true,
        ),
        (
            "a topic is deleted and created again with a new id",
            vec![
                MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: "orders".into(),
                }),
                topic("orders", 11),
                partition("orders", 0, 1),
            ],
            true,
        ),
        ("an ACL is granted", vec![describe_acl("payments")], true),
        (
            "an ACL is deleted",
            vec![MetadataRecord::V1DeleteAccessControlEntry(
                krabka_metadata::AclEntryFilter {
                    resource_type: Some(krabka_metadata::ResourceType::Topic),
                    resource_name: Some("orders".into()),
                    ..Default::default()
                },
            )],
            true,
        ),
        (
            "a topic is deleted",
            vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "payments".into(),
            })],
            false,
        ),
        ("a topic grows", vec![partition("orders", 1, 2)], false),
        (
            "a partition gets a new leader",
            vec![partition("orders", 0, 2)],
            false,
        ),
        (
            "a topic configuration changes",
            vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".into(),
                overrides: BTreeMap::from([("retention.ms".into(), "1000".into())]),
            })],
            false,
        ),
    ];
    let mut found = Vec::new();
    let mut expected = Vec::new();
    for (name, changes, may_change) in rows {
        let previous = image(&base);
        let next = image(&[base.as_slice(), changes.as_slice()].concat());
        found.push((name, regex_resolution_may_change(&previous, &next)));
        expected.push((name, may_change));
    }
    check!(found == expected);
}

async fn heartbeat(
    handle: &GroupActorHandle,
    request: ConsumerGroupHeartbeatRequest,
) -> ConsumerGroupHeartbeatResponse {
    let (reply, response) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request,
            client_id: "client".into(),
            client_host: "host".into(),
            regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
            reply,
        })
        .await
        .unwrap();
    response.await.unwrap()
}

/// Only the groups whose `__consumer_offsets` partition this broker leads
/// refresh their metadata, as Kafka applies an image to its active shards
/// only. Each group has one member, which subscribed to `orders` before the
/// topic existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metadata_update_refreshes_the_groups_this_broker_coordinates() {
    let metadata = SwitchableMetadata::new(snapshot_of(&[]));
    let coordinator = make_coord_with_metadata(metadata.clone());
    let groups = ["a", "b", "not-owned"];
    for group_id in groups {
        let handle = coordinator.get_or_create_consumer(group_id);
        let joined = heartbeat(
            &handle,
            ConsumerGroupHeartbeatRequest {
                group_id: group_id.into(),
                member_id: "m1".into(),
                member_epoch: 0,
                rebalance_timeout_ms: 60_000,
                subscribed_topic_names: Some(vec!["orders".into()]),
                topic_partitions: Some(vec![]),
                ..Default::default()
            },
        )
        .await;
        check!(joined.member_epoch == 1, "{group_id}");
    }

    metadata.set(snapshot_of(&[("orders", 1, 2)]));
    on_metadata_update(
        &coordinator,
        |group_id| group_id != "not-owned",
        &["orders".to_string()],
    )
    .await;

    let mut answers = Vec::new();
    for group_id in groups {
        let handle = coordinator.find(group_id).expect("the group's actor");
        let answer = heartbeat(
            &handle,
            ConsumerGroupHeartbeatRequest {
                group_id: group_id.into(),
                member_id: "m1".into(),
                member_epoch: 1,
                rebalance_timeout_ms: -1,
                ..Default::default()
            },
        )
        .await;
        answers.push((group_id, answer));
    }

    let answer = |member_epoch, assignment: Option<Vec<i32>>| ConsumerGroupHeartbeatResponse {
        member_id: Some("m1".into()),
        member_epoch,
        heartbeat_interval_ms: 5_000,
        assignment: assignment.map(|partitions| Assignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: krabka_protocol::primitives::uuid::Uuid([1; 16]),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    check!(
        answers
            == vec![
                ("a", answer(2, Some(vec![0, 1]))),
                ("b", answer(2, Some(vec![0, 1]))),
                ("not-owned", answer(1, None)),
            ]
    );
}
