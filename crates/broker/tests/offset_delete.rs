//! `OffsetDelete` (`api_key` 47, KIP-496) broker integration tests.
//!
//! Each test boots a single broker with `support::start`, waits until its
//! group coordinator is ready, drives the relevant flows over the PLAINTEXT
//! data plane, and asserts on the response shape. Single-broker tests stay
//! unconditional, and this file uses only `support::start`, which is
//! cross-platform, so it needs no platform gate.
#![cfg(test)]

use {
    crate::support::{
        classic::{ClassicTimeouts, classic_join_request, join_protocol, required_member_id},
        client::create_topic as create_support_topic,
        offsets::{
            offset_commit_partition, offset_commit_topic, offset_delete_partition,
            offset_delete_request, offset_delete_topic, offset_fetch_group, offset_fetch_request,
            offset_fetch_topic,
        },
    },
    assert2::{assert, check},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};
mod support;

use {
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::{
        Encode,
        owned::{
            consumer_protocol_subscription::ConsumerProtocolSubscription,
            join_group_response::JoinGroupResponse,
            offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
            offset_delete_response::{
                OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
            },
        },
    },
    support::topic_id_for,
};

const OFFSET_ABSENT_SENTINEL: i64 = -1; // OffsetFetch returns -1 when no offset is committed.

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    create_support_topic(&p.client, name, num_partitions).await;
}

async fn commit_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32, off: i64) {
    let id = topic_id_for(&p.client, topic).await;
    let resp = p
        .client
        .send(OffsetCommitRequest {
            group_id: group.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![offset_commit_topic(topic, id, vec![
                OffsetCommitRequestPartition {
                    committed_leader_epoch: -1,
                    ..offset_commit_partition(partition, off, Some(String::new()))
                },
            ])],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        resp.topics[0].partitions[0].error_code == 0,
        "OffsetCommit({group},{topic},{partition})"
    );
}

async fn fetch_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32) -> i64 {
    let id = topic_id_for(&p.client, topic).await;
    let resp = p
        .client
        .send(offset_fetch_request(offset_fetch_group(
            group,
            Some(vec![offset_fetch_topic(topic, id, vec![partition])]),
        )))
        .await
        .expect("OffsetFetch");
    resp.groups[0].topics[0].partitions[0].committed_offset
}

struct OffsetDeletion {
    broker: Option<InProcess>,
    group: String,
    topic: String,
    response: Option<OffsetDeleteResponse>,
    bootstrap: Option<JoinGroupResponse>,
    subscriber: Option<JoinGroupResponse>,
}

#[fixture]
fn deletion() -> OffsetDeletion {
    // Start this scenario with fresh inputs and observations.

    OffsetDeletion {
        broker: None,
        group: String::new(),
        topic: String::new(),
        response: None,
        bootstrap: None,
        subscriber: None,
    }
}

#[given("group {group} has offsets on topic {topic} with {partitions} partitions")]
async fn committed_offsets(
    deletion: &mut OffsetDeletion,
    group: String,
    topic: String,
    partitions: i32,
    datatable: Vec<Vec<String>>,
) {
    let broker = start().await;
    create_topic(&broker, &topic, partitions).await;
    for row in datatable.into_iter().skip(1) {
        let [partition, offset]: [String; 2] =
            row.try_into().expect("partition and committed offset");
        commit_offset(
            &broker,
            &group,
            &topic,
            partition.parse().expect("partition index"),
            offset.parse().expect("offset"),
        )
        .await;
    }
    deletion.broker = Some(broker);
    deletion.group = group;
    deletion.topic = topic;
}

#[given("topic t2 exists but group ghost has never joined or committed")]
async fn unknown_group(deletion: &mut OffsetDeletion) {
    let broker = start().await;
    create_topic(&broker, "t2", 1).await;
    deletion.broker = Some(broker);
    deletion.group = "ghost".into();
    deletion.topic = "t2".into();
}

#[when("offsets are deleted from topic {topic} for partitions {partitions}")]
async fn delete_offsets(deletion: &mut OffsetDeletion, topic: String, partitions: String) {
    let broker = deletion.broker.as_ref().expect("a coordinator was started");
    let partitions = partitions
        .split(',')
        .map(|partition| {
            offset_delete_partition(partition.trim().parse().expect("partition index"))
        })
        .collect();
    deletion.response = Some(
        broker
            .client
            .send(offset_delete_request(&deletion.group, vec![
                offset_delete_topic(&topic, partitions),
            ]))
            .await
            .expect("OffsetDelete"),
    );
}

#[then("offset deletion returns top-level {code} and topic {topic} partition results")]
fn partition_results(
    deletion: &OffsetDeletion,
    code: i16,
    topic: String,
    datatable: Vec<Vec<String>>,
) {
    let partitions = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [partition, error_code]: [String; 2] =
                row.try_into().expect("partition and error code");
            OffsetDeleteResponsePartition {
                partition_index: partition.parse().expect("partition index"),
                error_code: error_code.parse().expect("error code"),
                ..Default::default()
            }
        })
        .collect();
    let expected = OffsetDeleteResponse {
        error_code: code,
        topics: vec![OffsetDeleteResponseTopic {
            name: topic,
            partitions,
            ..Default::default()
        }],
        ..Default::default()
    };
    check!(deletion.response.as_ref().expect("OffsetDelete completed") == &expected);
}

#[then("the unknown group is refused without any topic rows")]
fn group_refusal(deletion: &OffsetDeletion) {
    let expected = OffsetDeleteResponse {
        error_code: 69,
        ..Default::default()
    };
    assert!(
        deletion.response.as_ref().expect("OffsetDelete completed") == &expected,
        "top-level GROUP_ID_NOT_FOUND (69) and no topic rows"
    );
}

#[then("the remaining committed offsets are exactly")]
async fn remaining_offsets(deletion: &OffsetDeletion, datatable: Vec<Vec<String>>) {
    let broker = deletion.broker.as_ref().expect("a coordinator was started");
    for row in datatable.into_iter().skip(1) {
        let [partition, offset]: [String; 2] =
            row.try_into().expect("partition and remaining offset");
        let partition = partition.parse().expect("partition index");
        let expected = if offset == "absent" {
            OFFSET_ABSENT_SENTINEL
        } else {
            offset.parse().expect("remaining offset")
        };
        check!(
            fetch_offset(broker, &deletion.group, &deletion.topic, partition).await == expected,
            "partition {partition}"
        );
    }
}

#[when("a consumer joins with a subscription to the offset topic")]
async fn subscribe(deletion: &mut OffsetDeletion) {
    let broker = deletion.broker.as_ref().expect("a coordinator was started");
    let subscription = ConsumerProtocolSubscription {
        topics: vec![deletion.topic.clone()],
        ..Default::default()
    };
    let mut encoded = BytesMut::new();
    encoded.put_i16(0);
    subscription
        .encode(&mut encoded, 0)
        .expect("encode subscription");
    let encoded = Bytes::from(encoded.to_vec());
    let timeouts = ClassicTimeouts {
        rebalance: krabka_units::millis(1_500),
        ..Default::default()
    };
    let bootstrap = broker
        .client
        .send(
            classic_join_request()
                .group_id(deletion.group.clone())
                .timeouts(timeouts)
                .protocols(vec![join_protocol("range", encoded.clone())])
                .call(),
        )
        .await
        .expect("JoinGroup1");
    let joined = broker
        .client
        .send(
            classic_join_request()
                .group_id(deletion.group.clone())
                .member_id(bootstrap.member_id.clone())
                .timeouts(timeouts)
                .protocols(vec![join_protocol("range", encoded)])
                .call(),
        )
        .await
        .expect("JoinGroup2");
    deletion.bootstrap = Some(bootstrap);
    deletion.subscriber = Some(joined);
}

#[then("membership bootstrap assigns an id and the subscriber joins successfully")]
fn subscribed(deletion: &OffsetDeletion) {
    let bootstrap = deletion
        .bootstrap
        .as_ref()
        .expect("membership bootstrap completed");
    let member = required_member_id(bootstrap.error_code, bootstrap.member_id.clone());
    assert!(!member.is_empty());
    assert!(
        deletion
            .subscriber
            .as_ref()
            .expect("subscription JoinGroup completed")
            .error_code
            == 0
    );
}

async fn stop_offset_deletion(deletion: OffsetDeletion) {
    deletion
        .broker
        .expect("a coordinator was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/offset_delete.feature",
    name = "Deleting one inactive-group partition preserves the others"
)]
#[tokio::test]
async fn delete_offsets_from_empty_group_round_trip(deletion: OffsetDeletion) {
    stop_offset_deletion(deletion).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offset_delete.feature",
    name = "An unknown group is refused before partition processing"
)]
#[tokio::test]
async fn delete_offsets_unknown_group_returns_group_id_not_found(deletion: OffsetDeletion) {
    stop_offset_deletion(deletion).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offset_delete.feature",
    name = "An existing group still receives a missing-topic partition error"
)]
#[tokio::test]
async fn delete_offsets_missing_topic_returns_unknown_topic_or_partition(deletion: OffsetDeletion) {
    stop_offset_deletion(deletion).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offset_delete.feature",
    name = "An invalid partition is refused before a valid partition is deleted"
)]
#[tokio::test]
async fn delete_offsets_partition_out_of_range_returns_unknown_topic_or_partition(
    deletion: OffsetDeletion,
) {
    stop_offset_deletion(deletion).await;
}

#[scenario(
    path = "tests/features/migrated/tests/offset_delete.feature",
    name = "A subscribed topic refuses deletion and retains its committed offset"
)]
#[tokio::test]
async fn delete_offsets_for_subscribed_topic_returns_group_subscribed(deletion: OffsetDeletion) {
    stop_offset_deletion(deletion).await;
}
