//! KIP-966 end to end: an ISR that crosses `min.insync.replicas` moves the
//! ELR the controller keeps, and `DescribeTopicPartitions` reports the move.
//!
//! The write side and the read side meet only in the metadata log, so a test
//! that stops at either one proves nothing about the pair. These drive the
//! real `AlterPartition` handler on a live controller and read the answer back
//! through the real `DescribeTopicPartitions` handler, which is the path
//! `kafka-topics --describe` takes.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use krabka_metadata::{
    LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord,
    TopicRecord,
};
use krabka_protocol::owned::{
    alter_partition_request::{
        AlterPartitionRequest, PartitionData as ReqPartitionData, TopicData as ReqTopicData,
    },
    alter_partition_response::AlterPartitionResponse,
    describe_topic_partitions_request::{DescribeTopicPartitionsRequest, TopicRequest},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
    },
};
use krabka_security::{AuthMethod, Principal};

use super::{ElrPublisher, TopicElr, state::PartitionElr};
use crate::{
    broker::Broker,
    codes,
    config_keys::{ELIGIBLE_LEADER_REPLICAS, MIN_INSYNC_REPLICAS},
    test_support::{
        decode_response, encode_request, request_context, start_broker_with_authorizer,
    },
};

const TOPIC: &str = "orders";
const TOPIC_ID_BYTES: [u8; 16] = [9; 16];
const LEADER_EPOCH: i32 = 7;
/// `AlterPartition` v2, whose `new_isr` is a plain broker-id list. v3
/// replaces it with `new_isr_with_epochs`, which drags in the KIP-903
/// broker-epoch eligibility check and so a registration for every proposed
/// member; the ELR rules do not vary by request version, so the older field
/// keeps the fixture to the partition state the test is actually about.
const ALTER_VERSION: i16 = 2;
const DESCRIBE_VERSION: i16 =
    krabka_protocol::owned::describe_topic_partitions_response::MAX_VERSION;

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn partition_record(isr: &[u64]) -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.into(),
        partition: 0,
        leader: NodeId(1),
        replicas: nodes(&[1, 2, 3]),
        isr: nodes(isr),
        leader_epoch: LeaderEpoch(LEADER_EPOCH),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![uuid::Uuid::nil(); 3],
        partition_epoch: 4,
    }
}

/// The records that put topic `orders` in the image: one RF=3 partition whose
/// ISR is full, and a `min.insync.replicas` of 2, which is what gives the
/// partition an ELR to fall below.
fn seed_records() -> Vec<MetadataRecord> {
    vec![
        MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.into(),
            topic_id: uuid::Uuid::from_bytes(TOPIC_ID_BYTES),
            partitions: 1,
            replication_factor: 3,
        }),
        MetadataRecord::V1Partition(partition_record(&[1, 2, 3])),
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.into(),
            overrides: [(MIN_INSYNC_REPLICAS.to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        }),
    ]
}

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|node| node == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn principal() -> Principal {
    Principal {
        name: "replica".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

fn peer() -> SocketAddr {
    "127.0.0.1:9092".parse().expect("peer address")
}

/// Propose `new_isr` for partition 0 through the real `AlterPartition`
/// handler, and assert the controller accepted it.
async fn alter_isr(broker: &Arc<Broker>, new_isr: &[i32]) {
    let principal = principal();
    let peer = peer();
    let ctx = request_context(&principal, &peer, "broker-client");
    let request = AlterPartitionRequest {
        broker_id: 1,
        broker_epoch: -1,
        topics: vec![ReqTopicData {
            topic_id: krabka_protocol::primitives::uuid::Uuid(TOPIC_ID_BYTES),
            partitions: vec![ReqPartitionData {
                partition_index: 0,
                leader_epoch: LEADER_EPOCH,
                new_isr: new_isr.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let bytes = crate::handlers::alter_partition::handle(
        broker,
        ALTER_VERSION,
        1,
        &encode_request(&request, ALTER_VERSION),
        &ctx,
    )
    .await
    .expect("AlterPartition");
    let response: AlterPartitionResponse = decode_response(&bytes, ALTER_VERSION);

    assert!(response.error_code == codes::NONE);
    assert!(
        response.topics[0].partitions[0].error_code == codes::NONE,
        "AlterPartition refused the proposal: {response:?}"
    );
}

/// The partition row `DescribeTopicPartitions` answers with for partition 0.
async fn describe_partition(broker: &Arc<Broker>) -> DescribeTopicPartitionsResponsePartition {
    let principal = principal();
    let peer = peer();
    let ctx = request_context(&principal, &peer, "admin-client");
    let request = DescribeTopicPartitionsRequest {
        topics: vec![TopicRequest {
            name: TOPIC.into(),
            ..Default::default()
        }],
        response_partition_limit: 2000,
        ..Default::default()
    };
    let bytes = crate::handlers::describe_topic_partitions::handle(
        broker,
        DESCRIBE_VERSION,
        2,
        &encode_request(&request, DESCRIBE_VERSION),
        &ctx,
    )
    .await
    .expect("DescribeTopicPartitions");
    let response: DescribeTopicPartitionsResponse = decode_response(&bytes, DESCRIBE_VERSION);

    response
        .topics
        .into_iter()
        .find(|topic| topic.name.as_deref() == Some(TOPIC))
        .expect("orders topic row")
        .partitions
        .into_iter()
        .next()
        .expect("partition 0 row")
}

/// The expected partition row for an ISR of `isr` and the ELR of `eligible`.
///
/// Only node 1 is registered, so nodes 2 and 3 are reported offline whatever
/// the ISR says. Comparing the whole struct keeps the ELR assertion honest:
/// the ELR columns cannot be read as having moved because some neighbouring
/// field moved instead.
fn expected_row(isr: &[i32], eligible: &[i32]) -> DescribeTopicPartitionsResponsePartition {
    DescribeTopicPartitionsResponsePartition {
        error_code: codes::NONE,
        partition_index: 0,
        leader_id: 1,
        leader_epoch: LEADER_EPOCH,
        replica_nodes: vec![1, 2, 3],
        isr_nodes: isr.to_vec(),
        eligible_leader_replicas: Some(eligible.to_vec()),
        last_known_elr: Some(Vec::new()),
        offline_replicas: vec![2, 3],
        ..Default::default()
    }
}

/// The issue's acceptance path: shrink the ISR below `min.insync.replicas`
/// and the replicas it dropped are reported eligible; expand it back to
/// `min.insync.replicas` and the set clears.
#[tokio::test]
async fn an_isr_that_crosses_min_insync_replicas_moves_the_reported_elr() {
    let (handle, _dir) =
        start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(seed_records())
        .await
        .expect("seed orders");

    assert!(describe_partition(&broker).await == expected_row(&[1, 2, 3], &[]));

    alter_isr(&broker, &[1]).await;
    assert!(describe_partition(&broker).await == expected_row(&[1], &[2, 3]));

    alter_isr(&broker, &[1, 2]).await;
    assert!(describe_partition(&broker).await == expected_row(&[1, 2], &[]));

    handle.shutdown().await;
}

/// A shrink that stops at `min.insync.replicas` leaves nothing eligible, so
/// the controller publishes nothing and the columns stay empty. Without this
/// row the test above would pass on a publisher that made every ISR change
/// eligible.
#[tokio::test]
async fn an_isr_that_stays_at_min_insync_replicas_reports_no_elr() {
    let (handle, _dir) =
        start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(seed_records())
        .await
        .expect("seed orders");

    alter_isr(&broker, &[1, 2]).await;

    assert!(describe_partition(&broker).await == expected_row(&[1, 2], &[]));

    handle.shutdown().await;
}

/// The state the controller publishes is an ordinary `V1TopicConfig`, so a
/// snapshot carries it and a replay of that snapshot projects the same two
/// lists. Nothing about the ELR needs a record type of its own to survive a
/// restart, and this is the test that says so.
#[test]
fn the_published_state_round_trips_through_a_snapshot() {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for record in seed_records() {
        image.apply(&record);
    }

    // Shrink to one replica, the way the controller does it, and apply what
    // the publisher decided.
    let mut changes = vec![MetadataRecord::V1Partition(partition_record(&[1]))];
    ElrPublisher::new(&image).extend(&mut changes);
    for record in &changes {
        image.apply(record);
    }
    let before = TopicElr::of_topic(&image, TOPIC).partition(0);
    assert!(
        before
            == PartitionElr {
                eligible_leader_replicas: vec![2, 3],
                last_known_elr: vec![],
            }
    );

    // Snapshot, restore, and read the same partition back.
    let mut restored = MetadataImage::new(uuid::Uuid::nil());
    for record in image.to_records() {
        restored.apply(&record);
    }

    assert!(TopicElr::of_topic(&restored, TOPIC).partition(0) == before);
    // The value itself survives byte for byte, not just its projection: a
    // snapshot that rewrote it would still project correctly today and drift
    // the first time the grammar grows.
    assert!(
        restored
            .topic_config(TOPIC)
            .and_then(|configs| configs.get(ELIGIBLE_LEADER_REPLICAS))
            == image
                .topic_config(TOPIC)
                .and_then(|configs| configs.get(ELIGIBLE_LEADER_REPLICAS))
    );
}
