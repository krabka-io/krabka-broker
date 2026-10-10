//! Metadata-image fixtures, request builders, and the broker harness that the
//! `AlterPartition` tests share.
//!
//! The per-partition validation tests and the end-to-end handler tests build
//! the same topic, the same partition record, and the same broker
//! registrations, so those fixtures live in one module rather than being
//! duplicated per test file.

use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use krabka_protocol::{
    owned::alter_partition_request::{
        AlterPartitionRequest, BrokerState, TopicData as ReqTopicData,
    },
    primitives::uuid::Uuid as ProtoUuid,
};

use crate::broker::Broker;

const TOPIC_ID_BYTES: [u8; 16] = [7; 16];

fn reg(node_id: u64, epoch: i64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        broker_epoch: epoch,
        host: "h".into(),
        ..crate::test_support::broker_registration(node_id)
    })
}

pub(super) struct PartitionFixture<'a> {
    pub(super) partition: i32,
    pub(super) leader: u64,
    pub(super) replicas: &'a [u64],
    pub(super) isr: &'a [u64],
    pub(super) leader_epoch: i32,
    pub(super) partition_epoch: i32,
}

/// An image with the topic "t" and one partition. `epochs` gives the
/// registered brokers.
pub(super) fn image_with_partition(
    fixture: &PartitionFixture<'_>,
    epochs: &[(u64, i64)],
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: topic_id(),
        partitions: fixture.partition + 1,
        replication_factor: i16::try_from(fixture.replicas.len()).expect("rf fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "t".into(),
        partition: fixture.partition,
        leader: krabka_metadata::NodeId(fixture.leader),
        replicas: fixture
            .replicas
            .iter()
            .map(|&n| krabka_metadata::NodeId(n))
            .collect(),
        isr: fixture
            .isr
            .iter()
            .map(|&n| krabka_metadata::NodeId(n))
            .collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(fixture.leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: fixture.partition_epoch,
    }));
    for &(id, ep) in epochs {
        image.apply(&reg(id, ep));
    }
    image
}

/// An image with topic "t" partition 0, replicas [1,2,3], isr [1,2], and
/// leader 1 at `leader_epoch` 5. `epochs` gives the registered brokers.
pub(super) fn image_with(epochs: &[(u64, i64)]) -> MetadataImage {
    image_with_partition(
        &PartitionFixture {
            partition: 0,
            leader: 1,
            replicas: &[1, 2, 3],
            isr: &[1, 2],
            leader_epoch: 5,
            partition_epoch: 0,
        },
        epochs,
    )
}

fn topic_id() -> uuid::Uuid {
    uuid::Uuid::from_bytes(TOPIC_ID_BYTES)
}

pub(super) fn wire_topic_id() -> ProtoUuid {
    ProtoUuid(TOPIC_ID_BYTES)
}

pub(super) fn bs(broker_id: i32, broker_epoch: i64) -> BrokerState {
    BrokerState {
        broker_id,
        broker_epoch,
        ..Default::default()
    }
}

pub(super) async fn seed_partition(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: topic_id(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                ..crate::handlers::test_support::replicated_partition(
                    crate::handlers::test_support::ReplicatedPartitionSetup {
                        topic: "t",
                        replicas: &[krabka_metadata::NodeId(1)],
                        ..Default::default()
                    },
                )
            }),
        ])
        .await
        .expect("seed topic partition");
}

/// A request from broker 1 at the broker epoch its registration holds, or -1
/// before it registers.
pub(super) fn request_with_topics(
    broker: &Broker,
    topics: Vec<ReqTopicData>,
) -> AlterPartitionRequest {
    AlterPartitionRequest {
        broker_id: 1,
        broker_epoch: broker
            .controller
            .current_image()
            .broker_epoch(krabka_metadata::NodeId(1))
            .unwrap_or(-1),
        topics,
        ..Default::default()
    }
}
