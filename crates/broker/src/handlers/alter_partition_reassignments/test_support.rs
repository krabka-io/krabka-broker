//! The request builder, the metadata images, and the context helper that the
//! `AlterPartitionReassignments` tests share.
//!
//! The response tests and the end-to-end handler tests build the same
//! single-partition request, and the
//! planning tests and the cancel-approval tests seed the same one-partition
//! image, so the fixtures live in one module rather than once per test file.

use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use krabka_protocol::owned::alter_partition_reassignments_request::{
    AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
};

use crate::test_support::ReassignmentSetup;

pub(super) fn request(
    allow_replication_factor_change: bool,
    topic: &str,
    partition_index: i32,
    replicas: Option<Vec<i32>>,
) -> AlterPartitionReassignmentsRequest {
    AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change,
        topics: vec![ReassignableTopic {
            name: topic.into(),
            partitions: vec![ReassignablePartition {
                partition_index,
                replicas,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

/// An image holding topic `foo` with one partition in the given reassignment
/// state, at partition epoch 0.
pub(super) fn img_with(
    replicas: &[u64],
    isr: &[u64],
    adding: &[u64],
    removing: &[u64],
    leader: u64,
) -> MetadataImage {
    img_with_epoch(replicas, isr, adding, removing, leader, 0)
}

/// [`img_with`], with the partition epoch pinned, for the tests that check the
/// epoch a planned record bumps to.
pub(super) fn img_with_epoch(
    replicas: &[u64],
    isr: &[u64],
    adding: &[u64],
    removing: &[u64],
    leader: u64,
    partition_epoch: i32,
) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    // Register brokers 1..=6 so validate_target accepts target lists.
    for n in 1u64..=6 {
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: "localhost".into(),
                ..crate::test_support::broker_registration(n)
            },
        ));
    }
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "foo".into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        partition_epoch,
        ..crate::test_support::reassignment_partition(ReassignmentSetup {
            replicas,
            isr,
            adding,
            removing,
            leader,
            ..Default::default()
        })
    }));
    img
}
