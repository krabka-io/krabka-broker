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

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ReassignmentRequestSetup<'a> {
    pub allow_replication_factor_change: bool,
    #[default("orders")]
    pub topic: &'a str,
    #[default(7)]
    pub partition_index: i32,
    #[default(Some(vec![1, 2]))]
    pub replicas: Option<Vec<i32>>,
}

pub(super) fn request(setup: ReassignmentRequestSetup<'_>) -> AlterPartitionReassignmentsRequest {
    let ReassignmentRequestSetup {
        allow_replication_factor_change,
        topic,
        partition_index,
        replicas,
    } = setup;
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

/// A registered six-broker image with one partition in the supplied reassignment state.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ReassignmentImageSetup<'a> {
    pub assignment: ReassignmentSetup<'a>,
    pub partition_epoch: i32,
}

pub(super) fn img_with(setup: ReassignmentImageSetup<'_>) -> MetadataImage {
    let ReassignmentImageSetup {
        assignment,
        partition_epoch,
    } = setup;
    let replicas = assignment.replicas;
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
        ..crate::test_support::reassignment_partition(assignment)
    }));
    img
}
