//! Metadata-image and liveness fixture builders that the reassignment unit
//! tests share. The policy tests and the background-task tests both need an
//! image with one in-flight reassignment, so the builders live in one place
//! rather than in either test module.

use std::sync::Arc;

use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use uuid::Uuid;

use crate::heartbeat::controller_state::ControllerLivenessState;

pub(super) fn img(
    replicas: &[u64],
    isr: &[u64],
    adding: &[u64],
    removing: &[u64],
    leader: u64,
) -> Arc<MetadataImage> {
    img_with_dirs(replicas, isr, adding, removing, leader, &[])
}

pub(super) async fn liveness(alive: &[u64]) -> ControllerLivenessState {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in alive {
        l.record_heartbeat(*n).await;
    }
    l
}

pub(super) fn first_partition(rec: &MetadataRecord) -> &PartitionRecord {
    match rec {
        MetadataRecord::V1Partition(p) => p,
        _ => panic!("expected V1Partition"),
    }
}

/// Builds an image with explicit directories. It tests that
/// `compute_reassignment_progress` keeps the directories aligned after a
/// completion removes a replica from the set.
pub(super) fn img_with_dirs(
    replicas: &[u64],
    isr: &[u64],
    adding: &[u64],
    removing: &[u64],
    leader: u64,
    directories: &[Uuid],
) -> Arc<MetadataImage> {
    let mut image = MetadataImage::new(Uuid::nil());
    for n in 1..=6u64 {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: String::new(),
                port: 0,
                ..crate::test_support::broker_registration(n)
            },
        ));
    }
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "foo".into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        directories: directories.to_vec(),
        ..crate::test_support::reassignment_partition(replicas, isr, (adding, removing), leader)
    }));
    Arc::new(image)
}
