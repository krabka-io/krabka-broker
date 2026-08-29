//! Fixtures shared by the unit tests of the `topology` module tree.
//!
//! The module builds a [`MetadataImage`] that holds a given set of topics with
//! their partition records, and an empty [`StoredSubtopology`] that a test then
//! fills in with only the fields the scenario needs.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
use uuid::Uuid;

use crate::coordinator::unified::streams::persistence::StoredSubtopology;

fn topic_record(name: &str, id: u8, partitions: i32) -> TopicRecord {
    TopicRecord {
        name: name.to_string(),
        topic_id: Uuid::from_bytes([id; 16]),
        partitions,
        replication_factor: 1,
    }
}

/// Builds an image that holds each `(name, id, partitions)` topic.
///
/// Each topic has `partitions` partition records, so
/// `topic_partition_count` resolves.
pub fn image_with(topics: &[(&str, u8, i32)]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for &(name, id, partitions) in topics {
        image.apply(&MetadataRecord::V1Topic(topic_record(name, id, partitions)));
        for p in 0..partitions {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.to_string(),
                partition: p,
                leader: krabka_audit::NodeId(1),
                replicas: vec![krabka_audit::NodeId(1)],
                isr: vec![krabka_audit::NodeId(1)],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
    }
    image
}

pub fn sub(id: &str) -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: id.to_string(),
        source_topics: vec![],
        source_topic_regex: vec![],
        repartition_sink_topics: vec![],
        state_changelog_topics: vec![],
        repartition_source_topics: vec![],
        copartition_groups: vec![],
    }
}
