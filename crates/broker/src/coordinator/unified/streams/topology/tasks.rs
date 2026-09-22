//! The expansion of the task counts into task lists, and the partition
//! snapshot of the external source topics.
//!
//! A task is `(subtopology_id, partition)`. [`super::configure_topics`]
//! decides the number of tasks of each subtopology.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::MetadataImage;

use crate::coordinator::unified::streams::persistence::{
    StreamsGroupPartitionMetadataValue, StreamsGroupTopologyValue, StreamsTopicMeta,
};

/// The partition-count snapshot of every external source topic of `topology`
/// that `image` holds, de-duplicated by name.
///
/// The coordinator persists it as the `StreamsGroupPartitionMetadataValue` of
/// the group.
#[must_use]
pub fn partition_metadata(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> StreamsGroupPartitionMetadataValue {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut topics = Vec::new();
    for sub in &topology.subtopologies {
        for src in &sub.source_topics {
            if seen.contains(src.as_str()) {
                continue;
            }
            if let Some(rec) = image.topic(src) {
                seen.insert(src.as_str());
                topics.push(StreamsTopicMeta {
                    topic_name: src.clone(),
                    topic_id: rec.topic_id,
                    num_partitions: image.topic_partition_count(src),
                });
            }
        }
    }
    StreamsGroupPartitionMetadataValue { topics }
}

/// Expands the per-subtopology task counts into the full set of tasks.
///
/// Each subtopology gets the partition list `0..num_tasks`.
#[must_use]
pub fn task_set(num_tasks: &BTreeMap<String, i32>) -> BTreeMap<String, Vec<i32>> {
    num_tasks
        .iter()
        .map(|(sub, &n)| (sub.clone(), (0..n.max(0)).collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;
    use crate::coordinator::unified::streams::topology::test_support::{image_with, sub};

    #[test]
    fn partition_metadata_snapshots_each_existing_source_topic_once() {
        let image = image_with(&[("in-a", 1, 6)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into(), "missing".into()];
        let mut s1 = sub("1");
        s1.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0, s1],
        };

        assert!(
            partition_metadata(&topology, &image)
                == StreamsGroupPartitionMetadataValue {
                    topics: vec![StreamsTopicMeta {
                        topic_name: "in-a".to_string(),
                        topic_id: Uuid::from_bytes([1; 16]),
                        num_partitions: 6,
                    }],
                }
        );
    }

    #[test]
    fn task_set_enumerates_zero_to_n() {
        let mut num_tasks = BTreeMap::new();
        num_tasks.insert("0".to_string(), 3);
        num_tasks.insert("1".to_string(), 0);
        let set = task_set(&num_tasks);
        assert!(set.get("0").unwrap() == &vec![0, 1, 2]);
        assert!(set.get("1").unwrap().is_empty());
    }
}
