//! Task derivation: the bounded fixpoint that resolves a task count for every
//! subtopology, and the expansion of those counts into task lists.
//!
//! A task is `(subtopology_id, partition)`, so the task count of a subtopology
//! is the partition count of the topics it reads. The fixpoint exists because a
//! repartition topic carries the task count of its producing subtopology into
//! the consuming one, which can chain.

use std::collections::{BTreeMap, BTreeSet};

use krabka_metadata::MetadataImage;

use crate::coordinator::unified::streams::persistence::{
    StreamsGroupPartitionMetadataValue, StreamsGroupTopologyValue, StreamsTopicMeta,
};

/// The result of a topology resolution against the current metadata image.
///
/// The result holds the per-subtopology task counts, which are partition
/// counts. It also holds the partition-count snapshot of every external source
/// topic that exists.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DerivedTasks {
    /// Subtopology id -> number of tasks, which is the partition count.
    ///
    /// A subtopology with a count that never resolves is not in this map.
    pub num_tasks: BTreeMap<String, i32>,
    /// Partition metadata for the external source topics in the image.
    ///
    /// The coordinator persists this data as the
    /// `StreamsGroupPartitionMetadataValue` of the group.
    pub partition_metadata: StreamsGroupPartitionMetadataValue,
}

/// Derives task counts for every subtopology with a bounded fixpoint over the
/// topology DAG.
///
/// Seeding: an external source topic of a subtopology that is present in
/// `image` contributes its `topic_partition_count`. A
/// `repartition_source_topics` or `state_changelog_topics` entry with an
/// explicit `partitions > 0` contributes that value.
///
/// Linkage: a `repartition_sink_topics` name in subtopology A is a
/// `repartition_source_topics` name in subtopology B. Once `num_tasks(A)` is
/// known, the repartition topic carries `num_tasks(A)` partitions into B.
///
/// This function iterates `subtopologies.len() + 1` times, or until one pass
/// makes no change, to propagate through chained repartitions. It leaves a
/// subtopology with no resolvable input unresolved.
#[must_use]
pub fn derive_tasks(topology: &StreamsGroupTopologyValue, image: &MetadataImage) -> DerivedTasks {
    let mut num_tasks: BTreeMap<String, i32> = BTreeMap::new();

    // Map repartition-sink-topic name -> producing subtopology id, so we can
    // feed the producer's task count into the consumer once it resolves.
    let mut sink_producer: BTreeMap<&str, &str> = BTreeMap::new();
    for sub in &topology.subtopologies {
        for sink in &sub.repartition_sink_topics {
            sink_producer.insert(sink.as_str(), sub.subtopology_id.as_str());
        }
    }

    let max_iters = topology.subtopologies.len() + 1;
    for _ in 0..max_iters {
        let mut changed = false;
        for sub in &topology.subtopologies {
            let mut best: Option<i32> = num_tasks.get(&sub.subtopology_id).copied();

            // External source topics present in the image.
            for src in &sub.source_topics {
                if image.topic(src).is_some() {
                    let pc = image.topic_partition_count(src);
                    if pc > 0 {
                        best = Some(best.map_or(pc, |b| b.max(pc)));
                    }
                }
            }

            // Repartition-source topics: explicit count if given, else the
            // partition count of the upstream subtopology that produces them.
            for rs in &sub.repartition_source_topics {
                if rs.partitions > 0 {
                    best = Some(best.map_or(rs.partitions, |b| b.max(rs.partitions)));
                } else if let Some(&pc) = sink_producer
                    .get(rs.name.as_str())
                    .and_then(|producer| num_tasks.get(*producer))
                {
                    best = Some(best.map_or(pc, |b| b.max(pc)));
                }
            }

            // State-changelog topics with an explicit partition count.
            for cl in &sub.state_changelog_topics {
                if cl.partitions > 0 {
                    best = Some(best.map_or(cl.partitions, |b| b.max(cl.partitions)));
                }
            }

            if let Some(n) = best
                && num_tasks.get(&sub.subtopology_id) != Some(&n)
            {
                num_tasks.insert(sub.subtopology_id.clone(), n);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // Snapshot every external source topic that exists in the image. De-dup by
    // name (a topic can be a source of more than one subtopology).
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

    DerivedTasks {
        num_tasks,
        partition_metadata: StreamsGroupPartitionMetadataValue { topics },
    }
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
    use crate::coordinator::unified::streams::{
        persistence::StoredTopicInfo,
        topology::test_support::{image_with, sub},
    };

    #[test]
    fn derive_tasks_single_external_source() {
        let image = image_with(&[("in-a", 1, 6)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };

        let derived = derive_tasks(&topology, &image);
        assert!(
            derived
                == DerivedTasks {
                    num_tasks: BTreeMap::from([("0".to_string(), 6)]),
                    partition_metadata: StreamsGroupPartitionMetadataValue {
                        topics: vec![StreamsTopicMeta {
                            topic_name: "in-a".to_string(),
                            topic_id: Uuid::from_bytes([1; 16]),
                            num_partitions: 6,
                        }],
                    },
                }
        );
    }

    #[test]
    fn derive_tasks_repartition_chain() {
        // Subtopology 0 reads external "in-a" (3 partitions) and produces
        // repartition sink "rp". Subtopology 1 reads "rp" as a repartition
        // source with no explicit count, so it must inherit num_tasks(0) = 3.
        let image = image_with(&[("in-a", 1, 3)]);

        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        s0.repartition_sink_topics = vec!["rp".into()];

        let mut s1 = sub("1");
        s1.repartition_source_topics = vec![StoredTopicInfo {
            name: "rp".into(),
            partitions: 0, // unknown until upstream resolves
            replication_factor: 0,
            topic_configs: vec![],
        }];

        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0, s1],
        };

        let derived = derive_tasks(&topology, &image);
        // Only the external source topic appears in the partition snapshot.
        assert!(
            derived
                == DerivedTasks {
                    num_tasks: BTreeMap::from([("0".to_string(), 3), ("1".to_string(), 3)]),
                    partition_metadata: StreamsGroupPartitionMetadataValue {
                        topics: vec![StreamsTopicMeta {
                            topic_name: "in-a".to_string(),
                            topic_id: Uuid::from_bytes([1; 16]),
                            num_partitions: 3,
                        }],
                    },
                }
        );
    }

    #[test]
    fn derive_tasks_unresolved_subtopology_absent() {
        // No external source, no resolvable repartition input -> unresolved.
        let image = image_with(&[]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["missing".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let derived = derive_tasks(&topology, &image);
        assert!(!derived.num_tasks.contains_key("0"));
        assert!(derived.partition_metadata.topics.is_empty());
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
