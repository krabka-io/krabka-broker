//! Validation of a stored topology against the current metadata image.
//!
//! The result is the KIP-1071 status list that keeps a member `NotReady`: one
//! entry for each missing source topic and one for each copartition group whose
//! members disagree on their partition count.

use krabka_metadata::MetadataImage;

use super::status;
use crate::coordinator::unified::streams::persistence::StreamsGroupTopologyValue;

/// Validates the topology against the metadata image.
///
/// This function returns a list of `(status_code, message)` pairs, one pair for
/// each unsatisfied condition. An empty vec means that the topology is fully
/// ready: all source topics exist, and all copartition groups have consistent
/// partition counts.
#[must_use]
pub fn validate_topology(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Vec<(i8, String)> {
    let mut out: Vec<(i8, String)> = Vec::new();

    for sub in &topology.subtopologies {
        // Missing source topics (exact names only; regex handled below).
        for src in &sub.source_topics {
            if image.topic(src).is_none() {
                out.push((
                    status::MISSING_SOURCE_TOPICS,
                    format!(
                        "subtopology '{}' references missing source topic '{}'",
                        sub.subtopology_id, src
                    ),
                ));
            }
        }

        // Regex source topics (`source_topic_regex`) are not resolved against
        // the metadata image here. They are treated as satisfiable; exact source
        // names still surface MISSING_SOURCE_TOPICS when absent.

        // Copartition groups: every member topic must have the same (known)
        // partition count. Indices map into this subtopology's topic arrays.
        for cg in &sub.copartition_groups {
            let mut counts: Vec<(String, i32)> = Vec::new();
            for &idx in &cg.source_topics {
                if let Some(name) = sub.source_topics.get(idx_to_usize(idx))
                    && image.topic(name).is_some()
                {
                    let pc = image.topic_partition_count(name);
                    if pc > 0 {
                        counts.push((name.clone(), pc));
                    }
                }
            }
            for &idx in &cg.repartition_source_topics {
                if let Some(info) = sub.repartition_source_topics.get(idx_to_usize(idx))
                    && info.partitions > 0
                {
                    counts.push((info.name.clone(), info.partitions));
                }
            }

            // If two resolvable members disagree, flag the group.
            if let Some((_, first)) = counts.first() {
                let first = *first;
                if let Some((bad_name, bad)) = counts.iter().find(|(_, c)| *c != first) {
                    out.push((
                        status::INCORRECTLY_PARTITIONED_TOPICS,
                        format!(
                            "subtopology '{}' copartition group has mismatched partition counts: \
                             expected {first}, but '{bad_name}' has {bad}",
                            sub.subtopology_id
                        ),
                    ));
                }
            }
        }
    }

    out
}

/// Maps a copartition-group `i16` index to a `usize`.
///
/// The index is a non-negative array offset. A negative index clamps to `0`.
/// The client never emits a negative index.
fn idx_to_usize(idx: i16) -> usize {
    usize::try_from(idx).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::streams::{
        persistence::StoredCopartitionGroup,
        topology::test_support::{image_with, sub},
    };

    #[test]
    fn validate_topology_ready_is_empty() {
        let image = image_with(&[("in-a", 1, 4)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        assert!(validate_topology(&topology, &image).is_empty());
    }

    #[test]
    fn validate_topology_flags_missing_source() {
        let image = image_with(&[]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into()];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let issues = validate_topology(&topology, &image);
        assert!(issues.len() == 1);
        check!(issues[0].0 == status::MISSING_SOURCE_TOPICS);
        check!(issues[0].1.contains("in-a"));
    }

    #[test]
    fn validate_topology_flags_copartition_mismatch() {
        // Two source topics with different partition counts in one copartition
        // group must flag INCORRECTLY_PARTITIONED_TOPICS.
        let image = image_with(&[("in-a", 1, 4), ("in-b", 2, 6)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into(), "in-b".into()];
        s0.copartition_groups = vec![StoredCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: vec![],
            repartition_source_topics: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        let issues = validate_topology(&topology, &image);
        assert!(
            issues
                .iter()
                .any(|(c, _)| *c == status::INCORRECTLY_PARTITIONED_TOPICS)
        );
    }

    #[test]
    fn validate_topology_copartition_match_ok() {
        let image = image_with(&[("in-a", 1, 4), ("in-b", 2, 4)]);
        let mut s0 = sub("0");
        s0.source_topics = vec!["in-a".into(), "in-b".into()];
        s0.copartition_groups = vec![StoredCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: vec![],
            repartition_source_topics: vec![],
        }];
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        };
        assert!(validate_topology(&topology, &image).is_empty());
    }
}
