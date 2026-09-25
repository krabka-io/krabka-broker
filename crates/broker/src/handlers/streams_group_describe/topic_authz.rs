//! Kafka's `handleStreamsGroupDescribe` topology topic-`Describe` filter: a
//! group whose topology names a topic the caller cannot `Describe` (across
//! every source, repartition sink, repartition source and changelog topic the
//! topology references) is hidden. Kafka replaces that group's row with
//! `TOPIC_AUTHORIZATION_FAILED` (29), no topology and no members, instead of
//! disclosing the topic names through the topology.
//!
//! This mirrors `streams_group_heartbeat::topic_authz`'s `required_topics`
//! and `describe_denied`, adapted to the describe view's stored topology
//! shape (`StreamsGroupTopologyValue`/`StoredSubtopology`) rather than the
//! heartbeat request's wire `Topology`.

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use crate::{
    broker::Broker, coordinator::unified::streams::persistence::StreamsGroupTopologyValue,
    handlers::RequestContext,
};

/// Kafka's `requiredTopics`: every source, repartition sink, repartition
/// source and changelog topic of the topology, deduplicated in first-seen
/// order across the subtopologies.
pub(super) fn required_topics(topology: &StreamsGroupTopologyValue) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for subtopology in &topology.subtopologies {
        let names = subtopology
            .source_topics
            .iter()
            .map(String::as_str)
            .chain(
                subtopology
                    .repartition_sink_topics
                    .iter()
                    .map(String::as_str),
            )
            .chain(
                subtopology
                    .repartition_source_topics
                    .iter()
                    .map(|topic| topic.name.as_str()),
            )
            .chain(
                subtopology
                    .state_changelog_topics
                    .iter()
                    .map(|topic| topic.name.as_str()),
            );
        for name in names {
            if seen.insert(name) {
                out.push(name.to_string());
            }
        }
    }
    out
}

/// Kafka's `filterByAuthorized(DESCRIBE, TOPIC, requiredTopics)`: `true` when
/// any of `required` denies `Describe`.
pub(super) fn describe_denied(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    required: &[String],
) -> bool {
    required.iter().any(|topic| {
        crate::handlers::acl_denied(
            broker.config.authorizer.as_ref(),
            image,
            ctx,
            ResourceType::Topic,
            topic,
            AclOperation::Describe,
        )
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{StoredSubtopology, StoredTopicInfo};

    fn topic_info(name: &str) -> StoredTopicInfo {
        StoredTopicInfo {
            name: name.into(),
            partitions: 0,
            replication_factor: 0,
            topic_configs: Vec::new(),
        }
    }

    fn subtopology(id: &str) -> StoredSubtopology {
        StoredSubtopology {
            subtopology_id: id.into(),
            source_topics: Vec::new(),
            source_topic_regex: Vec::new(),
            repartition_sink_topics: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }
    }

    /// Every kind of topic the topology names -- source, sink, repartition
    /// source and changelog -- contributes to `requiredTopics`, and a name
    /// that recurs is counted once, in the order it was first seen.
    #[test]
    fn required_topics_collects_every_kind_once_each() {
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![
                StoredSubtopology {
                    source_topics: vec!["orders".into()],
                    repartition_sink_topics: vec!["rp".into()],
                    ..subtopology("0")
                },
                StoredSubtopology {
                    source_topics: vec!["orders".into()],
                    repartition_source_topics: vec![topic_info("rp")],
                    state_changelog_topics: vec![topic_info("store-changelog")],
                    ..subtopology("1")
                },
            ],
        };

        check!(
            required_topics(&topology)
                == vec![
                    "orders".to_string(),
                    "rp".to_string(),
                    "store-changelog".to_string(),
                ]
        );
    }

    #[test]
    fn required_topics_empty_topology_yields_no_topics() {
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: Vec::new(),
        };

        check!(required_topics(&topology).is_empty());
    }
}
