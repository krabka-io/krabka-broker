//! Kafka's `handleStreamsGroupHeartbeat` topology checks: the required
//! topics are read straight off the wire topology, before the group
//! coordinator ever sees the request. A topology that names a Kafka internal
//! topic or an invalid topic name is refused with `STREAMS_INVALID_TOPOLOGY`,
//! and a required topic that `Describe` denies fails the whole request with
//! `TOPIC_AUTHORIZATION_FAILED` -- no partial disclosure, and the group
//! coordinator never runs.

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::streams_group_heartbeat_request::Topology;

use crate::{broker::Broker, handlers::RequestContext};

/// Kafka's `requiredTopics`: every source, repartition sink, repartition
/// source and changelog topic of the topology, deduplicated in first-seen
/// order across the subtopologies.
pub(super) fn required_topics(topology: &Topology) -> Vec<String> {
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

/// Kafka's `Topic.isInternal` and `Topic.isValid` checks on `required`, both
/// `STREAMS_INVALID_TOPOLOGY`, the internal-topic check first.
pub(super) fn invalid_topology_message(broker: &Broker, required: &[String]) -> Option<String> {
    let prohibited: Vec<&str> = required
        .iter()
        .map(String::as_str)
        .filter(|name| crate::internal_topics::is_internal_topic(&broker.config, name))
        .collect();
    if !prohibited.is_empty() {
        return Some(format!(
            "Use of Kafka internal topics {} in a Kafka Streams topology is prohibited.",
            prohibited.join(",")
        ));
    }

    let invalid: Vec<&str> = required
        .iter()
        .map(String::as_str)
        .filter(|name| krabka_log::topic_name::validate_topic_name(name).is_err())
        .collect();
    if !invalid.is_empty() {
        return Some(format!(
            "Topic names {} are not valid topic names.",
            invalid.join(",")
        ));
    }
    None
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
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request::topic_info::TopicInfo,
        streams_group_heartbeat_request::Subtopology,
    };

    use super::*;

    fn info(name: &str) -> TopicInfo {
        TopicInfo {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Every kind of topic the topology names -- source, sink, repartition
    /// source and changelog -- contributes to `requiredTopics`, and a name
    /// that recurs is counted once, in the order it was first seen.
    #[test]
    fn required_topics_collects_every_kind_once_each() {
        let topology = Topology {
            epoch: 1,
            subtopologies: vec![
                Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["orders".into()],
                    repartition_sink_topics: vec!["rp".into()],
                    ..Default::default()
                },
                Subtopology {
                    subtopology_id: "1".into(),
                    source_topics: vec!["orders".into()],
                    repartition_source_topics: vec![info("rp")],
                    state_changelog_topics: vec![info("store-changelog")],
                    ..Default::default()
                },
            ],
            ..Default::default()
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
}
