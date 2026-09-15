//! The name checks `CreateTopics` runs on a topic before any other check.
//!
//! Kafka runs them in two places. `ControllerApis.createTopics` removes
//! `__cluster_metadata` from the request and answers it `INVALID_REQUEST`.
//! `ReplicationControlManager.validateNewTopicNames` then answers
//! `INVALID_TOPIC_EXCEPTION` for a name that `Topic.validate` refuses, and for
//! a name that collides with an existing topic after `.` and `_` are unified.
//! An existing topic of the same name is not a collision: Kafka answers it
//! `TOPIC_ALREADY_EXISTS`, and so does the committing path here.

use krabka_log::topic_name::{topic_names_collide, validate_topic_name};
use krabka_metadata::MetadataImage;

use crate::codes;

/// The raft metadata log. It is a topic name in Kafka, and no client may
/// create it.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// The error code and message for a topic name that `CreateTopics` refuses,
/// or `None` when the name may go on to the other checks.
pub(super) fn topic_name_error(image: &MetadataImage, name: &str) -> Option<(i16, String)> {
    if name == CLUSTER_METADATA_TOPIC {
        return Some((
            codes::INVALID_REQUEST,
            format!("Creation of internal topic {CLUSTER_METADATA_TOPIC} is prohibited."),
        ));
    }
    if let Err(invalid) = validate_topic_name(name) {
        return Some((codes::INVALID_TOPIC_EXCEPTION, invalid.to_string()));
    }
    if !name.contains(['.', '_']) || image.topic(name).is_some() {
        return None;
    }
    // Kafka names the first colliding topic its set gives. Take the least
    // name, so that the message is deterministic.
    image
        .topics()
        .map(|topic| topic.name.as_str())
        .filter(|existing| *existing != name && topic_names_collide(existing, name))
        .min()
        .map(|existing| {
            (
                codes::INVALID_TOPIC_EXCEPTION,
                format!("Topic '{name}' collides with existing topic: {existing}"),
            )
        })
}
