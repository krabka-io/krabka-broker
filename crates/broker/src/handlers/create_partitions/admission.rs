//! The check that `CreatePartitions` runs before it touches a single topic:
//! the duplicate names Kafka refuses outright. The batch authorization that
//! decides which topic rows short-circuit to `TOPIC_AUTHORIZATION_FAILED` is
//! [`crate::handlers::denied_topics`].

use krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic;

/// The names that more than one request row carries, in the order of their
/// first row. Kafka's `ControllerApis.createPartitions` answers each once
/// with `INVALID_REQUEST` and grows none of them.
pub(super) fn duplicate_names(topics: &[CreatePartitionsTopic]) -> Vec<String> {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for topic in topics {
        *counts.entry(topic.name.as_str()).or_insert(0) += 1;
    }
    let mut seen = std::collections::HashSet::new();
    topics
        .iter()
        .map(|topic| topic.name.as_str())
        .filter(|name| counts[name] > 1 && seen.insert(*name))
        .map(str::to_owned)
        .collect()
}
