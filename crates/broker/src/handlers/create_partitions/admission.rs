//! The checks that `CreatePartitions` runs before it touches a single topic:
//! the duplicate names Kafka refuses outright, and the batch authorization
//! that decides which topic rows short-circuit to
//! `TOPIC_AUTHORIZATION_FAILED`.

use krabka_metadata::AclOperation;
use krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic;

use crate::authorizer::{AuthorizationResult, authorize_topics};

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

pub(super) fn denied_topics(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    peer: &std::net::SocketAddr,
    names: &[&str],
) -> std::collections::HashSet<String> {
    authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Alter,
        names.iter().copied(),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}
