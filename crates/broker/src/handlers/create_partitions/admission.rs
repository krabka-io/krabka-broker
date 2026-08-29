//! The checks that `CreatePartitions` runs before it touches a single topic:
//! the KIP-599 mutation count that the controller quota charges the caller,
//! and the batch authorization that decides which topic rows short-circuit to
//! `TOPIC_AUTHORIZATION_FAILED`.

use krabka_metadata::AclOperation;
use krabka_protocol::owned::create_partitions_request::CreatePartitionsRequest;

use crate::authorizer::{AuthorizationResult, authorize_topics};

pub(super) fn partition_mutation_count(
    request: &CreatePartitionsRequest,
    image: &krabka_metadata::MetadataImage,
) -> u64 {
    request
        .topics
        .iter()
        .map(|topic| {
            let current =
                i32::try_from(image.partitions_of(&topic.name).count()).unwrap_or(i32::MAX);
            u64::try_from((i64::from(topic.count) - i64::from(current)).max(0))
                .expect("mutation count is non-negative")
        })
        .sum()
}

pub(super) fn denied_topics(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    peer: &std::net::SocketAddr,
    request: &CreatePartitionsRequest,
) -> std::collections::HashSet<String> {
    authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Alter,
        request.topics.iter().map(|topic| topic.name.as_str()),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}
