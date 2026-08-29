//! The `Delete` ACL check that gates `DeleteTopics`.
//!
//! Authorization is batched over every topic the request names, because one
//! denied topic must not fail the whole request: the deny set it produces
//! stamps `TOPIC_AUTHORIZATION_FAILED` on that row and leaves the authorized
//! rows to delete normally.

use std::collections::HashSet;

use krabka_metadata::AclOperation;

use super::request::TopicNameRequest;
use crate::authorizer::{AuthorizationResult, authorize_topics};

/// Batch-authorizes every resolved topic name for `Delete` and returns the
/// names that came back `Deny`.
pub(super) fn denied_topic_names(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    peer: &std::net::SocketAddr,
    requests: &[TopicNameRequest],
) -> HashSet<String> {
    let known_names = requests.iter().filter_map(|(name, _, _)| name.as_deref());
    authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Delete,
        known_names,
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}
