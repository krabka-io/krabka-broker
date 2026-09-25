//! The `Delete` ACL check that gates `DeleteTopics`.
//!
//! Kafka checks `Delete` on the `Cluster` resource once, as a shortcut: an
//! Allow there authorizes every candidate topic name without a further
//! lookup. A Deny is not a whole-request failure -- it falls back to
//! `Delete` on each `Topic(name)` individually
//! (`ControllerApis.handleDeleteTopics`/`deleteTopics`), so a principal
//! scoped to a literal or prefixed topic ACL can still delete the topics its
//! ACL covers.
//!
//! Authorization is batched over every topic the request names, because one
//! denied topic must not fail the whole request: the deny set it produces
//! stamps `TOPIC_AUTHORIZATION_FAILED` on that row and leaves the authorized
//! rows to delete normally.

use std::collections::HashSet;

use krabka_metadata::AclOperation;

use super::request::TopicNameRequest;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
};

/// Whether `Delete` on the `Cluster` resource is denied for this principal.
pub(super) fn cluster_delete_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Delete,
        },
    ) == AuthorizationResult::Deny
}

/// Batch-authorizes every resolved topic name for `Delete` and returns the
/// names that came back `Deny`.
///
/// Cluster `Delete` is checked once first: when it is allowed, every name in
/// `requests` is authorized and this returns empty without a further
/// per-topic lookup. Only when cluster `Delete` is denied does this fall back
/// to `Delete` on each `Topic(name)` individually.
pub(super) fn denied_topic_names(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    requests: &[TopicNameRequest],
) -> HashSet<String> {
    if !cluster_delete_denied(broker, image, context) {
        return HashSet::new();
    }
    let known_names = requests.iter().filter_map(|(name, _, _)| name.as_deref());
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Delete,
        known_names,
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}
