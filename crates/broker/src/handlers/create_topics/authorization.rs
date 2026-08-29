//! The whole-request authorization gate of `CreateTopics`. Kafka checks
//! `Create` on the `Cluster` resource once for the request, and a denial
//! fails every topic row in it, so the check is a single predicate.

use krabka_metadata::AclOperation;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
};

pub(super) fn cluster_create_denied(
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
            operation: AclOperation::Create,
        },
    ) == AuthorizationResult::Deny
}
