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

/// KIP-525's second, per-topic check: may this principal be told what the
/// topic it just created is configured with?
///
/// Kafka's `ControllerApis.handleCreateTopics` filters the requested names by
/// `DESCRIBE_CONFIGS` on `Topic(name)` and hands the surviving set to the
/// controller, which fills `configs` for those and stamps
/// `TOPIC_AUTHORIZATION_FAILED` on `topicConfigErrorCode` for the rest. A
/// denial never fails the create: the topic exists either way, and only the
/// disclosure is withheld.
pub(super) fn describe_configs_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    topic: &str,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: krabka_metadata::ResourceType::Topic,
            resource_name: topic,
            operation: AclOperation::DescribeConfigs,
        },
    ) == AuthorizationResult::Deny
}
